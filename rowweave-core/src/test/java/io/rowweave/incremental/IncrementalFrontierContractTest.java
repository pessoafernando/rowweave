package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical frontier and core-owned ordering contract. */
class IncrementalFrontierContractTest {

    @Test
    void allClosedFrontierKindsRoundTripCanonically() throws Exception {
        OrderedFrontierKey orderedKey = OrderedFrontierKey.of(
                OrderedFrontierKey.int64(-9L),
                OrderedFrontierKey.utf8("\u5BA2\u6237-A"),
                OrderedFrontierKey.bytes(new byte[]{0, 1, (byte) 0xff}));
        IncrementalFrontier[] values = {
                IncrementalFrontier.begin(),
                IncrementalFrontier.int64Sequence(42L),
                IncrementalFrontier.timestampKey(-123456789L, orderedKey),
                IncrementalFrontier.manifestGeneration(7L, sha256("manifest-7"))
        };

        for (IncrementalFrontier value : values) {
            byte[] encoded = IncrementalFrontierCodec.encode(value);
            assertTrue(encoded.length <= IncrementalFrontierCodec.MAX_FRAME_BYTES);
            IncrementalFrontier decoded = IncrementalFrontierCodec.decode(encoded);
            assertEquals(value, decoded);
            assertArrayEquals(encoded, IncrementalFrontierCodec.encode(decoded));
        }
    }

    @Test
    void frontierFactoriesRejectInvalidAndMutableInputs() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontier.int64Sequence(-1L));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontier.manifestGeneration(-1L, sha256("x")));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontier.manifestGeneration(1L, new byte[31]));

        byte[] digest = sha256("immutable");
        IncrementalFrontier frontier = IncrementalFrontier.manifestGeneration(1L, digest);
        digest[0] ^= 1;
        assertArrayEquals(sha256("immutable"), frontier.manifestDigest());
        byte[] returned = frontier.manifestDigest();
        returned[1] ^= 1;
        assertArrayEquals(sha256("immutable"), frontier.manifestDigest());

        byte[] full = new byte[OrderedFrontierKey.MAX_COMPONENT_BYTES];
        Arrays.fill(full, (byte) 1);
        byte[] exactRemainder = new byte[996];
        Arrays.fill(exactRemainder, (byte) 1);
        OrderedFrontierKey exactFrontierLimit = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(full),
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(exactRemainder));
        assertEquals(IncrementalFrontierCodec.MAX_PAYLOAD_BYTES - 12,
                exactFrontierLimit.canonicalOrderBytes().length);
        IncrementalFrontier.timestampKey(0L, exactFrontierLimit);

        byte[] overRemainder = new byte[997];
        Arrays.fill(overRemainder, (byte) 1);
        OrderedFrontierKey tooLargeForFrontier = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(full),
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(overRemainder));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontier.timestampKey(0L, tooLargeForFrontier));
    }

    @Test
    void manifestGenerationNeverOrdersEqualGenerationByDigest() throws Exception {
        IncrementalFrontier first = IncrementalFrontier.manifestGeneration(9L, sha256("a"));
        IncrementalFrontier same = IncrementalFrontier.manifestGeneration(9L, sha256("a"));
        IncrementalFrontier swapped = IncrementalFrontier.manifestGeneration(9L, sha256("b"));
        IncrementalFrontier later = IncrementalFrontier.manifestGeneration(10L, sha256("c"));

        assertEquals(0, IncrementalFrontierOrder.compareConcrete(first, same));
        assertTrue(IncrementalFrontierOrder.compareConcrete(first, later) < 0);
        assertThrows(FrontierConflictException.class,
                () -> IncrementalFrontierOrder.compareConcrete(first, swapped));
    }

    @Test
    void beginAndDifferentConcreteKindsAreNeverOrdinaryComparable() throws Exception {
        IncrementalFrontier sequence = IncrementalFrontier.int64Sequence(1L);
        IncrementalFrontier timestamp = IncrementalFrontier.timestampKey(
                1L, OrderedFrontierKey.of(OrderedFrontierKey.int64(1L)));

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierOrder.compareConcrete(IncrementalFrontier.begin(), sequence));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierOrder.compareConcrete(sequence, timestamp));
        assertTrue(IncrementalFrontierOrder.isSnapshotTransition(
                IncrementalFrontier.begin(), sequence, IncrementalFrontier.Kind.INT64_SEQUENCE_V1));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierOrder.isSnapshotTransition(
                        IncrementalFrontier.begin(), sequence,
                        IncrementalFrontier.Kind.TIMESTAMP_KEY_V1));
    }

    @Test
    void timestampOrderUsesEpochThenCoreOrderedKey() {
        OrderedFrontierKey negative = OrderedFrontierKey.of(OrderedFrontierKey.int64(-1L));
        OrderedFrontierKey zero = OrderedFrontierKey.of(OrderedFrontierKey.int64(0L));
        OrderedFrontierKey textA = OrderedFrontierKey.of(OrderedFrontierKey.utf8("a"));
        OrderedFrontierKey textB = OrderedFrontierKey.of(OrderedFrontierKey.utf8("b"));

        assertTrue(IncrementalFrontierOrder.compareConcrete(
                IncrementalFrontier.timestampKey(10L, negative),
                IncrementalFrontier.timestampKey(10L, zero)) < 0);
        assertTrue(IncrementalFrontierOrder.compareConcrete(
                IncrementalFrontier.timestampKey(10L, textA),
                IncrementalFrontier.timestampKey(10L, textB)) < 0);
        assertTrue(IncrementalFrontierOrder.compareConcrete(
                IncrementalFrontier.timestampKey(9L, textB),
                IncrementalFrontier.timestampKey(10L, textA)) < 0);
    }

    @Test
    void orderedKeyWireIsCanonicalOrderPreservingAndPrefixSafe() {
        OrderedFrontierKey[] logicalOrder = {
                OrderedFrontierKey.of(OrderedFrontierKey.int64(Long.MIN_VALUE)),
                OrderedFrontierKey.of(OrderedFrontierKey.int64(-1L)),
                OrderedFrontierKey.of(OrderedFrontierKey.int64(0L)),
                OrderedFrontierKey.of(OrderedFrontierKey.int64(Long.MAX_VALUE)),
                OrderedFrontierKey.of(OrderedFrontierKey.utf8("a")),
                OrderedFrontierKey.of(OrderedFrontierKey.utf8("a"), OrderedFrontierKey.int64(0L)),
                OrderedFrontierKey.of(OrderedFrontierKey.utf8("aa")),
                OrderedFrontierKey.of(OrderedFrontierKey.utf8("b"))
        };

        for (int i = 0; i + 1 < logicalOrder.length; i++) {
            OrderedFrontierKey left = logicalOrder[i];
            OrderedFrontierKey right = logicalOrder[i + 1];
            assertTrue(OrderedFrontierKey.compare(left, right) < 0);
            assertTrue(unsignedLexicographic(
                    left.canonicalOrderBytes(), right.canonicalOrderBytes()) < 0,
                    "wire order must match logical order at index " + i);
            assertEquals(left, OrderedFrontierKey.decode(left.canonicalOrderBytes()));
            assertArrayEquals(left.canonicalOrderBytes(),
                    OrderedFrontierKey.decode(left.canonicalOrderBytes()).canonicalOrderBytes());
        }

        OrderedFrontierKey zero = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(new byte[]{0}));
        OrderedFrontierKey doubleZero = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(new byte[]{0, 0}));
        OrderedFrontierKey one = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(new byte[]{1}));
        assertTrue(unsignedLexicographic(
                zero.canonicalOrderBytes(), doubleZero.canonicalOrderBytes()) < 0);
        assertTrue(unsignedLexicographic(
                doubleZero.canonicalOrderBytes(), one.canonicalOrderBytes()) < 0);

        Random random = new Random(215L);
        for (int i = 0; i < 1000; i++) {
            long leftValue = random.nextLong();
            long rightValue = random.nextLong();
            OrderedFrontierKey left = OrderedFrontierKey.of(
                    OrderedFrontierKey.int64(leftValue));
            OrderedFrontierKey right = OrderedFrontierKey.of(
                    OrderedFrontierKey.int64(rightValue));
            assertEquals(Integer.signum(Long.compare(leftValue, rightValue)),
                    Integer.signum(unsignedLexicographic(
                            left.canonicalOrderBytes(), right.canonicalOrderBytes())));
        }
    }

    @Test
    void orderedKeyRejectsNullNonCanonicalTextAndMalformedWire() {
        assertEquals(OrderedFrontierKey.MAX_COMPONENT_BYTES,
                OrderedFrontierKey.MAX_COMPONENT_CODE_UNITS);
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.of((OrderedFrontierKey.Component) null));
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.utf8("e\u0301"));
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.utf8("\ud800"));
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.utf8("\udc00"));
        OrderedFrontierKey question = OrderedFrontierKey.of(OrderedFrontierKey.utf8("?"));
        assertEquals(question, OrderedFrontierKey.decode(question.canonicalOrderBytes()));
        char[] oversizedAscii = new char[OrderedFrontierKey.MAX_COMPONENT_CODE_UNITS + 1];
        Arrays.fill(oversizedAscii, 'a');
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.utf8(new String(oversizedAscii)));
        char[] oversizedUtf8 = new char[513];
        Arrays.fill(oversizedUtf8, '\u4e2d');
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.utf8(new String(oversizedUtf8)));
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.bytes(new byte[OrderedFrontierKey.MAX_COMPONENT_BYTES + 1]));
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.decode(new byte[]{99, 0}));

        byte[] valid = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(new byte[]{0, (byte) 0xff})).canonicalOrderBytes();
        assertNotEquals(0, valid.length);
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.decode(Arrays.copyOf(valid, valid.length - 1)));

        ByteArrayOutputStream tooMany = new ByteArrayOutputStream();
        tooMany.write(0x4f);
        tooMany.write(0x4b);
        tooMany.write(1);
        for (int i = 0; i < OrderedFrontierKey.MAX_COMPONENTS + 1; i++) {
            tooMany.write(0x30);
            tooMany.write(0);
            tooMany.write(0);
        }
        tooMany.write(0);
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.decode(tooMany.toByteArray()));
        assertThrows(IllegalArgumentException.class,
                () -> OrderedFrontierKey.decode(new byte[]{0x4f, 0x4b, 1, 0x30, 0, 1, 0}));
    }

    @Test
    void frontierCodecRejectsUnknownVersionKindCorruptionAndTrailingBytes() {
        byte[] valid = IncrementalFrontierCodec.encode(
                IncrementalFrontier.int64Sequence(5L));

        byte[] unknownVersion = valid.clone();
        unknownVersion[4] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(unknownVersion));

        byte[] unknownKind = valid.clone();
        unknownKind[5] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(unknownKind));

        byte[] unknownCodec = valid.clone();
        unknownCodec[6] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(unknownCodec));

        byte[] negativePayload = valid.clone();
        Arrays.fill(negativePayload, 7, 11, (byte) 0xff);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(negativePayload));

        byte[] mismatchedPayload = valid.clone();
        mismatchedPayload[10] = 7;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(mismatchedPayload));

        byte[] corrupt = valid.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(corrupt));

        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(trailing));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalFrontierCodec.decode(
                        new byte[IncrementalFrontierCodec.MAX_FRAME_BYTES + 1]));
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static int unsignedLexicographic(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int compared = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length, right.length);
    }
}
