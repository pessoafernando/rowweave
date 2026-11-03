package io.rowweave.portable;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortableBatchPlanCodecTest {

    private static final int FRAME_HEADER_BYTES = 16;
    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);

    @Test
    void canonicalFrameRoundTripsIndependentOfMapAndCapabilityOrder() {
        PortableBatchPlan first = plan(false);
        PortableBatchPlan reordered = plan(true);
        List<Long> reservations = new ArrayList<>();

        byte[] encoded = PortableBatchPlanCodec.encode(first,
                reservations::add);
        assertEquals("8f01df7c30977bcb114e2096f0c2d95f12ea3960f4572f1d696570e0bf06ecb9",
                sha256Hex(encoded));
        assertArrayEquals(encoded, PortableBatchPlanCodec.encode(reordered));
        assertEquals(first, PortableBatchPlanCodec.decode(encoded,
                reservations::add));
        assertArrayEquals("RWXXPBP1".getBytes(StandardCharsets.US_ASCII),
                PortableBatchPlanCodec.magic());
        assertFalse(contains(encoded, new byte[]{(byte) 0xac, (byte) 0xed, 0, 5}));
        assertTrue(reservations.stream().mapToLong(Long::longValue).sum() > 0);
    }

    @Test
    void corruptTruncatedOversizedAndUnknownVersionFramesFailClosed() {
        byte[] encoded = PortableBatchPlanCodec.encode(plan(false));
        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertCode("CHECKSUM_MISMATCH", corrupt);
        assertCode("FRAME_LENGTH_MISMATCH",
                Arrays.copyOf(encoded, encoded.length - 1));
        byte[] unknownVersion = encoded.clone();
        unknownVersion[PortableBatchPlanCodec.magic().length + 3] = 2;
        assertCode("UNSUPPORTED_ENVELOPE_VERSION", unknownVersion);
        assertCode("LIMIT_EXCEEDED",
                new byte[PortableBatchPlanCodec.MAX_FRAME_BYTES + 1]);
    }

    @Test
    void unknownOpcodeIsRejectedAfterIntegrityAndBeforeBindingUse()
            throws Exception {
        byte[] encoded = PortableBatchPlanCodec.encode(plan(false));
        byte[] unknownOpcode = encoded.clone();
        int opcodeOffset = firstNodeOpcodeOffset(unknownOpcode);
        ByteBuffer.wrap(unknownOpcode).putInt(opcodeOffset, 999);
        rewriteChecksum(unknownOpcode);

        assertCode("UNKNOWN_OPCODE", unknownOpcode);
    }

    private static PortableBatchPlan plan(boolean reverse) {
        PortableRowSchema schema = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
        Map<String, String> scalar = new LinkedHashMap<>();
        Map<String, List<String>> lists = new LinkedHashMap<>();
        if (reverse) {
            scalar.put("zeta", "last");
            scalar.put("alpha", "first");
            lists.put("zeta", Collections.singletonList("amount"));
            lists.put("alpha", Collections.singletonList("id"));
        } else {
            scalar.put("alpha", "first");
            scalar.put("zeta", "last");
            lists.put("alpha", Collections.singletonList("id"));
            lists.put("zeta", Collections.singletonList("amount"));
        }
        PortablePlanNode source = new PortablePlanNode(
                "source", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                scalar, lists, new PortableSourceBinding(
                        "jdbc-v1", DIGEST_A, new byte[]{1, 2},
                        Arrays.asList(new byte[]{3}, new byte[]{4})));
        PortablePlanNode sink = new PortablePlanNode(
                "sink", PortableOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSinkBinding(
                        "manifest-file-v1", DIGEST_B,
                        new byte[]{5}, new byte[]{6}));
        List<String> capabilities = reverse
                ? Arrays.asList("rowweave.source.jdbc.v1", "rowweave.runtime.batch.v1")
                : Arrays.asList("rowweave.runtime.batch.v1", "rowweave.source.jdbc.v1");
        return new PortableBatchPlan(1, 2,
                Collections.singletonList("sink"),
                Arrays.asList(source, sink), capabilities);
    }

    private static int firstNodeOpcodeOffset(byte[] frame) {
        ByteBuffer body = ByteBuffer.wrap(frame);
        body.position(FRAME_HEADER_BYTES + 8);
        skipTexts(body, body.getInt());
        skipTexts(body, body.getInt());
        assertTrue(body.getInt() > 0);
        skipText(body);
        return body.position();
    }

    private static void skipTexts(ByteBuffer input, int count) {
        for (int index = 0; index < count; index++) skipText(input);
    }

    private static void skipText(ByteBuffer input) {
        int length = input.getInt();
        input.position(input.position() + length);
    }

    private static void rewriteChecksum(byte[] frame) throws Exception {
        int bodyLength = ByteBuffer.wrap(frame).getInt(12);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(frame, FRAME_HEADER_BYTES, bodyLength);
        byte[] checksum = digest.digest();
        System.arraycopy(checksum, 0, frame,
                FRAME_HEADER_BYTES + bodyLength, checksum.length);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                hex.append(Character.forDigit((current >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(current & 0xf, 16));
            }
            return hex.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static boolean contains(byte[] source, byte[] target) {
        for (int start = 0; start <= source.length - target.length; start++) {
            boolean match = true;
            for (int index = 0; index < target.length; index++) {
                if (source[start + index] != target[index]) {
                    match = false;
                    break;
                }
            }
            if (match) return true;
        }
        return false;
    }

    private static void assertCode(String code, byte[] frame) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class,
                () -> PortableBatchPlanCodec.decode(frame));
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
