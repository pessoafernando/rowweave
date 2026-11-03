package io.rowweave.cluster;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageCommandJoinStrategyWireTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final DataSchema OUT = new DataSchema(
            new Field("id", DataType.IntegerType), new Field("rid", DataType.IntegerType));
    private static final ShuffleEndpoint ENDPOINT = new ShuffleEndpoint("127.0.0.1", 7001);
    private static final ObjectTrustContext TRUST = ObjectTrustContext.localTrusted(
            StageCommandJoinStrategyWireTest.class.getClassLoader());

    // Generated once by the accepted RWXXVSC2/schema-1 writer before 6. Never rewrite with the new writer.
    private static final String SCHEMA_1_JOIN_BASE64 ="UldYWFZTQzIAAAABAAAABgEAAAABAAAAAmlkAAAAAmlkAAAAAgAAAAj/////AAAAAP////8AAAABAAAAAmlkAAAAAAEBAAAACTEyNy4wLjAuMQAAG1n/////AQAAAAAAAAAAAAAAAAAAAAABAAAAAQAAAAJpZAAAAAJpZAAAAAIAAAABAQAAAAIAAAACaWQAAAACaWQAAAACAAAAA3JpZAAAAANyaWQAAAACAAAAAQAAAAJpZAAAAAVJTk5FUv////8A//////////8AAAAAAAAAAP//////////AQAA//////////8AAAAAAAAAAAD//////////////////////////89LrXsfiSy6FhIWifUMgQPhRsUW1dp2Np4aPuhJU+BN";
    private static final String SCHEMA_1_JOIN_SHA256 =
            "d6aa01b8bad3360a05f5e0a46119de5874de8d14e32fb1d8311df7dc7e5568da";

    @Test
    void writerUsesSchema7AndRoundTripsClosedJoinStrategies() {
        for (JoinPhysicalStrategy strategy : JoinPhysicalStrategy.values()) {
            byte[] encoded = encode(join(strategy));

            assertEquals(7, readInt(encoded, 8), "inner command schema");
            StageCommand decoded = StageCommandCodecV2.preflight(encoded, ignored -> { })
                    .materialize(TRUST, ignored -> { });
            assertEquals(strategy, decoded.joinStrategy());
        }
    }

    @Test
    void schema2JoinEvidenceWithoutSourceOwnershipStillDecodesEveryStrategy()
            throws Exception {
        for (JoinPhysicalStrategy strategy : JoinPhysicalStrategy.values()) {
            byte[] schema3 = stripSchema5EndpointFields(
                    stripSchema7ArtifactField(encode(join(strategy))));
            byte[] other = stripSchema5EndpointFields(
                    stripSchema7ArtifactField(encode(join(
                    strategy == JoinPhysicalStrategy.HASH
                            ? JoinPhysicalStrategy.SORT_MERGE
                            : JoinPhysicalStrategy.HASH))));
            int strategyByte = onlyBodyDifference(schema3, other);
            int sourceOwnershipPresent = strategyByte + 1;
            int inboundAuthorizationCount = sourceOwnershipPresent + 1;
            int sinkContextPresent = inboundAuthorizationCount + Integer.BYTES;
            schema3 = removeBodyByte(schema3, sinkContextPresent);
            byte[] withoutInboundAuthorization = removeBodyBytes(
                    schema3, inboundAuthorizationCount, Integer.BYTES);
            byte[] schema2 = removeBodyByte(
                    withoutInboundAuthorization, sourceOwnershipPresent);
            java.nio.ByteBuffer.wrap(schema2, 8, Integer.BYTES).putInt(2);
            resign(schema2);

            StageCommand decoded = StageCommandCodecV2.preflight(schema2, ignored -> { })
                    .materialize(TRUST, ignored -> { });
            assertEquals(strategy, decoded.joinStrategy());
            assertNull(decoded.sourceSplitAssignment());
        }
    }

    @Test
    void frozenSchema1FixtureKeepsDigestAndMapsOldJoinToSortMerge() throws Exception {
        byte[] fixture = Base64.getDecoder().decode(SCHEMA_1_JOIN_BASE64);
        assertEquals(SCHEMA_1_JOIN_SHA256, hex(MessageDigest.getInstance("SHA-256").digest(fixture)));

        StageCommand decoded = StageCommandCodecV2.preflight(fixture, ignored -> { })
                .materialize(TRUST, ignored -> { });

        assertEquals(StageCommand.Role.JOIN_CONSUMER, decoded.role());
        assertEquals(JoinPhysicalStrategy.SORT_MERGE, decoded.joinStrategy());
    }

    @Test
    void unknownSchema5StrategyFailsDuringPreflight() throws Exception {
        byte[] hash = encode(join(JoinPhysicalStrategy.HASH));
        byte[] sortMerge = encode(join(JoinPhysicalStrategy.SORT_MERGE));
        int strategyByte = onlyBodyDifference(hash, sortMerge);
        int ordinalStart = strategyByte - Integer.BYTES + 1;
        Arrays.fill(hash, ordinalStart, ordinalStart + Integer.BYTES, (byte) 0x7f);
        resign(hash);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.preflight(hash, ignored -> { }));
        assertTrue(failure.getMessage().contains("join strategy"), failure.getMessage());
    }

    @Test
    void schema2RejectsMissingOrRoleConflictingStrategyBeforeObjectMaterialization() throws Exception {
        StageCommand missing = join(JoinPhysicalStrategy.HASH);
        setRawStrategy(missing, null);
        assertThrows(IllegalArgumentException.class, () -> encode(missing));

        StageCommand producer = StageCommand.producer(META, 8, new EmptyInput(), "127.0.0.1", 7001);
        setRawStrategy(producer, JoinPhysicalStrategy.HASH);
        assertThrows(IllegalArgumentException.class, () -> encode(producer));

        StageCommand coGroup = join(JoinPhysicalStrategy.HASH).withCoGroupFn(new NoopCoGroup());
        assertThrows(IllegalArgumentException.class, () -> encode(coGroup));
    }

    @Test
    void legacyJavaNullFieldMapsOnlyOldNonCoGroupJoinToSortMerge() throws Exception {
        StageCommand oldJoin = join(JoinPhysicalStrategy.HASH);
        setRawStrategy(oldJoin, null);
        assertEquals(JoinPhysicalStrategy.SORT_MERGE, javaRoundTrip(oldJoin).joinStrategy());

        StageCommand oldCoGroup = join(JoinPhysicalStrategy.HASH).withCoGroupFn(new NoopCoGroup());
        setRawStrategy(oldCoGroup, null);
        assertNull(javaRoundTrip(oldCoGroup).joinStrategy());
    }

    private static StageCommand join(JoinPhysicalStrategy strategy) {
        return StageCommand.joinConsumer(META, META, OUT, 8, 1, 1,
                new String[]{"id"}, new String[]{"id"}, "INNER", strategy, ENDPOINT);
    }

    private static byte[] encode(StageCommand command) {
        return StageCommandCodecV2.encode(command, TRUST, ignored -> { });
    }

    private static StageCommand javaRoundTrip(StageCommand command) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(command);
        }
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return (StageCommand) input.readObject();
        }
    }

    private static void setRawStrategy(StageCommand command, JoinPhysicalStrategy strategy) throws Exception {
        java.lang.reflect.Field field = StageCommand.class.getDeclaredField("joinStrategy");
        field.setAccessible(true);
        field.set(command, strategy);
    }

    private static int onlyBodyDifference(byte[] left, byte[] right) {
        assertEquals(left.length, right.length);
        int difference = -1;
        for (int i = 0; i < left.length - 32; i++) {
            if (left[i] != right[i]) {
                assertEquals(-1, difference, "strategy must have one canonical ordinal difference");
                difference = i;
            }
        }
        assertTrue(difference >= 0, "strategy ordinal must be present in routing bytes");
        return difference;
    }

    /** Schema 4 appends one identity-present byte to each endpoint; this fixture has one sink. */
    private static int legacySinkIdentityMarker(byte[] encoded) {
        byte[] host = "127.0.0.1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        outer:
        for (int offset = 0; offset <= encoded.length - 32 - host.length; offset++) {
            for (int i = 0; i < host.length; i++) {
                if (encoded[offset + i] != host[i]) continue outer;
            }
            int marker = offset + host.length
                    + Integer.BYTES + Integer.BYTES + 1 + 2 * Long.BYTES;
            if (marker < encoded.length - 32 && encoded[marker] == 0) return marker;
        }
        throw new AssertionError("legacy sink endpoint identity marker not found");
    }

    private static byte[] stripSchema5EndpointFields(byte[] encoded) {
        int identityMarker = legacySinkIdentityMarker(encoded);
        // With no identity/grant, schema 5 appends [identity-present=0][grant-present=0].
        byte[] withoutGrantMarker = removeBodyByte(encoded, identityMarker + 1);
        return removeBodyByte(withoutGrantMarker, identityMarker);
    }

    /** Schema 7 appends a nullable portable-artifact slice after coGroup. */
    private static byte[] stripSchema7ArtifactField(byte[] encoded) {
        int artifactLength = encoded.length - 32 - Integer.BYTES;
        assertEquals(-1, readInt(encoded, artifactLength));
        return removeBodyBytes(encoded, artifactLength, Integer.BYTES);
    }

    private static void resign(byte[] envelope) throws Exception {
        int bodyLength = envelope.length - 32;
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(envelope, bodyLength));
        System.arraycopy(digest, 0, envelope, bodyLength, digest.length);
    }

    private static byte[] removeBodyByte(byte[] encoded, int offset) {
        return removeBodyBytes(encoded, offset, 1);
    }

    private static byte[] removeBodyBytes(byte[] encoded, int offset, int length) {
        int bodyLength = encoded.length - 32;
        if (length < 0 || offset < 0 || offset > bodyLength - length) {
            throw new AssertionError("bad remove range");
        }
        byte[] shortened = new byte[encoded.length - length];
        System.arraycopy(encoded, 0, shortened, 0, offset);
        System.arraycopy(encoded, offset + length, shortened, offset,
                bodyLength - offset - length);
        return shortened;
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) result.append(String.format("%02x", value & 0xff));
        return result.toString();
    }

    private static final class NoopCoGroup implements CoGroupFunction {
        private static final long serialVersionUID = 1L;
        @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector collector) { }
        @Override public DataSchema getResultSchema() { return OUT; }
    }

    private static final class EmptyInput implements Source {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }
}

