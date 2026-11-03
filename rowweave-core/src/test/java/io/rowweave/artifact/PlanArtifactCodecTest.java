package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static io.rowweave.artifact.PlanArtifactModelTest.artifact;
import static io.rowweave.artifact.PlanArtifactModelTest.requiredFeatures;
import static io.rowweave.artifact.PlanArtifactModelTest.source;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlanArtifactCodecTest {

    @Test
    void canonicalWireAndArtifactIdAreDeterministic() {
        PlanArtifact left = artifact("definition-1", requiredFeatures(),
                Arrays.asList("z.optional", "a.optional"),
                Collections.singletonList(source("source-1")), new byte[]{4, 5, 6});
        PlanArtifact right = artifact("definition-1", Arrays.asList(
                        PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS,
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH),
                Arrays.asList("a.optional", "z.optional"),
                Collections.singletonList(source("source-1")), new byte[]{4, 5, 6});

        byte[] encoded = PlanArtifactCodec.encode(left);
        assertEquals(encoded.length, PlanArtifactCodec.encodedFrameSize(left));
        assertArrayEquals(encoded, PlanArtifactCodec.encode(right));
        assertEquals(encoded.length, PlanArtifactCodec.encodedFrameSize(right));
        byte[] magic = PlanArtifactCodec.magic();
        assertArrayEquals(magic, Arrays.copyOf(encoded, magic.length));

        PlanArtifact decoded = PlanArtifactCodec.decode(encoded);
        assertEquals(left.artifactId(), decoded.artifactId());
        assertEquals(left.artifactId(), PlanArtifactCodec.artifactId(encoded));
        assertEquals(left.definitionId(), decoded.definitionId());
        assertArrayEquals(left.payload(), decoded.payload());
    }

    @Test
    void exactFrameSizeHandlesBmpAndSupplementaryUtf8WithoutEncoding() {
        PlanArtifact artifact = artifact("\u5B9A\u4E49-\uD83D\uDE80", requiredFeatures(),
                Collections.singletonList("\u53EF\u9009-\uD83D\uDE80"),
                Collections.singletonList(source("\u6E90-\uD83D\uDE80")),
                new byte[] {1, 2, 3, 4});

        assertEquals(PlanArtifactCodec.encode(artifact).length,
                PlanArtifactCodec.encodedFrameSize(artifact));
    }

    @Test
    void everyTruncationAndAnyTrailingByteFailClosed() {
        byte[] encoded = PlanArtifactCodec.encode(artifact("definition-1", requiredFeatures(),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[]{1, 2, 3}));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(PlanArtifactValidationException.class,
                    () -> PlanArtifactCodec.decode(truncated), "accepted truncation at " + length);
        }
        assertThrows(PlanArtifactValidationException.class,
                () -> PlanArtifactCodec.decode(Arrays.copyOf(encoded, encoded.length + 1)));
    }

    @Test
    void rejectsMagicVersionLengthChecksumAndMalformedUtf8BeforePayloadUse() throws Exception {
        byte[] encoded = PlanArtifactCodec.encode(artifact("def", requiredFeatures(),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[]{1}));

        byte[] magic = encoded.clone();
        magic[0] ^= 1;
        assertCode("INVALID_MAGIC", magic);

        byte[] version = encoded.clone();
        ByteBuffer.wrap(version).putInt(8, 99);
        assertCode("UNSUPPORTED_ENVELOPE_VERSION", version);

        byte[] length = encoded.clone();
        ByteBuffer.wrap(length).putInt(12, PlanArtifactLimits.MAX_BODY_BYTES + 1);
        assertCode("LIMIT_EXCEEDED", length);

        byte[] checksum = encoded.clone();
        checksum[checksum.length - 1] ^= 1;
        assertCode("CHECKSUM_MISMATCH", checksum);

        byte[] malformed = encoded.clone();
        int definitionStart = 16 + 4 + 4 + 4;
        malformed[definitionStart] = (byte) 0xc3;
        malformed[definitionStart + 1] = 0x28;
        rewriteChecksum(malformed);
        assertCode("MALFORMED_UTF8", malformed);
    }

    @Test
    void featureAndRuntimeNegotiationAreExplicit() {
        PlanArtifact unknownRequired = artifact("definition-1",
                Arrays.asList(PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
                        PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS, "future.required"),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[0]);
        assertCode("UNSUPPORTED_REQUIRED_FEATURE", PlanArtifactCodec.encode(unknownRequired));

        PlanArtifact unknownOptional = artifact("definition-1", requiredFeatures(),
                Collections.singletonList("future.optional"),
                Collections.singletonList(source("source-1")), new byte[0]);
        PlanArtifact optionalBack = PlanArtifactCodec.decode(PlanArtifactCodec.encode(unknownOptional));
        assertEquals(Collections.singletonList("future.optional"), optionalBack.optionalFeatures());

        PlanArtifact stream = new PlanArtifact(
                "stream-definition", 1, 1,
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.EVENT_TIME,
                requiredFeatures(), Collections.emptyList(),
                Collections.singletonList(source("source-1")),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH, new byte[0]);
        PlanArtifact streamBack = PlanArtifactCodec.decode(PlanArtifactCodec.encode(stream));
        assertEquals(PlanArtifact.RuntimeTarget.STREAM, streamBack.runtimeTarget());
        PlanArtifactValidationException unsupported = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateForCurrentRuntime(streamBack));
        assertEquals("UNSUPPORTED_RUNTIME", unsupported.code());
        assertFalse(unsupported.path().isEmpty());
    }

    @Test
    void residentCallbackPrechargesDecodeBuffersStringsPayloadAndObjects() {
        byte[] payload = new byte[257];
        byte[] encoded = PlanArtifactCodec.encode(artifact("definition-1", requiredFeatures(),
                Collections.emptyList(), Collections.singletonList(source("source-1")), payload));
        AtomicLong charged = new AtomicLong();

        PlanArtifact decoded = PlanArtifactCodec.decode(encoded, charged::addAndGet);

        assertArrayEquals(payload, decoded.payload());
        org.junit.jupiter.api.Assertions.assertTrue(charged.get() > payload.length * 2L + 512L,
                "payload copies and decoded structures must be charged before allocation");
    }

    @Test
    void rejectsUnsupportedReaderSchemaUnknownEnumAndMaliciousCounts() throws Exception {
        PlanArtifact newerReader = versionedArtifact(1, PlanArtifact.CURRENT_READER_VERSION + 1);
        assertCode("READER_TOO_OLD", PlanArtifactCodec.encode(newerReader));

        PlanArtifact newerSchema = versionedArtifact(PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION + 1, 1);
        assertCode("UNSUPPORTED_PLAN_SCHEMA", PlanArtifactCodec.encode(newerSchema));

        byte[] unknownRuntime = PlanArtifactCodec.encode(artifact("def", requiredFeatures(),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[0]));
        int bodyStart = 16;
        int runtimeOffsetInBody = 4 + 4 + 4 + 3;
        ByteBuffer.wrap(unknownRuntime).putInt(bodyStart + runtimeOffsetInBody, 999);
        rewriteChecksum(unknownRuntime);
        assertCode("INVALID_SEMANTICS", unknownRuntime);

        byte[] excessiveFeatures = PlanArtifactCodec.encode(artifact("def", requiredFeatures(),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[0]));
        int requiredCountOffsetInBody = runtimeOffsetInBody + 4 * 4;
        ByteBuffer.wrap(excessiveFeatures).putInt(
                bodyStart + requiredCountOffsetInBody, PlanArtifactLimits.MAX_FEATURES + 1);
        rewriteChecksum(excessiveFeatures);
        assertCode("LIMIT_EXCEEDED", excessiveFeatures);

        byte[] duplicateFeatures = PlanArtifactCodec.encode(artifact("def", Arrays.asList("aa", "bb"),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[0]));
        int firstFeatureLength = bodyStart + requiredCountOffsetInBody + 4;
        int secondFeatureText = firstFeatureLength + 4 + 2 + 4;
        duplicateFeatures[secondFeatureText] = 'a';
        duplicateFeatures[secondFeatureText + 1] = 'a';
        rewriteChecksum(duplicateFeatures);
        assertCode("DUPLICATE_FEATURE", duplicateFeatures);
    }

    private static PlanArtifact versionedArtifact(int planSchemaVersion, int minimumReaderVersion) {
        return new PlanArtifact(
                "definition-1", planSchemaVersion, minimumReaderVersion,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                requiredFeatures(), Collections.emptyList(),
                Collections.singletonList(source("source-1")),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH, new byte[0]);
    }

    private static void assertCode(String expected, byte[] encoded) {
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class, () -> PlanArtifactCodec.decode(encoded));
        assertEquals(expected, failure.code(), failure.getMessage());
    }

    private static void rewriteChecksum(byte[] encoded) throws Exception {
        int bodyLength = ByteBuffer.wrap(encoded).getInt(12);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                Arrays.copyOfRange(encoded, 16, 16 + bodyLength));
        System.arraycopy(digest, 0, encoded, 16 + bodyLength, digest.length);
    }
}
