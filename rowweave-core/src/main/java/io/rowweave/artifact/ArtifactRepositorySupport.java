package io.rowweave.artifact;

import java.util.Arrays;

/** Untrusted-input fence shared by ArtifactRepository backends. */
final class ArtifactRepositorySupport {

    static final int ARTIFACT_ID_LENGTH = 64;

    private ArtifactRepositorySupport() {
    }

    static StoredArtifact validateEncoded(byte[] encodedArtifact) {
        return validateEncoded(encodedArtifact, value -> { });
    }

    static StoredArtifact validateEncoded(byte[] encodedArtifact,
                                          java.util.function.LongConsumer reservation) {
        if (encodedArtifact == null) {
            throw new IllegalArgumentException("encodedArtifact is required");
        }
        PlanArtifact artifact = PlanArtifactCodec.decode(encodedArtifact, reservation);
        byte[] canonical = PlanArtifactCodec.encode(artifact, reservation);
        if (!Arrays.equals(encodedArtifact, canonical)) {
            throw new PlanArtifactValidationException(
                    "NON_CANONICAL_ARTIFACT", "envelope",
                    "artifact encoding is not canonical");
        }
        return new StoredArtifact(artifact.artifactId(), canonical);
    }

    static String validateId(String artifactId) {
        if (artifactId == null) {
            throw new IllegalArgumentException("artifactId is required");
        }
        if (artifactId.length() != ARTIFACT_ID_LENGTH) {
            throw new IllegalArgumentException(
                    "artifactId must be 64 lowercase hexadecimal characters");
        }
        for (int i = 0; i < artifactId.length(); i++) {
            char value = artifactId.charAt(i);
            if (!((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f'))) {
                throw new IllegalArgumentException(
                        "artifactId must be 64 lowercase hexadecimal characters");
            }
        }
        return artifactId;
    }

    static byte[] validateStored(String expectedArtifactId, byte[] encodedArtifact) {
        return validateStored(expectedArtifactId, encodedArtifact, value -> { });
    }

    static byte[] validateStored(String expectedArtifactId, byte[] encodedArtifact,
                                 java.util.function.LongConsumer reservation) {
        String expected = validateId(expectedArtifactId);
        StoredArtifact stored = validateEncoded(encodedArtifact, reservation);
        if (!expected.equals(stored.artifactId)) {
            throw new IllegalStateException(
                    "artifact content identity mismatch: expected=" + expected
                            + " actual=" + stored.artifactId);
        }
        return stored.bytes;
    }

    static final class StoredArtifact {
        final String artifactId;
        final byte[] bytes;

        StoredArtifact(String artifactId, byte[] bytes) {
            this.artifactId = artifactId;
            this.bytes = bytes;
        }
    }
}
