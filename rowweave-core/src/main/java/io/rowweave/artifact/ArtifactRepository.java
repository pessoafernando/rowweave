package io.rowweave.artifact;

/**
 * Immutable, content-addressed store for plan Artifacts.
 * An implementation validates the codec and the expected Artifact identity in full, and copies the bytes defensively.
 */
public interface ArtifactRepository {

    /** Validates and stores one canonical Artifact idempotently, returning the artifactId computed from the content. */
    String put(byte[] encodedArtifact);

    default String put(byte[] encodedArtifact, java.util.function.LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        throw new UnsupportedOperationException(
                "budget-aware artifact put must be implemented by the repository");
    }

    /** Returns validated Artifact bytes, or null when the Artifact does not exist. */
    byte[] get(String artifactId);

    /** A production backend must call the reservation before any read, copy or validation allocation. */
    default byte[] get(String artifactId, java.util.function.LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        throw new UnsupportedOperationException(
                "budget-aware artifact get must be implemented by the repository");
    }
}
