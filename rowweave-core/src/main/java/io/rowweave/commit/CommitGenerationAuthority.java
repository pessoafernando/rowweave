package io.rowweave.commit;

import java.nio.file.Path;

/** Allocates a durable, strictly monotonic and never-reused generation for one manifest commit domain. */
public interface CommitGenerationAuthority {

    /** A successful reservation for the same {@code attemptId} must idempotently return the same generation. */
    long reserve(Path domain, String attemptId);

    /** Read-only lookup of an existing claim; returns null when absent and never allocates a new generation. */
    Long find(Path domain, String attemptId);
}
