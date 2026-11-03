package io.rowweave.commit;

import java.nio.file.Path;

/** SPI for the generation and active-writer fencing of a cluster manifest domain. */
public interface ManifestCommitAuthority extends CommitGenerationAuthority {

    /** Non-blocking acquisition of the domain writer lease; returns null when the domain is held. */
    Lease tryAcquire(Path domain, String attemptId);

    interface Lease extends AutoCloseable {
        @Override
        void close();
    }
}
