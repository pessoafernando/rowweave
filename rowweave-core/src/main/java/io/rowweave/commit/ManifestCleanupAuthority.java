package io.rowweave.commit;

import io.rowweave.run.BatchSinkAbortRecord;

/** Fail-closed proof source consulted before an expired manifest claim may be cleaned. */
@FunctionalInterface
public interface ManifestCleanupAuthority {

    enum Decision {
        PRESERVE_LIVE,
        DELETE_ABORTED,
        DELETE_UNREFERENCED_EXPIRED,
        QUARANTINE_CONFLICT
    }

    Decision decide(String domainId, String attemptId, long generation);

    /**
     * Builds the narrow production proof accepted by Manifest v1 cleanup. The abort record is an
     * irreversible authority fact: commit intent cannot subsequently be established for it.
     */
    static ManifestCleanupAuthority forDurableAbort(BatchSinkAbortRecord abort) {
        if (abort == null) throw new IllegalArgumentException("abort record is required");
        return (domainId, attemptId, generation) -> {
            if (!abort.publicationId().equals(attemptId)) {
                return Decision.PRESERVE_LIVE;
            }
            if (!abort.commitDomainId().equals(domainId)
                    || abort.generation() != generation) {
                return Decision.QUARANTINE_CONFLICT;
            }
            return Decision.DELETE_ABORTED;
        };
    }
}
