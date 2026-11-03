package io.rowweave.commit;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ManifestAbortCleanupAuthorityTest {

    @Test
    void onlyTheExactDurablyAbortedPublicationIsDeletable() {
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                new RunAttemptFence("run-cleanup", "attempt-cleanup", 3L), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain-cleanup", 17L, "publication-cleanup", new byte[32],
                "PREPUBLICATION_FAILED", 10L);
        ManifestCleanupAuthority authority =
                ManifestCleanupAuthority.forDurableAbort(abort);

        assertEquals(ManifestCleanupAuthority.Decision.DELETE_ABORTED,
                authority.decide("domain-cleanup", "publication-cleanup", 17L));
        assertEquals(ManifestCleanupAuthority.Decision.PRESERVE_LIVE,
                authority.decide("domain-cleanup", "another-publication", 18L));
        assertEquals(ManifestCleanupAuthority.Decision.QUARANTINE_CONFLICT,
                authority.decide("domain-cleanup", "publication-cleanup", 18L));
        assertEquals(ManifestCleanupAuthority.Decision.QUARANTINE_CONFLICT,
                authority.decide("another-domain", "publication-cleanup", 17L));
    }
}
