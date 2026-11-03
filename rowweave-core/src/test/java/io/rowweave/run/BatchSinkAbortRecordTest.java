package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchSinkAbortRecordTest {

    @Test
    void canonicalRecordRoundTripsAndOnlyAdvancesDecidedToAborted() {
        byte[] identity = new byte[32];
        identity[0] = 31;
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                new RunAttemptFence("abort-run", "abort-attempt", 3L),
                7L, BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "manifest-domain", 11L, "publication-abort", identity,
                "STAGE_TERMINAL_FAILURE", 101L);

        BatchSinkAbortRecord decoded = BatchSinkAbortRecordCodec.decode(
                BatchSinkAbortRecordCodec.encode(decided));
        assertEquals(decided, decoded);
        assertEquals(BatchSinkAbortRecord.State.ABORT_DECIDED, decoded.state());
        assertArrayEquals(identity, decoded.identityDigest());

        BatchSinkAbortRecord completed = decoded.completed(109L);
        assertEquals(BatchSinkAbortRecord.State.ABORTED, completed.state());
        assertEquals(109L, completed.completedAtMillis());
        assertThrows(IllegalStateException.class, () -> completed.completed(110L));
    }

    @Test
    void rejectsCredentialsPathsAndInvalidIdentity() {
        assertThrows(IllegalArgumentException.class, () ->
                BatchSinkAbortRecord.decided(
                        new RunAttemptFence("abort-run", "abort-attempt", 3L),
                        7L,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                        "manifest-domain", 11L, "publication-abort", new byte[32],
                        "/private/failure.log", 101L));
        assertThrows(IllegalArgumentException.class, () ->
                BatchSinkAbortRecord.decided(
                        new RunAttemptFence("abort-run", "abort-attempt", 3L),
                        7L,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                        "manifest-domain", 11L, "publication-abort", new byte[31],
                        "FAILED", 101L));
    }
}
