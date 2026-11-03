package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** outbound cohort metadata is attempt-owned and resident-budgeted. */
class OutboundShuffleCohortRegistryTest {
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-outbound", "attempt-outbound", 41L);
    private static final UUID TARGET = new UUID(43L, 47L);

    @Test
    void firstRegistrationChargesBeforePublicationAndDuplicateDoesNotDoubleCharge() {
        long charge = OutboundShuffleCohortRegistry.residentCharge(2);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(charge);
        OutboundShuffleCohortRegistry registry =
                new OutboundShuffleCohortRegistry(budget);
        byte[] cohort = filled((byte) 0x51);
        ShuffleEndpoint first = endpoint(TARGET, 1, cohort, 2);
        ShuffleEndpoint second = endpoint(TARGET, 2, cohort, 2);

        OutboundShuffleCohortRegistry.PreparedHandle one = registry.prepare(
                "assignment-one", FENCE, Collections.singletonList(first));
        assertEquals(charge, budget.snapshot().usedBytes());
        assertEquals(1, registry.cohortCount());

        OutboundShuffleCohortRegistry.PreparedHandle two = registry.prepare(
                "assignment-two", FENCE, Collections.singletonList(second));
        assertEquals(charge, budget.snapshot().usedBytes());
        RuntimeException duplicate = assertThrows(IllegalStateException.class,
                () -> registry.prepare("assignment-duplicate", FENCE,
                        Collections.singletonList(first)));
        assertFalse(duplicate.getMessage().contains("81, 81"));
        assertEquals(charge, budget.snapshot().usedBytes());

        one.close();
        two.close();
        assertEquals(charge, budget.snapshot().usedBytes(),
                "assignment handles do not own the cohort root");
        java.util.List<byte[]> cancelled = registry.cancelAttempt(FENCE);
        assertEquals(1, cancelled.size());
        assertArrayEquals(cohort, cancelled.get(0),
                "transport cleanup receives the exact non-secret cohort id");
        assertEquals(0, registry.cohortCount());
        assertEquals(0L, budget.snapshot().usedBytes());
        registry.close();
    }

    @Test
    void terminalCoverageAndReturnedWireOwnershipReleaseTheRoot() {
        long charge = OutboundShuffleCohortRegistry.residentCharge(2);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(charge * 2L);
        OutboundShuffleCohortRegistry registry =
                new OutboundShuffleCohortRegistry(budget);
        byte[] cohort = filled((byte) 0x52);
        ShuffleEndpoint first = endpoint(TARGET, 1, cohort, 2);
        ShuffleEndpoint second = endpoint(TARGET, 2, cohort, 2);
        OutboundShuffleCohortRegistry.PreparedHandle prepared = registry.prepare(
                "assignment", FENCE, Arrays.asList(first, second));
        OutboundShuffleCohortRegistry.StreamHandle one = registry.open(first);
        OutboundShuffleCohortRegistry.StreamHandle two = registry.open(second);

        one.recordSent(73L);
        one.close();
        two.close();
        prepared.close();
        assertEquals(charge, budget.snapshot().usedBytes(),
                "delayed credit ownership must retain the root after all streams finish");
        one.recordReturned(73L);
        assertEquals(0L, budget.snapshot().usedBytes());
        assertEquals(0, registry.cohortCount());
        registry.close();
    }

    @Test
    void exhaustedBudgetAndDescriptorConflictLeaveNoPartialRoot() {
        long charge = OutboundShuffleCohortRegistry.residentCharge(1);
        ControlResidentBudget tiny = ControlResidentBudget.forTesting(charge - 1L);
        OutboundShuffleCohortRegistry rejected =
                new OutboundShuffleCohortRegistry(tiny);
        byte[] cohort = filled((byte) 0x53);
        ShuffleEndpoint endpoint = endpoint(TARGET, 1, cohort, 1);

        assertThrows(RuntimeException.class, () -> rejected.prepare(
                "assignment", FENCE, Collections.singletonList(endpoint)));
        assertEquals(0, rejected.cohortCount());
        assertEquals(0L, tiny.snapshot().usedBytes());

        ControlResidentBudget budget = ControlResidentBudget.forTesting(charge * 2L);
        OutboundShuffleCohortRegistry registry =
                new OutboundShuffleCohortRegistry(budget);
        registry.prepare("assignment", FENCE, Collections.singletonList(endpoint));
        ShuffleEndpoint conflict = endpoint(new UUID(43L, 48L), 1, cohort, 1);
        assertThrows(IllegalStateException.class, () -> registry.prepare(
                "conflict", FENCE, Collections.singletonList(conflict)));
        assertEquals(charge, budget.snapshot().usedBytes());
        registry.close();
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void closeIsIdempotentAndRejectsNewAdmission() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(1L << 20);
        OutboundShuffleCohortRegistry registry =
                new OutboundShuffleCohortRegistry(budget);
        ShuffleEndpoint endpoint = endpoint(TARGET, 1, filled((byte) 0x54), 1);
        registry.prepare("assignment", FENCE, Collections.singletonList(endpoint));

        registry.close();
        registry.close();
        assertEquals(0L, budget.snapshot().usedBytes());
        assertTrue(registry.isClosed());
        assertThrows(IllegalStateException.class, () -> registry.prepare(
                "late", FENCE, Collections.singletonList(endpoint)));
    }

    @Test
    void oneSixtyFourAndMaximumProducerCohortsHaveExactBoundedCharge() {
        int[] counts = {1, 64, ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET};
        long maximum = OutboundShuffleCohortRegistry.residentCharge(
                ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(maximum);
        OutboundShuffleCohortRegistry registry =
                new OutboundShuffleCohortRegistry(budget);

        for (int count : counts) {
            byte[] cohort = filled((byte) (count & 0x7f));
            ShuffleEndpoint endpoint = endpoint(TARGET, 1, cohort, count);
            registry.prepare("assignment-" + count, FENCE,
                    Collections.singletonList(endpoint));
            assertEquals(OutboundShuffleCohortRegistry.residentCharge(count),
                    budget.snapshot().usedBytes());
            registry.cancelAttempt(FENCE);
            assertEquals(0L, budget.snapshot().usedBytes());
        }
        registry.close();
    }

    private static ShuffleEndpoint endpoint(UUID target, int ordinal,
                                            byte[] cohort, int expected) {
        return ShuffleEndpoint.muxAuthorized("127.0.0.1", 7401, target,
                identity("destination", (byte) 7), grant((byte) (10 + ordinal)),
                ordinal, cohort, expected);
    }

    private static ShuffleProducerGrant grant(byte fill) {
        return ShuffleProducerGrant.fromBytes(filled(fill));
    }

    private static WorkerDataIdentity identity(String worker, byte fill) {
        return WorkerDataIdentity.authenticated(worker, "inc-" + worker,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", filled(fill));
    }

    private static byte[] filled(byte fill) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, fill);
        return bytes;
    }
}
