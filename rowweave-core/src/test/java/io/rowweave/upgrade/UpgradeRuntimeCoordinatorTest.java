package io.rowweave.upgrade;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpgradeRuntimeCoordinatorTest {
    @Test
    void freezeAndDrainShareOneLinearizationLockWithRuntimeAdmission() {
        InMemoryAuthority authority = new InMemoryAuthority(idle());
        UpgradeRuntimeCoordinator coordinator =
                new UpgradeRuntimeCoordinator(authority);
        UpgradeRuntimeCoordinator.Lease run = coordinator.beginRun();
        UpgradeRuntimeCoordinator.Lease interval = coordinator.beginInterval();

        assertEquals(UpgradeTransition.Code.ACCEPTED,
                coordinator.apply(command(UpgradeCommand.Kind.FREEZE_ADMISSION,
                        "freeze"), 1_000L).code());
        assertThrows(IllegalStateException.class, coordinator::beginRun);
        assertThrows(IllegalStateException.class, coordinator::beginInterval);
        assertThrows(IllegalStateException.class,
                coordinator::requireNewClaimAllowed);
        assertThrows(IllegalStateException.class,
                coordinator::requireNewAssignmentAllowed);
        coordinator.requireReconciliationAllowed();
        assertEquals(UpgradeTransition.Code.ACTIVE_WORK_REMAINS,
                coordinator.apply(command(UpgradeCommand.Kind.QUIESCE, "q"),
                        1_000L).code());

        run.close();
        assertEquals(UpgradeTransition.Code.ACTIVE_WORK_REMAINS,
                coordinator.apply(command(UpgradeCommand.Kind.QUIESCE, "q"),
                        1_000L).code());
        interval.close();
        assertEquals(UpgradeTransition.Code.ACCEPTED,
                coordinator.apply(command(UpgradeCommand.Kind.QUIESCE, "q"),
                        1_000L).code());
        assertEquals(0L, coordinator.activeRunCount());
        assertEquals(0L, coordinator.activeIntervalCount());
        run.close();
        interval.close();
        assertEquals(0L, coordinator.activeRunCount());
    }

    @Test
    void quiesceIncludesDurableWorkRediscoveredAfterLeaderFailover() {
        InMemoryAuthority authority = new InMemoryAuthority(idle());
        AtomicBoolean durableRunActive = new AtomicBoolean(true);
        UpgradeRuntimeCoordinator coordinator = new UpgradeRuntimeCoordinator(
                authority, () -> new UpgradeActiveWorkSnapshot(
                durableRunActive.get() ? 1L : 0L, 0L));

        assertEquals(UpgradeTransition.Code.ACCEPTED,
                coordinator.apply(command(UpgradeCommand.Kind.FREEZE_ADMISSION,
                        "freeze-failover"), 1_000L).code());
        assertEquals(UpgradeTransition.Code.ACTIVE_WORK_REMAINS,
                coordinator.apply(command(UpgradeCommand.Kind.QUIESCE,
                        "q-failover"), 1_000L).code());

        durableRunActive.set(false);
        assertEquals(UpgradeTransition.Code.ACCEPTED,
                coordinator.apply(command(UpgradeCommand.Kind.QUIESCE,
                        "q-failover"), 1_000L).code());
    }

    private static UpgradeAuthorityRecord idle() {
        String zero = digest('0');
        StoreFormatHeader source = new StoreFormatHeader(
                1, "store", 1, 0, zero);
        return new UpgradeAuthorityRecord(1, source,
                new StoreFormatHeader(1, "store", 1, 2, digest('b')),
                1L, UpgradePhase.IDLE, "legacy", digest('a'),
                "shadow", digest('c'),
                new UpgradeOwner(1L, "master", "inc", 1L),
                0L, "legacy", 0L, 1L, 0L, 0, zero, zero,
                2_000L, true, false,
                Collections.<UpgradeCommandEvidence>emptyList());
    }

    private static UpgradeCommand command(
            UpgradeCommand.Kind kind, String commandId) {
        return new UpgradeCommand(kind, commandId, "release", "store", 1L,
                "legacy", digest('a'), "shadow", digest('c'),
                new UpgradeOwner(1L, "master", "inc", 1L),
                0L, 0L, 0, digest('e'), digest('e'), 2_000L);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class InMemoryAuthority
            implements UpgradeAuthorityStore {
        private UpgradeAuthorityRecord record;

        private InMemoryAuthority(UpgradeAuthorityRecord record) {
            this.record = record;
        }

        @Override
        public UpgradeAuthorityRecord read() { return record; }

        @Override
        public UpgradeTransition apply(
                UpgradeCommand command, long activeRuns,
                long activeIntervals, long now) {
            UpgradeTransition result = UpgradeAuthorityKernel.apply(
                    record, command, activeRuns, activeIntervals, now);
            if (result.code() == UpgradeTransition.Code.ACCEPTED) {
                record = result.record();
            }
            return result;
        }
    }
}
