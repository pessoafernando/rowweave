package io.rowweave.upgrade;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpgradeAuthorityKernelTest {
    private static final String ZERO = repeat('0');
    private static final String SOURCE = repeat('a');
    private static final String TARGET = repeat('b');
    private static final UpgradeOwner OWNER =
            new UpgradeOwner(7L, "master-a", "inc-a", 11L);

    @Test
    void quiesceWaitsForBothRunAndIntervalDrainBeforeFreezingWrites() {
        UpgradeAuthorityRecord idle = idle();
        UpgradeTransition frozen = UpgradeAuthorityKernel.apply(idle,
                command(UpgradeCommand.Kind.FREEZE_ADMISSION, "freeze", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 1L, 1L, 1_000L);
        assertEquals(UpgradeTransition.Code.ACCEPTED, frozen.code());
        assertEquals(UpgradePhase.IDLE, frozen.record().phase());
        assertFalse(frozen.record().writesAllowed());
        assertFalse(UpgradeWriteAdmission.allowed(frozen.record(),
                UpgradeWriteAdmission.Action.NEW_SUBMISSION));
        assertFalse(UpgradeWriteAdmission.allowed(frozen.record(),
                UpgradeWriteAdmission.Action.NEW_CLAIM));
        assertFalse(UpgradeWriteAdmission.allowed(frozen.record(),
                UpgradeWriteAdmission.Action.NEW_ASSIGNMENT));
        assertTrue(UpgradeWriteAdmission.allowed(frozen.record(),
                UpgradeWriteAdmission.Action.RECONCILIATION));
        UpgradeCommand command = command(UpgradeCommand.Kind.QUIESCE,
                "quiesce-1", OWNER, 3L, SOURCE, TARGET, 2_000L);

        UpgradeTransition activeRun = UpgradeAuthorityKernel.apply(
                frozen.record(), command, 1L, 0L, 1_000L);
        UpgradeTransition activeInterval = UpgradeAuthorityKernel.apply(
                frozen.record(), command, 0L, 1L, 1_000L);
        assertEquals(UpgradeTransition.Code.ACTIVE_WORK_REMAINS,
                activeRun.code());
        assertSame(frozen.record(), activeRun.record());
        assertEquals(UpgradeTransition.Code.ACTIVE_WORK_REMAINS,
                activeInterval.code());
        assertSame(frozen.record(), activeInterval.record());

        UpgradeTransition drained = UpgradeAuthorityKernel.apply(
                frozen.record(), command, 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.ACCEPTED, drained.code());
        assertEquals(UpgradePhase.QUIESCED, drained.record().phase());
        assertFalse(drained.record().writesAllowed());
    }

    @Test
    void verifiedShadowCutoverIsAtomicReplaySafeAndFenced() {
        UpgradeAuthorityRecord quiesced = accepted(frozenIdle(),
                command(UpgradeCommand.Kind.QUIESCE, "q", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 1_000L);
        UpgradeAuthorityRecord copying = accepted(quiesced,
                command(UpgradeCommand.Kind.BEGIN_COPY, "c", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 1_000L);

        UpgradeTransition drift = UpgradeAuthorityKernel.apply(copying,
                command(UpgradeCommand.Kind.VERIFY, "v-drift", OWNER,
                        3L, SOURCE, repeat('c'), 2_000L), 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.DIGEST_DRIFT, drift.code());
        assertSame(copying, drift.record());

        UpgradeAuthorityRecord verified = accepted(copying,
                command(UpgradeCommand.Kind.VERIFY, "v", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 1_000L);
        assertEquals(UpgradePhase.VERIFIED, verified.phase());
        assertEquals(15L, verified.verifiedObjectCount());

        UpgradeCommand cutover = command(UpgradeCommand.Kind.CUTOVER,
                "cut", OWNER, 3L, SOURCE, TARGET, 2_000L);
        UpgradeTransition first = UpgradeAuthorityKernel.apply(
                verified, cutover, 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.ACCEPTED, first.code());
        assertEquals(UpgradePhase.CUTOVER, first.record().phase());
        assertEquals("shadow-4", first.record().activeNamespace());
        assertEquals(4L, first.record().activePointerVersion());
        assertEquals(10L, first.record().writerEpoch());
        assertEquals(2, first.record().activeHeader().activeRevision());
        assertFalse(first.record().writesAllowed());

        UpgradeTransition ackLossReplay = UpgradeAuthorityKernel.apply(
                first.record(), cutover, 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.REPLAYED, ackLossReplay.code());
        assertSame(first.record(), ackLossReplay.record());

        UpgradeTransition staleOwner = UpgradeAuthorityKernel.apply(
                verified, command(UpgradeCommand.Kind.CUTOVER, "stale",
                        new UpgradeOwner(6L, "master-z", "inc-z", 10L),
                        3L, SOURCE, TARGET, 2_000L), 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.STALE_OWNER, staleOwner.code());
        assertSame(verified, staleOwner.record());

        UpgradeTransition aba = UpgradeAuthorityKernel.apply(
                first.record(), command(UpgradeCommand.Kind.RESUME_WRITES,
                        "resume-old-pointer", OWNER, 3L, SOURCE, TARGET,
                        2_000L), 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.POINTER_VERSION_MISMATCH, aba.code());
        assertSame(first.record(), aba.record());
    }

    @Test
    void rollbackIsAllowedOnlyBeforeNewWritesAndCleanupWaitsForDeadline() {
        UpgradeAuthorityRecord cutover = cutover();
        UpgradeCommand rollback = commandAtPointer(
                UpgradeCommand.Kind.DATA_ROLLBACK, "rollback", 4L, 2_000L);
        UpgradeTransition rolledBack = UpgradeAuthorityKernel.apply(
                cutover, rollback, 0L, 0L, 1_500L);
        assertEquals(UpgradeTransition.Code.ACCEPTED, rolledBack.code());
        assertEquals(UpgradePhase.QUIESCED, rolledBack.record().phase());
        assertEquals("legacy", rolledBack.record().activeNamespace());
        assertEquals(5L, rolledBack.record().activePointerVersion());
        assertEquals(11L, rolledBack.record().writerEpoch());
        assertEquals(0, rolledBack.record().activeHeader().activeRevision());

        UpgradeAuthorityRecord resumed = accepted(cutover,
                commandAtPointer(UpgradeCommand.Kind.RESUME_WRITES,
                        "resume", 4L, 2_000L), 1_500L);
        assertTrue(resumed.writesAllowed());
        UpgradeTransition forbidden = UpgradeAuthorityKernel.apply(resumed,
                commandAtPointer(UpgradeCommand.Kind.DATA_ROLLBACK,
                        "late-rollback", 4L, 2_000L), 0L, 0L, 1_500L);
        assertEquals(UpgradeTransition.Code.ROLLBACK_FORBIDDEN, forbidden.code());
        assertSame(resumed, forbidden.record());

        UpgradeTransition earlyCleanup = UpgradeAuthorityKernel.apply(resumed,
                commandAtPointer(UpgradeCommand.Kind.MARK_CLEANUP_ELIGIBLE,
                        "cleanup", 4L, 2_000L), 0L, 0L, 1_999L);
        assertEquals(UpgradeTransition.Code.DEADLINE_NOT_REACHED,
                earlyCleanup.code());
        UpgradeTransition cleanup = UpgradeAuthorityKernel.apply(resumed,
                commandAtPointer(UpgradeCommand.Kind.MARK_CLEANUP_ELIGIBLE,
                        "cleanup", 4L, 2_000L), 0L, 0L, 2_000L);
        assertEquals(UpgradeTransition.Code.ACCEPTED, cleanup.code());
        assertEquals(UpgradePhase.CLEANUP_ELIGIBLE, cleanup.record().phase());
    }

    @Test
    void sameCommandIdWithDifferentBindingCannotMutateAuthority() {
        UpgradeAuthorityRecord quiesced = accepted(frozenIdle(),
                command(UpgradeCommand.Kind.QUIESCE, "same", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 1_000L);
        UpgradeTransition conflict = UpgradeAuthorityKernel.apply(quiesced,
                command(UpgradeCommand.Kind.BEGIN_COPY, "same", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 0L, 0L, 1_000L);
        assertEquals(UpgradeTransition.Code.COMMAND_ID_CONFLICT,
                conflict.code());
        assertSame(quiesced, conflict.record());
    }

    private static UpgradeAuthorityRecord cutover() {
        UpgradeAuthorityRecord record = accepted(idle(), command(
                UpgradeCommand.Kind.FREEZE_ADMISSION, "freeze2", OWNER,
                3L, SOURCE, TARGET, 2_000L), 1_000L);
        record = accepted(record,
                command(UpgradeCommand.Kind.QUIESCE, "q2", OWNER,
                        3L, SOURCE, TARGET, 2_000L), 1_000L);
        record = accepted(record, command(UpgradeCommand.Kind.BEGIN_COPY,
                "c2", OWNER, 3L, SOURCE, TARGET, 2_000L), 1_000L);
        record = accepted(record, command(UpgradeCommand.Kind.VERIFY,
                "v2", OWNER, 3L, SOURCE, TARGET, 2_000L), 1_000L);
        return accepted(record, command(UpgradeCommand.Kind.CUTOVER,
                "cut2", OWNER, 3L, SOURCE, TARGET, 2_000L), 1_000L);
    }

    private static UpgradeAuthorityRecord accepted(
            UpgradeAuthorityRecord current, UpgradeCommand command, long now) {
        UpgradeTransition transition = UpgradeAuthorityKernel.apply(
                current, command, 0L, 0L, now);
        assertEquals(UpgradeTransition.Code.ACCEPTED, transition.code());
        return transition.record();
    }

    private static UpgradeAuthorityRecord frozenIdle() {
        return accepted(idle(), command(UpgradeCommand.Kind.FREEZE_ADMISSION,
                "freeze-helper", OWNER, 3L, SOURCE, TARGET, 2_000L), 1_000L);
    }

    private static UpgradeAuthorityRecord idle() {
        return new UpgradeAuthorityRecord(1,
                new StoreFormatHeader(1, "store", 1, 0, ZERO),
                new StoreFormatHeader(1, "store", 1, 2, ZERO),
                4L, UpgradePhase.IDLE, "legacy", SOURCE,
                "shadow-4", TARGET, OWNER, 3L, "legacy", 3L, 9L,
                0L, 0, ZERO, ZERO, 0L, true, false,
                Collections.<UpgradeCommandEvidence>emptyList());
    }

    private static UpgradeCommand command(
            UpgradeCommand.Kind kind, String id, UpgradeOwner owner,
            long pointerVersion, String sourceDigest, String targetDigest,
            long deadline) {
        return new UpgradeCommand(kind, id, "release-2", "store", 4L,
                "legacy", sourceDigest, "shadow-4", targetDigest, owner,
                pointerVersion, 15L, 3, repeat('e'), repeat('e'), deadline);
    }

    private static UpgradeCommand commandAtPointer(
            UpgradeCommand.Kind kind, String id, long pointerVersion,
            long deadline) {
        return command(kind, id, OWNER, pointerVersion, SOURCE, TARGET, deadline);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
