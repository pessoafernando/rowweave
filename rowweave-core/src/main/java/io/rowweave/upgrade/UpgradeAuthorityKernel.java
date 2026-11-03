package io.rowweave.upgrade;

import java.util.ArrayList;
import java.util.List;

/** Backend-neutral state machine for File and ZooKeeper upgrade authorities. */
public final class UpgradeAuthorityKernel {
    private UpgradeAuthorityKernel() { }

    public static UpgradeTransition apply(
            UpgradeAuthorityRecord current, UpgradeCommand command,
            long activeRunCount, long activeIntervalCount,
            long authorityTimeMillis) {
        if (current == null || command == null) {
            throw new IllegalArgumentException("current record and command are required");
        }
        if (activeRunCount < 0L || activeIntervalCount < 0L
                || authorityTimeMillis < 0L) {
            throw new IllegalArgumentException("authority observations are invalid");
        }
        UpgradeTransition replay = replay(current, command);
        if (replay != null) return replay;
        UpgradeTransition binding = validateBinding(current, command);
        if (binding != null) return binding;
        if (current.commandEvidence().size()
                == UpgradeAuthorityRecord.MAX_COMMAND_EVIDENCE) {
            return rejected(UpgradeTransition.Code.EVIDENCE_EXHAUSTED, current);
        }
        return transition(current, command, activeRunCount,
                activeIntervalCount, authorityTimeMillis);
    }

    private static UpgradeTransition transition(
            UpgradeAuthorityRecord current, UpgradeCommand command,
            long activeRunCount, long activeIntervalCount, long now) {
        switch (command.kind()) {
            case FREEZE_ADMISSION:
                if (current.phase() != UpgradePhase.IDLE
                        || !current.writesAllowed()) return wrongPhase(current);
                return copy(current, command, UpgradePhase.IDLE,
                        current.activeHeader(), current.activeNamespace(),
                        current.activePointerVersion(), current.writerEpoch(), false,
                        current.newSemanticsObserved());
            case QUIESCE:
                if (current.phase() != UpgradePhase.IDLE
                        || current.writesAllowed()) return wrongPhase(current);
                if (activeRunCount != 0L || activeIntervalCount != 0L) {
                    return rejected(UpgradeTransition.Code.ACTIVE_WORK_REMAINS, current);
                }
                return accepted(current, command, UpgradePhase.QUIESCED,
                        current.activeHeader(), current.activeNamespace(),
                        current.activePointerVersion(), current.writerEpoch(),
                        0L, 0, zeroDigest(), zeroDigest(),
                        command.rollbackDeadlineEpochMillis(), false,
                        current.newSemanticsObserved());
            case BEGIN_COPY:
                if (current.phase() != UpgradePhase.QUIESCED
                        || current.writesAllowed()) return wrongPhase(current);
                return retained(current, command, UpgradePhase.COPYING);
            case VERIFY:
                if (current.phase() != UpgradePhase.COPYING) return wrongPhase(current);
                if (!command.verifiedSourceDigest().equals(
                        command.verifiedTargetDigest())) {
                    return rejected(UpgradeTransition.Code.DIGEST_DRIFT, current);
                }
                return accepted(current, command, UpgradePhase.VERIFIED,
                        current.activeHeader(), current.activeNamespace(),
                        current.activePointerVersion(), current.writerEpoch(),
                        command.verifiedObjectCount(), command.verifiedShardCount(),
                        command.verifiedSourceDigest(),
                        command.verifiedTargetDigest(),
                        current.rollbackDeadlineEpochMillis(), false,
                        current.newSemanticsObserved());
            case CUTOVER:
                if (current.phase() != UpgradePhase.VERIFIED
                        || current.writesAllowed()) return wrongPhase(current);
                return pointerChange(current, command, UpgradePhase.CUTOVER,
                        current.targetHeader(), current.shadowNamespace());
            case RESUME_WRITES:
                if (current.phase() != UpgradePhase.CUTOVER
                        || current.writesAllowed()) return wrongPhase(current);
                return copy(current, command, UpgradePhase.CUTOVER,
                        current.activeHeader(), current.activeNamespace(),
                        current.activePointerVersion(), current.writerEpoch(),
                        true, current.newSemanticsObserved());
            case MARK_NEW_SEMANTICS:
                if (current.phase() != UpgradePhase.CUTOVER
                        || !current.writesAllowed()) return wrongPhase(current);
                return copy(current, command, UpgradePhase.CUTOVER,
                        current.activeHeader(), current.activeNamespace(),
                        current.activePointerVersion(), current.writerEpoch(), true, true);
            case DATA_ROLLBACK:
                if (current.phase() != UpgradePhase.CUTOVER
                        || current.writesAllowed() || current.newSemanticsObserved()
                        || now > current.rollbackDeadlineEpochMillis()) {
                    return rejected(UpgradeTransition.Code.ROLLBACK_FORBIDDEN, current);
                }
                return pointerChange(current, command, UpgradePhase.QUIESCED,
                        current.sourceHeader(), current.sourceNamespace());
            case MARK_CLEANUP_ELIGIBLE:
                if (current.phase() != UpgradePhase.CUTOVER) return wrongPhase(current);
                if (now < current.rollbackDeadlineEpochMillis()) {
                    return rejected(UpgradeTransition.Code.DEADLINE_NOT_REACHED, current);
                }
                return retained(current, command, UpgradePhase.CLEANUP_ELIGIBLE);
            case FREEZE_FAILURE:
                if (current.phase() == UpgradePhase.CLEANUP_ELIGIBLE) {
                    return wrongPhase(current);
                }
                return copy(current, command, UpgradePhase.FAILED_FROZEN,
                        current.activeHeader(), current.activeNamespace(),
                        current.activePointerVersion(), current.writerEpoch(), false,
                        current.newSemanticsObserved());
            default:
                throw new IllegalArgumentException("unsupported upgrade command");
        }
    }

    private static UpgradeTransition replay(
            UpgradeAuthorityRecord current, UpgradeCommand command) {
        for (UpgradeCommandEvidence evidence : current.commandEvidence()) {
            if (evidence.commandId().equals(command.commandId())) {
                UpgradeTransition.Code code = evidence.commandDigest().equals(
                        command.commandDigest())
                        ? UpgradeTransition.Code.REPLAYED
                        : UpgradeTransition.Code.COMMAND_ID_CONFLICT;
                return rejected(code, current);
            }
        }
        return null;
    }

    private static UpgradeTransition validateBinding(
            UpgradeAuthorityRecord current, UpgradeCommand command) {
        if (!current.activeHeader().storeId().equals(command.storeId())
                || current.generation() != command.generation()
                || !current.sourceNamespace().equals(command.sourceNamespace())
                || !current.shadowNamespace().equals(command.targetNamespace())) {
            return rejected(UpgradeTransition.Code.BINDING_MISMATCH, current);
        }
        if (!current.owner().equals(command.owner())) {
            return rejected(UpgradeTransition.Code.STALE_OWNER, current);
        }
        if (!current.sourceDigest().equals(command.sourceDigest())
                || !current.shadowDigest().equals(command.targetDigest())) {
            return rejected(UpgradeTransition.Code.DIGEST_DRIFT, current);
        }
        if (current.activePointerVersion()
                != command.expectedActivePointerVersion()) {
            return rejected(
                    UpgradeTransition.Code.POINTER_VERSION_MISMATCH, current);
        }
        if (current.phase() != UpgradePhase.IDLE
                && current.rollbackDeadlineEpochMillis()
                != command.rollbackDeadlineEpochMillis()) {
            return rejected(UpgradeTransition.Code.BINDING_MISMATCH, current);
        }
        return null;
    }

    private static UpgradeTransition retained(
            UpgradeAuthorityRecord current, UpgradeCommand command,
            UpgradePhase phase) {
        return copy(current, command, phase, current.activeHeader(),
                current.activeNamespace(), current.activePointerVersion(),
                current.writerEpoch(), current.writesAllowed(),
                current.newSemanticsObserved());
    }

    private static UpgradeTransition pointerChange(
            UpgradeAuthorityRecord current, UpgradeCommand command,
            UpgradePhase phase, StoreFormatHeader activeHeader,
            String activeNamespace) {
        try {
            return copy(current, command, phase, activeHeader, activeNamespace,
                    Math.addExact(current.activePointerVersion(), 1L),
                    Math.addExact(current.writerEpoch(), 1L), false,
                    current.newSemanticsObserved());
        } catch (ArithmeticException exhausted) {
            return rejected(UpgradeTransition.Code.EVIDENCE_EXHAUSTED, current);
        }
    }

    private static UpgradeTransition copy(
            UpgradeAuthorityRecord current, UpgradeCommand command,
            UpgradePhase phase, StoreFormatHeader activeHeader,
            String activeNamespace, long activePointerVersion, long writerEpoch,
            boolean writesAllowed, boolean newSemanticsObserved) {
        return accepted(current, command, phase, activeHeader, activeNamespace,
                activePointerVersion, writerEpoch, current.verifiedObjectCount(),
                current.verifiedShardCount(), current.verifiedSourceDigest(),
                current.verifiedTargetDigest(),
                current.rollbackDeadlineEpochMillis(), writesAllowed,
                newSemanticsObserved);
    }

    private static UpgradeTransition accepted(
            UpgradeAuthorityRecord current, UpgradeCommand command,
            UpgradePhase phase, StoreFormatHeader activeHeader,
            String activeNamespace, long activePointerVersion, long writerEpoch,
            long objectCount, int shardCount, String verifiedSourceDigest,
            String verifiedTargetDigest, long rollbackDeadline,
            boolean writesAllowed, boolean newSemanticsObserved) {
        List<UpgradeCommandEvidence> evidence =
                new ArrayList<UpgradeCommandEvidence>(
                        current.commandEvidence().size() + 1);
        evidence.addAll(current.commandEvidence());
        evidence.add(new UpgradeCommandEvidence(
                command.commandId(), command.commandDigest()));
        UpgradeAuthorityRecord next = new UpgradeAuthorityRecord(
                current.recordVersion(), activeHeader, current.sourceHeader(),
                current.targetHeader(), current.generation(), phase,
                current.sourceNamespace(), current.sourceDigest(),
                current.shadowNamespace(), current.shadowDigest(), current.owner(),
                current.expectedActivePointerVersion(), activeNamespace,
                activePointerVersion, writerEpoch, objectCount, shardCount,
                verifiedSourceDigest, verifiedTargetDigest, rollbackDeadline,
                writesAllowed, newSemanticsObserved, evidence);
        return UpgradeTransition.of(UpgradeTransition.Code.ACCEPTED, next);
    }

    private static UpgradeTransition wrongPhase(UpgradeAuthorityRecord current) {
        return rejected(UpgradeTransition.Code.INVALID_PHASE, current);
    }

    private static UpgradeTransition rejected(
            UpgradeTransition.Code code, UpgradeAuthorityRecord current) {
        return UpgradeTransition.of(code, current);
    }

    private static String zeroDigest() {
        return "0000000000000000000000000000000000000000000000000000000000000000";
    }
}
