package io.rowweave.upgrade;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Complete, immutable authority state for one store upgrade generation. */
public final class UpgradeAuthorityRecord {
    public static final int VERSION_1 = 1;
    public static final int MAX_COMMAND_EVIDENCE = 16;

    private final int recordVersion;
    private final StoreFormatHeader activeHeader;
    private final StoreFormatHeader sourceHeader;
    private final StoreFormatHeader targetHeader;
    private final long generation;
    private final UpgradePhase phase;
    private final String sourceNamespace;
    private final String sourceDigest;
    private final String shadowNamespace;
    private final String shadowDigest;
    private final UpgradeOwner owner;
    private final long expectedActivePointerVersion;
    private final String activeNamespace;
    private final long activePointerVersion;
    private final long writerEpoch;
    private final long verifiedObjectCount;
    private final int verifiedShardCount;
    private final String verifiedSourceDigest;
    private final String verifiedTargetDigest;
    private final long rollbackDeadlineEpochMillis;
    private final boolean writesAllowed;
    private final boolean newSemanticsObserved;
    private final List<UpgradeCommandEvidence> commandEvidence;

    public UpgradeAuthorityRecord(
            int recordVersion, StoreFormatHeader activeHeader,
            StoreFormatHeader targetHeader, long generation, UpgradePhase phase,
            String sourceNamespace, String sourceDigest,
            String shadowNamespace, String shadowDigest, UpgradeOwner owner,
            long expectedActivePointerVersion, String activeNamespace,
            long activePointerVersion, long writerEpoch,
            long verifiedObjectCount, int verifiedShardCount,
            String verifiedSourceDigest, String verifiedTargetDigest,
            long rollbackDeadlineEpochMillis, boolean writesAllowed,
            boolean newSemanticsObserved,
            List<UpgradeCommandEvidence> commandEvidence) {
        this(recordVersion, activeHeader, activeHeader, targetHeader, generation,
                phase, sourceNamespace, sourceDigest, shadowNamespace,
                shadowDigest, owner, expectedActivePointerVersion,
                activeNamespace, activePointerVersion, writerEpoch,
                verifiedObjectCount, verifiedShardCount, verifiedSourceDigest,
                verifiedTargetDigest, rollbackDeadlineEpochMillis,
                writesAllowed, newSemanticsObserved, commandEvidence);
    }

    UpgradeAuthorityRecord(
            int recordVersion, StoreFormatHeader activeHeader,
            StoreFormatHeader sourceHeader, StoreFormatHeader targetHeader,
            long generation, UpgradePhase phase, String sourceNamespace,
            String sourceDigest, String shadowNamespace, String shadowDigest,
            UpgradeOwner owner, long expectedActivePointerVersion,
            String activeNamespace, long activePointerVersion, long writerEpoch,
            long verifiedObjectCount, int verifiedShardCount,
            String verifiedSourceDigest, String verifiedTargetDigest,
            long rollbackDeadlineEpochMillis, boolean writesAllowed,
            boolean newSemanticsObserved,
            List<UpgradeCommandEvidence> commandEvidence) {
        if (recordVersion != VERSION_1) {
            throw new IllegalArgumentException("unsupported authority record version");
        }
        if (activeHeader == null || sourceHeader == null || targetHeader == null || phase == null
                || owner == null || commandEvidence == null) {
            throw new IllegalArgumentException("authority fields are required");
        }
        if (!activeHeader.storeId().equals(sourceHeader.storeId())
                || !activeHeader.storeId().equals(targetHeader.storeId())
                || activeHeader.protocolEpoch() != sourceHeader.protocolEpoch()
                || activeHeader.protocolEpoch() != targetHeader.protocolEpoch()) {
            throw new IllegalArgumentException("store header identities differ");
        }
        if (generation <= 0L || expectedActivePointerVersion < 0L
                || activePointerVersion < 0L || writerEpoch <= 0L
                || verifiedObjectCount < 0L || verifiedShardCount < 0
                || rollbackDeadlineEpochMillis < 0L) {
            throw new IllegalArgumentException("authority counters are invalid");
        }
        UpgradeCodecSupport.requireText(sourceNamespace, "source namespace");
        UpgradeCodecSupport.requireDigest(sourceDigest, "source digest");
        UpgradeCodecSupport.requireText(shadowNamespace, "shadow namespace");
        UpgradeCodecSupport.requireDigest(shadowDigest, "shadow digest");
        UpgradeCodecSupport.requireText(activeNamespace, "active namespace");
        UpgradeCodecSupport.requireDigest(
                verifiedSourceDigest, "verified source digest");
        UpgradeCodecSupport.requireDigest(
                verifiedTargetDigest, "verified target digest");
        if (commandEvidence.size() > MAX_COMMAND_EVIDENCE) {
            throw new IllegalArgumentException("too much command evidence");
        }
        ArrayList<UpgradeCommandEvidence> evidence =
                new ArrayList<UpgradeCommandEvidence>(commandEvidence.size());
        for (UpgradeCommandEvidence item : commandEvidence) {
            if (item == null) throw new IllegalArgumentException("null command evidence");
            for (UpgradeCommandEvidence prior : evidence) {
                if (prior.commandId().equals(item.commandId())) {
                    throw new IllegalArgumentException("duplicate command evidence id");
                }
            }
            evidence.add(item);
        }
        validateShape(activeHeader, sourceHeader, targetHeader, phase,
                sourceNamespace, shadowNamespace, activeNamespace,
                activePointerVersion, expectedActivePointerVersion,
                verifiedSourceDigest, verifiedTargetDigest, writesAllowed,
                newSemanticsObserved);
        this.recordVersion = recordVersion;
        this.activeHeader = activeHeader;
        this.sourceHeader = sourceHeader;
        this.targetHeader = targetHeader;
        this.generation = generation;
        this.phase = phase;
        this.sourceNamespace = sourceNamespace;
        this.sourceDigest = sourceDigest;
        this.shadowNamespace = shadowNamespace;
        this.shadowDigest = shadowDigest;
        this.owner = owner;
        this.expectedActivePointerVersion = expectedActivePointerVersion;
        this.activeNamespace = activeNamespace;
        this.activePointerVersion = activePointerVersion;
        this.writerEpoch = writerEpoch;
        this.verifiedObjectCount = verifiedObjectCount;
        this.verifiedShardCount = verifiedShardCount;
        this.verifiedSourceDigest = verifiedSourceDigest;
        this.verifiedTargetDigest = verifiedTargetDigest;
        this.rollbackDeadlineEpochMillis = rollbackDeadlineEpochMillis;
        this.writesAllowed = writesAllowed;
        this.newSemanticsObserved = newSemanticsObserved;
        this.commandEvidence = Collections.unmodifiableList(evidence);
    }

    private static void validateShape(
            StoreFormatHeader activeHeader, StoreFormatHeader sourceHeader,
            StoreFormatHeader targetHeader, UpgradePhase phase,
            String sourceNamespace, String shadowNamespace,
            String activeNamespace, long activePointerVersion,
            long expectedPointerVersion, String verifiedSourceDigest,
            String verifiedTargetDigest, boolean writesAllowed,
            boolean newSemanticsObserved) {
        boolean sourceActive = activeHeader.equals(sourceHeader)
                && activeNamespace.equals(sourceNamespace);
        boolean targetActive = activeHeader.equals(targetHeader)
                && activeNamespace.equals(shadowNamespace);
        if (activePointerVersion < expectedPointerVersion) {
            throw new IllegalArgumentException("active pointer version regressed");
        }
        switch (phase) {
            case IDLE:
                if (!sourceActive || newSemanticsObserved) invalidPhase();
                break;
            case QUIESCED:
            case COPYING:
                if (!sourceActive || writesAllowed || newSemanticsObserved) {
                    invalidPhase();
                }
                break;
            case VERIFIED:
                if (!sourceActive || writesAllowed || newSemanticsObserved
                        || !verifiedSourceDigest.equals(verifiedTargetDigest)) {
                    invalidPhase();
                }
                break;
            case CUTOVER:
            case CLEANUP_ELIGIBLE:
                if (!targetActive
                        || !verifiedSourceDigest.equals(verifiedTargetDigest)) {
                    invalidPhase();
                }
                break;
            case FAILED_FROZEN:
                if (writesAllowed || (!sourceActive && !targetActive)
                        || (newSemanticsObserved && !targetActive)) {
                    invalidPhase();
                }
                break;
            default:
                invalidPhase();
        }
    }

    private static void invalidPhase() {
        throw new IllegalArgumentException("authority phase shape is invalid");
    }

    public int recordVersion() { return recordVersion; }
    public StoreFormatHeader activeHeader() { return activeHeader; }
    public StoreFormatHeader sourceHeader() { return sourceHeader; }
    public StoreFormatHeader targetHeader() { return targetHeader; }
    public long generation() { return generation; }
    public UpgradePhase phase() { return phase; }
    public String sourceNamespace() { return sourceNamespace; }
    public String sourceDigest() { return sourceDigest; }
    public String shadowNamespace() { return shadowNamespace; }
    public String shadowDigest() { return shadowDigest; }
    public UpgradeOwner owner() { return owner; }
    public long expectedActivePointerVersion() { return expectedActivePointerVersion; }
    public String activeNamespace() { return activeNamespace; }
    public long activePointerVersion() { return activePointerVersion; }
    public long writerEpoch() { return writerEpoch; }
    public long verifiedObjectCount() { return verifiedObjectCount; }
    public int verifiedShardCount() { return verifiedShardCount; }
    public String verifiedSourceDigest() { return verifiedSourceDigest; }
    public String verifiedTargetDigest() { return verifiedTargetDigest; }
    public long rollbackDeadlineEpochMillis() { return rollbackDeadlineEpochMillis; }
    public boolean writesAllowed() { return writesAllowed; }
    public boolean newSemanticsObserved() { return newSemanticsObserved; }
    public List<UpgradeCommandEvidence> commandEvidence() { return commandEvidence; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof UpgradeAuthorityRecord)) return false;
        UpgradeAuthorityRecord that = (UpgradeAuthorityRecord) other;
        return recordVersion == that.recordVersion && generation == that.generation
                && expectedActivePointerVersion == that.expectedActivePointerVersion
                && activePointerVersion == that.activePointerVersion
                && writerEpoch == that.writerEpoch
                && verifiedObjectCount == that.verifiedObjectCount
                && verifiedShardCount == that.verifiedShardCount
                && rollbackDeadlineEpochMillis == that.rollbackDeadlineEpochMillis
                && writesAllowed == that.writesAllowed
                && newSemanticsObserved == that.newSemanticsObserved
                && activeHeader.equals(that.activeHeader)
                && sourceHeader.equals(that.sourceHeader)
                && targetHeader.equals(that.targetHeader) && phase == that.phase
                && sourceNamespace.equals(that.sourceNamespace)
                && sourceDigest.equals(that.sourceDigest)
                && shadowNamespace.equals(that.shadowNamespace)
                && shadowDigest.equals(that.shadowDigest) && owner.equals(that.owner)
                && activeNamespace.equals(that.activeNamespace)
                && verifiedSourceDigest.equals(that.verifiedSourceDigest)
                && verifiedTargetDigest.equals(that.verifiedTargetDigest)
                && commandEvidence.equals(that.commandEvidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordVersion, activeHeader, sourceHeader,
                targetHeader, generation,
                phase, sourceNamespace, sourceDigest, shadowNamespace, shadowDigest,
                owner, expectedActivePointerVersion, activeNamespace,
                activePointerVersion, writerEpoch, verifiedObjectCount,
                verifiedShardCount, verifiedSourceDigest, verifiedTargetDigest,
                rollbackDeadlineEpochMillis, writesAllowed, newSemanticsObserved,
                commandEvidence);
    }
}
