package io.rowweave.stream.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** Versioned checkpoint state for the resident snapshot-to-log enumerator. */
public final class StreamCdcEnumeratorState {
    public static final int LEGACY_VERSION = 1;
    public static final int CURRENT_VERSION = 2;
    public static final int MAX_SPLITS = 1024;
    public static final int MAX_EXTERNAL_POSITION_BYTES = 64 * 1024;

    public enum Phase {
        SNAPSHOT(1), LOG(2);

        private final int wireId;

        Phase(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Phase fromWire(int wireId) {
            for (Phase value : values()) {
                if (value.wireId == wireId) return value;
            }
            invalid("unknown resident CDC phase");
            return null;
        }
    }

    public static final class CompletedSplit {
        private final String splitId;
        private final long recordCount;
        private final String contentDigest;

        public CompletedSplit(
                String splitId, long recordCount, String contentDigest) {
            this.splitId = StreamContractSupport.text(
                    splitId, "cdc.completedSplits.splitId", 256);
            if (recordCount < 0L) invalid("negative snapshot split count");
            this.recordCount = recordCount;
            this.contentDigest = StreamContractSupport.digest(
                    contentDigest, "cdc.completedSplits.digest");
        }

        public String splitId() { return splitId; }
        public long recordCount() { return recordCount; }
        public String contentDigest() { return contentDigest; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CompletedSplit)) return false;
            CompletedSplit that = (CompletedSplit) other;
            return recordCount == that.recordCount
                    && splitId.equals(that.splitId)
                    && contentDigest.equals(that.contentDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(splitId, recordCount, contentDigest);
        }
    }

    private final int version;
    private final String sourceOperatorUid;
    private final String snapshotId;
    private final long snapshotCutSequence;
    private final List<String> expectedSplitIds;
    private final List<CompletedSplit> completedSplits;
    private final Phase phase;
    private final long lastLogSequence;
    private final String handoffDigest;
    private final byte[] externalPositionFrame;

    public StreamCdcEnumeratorState(
            int version, String sourceOperatorUid, String snapshotId,
            long snapshotCutSequence, List<String> expectedSplitIds,
            List<CompletedSplit> completedSplits, Phase phase,
            long lastLogSequence, String handoffDigest) {
        this(version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, completedSplits, phase, lastLogSequence,
                handoffDigest, null);
    }

    public StreamCdcEnumeratorState(
            int version, String sourceOperatorUid, String snapshotId,
            long snapshotCutSequence, List<String> expectedSplitIds,
            List<CompletedSplit> completedSplits, Phase phase,
            long lastLogSequence, String handoffDigest,
            byte[] externalPositionFrame) {
        if ((version != LEGACY_VERSION && version != CURRENT_VERSION)
                || snapshotCutSequence < 0L) {
            invalid("unsupported resident CDC state version or snapshot cut");
        }
        this.version = version;
        this.sourceOperatorUid = StreamContractSupport.text(
                sourceOperatorUid, "cdc.sourceOperatorUid", 256);
        this.snapshotId = StreamContractSupport.digest(
                snapshotId, "cdc.snapshotId");
        this.snapshotCutSequence = snapshotCutSequence;
        this.expectedSplitIds = canonicalSplits(expectedSplitIds);
        this.completedSplits = completedPrefix(
                completedSplits, this.expectedSplitIds);
        if (phase == null) invalid("resident CDC phase is required");
        this.phase = phase;
        if (lastLogSequence < snapshotCutSequence) {
            invalid("resident CDC log position precedes snapshot cut");
        }
        this.lastLogSequence = lastLogSequence;
        if (phase == Phase.SNAPSHOT) {
            if (handoffDigest != null
                    || lastLogSequence != snapshotCutSequence
                    || externalPositionFrame != null) {
                invalid("snapshot phase cannot carry a log handoff");
            }
            this.handoffDigest = null;
        } else {
            if (this.completedSplits.size() != this.expectedSplitIds.size()) {
                invalid("log phase requires the complete snapshot split set");
            }
            this.handoffDigest = StreamContractSupport.digest(
                    handoffDigest, "cdc.handoffDigest");
        }
        if (version == LEGACY_VERSION) {
            if (externalPositionFrame != null) {
                invalid("resident CDC v1 cannot carry an external position");
            }
            this.externalPositionFrame = null;
        } else if (phase == Phase.LOG) {
            this.externalPositionFrame = externalPosition(
                    externalPositionFrame);
        } else {
            this.externalPositionFrame = null;
        }
    }

    public static StreamCdcEnumeratorState initial(
            String sourceOperatorUid, String snapshotId,
            long snapshotCutSequence, List<String> expectedSplitIds) {
        return new StreamCdcEnumeratorState(
                LEGACY_VERSION, sourceOperatorUid, snapshotId,
                snapshotCutSequence, expectedSplitIds,
                Collections.emptyList(), Phase.SNAPSHOT,
                snapshotCutSequence, null);
    }

    public static StreamCdcEnumeratorState initialV2(
            String sourceOperatorUid, String snapshotId,
            long snapshotCutSequence, List<String> expectedSplitIds) {
        return new StreamCdcEnumeratorState(
                CURRENT_VERSION, sourceOperatorUid, snapshotId,
                snapshotCutSequence, expectedSplitIds,
                Collections.<CompletedSplit>emptyList(), Phase.SNAPSHOT,
                snapshotCutSequence, null, null);
    }

    public StreamCdcEnumeratorState withCompletedSplit(
            CompletedSplit completed) {
        if (phase != Phase.SNAPSHOT || completed == null) {
            invalid("snapshot split completion is not legal in this phase");
        }
        List<CompletedSplit> next = new ArrayList<>(completedSplits);
        next.add(completed);
        return new StreamCdcEnumeratorState(
                version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, next, phase, lastLogSequence, null,
                externalPositionFrame);
    }

    public StreamCdcEnumeratorState toLog(String exactHandoffDigest) {
        if (version != LEGACY_VERSION) {
            invalid("resident CDC v2 handoff requires an external position");
        }
        return new StreamCdcEnumeratorState(
                version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, completedSplits, Phase.LOG,
                snapshotCutSequence, exactHandoffDigest);
    }

    public StreamCdcEnumeratorState toLog(
            String exactHandoffDigest, byte[] externalPosition) {
        if (version != CURRENT_VERSION) {
            invalid("resident CDC v1 handoff cannot carry an external position");
        }
        return new StreamCdcEnumeratorState(
                version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, completedSplits, Phase.LOG,
                snapshotCutSequence, exactHandoffDigest,
                externalPosition);
    }

    public StreamCdcEnumeratorState withLastLogSequence(long sequence) {
        if (phase != Phase.LOG || sequence <= lastLogSequence) {
            invalid("resident CDC log sequence must advance");
        }
        return new StreamCdcEnumeratorState(
                version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, completedSplits, phase, sequence,
                handoffDigest, externalPositionFrame);
    }

    public StreamCdcEnumeratorState withLastLogBoundary(
            long sequence, byte[] externalPosition) {
        if (version != CURRENT_VERSION || phase != Phase.LOG
                || sequence <= lastLogSequence) {
            invalid("resident CDC v2 log boundary must advance");
        }
        return new StreamCdcEnumeratorState(
                version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, completedSplits, phase, sequence,
                handoffDigest, externalPosition);
    }

    public StreamCdcEnumeratorState withExternalPosition(
            byte[] externalPosition) {
        if (version != CURRENT_VERSION || phase != Phase.LOG) {
            invalid("resident CDC external position requires v2 log phase");
        }
        return new StreamCdcEnumeratorState(
                version, sourceOperatorUid, snapshotId, snapshotCutSequence,
                expectedSplitIds, completedSplits, phase, lastLogSequence,
                handoffDigest, externalPosition);
    }

    public int version() { return version; }
    public String sourceOperatorUid() { return sourceOperatorUid; }
    public String snapshotId() { return snapshotId; }
    public long snapshotCutSequence() { return snapshotCutSequence; }
    public List<String> expectedSplitIds() { return expectedSplitIds; }
    public List<CompletedSplit> completedSplits() { return completedSplits; }
    public Phase phase() { return phase; }
    public long lastLogSequence() { return lastLogSequence; }
    public String handoffDigest() { return handoffDigest; }
    public boolean hasExternalPosition() {
        return externalPositionFrame != null;
    }
    public byte[] externalPositionFrame() {
        if (externalPositionFrame == null) {
            throw new IllegalStateException(
                    "resident CDC state has no external position");
        }
        return externalPositionFrame.clone();
    }

    private static List<String> canonicalSplits(List<String> values) {
        if (values == null || values.isEmpty() || values.size() > MAX_SPLITS) {
            invalid("resident CDC requires a bounded non-empty split set");
        }
        TreeSet<String> sorted = new TreeSet<>();
        for (String value : values) {
            String split = StreamContractSupport.text(
                    value, "cdc.expectedSplits", 256);
            if (!sorted.add(split)) invalid("duplicate resident CDC split ID");
        }
        return Collections.unmodifiableList(new ArrayList<>(sorted));
    }

    private static List<CompletedSplit> completedPrefix(
            List<CompletedSplit> values, List<String> expected) {
        if (values == null || values.size() > expected.size()) {
            invalid("completed snapshot split set exceeds expectation");
        }
        List<CompletedSplit> copy = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            CompletedSplit completed = values.get(index);
            if (completed == null
                    || !expected.get(index).equals(completed.splitId())) {
                invalid("completed snapshot splits must be the canonical prefix");
            }
            copy.add(completed);
        }
        return Collections.unmodifiableList(copy);
    }
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamCdcEnumeratorState)) return false;
        StreamCdcEnumeratorState that = (StreamCdcEnumeratorState) other;
        return version == that.version
                && snapshotCutSequence == that.snapshotCutSequence
                && lastLogSequence == that.lastLogSequence
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && snapshotId.equals(that.snapshotId)
                && expectedSplitIds.equals(that.expectedSplitIds)
                && completedSplits.equals(that.completedSplits)
                && phase == that.phase
                && Objects.equals(handoffDigest, that.handoffDigest)
                && java.util.Arrays.equals(externalPositionFrame,
                that.externalPositionFrame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, sourceOperatorUid, snapshotId,
                snapshotCutSequence, expectedSplitIds, completedSplits,
                phase, lastLogSequence, handoffDigest,
                java.util.Arrays.hashCode(externalPositionFrame));
    }

    private static byte[] externalPosition(byte[] frame) {
        if (frame == null || frame.length < 1
                || frame.length > MAX_EXTERNAL_POSITION_BYTES) {
            invalid("resident CDC external position exceeds its byte bound");
        }
        return frame.clone();
    }

    private static void invalid(String message) {
        throw StreamContractSupport.failure(
                "INVALID_CDC_ENUMERATOR_STATE", "cdc.enumerator", message);
    }
}
