package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCdcEnumeratorState;

/** Bounded operational view with all source, split and digest identities redacted. */
public final class StreamCdcStatusView {
    private final StreamCdcEnumeratorState.Phase phase;
    private final int totalSnapshotSplits;
    private final int completedSnapshotSplits;
    private final long snapshotCutSequence;
    private final long lastLogSequence;

    StreamCdcStatusView(StreamCdcEnumeratorState state) {
        this.phase = state.phase();
        this.totalSnapshotSplits = state.expectedSplitIds().size();
        this.completedSnapshotSplits = state.completedSplits().size();
        this.snapshotCutSequence = state.snapshotCutSequence();
        this.lastLogSequence = state.lastLogSequence();
    }

    public StreamCdcEnumeratorState.Phase phase() { return phase; }
    public int totalSnapshotSplits() { return totalSnapshotSplits; }
    public int completedSnapshotSplits() { return completedSnapshotSplits; }
    public long snapshotCutSequence() { return snapshotCutSequence; }
    public long lastLogSequence() { return lastLogSequence; }

    @Override
    public String toString() {
        return "StreamCdcStatusView{" + "phase=" + phase
                + ", totalSnapshotSplits=" + totalSnapshotSplits
                + ", completedSnapshotSplits=" + completedSnapshotSplits
                + ", snapshotCutSequence=" + snapshotCutSequence
                + ", lastLogSequence=" + lastLogSequence + '}';
    }
}
