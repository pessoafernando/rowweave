package io.rowweave.run;

import java.util.Arrays;

/**
 * Authority-minted observation of the exact canonical commit-intent bytes read from
 * the durable Manifest journal. Callers can inspect but cannot construct provenance.
 */
public final class DurableManifestCommitIntentObservation {
    private final RunAttemptFence fence;
    private final long assignmentGeneration;
    private final ManifestCommitIntent intent;
    private final byte[] canonicalIntentFrame;

    DurableManifestCommitIntentObservation(ManifestCommitJournalSnapshot snapshot) {
        if (snapshot == null || snapshot.intent() == null) {
            throw new IllegalArgumentException("durable Manifest intent is required");
        }
        this.intent = snapshot.intent();
        this.fence = snapshot.assignments().manifest().fence();
        this.assignmentGeneration = snapshot.assignments().manifest().generation();
        this.canonicalIntentFrame = ManifestCommitJournalCodec.encodeIntent(intent);
        ManifestCommitIntent decoded = ManifestCommitJournalCodec.decodeIntent(
                canonicalIntentFrame);
        if (!fence.equals(intent.fence())
                || assignmentGeneration != intent.assignmentGeneration()
                || !decoded.equals(intent)
                || !Arrays.equals(canonicalIntentFrame,
                ManifestCommitJournalCodec.encodeIntent(decoded))) {
            throw new IllegalArgumentException(
                    "durable Manifest intent observation is not canonical");
        }
    }

    public RunAttemptFence fence() { return fence; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public ManifestCommitIntent intent() { return intent; }
    public byte[] canonicalIntentFrame() { return canonicalIntentFrame.clone(); }
}
