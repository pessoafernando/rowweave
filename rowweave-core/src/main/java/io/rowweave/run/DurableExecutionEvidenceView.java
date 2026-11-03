package io.rowweave.run;

/** Immutable snapshot-first view used to recover advisory execution telemetry. */
public final class DurableExecutionEvidenceView {
    public enum State { MISSING, SNAPSHOT_ONLY, PRESENT }

    private static final DurableExecutionEvidenceView MISSING =
            new DurableExecutionEvidenceView(State.MISSING, null, null);

    private final State state;
    private final RunSnapshot snapshot;
    private final StageAssignmentSnapshot assignments;

    private DurableExecutionEvidenceView(
            State state, RunSnapshot snapshot, StageAssignmentSnapshot assignments) {
        this.state = state;
        this.snapshot = snapshot;
        this.assignments = assignments;
    }

    public static DurableExecutionEvidenceView missing() { return MISSING; }

    public static DurableExecutionEvidenceView snapshotOnly(RunSnapshot snapshot) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        return new DurableExecutionEvidenceView(State.SNAPSHOT_ONLY, snapshot, null);
    }

    public static DurableExecutionEvidenceView present(
            RunSnapshot snapshot, StageAssignmentSnapshot assignments) {
        requireLinkage(snapshot, assignments);
        return new DurableExecutionEvidenceView(State.PRESENT, snapshot, assignments);
    }

    private static void requireLinkage(
            RunSnapshot snapshot, StageAssignmentSnapshot assignments) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        if (assignments == null) throw new IllegalArgumentException("assignments are required");
        AttemptSnapshot attempt = snapshot.currentAttempt();
        RunAttemptFence fence = assignments.manifest().fence();
        if (attempt == null
                || !snapshot.runId().equals(fence.runId())
                || !attempt.attemptId().equals(fence.attemptId())
                || attempt.fenceToken() != fence.fenceToken()
                || !snapshot.artifactId().equals(assignments.manifest().artifactId())) {
            throw new IllegalArgumentException("snapshot/assignment evidence linkage mismatch");
        }
    }

    public State state() { return state; }
    public RunSnapshot snapshot() { return snapshot; }
    public StageAssignmentSnapshot assignments() { return assignments; }
}
