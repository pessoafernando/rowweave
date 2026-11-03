package io.rowweave.run;

/** Retention claim validation and state transition shared by the three backends. */
public final class RunRetentionAuthorityKernel {
    private RunRetentionAuthorityKernel() { }

    public static Transition evaluate(
            String runId, RunSnapshot snapshot, RunLifecycleRef lifecycle,
            boolean activeRecoveryPresent, long expectedTerminalRevision,
            long cutoffMillis, RetentionOwner owner, long observedOwnershipEpoch) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (expectedTerminalRevision <= 0L) {
            throw new IllegalArgumentException("expectedTerminalRevision must be > 0");
        }
        if (cutoffMillis < 0L) {
            throw new IllegalArgumentException("cutoffMillis must be >= 0");
        }
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (observedOwnershipEpoch < 0L) {
            throw new IllegalArgumentException("observedOwnershipEpoch must be >= 0");
        }
        if (snapshot == null && lifecycle == null) {
            return Transition.of(RunRetentionClaim.staleRun(), null);
        }
        if (snapshot == null || lifecycle == null) {
            throw new IllegalStateException("snapshot/lifecycle presence mismatch");
        }
        requireConsistent(runId, snapshot, lifecycle);
        if (!snapshot.status().isTerminal()
                || expectedTerminalRevision != snapshot.revision()) {
            return Transition.of(RunRetentionClaim.staleRun(), null);
        }
        if (activeRecoveryPresent) {
            throw new IllegalStateException("terminal Run retains active recovery entry");
        }
        if (lifecycle.state() == RunLifecycleState.ACTIVE) {
            throw new IllegalStateException("terminal Run retains active lifecycle state");
        }
        if (lifecycle.state() == RunLifecycleState.TERMINAL) {
            if (lifecycle.terminalAtMillis() > cutoffMillis) {
                return Transition.of(RunRetentionClaim.notDue(), null);
            }
            if (observedOwnershipEpoch != 0L) {
                return Transition.of(RunRetentionClaim.lostRace(), null);
            }
            RetentionToken token = RetentionToken.create(
                    runId, owner, 1L, snapshot.revision(),
                    lifecycle.terminalAtMillis());
            return Transition.of(RunRetentionClaim.acquired(token),
                    RunLifecycleRef.deleting(runId, snapshot.revision(),
                            snapshot.status(), snapshot.artifactId(),
                            lifecycle.terminalAtMillis(), token));
        }
        RetentionToken current = lifecycle.retentionToken();
        if (current.owner().equals(owner)) {
            return Transition.of(RunRetentionClaim.replayed(current), null);
        }
        if (current.ownershipEpoch() != observedOwnershipEpoch) {
            return Transition.of(RunRetentionClaim.lostRace(), null);
        }
        if (current.ownershipEpoch() == Long.MAX_VALUE) {
            throw new IllegalStateException("retention ownership epoch exhausted");
        }
        RetentionToken replacement = RetentionToken.create(
                runId, owner, current.ownershipEpoch() + 1L,
                snapshot.revision(), lifecycle.terminalAtMillis());
        return Transition.of(RunRetentionClaim.acquired(replacement),
                RunLifecycleRef.deleting(runId, snapshot.revision(),
                        snapshot.status(), snapshot.artifactId(),
                        lifecycle.terminalAtMillis(), replacement));
    }

    public static boolean current(RunLifecycleRef lifecycle, RetentionToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        return lifecycle != null
                && lifecycle.state() == RunLifecycleState.DELETING
                && token.runId().equals(lifecycle.runId())
                && token.equals(lifecycle.retentionToken());
    }

    private static void requireConsistent(
            String runId, RunSnapshot snapshot, RunLifecycleRef lifecycle) {
        if (!runId.equals(snapshot.runId()) || !runId.equals(lifecycle.runId())
                || snapshot.revision() != lifecycle.revision()
                || snapshot.status() != lifecycle.status()
                || !snapshot.artifactId().equals(lifecycle.artifactId())) {
            throw new IllegalStateException("snapshot/lifecycle identity mismatch");
        }
        if (!snapshot.status().isTerminal()
                && lifecycle.state() != RunLifecycleState.ACTIVE) {
            throw new IllegalStateException("active Run has non-active lifecycle state");
        }
    }

    public static final class Transition {
        private final RunRetentionClaim claim;
        private final RunLifecycleRef nextLifecycle;

        private Transition(RunRetentionClaim claim, RunLifecycleRef nextLifecycle) {
            this.claim = claim;
            this.nextLifecycle = nextLifecycle;
        }

        private static Transition of(
                RunRetentionClaim claim, RunLifecycleRef nextLifecycle) {
            return new Transition(claim, nextLifecycle);
        }

        public RunRetentionClaim claim() { return claim; }
        public RunLifecycleRef nextLifecycle() { return nextLifecycle; }
    }
}
