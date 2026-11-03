package io.rowweave.mem.spill;

/** Internal point-in-time spill usage; deliberately not an ExecutionReport/RPC contract. */
final class SpillWorkspaceUsageSnapshot {
    private final int activeJobs;
    private final int activeRuns;
    private final long reservedBytes;

    SpillWorkspaceUsageSnapshot(int activeJobs, int activeRuns, long reservedBytes) {
        this.activeJobs = activeJobs;
        this.activeRuns = activeRuns;
        this.reservedBytes = reservedBytes;
    }

    int activeJobs() { return activeJobs; }
    int activeRuns() { return activeRuns; }
    long reservedBytes() { return reservedBytes; }
}
