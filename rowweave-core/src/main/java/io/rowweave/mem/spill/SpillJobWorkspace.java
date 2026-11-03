package io.rowweave.mem.spill;

import java.nio.file.Path;

/** One owned job directory inside a {@link SpillWorkspaceManager}. */
public final class SpillJobWorkspace implements AutoCloseable, SpillFileAccountant {

    private final SpillWorkspaceManager owner;
    private final String jobId;
    private final Path directory;
    private final java.util.List<DiskSpillStore> stores = new java.util.ArrayList<>();
    private final java.util.List<Path> operatorDirectories = new java.util.ArrayList<>();
    private volatile boolean closed;
    private long reservedBytes;
    private long runCount;
    private long logicalBytesWritten;
    private long peakReservedBytes;
    private boolean telemetryInvalid;
    private int storeSequence;

    SpillJobWorkspace(SpillWorkspaceManager owner, String jobId, Path directory) {
        this.owner = owner;
        this.jobId = jobId;
        this.directory = directory;
    }

    public Path directory() {
        return directory;
    }

    /** Creates a workspace-owned, quota-accounted disk store for one operator. */
    public DiskSpillStore newSpillStore() {
        return owner.openSpillStore(this, null);
    }

    DiskSpillStore newSpillStoreForTests(SpillRunOutputFactory outputFactory) {
        return owner.openSpillStore(this, outputFactory);
    }

    public long reservedBytes() {
        return owner.reservedBytes(this);
    }

    String jobId() {
        return jobId;
    }

    synchronized boolean markClosed() {
        if (closed) return false;
        closed = true;
        return true;
    }

    boolean isClosed() {
        return closed;
    }

    int nextStoreSequence() {
        return ++storeSequence;
    }

    void registerStore(DiskSpillStore store, Path operatorDirectory) {
        stores.add(store);
        operatorDirectories.add(operatorDirectory);
    }

    java.util.List<DiskSpillStore> storesSnapshot() {
        return new java.util.ArrayList<>(stores);
    }

    java.util.List<Path> operatorDirectoriesSnapshot() {
        return new java.util.ArrayList<>(operatorDirectories);
    }

    long rawReservedBytes() {
        return reservedBytes;
    }

    void addReservedBytes(long bytes) {
        reservedBytes += bytes;
    }

    void subtractReservedBytes(long bytes) {
        reservedBytes -= bytes;
    }

    @Override
    public void runOpened() {
        owner.runOpened(this);
        synchronized (this) {
            runCount = addTelemetry(runCount, 1L);
        }
    }

    @Override
    public void runClosed() {
        owner.runClosed(this);
    }

    @Override
    public void reserve(long bytes) {
        owner.reserve(this, bytes);
        synchronized (this) {
            peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
        }
    }

    @Override
    public void release(long bytes) {
        owner.release(this, bytes);
    }

    @Override
    public synchronized void written(long logicalBytes) {
        if (logicalBytes <= 0L) {
            telemetryInvalid = true;
            return;
        }
        logicalBytesWritten = addTelemetry(logicalBytesWritten, logicalBytes);
    }

    public synchronized SpillJobTelemetrySnapshot telemetrySnapshot() {
        return new SpillJobTelemetrySnapshot(!telemetryInvalid,
                telemetryInvalid ? 0L : runCount,
                telemetryInvalid ? 0L : logicalBytesWritten,
                telemetryInvalid ? 0L : peakReservedBytes);
    }

    private long addTelemetry(long current, long increment) {
        if (increment < 0L || Long.MAX_VALUE - current < increment) {
            telemetryInvalid = true;
            return 0L;
        }
        return current + increment;
    }

    @Override
    public RuntimeException writeFailure(java.io.IOException cause) {
        return SpillDiskException.ioWriteFailed(jobId.substring(0, 16), cause);
    }

    @Override
    public void close() {
        owner.closeJob(this);
    }
}
