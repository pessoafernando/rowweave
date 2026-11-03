package io.rowweave.monitor;

/**
 * Immutable observation snapshot for one worker.
 * Populated from ServiceRegistry.liveWorkers() with heartbeat liveness determination.
 */
public final class WorkerView {

    private final String workerId;
    private final String host;
    private final int port;
    private final int slots;
    private final int freeSlots;
    private final long managedMemoryUsedBytes;
    private final long managedMemoryMaxBytes;
    private final long startTimeMillis;
    private final boolean alive;

    public WorkerView(String workerId, String host, int port, int slots, boolean alive) {
        this(workerId, host, port, slots, slots, 0, 0, 0, alive);
    }

    private WorkerView(String workerId, String host, int port, int slots,
                       int freeSlots, long managedMemoryUsedBytes,
                       long managedMemoryMaxBytes, long startTimeMillis, boolean alive) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.slots = slots;
        this.freeSlots = freeSlots;
        this.managedMemoryUsedBytes = managedMemoryUsedBytes;
        this.managedMemoryMaxBytes = managedMemoryMaxBytes;
        this.startTimeMillis = startTimeMillis;
        this.alive = alive;
    }

    public String workerId() { return workerId; }
    public String host() { return host; }
    public int port() { return port; }
    public int slots() { return slots; }
    public int freeSlots() { return freeSlots; }
    public long managedMemoryUsedBytes() { return managedMemoryUsedBytes; }
    public long managedMemoryMaxBytes() { return managedMemoryMaxBytes; }
    public long startTimeMillis() { return startTimeMillis; }
    public boolean alive() { return alive; }

    public static Builder builder(String workerId, String host, int port) {
        return new Builder(workerId, host, port);
    }

    public static final class Builder {
        private final String workerId;
        private final String host;
        private final int port;
        private int slots = 1;
        private int freeSlots = 1;
        private long managedMemoryUsedBytes = 0;
        private long managedMemoryMaxBytes = 0;
        private long startTimeMillis = 0;
        private boolean alive = true;

        private Builder(String workerId, String host, int port) {
            this.workerId = workerId;
            this.host = host;
            this.port = port;
        }

        public Builder slots(int value) { this.slots = value; return this; }
        public Builder freeSlots(int value) { this.freeSlots = value; return this; }
        public Builder managedMemoryUsedBytes(long value) { this.managedMemoryUsedBytes = value; return this; }
        public Builder managedMemoryMaxBytes(long value) { this.managedMemoryMaxBytes = value; return this; }
        public Builder startTimeMillis(long value) { this.startTimeMillis = value; return this; }
        public Builder alive(boolean value) { this.alive = value; return this; }

        public WorkerView build() {
            if (freeSlots < 0 || freeSlots > slots) {
                throw new IllegalArgumentException(
                        "freeSlots must be in [0, slots], got " + freeSlots + "/" + slots);
            }
            return new WorkerView(workerId, host, port, slots,
                    freeSlots, managedMemoryUsedBytes,
                    managedMemoryMaxBytes, startTimeMillis, alive);
        }
    }
}
