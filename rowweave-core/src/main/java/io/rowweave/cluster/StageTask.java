package io.rowweave.cluster;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.internal.RowSnapshot;
import io.rowweave.ops.Partitioner;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryAccumulator;
import io.rowweave.sched.Channel;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.net.MultiSocketChannelSource;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.SocketChannelSink;
import io.rowweave.sched.net.SocketChannelSource;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The unit that runs one stage inside the worker's process, on a thread taken from the
 * {@link WorkerServer} pool rather than in a process of its own. Three boundaries separate it from
 * whoever started it:
 *
 * <ul>
 * <li><b>Ports travel by callback</b>, {@link PortListener}, instead of a printed line. A role that
 * listens, CONSUMER, KEYED_OP, SINK or JOIN_CONSUMER, reports the port it bound as soon as its
 * inbound is open, naming the edge the port belongs to. The order bind, report, then let the producer
 * connect, is guaranteed by the sequence of the body itself.</li>
 * <li><b>Completion and row counts travel by return value</b>, {@link StageResult}.</li>
 * <li><b>A reason says why it stopped</b>, {@link StopReason}, which is EOS, CANCELLED or FAILED, and
 * a cooperative {@link #stop(StopReason)} poisons the channels. That is what releases a thread parked
 * on a read, where killing a process used to.</li>
 * </ul>
 *
 * <p><b>The data plane is unchanged</b>: stages still exchange rows over real sockets through
 * {@link SocketChannelSink} and {@link MultiSocketChannelSource}.
 */
public final class StageTask {

    private static final Logger LOG = LoggerFactory.getLogger(StageTask.class);

    static final int KEEPALIVE_MS = Integer.getInteger("rowweave.shuffle.keepaliveMs", 10_000);
    static final int SO_TIMEOUT_MS = Integer.getInteger("rowweave.shuffle.soTimeoutMs", 60_000);
    // Accepting is a rendezvous in reverse topological order: the downstream binds first and the upstream
    // connects later. The accept clock starts when the downstream binds, while the upstream cannot connect
    // until it has been assigned and scheduled onto the stage pool, and under the load of several clusters
    // in one process, forty sockets and more, that gap occasionally passes thirty seconds. Accept is a
    // second line of defence against hanging during setup, so it must not be more aggressive than the
    // master's timeout for a reported port; execution itself is governed by liveness await and the
    // optional hard limit on a job. So the timeout follows the configured stage timeout with a floor of
    // sixty seconds, which keeps a loaded machine from refusing a connection that is only slow to form.
    private static final int ACCEPT_TIMEOUT_MS = Integer.getInteger("rowweave.shuffle.acceptTimeoutMs",
            Math.max(60_000, Long.getLong("rowweave.cluster.stageTimeoutMs", 60_000L).intValue()));
    private static final int EQ_BLOCK_THRESHOLD = 1024;
    private static final int LOCAL_SINGLE_PRODUCER_BATCH_SIZE = 32;

    /**
     * Why a stage thread stopped, in place of a bare flag or an exit code.
     */
    public enum StopReason { EOS, CANCELLED, FAILED }

    /**
     * The outcome of running a stage: the reason it stopped, the rows it wrote, which means something only
     * for SINK and WHOLE_GRAPH and is zero for the rest, and the failure that ended it when the reason is
     * FAILED.
     */
    public static final class StageResult {
        private final StopReason reason;
        private final long rows;
        private final Throwable failure;
        private final StageExecutionTelemetry telemetry;

        StageResult(StopReason reason, long rows, Throwable failure) {
            this(reason, rows, failure, null);
        }

        private StageResult(StopReason reason, long rows, Throwable failure,
                            StageExecutionTelemetry telemetry) {
            this.reason = reason;
            this.rows = rows;
            this.failure = failure;
            this.telemetry = telemetry;
        }

        public StopReason reason() { return reason; }
        public long rows() { return rows; }
        public Throwable failure() { return failure; }
        public StageExecutionTelemetry telemetry() {
            return telemetry == null ? StageExecutionTelemetry.unavailable() : telemetry;
        }

        public static StageResult commandNotStarted(StopReason reason, Throwable failure) {
            if (reason == null) throw new IllegalArgumentException("stop reason is required");
            return new StageResult(reason, 0, failure, StageExecutionTelemetry.notExecuted());
        }

        private StageResult withTelemetry(StageExecutionTelemetry finalTelemetry) {
            return new StageResult(reason, rows, failure, finalTelemetry);
        }
    }

    /**
     * Reports the port a listener bound, from the stage thread, instead of a printed line. The edge is 0
     * for the single or left inbound and 1 for the right inbound of a JOIN_CONSUMER.
     */
    @FunctionalInterface
    public interface PortListener {
        void onPort(int port, int edge);
        PortListener NONE = (port, edge) -> { };
    }

    /**
     * Periodic reporting while a stage runs: the rows read, the rows produced, the state, which is READY
     * while it waits for data and RUNNING once it is processing, and the moment the first row arrived. A
     * breaker fills its working set first, so its input grows while its output stays at zero.
     */
    public interface ProgressListener {
        void onProgress(long inputRows, long outputRows, boolean running, long firstRowTime);
        ProgressListener NONE = (in, out, r, ft) -> { };
    }

    /** Package-local setup seam: production defaults remain the concrete bounded implementations. */
    interface SetupFactory {
        default io.rowweave.mem.ExecutionMemory executionMemory(
                boolean workspaceNeeded,
                io.rowweave.mem.MemoryOwnerRequirement... requirements) {
            return io.rowweave.mem.ExecutionMemory.forJob(workspaceNeeded, requirements);
        }

        default io.rowweave.sched.MaterializedRowBuffer materializedBuffer(
                DataSchema meta, io.rowweave.mem.spill.SpillStore spillStore) {
            return new io.rowweave.sched.MaterializedRowBuffer(meta, spillStore);
        }

        default Partitioner partitioner(DataSchema meta, int partitions, String[] keys, boolean normalized) {
            return normalized
                    ? Partitioner.numericNormalizedOfFields(meta, partitions, keys)
                    : Partitioner.ofFields(meta, partitions, keys);
        }

        default io.rowweave.sched.Channel endpoint(
                ShuffleEndpoint endpoint, DataSchema meta,
                Supplier<io.rowweave.sched.Channel> productionFactory) {
            return productionFactory.get();
        }
    }

    private static final SetupFactory DEFAULT_SETUP_FACTORY = new SetupFactory() { };

    private final StageCommand cmd;
    private final PortListener onPort;
    private final Supplier<java.util.concurrent.ScheduledExecutorService> progressTimerFactory;
    private final SetupFactory setupFactory;
    private volatile ProgressListener onProgress = ProgressListener.NONE;
    // The same-worker local channel rendezvous, injected by WorkerServer and shared across that worker.
    // Null means it was never wired, so the stage uses sockets alone.
    private volatile io.rowweave.sched.LocalShuffleRegistry localShuffle;
    // The registry that demultiplexes the worker's single data port, injected by WorkerServer and shared
    // across that worker. Null means muxing is off and each stage binds a port of its own. When muxing is
    // on, openInbound registers this stage's inbound channel under its routing key rather than binding,
    // and the server sends to it by that key.
    private volatile io.rowweave.sched.net.DataInboundRegistry dataInbound;
    private volatile io.rowweave.sched.net.MuxConnectionPool muxConnections;
    private volatile String muxJobId;  // the job id the mux registration is reclaimed by, injected by WorkerServer
    // The fan-out running now, which is where the produced row count is read from; the progress timer reads
    // its total. A sink role uses its own counter instead.
    private volatile FanoutSink currentFanout;
    // The memory behind a blocking fan-out's materialised buffer, closed in the finally block, which is
    // what deletes the spill directory. Null when no edge is blocking.
    private volatile io.rowweave.mem.ExecutionMemory blockingMem;
    private volatile CancelOwner blockingMemOwner;
    private volatile long[] sinkCount;  // the sink role's row counter, shared with whatever writes it
    private volatile StageExecutionTelemetryAccumulator telemetryAccumulator;
    // The moment the first row of data arrived. Zero means the stage is READY and waiting on its upstream;
    // anything else means it is RUNNING and genuinely processing.
    private volatile long firstRowTime = 0;
    // Rows read from upstream, which is the same as rows handled. Every record read adds one; a join
    // accumulates both inbound edges into this one counter.
    private final java.util.concurrent.atomic.AtomicLong inputRows = new java.util.concurrent.atomic.AtomicLong();
    private void markFirstRow() { if (firstRowTime == 0) firstRowTime = System.currentTimeMillis(); }
    /**
     * Consumes one record read: marks the first arrival, which moves READY to RUNNING, and counts the
     * input. Reading is handling here, because the map, filter or accumulate follows at once.
     */
    private void onInputRecord() { markFirstRow(); inputRows.incrementAndGet(); }
    /** Wraps a RowStream so each record that arrives marks the first row and the input count. */
    private RowStream markingStream(RowStream in) {
        return () -> { StreamEvent e = in.next(); if (e != null && e.isRecord()) onInputRecord(); return e; };
    }

    // Cooperative stop: non-null once this stage has been asked to stop. Poisoning the channels is what
    // wakes a thread parked on a read.
    private volatile StopReason stopRequest;
    // Cancellation, resource release and the irreversible terminal action all share one linearised state
    // machine.
    private enum Lifecycle { QUEUED, RUNNING, TERMINAL_CLAIMED, CANCELLED, FINISHED }

    @FunctionalInterface
    private interface CancellationCleanup {
        void cleanup(Throwable cause);
    }

    /**
     * For one owner, the first of cancellation, failure and normal retirement to claim the terminal state
     * wins, and the others become no-ops.
     */
    private static final class CancelOwner {
        private final CancellationCleanup cleanup;
        private final AtomicBoolean terminal = new AtomicBoolean();

        private CancelOwner(CancellationCleanup cleanup) {
            this.cleanup = cleanup;
        }

        private boolean retire() {
            return terminal.compareAndSet(false, true);
        }

        private boolean isTerminal() {
            return terminal.get();
        }

        private void cancel(Throwable cause) {
            if (terminal.compareAndSet(false, true)) {
                cleanup.cleanup(cause);
            }
        }
    }

    private final Object startStopLock = new Object();
    private Lifecycle lifecycle = Lifecycle.QUEUED;
    private RuntimeException cancellationSignal;
    private volatile Throwable cancellationPrimaryFailure;
    private int cancellationCleanupsInFlight;
    private final List<CancelOwner> cancellationOwners = new ArrayList<>();
    // Read and written only by the owner thread of the role; stop walks the cancellation owners and never
    // touches this map at the same time.
    private final Map<Channel, CancelOwner> channelOwners = new IdentityHashMap<>();

    public StageTask(StageCommand cmd, PortListener onPort) {
        this(cmd, onPort, StageTask::newProgressTimer, DEFAULT_SETUP_FACTORY);
    }

    StageTask(StageCommand cmd, PortListener onPort,
              Supplier<java.util.concurrent.ScheduledExecutorService> progressTimerFactory) {
        this(cmd, onPort, progressTimerFactory, DEFAULT_SETUP_FACTORY);
    }

    StageTask(StageCommand cmd, PortListener onPort, SetupFactory setupFactory) {
        this(cmd, onPort, StageTask::newProgressTimer, setupFactory);
    }

    StageTask(StageCommand cmd, PortListener onPort,
              Supplier<java.util.concurrent.ScheduledExecutorService> progressTimerFactory,
              SetupFactory setupFactory) {
        this.cmd = cmd;
        this.onPort = onPort == null ? PortListener.NONE : onPort;
        if (progressTimerFactory == null) {
            throw new IllegalArgumentException("progressTimerFactory is required");
        }
        this.progressTimerFactory = progressTimerFactory;
        if (setupFactory == null) {
            throw new IllegalArgumentException("setupFactory is required");
        }
        this.setupFactory = setupFactory;
    }

    private static java.util.concurrent.ScheduledExecutorService newProgressTimer() {
        return java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rowweave-progress");
            t.setDaemon(true);
            return t;
        });
    }

    /** Sets the progress callback, which WorkerServer uses to report progress to the master. */
    public void setProgressListener(ProgressListener l) { this.onProgress = l == null ? ProgressListener.NONE : l; }

    /**
     * Injection of the per-worker local shuffle registry, so two stages on one worker meet on a local
     * channel instead of a socket.
     */
    public void setLocalShuffle(io.rowweave.sched.LocalShuffleRegistry reg) { this.localShuffle = reg; }

    /**
     * Injection of the worker's single-port registry and the job id its registrations are reclaimed under.
     * Null means the stage binds a port of its own.
     */
    public void setDataInbound(io.rowweave.sched.net.DataInboundRegistry reg,
                               io.rowweave.sched.net.MuxConnectionPool connections,
                               String jobId) {
        this.dataInbound = reg;
        this.muxConnections = connections;
        this.muxJobId = jobId;
    }

    /**
     * Rows produced so far, from the fan-out or the sink count, or zero. Read by the progress timer.
     */
    private long currentRows() {
        FanoutSink fo = currentFanout; if (fo != null) return fo.emitted();
        long[] sc = sinkCount; if (sc != null) return sc[0];
        return 0;
    }

    /** Rows handled so far, meaning read from upstream. Read by the progress timer. */
    private long currentInputRows() { return inputRows.get(); }

    /** Rows this stage handled in total, read for the final report once the run returns. */
    public long inputRows() { return inputRows.get(); }

    /**
     * Stops the stage cooperatively: records the reason and poisons this task's inbound channels, which
     * wakes a thread parked on a poll or an accept so that it sees the poisoned stream, unwinds and reports
     * the reason. Idempotent.
     */
    public void stop(StopReason reason) {
        if (reason == null) {
            throw new IllegalArgumentException("stop reason is required");
        }
        RuntimeException cancel;
        List<CancelOwner> owners;
        synchronized (startStopLock) {
            if (lifecycle == Lifecycle.FINISHED
                    || lifecycle == Lifecycle.TERMINAL_CLAIMED
                    || lifecycle == Lifecycle.CANCELLED) {
                return;
            }
            this.stopRequest = reason;
            if (lifecycle == Lifecycle.QUEUED) {
                lifecycle = Lifecycle.CANCELLED;
                return;
            }
            lifecycle = Lifecycle.CANCELLED;
            if (cancellationSignal == null) {
                cancellationSignal = new RuntimeException("stage cancelled");
            }
            cancel = cancellationSignal;
            owners = new ArrayList<>(cancellationOwners);
            cancellationCleanupsInFlight++;
        }
        try {
            // The reverse of the acquisition order is the teardown order, and each owner has its own exactly-once
            // fence.
            for (int i = owners.size() - 1; i >= 0; i--) {
                try {
                    owners.get(i).cancel(cancel);
                } catch (Throwable cleanupFailure) {
                    appendSuppressed(cancel, cleanupFailure);
                }
            }
        } finally {
            synchronized (startStopLock) {
                cancellationCleanupsInFlight--;
                startStopLock.notifyAll();
            }
        }
    }

    /**
     * Runs this stage, blocking until it completes, fails or is cancelled, and returns a
     * {@link StageResult}. Nothing escapes as an exception: it is folded into a FAILED result.
     */
    public StageResult run() {
        synchronized (startStopLock) {
            if (lifecycle == Lifecycle.CANCELLED) {
                lifecycle = Lifecycle.FINISHED;
                return StageResult.commandNotStarted(stopRequest, null);
            }
            if (lifecycle != Lifecycle.QUEUED) {
                throw new IllegalStateException("stage task can only run once");
            }
            lifecycle = Lifecycle.RUNNING;
        }
        StageExecutionTelemetryAccumulator telemetry = new StageExecutionTelemetryAccumulator();
        telemetry.add(StageExecutionTelemetry.complete(0, 0, 0, 0, 0, 0));
        this.telemetryAccumulator = telemetry;
        // A timing probe behind rowweave.profile.timing prints the role and the wall clock of each stage, to
        // name the stage the seconds are going to. It costs nothing while the property is unset.
        boolean profTiming = Boolean.getBoolean("rowweave.profile.timing");
        long start = profTiming ? System.nanoTime() : 0;
        // The progress timer starts only when a listener was injected. Every half second it reports the counts
        // so far, so a monitor sees them grow while the stage runs.
        java.util.concurrent.ScheduledExecutorService progressTimer = null;
        StageResult result = null;
        try {
            if (onProgress != ProgressListener.NONE) {
                java.util.concurrent.ScheduledExecutorService candidate = progressTimerFactory.get();
                if (candidate == null) {
                    throw new IllegalStateException("progress timer factory returned null");
                }
                try {
                    candidate.scheduleAtFixedRate(() -> {
                        try { onProgress.onProgress(currentInputRows(), currentRows(), firstRowTime > 0, firstRowTime); }
                        catch (RuntimeException ignored) { }
                    }, 300, 300, java.util.concurrent.TimeUnit.MILLISECONDS);
                    progressTimer = candidate;
                } catch (Throwable setupFailure) {
                    try {
                        candidate.shutdownNow();
                    } catch (Throwable cleanupFailure) {
                        setupFailure.addSuppressed(cleanupFailure);
                    }
                    throw setupFailure;
                }
            }
            switch (cmd.role()) {
                case CONSUMER:      result = runConsumer(); break;
                case PRODUCER:      result = runProducer(); break;
                case ROUTER:        result = runRouter(); break;
                case KEYED_OP:      result = runKeyedOp(); break;
                case SINK:          result = runSink(); break;
                case JOIN_CONSUMER: result = runJoinConsumer(); break;
                case WHOLE_GRAPH:   result = runWholeGraph(); break;
                case SOURCE_STAGE:  result = runSourceStage(); break;
                default:            throw new IllegalArgumentException("unknown role: " + cmd.role());
            }
        } catch (Throwable t) {
            claimFailure();
            StopReason r = isCancellationWinner() ? stopRequest : StopReason.FAILED;
            if (r == StopReason.FAILED) {
                LOG.warn("stage failed role={} inbound={}", cmd.role(), cmd.inboundConnections(), t);
            }
            result = new StageResult(r, 0, t);
        } finally {
            if (progressTimer != null) {
                try {
                    progressTimer.shutdownNow();  // stop the progress timer
                } catch (Throwable cleanupFailure) {
                    if (result != null && result.failure() != null) {
                        appendSuppressed(result.failure(), cleanupFailure);
                    } else {
                        result = new StageResult(StopReason.FAILED, 0, cleanupFailure);
                    }
                }
            }
            if (profTiming) {
                LOG.debug("stage done role={} inbound={} wallMs={}",
                        cmd.role(), cmd.inboundConnections(), (System.nanoTime() - start) / 1_000_000);
            }
        }
        if (result == null) {
            result = new StageResult(StopReason.FAILED, 0,
                    new IllegalStateException("stage completed without a result"));
        }
        result = finishResult(result);
        return result.withTelemetry(telemetry.freeze());
    }

    private StageResult finishResult(StageResult result) {
        final StopReason requested;
        final RuntimeException signal;
        boolean interrupted = false;
        synchronized (startStopLock) {
            boolean cancelled = lifecycle == Lifecycle.CANCELLED;
            if (lifecycle == Lifecycle.RUNNING) {
                lifecycle = Lifecycle.TERMINAL_CLAIMED;
            }
            while (cancellationCleanupsInFlight > 0) {
                try {
                    startStopLock.wait();
                } catch (InterruptedException waitInterrupted) {
                    interrupted = true;
                }
            }
            requested = cancelled ? stopRequest : null;
            signal = cancellationSignal;
            lifecycle = Lifecycle.FINISHED;
            cancellationOwners.clear();
            channelOwners.clear();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (requested == null) {
            return result;
        }
        Throwable failure = cancellationPrimaryFailure != null
                ? cancellationPrimaryFailure : (signal != null ? signal : result.failure());
        Throwable roleFailure = result.failure();
        if (roleFailure != null && roleFailure != failure
                && !containsThrowable(roleFailure, failure)
                && !containsThrowable(failure, roleFailure)) {
            appendSuppressed(failure, roleFailure);
        }
        if (signal != null && signal.getSuppressed().length > 0
                && cancellationPrimaryFailure != null && failure != signal) {
                for (Throwable cleanupFailure : signal.getSuppressed()) {
                    appendSuppressed(failure, cleanupFailure);
                }
        }
        return new StageResult(requested, result.rows(), failure);
    }

    private static boolean containsThrowable(Throwable root, Throwable target) {
        if (root == null || target == null) return false;
        java.util.Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
        pending.push(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.pop();
            if (current == target) return true;
            if (!seen.add(current)) continue;
            Throwable cause = current.getCause();
            if (cause != null) pending.push(cause);
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed != null) pending.push(suppressed);
            }
        }
        return false;
    }

    /**
     * The single linearisation point for normal EOS and for publishing a prepared sink. If cancellation wins
     * first, the caller must not send EOS.
     */
    private boolean claimTerminal() {
        synchronized (startStopLock) {
            if (lifecycle == Lifecycle.RUNNING) {
                lifecycle = Lifecycle.TERMINAL_CLAIMED;
                return true;
            }
            return lifecycle == Lifecycle.TERMINAL_CLAIMED;
        }
    }

    /**
     * A failure claims the terminal state before cleanup runs, and the same lock decides which of it and a
     * concurrent cancellation wins.
     */
    private void claimFailure() {
        synchronized (startStopLock) {
            if (lifecycle == Lifecycle.RUNNING) {
                lifecycle = Lifecycle.TERMINAL_CLAIMED;
            }
        }
    }

    private boolean isCancellationWinner() {
        synchronized (startStopLock) {
            return lifecycle == Lifecycle.CANCELLED;
        }
    }

    private RuntimeException cancellationException() {
        synchronized (startStopLock) {
            if (cancellationSignal == null) {
                cancellationSignal = new RuntimeException("stage cancelled");
            }
            return cancellationSignal;
        }
    }

    private CancelOwner own(CancellationCleanup cleanup) {
        CancelOwner owner = new CancelOwner(cleanup);
        registerOwner(owner);
        return owner;
    }

    private <T extends Channel> T ownChannel(T channel) {
        CancelOwner owner = new CancelOwner(channel::fail);
        channelOwners.put(channel, owner);
        registerOwner(owner);
        return channel;
    }

    private void registerOwner(CancelOwner owner) {
        RuntimeException cancelled = null;
        synchronized (startStopLock) {
            if (lifecycle == Lifecycle.RUNNING) {
                cancellationOwners.add(owner);
                return;
            }
            if (lifecycle == Lifecycle.CANCELLED) {
                cancelled = cancellationException();
                cancellationCleanupsInFlight++;
            } else {
                throw new IllegalStateException("resource acquired outside running stage: " + lifecycle);
            }
        }
        try {
            owner.cancel(cancelled);
        } catch (Throwable cleanupFailure) {
            appendSuppressed(cancelled, cleanupFailure);
        } finally {
            synchronized (startStopLock) {
                cancellationCleanupsInFlight--;
                startStopLock.notifyAll();
            }
        }
        throw cancelled;
    }

    private void failOwnedChannel(Channel channel, Throwable primary) {
        CancelOwner owner = channelOwners.get(channel);
        try {
            if (owner == null) {
                channel.fail(primary);
            } else {
                owner.cancel(primary);
            }
        } catch (Throwable cleanupFailure) {
            appendSuppressed(primary, cleanupFailure);
        }
    }

    private void closeOwnedChannel(Channel channel) {
        CancelOwner owner = channelOwners.get(channel);
        if (owner == null || owner.retire()) {
            channel.close();
        }
    }

    private void cleanupOwner(CancelOwner owner, Throwable primary) {
        if (owner == null) return;
        Throwable effectivePrimary = primary;
        if (effectivePrimary == null && isCancellationWinner()) {
            effectivePrimary = cancellationException();
        }
        try {
            owner.cancel(effectivePrimary);
        } catch (Throwable cleanupFailure) {
            // Cancellation may win while an already-started cleanup is blocked.
            // Re-read the lifecycle here so the cleanup fact is attached to the
            // canonical cancellation signal instead of escaping on the role thread.
            if (effectivePrimary == null && isCancellationWinner()) {
                effectivePrimary = cancellationException();
            }
            if (effectivePrimary == null) {
                if (cleanupFailure instanceof RuntimeException) throw (RuntimeException) cleanupFailure;
                if (cleanupFailure instanceof Error) throw (Error) cleanupFailure;
                throw new IllegalStateException("unexpected checked cleanup failure", cleanupFailure);
            }
            appendSuppressed(effectivePrimary, cleanupFailure);
        }
    }

    private StageResult runConsumer() throws Exception {
        SocketChannelSource source = ownChannel(
                new SocketChannelSource(cmd.dataSchema(), cmd.window(), KEEPALIVE_MS, SO_TIMEOUT_MS));
        Sink output = cmd.output();
        AtomicBoolean outputAbortStarted = new AtomicBoolean();
        CancelOwner outputOwner = own(cause -> {
            if (outputAbortStarted.compareAndSet(false, true)) {
                try {
                    output.abort();
                } catch (Throwable abortFailure) {
                    cancellationPrimaryFailure = abortFailure;
                    throw abortFailure;
                }
            }
        });
        Throwable primary = null;
        try {
            onPort.onPort(source.port(), 0);  // report the port just bound through the callback, not stdout
            StreamEvent e;
            while (stopRequest == null && (e = source.poll()) != null) {
                if (e.isRecord()) {
                    output.collect(e.row());
                } else if (e.isError()) {
                    throw new RuntimeException("upstream error frame", e.cause());
                }
            }
            if (stopRequest != null) {
                cleanupOwner(outputOwner, cancellationException());
                return new StageResult(stopRequest, 0, null);
            }
            if (!claimTerminal()) {
                cleanupOwner(outputOwner, cancellationException());
                return new StageResult(stopRequest, 0, null);
            }
            output.close();
            outputOwner.retire();
            return new StageResult(StopReason.EOS, 0, null);
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            cleanupOwner(outputOwner, t);
            failOwnedChannel(source, t);
            throw t;
        } finally {
            if (primary == null) closeOwnedChannel(source);
        }
    }

    private StageResult runProducer() {
        Channel sink = ownChannel(openEndpoint(
                new ShuffleEndpoint(cmd.host(), cmd.port()), cmd.dataSchema()));
        // Read through SourceExecutor, which also closes it, and stop() closes it too on cancellation. Rows are
        // sent one at a time.
        io.rowweave.sched.stream.RowStream.CloseableRowStream rs =
                io.rowweave.sched.stream.RowStream.fromSource(cmd.input());
        CancelOwner sourceOwner = own(cause -> rs.close());
        Throwable primary = null;
        try {
            StreamEvent e;
            while ((e = rs.next()) != null) {
                if (!e.isRecord()) { continue; }
                if (stopRequest != null) {
                    return new StageResult(stopRequest, 0, null);
                }
                sink.send(StreamEvent.record(e.row()));
            }
            if (!claimTerminal()) {
                return new StageResult(stopRequest, 0, null);
            }
            closeOwnedChannel(sink);
            return new StageResult(StopReason.EOS, 0, null);
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            failOwnedChannel(sink, t);
            throw t;
        } finally {
            cleanupOwner(sourceOwner, primary);
        }
    }

    private StageResult runRouter() {
        List<ShuffleEndpoint> down = cmd.downstream();
        int p = down.size();
        Partitioner partitioner = setupFactory.partitioner(
                cmd.dataSchema(), p, cmd.keyFields(), cmd.numericNormalized());
        List<io.rowweave.sched.Channel> sinks = new ArrayList<>(p);
        // Read through SourceExecutor, which also closes it, and stop() closes it too on cancellation.
        io.rowweave.sched.stream.RowStream.CloseableRowStream rs =
                io.rowweave.sched.stream.RowStream.fromSource(cmd.input());
        CancelOwner sourceOwner = own(cause -> rs.close());
        Throwable primary = null;
        try {
            for (ShuffleEndpoint ep : down) {
                sinks.add(ownChannel(openEndpoint(ep, cmd.dataSchema())));
            }
            StreamEvent e;
            while ((e = rs.next()) != null) {
                if (!e.isRecord()) { continue; }
                if (stopRequest != null) {
                    return new StageResult(stopRequest, 0, null);
                }
                int part = partitioner.partitionOf(e.row());
                sinks.get(part).send(StreamEvent.record(e.row()));
            }
            if (!claimTerminal()) {
                return new StageResult(stopRequest, 0, null);
            }
            for (io.rowweave.sched.Channel s : sinks) {
                closeOwnedChannel(s);
            }
            return new StageResult(StopReason.EOS, 0, null);
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            failOwnedChannelsReverse(sinks, t);
            throw t;
        } finally {
            cleanupOwner(sourceOwner, primary);
        }
    }

    private StageResult runSourceStage() {
        DataSchema outMeta = cmd.joinOutMeta();
        PlanGraph fragment = cmd.fragment();
        boolean positioned = false;
        for (int i = 0; i < fragment.nodeCount(); i++) {
            PlanNode node = fragment.node(i);
            if (io.rowweave.plan.SourcePositionPlanValidator.isPositionConsumer(node)) {
                positioned = true;
                break;
            }
        }
        final io.rowweave.sched.stream.RowStream.CloseableRowStream src;
        if (positioned) {
            if (!(cmd.input() instanceof io.rowweave.AssignedSourceBatch)) {
                throw new IllegalArgumentException(
                        "QUALITY_SOURCE_POSITION_INVALID: assigned exact source batch is required");
            }
            io.rowweave.AssignedSourceBatch assigned =
                    (io.rowweave.AssignedSourceBatch) cmd.input();
            int sourceIndex = fragment.singleSourceIndex();
            fragment = fragment.replaceSourceBindings(sourceIndex, assigned.bindings());
            io.rowweave.plan.SourcePositionPlanValidator.validate(fragment);
            io.rowweave.plan.TypedRowClassificationPlanValidator
                    .validateWorkerFragment(fragment);
            io.rowweave.plan.ObjectStorageQualityPlanValidator
                    .validateWorkerFragment(fragment);
            io.rowweave.plan.TypedQualityPublicationPlanValidator
                    .validateWorkerFragment(fragment);
            src = io.rowweave.sched.stream.RowStream.fromSourceBindings(
                    fragment.node(sourceIndex).param(PlanNode.OPERATOR_UID_PARAM),
                    assigned.bindings());
        } else {
            // Read through SourceExecutor, which also closes it, and stop() closes it too on cancellation.
            src = io.rowweave.sched.stream.RowStream.fromSource(cmd.input());
        }
        CancelOwner sourceOwner = own(cause -> src.close());
        FanoutSink fanout = null;
        Throwable primary = null;
        try {
            fanout = openFanout(cmd, outMeta); this.currentFanout = fanout;
            RowStream inStream = markingStream(src);  // mark the first row to arrive
            java.util.Map<Integer, RowStream> bindings = Collections.singletonMap(0, inStream);
            new PushDagExecutor(1).executeLinearBoundReported(
                    fragment, bindings, fanout.terminal("source-stage"), telemetryAccumulator);
            fanout.close();
            return new StageResult(StopReason.EOS, fanout.emitted(), null);  // report the rows this stage actually produced
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            final FanoutSink failedFanout = fanout;
            cleanupOwned(t,
                    this::closeBlockingMem,
                    () -> { if (failedFanout != null) failedFanout.fail(t); });
            throw t;
        } finally {
            final Throwable terminalPrimary = primary;
            cleanupOwned(primary, this::closeBlockingMem,
                    () -> cleanupOwner(sourceOwner, terminalPrimary));
        }
    }

    /**
     * Close the memory behind a blocking materialised buffer, which deletes its spill directory; a no-op
     * when no edge was blocking. Reached on all three paths, success, failure and cancellation.
     */
    private void closeBlockingMem() {
        io.rowweave.mem.ExecutionMemory m = blockingMem;
        if (m != null) {
            blockingMem = null;
            CancelOwner owner = blockingMemOwner;
            blockingMemOwner = null;
            if (owner == null) {
                closeExecutionMemory(m);
            } else {
                cleanupOwner(owner, null);
            }
        }
    }

    /**
     * Open the inbound of a role that listens, which may mix sockets and local channels. The socket count
     * is inboundConnections minus localInbound. When some producers are local, the shared channel is
     * registered with this worker's registry first, so a downstream that builds ahead of its upstream is
     * waiting for it, and an upstream on the same worker finds it by awaiting. Only when there is a socket
     * producer does the stage bind and report a port for that edge. With no local shuffle injected, or no
     * local producer, this is a plain socket inbound and behaves as it always did.
     */
    private MultiSocketChannelSource openInbound(int totalConns, int localConns, String localKey, DataSchema meta, int edge) {
        int socketConns = totalConns - localConns;
        java.util.UUID muxTarget = (edge == 0) ? cmd.inboundTargetId() : cmd.inboundTargetIdRight();
        // Muxing: when a single-port registry was injected and this edge has a routing key, the downstream does
        // not bind a ServerSocket of its own. It builds a merged channel with no socket server behind it, where
        // the latch count still carries the number of socket producers, and registers the shared channel with
        // the worker's registry under that key. The server's demultiplexing handler then routes offers and
        // producer completions to it. No port is reported, because the master already knows the endpoint: the
        // worker's data port together with the key it assigned.
        if (dataInbound != null && muxTarget != null && socketConns > 0) {
            // The latch counts socket producers plus local producers on the same worker; the socket side binds nothing.
            MultiSocketChannelSource inbound = ownChannel(MultiSocketChannelSource.muxInbound(
                    meta, totalConns, cmd.window(), ACCEPT_TIMEOUT_MS, KEEPALIVE_MS, SO_TIMEOUT_MS));
            CancelOwner localRegistration = null;
            CancelOwner dataRegistration = null;
            try {
                if (localConns > 0 && localShuffle != null && localKey != null) {
                    localShuffle.register(localKey, inbound.sharedChannel());
                    localRegistration = own(cause ->
                            localShuffle.cancel(localKey, registryCancellation(cause)));
                }
                dataInbound.register(muxTarget, muxJobId, inbound.sharedChannel(), meta,
                        socketConns, cmd.window());  // the server demuxes to this channel by key and consumes its credit budget
                dataRegistration = own(cause ->
                        dataInbound.cancel(muxTarget, registryCancellation(cause)));
                return inbound;
            } catch (Throwable setupFailure) {
                claimFailure();
                cleanupOwner(dataRegistration, setupFailure);
                cleanupOwner(localRegistration, setupFailure);
                failOwnedChannel(inbound, setupFailure);
                throw setupFailure;
            }
        }
        // The unshared path: each stage binds its own ServerSocket and reports the port it got, unchanged when
        // muxing is off.
        MultiSocketChannelSource inbound = ownChannel(new MultiSocketChannelSource(
                meta, socketConns, localConns, cmd.window(), ACCEPT_TIMEOUT_MS, KEEPALIVE_MS, SO_TIMEOUT_MS));
        CancelOwner localRegistration = null;
        try {
            if (localConns > 0 && localShuffle != null && localKey != null) {
                localShuffle.register(localKey, inbound.sharedChannel());  // a downstream that registered first wakes the same-worker upstream now awaiting
                localRegistration = own(cause ->
                        localShuffle.cancel(localKey, registryCancellation(cause)));
            }
            if (socketConns > 0) {
                onPort.onPort(inbound.port(), edge);  // a port is reported only when there is a socket producer upstream
            }
            return inbound;
        } catch (Throwable setupFailure) {
            claimFailure();
            cleanupOwner(localRegistration, setupFailure);
            failOwnedChannel(inbound, setupFailure);
            throw setupFailure;
        }
    }

    private static RuntimeException registryCancellation(Throwable primary) {
        return primary instanceof RuntimeException
                ? (RuntimeException) primary
                : new RuntimeException("stage inbound setup failed", primary);
    }

    private StageResult runKeyedOp() {
        io.rowweave.plan.ObjectStorageQualityPlanValidator
                .validateWorkerFragment(cmd.fragment());
        io.rowweave.plan.TypedRowClassificationPlanValidator
                .validateWorkerFragment(cmd.fragment());
        io.rowweave.plan.TypedQualityPublicationPlanValidator
                .validateWorkerFragment(cmd.fragment());
        MultiSocketChannelSource inbound =
                openInbound(cmd.inboundConnections(), cmd.localInbound(), cmd.localInboundKey(), cmd.dataSchema(), 0);
        FanoutSink fanout = null;
        Throwable primary = null;
        try {
            DataSchema outMeta = cmd.downstream() != null ? cmd.joinOutMeta() : cmd.fragment().root().outMeta();
            fanout = openFanout(cmd, outMeta);
            this.currentFanout = fanout;
            java.util.Map<Integer, RowStream> bindings = Collections.singletonMap(0, channelStream(inbound));
            new PushDagExecutor(1).executeLinearBoundReported(
                    cmd.fragment(), bindings, fanout.terminal("keyed-op"), telemetryAccumulator);
            fanout.close();
            return new StageResult(StopReason.EOS, fanout.emitted(), null);  // report the rows this stage actually produced
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            final FanoutSink failedFanout = fanout;
            cleanupOwned(t,
                    this::closeBlockingMem,
                    () -> { if (failedFanout != null) failedFanout.fail(t); },
                    () -> failOwnedChannel(inbound, t));
            throw t;
        } finally {
            final Throwable terminalPrimary = primary;
            cleanupOwned(primary, this::closeBlockingMem,
                    () -> { if (terminalPrimary == null) closeOwnedChannel(inbound); });
        }
    }

    private StageResult runSink() {
        io.rowweave.plan.ObjectStorageQualityPlanValidator
                .validateWorkerFragment(cmd.sinkFragment());
        io.rowweave.plan.TypedRowClassificationPlanValidator
                .validateWorkerFragment(cmd.sinkFragment());
        io.rowweave.plan.TypedQualityPublicationPlanValidator
                .validateWorkerFragment(cmd.sinkFragment());
        io.rowweave.plan.TypedQualityPublicationPlanValidator
                .validateWorkerSinkFragment(cmd.sinkFragment(),
                        cmd.providerOutputProxy() == null
                                ? cmd.output() : cmd.providerOutputProxy());
        MultiSocketChannelSource inbound =
                openInbound(cmd.inboundConnections(), cmd.localInbound(), cmd.localInboundKey(), cmd.dataSchema(), 0);
        Sink output = cmd.output();
        long[] count = {0}; this.sinkCount = count;
        AtomicBoolean outputAbortStarted = new AtomicBoolean();
        Object outputTerminalLock = new Object();
        CancelOwner outputOwner = own(cause -> {
            synchronized (outputTerminalLock) {
                if (outputAbortStarted.compareAndSet(false, true)) {
                    if (cmd.commitGroupId() == null) output.abort(); else output.rollbackCommit();
                }
            }
        });
        Throwable primary = null;
        try {
            if (output instanceof io.rowweave.ManifestFileSink) {
                ((io.rowweave.ManifestFileSink) output).bindManifestAttempt(
                        cmd.manifestAttemptId(), cmd.manifestGeneration());
            }
            Sink terminalOutput = cmd.commitGroupId() == null ? output : preparingView(output);
            Sink counting = countingWrap(terminalOutput, count, outputAbortStarted);
            PipelineExecutor.Stage terminal = PipelineExecutor.outputStage(
                    terminalLifecycleView(counting, outputOwner,
                            cmd.commitGroupId() != null, outputTerminalLock));
            java.util.Map<Integer, RowStream> bindings = Collections.singletonMap(0, channelStream(inbound));
            new PushDagExecutor(1).executeLinearBoundReported(
                    cmd.sinkFragment(), bindings, terminal, telemetryAccumulator);
            return new StageResult(StopReason.EOS, count[0], null);
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            cleanupOwner(outputOwner, t);
            failOwnedChannel(inbound, t);
            throw t;
        } finally {
            if (primary == null) closeOwnedChannel(inbound);
        }
    }

    private StageResult runJoinConsumer() {
        MultiSocketChannelSource leftIn =
                openInbound(cmd.inboundConnections(), cmd.localInbound(), cmd.localInboundKey(), cmd.dataSchema(), 0);
        MultiSocketChannelSource rightIn = null;
        io.rowweave.mem.ExecutionMemory mem = null;
        CancelOwner memoryOwner = null;
        FanoutSink fanout = null;
        Throwable primary = null;
        try {
            rightIn = openInbound(cmd.rightInboundConnections(), cmd.localInboundRight(),
                    cmd.localInboundKeyRight(), cmd.rightMeta(), 1);
            final io.rowweave.mem.ExecutionMemory executionMemory =
                    setupFactory.executionMemory(true,
                            io.rowweave.mem.MemoryOwnerRequirement.partitioned(2));
            mem = executionMemory;
            memoryOwner = own(cause -> closeExecutionMemory(executionMemory));
            fanout = openFanout(cmd, cmd.joinOutMeta());
            this.currentFanout = fanout;
            io.rowweave.sched.stream.RowSink sinkOut = fanout.rowSink("binary");
            PipelineExecutor.BinaryStage consumer;
            if (cmd.coGroupFn() != null) {
                consumer = PipelineExecutor.managedBinaryBreaker(executionMemory, joinMM -> {
                    // Partitioning stays scale sensitive, and the CoGroup working set is built only once the first event on
                    // the other side arrives.
                    io.rowweave.ops.join.CoGroup cg = io.rowweave.ops.join.CoGroup.of(
                            cmd.dataSchema(), cmd.rightMeta(), cmd.keyFields(), cmd.rightKeyFields(), cmd.coGroupFn());
                    cg.withSpill(joinMM, executionMemory.newSpillStore(), executionMemory.segmentPool())
                            .withEqualKeyBlockThreshold(EQ_BLOCK_THRESHOLD);
                    return cg::coGroup;
                });
            } else {
                int probeRows = Math.min(Integer.getInteger("rowweave.join.probeRows", 4096),
                        Math.max(1, cmd.window() / 2));
                consumer = io.rowweave.ops.join.JoinRuntimeFactory.managed(
                        executionMemory, cmd.dataSchema(), cmd.rightMeta(), cmd.keyFields(), cmd.rightKeyFields(),
                        io.rowweave.ops.join.JoinType.valueOf(cmd.joinType()), cmd.joinStrategy(),
                        probeRows, EQ_BLOCK_THRESHOLD);
            }
            consumer.run(channelStream(leftIn), channelStream(rightIn), sinkOut);
            fanout.close();
            cleanupOwner(memoryOwner, null);
            mem = null;
            return new StageResult(StopReason.EOS, fanout.emitted(), null);  // report the rows this join produced
        } catch (Throwable t) {
            claimFailure();
            primary = t;
            final FanoutSink failedFanout = fanout;
            final MultiSocketChannelSource failedRight = rightIn;
            final CancelOwner failedMemoryOwner = memoryOwner;
            mem = null;
            cleanupOwned(t,
                    this::closeBlockingMem,
                    () -> { if (failedFanout != null) failedFanout.fail(t); },
                    () -> cleanupOwner(failedMemoryOwner, t),
                    () -> { if (failedRight != null) failOwnedChannel(failedRight, t); },
                    () -> failOwnedChannel(leftIn, t));
            throw t;
        } finally {
            final MultiSocketChannelSource ownedRight = rightIn;
            final CancelOwner ownedMemory = memoryOwner;
            final Throwable terminalPrimary = primary;
            cleanupOwned(primary,
                    this::closeBlockingMem,
                    () -> cleanupOwner(ownedMemory, terminalPrimary),
                    () -> { if (ownedRight != null && terminalPrimary == null) closeOwnedChannel(ownedRight); },
                    () -> { if (terminalPrimary == null) closeOwnedChannel(leftIn); });
        }
    }

    private StageResult runWholeGraph() {
        io.rowweave.plan.PlanGraph graph = cmd.fragment();
        Thread ownerThread = Thread.currentThread();
        CancelOwner interruptOwner = own(cause -> ownerThread.interrupt());
        try {
            io.rowweave.plan.ExecutionReport report =
                    new PushDagExecutor(1).runWholeGraphReported(
                            graph, telemetryAccumulator, this::claimTerminal);
            return new StageResult(StopReason.EOS, report.rowsOut(), null);
        } catch (Throwable failure) {
            claimFailure();
            throw failure;
        } finally {
            interruptOwner.retire();
        }
    }

    private void captureExecutionMemory(io.rowweave.mem.ExecutionMemory memory) {
        StageExecutionTelemetryAccumulator target = telemetryAccumulator;
        if (target == null) return;
        try {
            io.rowweave.mem.ExecutionMemorySnapshot snapshot = memory.telemetrySnapshot();
            target.add(snapshot.complete()
                    ? StageExecutionTelemetry.complete(snapshot.peakReservedBytes(),
                    snapshot.acquiredBytes(), snapshot.pressureSpillCount(),
                    snapshot.spillRunCount(), snapshot.spillLogicalBytesWritten(),
                    snapshot.spillPeakReservedBytes())
                    : StageExecutionTelemetry.unavailable());
        } catch (Throwable ignoredTelemetryFailure) {
            target.add(StageExecutionTelemetry.unavailable());
        }
    }

    private void closeExecutionMemory(io.rowweave.mem.ExecutionMemory memory) {
        if (memory == null) return;
        try {
            memory.close();
        } finally {
            captureExecutionMemory(memory);
        }
    }

    // ---- the fan-out sink groups, one per downstream stage. The sinks are a heterogeneous list of
    // channels, socket or local, so closing and failing walk the whole list, while the partitioning
    // rule for choosing a target is unchanged.

    /**
     * Where one downstream stage's rows go: a sink for each of its instances plus the partitioner that
     * chooses between them. A stage with one downstream has a single group, as before; a stage whose
     * output is shared has one group per downstream, so a row goes to each group, partitioned inside
     * each one.
     */
    private static final class FanoutGroup {
        final List<io.rowweave.sched.Channel> sinks;
        final Partitioner partitioner;  // null means send everything to the single instance
        final StreamEvent[][] localBatches;
        final int[] localBatchCounts;
        FanoutGroup(List<io.rowweave.sched.Channel> sinks, Partitioner partitioner) {
            this.sinks = sinks;
            this.partitioner = partitioner;
            this.localBatches = new StreamEvent[sinks.size()][];
            this.localBatchCounts = new int[sinks.size()];
        }
        void send(StreamEvent e, boolean copy) {
            int part = partitioner == null ? 0 : partitioner.partitionOf(e.row());
            // Broadcasting to several groups, every group but the last is handed a typed snapshot, because a DataRow
            // and the mutable time values it carries must not be aliased between consumers.
            StreamEvent out = copy ? StreamEvent.record(RowSnapshot.copyOf(e.row())) : e;
            io.rowweave.sched.Channel sink = sinks.get(part);
            if (!(sink instanceof io.rowweave.sched.LocalChannelSink)
                    || !((io.rowweave.sched.LocalChannelSink) sink).supportsRecordBatch()) {
                sink.send(out);
                return;
            }
            StreamEvent[] batch = localBatches[part];
            if (batch == null) {
                batch = new StreamEvent[LOCAL_SINGLE_PRODUCER_BATCH_SIZE];
                localBatches[part] = batch;
            }
            int count = localBatchCounts[part];
            batch[count++] = out;
            if (count == LOCAL_SINGLE_PRODUCER_BATCH_SIZE) {
                ((io.rowweave.sched.LocalChannelSink) sink).sendRecords(batch, count);
                java.util.Arrays.fill(batch, 0, count, null);
                count = 0;
            }
            localBatchCounts[part] = count;
        }
        void flush() {
            for (int part = 0; part < sinks.size(); part++) {
                int count = localBatchCounts[part];
                if (count == 0) continue;
                ((io.rowweave.sched.LocalChannelSink) sinks.get(part))
                        .sendRecords(localBatches[part], count);
                java.util.Arrays.fill(localBatches[part], 0, count, null);
                localBatchCounts[part] = 0;
            }
        }
    }

    private final class FanoutSink {
        // One group is the ordinary case, one row to one partition with no copy; several groups is a broadcast.
        private final List<FanoutGroup> groups;
        // Used only on the exception and cancellation paths. Resources are recorded in the order they were
        // really acquired and released in strictly reverse order, so a cleanup failure cannot mask the primary.
        private final List<io.rowweave.sched.Channel> ownershipOrder;
        // A blocking edge. When set, the fan-out materialises and then replays each group concurrently on a
        // thread of its own, which is what breaks the ring a fan-out that re-joins would form. Unset means
        // pipelined, streaming the broadcast as it always did. The fork's output is drained into the buffer
        // first, which writes each row out as it fills and therefore never waits on a downstream; then each
        // group pushes from its own cursor.
        private final io.rowweave.sched.MaterializedRowBuffer blockingBuffer;
        private String blockingLabel;
        // The terminal path already replayed inside blockingTerminal; only a binary's direct RowSink writes need
        // close() to trigger the replay.
        private boolean blockingDirectWrite;
        // The rows this stage produced, counted as the records read into the fan-out rather than the sends they
        // caused, so a broadcast to several groups counts a row once, matching the local path.
        private volatile long emitted = 0;

        FanoutSink(List<FanoutGroup> groups, List<io.rowweave.sched.Channel> ownershipOrder) {
            this(groups, null, ownershipOrder);
        }

        FanoutSink(List<FanoutGroup> groups,
                   io.rowweave.sched.MaterializedRowBuffer blockingBuffer,
                   List<io.rowweave.sched.Channel> ownershipOrder) {
            this.groups = groups;
            this.blockingBuffer = blockingBuffer;
            this.ownershipOrder = new ArrayList<>(ownershipOrder);
        }

        boolean isBlocking() { return blockingBuffer != null; }

        long emitted() { return emitted; }

        private void broadcast(StreamEvent e) {
            // Each group computes its own partition and sends; with several groups, every one but the last gets a
            // copy. A single group sends without copying.
            int last = groups.size() - 1;
            for (int g = 0; g < groups.size(); g++) {
                groups.get(g).send(e, g != last);
            }
            emitted++;
        }

        PipelineExecutor.Stage terminal(String stageName) {
            if (isBlocking()) {
                return blockingTerminal(stageName);
            }
            return (in, ignored) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        broadcast(e);
                    } else if (e.isError()) {
                        throw new RuntimeException(stageName + " upstream error", e.cause());
                    }
                }
            };
        }

        /**
         * The blocking terminal, which breaks the ring a fan-out re-join forms. First the fork's output is
         * drained once into a {@link io.rowweave.sched.MaterializedRowBuffer}, and filling writes each row out
         * as it arrives, so it is never held up by any downstream; that severs the edge where a live producer
         * is stalled because only one side of a re-join is draining. Then every subpartition is replayed
         * concurrently on its own thread, each with its own cursor and its group's partitioner, and each closes
         * and sends its own EOS when it is done. If a group parks because its consumer is not draining, only
         * that thread parks, so no cycle of waiting exists and the argument holds whatever the consumer's drain
         * order is.
         */
        PipelineExecutor.Stage blockingTerminal(String stageName) {
            return (in, ignored) -> {
                // Materialise, then replay each group concurrently on its own thread, which decouples progress from
                // drain order.
                List<io.rowweave.sched.BlockingFanout.Group> bgroups = new ArrayList<>(groups.size());
                for (FanoutGroup g : groups) {
                    bgroups.add(blockingGroup(g));
                }
                emitted = io.rowweave.sched.BlockingFanout.materializeAndReplay(
                        in, blockingBuffer, bgroups, stageName, StageTask.this::claimTerminal);
            };
        }

        io.rowweave.sched.stream.RowSink rowSink(String stageName) {
            if (isBlocking()) {
                // A binary can produce no rows at all, and even then close() must replay the empty partitions and send
                // EOS down every consumer edge.
                blockingLabel = stageName;
                blockingDirectWrite = true;
            }
            return event -> {
                if (event.isRecord()) {
                    if (isBlocking()) {
                        // A binary breaker writes through the RowSink, so its output has to go through the blocking partition
                        // rather than past it; otherwise no replay would send the per-group EOS and an in-order consumer would
                        // re-form the backpressure ring.
                        blockingBuffer.append(event.row());
                        emitted++;
                    } else {
                        broadcast(event);
                    }
                } else if (event.isError()) {
                    throw new RuntimeException(stageName + " error before shuffle send", event.cause());
                }
            };
        }

        void close() {
            if (isBlocking()) {
                if (!blockingDirectWrite) {
                    return;  // terminal() already ran materializeAndReplay, and each replay group closes on its own
                }
                blockingBuffer.finish();
                List<io.rowweave.sched.BlockingFanout.Group> bgroups = new ArrayList<>(groups.size());
                for (FanoutGroup g : groups) {
                    bgroups.add(blockingGroup(g));
                }
                emitted = io.rowweave.sched.BlockingFanout.replayFilled(
                        blockingBuffer, bgroups,
                        blockingLabel == null ? "blocking-fanout" : blockingLabel,
                        emitted, StageTask.this::claimTerminal);
                return;
            }
            // The flush may block on downstream backpressure and must happen before the terminal state is
            // linearised; a concurrent cancellation poisons the channel to wake it.
            for (FanoutGroup g : groups) {
                g.flush();
            }
            if (!claimTerminal()) {
                throw cancellationException();
            }
            for (FanoutGroup g : groups) {
                for (io.rowweave.sched.Channel s : g.sinks) {
                    closeOwnedChannel(s);
                }
            }
        }

        void fail(Throwable t) {
            failOwnedChannelsReverse(ownershipOrder, t);
        }

        private io.rowweave.sched.BlockingFanout.Group blockingGroup(FanoutGroup group) {
            return new io.rowweave.sched.BlockingFanout.Group(
                    group.sinks, group.partitioner,
                    () -> closeOwnedChannels(group.sinks),
                    () -> closeOwnedChannelsWithRunningPermit(group.sinks),
                    failure -> failOwnedChannelsReverse(group.sinks, failure));
        }
    }

    /**
     * Build one fan-out target sink. A socket target is a {@code SocketChannelSink}; a local target takes the
     * shared {@code MultiProducerChannel} the downstream registered ahead of it with this worker's
     * {@link io.rowweave.sched.LocalShuffleRegistry}, wrapped as a {@code LocalChannelSink}, and the await is
     * bounded so a target that never registers cannot hang the stage. With no local registry injected, on
     * the unshared path, a local target should never be named.
     */
    private io.rowweave.sched.Channel openTarget(ShuffleTarget t, DataSchema outMeta) {
        if (t.isLocal()) {
            if (localShuffle == null) {
                throw new IllegalStateException("local shuffle target but no LocalShuffleRegistry injected: " + t.localKey());
            }
            io.rowweave.sched.MultiProducerChannel shared =
                    (io.rowweave.sched.MultiProducerChannel) localShuffle.await(t.localKey(), ACCEPT_TIMEOUT_MS);
            return new io.rowweave.sched.LocalChannelSink(shared, cmd.objectReuse());
        }
        return openEndpoint(t.endpoint(), outMeta);
    }

    /**
     * A muxed endpoint goes through the worker's connection pool; an endpoint with no routing key keeps its
     * own dedicated connection as before.
     */
    private io.rowweave.sched.Channel openEndpoint(ShuffleEndpoint ep, DataSchema outMeta) {
        return setupFactory.endpoint(ep, outMeta, () -> openProductionEndpoint(ep, outMeta));
    }

    private io.rowweave.sched.Channel openProductionEndpoint(ShuffleEndpoint ep, DataSchema outMeta) {
        // A muxed endpoint connects to the worker's single data port and frames the routing key with every
        // request, and the server demultiplexes by it. An endpoint with no key connects to the port its stage
        // bound and carries no key.
        if (muxConnections != null
                && !java.util.Objects.equals(ep.targetId(), io.rowweave.sched.net.Frame.NO_TARGET)) {
            return muxConnections.acquire(outMeta, ep, cmd.window(),
                    KEEPALIVE_MS, SO_TIMEOUT_MS);
        }
        return new SocketChannelSink(outMeta, ep.host(), ep.port(), cmd.window(), KEEPALIVE_MS, SO_TIMEOUT_MS);
    }

    /**
     * A mixed fan-out opens every socket outbound first and only then waits for the local rendezvous on the
     * same worker. The local await blocks, so opening in declaration order would let a target that has not
     * registered yet hold up the socket connects queued behind it, and the remote consumer would then see
     * its accept guard expire before its producer had even connected.
     *
     * <p>The matrix returned is indexed exactly as the groups and their targets were given, so partitioning
     * and broadcast semantics are unchanged.
     */
    private static final class OpenedTargetGroups {
        final List<List<io.rowweave.sched.Channel>> groups;
        final List<io.rowweave.sched.Channel> acquisitionOrder;

        OpenedTargetGroups(List<List<io.rowweave.sched.Channel>> groups,
                           List<io.rowweave.sched.Channel> acquisitionOrder) {
            this.groups = groups;
            this.acquisitionOrder = acquisitionOrder;
        }
    }

    private OpenedTargetGroups openTargetGroupsSocketFirst(
            List<List<ShuffleTarget>> targetGroups, DataSchema outMeta) {
        List<List<io.rowweave.sched.Channel>> opened = new ArrayList<>(targetGroups.size());
        List<io.rowweave.sched.Channel> acquisitionOrder = new ArrayList<>();
        for (List<ShuffleTarget> targets : targetGroups) {
            opened.add(new ArrayList<>(Collections.nCopies(targets.size(), null)));
        }
        try {
            openTargetPass(targetGroups, opened, acquisitionOrder, outMeta, false);
            openTargetPass(targetGroups, opened, acquisitionOrder, outMeta, true);
            return new OpenedTargetGroups(opened, acquisitionOrder);
        } catch (RuntimeException | Error failure) {
            failOwnedChannelsReverse(acquisitionOrder, failure);
            throw failure;
        }
    }

    private void openTargetPass(List<List<ShuffleTarget>> targetGroups,
                                List<List<io.rowweave.sched.Channel>> opened,
                                List<io.rowweave.sched.Channel> acquisitionOrder,
                                DataSchema outMeta,
                                boolean local) {
        for (int group = 0; group < targetGroups.size(); group++) {
            List<ShuffleTarget> targets = targetGroups.get(group);
            for (int target = 0; target < targets.size(); target++) {
                ShuffleTarget descriptor = targets.get(target);
                if (descriptor.isLocal() == local) {
                    io.rowweave.sched.Channel channel = ownChannel(openTarget(descriptor, outMeta));
                    opened.get(group).set(target, channel);
                    acquisitionOrder.add(channel);
                }
            }
        }
    }

    private FanoutSink openFanout(StageCommand cmd, DataSchema outMeta) {
        List<io.rowweave.sched.Channel> ownedChannels = new ArrayList<>();
        io.rowweave.mem.ExecutionMemory ownedBlockingMemory = null;
        try {
            // One group per downstream stage whose output is shared; a row is broadcast to all of them.
            java.util.List<StageCommand.BroadcastGroup> bgroups = cmd.broadcastGroups();
            if (bgroups != null && bgroups.size() > 1) {
                List<FanoutGroup> groups = new ArrayList<>(bgroups.size());
                List<List<ShuffleTarget>> targetGroups = new ArrayList<>(bgroups.size());
                for (StageCommand.BroadcastGroup bg : bgroups) {
                    targetGroups.add(bg.targets);
                }
                OpenedTargetGroups opened = openTargetGroupsSocketFirst(targetGroups, outMeta);
                ownedChannels.addAll(opened.acquisitionOrder);
                // If any group is blocking the whole fan-out is, so the result is either materialised and replayed or
                // pipelined: a half-live, half-materialised fan-out would let the live side starve the shared producer.
                boolean anyBlocking = false;
                for (int group = 0; group < bgroups.size(); group++) {
                    StageCommand.BroadcastGroup bg = bgroups.get(group);
                    int n = bg.targets.size();
                    List<io.rowweave.sched.Channel> sinks = opened.groups.get(group);
                    Partitioner p = n == 1 ? null
                            : downstreamPartitioner(
                                    outMeta, n, bg.downKeys, bg.normalized,
                                    bg.fixedPartition());
                    groups.add(new FanoutGroup(sinks, p));
                    anyBlocking |= bg.blocking;
                }
                if (anyBlocking) {
                    // The backing store for a blocking materialisation is disk only and needs a workspace, so it takes no
                    // guaranteed share of managed memory.
                    ownedBlockingMemory = setupFactory.executionMemory(true);
                    this.blockingMem = ownedBlockingMemory;  // closed by the run method's finally block, which deletes the spill directory
                    final io.rowweave.mem.ExecutionMemory memoryOwner = ownedBlockingMemory;
                    CancelOwner registeredMemory = new CancelOwner(
                            cause -> closeExecutionMemory(memoryOwner));
                    this.blockingMemOwner = registeredMemory;
                    registerOwner(registeredMemory);
                    io.rowweave.sched.MaterializedRowBuffer buf =
                            setupFactory.materializedBuffer(
                                    outMeta, ownedBlockingMemory.newSpillStore());
                    return new FanoutSink(groups, buf, ownedChannels);
                }
                return new FanoutSink(groups, ownedChannels);
            }
            // Per-partition targets are preferred when present, since they can mix sockets and local channels; other
            // wise the plain socket endpoint list is used.
            java.util.List<ShuffleTarget> targets = cmd.downTargets();
            if (targets != null) {
                int n = targets.size();
                OpenedTargetGroups opened = openTargetGroupsSocketFirst(
                        Collections.singletonList(targets), outMeta);
                ownedChannels.addAll(opened.acquisitionOrder);
                List<io.rowweave.sched.Channel> sinks = opened.groups.get(0);
                Partitioner partitioner = n == 1 ? null
                        : downstreamPartitioner(
                                outMeta, n, cmd.downKeys(),
                                cmd.downNormalized(),
                                cmd.downFixedPartition());
                return new FanoutSink(
                        Collections.singletonList(new FanoutGroup(sinks, partitioner)),
                        ownedChannels);
            }
            List<ShuffleEndpoint> down = cmd.downstream();
            if (down == null) {
                ShuffleEndpoint se = cmd.sinkEndpoint();
                List<io.rowweave.sched.Channel> sinks = new ArrayList<>(1);
                io.rowweave.sched.Channel channel = ownChannel(openEndpoint(se, outMeta));
                sinks.add(channel);
                ownedChannels.add(channel);
                return new FanoutSink(
                        Collections.singletonList(new FanoutGroup(sinks, null)),
                        ownedChannels);
            }
            int n = down.size();
            List<io.rowweave.sched.Channel> sinks = new ArrayList<>(n);
            for (ShuffleEndpoint ep : down) {
                io.rowweave.sched.Channel channel = ownChannel(openEndpoint(ep, outMeta));
                sinks.add(channel);
                ownedChannels.add(channel);
            }
            Partitioner partitioner = n == 1 ? null
                    : downstreamPartitioner(
                            outMeta, n, cmd.downKeys(),
                            cmd.downNormalized(),
                            cmd.downFixedPartition());
            return new FanoutSink(
                    Collections.singletonList(new FanoutGroup(sinks, partitioner)),
                    ownedChannels);
        } catch (Throwable setupFailure) {
            if (ownedBlockingMemory != null) {
                try {
                    closeBlockingMem();
                } catch (Throwable cleanupFailure) {
                    appendSuppressed(setupFailure, cleanupFailure);
                }
            }
            failOwnedChannelsReverse(ownedChannels, setupFailure);
            throw setupFailure;
        }
    }

    private Partitioner downstreamPartitioner(
            DataSchema meta, int partitions, String[] keys,
            boolean normalized, int fixedPartition) {
        return fixedPartition >= 0
                ? Partitioner.fixed(fixedPartition, partitions)
                : setupFactory.partitioner(
                        meta, partitions, keys, normalized);
    }

    private void failOwnedChannelsReverse(List<io.rowweave.sched.Channel> channels, Throwable primary) {
        for (int i = channels.size() - 1; i >= 0; i--) {
            failOwnedChannel(channels.get(i), primary);
        }
    }

    private void closeOwnedChannels(List<io.rowweave.sched.Channel> channels) {
        Throwable failure = null;
        for (io.rowweave.sched.Channel channel : channels) {
            try {
                closeOwnedChannel(channel);
            } catch (Throwable closeFailure) {
                if (failure == null) failure = closeFailure;
                else appendSuppressed(failure, closeFailure);
            }
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) {
            throw new IllegalStateException("unexpected checked channel close failure", failure);
        }
    }

    /**
     * A blocking fan-out does not freeze the whole stage when the group is not the last one, but its EOS
     * permit still contends with cancellation on the same linearising lock. Once the permit is claimed the
     * close really happens; if cancellation wins first, every owner is still the stop path's to release and
     * a late EOS is not allowed.
     */
    private void closeOwnedChannelsWithRunningPermit(
            List<io.rowweave.sched.Channel> channels) {
        synchronized (startStopLock) {
            if (lifecycle != Lifecycle.RUNNING) {
                throw cancellationException();
            }
            for (io.rowweave.sched.Channel channel : channels) {
                CancelOwner owner = channelOwners.get(channel);
                if (owner == null || owner.isTerminal()) {
                    throw new IllegalStateException(
                            "blocking fanout channel has no active owner at terminal permit");
                }
            }
        }
        // Owners deliberately remain cancellable while a transport performs graceful close.
        // Socket close may wait for its peer; stop() must still be able to fail the channel and
        // unblock that wait after the EOS permit has won.
        closeChannelsAfterPermit(channels);
        synchronized (startStopLock) {
            if (lifecycle != Lifecycle.RUNNING) {
                throw cancellationException();
            }
            for (io.rowweave.sched.Channel channel : channels) {
                if (!channelOwners.get(channel).retire()) {
                    throw new IllegalStateException(
                            "blocking fanout channel owner changed during terminal permit");
                }
            }
        }
    }

    private static void closeChannelsAfterPermit(
            List<io.rowweave.sched.Channel> channels) {
        Throwable failure = null;
        for (io.rowweave.sched.Channel channel : channels) {
            try {
                channel.close();
            } catch (Throwable closeFailure) {
                if (failure == null) failure = closeFailure;
                else appendSuppressed(failure, closeFailure);
            }
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) {
            throw new IllegalStateException("unexpected checked channel close failure", failure);
        }
    }

    private static void appendSuppressed(Throwable primary, Throwable cleanup) {
        if (primary != null && cleanup != null && primary != cleanup
                && !containsThrowable(primary, cleanup)
                && !containsThrowable(cleanup, primary)) {
            primary.addSuppressed(cleanup);
        }
    }

    /**
     * Runs the whole cleanup in the LIFO order given. When there is already a primary failure the cleanup
     * failures are only attached as suppressed; on the success path the first one is raised.
     */
    private static void cleanupOwned(Throwable primary, Runnable... cleanupActions) {
        Throwable observed = primary;
        for (Runnable action : cleanupActions) {
            try {
                action.run();
            } catch (Throwable cleanupFailure) {
                if (observed == null) {
                    observed = cleanupFailure;
                } else {
                    appendSuppressed(observed, cleanupFailure);
                }
            }
        }
        if (primary == null && observed != null) {
            if (observed instanceof RuntimeException) throw (RuntimeException) observed;
            if (observed instanceof Error) throw (Error) observed;
            throw new IllegalStateException("unexpected checked cleanup failure", observed);
        }
    }

    private static Sink countingWrap(Sink delegate, long[] count, AtomicBoolean abortStarted) {
        return new Sink() {
            @Override public DataSchema getSchema() { return delegate.getSchema(); }
            @Override public void collect(DataRow row) { delegate.collect(row); count[0]++; }
            @Override public void close() { delegate.close(); }
            @Override public void abort() {
                if (abortStarted.compareAndSet(false, true)) delegate.abort();
            }
            @Override public boolean supportsCoordinatedCommit() { return delegate.supportsCoordinatedCommit(); }
            @Override public void prepareCommit() { delegate.prepareCommit(); }
            @Override public void commitPrepared() { delegate.commitPrepared(); }
            @Override public void rollbackCommit() { delegate.rollbackCommit(); }
            @Override public void finalizeCommit() { delegate.finalizeCommit(); }
            @Override public boolean isWorkerWritable() { return delegate.isWorkerWritable(); }
            @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
        };
    }

    /**
     * The thread on the far side of a sink edge contends with cancellation before it really closes or
     * prepares. Once it has claimed the terminal state, a cancellation arriving later must not reclassify a
     * result that has already been committed.
     */
    private Sink terminalLifecycleView(
            Sink delegate, CancelOwner outputOwner,
            boolean reversibleClose, Object terminalLock) {
        return new Sink() {
            @Override public DataSchema getSchema() { return delegate.getSchema(); }
            @Override public void collect(DataRow row) {
                synchronized (terminalLock) {
                    if (isCancellationWinner()) {
                        throw cancellationException();
                    }
                    delegate.collect(row);
                }
            }
            @Override public void close() {
                synchronized (terminalLock) {
                    if (reversibleClose) {
                        if (isCancellationWinner()) {
                            RuntimeException cancelled = cancellationException();
                            cleanupOwner(outputOwner, cancelled);
                            throw cancelled;
                        }
                        delegate.close();
                        if (!claimTerminal()) {
                            RuntimeException cancelled = cancellationException();
                            cleanupOwner(outputOwner, cancelled);
                            throw cancelled;
                        }
                    } else {
                        if (!claimTerminal()) {
                            RuntimeException cancelled = cancellationException();
                            cleanupOwner(outputOwner, cancelled);
                            throw cancelled;
                        }
                        delegate.close();
                    }
                    outputOwner.retire();
                }
            }
            @Override public void abort() { delegate.abort(); }
            @Override public boolean supportsCoordinatedCommit() { return delegate.supportsCoordinatedCommit(); }
            @Override public void prepareCommit() { delegate.prepareCommit(); }
            @Override public void commitPrepared() { delegate.commitPrepared(); }
            @Override public void rollbackCommit() { delegate.rollbackCommit(); }
            @Override public void finalizeCommit() { delegate.finalizeCommit(); }
            @Override public boolean isWorkerWritable() { return delegate.isWorkerWritable(); }
            @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
        };
    }

    private static Sink preparingView(Sink delegate) {
        return new Sink() {
            @Override public DataSchema getSchema() { return delegate.getSchema(); }
            @Override public void collect(DataRow row) { delegate.collect(row); }
            @Override public void close() { delegate.prepareCommit(); }
            @Override public void abort() { delegate.rollbackCommit(); }
            @Override public boolean supportsCoordinatedCommit() { return delegate.supportsCoordinatedCommit(); }
            @Override public void prepareCommit() { delegate.prepareCommit(); }
            @Override public void commitPrepared() { delegate.commitPrepared(); }
            @Override public void rollbackCommit() { delegate.rollbackCommit(); }
            @Override public void finalizeCommit() { delegate.finalizeCommit(); }
            @Override public boolean isWorkerWritable() { return delegate.isWorkerWritable(); }
            @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
        };
    }

    private RowStream channelStream(Channel channel) {
        return RowStream.fromChannel(channel, this::onInputRecord);
    }
}
