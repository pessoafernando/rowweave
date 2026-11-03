package io.rowweave;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.ClusterSubmissionPreflight;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.JobClient;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.MonitorBootstrap;
import io.rowweave.monitor.MonitorLauncher;
import io.rowweave.plan.ExecutionReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rowweave.plan.PlanGraph;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Process-wide, lazily started Batch runtime backed by the cluster role implementation. */
final class InProcessBatchRuntime implements JobClient, AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(InProcessBatchRuntime.class);
    private static final Object PROCESS_LOCK = new Object();
    private static final AtomicLong START_COUNT = new AtomicLong();
    private static volatile InProcessBatchRuntime processWide;
    private static volatile JobMonitorRegistry processWideMonitor =
            JobMonitorRegistry.NOOP;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(
                InProcessBatchRuntime::shutdownProcessWide,
                "rowweave-inprocess-batch-shutdown"));
    }

    private final String runtimeId = UUID.randomUUID().toString();
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final AtomicLong completedSubmissions = new AtomicLong();
    private volatile InProcessCluster cluster;
    private volatile MonitorLauncher monitorLauncher;
    private volatile int monitorPort;
    private volatile boolean closed;

    private InProcessBatchRuntime() {
    }

    /**
     * Installs the registry used by the next lazily-created local runtime.
     * A {@code null} value restores the zero-overhead no-op behavior.
     */
    static void setProcessWideMonitor(JobMonitorRegistry monitor) {
        processWideMonitor = monitor == null
                ? JobMonitorRegistry.NOOP : monitor;
    }

    static InProcessBatchRuntime processWide() {
        InProcessBatchRuntime current = processWide;
        if (current != null) {
            return current;
        }
        synchronized (PROCESS_LOCK) {
            if (processWide == null) {
                processWide = new InProcessBatchRuntime();
            }
            return processWide;
        }
    }

    @Override
    public long submitToSink(PlanGraph sinkGraph) {
        return submit(LegacyPlanArtifactAdapter.wrap(
                "inprocess-runtime-legacy", sinkGraph)).rowsOut();
    }

    @Override
    public ExecutionReport submit(PlanGraph fullGraph) {
        return submit(LegacyPlanArtifactAdapter.wrapLocal(
                "inprocess-runtime-legacy", fullGraph));
    }

    @Override
    public ExecutionReport submit(PlanArtifact artifact) {
        return submit(artifact, false);
    }

    ExecutionReport submit(PlanArtifact artifact, boolean canonicalizeOutput) {
        ClusterSubmissionPreflight.validate(artifact);
        lifecycle.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("in-process Batch runtime is closed");
            }
            ExecutionReport report = ensureCluster().submit(
                    artifact, canonicalizeOutput);
            completedSubmissions.incrementAndGet();
            return report;
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private InProcessCluster ensureCluster() {
        InProcessCluster current = cluster;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (cluster == null) {
                int parallelismHint = Integer.getInteger(
                        "rowweave.local.runtime.parallelismHint",
                        Math.max(1, Math.min(16,
                                Runtime.getRuntime().availableProcessors())));
                JobMonitorRegistry registry = processWideMonitor;
                if (registry == JobMonitorRegistry.NOOP
                        && Boolean.getBoolean("rowweave.monitor.enable")) {
                    registry = startMonitorService();
                }
                try {
                    cluster = new InProcessCluster(parallelismHint, registry);
                } catch (RuntimeException | Error failure) {
                    stopMonitorService();
                    throw failure;
                }
                START_COUNT.incrementAndGet();
            }
            return cluster;
        }
    }


    /**
     * Starts the optional HTTP dashboard before execution, using the same
     * ServiceLoader contract as cluster mode. Startup is advisory: a missing
     * or failed launcher leaves execution on the zero-overhead registry.
     */
    private JobMonitorRegistry startMonitorService() {
        MonitorLauncher launcher = MonitorBootstrap.discover();
        if (launcher == null) {
            LOG.warn("rowweave.monitor.enable=true but no MonitorLauncher "
                    + "implementation is on the classpath; monitoring is disabled");
            return JobMonitorRegistry.NOOP;
        }
        try {
            InMemoryJobMonitor registry = new InMemoryJobMonitor(
                    Integer.getInteger("rowweave.monitor.history", 200));
            int port = launcher.start(
                    registry,
                    Integer.getInteger("rowweave.monitor.port", 0),
                    null,
                    Integer.getInteger("rowweave.monitor.ioThreads",
                            Math.max(2,
                                    Runtime.getRuntime().availableProcessors() / 2)));
            this.monitorLauncher = launcher;
            this.monitorPort = port;
            return registry;
        } catch (RuntimeException | LinkageError failure) {
            try {
                launcher.stop();
            } catch (RuntimeException | LinkageError closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            LOG.warn("local monitor HTTP failed to start; monitoring is disabled", failure);
            return JobMonitorRegistry.NOOP;
        }
    }

    private void stopMonitorService() {
        MonitorLauncher launcher = monitorLauncher;
        monitorLauncher = null;
        monitorPort = 0;
        if (launcher != null) {
            try {
                launcher.stop();
            } catch (RuntimeException failure) {
                LOG.debug("local monitor HTTP stop failed", failure);
            }
        }
    }

    static int processWideMonitorPortForTesting() {
        return processWide == null ? 0 : processWide.monitorPort;
    }

    @Override
    public void close() {
        lifecycle.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            InProcessCluster current = cluster;
            cluster = null;
            stopMonitorService();
            if (current != null) {
                current.close();
            }
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    private static void shutdownProcessWide() {
        InProcessBatchRuntime detached;
        synchronized (PROCESS_LOCK) {
            detached = processWide;
            processWide = null;
        }
        if (detached != null) {
            detached.close();
        }
    }

    static void shutdownProcessWideForTesting() {
        shutdownProcessWide();
    }

    static void resetProcessWideForTesting() {
        shutdownProcessWide();
        START_COUNT.set(0L);
    }

    static Snapshot snapshotForTesting() {
        InProcessBatchRuntime current = processWide;
        if (current == null) {
            return new Snapshot(null, false, START_COUNT.get(), 0L,
                    0, 0, 0, 0);
        }
        InProcessCluster active = current.cluster;
        InProcessCluster.Diagnostics diagnostics = active == null
                ? null : active.diagnostics();
        return new Snapshot(current.runtimeId, active != null && !current.closed,
                START_COUNT.get(), current.completedSubmissions.get(),
                diagnostics == null ? 0 : diagnostics.controlListenerCount(),
                diagnostics == null ? 0 : diagnostics.controlAcceptThreadCount(),
                diagnostics == null ? 0 : diagnostics.handlerThreadCount(),
                diagnostics == null ? 0 : diagnostics.dataListenerCount());
    }

    static final class Snapshot {
        private final String runtimeId;
        private final boolean running;
        private final long startCount;
        private final long completedSubmissionCount;
        private final int controlListenerCount;
        private final int controlAcceptThreadCount;
        private final int handlerThreadCount;
        private final int dataListenerCount;

        private Snapshot(String runtimeId, boolean running, long startCount,
                         long completedSubmissionCount, int controlListenerCount,
                         int controlAcceptThreadCount, int handlerThreadCount,
                         int dataListenerCount) {
            this.runtimeId = runtimeId;
            this.running = running;
            this.startCount = startCount;
            this.completedSubmissionCount = completedSubmissionCount;
            this.controlListenerCount = controlListenerCount;
            this.controlAcceptThreadCount = controlAcceptThreadCount;
            this.handlerThreadCount = handlerThreadCount;
            this.dataListenerCount = dataListenerCount;
        }

        String runtimeId() { return runtimeId; }
        boolean running() { return running; }
        long startCount() { return startCount; }
        long completedSubmissionCount() { return completedSubmissionCount; }
        int controlListenerCount() { return controlListenerCount; }
        int controlAcceptThreadCount() { return controlAcceptThreadCount; }
        int handlerThreadCount() { return handlerThreadCount; }
        int dataListenerCount() { return dataListenerCount; }
    }
}
