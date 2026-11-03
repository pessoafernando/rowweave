package io.rowweave.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcTransport;
import io.rowweave.cluster.rpc.RpcTransports;
import io.rowweave.plan.PlanGraph;

import java.io.File;

/**
 * The public owned single-JVM cluster lifecycle. It runs the same cluster architecture
 * specialized to one JVM. One {@link MasterServer} and one {@link WorkerServer} with many
 * slots are created in the same process, and the RPC between master, worker and client is a direct in-process call,
 * with no socket and no serialization, resolved through {@link InProcessRpcRegistry} and the
 * transport factory. The data plane is entirely {@link io.rowweave.sched.LocalChannel}: with one
 * worker every stage lands on that worker, so every exchange is local. Operator execution,
 * parallelism, spilling and monitoring are the same code the cluster path runs.
 *
 * <p><b>Construction order</b>: the master's logical endpoint is created first, then the worker,
 * whose constructor registers in-process against that endpoint. Neither role opens an RpcServer or
 * an accept thread. Portable and durable commands are still canonical V2 encoded; only a legacy v1
 * command that is not durable and stays inside this JVM uses a transient object binding, and even
 * then it passes the same fence, admission and resource checks.
 */
public final class InProcessCluster implements JobClient, AutoCloseable {

    private final MasterServer master;
    private final WorkerServer worker;
    private final ClusterJobClient client;
    private final File tmpDir;
    private final io.rowweave.deployment.DeploymentRuntime workerRuntime;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * {@code parallelism} is only a hint for sizing this worker's slots, {@code max(64, P * 16)};
     * it does not decide the job's parallelism. That comes from the submitted PlanGraph, where
     * {@code BatchSession.setParallelism} compiled it in. The value passed here must
     * therefore be at least the parallelism of any job this cluster will run: on a single worker the
     * whole job lands here, so demand above the slot count is refused at admission. Callers should
     * keep it consistent with {@code setParallelism}.
     */
    public InProcessCluster(int parallelism) {
        this(parallelism, defaultTmpDir());
    }

    public InProcessCluster(int parallelism, File tmpDir) {
        this(parallelism, tmpDir, io.rowweave.monitor.JobMonitorRegistry.NOOP);
    }

    /** Local-runtime overload that installs monitoring without changing the default work directory. */
    public InProcessCluster(int parallelism, io.rowweave.monitor.JobMonitorRegistry monitor) {
        this(parallelism, defaultTmpDir(), monitor);
    }

    /**
     * Constructs the cluster with a run monitor. Passing a {@link io.rowweave.monitor.JobMonitorRegistry}
     * that is not the NOOP instance, for example an {@code InMemoryJobMonitor}, makes the master
     * record the job and stage views that the monitor HTTP surface renders. The default NOOP
     * registry costs nothing and changes no behaviour.
     */
    public InProcessCluster(int parallelism, File tmpDir, io.rowweave.monitor.JobMonitorRegistry monitor) {
        this.tmpDir = tmpDir;
        MasterServer createdMaster = null;
        WorkerServer createdWorker = null;
        io.rowweave.deployment.DeploymentRuntime createdWorkerRuntime = null;
        try {
            // Master first: its handlers are ready before the logical endpoint is activated, and
            // the monitor is injected at the deepest constructor. MasterServer no longer holds a P;
            // the submitted graph carries it, and parallelism here only sizes local worker slots.
            createdMaster = MasterServer.newInProcess(5000,
                    new io.rowweave.cluster.registry.InProcessRegistry(), monitor);
            createdMaster.startFixedLeadershipServices();
            // Then the worker: its constructor registers through the leader channel, which the
            // transport factory resolves in-process to the master. The slot count has to be enough
            // for the parallelism. Once admission is on, which is the default, the whole job lands
            // on this one worker and the demand is the number of stage instances across every stage;
            // a multi-source or binary DAG easily reaches dozens at a modest P. Slots are a ceiling
            // on daemon threads rather than on CPU, so they are generous, at least 64 and growing
            // with P, to keep the ledger from refusing the graph this cluster exists to run.
            int slots = Math.max(64, parallelism * 16);
            File wdir = new File(tmpDir, "w0"); wdir.mkdirs();
            createdWorkerRuntime = inProcessWorkerRuntime();
            createdWorker = WorkerServer.newInProcess(
                    "w0", slots, "127.0.0.1", createdMaster.port(), wdir,
                    createdWorkerRuntime);
        } catch (RuntimeException | Error failure) {
            closeAfterConstructionFailure(createdWorker, failure);
            closeAfterConstructionFailure(createdWorkerRuntime, failure);
            closeAfterConstructionFailure(createdMaster, failure);
            throw failure;
        }
        this.master = createdMaster;
        this.worker = createdWorker;
        this.workerRuntime = createdWorkerRuntime;
        // Client to master is in-process too: submitToSink resolves through RpcTransports.connect.
        this.client = new ClusterJobClient("127.0.0.1", master.port());
    }

    private static File defaultTmpDir() {
        try {
            return java.nio.file.Files.createTempDirectory("rowweave-inproc-cluster").toFile();
        } catch (java.io.IOException e) {
            throw new RuntimeException("failed to create InProcessCluster tmp dir", e);
        }
    }

    private static io.rowweave.deployment.DeploymentRuntime inProcessWorkerRuntime() {
        java.util.Properties properties = new java.util.Properties();
        properties.putAll(System.getProperties());
        properties.setProperty("rowweave.shuffle.mux", "false");
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(properties);
        return io.rowweave.deployment.DeploymentRuntime.load(
                properties, security, Runtime.getRuntime().maxMemory(), null);
    }

    public Diagnostics diagnostics() {
        return new Diagnostics(
                master.controlListenerCountForDiagnostics()
                        + worker.controlListenerCountForDiagnostics(),
                master.controlAcceptThreadCountForDiagnostics()
                        + worker.controlAcceptThreadCountForDiagnostics(),
                master.controlHandlerThreadCountForDiagnostics()
                        + worker.controlHandlerThreadCountForDiagnostics(),
                worker.dataListenerCountForDiagnostics());
    }

    public static final class Diagnostics {
        private final int controlListenerCount;
        private final int controlAcceptThreadCount;
        private final int handlerThreadCount;
        private final int dataListenerCount;

        private Diagnostics(int controlListenerCount, int controlAcceptThreadCount,
                            int handlerThreadCount, int dataListenerCount) {
            this.controlListenerCount = controlListenerCount;
            this.controlAcceptThreadCount = controlAcceptThreadCount;
            this.handlerThreadCount = handlerThreadCount;
            this.dataListenerCount = dataListenerCount;
        }

        public int controlListenerCount() { return controlListenerCount; }
        public int controlAcceptThreadCount() { return controlAcceptThreadCount; }
        public int handlerThreadCount() { return handlerThreadCount; }
        public int dataListenerCount() { return dataListenerCount; }
    }

    boolean terminalRunJanitorActiveForTest() {
        return master.terminalRunJanitorActive();
    }

    @Override
    public long submitToSink(PlanGraph sinkGraph) {
        return submit(sinkGraph).rowsOut();
    }

    @Override
    public io.rowweave.plan.ExecutionReport submitToSinkReported(PlanGraph sinkGraph) {
        return submit(sinkGraph);
    }

    /** Submits the whole graph once: several SINK nodes stay in one PlanGraph, not a job per sink. */
    @Override
    public io.rowweave.plan.ExecutionReport submit(PlanGraph fullGraph) {
        return submit(io.rowweave.artifact.LegacyPlanArtifactAdapter.wrap(
                "inprocess-cluster-legacy", fullGraph));
    }

    @Override
    public io.rowweave.plan.ExecutionReport submit(PlanArtifact artifact) {
        return submitInternal(artifact, null);
    }

    /** Same-JVM default-runtime submission with the existing BatchSession output-order hint. */
    public io.rowweave.plan.ExecutionReport submit(
            PlanArtifact artifact, boolean canonicalizeOutput) {
        return submitInternal(artifact, canonicalizeOutput);
    }

    private io.rowweave.plan.ExecutionReport submitInternal(
            PlanArtifact artifact, Boolean canonicalizeOutput) {
        if (artifact != null && artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            return client.submit(artifact);
        }
        ClusterSubmissionPreflight.validate(artifact);
        if (!io.rowweave.cluster.rpc.InProcessRpcRegistry.isRegistered(
                "127.0.0.1", master.port())) {
            throw new IllegalStateException("in-process Master endpoint is closed");
        }
        String publicationId = java.util.UUID.randomUUID().toString();
        io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy timeoutPolicy =
                io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy
                        .clientFromSystemProperties();
        RpcMessage request = canonicalizeOutput == null
                ? RpcMessage.submitPlanArtifactInProcess(
                        artifact, publicationId, timeoutPolicy.budget())
                : RpcMessage.submitPlanArtifactInProcess(
                        artifact, publicationId, timeoutPolicy.budget(),
                        canonicalizeOutput);
        RpcMessage result;
        try (RpcTransport transport = RpcTransports.connect(
                "127.0.0.1", master.port(), 5_000,
                timeoutPolicy.clientReadTimeoutMillis())) {
            result = transport.call(request);
        }
        if (result == null || result.type() != RpcMessage.Type.JOB_RESULT
                || !result.success()) {
            throw new IllegalStateException("cluster job failed: "
                    + (result == null ? "null response" : result.errorMessage()));
        }
        return ClusterJobClient.executionReport(result);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try { client.close(); } catch (RuntimeException ignored) { }
        try { worker.close(); } catch (RuntimeException ignored) { }
        try { workerRuntime.close(); } catch (RuntimeException ignored) { }
        try { master.close(); } catch (RuntimeException ignored) { }
    }

    private static void closeAfterConstructionFailure(AutoCloseable closeable, Throwable failure) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Throwable closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
