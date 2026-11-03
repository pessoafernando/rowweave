package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.cluster.rpc.RpcRoleEndpoint;
import io.rowweave.cluster.rpc.RpcRoleEndpoints;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.cluster.rpc.WhitelistObjectInputStream;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;

import java.io.ByteArrayInputStream;
import java.io.File;

/**
 * The long-lived Worker service, **threaded**: an RPC server that accepts {@link RpcMessage.Type#ASSIGN_STAGE}
 * and runs one {@link StageTask} per stage on an **in-process thread pool**. It no longer forks JVM child
 * processes, since a WorkerServer represents only itself and spends only thread resources.
 *
 * <p><b>Asynchronous two-step protocol</b>: assignStage **acks immediately** and does not wait for the stage
 * to finish, because a listening stage blocks waiting for data, which would pin the RPC handler and deadlock
 * the master's call. Execution moves to the thread pool:
 * <ul>
 * <li>once StageTask has bound its inbound channel, the {@link StageTask.PortListener} callback drives an
 * active {@link RpcMessage#portReport} RPC on another thread, instead of parsing stdout text;</li>
 * <li>when StageTask returns its {@link StageTask.StageResult} (rows plus StopReason), an active
 * {@link RpcMessage#stageReport} RPC carries it back.</li>
 * </ul>
 * Reports reuse long-lived connections through the {@code reportCache} lane, while REGISTER and HEARTBEAT
 * each keep their own lane, so a slow synchronous call cannot head-block the heartbeat. PROGRESS and
 * HEARTBEAT are one-way sends; STAGE and PORT are reliable calls.
 *
 * <p>StageTask runs on a cached thread pool, so the pure Push stages of an admitted job can start together
 * and a fixed pool cannot deadlock the pipeline. The master enables gang admission for the whole job by
 * default, atomically taking {@code slots} for the per-worker stage instance counts in the PhysicalPlan; the
 * Worker assignment ledger then applies the same slot count as a fail-closed hard gate on every new
 * registration, so a failover, a dual master or a direct ASSIGN cannot bypass the master ledger. The pool
 * neither queues nor caps threads, but accepted active stages never exceed the slots.
 */
public final class WorkerServer implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(WorkerServer.class);
    private static final boolean TRACE_HEARTBEATS =
            Boolean.getBoolean("rowweave.cluster.heartbeatTrace");

    // A guard that keeps one worker per process. A worker is a process role of its own, and several of
    // them in one JVM is a pretend cluster, which production forbids. The slot is claimed by
    // compare-and-set once the constructor succeeds and released idempotently by close(). A launched
    // worker already starts exactly one per process, so nothing changes there. The way out below steps
    // around the guard and is how an in-process multi-worker test says out loud that it is constructing a
    // pretend cluster.
    private static final java.util.concurrent.atomic.AtomicReference<WorkerServer> LIVE =
            new java.util.concurrent.atomic.AtomicReference<>();

    private final boolean guarded;  // true means this instance holds the live slot and releases it when it closes

    private final String workerId;
    /** This worker's generation within the process, set once at construction and carried on every registry and RPC message. */
    private final String incarnationId;
    private final int slots;
    private final int maxStageThreads;
    private final RpcRoleEndpoint rpcEndpoint;
    private final io.rowweave.plan.ObjectTrustContext objectTrustContext;
    private final io.rowweave.cluster.rpc.ControlSecurityProfile controlSecurityProfile;
    private final io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata;
    private final io.rowweave.cluster.rpc.ProtocolHelloV1 protocolHello;
    private final io.rowweave.deployment.DeploymentRuntime deploymentRuntime;
    private final boolean ownsDeploymentRuntime;
    private final File tmpDir;
    // One host currently carries both the control endpoint and the shuffle data endpoint, so it is taken
    // from the verified control security configuration. The loopback default no longer discovers or
    // publishes an interface address, and traffic between hosts is allowed only over configured mutual
    // TLS. The older worker host property is still read for the data plane but cannot step around the
    // control fence.
    private final String advertisedHost;
    // The current leader is resolved through a locator, so registration, heartbeat and reports all reach
    // whoever leads now and a worker follows a failover by itself. A host and port fixed at startup would
    // keep talking to a master that had stopped being the leader. The older constructor that names a host
    // and port wraps them as a fixed locator, so a single-master deployment behaves exactly as before.
    private final MasterLocator masterLocator;
    // Non-null only for a registry whose lease the worker owns. It reuses the locator's own session, so
    // the ephemeral node describing this worker lives and dies with the worker rather than with the
    // master. Closing the registry still belongs to whoever created it.
    private final ServiceRegistry workerLeaseRegistry;
    private final Object workerLeaseLifecycle = new Object();
    private enum Lifecycle { OPEN, CLOSING, CLOSED }
    private final Object closeLifecycle = new Object();
    private volatile Lifecycle lifecycle = Lifecycle.OPEN;
    private volatile boolean reportStopping;
    private volatile long closeDeadlineNanos = Long.MAX_VALUE;
    private Throwable closeFailure;
    private final long closeDrainMs;
    /**
     * The bound on a real terminal action such as a cancellation arriving while a job runs. An
     * acknowledgement still reports only a terminal state that actually happened.
     */
    private final long cancelDrainMs;
    private int workerLeaseOpsInFlight;
    // Enough threads to start every stage the master has already admitted for the whole job, since
    // admission is gang-scheduled across the job. Slots are bounded by the master's ledger rather than by
    // a fixed pool here that would queue one stage at a time, because pure Push stages waiting on each
    // other would never make progress.
    private final java.util.concurrent.ExecutorService stagePool;
    // Ledger of QUEUED, RUNNING and terminal tombstones: it absorbs idempotent redeliveries and CANCEL,
    // and bounds active assignments by the slot count.
    private final WorkerAssignmentLedger assignments;
    private final io.rowweave.internal.ProviderActionSupervisor
            providerActionSupervisor;
    private final io.rowweave.deployment.PortableRuntimeBindingRegistry
            portableRuntimeBindings;
    private final io.rowweave.internal.IncrementalSourceControlLedger
            incrementalSourceControlLedger =
            io.rowweave.internal.IncrementalSourceControlLedger.forRole();
    private volatile ProviderPreparedGroupCommandCodec.Action
            providerAckLossActionForTesting;
    private volatile String blockingProviderActionForTesting;
    private final java.util.concurrent.atomic.AtomicLong
            providerActionInvocationCountForTesting =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicInteger
            providerAckLossCountForTesting =
            new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicBoolean
            providerPoisonFenceStarted =
            new java.util.concurrent.atomic.AtomicBoolean();
    // A stage's result has to outlive the control call to the master that acknowledged it, so it is held
    // apart from the assignment ledger, which clears the command.
    private final PreparedOutputRegistry preparedOutputs;
    // The per-worker in-process shuffle rendezvous. Two adjacent stages on the same worker meet on a local
    // channel: the downstream registers the inbound channel it built ahead of time and the upstream awaits
    // it. Every stage task on this worker shares one instance, and rows pass by reference within the JVM
    // with no serialization at all.
    private final io.rowweave.sched.LocalShuffleRegistry localShuffle;

    // Multiplexing of the worker's single data port, on by default. When it is on, one shared data server
    // owns one port and one registry per worker routes inbound by key, so a stage registers rather than
    // binding a socket of its own. Setting it off explicitly leaves both null and every stage falls back
    // to binding and reporting a port, which is the rollback path an operator can choose.
    // The configuration is snapshotted per worker at construction rather than held as a static of the
    // class, because otherwise the first worker's settings would be frozen for every one after it.
    private final DataPlaneConfig dataPlaneConfig;
    private final io.rowweave.cluster.rpc.DataPlaneRegistration dataPlaneRegistration;
    private final io.rowweave.cluster.rpc.DataPlaneSecurityContext dataPlaneSecurityContext;
    private final io.rowweave.sched.net.DataPlaneHandshakeAdmission dataPlaneHandshakeAdmission;
    private final io.rowweave.sched.net.WorkerDataIdentity localDataIdentity;
    private final long dataServerAwaitTimeoutMs;
    private final io.rowweave.sched.net.DataInboundRegistry dataInbound;
    private final io.rowweave.sched.net.DataInboundAuthorizationRegistry dataAuthorizations;
    private final io.rowweave.sched.net.MuxConnectionPool muxConnections;
    private volatile io.rowweave.sched.net.WorkerDataServer dataServer;  // started when first needed, closed with the worker

    // Reuse of the worker's control connection to the leader, separated into three lanes. The report lane
    // carries stage, port and progress reports, the registration lane carries the synchronous register,
    // and the heartbeat lane carries one-way heartbeats and the departure notice. Keeping them apart is
    // what stops one stuck call from killing a job: a report blocked on a slow or wedged master holds its
    // thread for the whole read timeout, and sharing that connection would head-block the heartbeat, make
    // the master declare a live worker unreachable and stop the job it was running. The key is the
    // endpoint the locator resolved, so a failover to a new endpoint builds a new connection and the stale
    // entry is dropped.
    private final io.rowweave.cluster.rpc.RpcTransportCache reportCache;
    private final io.rowweave.cluster.rpc.RpcTransportCache registrationCache;
    private final io.rowweave.cluster.rpc.RpcTransportCache heartbeatCache;
    private final LeaderSubscriptionSlot leaderSubscription;

    public WorkerServer(String workerId, int slots, String masterHost, int masterPort, File tmpDir) {
        this(workerId, slots, new FixedMasterLocator(new Endpoint(masterHost, masterPort)),
                tmpDir, true);
    }

    /**
     * Addressing goes through a master locator; passing the registry-backed one lets the worker follow a
     * leader change after a failover.
     */
    public WorkerServer(String workerId, int slots, MasterLocator masterLocator, File tmpDir) {
        this(workerId, slots, masterLocator, tmpDir, true);
    }

    /** ClusterMain injects one process deployment identity shared by co-located roles. */
    public WorkerServer(String workerId, int slots, MasterLocator masterLocator, File tmpDir,
                        io.rowweave.deployment.DeploymentRuntime deploymentRuntime) {
        this(workerId, slots, masterLocator, tmpDir, true,
                RuntimeSource.borrowed(deploymentRuntime),
                io.rowweave.mem.WorkerConfig.maxStageThreads(), RoleConstructionProbe.NOOP);
    }

    /** ClusterMain entry with the preflighted per-assignment thread ceiling. */
    public WorkerServer(String workerId, int slots, MasterLocator masterLocator, File tmpDir,
                        io.rowweave.deployment.DeploymentRuntime deploymentRuntime,
                        int maxStageThreads) {
        this(workerId, slots, masterLocator, tmpDir, true,
                RuntimeSource.borrowed(deploymentRuntime), maxStageThreads,
                RoleConstructionProbe.NOOP);
    }

    /**
     * A way out for tests only. It steps around the process guard and allows several workers in one JVM,
     * which is how an in-process multi-worker test declares that it is knowingly constructing a pretend
     * cluster. Production must not reach for it: a launched worker is built through the constructor and is
     * bound by the guard. The name is the warning, and it is public only because the tests that need it
     * span packages.
     */
    public static WorkerServer newUnguarded(String workerId, int slots, String masterHost, int masterPort, File tmpDir) {
        return new WorkerServer(workerId, slots,
                new FixedMasterLocator(new Endpoint(masterHost, masterPort)), tmpDir, false);
    }

    /** PC3 test/profile entry with an explicit per-assignment thread ceiling. */
    static WorkerServer newUnguarded(String workerId, int slots, String masterHost,
                                     int masterPort, File tmpDir, int maxStageThreads) {
        return new WorkerServer(workerId, slots,
                new FixedMasterLocator(new Endpoint(masterHost, masterPort)), tmpDir, false,
                RuntimeSource.owned(
                        io.rowweave.deployment.DeploymentRuntime::fromSystemPropertiesForWorker),
                maxStageThreads, RoleConstructionProbe.NOOP);
    }

    /** The master-locator form of the unguarded constructor, used by failover tests. */
    public static WorkerServer newUnguarded(String workerId, int slots, MasterLocator masterLocator, File tmpDir) {
        return new WorkerServer(workerId, slots, masterLocator, tmpDir, false);
    }

    static WorkerServer newUnguarded(
            String workerId, int slots, MasterLocator masterLocator,
            File tmpDir,
            io.rowweave.deployment.DeploymentRuntime deploymentRuntime) {
        return new WorkerServer(workerId, slots, masterLocator, tmpDir, false,
                RuntimeSource.borrowed(deploymentRuntime),
                io.rowweave.mem.WorkerConfig.maxStageThreads(),
                RoleConstructionProbe.NOOP);
    }

    private WorkerServer(String workerId, int slots, MasterLocator masterLocator, File tmpDir, boolean guarded) {
        this(workerId, slots, masterLocator, tmpDir, guarded,
                RuntimeSource.owned(
                        io.rowweave.deployment.DeploymentRuntime::fromSystemPropertiesForWorker),
                io.rowweave.mem.WorkerConfig.maxStageThreads(), RoleConstructionProbe.NOOP);
    }

    @FunctionalInterface
    interface DeploymentRuntimeFactory {
        io.rowweave.deployment.DeploymentRuntime create();
    }

    static final class RuntimeSource {
        private final io.rowweave.deployment.DeploymentRuntime borrowed;
        private final DeploymentRuntimeFactory ownedFactory;

        private RuntimeSource(io.rowweave.deployment.DeploymentRuntime borrowed,
                              DeploymentRuntimeFactory ownedFactory) {
            this.borrowed = borrowed;
            this.ownedFactory = ownedFactory;
        }

        static RuntimeSource borrowed(io.rowweave.deployment.DeploymentRuntime runtime) {
            return new RuntimeSource(java.util.Objects.requireNonNull(
                    runtime, "deploymentRuntime"), null);
        }

        static RuntimeSource owned(DeploymentRuntimeFactory factory) {
            return new RuntimeSource(null, java.util.Objects.requireNonNull(
                    factory, "deployment runtime factory"));
        }

        boolean owned() { return ownedFactory != null; }
    }

    WorkerServer(String workerId, int slots, MasterLocator masterLocator, File tmpDir,
                 boolean guarded, RuntimeSource runtimeSource, int maxStageThreads,
                 RoleConstructionProbe probe) {
        this(workerId, slots, masterLocator, tmpDir, guarded, runtimeSource,
                maxStageThreads, probe, false);
    }

    static WorkerServer newInProcess(
            String workerId, int slots, String masterHost, int masterPort,
            File tmpDir, io.rowweave.deployment.DeploymentRuntime deploymentRuntime) {
        return new WorkerServer(workerId, slots,
                new FixedMasterLocator(new Endpoint(masterHost, masterPort)),
                tmpDir, false, RuntimeSource.borrowed(deploymentRuntime),
                io.rowweave.mem.WorkerConfig.maxStageThreads(),
                RoleConstructionProbe.NOOP, true);
    }

    private WorkerServer(String workerId, int slots, MasterLocator masterLocator, File tmpDir,
                 boolean guarded, RuntimeSource runtimeSource, int maxStageThreads,
                 RoleConstructionProbe probe, boolean inProcessEndpoint) {
        java.util.Objects.requireNonNull(workerId, "workerId");
        java.util.Objects.requireNonNull(masterLocator, "masterLocator");
        java.util.Objects.requireNonNull(tmpDir, "tmpDir");
        java.util.Objects.requireNonNull(runtimeSource, "runtimeSource");
        java.util.Objects.requireNonNull(probe, "probe");
        if (slots < 1) {
            throw new IllegalArgumentException("worker slots must be >= 1, got " + slots);
        }
        if (maxStageThreads < 1) {
            throw new IllegalArgumentException(
                    "worker maxStageThreads must be >= 1, got " + maxStageThreads);
        }
        long configuredCloseDrainMs = Long.getLong("rowweave.worker.closeDrainMs", 5_000L);
        if (configuredCloseDrainMs < 0L) {
            throw new IllegalArgumentException("rowweave.worker.closeDrainMs must be >= 0");
        }
        long configuredCancelDrainMs = Long.getLong("rowweave.worker.cancelDrainMs", 5_000L);
        if (configuredCancelDrainMs < 0L) {
            throw new IllegalArgumentException("rowweave.worker.cancelDrainMs must be >= 0");
        }
        java.util.Properties startupProperties = systemPropertiesSnapshot();
        io.rowweave.cluster.rpc.RpcAdmissionConfig rpcAdmission =
                io.rowweave.cluster.rpc.RpcAdmissionConfig.fromProperties(startupProperties);
        long configuredDataAwaitTimeoutMs = longProperty(
                startupProperties, "rowweave.cluster.stageTimeoutMs", 60_000L);
        if (configuredDataAwaitTimeoutMs < 0L) {
            throw new IllegalArgumentException("rowweave.cluster.stageTimeoutMs must be >= 0");
        }
        int preparedLimit = Integer.getInteger(
                "rowweave.worker.preparedOutputLimit", Math.max(64, slots * 4));
        long preparedRetentionMs = Long.getLong(
                "rowweave.worker.preparedOutputRetentionMs", 600_000L);
        if (preparedLimit <= 0) {
            throw new IllegalArgumentException("prepared output limit must be > 0");
        }
        if (preparedRetentionMs <= 0L) {
            throw new IllegalArgumentException("prepared output retention must be > 0");
        }
        long heartbeatIntervalMs = Long.getLong("rowweave.worker.heartbeatMs", 1000L);
        if (heartbeatIntervalMs <= 0L) {
            throw new IllegalArgumentException("rowweave.worker.heartbeatMs must be > 0");
        }
        long portableActionTimeoutMs = longProperty(
                startupProperties, "rowweave.portable.actionTimeoutMs", 30_000L);
        if (portableActionTimeoutMs <= 0L
                || portableActionTimeoutMs
                > io.rowweave.deployment.PortableRuntimeBindingRegistry
                .MAX_ACTION_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException(
                    "rowweave.portable.actionTimeoutMs is outside its limit");
        }
        this.guarded = guarded;
        this.closeDrainMs = configuredCloseDrainMs;
        this.cancelDrainMs = configuredCancelDrainMs;
        RoleConstructionTransaction transaction = new RoleConstructionTransaction();
        try {
            io.rowweave.deployment.DeploymentRuntime deploymentRuntime = runtimeSource.owned()
                    ? transaction.acquire(() -> runtimeSource.ownedFactory.create(),
                    io.rowweave.deployment.DeploymentRuntime::close)
                    : runtimeSource.borrowed;
            this.deploymentRuntime = deploymentRuntime;
            this.ownsDeploymentRuntime = runtimeSource.owned();
            if (ownsDeploymentRuntime) probe.after(WorkerConstructionStep.OWNED_RUNTIME);
            deploymentRuntime.requireAdmissionOpen();
            ControlSecurityConfig controlSecurity = deploymentRuntime.controlSecurity();
            controlSecurity.requireImplementedTransport();
            controlSecurity.requireLocalRole(
                    io.rowweave.cluster.rpc.ControlRole.WORKER);
            this.dataPlaneConfig = deploymentRuntime.requireDataPlaneConfig();
            this.dataPlaneRegistration = controlSecurity.workerDataRegistration();
            this.dataServerAwaitTimeoutMs = configuredDataAwaitTimeoutMs;
            this.controlSecurityProfile = controlSecurity.profile();
            this.securityMetadata = controlSecurity.securityMetadata();
            this.protocolHello = deploymentRuntime.protocolHello();
            this.objectTrustContext = deploymentRuntime.objectTrustContext();
            // The record that this process is live belongs to the constructor's transaction, so any later failure
            // has to release it.
            if (guarded) {
                transaction.acquire(() -> {
                    if (!LIVE.compareAndSet(null, this)) {
                        throw new IllegalStateException(
                                "This JVM already has a WorkerServer; production allows one worker per JVM."
                                + " Use WorkerServer.newUnguarded(...) for tests that require multiple workers in one JVM.");
                    }
                    return this;
                }, ignored -> LIVE.compareAndSet(this, null));
                probe.after(WorkerConstructionStep.LIVE_CLAIM);
            }
            this.stagePool = transaction.acquire(
                    () -> java.util.concurrent.Executors.newCachedThreadPool(r -> {
                        Thread thread = new Thread(r, "rowweave-stage");
                        thread.setDaemon(true);
                        return thread;
                    }), java.util.concurrent.ExecutorService::shutdownNow);
            probe.after(WorkerConstructionStep.STAGE_POOL);
            this.localShuffle = transaction.acquire(
                    io.rowweave.sched.LocalShuffleRegistry::new,
                    io.rowweave.sched.LocalShuffleRegistry::close);
            probe.after(WorkerConstructionStep.LOCAL_SHUFFLE);

            io.rowweave.cluster.rpc.RpcTransportCache.Connector connector =
                    (host, masterPort) -> io.rowweave.cluster.rpc.RpcTransports.connect(
                            host, masterPort, 5_000, 30_000,
                            controlSecurity, protocolHello);
            io.rowweave.cluster.rpc.RpcTransportCache.Cacheable cacheable =
                    (host, masterPort) -> !io.rowweave.cluster.rpc.InProcessRpcRegistry
                            .isRegistered(host, masterPort);
            this.reportCache = transaction.acquire(
                    () -> new io.rowweave.cluster.rpc.RpcTransportCache(connector, cacheable),
                    io.rowweave.cluster.rpc.RpcTransportCache::shutdown);
            probe.after(WorkerConstructionStep.REPORT_CACHE);
            this.registrationCache = transaction.acquire(
                    () -> new io.rowweave.cluster.rpc.RpcTransportCache(connector, cacheable),
                    io.rowweave.cluster.rpc.RpcTransportCache::shutdown);
            this.heartbeatCache = transaction.acquire(
                    () -> new io.rowweave.cluster.rpc.RpcTransportCache(connector, cacheable),
                    io.rowweave.cluster.rpc.RpcTransportCache::shutdown);
            probe.after(WorkerConstructionStep.HEARTBEAT_CACHE);

            this.workerId = workerId;
            this.advertisedHost = controlSecurity.advertisedHost();
            this.incarnationId = java.util.UUID.randomUUID().toString();
            this.slots = slots;
            this.maxStageThreads = maxStageThreads;
            this.assignments = new WorkerAssignmentLedger(slots);
            this.providerActionSupervisor =
                    io.rowweave.internal.ProviderActionSupervisor.processWide(
                            Integer.getInteger(
                                    "rowweave.provider.action.parallelism",
                                    Math.min(slots, io.rowweave.internal
                                            .ProviderActionSupervisor.MAX_CAPACITY)));
            this.portableRuntimeBindings = deploymentRuntime
                    .portableRuntimeBindings(
                            providerActionSupervisor,
                            portableActionTimeoutMs);
            this.dataInbound = dataPlaneConfig.muxEnabled()
                    ? transaction.acquire(
                            io.rowweave.sched.net.DataInboundRegistry::new,
                            io.rowweave.sched.net.DataInboundRegistry::close)
                    : null;
            if (dataInbound != null) probe.after(WorkerConstructionStep.DATA_INBOUND);
            boolean mtlsData = dataPlaneConfig.protocol()
                    == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1;
            this.dataPlaneSecurityContext = mtlsData
                    ? deploymentRuntime.requireDataPlaneSecurityContext() : null;
            this.dataPlaneHandshakeAdmission = mtlsData
                    ? deploymentRuntime.requireDataPlaneHandshakeAdmission() : null;
            this.localDataIdentity = mtlsData
                    ? io.rowweave.sched.net.WorkerDataIdentity.authenticated(
                            workerId, incarnationId, dataPlaneRegistration.protocol(),
                            securityMetadata.deploymentSetId(), securityMetadata.authzSetId(),
                            dataPlaneRegistration.certificateFingerprint())
                    : null;
            this.dataAuthorizations = dataPlaneConfig.muxEnabled() && mtlsData
                    ? transaction.acquire(
                            io.rowweave.sched.net.DataInboundAuthorizationRegistry::new,
                            io.rowweave.sched.net.DataInboundAuthorizationRegistry::close)
                    : null;
            if (dataAuthorizations != null) probe.after(WorkerConstructionStep.DATA_AUTHZ);
            this.muxConnections = dataPlaneConfig.muxEnabled()
                    ? transaction.acquire(
                            () -> mtlsData
                                    ? new io.rowweave.sched.net.MuxConnectionPool(
                                            dataPlaneConfig, dataPlaneSecurityContext,
                                            dataPlaneHandshakeAdmission, localDataIdentity)
                                    : new io.rowweave.sched.net.MuxConnectionPool(),
                            io.rowweave.sched.net.MuxConnectionPool::close)
                    : null;
            if (muxConnections != null) probe.after(WorkerConstructionStep.MUX_POOL);
            this.preparedOutputs = transaction.acquire(
                    () -> new PreparedOutputRegistry(preparedLimit, preparedRetentionMs),
                    PreparedOutputRegistry::close);
            probe.after(WorkerConstructionStep.PREPARED_OUTPUTS);
            this.masterLocator = masterLocator;
            this.workerLeaseRegistry = masterLocator instanceof RegistryMasterLocator
                    ? ((RegistryMasterLocator) masterLocator).workerLeaseRegistry()
                    : null;
            this.tmpDir = tmpDir;
            this.leaderSubscription = new LeaderSubscriptionSlot();
            this.rpcEndpoint = transaction.acquire(
                    () -> inProcessEndpoint
                            ? RpcRoleEndpoints.inProcess(
                                    advertisedHost, handler(), rpcAdmission.maxConnections())
                            : RpcRoleEndpoints.socket(RpcServer.bindDormant(
                                    (request, context) -> handle(request, context), 0,
                                    controlSecurity, rpcAdmission,
                                    io.rowweave.cluster.rpc.RpcRequestPolicy.WORKER,
                                    protocolHello)),
                    RpcRoleEndpoint::close);
            probe.after(WorkerConstructionStep.RPC_BOUND);
        } catch (Throwable constructionFailure) {
            transaction.rollback(constructionFailure);
            rethrowConstructionFailure(constructionFailure);
            throw new AssertionError("unreachable");
        }

        transaction.commit();
        try {
            rpcEndpoint.activate();
            probe.after(WorkerConstructionStep.RPC_ACTIVE);
            // The leadership watch is a resource taken after construction, so the worker owns its handle and close
            // establishes the callback fence before anything else is closed.
            if (masterLocator instanceof RegistryMasterLocator) {
                leaderSubscription.install(
                        ((RegistryMasterLocator) masterLocator)
                                .subscribeLeaderChanges(this::onLeaderChanged));
                probe.after(WorkerConstructionStep.LEADER_SUBSCRIPTION);
            }
            registerWorkerLease();
            if (workerLeaseRegistry != null) {
                probe.after(WorkerConstructionStep.WORKER_LEASE);
            }
            registerWithLeader();
            probe.after(WorkerConstructionStep.MASTER_REGISTRATION);
            // Beating starts with construction, on a thread of its own that stage backpressure cannot delay, so
            // the master's progress-based liveness judgement always has a signal in any deployment instead of a
            // fixed wall-clock deadline that stops a large job still running. The interval sits well below the
            // staleness threshold, and beats continue while the worker is busy computing.
            startHeartbeat(heartbeatIntervalMs);
            probe.after(WorkerConstructionStep.HEARTBEAT);
        } catch (Throwable postInitFailure) {
            try {
                close();
            } catch (Throwable cleanupFailure) {
                if (cleanupFailure != postInitFailure) {
                    postInitFailure.addSuppressed(cleanupFailure);
                }
            }
            rethrowConstructionFailure(postInitFailure);
            throw new AssertionError("unreachable");
        }
    }

    private static void rethrowConstructionFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    public int port() {
        return rpcEndpoint.port();
    }

    int controlListenerCountForDiagnostics() {
        return rpcEndpoint.hasNetworkListener() ? 1 : 0;
    }

    int controlAcceptThreadCountForDiagnostics() {
        return rpcEndpoint.acceptThreadAlive() ? 1 : 0;
    }

    int controlHandlerThreadCountForDiagnostics() {
        return rpcEndpoint.handlerThreadCount();
    }

    int dataListenerCountForDiagnostics() {
        return dataServer == null ? 0 : 1;
    }

    /** Identity for tests and diagnostics, visible inside the package; it is not part of the job API. */
    String incarnationId() {
        return incarnationId;
    }

    void loseNextProviderGroupAcksForTesting(
            ProviderPreparedGroupCommandCodec.Action action, int count) {
        if (action == null || count < 0) {
            throw new IllegalArgumentException(
                    "provider ACK-loss hook is invalid");
        }
        providerAckLossActionForTesting = action;
        providerAckLossCountForTesting.set(count);
    }

    void blockProviderActionForTesting(String action) {
        if (action == null || action.isEmpty()) {
            throw new IllegalArgumentException(
                    "provider blocking action is required");
        }
        blockingProviderActionForTesting = action;
    }

    private void beforeProviderActionForTesting(String action) {
        providerActionInvocationCountForTesting.incrementAndGet();
        if (!action.equals(blockingProviderActionForTesting)) return;
        providerActionSupervisor.call(
                "test-blocking-provider-" + action,
                deadlineAfterMillis(500L), () -> {
                    for (;;) {
                        java.util.concurrent.locks.LockSupport.parkNanos(
                                java.util.concurrent.TimeUnit.MILLISECONDS
                                        .toNanos(10L));
                    }
                });
    }

    boolean providerRuntimePoisoned() {
        return providerActionSupervisor.isPoisoned();
    }

    long providerActionInvocationCountForTesting() {
        return providerActionInvocationCountForTesting.get();
    }

    boolean awaitClosedForTesting(long timeout, java.util.concurrent.TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (closeLifecycle) {
            while (lifecycle != Lifecycle.CLOSED
                    && System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) break;
                java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(
                        closeLifecycle, remaining);
            }
            return lifecycle == Lifecycle.CLOSED;
        }
    }

    private boolean isOpen() {
        return lifecycle == Lifecycle.OPEN;
    }

    private void requireOpen() {
        if (!isOpen()) {
            throw new IllegalStateException("worker is closing or closed");
        }
    }

    /**
     * Reacting to a leader change as it is observed. Connections to the old leader are dropped on both
     * lanes so the next call is built against the new one, and the worker re-registers immediately instead
     * of waiting for the next heartbeat, which shortens the window in which the new leader does not know
     * it. This runs on the registry's dispatch thread, so the cheap part happens here and the
     * re-registration moves to a thread of its own rather than making a blocking call in place; a failure
     * is absorbed so it cannot damage that thread.
     */
    private void onLeaderChanged() {
        reportCache.close();  // drops every cached connection, so the old leader's is gone and the next call rebuilds against the current endpoint
        registrationCache.close();
        heartbeatCache.close();
        java.util.concurrent.ScheduledExecutorService timer = maintenanceTimer;
        if (isOpen() && timer != null && !timer.isShutdown()) {
            try {
                timer.execute(() -> {
                    if (!isOpen()) {
                        return;
                    }
                    try {
                        // A term boundary is not stepped over: every stage still active in the old term is cancelled in two
                        // phases before the worker registers with whoever leads now.
                        preparedOutputs.rollbackAll();
                        if (!cancelAssignmentsFromPreviousLeader()) {
                            LOG.warn("leader-change cancellation timed out after {}ms; worker={} will not "
                                            + "register while previous-term stages remain active",
                                    cancelDrainMs, workerId);
                            return;
                        }
                        // Register with the current leader at once, the same call the scheduled heartbeat makes, which is
                        // idempotent.
                        registerWithLeader();
                    } catch (RuntimeException ignored) {
                        // The new leader may not be reachable yet while an election is settling; the next scheduled heartbeat retries.
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException ignored) {
                // Closing races the watch: the timer is already shut down, so nothing registers again after this point.
            }
        }
    }

    /**
     * Resolves the current leader through the locator for every message and sends it over the lane named,
     * reusing the long-lived connection. When a failover changes the endpoint, the new key builds a new
     * connection, and when a call throws the entry is dropped so the next attempt reconnects. Why there
     * are separate lanes is described where the caches are declared.
     */
    private RpcMessage callLeader(io.rowweave.cluster.rpc.RpcTransportCache lane, RpcMessage msg) {
        Endpoint leader = masterLocator.locateLeader();
        RpcMessage request = secureOutbound(msg);
        RpcMessage response = lane.call(leader.host(), leader.port(), request);
        response = secureInbound(response);
        if (response == null || response.type() != RpcMessage.Type.ACK) {
            String error = response != null ? response.errorMessage() : "null response";
            throw new IllegalStateException("active master rejected " + msg.type() + ": " + error);
        }
        return response;
    }

    /**
     * Registration has a success contract of its own that depends on the profile, and cannot borrow the
     * check for an empty acknowledgement.
     */
    private void registerWithLeader() {
        Endpoint leader = masterLocator.locateLeader();
        RpcMessage request = RpcMessage.registerWorker(
                workerId, advertisedHost, rpcEndpoint.port(), slots, dataPort(), incarnationId,
                dataPlaneRegistration);
        RpcMessage response = registrationCache.call(
                leader.host(), leader.port(), secureOutbound(request));
        response = secureInbound(response);
        io.rowweave.cluster.rpc.DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                controlSecurityProfile, dataPlaneRegistration, incarnationId, response);
    }

    /**
     * Sends one-way to the current leader, with no frame back. Used for heartbeats and progress, which may
     * be dropped and must not block the sender.
     */
    private void sendLeader(io.rowweave.cluster.rpc.RpcTransportCache lane, RpcMessage msg) {
        Endpoint leader = masterLocator.locateLeader();
        lane.send(leader.host(), leader.port(), secureOutbound(msg));
    }

    /**
     * Sends a heartbeat, called by a test or by the timer. Stage work runs on its own threads and this
     * process does not park, so a heartbeat is never delayed by stage backpressure.
     */
    public void heartbeat() {
        requireOpen();
        long available = freeSlots();
        if (TRACE_HEARTBEATS) {
            LOG.info("heartbeat trace sending worker={} incarnation={} freeSlots={}",
                    workerId, incarnationId, available);
        }
        sendLeader(heartbeatCache, RpcMessage.heartbeat(workerId, available, incarnationId));  // one-way, with no frame back
        if (TRACE_HEARTBEATS) {
            LOG.info("heartbeat trace sent worker={} incarnation={}", workerId, incarnationId);
        }
    }

    /**
     * Free slots are the total minus what is queued or running, which has to include a stage already
     * acknowledged but not yet started.
     */
    public long freeSlots() {
        return Math.max(0, (long) slots - assignments.activeCount());
    }

    private volatile java.util.concurrent.ScheduledExecutorService heartbeatTimer;
    private volatile java.util.concurrent.ScheduledExecutorService maintenanceTimer;

    /**
     * The heartbeat sent by a process that stays alive, over a connection of its own that never
     * interleaves with stage backpressure. It goes out every interval, so the master's monitor does not
     * declare this worker unreachable and stop the jobs it is running. close() ends it.
     */
    public synchronized void startHeartbeat(long intervalMs) {
        synchronized (closeLifecycle) {
            requireOpen();
            if (heartbeatTimer != null) {
                return;
            }
            java.util.concurrent.ScheduledExecutorService beatCandidate =
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rowweave-worker-heartbeat-" + workerId);
                t.setDaemon(true);
                return t;
            });
            java.util.concurrent.ScheduledExecutorService maintenanceCandidate =
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rowweave-worker-maintenance-" + workerId);
                t.setDaemon(true);
                return t;
            });
            try {
                HeartbeatScheduler.activate(maintenanceCandidate, () -> {
                    try {
                        if (!isOpen()) return;
                        purgeExpiredRendezvous();
                        reportWorkerLease();
                        // Where the master has not yet seen this worker, register again on the next tick; a synchronous call
                        // must not occupy the heartbeat timer's thread.
                        registerWithLeader();
                    } catch (RuntimeException ignored) {
                        // The master is unreachable or an election is in progress: try again on the next maintenance tick.
                    }
                }, intervalMs);
                HeartbeatScheduler.activate(beatCandidate, () -> {
                    try {
                        if (isOpen()) heartbeat();
                    } catch (RuntimeException ignored) {
                        // A one-way heartbeat may be dropped and the next tick repeats it, with the master's own timeout as the backstop.
                        if (TRACE_HEARTBEATS) {
                            LOG.warn("heartbeat trace send failed worker={}", workerId, ignored);
                        }
                    }
                }, intervalMs);
            } catch (Throwable activationFailure) {
                maintenanceCandidate.shutdownNow();
                beatCandidate.shutdownNow();
                throw activationFailure;
            }
            maintenanceTimer = maintenanceCandidate;
            heartbeatTimer = beatCandidate;
        }
    }

    private RegistryWorker workerRegistryRecord() {
        return new RegistryWorker(workerId, advertisedHost, rpcEndpoint.port(), slots, dataPort(),
                incarnationId, securityMetadata,
                securityMetadata == null ? null : dataPlaneRegistration,
                protocolHello);
    }

    private void registerWorkerLease() {
        maintainWorkerLease(true);
    }

    private void reportWorkerLease() {
        maintainWorkerLease(false);
    }

    /**
     * A small generation fence between close and the periodic report: before close removes the node, every
     * lease write that has already begun must have finished.
     */
    private void maintainWorkerLease(boolean initialRegistration) {
        if (workerLeaseRegistry == null) {
            return;
        }
        synchronized (workerLeaseLifecycle) {
            if (!isOpen()) {
                return;
            }
            workerLeaseOpsInFlight++;
        }
        try {
            RegistryWorker record = workerRegistryRecord();
            if (initialRegistration) {
                workerLeaseRegistry.register(record);
            } else {
                workerLeaseRegistry.report(record);
            }
        } finally {
            boolean removeLateLease;
            synchronized (workerLeaseLifecycle) {
                workerLeaseOpsInFlight--;
                removeLateLease = !isOpen();
                workerLeaseLifecycle.notifyAll();
            }
            // If close's bounded wait returns first, a register or report arriving later withdraws itself rather
            // than bringing a closed worker back to life.
            if (removeLateLease) {
                deregisterWorkerLeaseQuietly();
            }
        }
    }

    private void closeWorkerLease(long deadlineNanos) {
        synchronized (workerLeaseLifecycle) {
            while (workerLeaseOpsInFlight > 0 && System.nanoTime() < deadlineNanos) {
                try {
                    long remainMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                            Math.max(1L, deadlineNanos - System.nanoTime()));
                    workerLeaseLifecycle.wait(remainMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        deregisterWorkerLeaseQuietly();
    }

    private void deregisterWorkerLeaseQuietly() {
        if (workerLeaseRegistry == null) {
            return;
        }
        try {
            workerLeaseRegistry.deregister(workerId);
        } catch (RuntimeException cleanupFailure) {
            // The registry session may already have been closed from outside, in which case the lease went with it
            // and that must not block closing local resources.
            LOG.debug("worker lease deregister skipped worker={} cause={}",
                    workerId, cleanupFailure.toString());
        }
    }

    /** Driven by the heartbeat schedule; the short maintenance step with no call in it is separate so a test can drive it directly. */
    void purgeExpiredRendezvous() {
        localShuffle.purgeExpired();
        if (dataInbound != null) {
            dataInbound.purgeExpired();
        }
        preparedOutputs.purgeExpired();
    }

    /** Exposes the RPC handler so that an in-process transport can register it. */
    public java.util.function.Function<RpcMessage, RpcMessage> handler() {
        return request -> {
            RpcMessage response = handle(secureInbound(request), null);
            return response == null ? null : secureOutbound(response);
        };
    }

    private RpcMessage secureOutbound(RpcMessage message) {
        return io.rowweave.cluster.rpc.RpcSecurityFence.outbound(
                message, securityMetadata,
                controlSecurityProfile == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS);
    }

    private RpcMessage secureInbound(RpcMessage message) {
        return io.rowweave.cluster.rpc.RpcSecurityFence.inbound(
                message, securityMetadata,
                controlSecurityProfile == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS);
    }

    /**
     * An assignment is registered idempotently by stage and fingerprint, and an acknowledgement for a
     * cancellation is returned only once the stage has really reached a terminal state.
     */
    private RpcMessage handle(
            RpcMessage req, io.rowweave.cluster.rpc.RpcRequestContext requestContext) {
        io.rowweave.cluster.rpc.RpcRequestPolicy.WORKER.requireAllowedDirect(req.type());
        if (req.type() == RpcMessage.Type.ASSIGN_STAGE) {
            if (!isOpen()) {
                return RpcMessage.jobResult(false, 0, "worker is closing or closed");
            }
            try {
                deploymentRuntime.requireAdmissionOpen();
            } catch (RuntimeException changed) {
                return RpcMessage.jobResult(false, 0, changed.getMessage());
            }
            if (!incarnationId.equals(req.incarnationId())) {
                return RpcMessage.jobResult(false, 0,
                        "worker incarnation mismatch expected=" + incarnationId + " actual=" + req.incarnationId());
            }
            String leaderError = assignmentLeaderError(req);
            if (leaderError != null) {
                return RpcMessage.jobResult(false, 0, leaderError);
            }
            long stageId = req.stageId();
            Object inProcCmd = req.stageCommand();
            final DurableAssignmentEnvelope durable;
            try {
                durable = durableEnvelope(req);
            } catch (RuntimeException invalid) {
                return RpcMessage.jobResult(false, 0,
                        "invalid durable stage assignment: " + invalid.getMessage());
            }
            final PreparedStageCommand prepared;
            StageCommand decodedCommand = null;
            try {
                if (inProcCmd != null && !(inProcCmd instanceof StageCommand)) {
                    throw new IllegalArgumentException("in-process command has wrong type");
                }
                if (inProcCmd instanceof StageCommand) {
                    if (durable != null) {
                        throw new IllegalArgumentException(
                                "durable assignment requires canonical V2 command bytes");
                    }
                    prepared = null;
                    decodedCommand = (StageCommand) inProcCmd;
                    decodedCommand.attachObjectTrustContext(objectTrustContext);
                    // The logical endpoint can only be registered inside this JVM. Master already
                    // admitted and constructed this command; the common fence, capacity, shuffle
                    // and thread-cost checks below remain authoritative without forcing legacy
                    // v1 functions through the socket serialization contract.
                } else if (StageCommandCodecV2.hasMagic(req.stageCommandBlob())) {
                    prepared = StageCommandCodecV2.preflight(
                            req.stageCommandBlob(), req::reserveResidentBytes);
                } else {
                    if (!allowsLegacyStageCommand(controlSecurityProfile, durable != null)) {
                        throw new IllegalArgumentException(
                                "legacy StageCommand serialization is allowed only for non-durable LOOPBACK compatibility");
                    }
                    prepared = null;
                    decodedCommand = deserializeCommand(req);
                    decodedCommand.attachObjectTrustContext(objectTrustContext);
                }
            } catch (io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted) {
                return RpcMessage.jobResult(false, 0, exhausted.getMessage());
            } catch (Exception | LinkageError decodeFailure) {
                return RpcMessage.jobResult(false, 0,
                        "invalid stage command: " + decodeFailure.getMessage());
            }
            io.rowweave.run.RunAttemptFence commandFence = prepared == null
                    ? decodedCommand.runAttemptFence() : prepared.runAttemptFence();
            if (req.runAttemptFence() == null || commandFence == null) {
                return RpcMessage.jobResult(false, 0, "missing run attempt fence");
            }
            if (!req.runAttemptFence().equals(commandFence)) {
                return RpcMessage.jobResult(false, 0, "run attempt fence mismatch");
            }
            boolean portableArtifact = prepared != null
                    && prepared.hasPortableArtifact();
            if (portableArtifact) {
                try {
                    prepared.preflightPortableArtifact(
                            deploymentRuntime.portableBindingVerifier(),
                            durable != null, req::reserveResidentBytes);
                } catch (RuntimeException | LinkageError rejected) {
                    return RpcMessage.jobResult(false, 0,
                            "invalid portable stage artifact: "
                                    + rejected.getMessage());
                }
            }
            final io.rowweave.ProviderBatchSink admittedProviderProxy;
            if (durable != null) {
                try {
                    admittedProviderProxy = portableArtifact ? null
                            : prepared.providerOutputForAdmission(
                                    deploymentRuntime
                                            .durableSinkProviderRegistry(),
                                    req::reserveResidentBytes);
                    requireProviderAssignmentIdentity(
                            prepared, durable, admittedProviderProxy);
                    SourceAssignmentVerifier.requireRoutingMatches(
                            durable.token, prepared.role(), prepared.sourceSplitAssignment());
                } catch (RuntimeException mismatch) {
                    return RpcMessage.jobResult(false, 0,
                            "invalid durable source assignment: " + mismatch.getMessage());
                }
            } else {
                admittedProviderProxy = null;
            }
            WorkerAssignmentLedger.Registration registration;
            synchronized (closeLifecycle) {
                if (lifecycle != Lifecycle.OPEN) {
                    return RpcMessage.jobResult(false, 0, "worker is closing or closed");
                }
                try {
                    if (inProcCmd instanceof StageCommand) {
                        if (durable != null) {
                            throw new IllegalArgumentException(
                                    "durable assignment requires canonical socket command bytes");
                        }
                        registration = assignments.registerInProcess(
                                stageId, inProcCmd, req.runAttemptFence());
                    } else if (durable != null) {
                        registration = assignments.registerDurableSocket(
                                stageId, req.stageCommandBlob(), req.runAttemptFence(),
                                durable.token, durable.commandDigest,
                                req.residentLease(), req.residentOwnerToken());
                    } else {
                        registration = assignments.registerSocket(
                                stageId, req.stageCommandBlob(), req.runAttemptFence(), false,
                                req.residentLease(), req.residentOwnerToken());
                    }
                } catch (RuntimeException e) {
                    return RpcMessage.jobResult(false, 0, "invalid stage assignment: " + e.getMessage());
                }
            }
            if (registration.decision == WorkerAssignmentLedger.Decision.DUPLICATE) {
                long admissionDeadline = requestContext == null
                        ? deadlineAfterMillis(Long.getLong(
                        "rowweave.control.frameDeadlineMs", 30_000L))
                        : requestContext.requestDeadlineNanos();
                WorkerAssignmentLedger.AdmissionWait observed =
                        assignments.awaitAdmissionUntil(
                                registration.slot, admissionDeadline);
                if (observed == WorkerAssignmentLedger.AdmissionWait.ADMITTED) {
                    return RpcMessage.ack();
                }
                if (observed == WorkerAssignmentLedger.AdmissionWait.REJECTED) {
                    return RpcMessage.jobResult(false, 0,
                            assignments.durableAdmissionError(registration.slot));
                }
                // A duplicate waiter deadline is transport-local; it must not publish a
                // business rejection or mutate the shared PREPARING slot.
                throw new IllegalStateException(
                        "duplicate assignment admission wait deadline expired");
            }
            if (registration.decision != WorkerAssignmentLedger.Decision.NEW) {
                String error = registration.decision == WorkerAssignmentLedger.Decision.CAPACITY_EXHAUSTED
                        ? "worker " + workerId + " " + registration.error
                        : registration.error;
                return RpcMessage.jobResult(false, 0, error);
            }
            ProviderSinkWriteSession.Admission providerAdmission = null;
            if (admittedProviderProxy != null) {
                try {
                    io.rowweave.DurableSinkProviderKey providerKey =
                            new io.rowweave.DurableSinkProviderKey(
                                    admittedProviderProxy.batchSinkDescriptor()
                                            .connectorType(),
                                    admittedProviderProxy.batchSinkDescriptor()
                                            .connectorVersion(),
                                    admittedProviderProxy.batchSinkDescriptor()
                                            .capabilityProfile());
                    io.rowweave.deployment.DurableBatchSinkProviderRegistry
                            providerRegistry = deploymentRuntime
                            .durableSinkProviderRegistry();
                    io.rowweave.DurableBatchSinkProviderContract contract =
                            providerRegistry.requireContract(providerKey);
                    io.rowweave.DurableBatchSinkProvider provider =
                            providerRegistry.loadProvider(
                                    providerKey, providerActionSupervisor,
                                    deadlineAfterMillis(contract.attemptBudget()
                                            .actionTimeoutMillis()));
                    providerAdmission = ProviderSinkWriteSession.admit(
                            provider, contract, admittedProviderProxy,
                            prepared.sinkAttemptIdentity(),
                            providerActionSupervisor,
                            this::beforeProviderActionForTesting);
                } catch (RuntimeException | LinkageError rejected) {
                    String error = "provider Sink admission rejected: "
                            + rejected.getMessage();
                    assignments.rejectAdmission(registration.slot, error);
                    return RpcMessage.jobResult(false, 0, error);
                }
            }
            if (durable != null) {
                RpcMessage receipt = deliverAssignmentAcceptance(req, durable);
                if (receipt.type() != RpcMessage.Type.ACK) {
                    String error = receipt.errorMessage() == null
                            ? "durable assignment receipt rejected" : receipt.errorMessage();
                    assignments.rejectAdmission(registration.slot, error);
                    return RpcMessage.jobResult(false, 0, error);
                }
            }
            final StageCommand command;
            ProviderSinkWriteSession providerSession = null;
            try {
                command = prepared == null ? decodedCommand
                        : portableArtifact
                        ? prepared.materializePortableArtifact(
                                portableRuntimeBindings,
                                portableRuntimeBindings,
                                bytes -> assignments.reserveResident(
                                        registration.slot, bytes))
                        : prepared.materialize(
                                objectTrustContext,
                                bytes -> assignments.reserveResident(
                                        registration.slot, bytes));
                command.attachObjectTrustContext(objectTrustContext);
                if (portableArtifact) {
                    validatePortableRuntimeGraph(command.portableRuntimeGraph(),
                            durable != null);
                    command.releasePortableRuntimeGraph();
                }
                if (durable != null) {
                    SourceAssignmentVerifier.requireMaterializedMatches(command);
                    bindIncrementalSourceRuntime(registration.slot, command);
                }
                StageThreadCostModel.requireWithin(
                        command, dataPlaneConfig, maxStageThreads);
                prepareShuffleAssignment(registration.slot, stageId, command);
                if (providerAdmission != null) {
                    if (!providerAdmission.matchesProxy(
                            command.providerOutputProxy())) {
                        throw new IllegalArgumentException(
                                "provider output changed after acceptance");
                    }
                    if (assignments.isCancellationRequested(registration.slot)) {
                        throw new IllegalStateException(
                                "stage assignment cancelled during provider admission");
                    }
                    providerSession = providerAdmission.open(
                            prepared.publicationId(),
                            prepared.publicationGeneration(),
                            bytes -> assignments.reserveResident(
                                    registration.slot, bytes),
                            () -> assignments.isCancellationRequested(
                                    registration.slot));
                    command.attachProviderRuntimeOutput(providerSession);
                }
                if (assignments.attachCommand(registration.slot, command)) {
                    if (providerSession != null) {
                        providerSession.rollbackCommit();
                    }
                    assignments.rejectAdmission(registration.slot,
                            "stage assignment cancelled during admission");
                    return RpcMessage.jobResult(false, 0,
                            "stage assignment cancelled during admission");
                }
            } catch (ProcessCapacityException stageCapacityFailure) {
                rollbackProviderSession(providerSession, stageCapacityFailure);
                return failMaterializationAfterRegistration(
                        registration.slot, durable, stageCapacityFailure.getMessage());
            } catch (io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted) {
                rollbackProviderSession(providerSession, exhausted);
                return failMaterializationAfterRegistration(
                        registration.slot, durable, exhausted.getMessage());
            } catch (Exception | LinkageError materializationFailure) {
                rollbackProviderSession(providerSession, materializationFailure);
                return failMaterializationAfterRegistration(registration.slot, durable,
                        "invalid stage command materialization: "
                                + materializationFailure.getMessage());
            }
            try {
                java.util.concurrent.Future<?> future = stagePool.submit(
                        () -> {
                            if (assignments.awaitStartPermission(registration.slot)) {
                                runAssignment(registration.slot, command);
                            }
                        });
                if (!assignments.attachFutureAndAdmit(registration.slot, future)) {
                    cancelOwnedRegistries(command, new IllegalStateException(
                            "stage assignment cancelled before admission"));
                    return RpcMessage.jobResult(false, 0,
                            "stage assignment cancelled before admission");
                }
            } catch (RuntimeException submitFailure) {
                String error = "stage executor rejected assignment: " + submitFailure.getMessage();
                assignments.rejectAdmission(registration.slot, error);
                cancelOwnedRegistries(command, new IllegalStateException(
                        "stage executor rejected assignment", submitFailure));
                if (durable != null) {
                    reportDurableTerminal(registration.slot,
                            io.rowweave.run.StageTerminalOutcome.FAILED,
                            0L, 0L, "COMMAND_EXECUTION", error,
                            io.rowweave.plan.StageExecutionTelemetry.notExecuted());
                }
                return RpcMessage.jobResult(false, 0, error);
            }
            return RpcMessage.ack();  // acknowledged at once, without waiting for execution
        }
        if (req.type() == RpcMessage.Type.RECONCILE_DURABLE_STAGE) {
            if (!incarnationId.equals(req.incarnationId())) {
                return RpcMessage.jobResult(false, 0, "worker incarnation mismatch");
            }
            String leaderError = assignmentLeaderError(req);
            if (leaderError != null) {
                return RpcMessage.jobResult(false, 0, leaderError);
            }
            final StageAssignmentToken token;
            final byte[] commandDigest = req.stageCommandDigest();
            try {
                token = StageAssignmentManifestCodec.decodeToken(
                        req.stageAssignmentTokenBytes());
                if (commandDigest == null
                        || commandDigest.length != io.rowweave.run.StageTerminalReceipt.DIGEST_BYTES
                        || token.runtimeStageId() != req.stageId()
                        || !workerId.equals(token.workerId())
                        || !incarnationId.equals(token.workerIncarnationId())
                        || !token.fence().equals(req.runAttemptFence())) {
                    throw new IllegalArgumentException("reconciliation envelope identity mismatch");
                }
            } catch (RuntimeException invalid) {
                return RpcMessage.jobResult(false, 0,
                        "invalid durable stage reconciliation: " + invalid.getMessage());
            }
            WorkerAssignmentLedger.Reconciliation reconciliation =
                    assignments.reconcile(req.stageId(), token, commandDigest);
            if (reconciliation.decision
                    == WorkerAssignmentLedger.ReconciliationDecision.ABSENT) {
                return RpcMessage.durableStageReconciledAbsent();
            }
            if (reconciliation.decision
                    == WorkerAssignmentLedger.ReconciliationDecision.CONFLICT) {
                return RpcMessage.jobResult(false, 0,
                        "durable stage reconciliation identity conflicts with worker ledger");
            }
            if (reconciliation.decision
                    == WorkerAssignmentLedger.ReconciliationDecision.ACTIVE) {
                if (!cancelStage(req.stageId(), req.runAttemptFence())) {
                    return RpcMessage.jobResult(false, 0,
                            "durable stage reconciliation drain timed out");
                }
                reconciliation = assignments.reconcile(req.stageId(), token, commandDigest);
            }
            String replayFailure = null;
            if (reconciliation.decision
                    == WorkerAssignmentLedger.ReconciliationDecision.TERMINAL_PENDING) {
                replayFailure = replayDurableTerminal(
                        reconciliation.slot, reconciliation.receipt);
                reconciliation = assignments.reconcile(req.stageId(), token, commandDigest);
            }
            if (reconciliation.decision
                    != WorkerAssignmentLedger.ReconciliationDecision.TERMINAL_ACKNOWLEDGED) {
                return RpcMessage.jobResult(false, 0,
                        "durable terminal report is not yet acknowledged"
                                + (replayFailure == null ? "" : ": " + replayFailure));
            }
            req.reserveResponseResidentBytes(
                    io.rowweave.run.StageAssignmentManifestCodec.MAX_TERMINAL_RECEIPT_FRAME_BYTES
                            + io.rowweave.plan.StageExecutionTelemetryCodec.MAX_FRAME_BYTES);
            return RpcMessage.durableStageReconciledTerminal(
                    reconciliation.receipt,
                    reconciliation.preparedSinkReceipt);
        }
        if (req.type() == RpcMessage.Type.CANCEL_STAGE) {
            if (!incarnationId.equals(req.incarnationId())) {
                return RpcMessage.ack();
            }
            try {
                if (cancelStage(req.stageId(), req.runAttemptFence())) {
                    return RpcMessage.ack();
                }
            } catch (IllegalArgumentException invalidFence) {
                return RpcMessage.jobResult(false, 0, invalidFence.getMessage());
            }
            return RpcMessage.jobResult(false, 0,
                    "stage " + req.stageId() + " cancellation timed out after " + cancelDrainMs + "ms");
        }
        if (req.type() == RpcMessage.Type.EXECUTE_PROVIDER_GROUP) {
            if (!incarnationId.equals(req.incarnationId())) {
                return RpcMessage.jobResult(false, 0,
                        "worker incarnation mismatch");
            }
            String leaderError = assignmentLeaderError(req);
            if (leaderError != null) {
                return RpcMessage.jobResult(false, 0, leaderError);
            }
            try {
                ProviderPreparedGroupCommandCodec.Command command =
                        ProviderPreparedGroupCommandCodec.decode(
                                req.commitPayload(), req::reserveResidentBytes);
                io.rowweave.commit.BatchSinkOutcome outcome =
                        ProviderPreparedGroupExecutor.execute(
                                command, req.commitPayload().length,
                                req.attemptId(), req.commitGeneration(),
                                deploymentRuntime.durableSinkProviderRegistry(),
                                providerActionSupervisor,
                                req::reserveResidentBytes, () -> !isOpen(),
                                this::beforeProviderActionForTesting);
                if (command.action == providerAckLossActionForTesting
                        && providerAckLossCountForTesting.getAndUpdate(
                        value -> value > 0 ? value - 1 : 0) > 0) {
                    return RpcMessage.jobResult(false, 0,
                            "provider group action ACK lost after apply");
                }
                req.reserveResponseResidentBytes(1024L);
                return RpcMessage.providerGroupResult(outcome);
            } catch (RuntimeException | Error providerFailure) {
                fencePoisonedProviderRuntime();
                return RpcMessage.jobResult(false, 0,
                        providerFailure.getMessage() == null
                                ? "provider group action failed"
                                : providerFailure.getMessage());
            }
        }
        if (req.type()
                == RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION) {
            if (!isOpen()) {
                return RpcMessage.jobResult(false, 0,
                        "worker is closing or closed");
            }
            if (!incarnationId.equals(req.incarnationId())) {
                return RpcMessage.jobResult(false, 0,
                        "worker incarnation mismatch");
            }
            String leaderError = assignmentLeaderError(req);
            if (leaderError != null) {
                return RpcMessage.jobResult(false, 0, leaderError);
            }
            try {
                deploymentRuntime.requireAdmissionOpen();
                IncrementalSourceActionCommand command =
                        IncrementalSourceActionCommandCodec.decode(
                                req.incrementalSourceActionBytes(),
                                req::reserveResidentBytes);
                io.rowweave.incremental.IncrementalSourceProviderContract
                        admitted = deploymentRuntime
                        .incrementalSourceProviderRegistry()
                        .requireContract(command.contract().providerKey());
                if (!admitted.equals(command.contract())) {
                    throw new IllegalArgumentException(
                            "incremental source command contract differs from deployment");
                }
                long actionDeadline = deadlineAfterMillis(
                        admitted.actionBudget().actionTimeoutMillis());
                boolean large = command.action()
                        == IncrementalSourceActionCommand.Action.MATERIALIZE;
                long sourceResidentBytes = large
                        ? admitted.actionBudget().maxLargeResidentBytes()
                        : admitted.actionBudget().maxNormalResidentBytes();
                try (io.rowweave.internal.IncrementalSourceControlLedger.Lease ignored =
                             large
                                     ? incrementalSourceControlLedger.acquireLarge(
                                             sourceResidentBytes, actionDeadline)
                                     : incrementalSourceControlLedger.acquireNormal(
                                             sourceResidentBytes, actionDeadline)) {
                    io.rowweave.IncrementalSourceProvider provider =
                            deploymentRuntime.incrementalSourceProviderRegistry()
                            .loadProvider(admitted.providerKey(),
                                    providerActionSupervisor, actionDeadline);
                    io.rowweave.incremental.IncrementalSourceActionExecutor executor =
                            new io.rowweave.incremental.IncrementalSourceActionExecutor(
                                    admitted, command.configuration(), provider,
                                    providerActionSupervisor);
                    req.reserveResponseResidentBytes(
                            admitted.actionBudget().maxLargeResidentBytes());
                    IncrementalSourceActionResult result;
                    if (command.action()
                            == IncrementalSourceActionCommand.Action.CAPTURE) {
                        result = IncrementalSourceActionResult.captured(
                                executor.capture(
                                        command.captureRequest(), actionDeadline));
                    } else {
                        result = IncrementalSourceActionResult.materialized(
                                executor.materialize(command.capturedTarget(),
                                        actionDeadline,
                                        req::reserveResidentBytes));
                    }
                    return RpcMessage.incrementalSourceActionResult(
                            IncrementalSourceActionResultCodec.encode(result));
                }
            } catch (RuntimeException | Error providerFailure) {
                fencePoisonedProviderRuntime();
                return RpcMessage.jobResult(false, 0,
                        providerFailure.getMessage() == null
                                ? "incremental source action failed"
                                : providerFailure.getMessage());
            }
        }
        if (req.type()
                == RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER
                || req.type()
                == RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST) {
            if (!isOpen()) {
                return RpcMessage.jobResult(false, 0,
                        "worker is closing or closed");
            }
            if (!incarnationId.equals(req.incarnationId())) {
                return RpcMessage.jobResult(false, 0,
                        "worker incarnation mismatch");
            }
            String leaderError = assignmentLeaderError(req);
            if (leaderError != null) {
                return RpcMessage.jobResult(false, 0, leaderError);
            }
            try {
                IncrementalControlMemberInstallCodec.Decoded command =
                        IncrementalControlMemberInstallCodec.decode(
                                req.commitPayload());
                io.rowweave.incremental.IncrementalControlMember control =
                        io.rowweave.incremental.IncrementalControlMemberCodec
                                .decode(command.controlFrame);
                if (!req.runAttemptFence().equals(control.fence())
                        || !req.attemptId().equals(control.publicationId())
                        || req.commitGeneration() != control.generation()) {
                    throw new IllegalArgumentException(
                            "incremental control install identity differs");
                }
                io.rowweave.commit.ManifestFileStore store =
                        new io.rowweave.commit.ManifestFileStore(
                                java.nio.file.Paths.get(command.domainPath));
                io.rowweave.commit.CommitManifest manifest =
                        io.rowweave.commit.CommitManifest.decode(
                                command.manifestFrame);
                if (!store.domainId().equals(manifest.domainId())) {
                    throw new IllegalArgumentException(
                            "incremental publication domain differs");
                }
                io.rowweave.incremental.IncrementalControlMemberCodec
                        .requirePublicationManifest(
                                command.controlFrame, manifest);
                if (req.type()
                        == RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST) {
                    store.publish(manifest);
                    return RpcMessage.ack();
                }
                java.nio.file.Path staged = store.stagingPath(
                        control.publicationId(),
                        io.rowweave.incremental.IncrementalControlMemberCodec
                                .MEMBER_NAME);
                try {
                    java.nio.file.Files.write(staged, command.controlFrame,
                            java.nio.file.StandardOpenOption.CREATE_NEW,
                            java.nio.file.StandardOpenOption.WRITE);
                } catch (java.nio.file.FileAlreadyExistsException replay) {
                    byte[] existing = java.nio.file.Files.readAllBytes(staged);
                    if (!java.security.MessageDigest.isEqual(
                            existing, command.controlFrame)) {
                        throw new IllegalStateException(
                                "incremental control staging conflict", replay);
                    }
                }
                store.forceStaged(staged);
                io.rowweave.commit.CommitManifest.Member member =
                        store.installMember(
                                control.generation(),
                                io.rowweave.incremental
                                        .IncrementalControlMemberCodec.MEMBER_NAME,
                                io.rowweave.incremental
                                        .IncrementalControlMemberCodec
                                        .SCHEMA_FINGERPRINT,
                                staged);
                req.reserveResponseResidentBytes(64L * 1024L);
                return RpcMessage.manifestMemberInstalled(
                        io.rowweave.commit.ManifestCommitWire
                                .encodeMember(member));
            } catch (RuntimeException | java.io.IOException failure) {
                return RpcMessage.jobResult(false, 0,
                        failure.getMessage() == null
                                ? "incremental control install failed"
                                : failure.getMessage());
            }
        }
        if (req.type() == RpcMessage.Type.COMMIT_PREPARED_OUTPUT
                || req.type() == RpcMessage.Type.RENEW_PREPARED_OUTPUT
                || req.type() == RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT
                || req.type() == RpcMessage.Type.FINALIZE_PREPARED_OUTPUT
                || req.type() == RpcMessage.Type.INSTALL_MANIFEST_MEMBER
                || req.type() == RpcMessage.Type.SEAL_MANIFEST_MEMBER
                || req.type() == RpcMessage.Type.PUBLISH_MANIFEST_GROUP) {
            if (!incarnationId.equals(req.incarnationId())) {
                return req.type() == RpcMessage.Type.RENEW_PREPARED_OUTPUT
                        ? null : RpcMessage.ack();  // a control message from an older generation must not touch the worker that replaced it
            }
            String leaderError = assignmentLeaderError(req);
            if (leaderError != null) {
                return req.type() == RpcMessage.Type.RENEW_PREPARED_OUTPUT
                        ? null : RpcMessage.jobResult(false, 0, leaderError);
            }
            try {
                if (req.type() == RpcMessage.Type.RENEW_PREPARED_OUTPUT) {
                    preparedOutputs.renew(
                            req.commitGroupId(), req.stageId(), req.runAttemptFence());
                    return null; // One-way lease notification; no ACK may accumulate on the socket.
                } else if (req.type() == RpcMessage.Type.COMMIT_PREPARED_OUTPUT) {
                    preparedOutputs.commit(
                            req.commitGroupId(), req.stageId(), req.runAttemptFence());
                } else if (req.type() == RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT) {
                    preparedOutputs.rollback(
                            req.commitGroupId(), req.stageId(), req.runAttemptFence());
                } else if (req.type() == RpcMessage.Type.INSTALL_MANIFEST_MEMBER) {
                    // install includes an irreversible data publication; reserve the bounded one-member
                    // response envelope before that side effect and before ManifestCommitWire allocates.
                    req.reserveResponseResidentBytes(64L * 1024L);
                    byte[] member = preparedOutputs.installManifestMember(
                            req.commitGroupId(), req.stageId(), req.attemptId(),
                            req.commitGeneration(), req.runAttemptFence());
                    return RpcMessage.manifestMemberInstalled(member);
                } else if (req.type() == RpcMessage.Type.SEAL_MANIFEST_MEMBER) {
                    preparedOutputs.sealManifestMember(
                            req.commitGroupId(), req.stageId(), req.attemptId(),
                            req.commitGeneration(), req.runAttemptFence());
                } else if (req.type() == RpcMessage.Type.PUBLISH_MANIFEST_GROUP) {
                    preparedOutputs.publishManifestGroup(
                            req.commitGroupId(), req.stageId(), req.attemptId(),
                            req.commitGeneration(), req.commitPayload(), req.runAttemptFence());
                } else {
                    preparedOutputs.finalizeCommit(
                            req.commitGroupId(), req.stageId(), req.runAttemptFence());
                }
                return RpcMessage.ack();
            } catch (RuntimeException | Error controlFailure) {
                return req.type() == RpcMessage.Type.RENEW_PREPARED_OUTPUT
                        ? null : RpcMessage.jobResult(false, 0, controlFailure.getMessage());
            }
        }
        return RpcMessage.ack();
    }

    private RpcMessage failMaterializationAfterRegistration(
            WorkerAssignmentLedger.Slot slot, DurableAssignmentEnvelope durable, String error) {
        if (durable != null) {
            assignments.rejectAdmission(slot, error);
            reportDurableTerminal(slot, io.rowweave.run.StageTerminalOutcome.FAILED,
                    0L, 0L, "COMMAND_MATERIALIZATION", error,
                    io.rowweave.plan.StageExecutionTelemetry.notExecuted());
        } else {
            assignments.rejectAdmission(slot, error);
        }
        return RpcMessage.jobResult(false, 0, error);
    }

    private static void validatePortableRuntimeGraph(
            io.rowweave.plan.PlanGraph graph, boolean durable) {
        if (graph == null) {
            throw new IllegalArgumentException(
                    "portable runtime graph is required");
        }
        io.rowweave.artifact.IncrementalRuntimeGate
                .rejectGenericExecution(graph);
        io.rowweave.plan.PlanOps.validateExecutableTypes(graph);
        io.rowweave.plan.OutputDestinationValidator.validate(graph);
        ClusterOutputValidator.validate(graph);
        ClusterPlanCapabilityValidator.validate(graph);
        if (durable) {
            for (int index = 0; index < graph.nodeCount(); index++) {
                io.rowweave.plan.PlanNode node = graph.node(index);
                if (node.kind() == io.rowweave.plan.PlanNode.Kind.SINK
                        && node.batchSinkDescriptor().capabilityProfile()
                        != io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .SIDE_EFFECT_FREE_V1) {
                    throw new IllegalArgumentException(
                            "durable portable Sink is not side-effect-free");
                }
            }
        }
    }

    private static void rollbackProviderSession(
            ProviderSinkWriteSession session, Throwable primary) {
        if (session == null) return;
        try {
            session.rollbackCommit();
        } catch (Throwable cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    private static void requireProviderAssignmentIdentity(
            PreparedStageCommand prepared, DurableAssignmentEnvelope durable,
            io.rowweave.ProviderBatchSink providerOutput) {
        if (providerOutput == null) return;
        io.rowweave.run.SinkAttemptIdentity identity =
                prepared.sinkAttemptIdentity();
        io.rowweave.run.StageAssignmentToken token = durable.token;
        if (identity == null || prepared.publicationId() == null
                || prepared.publicationId().isEmpty()
                || prepared.publicationGeneration() <= 0L
                || !identity.fence().equals(token.fence())
                || identity.assignmentGeneration()
                != token.manifestGeneration()
                || !identity.stageExecutionId().equals(
                token.stageExecutionId())
                || identity.logicalStage() != token.logicalStage()
                || identity.instanceIndex() != token.instanceIndex()
                || token.role() != io.rowweave.run.StageAssignmentRole.SINK
                && token.role()
                != io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH
                || !java.security.MessageDigest.isEqual(
                identity.descriptorDigest(),
                io.rowweave.BatchSinkDescriptorCodec.digest(
                        providerOutput.batchSinkDescriptor()))) {
            throw new IllegalArgumentException(
                    "provider Sink identity does not match assignment token");
        }
    }

    /** Installs both consumer digests and producer cohorts before the admission ACK. */
    private void prepareShuffleAssignment(WorkerAssignmentLedger.Slot slot,
                                          long stageId, StageCommand command) {
        java.util.List<io.rowweave.sched.net.ShuffleEndpoint> outbound =
                authorizedOutboundEndpoints(command);
        if (command.inboundAuthorizations() == null && outbound.isEmpty()) {
            return;
        }
        if (assignments.prepareOwned(slot,
                () -> buildPreparedShuffleAssignment(stageId, command, outbound))) {
            throw new IllegalStateException(
                    "stage assignment cancelled during shuffle prepare");
        }
    }

    private AutoCloseable buildPreparedShuffleAssignment(
            long stageId, StageCommand command,
            java.util.List<io.rowweave.sched.net.ShuffleEndpoint> outbound) {
        java.util.List<AutoCloseable> prepared = new java.util.ArrayList<>(2);
        boolean outboundPrepared = false;
        try {
            if (command.inboundAuthorizations() != null) {
                if (dataAuthorizations == null) {
                    throw new IllegalStateException(
                            "inbound shuffle authorization requires MTLS data plane");
                }
                prepared.add(dataAuthorizations.prepare(
                        "s" + stageId, command.runAttemptFence(),
                        command.inboundAuthorizations()));
            }
            if (!outbound.isEmpty()) {
                if (muxConnections == null) {
                    throw new IllegalStateException(
                            "outbound shuffle authorization requires MTLS data plane");
                }
                prepared.add(muxConnections.prepareOutboundCohorts(
                        "s" + stageId, command.runAttemptFence(), outbound));
                outboundPrepared = true;
            }
            return new PreparedShuffleAssignment(prepared);
        } catch (RuntimeException | Error failure) {
            try {
                closePrepared(prepared);
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (outboundPrepared && muxConnections != null) {
                try {
                    muxConnections.cancelOutboundAttempt(command.runAttemptFence());
                } catch (RuntimeException cancelFailure) {
                    failure.addSuppressed(cancelFailure);
                }
            }
            throw failure;
        }
    }

    static java.util.List<io.rowweave.sched.net.ShuffleEndpoint>
            authorizedOutboundEndpoints(StageCommand command) {
        java.util.LinkedHashSet<io.rowweave.sched.net.ShuffleEndpoint> endpoints =
                new java.util.LinkedHashSet<>();
        if (command == null) return new java.util.ArrayList<>();
        addAuthorized(endpoints, command.downstream());
        addAuthorized(endpoints, command.sinkEndpoint());
        addAuthorizedTargets(endpoints, command.downTargets());
        if (command.broadcastGroups() != null) {
            for (StageCommand.BroadcastGroup group : command.broadcastGroups()) {
                if (group != null) addAuthorizedTargets(endpoints, group.targets);
            }
        }
        return new java.util.ArrayList<>(endpoints);
    }

    private static void addAuthorized(
            java.util.Set<io.rowweave.sched.net.ShuffleEndpoint> destination,
            java.util.List<io.rowweave.sched.net.ShuffleEndpoint> endpoints) {
        if (endpoints == null) return;
        for (io.rowweave.sched.net.ShuffleEndpoint endpoint : endpoints) {
            addAuthorized(destination, endpoint);
        }
    }

    private static void addAuthorized(
            java.util.Set<io.rowweave.sched.net.ShuffleEndpoint> destination,
            io.rowweave.sched.net.ShuffleEndpoint endpoint) {
        if (endpoint != null && endpoint.producerGrant() != null) {
            destination.add(endpoint);
        }
    }

    private static void addAuthorizedTargets(
            java.util.Set<io.rowweave.sched.net.ShuffleEndpoint> destination,
            java.util.List<ShuffleTarget> targets) {
        if (targets == null) return;
        for (ShuffleTarget target : targets) {
            if (target != null && !target.isLocal()) {
                addAuthorized(destination, target.endpoint());
            }
        }
    }

    private static void closePrepared(java.util.List<AutoCloseable> prepared) {
        RuntimeException failure = null;
        for (int i = prepared.size() - 1; i >= 0; i--) {
            try {
                prepared.get(i).close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            } catch (Exception closeFailure) {
                RuntimeException wrapped = new RuntimeException(
                        "failed to close prepared shuffle assignment", closeFailure);
                if (failure == null) failure = wrapped;
                else failure.addSuppressed(wrapped);
            }
        }
        if (failure != null) throw failure;
    }

    private static final class PreparedShuffleAssignment implements AutoCloseable {
        private java.util.List<AutoCloseable> prepared;

        PreparedShuffleAssignment(java.util.List<AutoCloseable> prepared) {
            this.prepared = new java.util.ArrayList<>(prepared);
        }

        @Override
        public synchronized void close() {
            if (prepared == null) return;
            java.util.List<AutoCloseable> owned = prepared;
            prepared = null;
            closePrepared(owned);
        }
    }

    static boolean allowsLegacyStageCommand(
            io.rowweave.cluster.rpc.ControlSecurityProfile profile, boolean durable) {
        return profile == io.rowweave.cluster.rpc.ControlSecurityProfile.LOOPBACK && !durable;
    }

    private DurableAssignmentEnvelope durableEnvelope(RpcMessage request) {
        byte[] encoded = request.stageAssignmentTokenBytes();
        if (encoded == null) return null;
        if (request.stageCommandBlob() == null) {
            throw new IllegalArgumentException(
                    "durable assignment requires canonical command bytes");
        }
        StageAssignmentToken token = StageAssignmentManifestCodec.decodeToken(encoded);
        if (token.runtimeStageId() != request.stageId()
                || !token.workerId().equals(workerId)
                || !token.workerIncarnationId().equals(incarnationId)
                || !token.fence().equals(request.runAttemptFence())) {
            throw new IllegalArgumentException("assignment token does not match RPC envelope");
        }
        return new DurableAssignmentEnvelope(token, encoded, sha256(request.stageCommandBlob()));
    }

    private RpcMessage deliverAssignmentAcceptance(
            RpcMessage assignment, DurableAssignmentEnvelope durable) {
        RpcMessage request = RpcMessage.assignmentAccepted(
                workerId, assignment.stageId(), durable.encodedToken, durable.commandDigest,
                incarnationId, assignment.runAttemptFence());
        java.util.concurrent.atomic.AtomicReference<RpcMessage> response =
                new java.util.concurrent.atomic.AtomicReference<>();
        CriticalReportRetrier.Result delivered = CriticalReportRetrier.deliver(() -> {
            Endpoint leader = masterLocator.locateLeader();
            RpcMessage current = reportCache.call(leader.host(), leader.port(), request);
            if (current != null && current.type() == RpcMessage.Type.ACK) {
                response.set(current);
                return;
            }
            String error = current == null ? "null response" : current.errorMessage();
            if (error != null && error.startsWith("assignment acceptance unavailable:")) {
                throw new IllegalStateException(error);
            }
            response.set(current == null
                    ? RpcMessage.jobResult(false, 0, "assignment acceptance unavailable: null response")
                    : current);
        }, () -> reportStopping || lifecycle != Lifecycle.OPEN
                || Thread.currentThread().isInterrupted(), Thread::sleep);
        if (delivered.delivered() && response.get() != null) {
            return response.get();
        }
        return RpcMessage.jobResult(false, 0,
                "assignment acceptance unavailable: worker is stopping");
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static final class DurableAssignmentEnvelope {
        final StageAssignmentToken token;
        final byte[] encodedToken;
        final byte[] commandDigest;

        DurableAssignmentEnvelope(StageAssignmentToken token,
                                  byte[] encodedToken, byte[] commandDigest) {
            this.token = token;
            this.encodedToken = encodedToken.clone();
            this.commandDigest = commandDigest.clone();
        }
    }

    /**
     * A cancellation for a stage this worker does not know leaves a negative tombstone; for a stage already
     * accepted it waits for the executing thread to actually exit.
     */
    private boolean cancelStage(long stageId, io.rowweave.run.RunAttemptFence runAttemptFence) {
        WorkerAssignmentLedger.Cancellation cancellation =
                assignments.cancel(stageId, runAttemptFence);
        rollbackPreparedOutputForCancellation(cancellation);
        RuntimeException cause = new IllegalStateException("stage " + stageId + " cancelled");
        cancelOwnedRegistries(cancellation.command, cause);
        if (cancellation.task != null) {
            cancellation.task.stop(StageTask.StopReason.CANCELLED);
        }
        if (cancellation.future != null) {
            cancellation.future.cancel(true);
        }
        if (cancellation.reportCancelled) {
            if (cancellation.slot.durableAssignment) {
                reportDurableTerminal(cancellation.slot,
                        io.rowweave.run.StageTerminalOutcome.CANCELED,
                        0L, 0L, "CANCELED", "stage cancelled",
                        io.rowweave.plan.StageExecutionTelemetry.notExecuted());
            } else if (assignments.claimReport(cancellation.slot)) {
                deliverEphemeralTerminal(cancellation.slot, false, 0, 0,
                        "stage cancelled",
                        io.rowweave.plan.StageExecutionTelemetry.notExecuted());
            }
        }
        long deadline = lifecycle == Lifecycle.OPEN ? deadlineAfterMillis(cancelDrainMs) : closeDeadlineNanos;
        return assignments.awaitTerminalUntil(cancellation.slot, deadline);
    }

    private void rollbackPreparedOutputForCancellation(
            WorkerAssignmentLedger.Cancellation cancellation) {
        StageCommand command = cancellation.command;
        String groupId = command == null ? null : command.commitGroupId();
        if (groupId == null) {
            preparedOutputs.rollbackStage(
                    cancellation.slot.stageId, cancellation.slot.runAttemptFence);
        } else {
            // Cancellation can win after Sink.close but before register().
            // The known group creates a tombstone that rejects the late handoff.
            preparedOutputs.rollback(groupId,
                    cancellation.slot.stageId, cancellation.slot.runAttemptFence);
        }
    }

    /**
     * A worker registered for high availability accepts an assignment only when its endpoint and term match
     * the role record that is current.
     */
    private String assignmentLeaderError(RpcMessage req) {
        if (!(masterLocator instanceof RegistryMasterLocator)) {
            return null;
        }
        if (req.host() == null || req.port() <= 0) {
            return "missing active leader claim on stage assignment";
        }
        final RegistryMasterLocator.LeaderLease current;
        try {
            current = ((RegistryMasterLocator) masterLocator).locateLeaderLease();
        } catch (RuntimeException noLeader) {
            return "no active leader for stage assignment";
        }
        if (!java.util.Objects.equals(req.host(), current.endpoint.host())
                || req.port() != current.endpoint.port()) {
            return "stale leader endpoint on stage assignment: claimed=" + req.host() + ":" + req.port()
                    + " current=" + current.endpoint;
        }
        if (req.leaderTermToken() != current.termToken) {
            return "stale leader term on stage assignment";
        }
        return null;
    }

    /**
     * Cancellation happens in two phases so that stopping never waits on one pipeline stage while the
     * stages feeding it have not been stopped yet, which would be mutual waiting.
     */
    private boolean cancelAssignmentsFromPreviousLeader() {
        java.util.List<WorkerAssignmentLedger.Cancellation> cancellations = new java.util.ArrayList<>();
        long deadline = deadlineAfterMillis(cancelDrainMs);
        RuntimeException cause = new IllegalStateException("master leadership changed");
        for (WorkerAssignmentLedger.Slot slot : assignments.activeSlots()) {
            WorkerAssignmentLedger.Cancellation cancellation =
                    assignments.cancel(slot.stageId, slot.runAttemptFence);
            rollbackPreparedOutputForCancellation(cancellation);
            // On the ephemeral path the cancellation owner still swallows reports for the old stage; a durable slot
            // has to let the real thread record the first terminal state.
            if (!cancellation.slot.durableAssignment) {
                assignments.claimReport(cancellation.slot);
            }
            cancelOwnedRegistries(cancellation.command, cause);
            if (cancellation.task != null) {
                cancellation.task.stop(StageTask.StopReason.CANCELLED);
            }
            if (cancellation.future != null) {
                cancellation.future.cancel(true);
            }
            cancellations.add(cancellation);
        }
        boolean allTerminal = true;
        for (WorkerAssignmentLedger.Cancellation cancellation : cancellations) {
            if (!assignments.awaitTerminalUntil(cancellation.slot, deadline)) {
                allTerminal = false;
            }
            if (cancellation.slot.durableAssignment) {
                reportDurableTerminal(cancellation.slot,
                        io.rowweave.run.StageTerminalOutcome.CANCELED,
                        0L, 0L, "LEADER_CHANGED", "master leadership changed",
                        io.rowweave.plan.StageExecutionTelemetry.unavailable());
            } else {
                // Legacy report ownership is deliberately swallowed across leader terms.
                // Drop the small frozen value once the execution owner has stopped.
                assignments.clearTelemetry(cancellation.slot);
            }
        }
        return allTerminal;
    }

    /** Saturating monotonic deadline; configuration may legally be Long.MAX_VALUE milliseconds. */
    private static long deadlineAfterMillis(long timeoutMs) {
        long now = System.nanoTime();
        long budget = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        if (budget > 0L && now > Long.MAX_VALUE - budget) {
            return Long.MAX_VALUE;
        }
        return now + budget;
    }

    /** Observation for tests: how many stage tasks are running right now. */
    public int runningStageCount() {
        return assignments.activeCount();
    }

    /**
     * The worker's single data port, above zero when multiplexing is on and -1 when it is off. The first
     * call starts the data server, and the port travels with the registration so the master can place
     * producers against it.
     */
    public synchronized int dataPort() {
        synchronized (closeLifecycle) {
            requireOpen();
            if (dataInbound == null) {
                return -1; // mux OFF
            }
            if (dataServer == null) {
                dataServer = localDataIdentity == null
                        ? new io.rowweave.sched.net.WorkerDataServer(
                                dataInbound, dataServerAwaitTimeoutMs, 0, dataPlaneConfig)
                        : new io.rowweave.sched.net.WorkerDataServer(
                                dataInbound, dataServerAwaitTimeoutMs, 0, dataPlaneConfig,
                                dataPlaneSecurityContext, dataPlaneHandshakeAdmission,
                                localDataIdentity, dataAuthorizations);
            }
            return dataServer.port();
        }
    }

    private static java.util.Properties systemPropertiesSnapshot() {
        java.util.Properties snapshot = new java.util.Properties();
        snapshot.putAll(System.getProperties());
        return snapshot;
    }

    private static long longProperty(java.util.Properties properties, String key, long defaultValue) {
        String configured = properties.getProperty(key);
        if (configured == null || configured.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(configured.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be a long", invalid);
        }
    }

    private void bindIncrementalSourceRuntime(
            WorkerAssignmentLedger.Slot slot, StageCommand command) {
        io.rowweave.Source frozenInput = command.input();
        final java.util.List<io.rowweave.BatchSourceSplitBinding> frozen;
        if (frozenInput instanceof io.rowweave.IncrementalSourceSplit) {
            io.rowweave.IncrementalSourceSplit proxy =
                    (io.rowweave.IncrementalSourceSplit) frozenInput;
            frozen = java.util.Collections.singletonList(
                    new io.rowweave.BatchSourceSplitBinding(
                            proxy, proxy.batchSourceSplit()));
        } else if (frozenInput instanceof io.rowweave.AssignedSourceBatch) {
            frozen = ((io.rowweave.AssignedSourceBatch) frozenInput)
                    .bindings();
            if (frozen.isEmpty()
                    || !(frozen.get(0).input()
                    instanceof io.rowweave.IncrementalSourceSplit)) {
                return;
            }
        } else {
            return;
        }
        io.rowweave.incremental.IncrementalPlanDescriptor descriptor = null;
        io.rowweave.plan.PlanGraph fragment = command.fragment();
        if (fragment != null) {
            for (int index = 0; index < fragment.nodeCount(); index++) {
                io.rowweave.plan.PlanNode node = fragment.node(index);
                if (node.kind() == io.rowweave.plan.PlanNode.Kind.SOURCE
                        && node.hasIncrementalPlanDescriptor()) {
                    if (descriptor != null) {
                        throw new IllegalArgumentException(
                                "incremental source stage has multiple descriptors");
                    }
                    descriptor = node.incrementalPlanDescriptor();
                }
            }
        }
        java.util.List<io.rowweave.BatchSourceSplitBinding> runtimeBindings =
                new java.util.ArrayList<>(frozen.size());
        for (int index = 0; index < frozen.size(); index++) {
            io.rowweave.BatchSourceSplitBinding binding = frozen.get(index);
            if (!(binding.input()
                    instanceof io.rowweave.IncrementalSourceSplit)) {
                throw new IllegalArgumentException(
                        "incremental source batch mixes input kinds");
            }
            io.rowweave.IncrementalSourceSplit proxy =
                    (io.rowweave.IncrementalSourceSplit) binding.input();
            io.rowweave.incremental.IncrementalPlanDescriptor frozenDescriptor =
                    proxy.planDescriptor();
            if (descriptor == null) {
                descriptor = frozenDescriptor;
            } else if (frozenDescriptor != null
                    && !descriptor.equals(frozenDescriptor)) {
                throw new IllegalArgumentException(
                        "incremental source descriptor differs from command");
            }
            if (descriptor == null) {
                throw new IllegalArgumentException(
                        "incremental source stage descriptor is missing");
            }
            io.rowweave.deployment.IncrementalSourceProviderRegistry registry =
                    deploymentRuntime.incrementalSourceProviderRegistry();
            io.rowweave.incremental.IncrementalSourceProviderContract contract =
                    registry.requireContract(proxy.configuration().providerKey());
            long deadline = deadlineAfterMillis(
                    contract.actionBudget().actionTimeoutMillis());
            io.rowweave.IncrementalSourceProvider provider =
                    registry.loadProvider(
                            proxy.configuration().providerKey(),
                            providerActionSupervisor, deadline);
            io.rowweave.incremental.IncrementalChangelogRowValidator validator =
                    new io.rowweave.incremental.IncrementalChangelogRowValidator(
                            proxy.getSchema(), descriptor,
                            proxy.capturedTarget(), proxy.partitionSetDigest(),
                            proxy.partitionOrdinal());
            io.rowweave.BatchExecutableSource runtime =
                    io.rowweave.IncrementalSourceRuntime.bind(
                            binding, contract, proxy.configuration(),
                            proxy.capturedTarget(), proxy.partitionSetDigest(),
                            provider, providerActionSupervisor, deadline,
                            () -> assignments.isCancellationRequested(slot),
                            validator);
            runtimeBindings.add(new io.rowweave.BatchSourceSplitBinding(
                    runtime, binding.descriptor()));
        }
        io.rowweave.Source runtimeInput = runtimeBindings.size() == 1
                && frozenInput instanceof io.rowweave.IncrementalSourceSplit
                ? runtimeBindings.get(0).input()
                : new io.rowweave.AssignedSourceBatch(runtimeBindings);
        command.attachIncrementalRuntimeInput(runtimeInput);
    }

    /**
     * Runs a stage as a thread in this process. A port bound during startup reaches the master as a report
     * through the callback, which is a thread notification rather than a printed line, and the result
     * arrives as a stage report, which replaces the printed row count and the exit code. Reports use a
     * connection of their own.
     */
    private void runAssignment(WorkerAssignmentLedger.Slot slot, StageCommand cmd) {
        if (!assignments.tryStart(slot)) {
            return;
        }
        try {
            reserveNestedPlanMaterialization(slot, cmd);
            if (assignments.attachCommand(slot, cmd)) {
                cancelOwnedRegistries(cmd, new IllegalStateException("stage " + slot.stageId + " cancelled"));
                reportCancelledOnce(slot);
                return;
            }
            runStageWith(slot, cmd);
        } catch (Throwable t) {
            if (slot.durableAssignment) {
                reportDurableTerminal(slot, io.rowweave.run.StageTerminalOutcome.FAILED,
                        0L, 0L, "COMMAND_EXECUTION", describeFailure(t),
                        io.rowweave.plan.StageExecutionTelemetry.notExecuted());
            } else if (assignments.claimReport(slot)) {
                deliverEphemeralTerminal(slot, false, 0, 0,
                        "bad stage command: " + t.getMessage(),
                        io.rowweave.plan.StageExecutionTelemetry.notExecuted());
            }
        } finally {
            assignments.complete(slot);
            fencePoisonedProviderRuntime();
        }
    }

    private void fencePoisonedProviderRuntime() {
        if (!providerActionSupervisor.isPoisoned()
                || !providerPoisonFenceStarted.compareAndSet(false, true)) {
            return;
        }
        Thread fence = new Thread(() -> {
            try {
                close();
            } catch (Throwable failure) {
                LOG.error("poisoned provider Worker close failed worker={}",
                        workerId, failure);
            }
        }, "rowweave-provider-poison-fence");
        fence.setDaemon(true);
        fence.start();
    }

    private void reserveNestedPlanMaterialization(
            WorkerAssignmentLedger.Slot slot, StageCommand command) {
        command.attachResidentReservation(bytes -> assignments.reserveResident(slot, bytes));
    }

    /** Executes a stage command already resolved, the same path whether the object arrived in-process or was read from a socket. */
    private void runStageWith(WorkerAssignmentLedger.Slot slot, StageCommand cmd) {
        long stageId = slot.stageId;
        // The port callback reports from the thread once it has bound, naming the edge: 0 for the single or
        // left inbound, 1 for the right.
        StageTask task = new StageTask(cmd,
                (port, edge) -> deliverReport(RpcMessage.portReport(
                        workerId, stageId, port, edge, incarnationId, slot.runAttemptFence)));
        task.setLocalShuffle(localShuffle);  // the same-worker local channel rendezvous
        if (dataInbound != null) {  // multiplexing is on, so the single-port registry is injected; entries are grouped per stage and reclaimed at a terminal state
            task.setDataInbound(dataInbound, muxConnections, "s" + stageId);
        }
        // The progress callback reports periodically: rows handled in, rows produced, and whether the stage is
        // waiting for data or working.
        task.setProgressListener((in, out, running, ft) ->
                deliverReport(RpcMessage.progressReport(
                        workerId, stageId, in, out, running, incarnationId, slot.runAttemptFence)));
        if (assignments.attachTask(slot, task)) {
            cancelOwnedRegistries(cmd, new IllegalStateException("stage " + stageId + " cancelled"));
            task.stop(StageTask.StopReason.CANCELLED);
        }
        try {
            StageTask.StageResult result = task.run();
            // Freeze immediately on return from the execution owner. A later prepared-output
            // handoff failure/cancellation must not rewrite an executed stage as NOT_EXECUTED.
            io.rowweave.plan.StageExecutionTelemetry terminalTelemetry =
                    assignments.freezeTelemetry(slot, result.telemetry());
            if (result.reason() == StageTask.StopReason.EOS) {
                io.rowweave.run.PreparedSinkReceipt providerPrepared = null;
                if (cmd.output() instanceof ProviderSinkWriteSession) {
                    providerPrepared = new io.rowweave.run.PreparedSinkReceipt(
                            slot.durableToken, slot.durableCommandDigest,
                            ((ProviderSinkWriteSession) cmd.output())
                                    .preparedLocator());
                } else if (cmd.role() == StageCommand.Role.SINK
                        && cmd.commitGroupId() != null) {
                    if (!preparedOutputs.register(
                            cmd.commitGroupId(), stageId, cmd.output(), slot.runAttemptFence)) {
                        throw new IllegalStateException(
                                "prepared output group was already rolled back: " + cmd.commitGroupId());
                    }
                }
                // Input is the rows read from upstream; output is the rows the result reports.
                if (slot.durableAssignment) {
                    reportDurableTerminal(slot, io.rowweave.run.StageTerminalOutcome.SUCCEEDED,
                            task.inputRows(), result.rows(), null, null,
                            terminalTelemetry, providerPrepared);
                } else if (assignments.claimReport(slot)) {
                    deliverEphemeralTerminal(slot, true, task.inputRows(),
                            result.rows(), null, terminalTelemetry);
                }
            } else {
                String err = result.reason() == StageTask.StopReason.CANCELLED
                        ? "stage cancelled"
                        : (result.failure() != null ? describeFailure(result.failure()) : "stage failed");
                if (slot.durableAssignment) {
                    reportDurableTerminal(slot,
                            result.reason() == StageTask.StopReason.CANCELLED
                                    ? io.rowweave.run.StageTerminalOutcome.CANCELED
                                    : io.rowweave.run.StageTerminalOutcome.FAILED,
                            task.inputRows(), 0L,
                            result.reason() == StageTask.StopReason.CANCELLED
                                    ? "CANCELED" : "STAGE_FAILED", err,
                            terminalTelemetry);
                } else if (assignments.claimReport(slot)) {
                    deliverEphemeralTerminal(slot, false, task.inputRows(), 0,
                            err, terminalTelemetry);
                }
            }
        } finally {
            // The stage task has closed its inbound already. A slot left by a local exchange that succeeded keeps a
            // short time-to-live so a late rendezvous is still answered, while a cancelled tombstone drops its
            // large references at once.
            localShuffle.unregister(cmd.localInboundKey());
            localShuffle.unregister(cmd.localInboundKeyRight());
            if (dataInbound != null) {
                dataInbound.unregisterJob("s" + stageId);  // reclaim the routes this stage registered, so a long-lived table cannot grow
            }
        }
    }

    /** Lets a test or a diagnostic see the metadata of the long-lived rendezvous without exposing the channel. */
    int rendezvousEntryCount() {
        return localShuffle.entryCount() + (dataInbound == null ? 0 : dataInbound.entryCount());
    }

    int preparedOutputActiveCount() {
        return preparedOutputs.activeCount();
    }

    int preparedOutputEntryCount() {
        return preparedOutputs.entryCount();
    }

    private void reportCancelledOnce(WorkerAssignmentLedger.Slot slot) {
        if (slot.durableAssignment) {
            reportDurableTerminal(slot, io.rowweave.run.StageTerminalOutcome.CANCELED,
                    0L, 0L, "CANCELED", "stage cancelled",
                    io.rowweave.plan.StageExecutionTelemetry.notExecuted());
        } else if (assignments.claimReport(slot)) {
            deliverEphemeralTerminal(slot, false, 0, 0, "stage cancelled",
                    io.rowweave.plan.StageExecutionTelemetry.notExecuted());
        }
    }

    private boolean deliverEphemeralTerminal(
            WorkerAssignmentLedger.Slot slot, boolean success,
            long inputRows, long outputRows, String error,
            io.rowweave.plan.StageExecutionTelemetry telemetry) {
        io.rowweave.plan.StageExecutionTelemetry frozen =
                assignments.freezeTelemetry(slot, telemetry);
        try {
            return deliverReport(RpcMessage.stageReportWithTelemetry(
                    workerId, slot.stageId, success, inputRows, outputRows, error,
                    incarnationId, slot.runAttemptFence, frozen));
        } finally {
            assignments.clearTelemetry(slot);
        }
    }

    private void reportDurableTerminal(WorkerAssignmentLedger.Slot slot,
                                       io.rowweave.run.StageTerminalOutcome outcome,
                                       long inputRows, long outputRows,
                                       String failureCategory, String failureDetail,
                                       io.rowweave.plan.StageExecutionTelemetry telemetry) {
        reportDurableTerminal(slot, outcome, inputRows, outputRows,
                failureCategory, failureDetail, telemetry, null);
    }

    private void reportDurableTerminal(WorkerAssignmentLedger.Slot slot,
                                       io.rowweave.run.StageTerminalOutcome outcome,
                                       long inputRows, long outputRows,
                                       String failureCategory, String failureDetail,
                                       io.rowweave.plan.StageExecutionTelemetry telemetry,
                                       io.rowweave.run.PreparedSinkReceipt preparedSinkReceipt) {
        io.rowweave.plan.StageExecutionTelemetry frozenTelemetry =
                assignments.freezeTelemetry(slot, telemetry);
        final io.rowweave.run.StageTerminalReceipt candidate;
        long completedAt = System.currentTimeMillis();
        if (outcome == io.rowweave.run.StageTerminalOutcome.SUCCEEDED) {
            candidate = io.rowweave.run.StageTerminalReceipt.succeeded(
                    slot.durableToken, slot.durableCommandDigest,
                    inputRows, outputRows, completedAt, frozenTelemetry);
        } else {
            byte[] failureDigest = sha256((failureDetail == null ? failureCategory : failureDetail)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            candidate = outcome == io.rowweave.run.StageTerminalOutcome.CANCELED
                    ? io.rowweave.run.StageTerminalReceipt.canceled(
                    slot.durableToken, slot.durableCommandDigest,
                    inputRows, outputRows, failureCategory, failureDigest, completedAt,
                    frozenTelemetry)
                    : io.rowweave.run.StageTerminalReceipt.failed(
                    slot.durableToken, slot.durableCommandDigest,
                    inputRows, outputRows, failureCategory, failureDigest, completedAt,
                    frozenTelemetry);
        }
        io.rowweave.run.StageTerminalReceipt frozen;
        try {
            frozen = assignments.freezeTerminal(
                    slot, candidate, preparedSinkReceipt);
        } finally {
            assignments.clearTelemetry(slot);
        }
        io.rowweave.run.PreparedSinkReceipt frozenPrepared =
                assignments.preparedSinkReceipt(slot);
        if (assignments.claimReport(slot)) {
            if (deliverReport(RpcMessage.durableStageReport(
                    workerId, frozen, frozenPrepared))) {
                assignments.acknowledgeTerminalReport(
                        slot, frozen, frozenPrepared);
            } else {
                assignments.releaseTerminalReportClaim(slot, frozen);
            }
        }
    }

    private String replayDurableTerminal(WorkerAssignmentLedger.Slot slot,
                                         io.rowweave.run.StageTerminalReceipt receipt) {
        if (receipt == null) {
            return "terminal receipt is absent";
        }
        io.rowweave.run.PreparedSinkReceipt preparedSinkReceipt =
                assignments.preparedSinkReceipt(slot);
        if (assignments.claimReport(slot)) {
            if (deliverReport(RpcMessage.durableStageReport(
                    workerId, receipt, preparedSinkReceipt))) {
                assignments.acknowledgeTerminalReport(
                        slot, receipt, preparedSinkReceipt);
                return null;
            } else {
                assignments.releaseTerminalReportClaim(slot, receipt);
                return "terminal delivery stopped before ACK";
            }
        }
        // A leader-change retry may still own the ordinary report claim. Recovery cannot
        // wait for that thread to win a race with the newly-established ownership token:
        // make one duplicate delivery of the immutable receipt. The authority is
        // idempotent, and reconciliation still returns TERMINAL only after this or the
        // concurrent owner observes an ACK.
        try {
            callLeader(reportCache, RpcMessage.durableStageReport(
                    workerId, receipt, preparedSinkReceipt));
            assignments.acknowledgeTerminalReport(
                    slot, receipt, preparedSinkReceipt);
            return null;
        } catch (RuntimeException unavailable) {
            // The Master-side reconciliation loop owns bounded retry/backoff.
            return unavailable.getMessage();
        }
    }

    /**
     * Cancels both the inbound this stage owns and the local outbound rendezvous a producer may be waiting on.
     */
    private void cancelOwnedRegistries(StageCommand cmd, RuntimeException cause) {
        if (cmd == null) {
            return;
        }
        if (dataInbound != null) {
            if (cmd.inboundTargetId() != null) {
                dataInbound.cancel(cmd.inboundTargetId(), cause);
            }
            if (cmd.inboundTargetIdRight() != null) {
                dataInbound.cancel(cmd.inboundTargetIdRight(), cause);
            }
        }
        if (muxConnections != null && cmd.runAttemptFence() != null) {
            muxConnections.cancelOutboundAttempt(cmd.runAttemptFence());
        }
        cancelLocalKey(cmd.localInboundKey(), cause);
        cancelLocalKey(cmd.localInboundKeyRight(), cause);
        cancelLocalTargets(cmd.downTargets(), cause);
        if (cmd.broadcastGroups() != null) {
            for (StageCommand.BroadcastGroup group : cmd.broadcastGroups()) {
                if (group != null) {
                    cancelLocalTargets(group.targets, cause);
                }
            }
        }
    }

    private void cancelLocalTargets(java.util.List<ShuffleTarget> targets, RuntimeException cause) {
        if (targets == null) {
            return;
        }
        for (ShuffleTarget target : targets) {
            if (target != null && target.isLocal()) {
                cancelLocalKey(target.localKey(), cause);
            }
        }
    }

    private void cancelLocalKey(String key, RuntimeException cause) {
        if (key != null) {
            localShuffle.cancel(key, cause);
        }
    }

    /**
     * Flattens the cause chain of a failure. What the cluster reports for a stage is its message, and the
     * wrapper at the top of a pipeline would otherwise hide what actually went wrong, so each layer's text
     * is joined.
     */
    private static String describeFailure(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && sb.length() < 500; c = c.getCause()) {
            if (sb.length() > 0) sb.append(" <- ");
            sb.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage());
            if (c.getCause() == c) break;
        }
        return sb.toString();
    }

    /**
     * Migration compatibility for ephemeral loopback only: a legacy stage command blob is deserialized
     * against a whitelist.
     */
    private StageCommand deserializeCommand(RpcMessage request) throws Exception {
        byte[] blob = request.stageCommandBlob();
        if (request.residentLease() != null) {
            request.residentLease().reserve(request.residentOwnerToken(), 3L * blob.length);
        }
        try (WhitelistObjectInputStream in = request.residentLease() == null
                ? new WhitelistObjectInputStream(new ByteArrayInputStream(blob))
                : new WhitelistObjectInputStream(new ByteArrayInputStream(blob),
                request.residentLease(), request.residentOwnerToken())) {
            try {
                StageCommand command = (StageCommand) in.readObject();
                if (in.residentFailure() != null) throw in.residentFailure();
                return command;
            } catch (Exception failure) {
                if (in.residentFailure() != null) throw in.residentFailure();
                throw failure;
            }
        }
    }

    /** Port and stage reports go to the leader resolved at that moment, so after a failover they reach the new one. */
    boolean deliverReport(RpcMessage msg) {
        // Two kinds of report are treated differently, and deliberately so. A progress report is monitoring
        // only and may be lost, so it is sent one-way with no frame back and the next tick repairs it. A stage
        // or port report is load-bearing: losing one would leave the barrier that awaits the job hanging
        // forever, since a job carries no deadline unless one is set. Those keep the call and its
        // acknowledgement and retry with exponential backoff while the worker lives. Every attempt resolves
        // the leader again, so a failover mid-retry is tolerated.
        if (msg.type() == RpcMessage.Type.PROGRESS_REPORT) {
            if (!isOpen()) {
                return false;
            }
            try {
                sendLeader(reportCache, msg);
            } catch (RuntimeException ignored) {
                // Monitoring that may be dropped: the next tick, three hundred milliseconds away, repeats it.
            }
            return true;
        }
        if (msg.type() != RpcMessage.Type.STAGE_REPORT && msg.type() != RpcMessage.Type.PORT_REPORT) {
            throw new IllegalArgumentException("unsupported worker report type: " + msg.type());
        }
        java.util.concurrent.atomic.AtomicBoolean warned = new java.util.concurrent.atomic.AtomicBoolean();
        // Cancellation interrupts the stage's owner thread, but the terminal report must still be attempted at
        // least once before an interrupt is allowed to end the retries.
        java.util.concurrent.atomic.AtomicBoolean attempted = new java.util.concurrent.atomic.AtomicBoolean();
        CriticalReportRetrier.Result result = CriticalReportRetrier.deliver(() -> {
            attempted.set(true);
            try {
                callLeader(reportCache, msg);
            } catch (RuntimeException failure) {
                if (warned.compareAndSet(false, true)) {
                    LOG.warn("critical report delivery failed; retrying until ACK type={} stageId={}",
                            msg.type(), msg.stageId(), failure);
                }
                throw failure;
            }
        }, () -> reportStopping || (attempted.get() && Thread.currentThread().isInterrupted())
                || (lifecycle == Lifecycle.CLOSING && System.nanoTime() >= closeDeadlineNanos), Thread::sleep);
        if (result.delivered()) {
            if (result.attempts() > 1) {
                LOG.info("critical report delivery recovered type={} stageId={} attempts={}",
                        msg.type(), msg.stageId(), result.attempts());
            }
        } else if (result.firstFailure() != null) {
            LOG.warn("critical report delivery stopped during worker shutdown type={} stageId={} attempts={}",
                    msg.type(), msg.stageId(), result.attempts());
        }
        return result.delivered();
    }

    /**
     * Withdraws the endpoint one-way over the heartbeat lane, without waiting for an acknowledgement and
     * without drawing on the budget a stage close is allowed for draining.
     */
    private void sendDeparture() {
        try {
            sendLeader(heartbeatCache, RpcMessage.deregisterWorker(
                    workerId, advertisedHost, rpcEndpoint.port(), slots,
                    dataServer != null ? dataServer.port() : -1, incarnationId));
        } catch (RuntimeException departureFailure) {
            // When the master cannot be reached the worker falls back to its heartbeat going stale; closing local
            // resources must not be blocked by leaving the control plane.
            LOG.debug("worker departure notification skipped worker={} cause={}",
                    workerId, departureFailure.toString());
        }
    }

    private static final class CloseCancellation {
        final WorkerAssignmentLedger.Cancellation cancellation;
        final boolean report;
        boolean terminal;

        CloseCancellation(WorkerAssignmentLedger.Cancellation cancellation, boolean report) {
            this.cancellation = cancellation;
            this.report = report;
        }
    }

    private boolean beginClose() {
        synchronized (closeLifecycle) {
            if (lifecycle != Lifecycle.OPEN) {
                return false;
            }
            lifecycle = Lifecycle.CLOSING;
            long now = System.nanoTime();
            long budget = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(closeDrainMs);
            closeDeadlineNanos = budget >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + budget;
            return true;
        }
    }

    private void awaitClosedByOwner() {
        boolean interrupted = false;
        synchronized (closeLifecycle) {
            while (lifecycle != Lifecycle.CLOSED) {
                try {
                    closeLifecycle.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (closeFailure != null) {
            rethrowCloseFailure(closeFailure);
        }
    }

    private static Throwable runCloseStep(Throwable primary, Runnable step) {
        try {
            step.run();
        } catch (Throwable failure) {
            if (primary == null) {
                return failure;
            }
            if (primary != failure) {
                primary.addSuppressed(failure);
            }
        }
        return primary;
    }

    private static void rethrowCloseFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    private Throwable closeOwner() {
        Throwable failure = null;
        try {
        // Withdraw the high-availability watch and fence its callbacks first, so that closing cannot
        // re-register, rebuild a connection or hand a task to the timer.
        failure = runCloseStep(failure, leaderSubscription::close);
        java.util.concurrent.ScheduledExecutorService timer = heartbeatTimer;
        if (timer != null) {
            failure = runCloseStep(failure, timer::shutdownNow);
        }
        java.util.concurrent.ScheduledExecutorService maintenance = maintenanceTimer;
        if (maintenance != null) {
            failure = runCloseStep(failure, maintenance::shutdownNow);
        }
        failure = runCloseStep(failure, () -> closeWorkerLease(closeDeadlineNanos));
        // While the data and RPC listeners are still alive, withdraw this worker's endpoint from the master
        // one-way; this does not consume the deadline a stage close has for draining.
        sendDeparture();

        java.util.List<CloseCancellation> cancellations = new java.util.ArrayList<>();
        for (WorkerAssignmentLedger.Slot slot : assignments.activeSlots()) {
            try {
                WorkerAssignmentLedger.Cancellation cancellation =
                        assignments.cancel(slot.stageId, slot.runAttemptFence);
                rollbackPreparedOutputForCancellation(cancellation);
                // Close owns the legacy claim. Durable execution keeps its own claim so it can
                // freeze the authoritative receipt before close optionally replays that value.
                boolean report = cancellation.slot.hadAssignment
                        && (cancellation.slot.durableAssignment
                        || assignments.claimReport(cancellation.slot));
                RuntimeException cause =
                        new IllegalStateException("stage " + slot.stageId + " cancelled by worker close");
                try {
                    cancelOwnedRegistries(cancellation.command, cause);
                } catch (Throwable registryFailure) {
                    failure = appendCloseFailure(failure, registryFailure);
                }
                // A cooperative stop has to reach the pipeline's child threads first; interrupting the owner is only a
                // backstop for a block that is not a channel.
                try {
                    if (cancellation.task != null) {
                        cancellation.task.stop(StageTask.StopReason.CANCELLED);
                    }
                } catch (Throwable stopFailure) {
                    failure = appendCloseFailure(failure, stopFailure);
                }
                try {
                    if (cancellation.future != null) {
                        cancellation.future.cancel(true);
                    }
                } catch (Throwable interruptFailure) {
                    failure = appendCloseFailure(failure, interruptFailure);
                }
                cancellations.add(new CloseCancellation(cancellation, report));
            } catch (Throwable cancelFailure) {
                failure = appendCloseFailure(failure, cancelFailure);
            }
        }

        int unfinished = 0;
        for (CloseCancellation closeCancellation : cancellations) {
            closeCancellation.terminal = assignments.awaitTerminalUntil(
                    closeCancellation.cancellation.slot, closeDeadlineNanos);
            if (!closeCancellation.terminal) {
                unfinished++;
            }
        }
        for (CloseCancellation closeCancellation : cancellations) {
            if (closeCancellation.report && closeCancellation.terminal) {
                WorkerAssignmentLedger.Slot slot = closeCancellation.cancellation.slot;
                if (slot.durableAssignment) {
                    io.rowweave.run.StageTerminalReceipt receipt =
                            assignments.terminalReceipt(slot);
                    if (receipt == null && closeCancellation.cancellation.reportCancelled) {
                        reportDurableTerminal(slot,
                                io.rowweave.run.StageTerminalOutcome.CANCELED,
                                0L, 0L, "CANCELED", "stage cancelled by worker close",
                                io.rowweave.plan.StageExecutionTelemetry.notExecuted());
                    } else if (receipt != null
                            && assignments.hasPendingTerminalReport(slot)) {
                        replayDurableTerminal(slot, receipt);
                    }
                } else {
                    deliverEphemeralTerminal(slot,
                            false, 0, 0, "stage cancelled by worker close",
                            io.rowweave.plan.StageExecutionTelemetry.notExecuted());
                }
            } else if (closeCancellation.report) {
                // close owns the claim but its bounded drain expired. A late task.run()
                // return must not retain an undeliverable frozen value in the closed ledger.
                if (!closeCancellation.cancellation.slot.durableAssignment) {
                    assignments.clearTelemetry(closeCancellation.cancellation.slot);
                }
            }
        }

        failure = runCloseStep(failure, stagePool::shutdownNow);
        long remaining = closeDeadlineNanos - System.nanoTime();
        if (remaining > 0L) {
            try {
                if (!stagePool.awaitTermination(remaining, java.util.concurrent.TimeUnit.NANOSECONDS)) {
                    unfinished = Math.max(unfinished, assignments.activeCount());
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                unfinished = Math.max(unfinished, assignments.activeCount());
            }
        }
        if (unfinished > 0 || assignments.activeCount() > 0) {
            LOG.warn("worker close drain timed out after {}ms worker={} activeStages={} rendezvous={}",
                    closeDrainMs, workerId, assignments.activeCount(), rendezvousEntryCount());
        }

        } catch (Throwable unexpected) {
            failure = appendCloseFailure(failure, unexpected);
        } finally {
            reportStopping = true;
            try {
                io.rowweave.mem.ResourceCloser.close(failure,
                        () -> stagePool.shutdownNow(),
                        preparedOutputs,
                        reportCache::shutdown,
                        registrationCache::shutdown,
                        heartbeatCache::shutdown,
                        localShuffle,
                        muxConnections,
                        dataServer,
                        dataAuthorizations,
                        dataInbound,
                        rpcEndpoint,
                        ownsDeploymentRuntime ? deploymentRuntime : null);
            } catch (Throwable cleanupFailure) {
                failure = appendCloseFailure(failure, cleanupFailure);
            }
        }
        return failure;
    }

    private static Throwable appendCloseFailure(Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
    }

    @Override
    public void close() {
        if (!beginClose()) {
            awaitClosedByOwner();
            return;
        }
        Throwable failure = null;
        try {
            failure = closeOwner();
        } catch (Throwable unexpected) {
            failure = appendCloseFailure(failure, unexpected);
        } finally {
            // A guarded replacement may appear only once every resource of the old worker has been through close.
            if (guarded) {
                LIVE.compareAndSet(this, null);
            }
            synchronized (closeLifecycle) {
                closeFailure = failure;
                lifecycle = Lifecycle.CLOSED;
                closeLifecycle.notifyAll();
            }
        }
        if (failure != null) {
            rethrowCloseFailure(failure);
        }
    }
}
