package io.rowweave.cluster;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.ArtifactLifecycleRepository;
import io.rowweave.artifact.ArtifactJanitor;
import io.rowweave.artifact.ArtifactGcOwner;
import io.rowweave.artifact.ArtifactRetentionPolicy;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;

import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.cluster.rpc.RpcRoleEndpoint;
import io.rowweave.cluster.rpc.RpcRoleEndpoints;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.OutputDestinationValidator;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.AttemptSnapshot;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.RunExecutionEvent;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunHistoryRetiringException;
import io.rowweave.run.RunLifecycleCatalog;
import io.rowweave.run.RunLifecycleCatalogProvider;
import io.rowweave.run.RunRetentionAuthority;
import io.rowweave.run.RunRetentionAuthorityProvider;
import io.rowweave.run.RunRetentionPolicy;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.TerminalRunJanitor;
import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunCatalogProvider;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.RunSubmissionPolicyValidator;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.RecoveryFencedStageAssignmentAuthority;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalOutcome;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.PreparedSinkReceiptCodec;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunDeadlineAuthority;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.WholeAttemptRetryAuthority;
import io.rowweave.run.WholeAttemptRetryDecision;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.upgrade.UpgradeAuthorityStore;
import io.rowweave.upgrade.UpgradeAuthorityStoreProvider;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.UpgradeCommand;
import io.rowweave.upgrade.UpgradeRuntimeCoordinator;
import io.rowweave.upgrade.UpgradeTransition;
import io.rowweave.upgrade.RecoverableRunUpgradeCensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The control plane master: an RPC server, a registry of workers, the detector that notices a worker
 * has gone quiet, and the least damage done when one does. This is the process-level implementation,
 * apart from the thread-level {@link Master} that runs the same roles inside one JVM.
 *
 * <p>Scope: registration, heartbeat, the detection of a lost worker and the response to it. When a
 * worker is lost, the stages it owned that had not finished are marked failed, which releases whoever
 * is blocked on the submission so that it cannot hang. Submission, assignment and reports travel the
 * same path as the partitioning, placement and port wiring.
 *
 * <p><b>Heartbeat handling is separate</b>, and that is a requirement rather than a nicety. The RPC
 * server handles heartbeat and registration on a thread per connection, apart from the threads that
 * execute stages, so a worker parked on backpressure can still answer for its own liveness and is not
 * mistaken for dead.
 */
public final class MasterServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MasterServer.class);
    private static final boolean TRACE_HEARTBEATS =
            Boolean.getBoolean("rowweave.cluster.heartbeatTrace");
    private static final int RUN_EVENT_QUERY_MAX_READS = 16;

    /**
     * The well-known identity of the elected leader's role record in the registry, reused by MasterElection.
     * Placement must exclude a role record from the list of workers.
     */
    public static final String MASTER_LEADER_ROLE_ID = "__rowweave_master_leader__";

    /** What is known about a registered worker. */
    public static final class WorkerInfo {
        final String workerId;
        final String host;
        final int port;
        final int slots;
        final int dataPort;
        final String incarnationId;
        final io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata;
        final io.rowweave.cluster.rpc.DataPlaneRegistration dataPlaneRegistration;
        final io.rowweave.cluster.rpc.ProtocolHelloV1 protocolHello;
        final BeatHeartbeat heartbeat;

        WorkerInfo(String workerId, String host, int port, int slots, int dataPort,
                   String incarnationId, long stalenessMillis,
                   io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata,
                   io.rowweave.cluster.rpc.DataPlaneRegistration dataPlaneRegistration,
                   io.rowweave.cluster.rpc.ProtocolHelloV1 protocolHello) {
            this.workerId = workerId;
            this.host = host;
            this.port = port;
            this.slots = slots;
            this.dataPort = dataPort;
            this.incarnationId = incarnationId;
            this.securityMetadata = securityMetadata;
            this.dataPlaneRegistration = dataPlaneRegistration;
            this.protocolHello = protocolHello;
            this.heartbeat = new BeatHeartbeat(stalenessMillis);
        }

        public String workerId() { return workerId; }
        public String host() { return host; }
        public int port() { return port; }
        public int slots() { return slots; }
        public int dataPort() { return dataPort; }
        public String incarnationId() { return incarnationId; }
        public io.rowweave.cluster.rpc.DataPlaneRegistration dataPlaneRegistration() {
            return dataPlaneRegistration;
        }
        public io.rowweave.cluster.rpc.ProtocolHelloV1 protocolHello() {
            return protocolHello;
        }
    }

    private final long heartbeatStalenessMillis;
    private final WorkerLivenessPauseGuard workerLivenessPauseGuard;

    /**
     * A process-wide scheduling pause ages both the Master sweep and Worker beats. The first
     * observation after such a pause gives beats one staleness window to resume; regular Master
     * observations still fail a silent Worker at the configured threshold.
     */
    private static final class WorkerLivenessPauseGuard {
        private final long stalenessNanos;
        private final long missedSweepNanos;
        private long lastObservationNanos = System.nanoTime();
        private long graceStartedNanos;
        private boolean graceActive;

        WorkerLivenessPauseGuard(long stalenessMillis, long expectedCheckMillis) {
            this.stalenessNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    stalenessMillis);
            long checkNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    expectedCheckMillis);
            this.missedSweepNanos = Math.max(stalenessNanos,
                    checkNanos > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : checkNanos * 2L);
        }

        synchronized boolean isAlive(BeatHeartbeat heartbeat) {
            long now = System.nanoTime();
            if (now - lastObservationNanos > missedSweepNanos) {
                graceStartedNanos = now;
                graceActive = true;
            }
            lastObservationNanos = now;
            if (heartbeat.isAlive()) {
                return true;
            }
            if (graceActive && now - graceStartedNanos < stalenessNanos) {
                return true;
            }
            graceActive = false;
            return false;
        }
    }
    // Whether two adjacent stages on the same worker pass a DataRow by reference when they meet on a local
    // channel, which is the default and what the operator contract of not mutating a row after emitting it
    // already guarantees. Turning it off takes a typed snapshot at the channel boundary instead, as a
    // defence. An edge over a socket is unaffected, since serialization isolates it already. The system
    // property defaults to true, and a test may set it per job.
    private volatile boolean objectReuse =
            Boolean.parseBoolean(System.getProperty("rowweave.cluster.objectReuse", "true"));
    // Workers are discovered through a registry carrying host, port and slots, so several masters share one
    // view of them. The local map holds only what a heartbeat proves alive, because a registry
    // implementation may keep no lease of its own and an in-process one is alive for as long as the process
    // is. Judging liveness by heartbeat stays a concern of the master that hears it. A registration writes
    // both, and placement reads its endpoints from the registry.
    private final ServiceRegistry registry;
    // The durable API uses only the authority the registry shares. When there is none it fails closed,
    // rather than inventing a local substitute.
    private final RunStateStore runStateStore;
    // Incremental control must enter through one non-splittable
    // frontier+Run+Sink gateway. Never reconstruct it from independent providers.
    private final io.rowweave.incremental.IncrementalAuthorityGateway
            incrementalAuthorityGateway;
    private final UpgradeRuntimeCoordinator upgradeRuntimeCoordinator;
    private final UpgradeAuthorityStore upgradeAuthorityStore;
    private final io.rowweave.internal.IncrementalSourceControlLedger
            incrementalSourceControlLedger =
            io.rowweave.internal.IncrementalSourceControlLedger.forRole();
    private final RunCancellationAuthority runCancellationAuthority;
    private final RunDeadlineAuthority runDeadlineAuthority;
    private final DurableStageAssignmentAuthority stageAssignmentAuthority;
    private final boolean durableExecutionEvidenceProviderPresent;
    private final io.rowweave.run.DurableExecutionEvidenceAuthority
            durableExecutionEvidenceAuthority;
    private final WholeAttemptRetryAuthority wholeAttemptRetryAuthority;
    private final RetryRecoveryMetrics retryRecoveryMetrics;
    private final RecoverableRunCatalog recoverableRunCatalog;
    private final RunRecoveryOwnership recoveryOwnership;
    private final LeaderRecoveryInventory recoveryInventory;
    private final TerminalRunJanitor terminalRunJanitor;
    private final long recoveryInventoryAwaitMillis;
    private final Object recoveryInventoryLifecycle = new Object();
    private volatile long recoveryInventoryGeneration;
    private volatile long recoveryInventoryTerm;
    private final java.util.Set<String> locallyExecutingDurableRuns =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** Ephemeral jobs admitted through the same-JVM Artifact binding may use object StageCommand delivery. */
    private final java.util.Set<String> inProcessLegacyJobs =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final ArtifactRepository artifactRepository;
    private final ArtifactLifecycleRepository artifactLifecycleRepository;
    private final ArtifactReferenceAuthority artifactReferenceAuthority;
    private final ArtifactJanitor artifactJanitor;
    private final io.rowweave.commit.ManifestCommitAuthority manifestCommitAuthority;
    private final DurableSinkAuthorityAdapterFactory testProviderAuthorityFactory;
    // Where run observations are collected, which by default costs nothing. The job lifecycle pushes events
    // to it, and an exception from an observer is kept away from the job.
    private final io.rowweave.monitor.JobMonitorRegistry monitor2;
    // The map from a global stage id to the job, stage index, role and operator chain the monitor knows, so
    // a stage report can push an update for that instance in real time. It is populated only when a real
    // observer was injected, since the default costs nothing and builds no table. Entries are removed by
    // job once it finishes, so a long-lived master does not accumulate them.
    private final Map<Long, StageRef> stageRefs = new ConcurrentHashMap<>();
    private static final class StageRef {
        final String monitorJobId; final int stageIndex; final String role; final String opChain;
        final java.util.List<Integer> inputs; final String workerId; final long startTime;
        final int instanceIndex; final int parallelism;
        final java.util.List<String> inputChannels;  // channel kinds lined up with the inputs, used to colour the monitor's edges
        final SourceMonitorSummary sourceSummary;
        StageRef(String j, int i, String r, String op, java.util.List<Integer> in, String w, long start, int inst, int par,
                 java.util.List<String> chans, SourceMonitorSummary source) {
            monitorJobId=j; stageIndex=i; role=r; opChain=op; inputs=in; workerId=w; startTime=start; instanceIndex=inst; parallelism=par;
            inputChannels=chans;
            sourceSummary=source;
        }
    }
    private static final class SourceMonitorSummary {
        static final SourceMonitorSummary NONE = new SourceMonitorSummary(0, null, null);
        final int splitCount;
        final String replayMode;
        final String descriptorDigestPrefix;
        SourceMonitorSummary(int count, String mode, String prefix) {
            splitCount = count;
            replayMode = mode;
            descriptorDigestPrefix = prefix;
        }
        void apply(io.rowweave.monitor.StageView.Builder builder) {
            if (splitCount > 0) {
                builder.sourceSummary(splitCount, replayMode, descriptorDigestPrefix);
            }
        }
    }
    // One instance table per job and stage, keyed by job and stage index, holding each instance's state,
    // row count and worker.
    private final Map<String, InstanceState[]> stageInstances = new ConcurrentHashMap<>();
    private static final class InstanceState { volatile String state="RUNNING"; volatile long rowCount=0; volatile String worker;
        volatile long startTime=0; volatile long endTime=0;  // each instance's own start and end times
        volatile long inputRows=0; }  // rows read in from upstream; rowCount is rows produced
    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    // A worker whose cancellation could not be confirmed takes no further admission or placement until it
    // reports everything idle or its endpoint is restarted.
    private final Map<String, QuarantinedWorker> quarantinedWorkers = new ConcurrentHashMap<>();
    private static final class QuarantinedWorker {
        final String host; final int port; final int slots; final int dataPort; final String incarnationId;
        final boolean departureFenced;
        QuarantinedWorker(String host, int port, int slots, int dataPort, String incarnationId,
                          boolean departureFenced) {
            this.host = host; this.port = port; this.slots = slots; this.dataPort = dataPort;
            this.incarnationId = incarnationId;
            this.departureFenced = departureFenced;
        }
        boolean sameEndpoint(RpcMessage register) {
            return port == register.port() && dataPort == register.dataPort()
                    && java.util.Objects.equals(host, register.host())
                    && java.util.Objects.equals(incarnationId, register.incarnationId());
        }
    }
    private final RpcRoleEndpoint rpcEndpoint;
    private final EphemeralSubmitTimeoutPolicy ephemeralSubmitTimeoutPolicy;
    private final ObjectTrustContext objectTrustContext;
    private final io.rowweave.cluster.rpc.ControlSecurityProfile controlSecurityProfile;
    private final ControlSecurityConfig controlSecurity;
    private final io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata;
    private final io.rowweave.cluster.rpc.ProtocolHelloV1 protocolHello;
    private final String deploymentSetId;
    private final io.rowweave.deployment.DeploymentRuntime deploymentRuntime;
    private final boolean ownsDeploymentRuntime;
    // The master's long-lived control connections to workers, in place of connecting, calling and closing
    // for every assignment or cancellation. The key is the host and port, so a worker that restarts on a
    // different port, or replaces another in high availability, resolves to a new entry, and a call that
    // throws drops its entry so the next attempt reconnects. The in-process transport bypasses this
    // entirely, which is why the local path pays nothing.
    private final io.rowweave.cluster.rpc.RpcTransportCache rpcCache;
    // Assignments go through a dispatcher of their own with a bound, and several lanes per endpoint; a
    // cancellation or a report still uses the shared connection cache.
    private final StageAssignmentDispatcher assignmentDispatcher;
    private final io.rowweave.cluster.rpc.RpcTransportLanePool assignmentLanes;
    private final StageCancellationCoordinator cancellationCoordinator;
    // Liveness is swept over the workers present now, rather than a snapshot of the ones known at startup.
    private final Object workerLifecycleLock = new Object();
    private final Object heartbeatMonitorLifecycle = new Object();
    private volatile java.util.concurrent.ScheduledExecutorService heartbeatMonitorTimer;
    private volatile Thread heartbeatMonitorThread;
    private long heartbeatMonitorGeneration;
    private int heartbeatSweepsInFlight;
    private boolean heartbeatMonitorClosed;
    // Cluster slot ledger plus the admission switch, on by default (rowweave.cluster.admission=true):
    // setting that system property to false turns it off, and then the ledger is skipped and no gang
    // admission is done for the whole job. When on, handleSubmit acquires the slots the job needs (the sum
    // of its stage instance counts), waits when they are short and fails on timeout, and releases them when
    // the job reaches a terminal state or a worker dies.
    private final SlotLedger slotLedger = new SlotLedger();
    private final boolean admissionEnabled =
            Boolean.parseBoolean(System.getProperty("rowweave.cluster.admission", "true"));
    private final long admitTimeoutMs = Long.getLong("rowweave.cluster.admitTimeoutMs", 60_000L);
    // Letting a job start at lower parallelism when slots are short is on by default: first wait out a grace
    // for the parallelism asked for, then run at the highest that fits now. Setting it to false restores the
    // older behaviour of waiting until the timeout or failing. The effective parallelism is chosen from the
    // slots free, with the desired value attempted first.
    private final boolean downgradeEnabled = ParallelismDowngradeConfig.enabled();
    // The grace given to the parallelism asked for, on the expectation that another job will release enough
    // for the whole of it. By default a quarter of the admission timeout bounded at five seconds, and never
    // more than that timeout.
    private final long downgradeGraceMs = ParallelismDowngradeConfig.graceMs(admitTimeoutMs);
    // The hook that limits the damage from a lost worker by marking its job failed, injected along the
    // submission path; a test observes it through a counter.
    private volatile java.util.function.Consumer<String> onWorkerLost = workerId -> { };
    // Null means a single fixed master, or a local path always active. Anything else means every part of
    // the stateful control plane must sit behind the election fence.
    private volatile MasterElection leadership;
    private volatile boolean closed;
    // Registering the owner of a submission and the fence that closes share one lock, so closing cannot miss
    // a submission that has just begun.
    private final Object submitLifecycle = new Object();
    private int activeSubmits;
    private final long closeDrainMs;
    // One deadline covers the whole control transaction of a job with several sinks; it is not reset per
    // sink, which would allow the wait to grow with the number of sinks.
    private final long outputCommitTimeoutMs = Math.max(
            1L, Long.getLong("rowweave.cluster.outputCommitTimeoutMs", 30_000L));
    // A probe for tests that must fail at a chosen point; the default does nothing and takes no part in the
    // protocol's state.
    private volatile Runnable beforeManifestPublishHook = () -> { };
    private volatile Runnable afterManifestPublishHook = () -> { };
    private volatile Runnable beforeDurableDecisionHookForTesting = () -> { };
    private volatile Runnable afterDurableDecisionHookForTesting = () -> { };
    private final java.util.concurrent.atomic.AtomicInteger
            durableTerminalAckLossForTesting =
            new java.util.concurrent.atomic.AtomicInteger();
    // Several assignments may hold the read lock at once; closing takes it as a fair write lock to linearise
    // the point at which dispatches already begun have finished and no new one may start.
    private final java.util.concurrent.locks.ReentrantReadWriteLock assignmentLifecycle =
            new java.util.concurrent.locks.ReentrantReadWriteLock(true);
    // The local linearisation point shared by a public cancellation and claiming a durable physical
    // assignment. It spans only the authority check and registering the owner, never a call to a worker, so
    // a slow or unreachable worker cannot stall a cancellation request.
    private final java.util.concurrent.locks.ReentrantReadWriteLock runDispatchLifecycle =
            new java.util.concurrent.locks.ReentrantReadWriteLock(true);

    private static final String MASTER_CLOSING = "master is closing or closed";
    private static final String EPHEMERAL_EXECUTION_TIMEOUT =
            "CLUSTER_JOB_EXECUTION_TIMEOUT";
    private static final String EPHEMERAL_TIMEOUT_CLEANUP_INCOMPLETE =
            "CLUSTER_JOB_TIMEOUT_CLEANUP_INCOMPLETE";

    private static final class DurableRunAcceptanceRejectedException
            extends IllegalStateException {
        private DurableRunAcceptanceRejectedException(String message) {
            super(message);
        }
    }

    public MasterServer(long heartbeatStalenessMillis) {
        this(heartbeatStalenessMillis, 0);  // zero means an ephemeral port, for a test in one process
    }

    /** Binds a port known in advance, so a worker or client can reach a master running in its own process; zero means an ephemeral one. */
    public MasterServer(long heartbeatStalenessMillis, int port) {
        this(heartbeatStalenessMillis, port, new InProcessRegistry());
    }

    /**
     * Constructs with a {@link ServiceRegistry} shared by the caller. Several masters, a leader and its
     * standbys, are given the same registry instance, so a worker registered with one is visible to all of
     * them and the new leader after a failover still sees them. The default constructor builds its own
     * in-process registry, which leaves a single master behaving as before.
     */
    public MasterServer(long heartbeatStalenessMillis, int port, ServiceRegistry registry) {
        this(heartbeatStalenessMillis, port, registry, io.rowweave.monitor.JobMonitorRegistry.NOOP);
    }

    /**
     * The deepest constructor, which takes where run observations are collected. A monitor in the same
     * process passes the shared instance so this master records its jobs and stages there; the default
     * costs nothing and changes no behaviour.
     *
     * <p><b>The master holds no parallelism for a job.</b> Parallelism is a property of the job and arrives
     * with the graph that is submitted, which is why it is read from the graph and why it is absent from
     * this signature and unrelated to a worker's slots.
     */
    public MasterServer(long heartbeatStalenessMillis, int port, ServiceRegistry registry,
                        io.rowweave.monitor.JobMonitorRegistry monitor) {
        this(heartbeatStalenessMillis, port, registry, monitor,
                registry instanceof ArtifactRepositoryProvider
                        ? ((ArtifactRepositoryProvider) registry).artifactRepository() : null);
    }

    /**
     * An explicitly injected repository takes priority; null means a durable submission is unavailable and
     * fails closed.
     */
    public MasterServer(long heartbeatStalenessMillis, int port, ServiceRegistry registry,
                        io.rowweave.monitor.JobMonitorRegistry monitor,
                        ArtifactRepository artifactRepository) {
        this(heartbeatStalenessMillis, port, registry, monitor, artifactRepository,
                RuntimeSource.owned(() ->
                        io.rowweave.deployment.DeploymentRuntime.fromSystemProperties(
                                ControlSecurityConfig.fromSystemProperties())),
                RoleConstructionProbe.NOOP, null, false);
    }

    /** ClusterMain injects one process deployment identity shared by co-located roles. */
    public MasterServer(long heartbeatStalenessMillis, int port, ServiceRegistry registry,
                        io.rowweave.monitor.JobMonitorRegistry monitor,
                        ArtifactRepository artifactRepository,
                        io.rowweave.deployment.DeploymentRuntime deploymentRuntime) {
        this(heartbeatStalenessMillis, port, registry, monitor, artifactRepository,
                RuntimeSource.borrowed(deploymentRuntime), RoleConstructionProbe.NOOP, null, false);
    }

    /** Package-local signed conformance seam; production constructors never supply it. */
    MasterServer(long heartbeatStalenessMillis, int port, ServiceRegistry registry,
                 io.rowweave.monitor.JobMonitorRegistry monitor,
                 ArtifactRepository artifactRepository,
                 io.rowweave.deployment.DeploymentRuntime deploymentRuntime,
                 DurableSinkAuthorityAdapterFactory providerAuthorityFactory) {
        this(heartbeatStalenessMillis, port, registry, monitor,
                artifactRepository, RuntimeSource.borrowed(deploymentRuntime),
                RoleConstructionProbe.NOOP, providerAuthorityFactory, false);
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

        static RuntimeSource borrowed(
                io.rowweave.deployment.DeploymentRuntime runtime) {
            return new RuntimeSource(java.util.Objects.requireNonNull(
                    runtime, "deploymentRuntime"), null);
        }

        static RuntimeSource owned(DeploymentRuntimeFactory factory) {
            return new RuntimeSource(null, java.util.Objects.requireNonNull(
                    factory, "deployment runtime factory"));
        }

        boolean owned() {
            return ownedFactory != null;
        }
    }

    MasterServer(long heartbeatStalenessMillis, int port, ServiceRegistry registry,
                 io.rowweave.monitor.JobMonitorRegistry monitor,
                 ArtifactRepository artifactRepository,
                 RuntimeSource runtimeSource,
                 RoleConstructionProbe probe) {
        this(heartbeatStalenessMillis, port, registry, monitor,
                artifactRepository, runtimeSource, probe, null, false);
    }

    static MasterServer newInProcess(
            long heartbeatStalenessMillis, ServiceRegistry registry,
            io.rowweave.monitor.JobMonitorRegistry monitor) {
        return new MasterServer(heartbeatStalenessMillis, 0, registry, monitor,
                registry instanceof ArtifactRepositoryProvider
                        ? ((ArtifactRepositoryProvider) registry).artifactRepository() : null,
                RuntimeSource.owned(() ->
                        io.rowweave.deployment.DeploymentRuntime.fromSystemProperties(
                                ControlSecurityConfig.fromSystemProperties())),
                RoleConstructionProbe.NOOP, null, true);
    }

    private MasterServer(long heartbeatStalenessMillis, int port,
                 ServiceRegistry registry,
                 io.rowweave.monitor.JobMonitorRegistry monitor,
                 ArtifactRepository artifactRepository,
                 RuntimeSource runtimeSource,
                 RoleConstructionProbe probe,
                 DurableSinkAuthorityAdapterFactory providerAuthorityFactory,
                 boolean inProcessEndpoint) {
        java.util.Objects.requireNonNull(registry, "registry");
        java.util.Objects.requireNonNull(monitor, "monitor");
        java.util.Objects.requireNonNull(runtimeSource, "runtimeSource");
        java.util.Objects.requireNonNull(probe, "probe");
        long configuredCloseDrainMs = Long.getLong("rowweave.master.closeDrainMs", 5_000L);
        if (configuredCloseDrainMs < 0) {
            throw new IllegalArgumentException("rowweave.master.closeDrainMs must be >= 0");
        }
        long heartbeatCheckMs = Long.getLong("rowweave.master.heartbeatCheckMs",
                Math.max(100L, Math.min(1_000L, heartbeatStalenessMillis / 3L)));
        if (heartbeatCheckMs <= 0) {
            throw new IllegalArgumentException("rowweave.master.heartbeatCheckMs must be > 0");
        }
        int assignParallelism = Integer.getInteger("rowweave.cluster.assign.parallelism", 4);
        if (assignParallelism < 1) {
            throw new IllegalArgumentException("rowweave.cluster.assign.parallelism must be >= 1");
        }
        RoleConstructionTransaction transaction = new RoleConstructionTransaction();
        try {
            io.rowweave.deployment.DeploymentRuntime deploymentRuntime = runtimeSource.owned()
                    ? transaction.acquire(() -> runtimeSource.ownedFactory.create(),
                    io.rowweave.deployment.DeploymentRuntime::close)
                    : runtimeSource.borrowed;
            this.deploymentRuntime = deploymentRuntime;
            this.ownsDeploymentRuntime = runtimeSource.owned();
            if (ownsDeploymentRuntime) probe.after(MasterConstructionStep.OWNED_RUNTIME);
            deploymentRuntime.requireAdmissionOpen();
            ControlSecurityConfig controlSecurity = deploymentRuntime.controlSecurity();
            controlSecurity.requireImplementedTransport();
            controlSecurity.requireLocalRole(
                    io.rowweave.cluster.rpc.ControlRole.MASTER);
            this.ephemeralSubmitTimeoutPolicy =
                    EphemeralSubmitTimeoutPolicy.masterFromSystemProperties();
            this.controlSecurity = controlSecurity;
            this.controlSecurityProfile = controlSecurity.profile();
            this.securityMetadata = controlSecurity.securityMetadata();
            this.protocolHello = deploymentRuntime.protocolHello();
            this.deploymentSetId = deploymentRuntime.deploymentSetId();
            this.objectTrustContext = deploymentRuntime.objectTrustContext();
            this.rpcCache = transaction.acquire(
                    () -> new io.rowweave.cluster.rpc.RpcTransportCache(
                            (host, workerPort) -> io.rowweave.cluster.rpc.RpcTransports.connect(
                                    host, workerPort, 5_000, 30_000,
                                    controlSecurity, protocolHello),
                            (host, workerPort) -> !io.rowweave.cluster.rpc.InProcessRpcRegistry
                                    .isRegistered(host, workerPort)),
                    io.rowweave.cluster.rpc.RpcTransportCache::shutdown);
            probe.after(MasterConstructionStep.RPC_CACHE);
            io.rowweave.cluster.rpc.RpcAdmissionConfig rpcAdmission =
                    io.rowweave.cluster.rpc.RpcAdmissionConfig.fromSystemProperties();
            this.heartbeatStalenessMillis = heartbeatStalenessMillis;
            this.workerLivenessPauseGuard = new WorkerLivenessPauseGuard(
                    heartbeatStalenessMillis, heartbeatCheckMs);
            this.closeDrainMs = configuredCloseDrainMs;
            this.registry = registry;
            this.runStateStore = registry instanceof RunStateStoreProvider
                    ? ((RunStateStoreProvider) registry).runStateStore() : null;
            this.incrementalAuthorityGateway = registry instanceof
                    io.rowweave.incremental.IncrementalAuthorityGatewayProvider
                    ? ((io.rowweave.incremental.IncrementalAuthorityGatewayProvider) registry)
                    .incrementalAuthorityGateway() : null;
            UpgradeAuthorityStore upgradeAuthority = registry instanceof
                    UpgradeAuthorityStoreProvider
                    ? ((UpgradeAuthorityStoreProvider) registry)
                    .upgradeAuthorityStore() : null;
            this.upgradeAuthorityStore = upgradeAuthority;
            RecoverableRunCatalog upgradeRunCatalog = registry instanceof
                    RecoverableRunCatalogProvider
                    ? ((RecoverableRunCatalogProvider) registry)
                    .recoverableRunCatalog() : null;
            this.upgradeRuntimeCoordinator = upgradeAuthority == null
                    ? null : new UpgradeRuntimeCoordinator(upgradeAuthority,
                    upgradeRunCatalog == null
                            ? io.rowweave.upgrade.UpgradeActiveWorkCensus.EMPTY
                            : new RecoverableRunUpgradeCensus(upgradeRunCatalog));
            this.runCancellationAuthority = runStateStore instanceof RunCancellationAuthority
                    ? (RunCancellationAuthority) runStateStore : null;
            this.runDeadlineAuthority = runStateStore instanceof RunDeadlineAuthority
                    ? (RunDeadlineAuthority) runStateStore : null;
            this.stageAssignmentAuthority = registry instanceof DurableStageAssignmentAuthorityProvider
                    ? ((DurableStageAssignmentAuthorityProvider) registry)
                    .durableStageAssignmentAuthority()
                    : (runStateStore instanceof DurableStageAssignmentAuthority
                    ? (DurableStageAssignmentAuthority) runStateStore : null);
            this.durableExecutionEvidenceProviderPresent =
                    registry instanceof io.rowweave.run.DurableExecutionEvidenceAuthorityProvider;
            this.durableExecutionEvidenceAuthority = durableExecutionEvidenceProviderPresent
                            ? ((io.rowweave.run.DurableExecutionEvidenceAuthorityProvider) registry)
                            .durableExecutionEvidenceAuthority()
                            : null;
            this.wholeAttemptRetryAuthority = runStateStore
                    instanceof WholeAttemptRetryAuthority
                    && stageAssignmentAuthority == runStateStore
                    ? (WholeAttemptRetryAuthority) runStateStore : null;
            this.recoverableRunCatalog = registry instanceof RecoverableRunCatalogProvider
                    ? ((RecoverableRunCatalogProvider) registry).recoverableRunCatalog() : null;
            this.recoveryOwnership = registry instanceof RunRecoveryOwnershipProvider
                    ? ((RunRecoveryOwnershipProvider) registry).runRecoveryOwnership() : null;
            RunLifecycleCatalog lifecycleCatalog = registry instanceof RunLifecycleCatalogProvider
                    ? ((RunLifecycleCatalogProvider) registry).runLifecycleCatalog() : null;
            RunRetentionAuthority retentionAuthority =
                    registry instanceof RunRetentionAuthorityProvider
                            ? ((RunRetentionAuthorityProvider) registry)
                            .runRetentionAuthority() : null;
            if (lifecycleCatalog != null
                    && retentionAuthority != null
                    && lifecycleCatalog == runStateStore
                    && retentionAuthority == runStateStore) {
                this.terminalRunJanitor = transaction.acquire(
                        () -> new TerminalRunJanitor(
                                lifecycleCatalog, retentionAuthority,
                                RunRetentionPolicy.fromProperties(System.getProperties()),
                                System::currentTimeMillis,
                                snapshot -> publishTerminalRunRetention(monitor, snapshot)),
                        TerminalRunJanitor::close);
                probe.after(MasterConstructionStep.TERMINAL_JANITOR);
            } else {
                this.terminalRunJanitor = null;
            }
            this.artifactRepository = artifactRepository;
            ArtifactReferenceAuthority configuredReferenceAuthority =
                    registry instanceof ArtifactReferenceAuthorityProvider
                            ? ((ArtifactReferenceAuthorityProvider) registry)
                            .artifactReferenceAuthority() : null;
            boolean lifecycleCapabilityMatches =
                    artifactRepository instanceof ArtifactLifecycleRepository
                            && configuredReferenceAuthority != null
                            && configuredReferenceAuthority == runStateStore;
            this.artifactLifecycleRepository = lifecycleCapabilityMatches
                    ? (ArtifactLifecycleRepository) artifactRepository : null;
            this.artifactReferenceAuthority = lifecycleCapabilityMatches
                    ? configuredReferenceAuthority : null;
            if (lifecycleCapabilityMatches) {
                this.artifactJanitor = transaction.acquire(
                        () -> new ArtifactJanitor(
                                this.artifactLifecycleRepository,
                                this.artifactReferenceAuthority,
                                ArtifactRetentionPolicy.fromProperties(System.getProperties()),
                                System::currentTimeMillis,
                                snapshot -> publishArtifactRetention(monitor, snapshot)),
                        ArtifactJanitor::close);
                probe.after(MasterConstructionStep.ARTIFACT_JANITOR);
            } else {
                this.artifactJanitor = null;
            }
            this.recoveryInventoryAwaitMillis = Long.getLong(
                    "rowweave.master.recoveryInventory.awaitMs", 30_000L);
            if (recoveryInventoryAwaitMillis < 0L) {
                throw new IllegalArgumentException(
                        "rowweave.master.recoveryInventory.awaitMs must be >= 0");
            }
            if (runStateStore instanceof RecoveryFencedRunStateStore
                    && recoverableRunCatalog != null
                    && recoveryOwnership != null && artifactRepository != null) {
                int pageSize = Integer.getInteger(
                        "rowweave.master.recoveryInventory.pageSize", 128);
                long initialBackoff = Long.getLong(
                        "rowweave.master.recoveryInventory.initialBackoffMs", 50L);
                long maxBackoff = Long.getLong(
                        "rowweave.master.recoveryInventory.maxBackoffMs", 2_000L);
                int sampleLimit = Integer.getInteger(
                        "rowweave.master.recoveryInventory.sampleLimit", 16);
                this.retryRecoveryMetrics = new RetryRecoveryMetrics(monitor, pageSize);
                this.recoveryInventory = transaction.acquire(
                        () -> new LeaderRecoveryInventory(
                                runStateStore, recoverableRunCatalog, recoveryOwnership,
                                artifactRepository, stageAssignmentAuthority,
                                new LeaderRecoveryInventory.Config(
                                        pageSize, initialBackoff, maxBackoff, sampleLimit),
                                this::recoverInheritedRun, deploymentSetId,
                                controlSecurityProfile
                                        == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS,
                                locallyExecutingDurableRuns::contains,
                                retryRecoveryMetrics),
                        LeaderRecoveryInventory::close);
                probe.after(MasterConstructionStep.RECOVERY_INVENTORY);
            } else {
                this.recoveryInventory = null;
                this.retryRecoveryMetrics = new RetryRecoveryMetrics(monitor, 1);
            }
            this.manifestCommitAuthority = registry instanceof io.rowweave.commit.ManifestCommitAuthorityProvider
                    ? ((io.rowweave.commit.ManifestCommitAuthorityProvider) registry).manifestCommitAuthority()
                    : new io.rowweave.commit.FileManifestCommitAuthority();
            this.testProviderAuthorityFactory = providerAuthorityFactory;
            this.monitor2 = monitor;
            this.assignmentDispatcher = transaction.acquire(
                    () -> new StageAssignmentDispatcher(assignParallelism),
                    StageAssignmentDispatcher::close);
            probe.after(MasterConstructionStep.ASSIGN_DISPATCHER);
            this.cancellationCoordinator = transaction.acquire(
                    () -> new StageCancellationCoordinator(assignParallelism),
                    StageCancellationCoordinator::close);
            probe.after(MasterConstructionStep.CANCEL_COORDINATOR);
            this.assignmentLanes = transaction.acquire(
                    () -> new io.rowweave.cluster.rpc.RpcTransportLanePool(
                            assignParallelism,
                            (host, workerPort) -> io.rowweave.cluster.rpc.RpcTransports.connect(
                                    host, workerPort, 5_000, 30_000,
                                    controlSecurity, protocolHello),
                            (host, workerPort) -> !io.rowweave.cluster.rpc.InProcessRpcRegistry
                                    .isRegistered(host, workerPort)),
                    io.rowweave.cluster.rpc.RpcTransportLanePool::close);
            probe.after(MasterConstructionStep.ASSIGN_LANES);
            this.rpcEndpoint = transaction.acquire(
                    () -> inProcessEndpoint
                            ? RpcRoleEndpoints.inProcess(
                                    "127.0.0.1", handler(),
                                    rpcAdmission.maxConnections())
                            : RpcRoleEndpoints.socket(RpcServer.bindDormant(
                                    (request, context) -> handle(request, context),
                                    port, controlSecurity, rpcAdmission,
                                    io.rowweave.cluster.rpc.RpcRequestPolicy.MASTER,
                                    protocolHello)),
                    RpcRoleEndpoint::close);
            probe.after(MasterConstructionStep.RPC_BOUND);
        } catch (Throwable constructionFailure) {
            transaction.rollback(constructionFailure);
            rethrowConstructionFailure(constructionFailure);
            throw new AssertionError("unreachable");
        }

        transaction.commit();
        try {
            rpcEndpoint.activate();
            probe.after(MasterConstructionStep.RPC_ACTIVE);
            startHeartbeatMonitor(heartbeatCheckMs);
            probe.after(MasterConstructionStep.HEARTBEAT);
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

    /** Where run observations are collected, read by the monitor HTTP service in the same process in order to render them. */
    public io.rowweave.monitor.JobMonitorRegistry monitor() {
        return monitor2;
    }

    /**
     * Per-job row reuse: does a local channel between two stages on one worker pass the row reference directly, or copy at the boundary.
     * The default is true.
     */
    public MasterServer setObjectReuse(boolean reuse) {
        this.objectReuse = reuse;
        return this;
    }

    /**
     * Failure drill for tests: ends leadership once, either before a manifest is published or after a durable publish.
     */
    public void setManifestPublishHooks(Runnable beforePublish, Runnable afterPublish) {
        beforeManifestPublishHook = beforePublish == null ? () -> { } : beforePublish;
        afterManifestPublishHook = afterPublish == null ? () -> { } : afterPublish;
    }

    void loseNextDurableTerminalAcksForTesting(int count) {
        if (count < 0) throw new IllegalArgumentException("count must be >= 0");
        durableTerminalAckLossForTesting.set(count);
    }

    void setDurableDecisionHooksForTesting(
            Runnable beforeDecision, Runnable afterDecision) {
        beforeDurableDecisionHookForTesting = beforeDecision == null
                ? () -> { } : beforeDecision;
        afterDurableDecisionHookForTesting = afterDecision == null
                ? () -> { } : afterDecision;
    }

    /** The row-reuse setting currently in effect. */
    public boolean objectReuse() {
        return objectReuse;
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

    /**
     * Bound once, before the election starts. A master that is never bound keeps the single fixed leader behaviour it had before.
     */
    public synchronized void bindLeadership(MasterElection election) {
        if (closed) {
            throw new IllegalStateException("master is closed");
        }
        if (leadership != null) {
            throw new IllegalStateException("master leadership already bound");
        }
        if ((terminalRunJanitor != null && terminalRunJanitor.active())
                || (artifactJanitor != null && artifactJanitor.active())) {
            throw new IllegalStateException(
                    "fixed leadership services already started");
        }
        leadership = java.util.Objects.requireNonNull(election, "election");
        election.addLeadershipListener(this::onLeadershipChanged);
    }

    /**
     * The explicit lifecycle entry for a single fixed master or a local cluster. The high availability path must not call it and waits to
     * acquire the election instead.
     */
    public synchronized void startFixedLeadershipServices() {
        if (closed) throw new IllegalStateException("master is closed");
        if (leadership != null) {
            throw new IllegalStateException("election leadership is bound");
        }
        if (terminalRunJanitor == null) {
            throw new IllegalStateException(
                    "terminal run retention capability unavailable");
        }
        if (!terminalRunJanitor.active()) {
            terminalRunJanitor.begin(
                    new RetentionOwner("fixed-master@127.0.0.1:" + port(), 1L),
                    () -> !closed && leadership == null);
        }
        if (artifactJanitor != null && !artifactJanitor.active()) {
            artifactJanitor.begin(
                    new ArtifactGcOwner(
                            "fixed-master@127.0.0.1:" + port(), 1L),
                    () -> !closed && leadership == null);
        }
    }

    boolean terminalRunJanitorActive() {
        return terminalRunJanitor != null && terminalRunJanitor.active();
    }

    boolean artifactJanitorActive() {
        return artifactJanitor != null && artifactJanitor.active();
    }

    ArtifactJanitor.PassResult runArtifactRetentionPassForTest(
            ArtifactGcOwner owner) {
        if (artifactJanitor == null) {
            throw new IllegalStateException(
                    "Artifact retention capability unavailable");
        }
        return artifactJanitor.runOnePass(owner, this::isActiveLeader);
    }

    TerminalRunJanitor.PassResult runTerminalRetentionPassForTest(
            RetentionOwner owner) {
        if (terminalRunJanitor == null) {
            throw new IllegalStateException(
                    "terminal run retention capability unavailable");
        }
        return terminalRunJanitor.runOnePass(owner, this::isActiveLeader);
    }

    private static void publishTerminalRunRetention(
            io.rowweave.monitor.JobMonitorRegistry monitor,
            TerminalRunJanitor.Snapshot snapshot) {
        io.rowweave.monitor.RunRetentionView view =
                new io.rowweave.monitor.RunRetentionView(
                        snapshot.backlog(), snapshot.lifecycleCapacityPerShard(),
                        snapshot.scanPageCapacity(),
                        snapshot.oldestTerminalAgeMillis(),
                        snapshot.claimedRuns(), snapshot.purgedRuns(),
                        snapshot.errorRuns(), snapshot.cycleComplete(),
                        snapshot.diagnosticSamples());
        try {
            monitor.onRunRetention(view);
        } catch (RuntimeException advisoryFailure) {
            LOG.warn("terminal retention monitor publication failed", advisoryFailure);
        }
        if (snapshot.diagnosticSamples().isEmpty()) {
            LOG.debug("terminal retention: backlog={} shardCapacity={} oldestAgeMs={} "
                            + "claimed={} purged={} deletedRecords={} errors={}",
                    snapshot.backlog(), snapshot.lifecycleCapacityPerShard(),
                    snapshot.oldestTerminalAgeMillis(), snapshot.claimedRuns(),
                    snapshot.purgedRuns(), snapshot.deletedRecords(),
                    snapshot.errorRuns());
        } else {
            LOG.warn("terminal retention errors: total={} samples={}",
                    snapshot.errorRuns(), snapshot.diagnosticSamples());
        }
    }

    private static void publishArtifactRetention(
            io.rowweave.monitor.JobMonitorRegistry monitor,
            ArtifactJanitor.Snapshot snapshot) {
        io.rowweave.monitor.ArtifactRetentionView view =
                new io.rowweave.monitor.ArtifactRetentionView(
                        snapshot.backlog(), snapshot.scanPageCapacity(),
                        snapshot.inventoryScope().name(),
                        snapshot.inventoryObservedShard(),
                        snapshot.inventoryEntries(), snapshot.inventoryBytes(),
                        snapshot.inventoryEntryCapacity(),
                        snapshot.inventoryByteCapacity(),
                        snapshot.oldestCandidateAgeMillis(),
                        snapshot.deletedArtifacts(),
                        snapshot.retainedReferenced(), snapshot.staleOwners(),
                        snapshot.errorArtifacts(), snapshot.notReadyPasses(),
                        snapshot.cycleComplete(),
                        snapshot.referenceStatus().name(),
                        snapshot.inventoryStatus().name(),
                        snapshot.diagnosticSamples());
        try {
            monitor.onArtifactRetention(view);
        } catch (RuntimeException advisoryFailure) {
            LOG.warn("Artifact retention monitor publication failed",
                    advisoryFailure);
        }
    }

    private boolean isActiveLeader() {
        if (closed) {
            return false;
        }
        MasterElection election = leadership;
        return election == null || election.isLeader();
    }

    private void requireActiveLeader() {
        if (!isActiveLeader()) {
            throw new IllegalStateException("not active master leader");
        }
    }

    /**
     * Losing leadership releases only the waits and the admission truth this master holds locally. Cancelling the job's threads and
     * watching the workers are each completed by their own owner.
     */
    private void onLeadershipChanged(boolean active) {
        if (active) {
            startRecoveryInventoryForCurrentTerm();
            startTerminalRunJanitorForCurrentTerm();
            startArtifactJanitorForCurrentTerm();
            return;
        }
        if (terminalRunJanitor != null) terminalRunJanitor.loseLeadership();
        if (artifactJanitor != null) artifactJanitor.loseLeadership();
        synchronized (recoveryInventoryLifecycle) {
            recoveryInventoryGeneration = 0L;
            recoveryInventoryTerm = 0L;
            if (recoveryInventory != null) recoveryInventory.loseLeadership();
        }
        IllegalStateException cause = new IllegalStateException("master leadership lost");
        failActiveStageTracking(cause);
        synchronized (workerLifecycleLock) {
            for (String workerId : workers.keySet()) {
                slotLedger.releaseWorker(workerId);
            }
            workers.clear();
            quarantinedWorkers.clear();
        }
    }

    private void startRecoveryInventoryForCurrentTerm() {
        MasterElection election = leadership;
        if (election == null || closed || !election.isLeader()) return;
        long term = election.termToken();
        if (term == 0L) return;
        synchronized (recoveryInventoryLifecycle) {
            if (closed || !election.isLeader() || election.termToken() != term) return;
            recoveryInventoryTerm = term;
            recoveryInventoryGeneration = recoveryInventory == null ? 0L
                    : recoveryInventory.begin(
                    new RecoveryOwner(election.self().toString(), term),
                    () -> !closed && leadership == election
                            && election.isLeader() && election.termToken() == term);
        }
    }

    private void startTerminalRunJanitorForCurrentTerm() {
        MasterElection election = leadership;
        if (election == null || closed || !election.isLeader()
                || terminalRunJanitor == null) return;
        long term = election.termToken();
        if (term == 0L) return;
        terminalRunJanitor.begin(
                new RetentionOwner(election.self().toString(), term),
                () -> !closed && leadership == election
                        && election.isLeader() && election.termToken() == term);
    }

    private void startArtifactJanitorForCurrentTerm() {
        MasterElection election = leadership;
        if (election == null || closed || !election.isLeader()
                || artifactJanitor == null) return;
        long term = election.termToken();
        if (term == 0L) return;
        artifactJanitor.begin(
                new ArtifactGcOwner(election.self().toString(), term),
                () -> !closed && leadership == election
                        && election.isLeader() && election.termToken() == term);
    }

    private void requireRecoveryInventoryReady() {
        MasterElection election = leadership;
        if (election == null) return;
        long term = election.termToken();
        long generation = recoveryInventoryGeneration;
        if (!election.isLeader() || term == 0L) {
            throw new IllegalStateException("not active master leader");
        }
        if (recoveryInventory == null || recoveryInventoryTerm != term || generation == 0L) {
            throw new IllegalStateException("leader recovery inventory capability unavailable");
        }
        try {
            recoveryInventory.awaitSettled(generation,
                    recoveryInventoryAwaitMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting leader recovery inventory",
                    interrupted);
        }
        if (!election.isLeader() || election.termToken() != term
                || !recoveryInventory.ready(generation)) {
            LeaderRecoveryInventory.Snapshot state = recoveryInventory.snapshot();
            throw new IllegalStateException("leader recovery inventory not ready: state="
                    + state.state() + " activeRuns=" + state.activeRuns());
        }
    }

    long recoveryInventoryGeneration() {
        return recoveryInventoryGeneration;
    }

    LeaderRecoveryInventory.Snapshot recoveryInventorySnapshot() {
        return recoveryInventory == null
                ? new LeaderRecoveryInventory.Snapshot(
                0L, LeaderRecoveryInventory.State.BLOCKED_CAPABILITY,
                0, java.util.Collections.emptyList(),
                "recovery inventory capability unavailable")
                : recoveryInventory.snapshot();
    }

    boolean awaitRecoveryInventory(long generation, long timeout,
                                   java.util.concurrent.TimeUnit unit)
            throws InterruptedException {
        return recoveryInventory != null
                && recoveryInventory.awaitSettled(generation, timeout, unit);
    }

    /**
     * Injection of what limits the damage when a worker is lost: its unfinished stages are marked failed, which releases whoever is blocked
     * on the submission. A test observes the loss through this.
     */
    public void setOnWorkerLost(java.util.function.Consumer<String> cb) {
        this.onWorkerLost = cb;
    }

    /**
     * Starts or reconfigures the liveness sweep over the workers present now, so every worker registered from here on is checked as well.
     */
    public synchronized void startHeartbeatMonitor(long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be > 0");
        }
        stopHeartbeatMonitor(false);
        final long generation;
        synchronized (heartbeatMonitorLifecycle) {
            if (heartbeatMonitorClosed) {
                throw new IllegalStateException("master heartbeat monitor is closed");
            }
            generation = ++heartbeatMonitorGeneration;
            java.util.concurrent.ScheduledExecutorService candidate =
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rowweave-master-heartbeat");
                t.setDaemon(true);
                heartbeatMonitorThread = t;
                return t;
            });
            HeartbeatScheduler.activate(
                    candidate, () -> runHeartbeatSweep(generation), intervalMs);
            heartbeatMonitorTimer = candidate;
        }
    }

    private void runHeartbeatSweep(long generation) {
        synchronized (heartbeatMonitorLifecycle) {
            if (heartbeatMonitorClosed || generation != heartbeatMonitorGeneration) {
                return;
            }
            heartbeatSweepsInFlight++;
        }
        try {
            for (WorkerInfo worker : workers.values()) {
                if (!workerLivenessPauseGuard.isAlive(worker.heartbeat)) {
                    try {
                        markWorkerLost(worker);
                    } catch (Throwable lossFailure) {
                        LOG.warn("worker liveness cleanup failed worker={}", worker.workerId, lossFailure);
                    }
                }
            }
        } finally {
            synchronized (heartbeatMonitorLifecycle) {
                heartbeatSweepsInFlight--;
                heartbeatMonitorLifecycle.notifyAll();
            }
        }
    }

    private void stopHeartbeatMonitor(boolean closing) {
        java.util.concurrent.ScheduledExecutorService timer;
        synchronized (heartbeatMonitorLifecycle) {
            if (closing) {
                heartbeatMonitorClosed = true;
            }
            heartbeatMonitorGeneration++;
            timer = heartbeatMonitorTimer;
            heartbeatMonitorTimer = null;
        }
        if (timer != null) {
            timer.shutdownNow();
        }
        if (Thread.currentThread() != heartbeatMonitorThread) {
            synchronized (heartbeatMonitorLifecycle) {
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
                while (heartbeatSweepsInFlight > 0 && System.nanoTime() < deadline) {
                    try {
                        long remainMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                Math.max(1L, deadline - System.nanoTime()));
                        heartbeatMonitorLifecycle.wait(remainMs);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                if (heartbeatSweepsInFlight > 0) {
                    LOG.warn("master heartbeat sweep did not stop within 2s");
                }
            }
        }
    }

    /**
     * Handles one generation of workers being lost, once. Re-checking the identity is what keeps a heartbeat that arrived late from
     * condemning a registration that came after it.
     */
    private void markWorkerLost(WorkerInfo worker) {
        boolean lost = false;
        synchronized (workerLifecycleLock) {
            if (workers.get(worker.workerId) != worker
                    || workerLivenessPauseGuard.isAlive(worker.heartbeat)) {
                return;
            }
            workers.remove(worker.workerId, worker);
            RegistryWorker endpoint = new RegistryWorker(
                    worker.workerId, worker.host, worker.port, worker.slots, worker.dataPort,
                    worker.incarnationId, worker.securityMetadata,
                    worker.dataPlaneRegistration);
            quarantineWorkerLocked(worker.workerId, endpoint);
            failStagesOnWorker(worker.workerId, endpoint,
                    new IllegalStateException("worker lost (heartbeat stale): " + worker.workerId));
            lost = true;
        }
        if (lost) {
            try {
                onWorkerLost.accept(worker.workerId);
            } catch (Throwable callbackFailure) {
                LOG.warn("worker lost callback failed worker={}", worker.workerId, callbackFailure);
            }
        }
    }

    private void failStagesOnWorker(String workerId, RegistryWorker endpoint, RuntimeException cause) {
        for (Map.Entry<Long, StageOwner> assignment : stageWorker.entrySet()) {
            StageOwner owner = assignment.getValue();
            if (!workerId.equals(owner.workerId) || !sameEndpoint(owner.endpoint, endpoint)) {
                continue;
            }
            StageTracker tracker = stageTrackers.get(assignment.getKey());
            if (tracker != null) {
                tracker.fail(cause.getMessage());
            }
            StagePortTracker portTracker = portTrackers.get(assignment.getKey());
            if (portTracker != null) {
                portTracker.fail(cause);
            }
        }
    }

    public Map<String, WorkerInfo> workers() {
        return workers;
    }

    public WorkerInfo worker(String id) {
        return workers.get(id);
    }

    /** Exposes the RPC handler so that an in-process transport can register it; the socket path is unchanged. */
    public java.util.function.Function<RpcMessage, RpcMessage> handler() {
        return request -> {
            RpcMessage response = handle(secureInbound(request), null);
            return response == null ? null : secureOutbound(response);
        };
    }

    /** Applies an operator-issued store transition through the active leader. */
    public UpgradeTransition applyStoreUpgrade(
            UpgradeCommand command, long authorityTimeMillis) {
        if (closed || !isActiveLeader()) {
            throw new IllegalStateException(
                    "store upgrade unavailable: not active master leader");
        }
        if (upgradeRuntimeCoordinator == null) {
            throw new IllegalStateException("store upgrade authority unavailable");
        }
        requireWriterRevisionTransition(command, authorityTimeMillis);
        return upgradeRuntimeCoordinator.apply(command, authorityTimeMillis);
    }

    private void requireWriterRevisionTransition(
            UpgradeCommand command, long authorityTimeMillis) {
        if (command == null || (command.kind() != UpgradeCommand.Kind.CUTOVER
                && command.kind() != UpgradeCommand.Kind.RESUME_WRITES)) {
            return;
        }
        io.rowweave.upgrade.UpgradeAuthorityRecord current =
                upgradeAuthorityStore.read();
        UpgradeTransition preview = io.rowweave.upgrade.UpgradeAuthorityKernel.apply(
                current, command, 0L, 0L, authorityTimeMillis);
        if (preview.code() != UpgradeTransition.Code.ACCEPTED) {
            return;
        }
        StoreFormatHeader writerHeader = command.kind() == UpgradeCommand.Kind.CUTOVER
                ? current.targetHeader() : current.activeHeader();
        io.rowweave.upgrade.WriterRevisionGate.evaluate(
                writerHeader, writableProtocolHellos()).requireCurrent();
    }

    private java.util.List<io.rowweave.cluster.rpc.ProtocolHelloV1>
            writableProtocolHellos() {
        java.util.List<io.rowweave.cluster.rpc.ProtocolHelloV1> hellos =
                new java.util.ArrayList<>();
        hellos.add(protocolHello);
        for (RegistryWorker worker : registry.liveWorkers()) {
            if (!MASTER_LEADER_ROLE_ID.equals(worker.workerId())) {
                hellos.add(worker.protocolHello());
            }
        }
        return hellos;
    }

    io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata() {
        return securityMetadata;
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

    /** The RPC handler, which runs on the connection's own thread, apart from the threads executing stages. */
    private RpcMessage handle(
            RpcMessage req, io.rowweave.cluster.rpc.RpcRequestContext requestContext) {
        io.rowweave.cluster.rpc.RpcRequestPolicy.MASTER.requireAllowedDirect(req.type());
        if (req.type() == RpcMessage.Type.CANCEL_RUN) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.runCancellationFailure(
                        req.runId(), "run cancellation unavailable: not active master leader");
            }
            return handleRunCancellation(req);
        }
        if (req.type() == RpcMessage.Type.QUERY_RUN) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.runSnapshotFailure(
                        "run query unavailable: not active master leader");
            }
            return handleRunQuery(req);
        }
        if (req.type() == RpcMessage.Type.QUERY_INCREMENTAL_FRONTIER) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.incrementalFrontierViewFailure(
                        req.runId(), "frontier query unavailable: not active master leader");
            }
            return handleIncrementalFrontierQuery(req);
        }
        if (req.type() == RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.incrementalFrontierViewFailure(
                        req.runId(),
                        "feed initialization unavailable: not active master leader");
            }
            return handleIncrementalFeedInitialization(req);
        }
        if (req.type() == RpcMessage.Type.QUERY_RUN_EVENTS) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.runEventsPageFailure(
                        req.runId(), "run event query unavailable: not active master leader");
            }
            return handleRunEventsQuery(req);
        }
        if (req.type() == RpcMessage.Type.QUERY_QUALITY_GATE_DECISION) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.qualityGateDecisionFailure(
                        req.runId(),
                        "quality decision query unavailable: not active master leader");
            }
            return handleQualityGateDecisionQuery(req);
        }
        if (req.type() == RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME) {
            if (closed || !isActiveLeader()) {
                return RpcMessage.batchSinkOutcome(
                        req.runId(),
                        io.rowweave.commit.BatchSinkOutcome.State
                                .CORRUPT_OR_CONFLICT.name(),
                        0L, null, "OUTCOME_AUTHORITY_UNAVAILABLE");
            }
            return handleBatchSinkOutcomeQuery(req);
        }
        if (req.type() == RpcMessage.Type.QUERY_MANIFEST_OUTCOME) {
            if (closed || !isActiveLeader()) {
                return unavailableResult();
            }
            return handleManifestOutcomeQuery(req);
        }
        if (req.type() == RpcMessage.Type.SUBMIT_JOB
                || req.type() == RpcMessage.Type.SUBMIT_PLAN_ARTIFACT
                || req.type() == RpcMessage.Type.SUBMIT_RUN
                || req.type() == RpcMessage.Type.SUBMIT_INCREMENTAL_RUN) {
            if (req.type() == RpcMessage.Type.SUBMIT_JOB
                    && controlSecurityProfile
                    == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS) {
                return RpcMessage.jobResult(false, 0,
                        "MTLS requires SUBMIT_PLAN_ARTIFACT or SUBMIT_RUN");
            }
            if (!beginSubmit()) {
                return closingResult();
            }
            UpgradeRuntimeCoordinator.Lease upgradeRun = null;
            UpgradeRuntimeCoordinator.Lease upgradeInterval = null;
            try {
                if (!isActiveLeader()) {
                    return unavailableResult();
                }
                if (upgradeRuntimeCoordinator != null) {
                    try {
                        upgradeRun = upgradeRuntimeCoordinator.beginRun();
                        if (req.type()
                                == RpcMessage.Type.SUBMIT_INCREMENTAL_RUN) {
                            upgradeInterval =
                                    upgradeRuntimeCoordinator.beginInterval();
                        }
                    } catch (IllegalStateException admissionFailure) {
                        if (upgradeRun != null) upgradeRun.close();
                        return RpcMessage.jobResult(false, 0,
                                admissionFailure.getMessage());
                    }
                }
                if (req.type() == RpcMessage.Type.SUBMIT_RUN) {
                    return handleDurableSubmit(req);
                }
                if (req.type() == RpcMessage.Type.SUBMIT_INCREMENTAL_RUN) {
                    return handleIncrementalSubmit(req);
                }
                return handleSubmit(req);
            } finally {
                if (upgradeInterval != null) upgradeInterval.close();
                if (upgradeRun != null) upgradeRun.close();
                endSubmit();
            }
        }
        if (closed) {
            switch (req.type()) {
                case HEARTBEAT:
                case PROGRESS_REPORT:
                case DEREGISTER_WORKER:
                    return null;
                case REGISTER_WORKER:
                    return closingResult();
                case STAGE_REPORT:
                case PORT_REPORT:
                case ASSIGNMENT_ACCEPTED:
                    break;  // while closing drains, a worker's terminal report is still accepted and acknowledged, so it is not left retrying a load-bearing report forever
                default:
                    break;
            }
        }
        if (!closed && !isActiveLeader()) {
            switch (req.type()) {
                case HEARTBEAT:
                case PROGRESS_REPORT:
                case DEREGISTER_WORKER:
                    return null;  // a one-way protocol must not answer with a failure onto a reused connection
                case REGISTER_WORKER:
                case STAGE_REPORT:
                case PORT_REPORT:
                case ASSIGNMENT_ACCEPTED:
                    return unavailableResult();
                default:
                    break;
            }
        }
        switch (req.type()) {
            case REGISTER_WORKER: {
                final io.rowweave.cluster.rpc.ProtocolHelloV1 workerProtocolHello;
                if (protocolHello == null) {
                    workerProtocolHello = null;
                } else if (requestContext == null
                        || requestContext.negotiatedProtocol() == null
                        || requestContext.remoteProtocolHello() == null
                        || req.incarnationId() == null) {
                    return RpcMessage.jobResult(false, 0,
                            "worker protocol registration rejected: negotiated incarnation required");
                } else {
                    workerProtocolHello = requestContext.remoteProtocolHello();
                }
                final io.rowweave.cluster.rpc.DataPlaneRegistration acceptedDataRegistration;
                try {
                    acceptedDataRegistration = io.rowweave.cluster.rpc
                            .DataPlaneRegistrationNegotiator.requireMasterCompatible(
                                    controlSecurityProfile, req.dataPlaneRegistration());
                } catch (IllegalArgumentException incompatible) {
                    return RpcMessage.jobResult(false, 0,
                            "worker data registration rejected: incompatible capability");
                }
                if (controlSecurityProfile
                        == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS) {
                    io.rowweave.cluster.rpc.AuthenticatedRpcPeer peer = requestContext == null
                            ? null : requestContext.authenticatedPeer();
                    if (peer == null
                            || !peer.hasRole(io.rowweave.cluster.rpc.ControlRole.WORKER)
                            || !java.security.MessageDigest.isEqual(
                                    acceptedDataRegistration.certificateFingerprint(),
                                    peer.certificateFingerprint())) {
                        return RpcMessage.jobResult(false, 0,
                                "worker data registration rejected: authenticated identity mismatch");
                    }
                }
                io.rowweave.cluster.rpc.DataPlaneRegistration storedDataRegistration =
                        controlSecurityProfile
                        == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS
                                ? acceptedDataRegistration : null;
                // Where the registry implementation is owned by the master, as the in-process one is, the master still
                // writes the shared record. A ZooKeeper ephemeral node must be held by the worker's own session, so the
                // master keeps only heartbeat and slot state locally and never takes that lease over. Stored here is the
                // worker's data port, or -1 when multiplexing is off, which is what an assignment needs in order to build
                // a muxed shuffle endpoint.
                synchronized (workerLifecycleLock) {
                    // The admission check and the clearing that follows a loss take the same lock, which closes the window
                    // where authority is lost just after the check and the new term's state is then polluted.
                    if (!isActiveLeader()) {
                        return unavailableResult();
                    }
                    if (!registry.workerRegistrationOwnedByWorker()) {
                        registry.register(new RegistryWorker(
                                req.workerId(), req.host(), req.port(), req.slots(), req.dataPort(),
                                req.incarnationId(), req.securityMetadata(),
                                storedDataRegistration, workerProtocolHello));
                    }
                    workers.put(req.workerId(),
                            new WorkerInfo(req.workerId(), req.host(), req.port(), req.slots(), req.dataPort(),
                                    req.incarnationId(), heartbeatStalenessMillis,
                                    req.securityMetadata(), storedDataRegistration,
                                    workerProtocolHello));
                    QuarantinedWorker quarantined = quarantinedWorkers.get(req.workerId());
                    if (quarantined == null) {
                        slotLedger.setWorkerCapacity(req.workerId(), req.slots());
                    } else if (!quarantined.sameEndpoint(req)) {
                        // A changed rpc endpoint or incarnation means the process restarted, so no stage of the old one can
                        // still be holding resources in the new.
                        if (quarantinedWorkers.remove(req.workerId(), quarantined)) {
                            slotLedger.setWorkerCapacity(req.workerId(), req.slots());
                        }
                    }
                }
                return controlSecurityProfile
                        == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS
                        ? RpcMessage.workerRegistered(
                                req.incarnationId(), acceptedDataRegistration)
                        : RpcMessage.ack();
            }
            case DEREGISTER_WORKER:
                handleWorkerDeparture(req);
                return null;
            case HEARTBEAT: {
                boolean currentGeneration = false;
                synchronized (workerLifecycleLock) {
                    WorkerInfo w = workers.get(req.workerId());
                    currentGeneration = w != null
                            && java.util.Objects.equals(w.incarnationId, req.incarnationId());
                    if (currentGeneration) {
                        w.heartbeat.beat();
                    }
                    QuarantinedWorker quarantined = quarantinedWorkers.get(req.workerId());
                    if (currentGeneration && quarantined != null && !quarantined.departureFenced
                            && java.util.Objects.equals(quarantined.incarnationId, req.incarnationId())
                            && req.freeSlots() == quarantined.slots
                            && quarantinedWorkers.remove(req.workerId(), quarantined)) {
                        slotLedger.setWorkerCapacity(req.workerId(), quarantined.slots);
                    }
                }
                // The freeSlots a worker reports is used only for reconciliation: the ledger stays
                // authoritative and admission never reads the report, because heartbeats lag and trusting
                // them would oversell. A ledger-minus-report difference away from 0 is a drift or leak
                // signal and is logged at debug; an unknown -1 is skipped.
                int drift = currentGeneration ? slotLedger.reconcile(req.workerId(), req.freeSlots()) : 0;
                if (TRACE_HEARTBEATS) {
                    LOG.info("heartbeat trace received worker={} incarnation={} currentGeneration={} freeSlots={}",
                            req.workerId(), req.incarnationId(), currentGeneration, req.freeSlots());
                }
                if (drift != 0 && LOG.isDebugEnabled()) {
                    LOG.debug("slot reconcile drift worker={} ledgerFree={} reportedFree={} drift={}",
                            req.workerId(), slotLedger.freeSlots(req.workerId()), req.freeSlots(), drift);
                }
                return null;  // one-way protocol: the server sends no frame back, so no stray acknowledgement can be read against the wrong request
            }
            case STAGE_REPORT: {
                if (req.stageTerminalReceiptBytes() != null) {
                    return handleDurableStageTerminal(req);
                }
                if (!stageMessageCurrent(req)) {
                    return RpcMessage.ack();
                }
                beatWorker(req.workerId());  // any message from a worker is evidence that it is alive
                StageTracker t = stageTrackers.get(req.stageId());
                if (!req.success()) {
                    LOG.warn("stage report FAILED stageId={} err={}", req.stageId(), req.errorMessage());
                }
                if (t != null) {
                    t.onReport(req.success(), req.inputRows(), req.rowCount(),
                            req.errorMessage(), decodeStageTelemetry(req),
                            () -> pushStageUpdate(req));
                }
                return RpcMessage.ack();
            }
            case ASSIGNMENT_ACCEPTED:
                return handleAssignmentAcceptance(req);
            case PROGRESS_REPORT: {
                if (!stageMessageCurrent(req)) {
                    return null;
                }
                beatWorker(req.workerId());
                pushStageProgress(req);  // the rows a running stage has reached, pushed so a monitor sees them grow
                return null;  // one-way protocol: the server sends no frame back
            }
            case PORT_REPORT: {
                if (!stageMessageCurrent(req)) {
                    return RpcMessage.ack();
                }
                beatWorker(req.workerId());
                StagePortTracker pt = portTrackers.get(req.stageId());
                if (pt != null) {
                    pt.onPort(req.portIndex(), req.port());
                }
                return RpcMessage.ack();
            }
            default:
                return RpcMessage.ack();
        }
    }

    private RpcMessage handleDurableStageTerminal(RpcMessage request) {
        if (stageAssignmentAuthority == null) {
            return RpcMessage.jobResult(false, 0,
                    "stage terminal unavailable: authority capability missing");
        }
        final StageTerminalReceipt receipt;
        final PreparedSinkReceipt preparedSink;
        try {
            StageTerminalReceipt baseReceipt = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    request.stageTerminalReceiptBytes());
            if (baseReceipt.outcome() == io.rowweave.run.StageTerminalOutcome.LOST
                    || baseReceipt.token().runtimeStageId() != request.stageId()
                    || !baseReceipt.token().workerId().equals(request.workerId())
                    || !baseReceipt.token().workerIncarnationId().equals(request.incarnationId())
                    || !baseReceipt.token().fence().equals(request.runAttemptFence())
                    || baseReceipt.inputRows() != request.inputRows()
                    || baseReceipt.outputRows() != request.rowCount()
                    || baseReceipt.succeeded() != request.success()
                    || !java.util.Objects.equals(
                    baseReceipt.failureCategory(), request.errorMessage())) {
                throw new IllegalArgumentException("terminal receipt does not match RPC envelope");
            }
            receipt = baseReceipt.withTelemetry(decodeStageTelemetry(request));
            byte[] preparedBytes = request.preparedSinkReceiptBytes();
            preparedSink = preparedBytes == null ? null
                    : PreparedSinkReceiptCodec.decode(preparedBytes);
            if (preparedSink != null && (!receipt.succeeded()
                    || !preparedSink.token().equals(receipt.token())
                    || !java.security.MessageDigest.isEqual(
                    preparedSink.commandDigest(), receipt.commandDigest()))) {
                throw new IllegalArgumentException(
                        "prepared Sink receipt does not match terminal receipt");
            }
        } catch (RuntimeException invalid) {
            return RpcMessage.jobResult(false, 0,
                    "stage terminal rejected: invalid envelope");
        }

        try {
            StageOwner localOwner = stageWorker.get(request.stageId());
            io.rowweave.incremental.FrontierRecoveryToken
                    reportFrontierToken = localOwner == null
                    ? null : localOwner.frontierToken;
            if (reportFrontierToken == null && localOwner == null
                    && incrementalAuthorityGateway != null) {
                io.rowweave.incremental.IncrementalAuthorityRecord active =
                        incrementalAuthorityGateway.activeRecordForRun(
                                receipt.token().fence().runId());
                if (active != null) {
                    reportFrontierToken = incrementalAuthorityGateway
                            .frontierOwnership(active.feedId());
                }
            }
            boolean incrementalReport = reportFrontierToken != null;
            RecoveryToken recoveryToken = incrementalReport
                    ? incrementalAuthorityGateway.runOwnership(
                            receipt.token().fence().runId())
                    : currentReportRecoveryToken(
                            localOwner, receipt.token().fence().runId());
            if (localOwner == null && recoveryToken == null && leadership != null) {
                return RpcMessage.jobResult(false, 0,
                        "stage terminal unavailable: recovery claim not established");
            }

            StageTerminalDecision decision;
            if (incrementalReport) {
                decision = incrementalAuthorityGateway.recordStageTerminal(
                        reportFrontierToken, recoveryToken, receipt);
            } else if (recoveryToken == null) {
                decision = stageAssignmentAuthority.recordTerminal(receipt);
            } else if (stageAssignmentAuthority
                    instanceof RecoveryFencedStageAssignmentAuthority) {
                decision = ((RecoveryFencedStageAssignmentAuthority)
                        stageAssignmentAuthority).recordTerminalRecovered(
                        recoveryToken, receipt);
            } else {
                return RpcMessage.jobResult(false, 0,
                        "stage terminal unavailable: recovery-fenced authority missing");
            }
            if (decision.code() == StageTerminalDecision.Code.STALE_RUN
                    && terminalReportAfterCleanup(receipt)) {
                return RpcMessage.ack();
            }
            if (decision.code() == StageTerminalDecision.Code.ACCEPTANCE_REQUIRED
                    && recoveryToken != null) {
                // A recovery claim atomically advances the acceptance generation. An old
                // token that still has no receipt can therefore never acquire execution
                // authority later; absorb its cancellation without manufacturing a
                // terminal receipt or pinning the Worker's report lane forever.
                return RpcMessage.ack();
            }
            if (decision.code() != StageTerminalDecision.Code.RECORDED
                    && decision.code() != StageTerminalDecision.Code.ALREADY_RECORDED) {
                String kind = decision.code() == StageTerminalDecision.Code.UNAVAILABLE
                        || decision.code() == StageTerminalDecision.Code.RECOVERY_OWNED
                        ? "unavailable" : "rejected";
                return RpcMessage.jobResult(false, 0,
                        "stage terminal " + kind + ": " + decision.code());
            }

            if (preparedSink != null) {
                // Terminal and prepared-locator evidence are two independently
                // retriable CASes. Recovery inventory may rotate the owner after
                // observing the terminal CAS, so never reuse the token sampled
                // before that first mutation.
                recoveryToken = incrementalReport
                        ? incrementalAuthorityGateway.runOwnership(
                                receipt.token().fence().runId())
                        : currentReportRecoveryToken(
                        localOwner, receipt.token().fence().runId());
                PreparedSinkDecision preparedDecision;
                if (incrementalReport) {
                    preparedDecision = incrementalAuthorityGateway
                            .recordPreparedSink(
                                    reportFrontierToken, recoveryToken,
                                    preparedSink);
                } else if (recoveryToken == null) {
                    preparedDecision = stageAssignmentAuthority.recordPreparedSink(
                            preparedSink);
                } else if (stageAssignmentAuthority
                        instanceof RecoveryFencedStageAssignmentAuthority) {
                    preparedDecision = ((RecoveryFencedStageAssignmentAuthority)
                            stageAssignmentAuthority).recordPreparedSinkRecovered(
                            recoveryToken, preparedSink);
                } else {
                    return RpcMessage.jobResult(false, 0,
                            "prepared Sink unavailable: recovery-fenced authority missing");
                }
                if (preparedDecision.code() != PreparedSinkDecision.Code.RECORDED
                        && preparedDecision.code()
                        != PreparedSinkDecision.Code.ALREADY_RECORDED) {
                    String kind = preparedDecision.code()
                            == PreparedSinkDecision.Code.UNAVAILABLE
                            || preparedDecision.code()
                            == PreparedSinkDecision.Code.RECOVERY_OWNED
                            ? "unavailable" : "rejected";
                    return RpcMessage.jobResult(false, 0,
                            "prepared Sink " + kind + ": "
                                    + preparedDecision.code());
                }
            }

            beatWorker(request.workerId());
            if (!request.success()) {
                LOG.warn("stage report FAILED stageId={} category={}",
                        request.stageId(), request.errorMessage());
            }
            StageTracker tracker = stageTrackers.get(request.stageId());
            if (tracker != null) {
                tracker.onReport(request.success(), request.inputRows(),
                        request.rowCount(), request.errorMessage(),
                        io.rowweave.plan.StageExecutionTelemetry.unavailable(),
                        () -> pushStageUpdate(request));
            }
            if (durableTerminalAckLossForTesting.getAndUpdate(
                    value -> value > 0 ? value - 1 : 0) > 0) {
                return RpcMessage.jobResult(false, 0,
                        "stage terminal unavailable: injected ACK loss");
            }
            return RpcMessage.ack();
        } catch (RuntimeException unavailable) {
            return RpcMessage.jobResult(false, 0,
                    "stage terminal unavailable: " + unavailable.getMessage());
        }
    }

    private RecoveryToken currentReportRecoveryToken(
            StageOwner localOwner, String runId) {
        RecoveryToken assigned = localOwner == null
                ? null : localOwner.recoveryToken;
        if (recoveryOwnership == null) {
            return assigned;
        }
        RecoveryToken current = recoveryOwnership.ownership(runId);
        if (assigned != null && current == null) {
            throw new IllegalStateException(
                    "recovery claim not established for durable stage report");
        }
        if (current != null && leadership != null) {
            MasterElection election = leadership;
            if (!election.isLeader()
                    || election.termToken()
                    != current.owner().leaderTermToken()
                    || !election.self().toString().equals(
                    current.owner().ownerId())) {
                throw new IllegalStateException(
                        "durable stage report recovery owner is not this leader");
            }
        }
        return current;
    }

    private boolean terminalReportAfterCleanup(StageTerminalReceipt receipt) {
        if (runStateStore == null) return false;
        RunSnapshot run = runStateStore.get(receipt.token().fence().runId());
        AttemptSnapshot attempt = run == null ? null : run.currentAttempt();
        return run != null && run.status().isTerminal() && attempt != null
                && attempt.attemptId().equals(receipt.token().fence().attemptId())
                && attempt.fenceToken() == receipt.token().fence().fenceToken();
    }

    private RpcMessage handleRunQuery(RpcMessage req) {
        if (runStateStore == null) {
            return RpcMessage.runSnapshotFailure("run state authority unavailable");
        }
        try {
            io.rowweave.run.DurableExecutionEvidenceView view = null;
            final RunSnapshot snapshot;
            if (!durableExecutionEvidenceProviderPresent) {
                snapshot = runStateStore.get(req.runId());
            } else {
                view = requireDurableExecutionEvidenceAuthority()
                        .executionEvidence(req.runId());
                if (view == null) {
                    throw new IllegalStateException(
                            "coherent execution evidence authority returned null");
                }
                snapshot = view.snapshot();
            }
            return snapshot == null
                    ? RpcMessage.runSnapshotNotFound()
                    : runSnapshotResponse(snapshot, view, req);
        } catch (RuntimeException failure) {
            return RpcMessage.runSnapshotFailure(
                    "run query failed: " + failure.getMessage());
        }
    }

    private RpcMessage handleIncrementalFrontierQuery(RpcMessage req) {
        if (incrementalAuthorityGateway == null) {
            return RpcMessage.incrementalFrontierViewFailure(
                    req.runId(), "frontier authority unavailable");
        }
        try {
            io.rowweave.incremental.IncrementalAuthorityRecord record =
                    incrementalAuthorityGateway.record(req.runId());
            if (record == null) {
                return RpcMessage.incrementalFrontierViewNotFound(req.runId());
            }
            io.rowweave.incremental.IncrementalFrontierView view =
                    io.rowweave.incremental.IncrementalFrontierView.from(record);
            req.reserveResponseResidentBytes(
                    io.rowweave.incremental.IncrementalFrontierViewCodec
                            .MAX_FRAME_BYTES);
            byte[] encoded = io.rowweave.incremental.IncrementalFrontierViewCodec
                    .encode(view);
            return RpcMessage.incrementalFrontierView(req.runId(), encoded);
        } catch (RuntimeException failure) {
            return RpcMessage.incrementalFrontierViewFailure(
                    req.runId(), "frontier query unavailable");
        }
    }

    private RpcMessage handleIncrementalFeedInitialization(RpcMessage req) {
        if (incrementalAuthorityGateway == null) {
            return RpcMessage.incrementalFrontierViewFailure(
                    req.runId(), "feed initialization authority unavailable");
        }
        try {
            io.rowweave.incremental.IncrementalInitializeRequest initialize =
                    io.rowweave.incremental.IncrementalInitializeRequestCodec
                            .decode(req.incrementalInitializeBytes());
            if (!req.runId().equals(initialize.feedId())) {
                return RpcMessage.incrementalFrontierViewFailure(
                        req.runId(), "feed initialization identity mismatch");
            }
            io.rowweave.incremental.IncrementalAuthorityDecision decision =
                    incrementalAuthorityGateway.initialize(
                            req.attemptId(), initialize);
            if (decision.code()
                    != io.rowweave.incremental.IncrementalAuthorityDecision
                    .Code.ACCEPTED
                    && decision.code()
                    != io.rowweave.incremental.IncrementalAuthorityDecision
                    .Code.ALREADY_APPLIED) {
                return RpcMessage.incrementalFrontierViewFailure(
                        req.runId(), "feed initialization rejected: "
                                + decision.code().name());
            }
            io.rowweave.incremental.IncrementalFrontierView view =
                    io.rowweave.incremental.IncrementalFrontierView.from(
                            decision.record());
            req.reserveResponseResidentBytes(
                    io.rowweave.incremental.IncrementalFrontierViewCodec
                            .MAX_FRAME_BYTES);
            return RpcMessage.incrementalFrontierView(
                    req.runId(),
                    io.rowweave.incremental.IncrementalFrontierViewCodec
                            .encode(view));
        } catch (RuntimeException failure) {
            return RpcMessage.incrementalFrontierViewFailure(
                    req.runId(), "feed initialization unavailable");
        }
    }

    private RpcMessage runSnapshotResponse(
            RunSnapshot snapshot,
            io.rowweave.run.DurableExecutionEvidenceView view,
            RpcMessage request) {
        byte[] snapshotBytes = RunStateCodec.encodeSnapshot(
                snapshot, request::reserveResponseResidentBytes);
        if (view == null) return RpcMessage.runSnapshot(snapshotBytes);
        try {
            request.reserveResponseResidentBytes(
                    io.rowweave.plan.ExecutionTelemetrySummaryCodec
                            .MAX_MATERIALIZED_RESIDENT_BYTES);
            return RpcMessage.runSnapshotWithTelemetrySummary(
                    snapshotBytes, DurableExecutionTelemetryResolver.resolve(view));
        } catch (RuntimeException | LinkageError unavailableTelemetry) {
            return RpcMessage.runSnapshot(snapshotBytes);
        }
    }

    private RpcMessage handleRunCancellation(RpcMessage req) {
        if (runCancellationAuthority == null) {
            return RpcMessage.runCancellationFailure(
                    req.runId(), "run cancellation unavailable: authority capability missing");
        }
        try {
            java.util.concurrent.locks.Lock fence = runDispatchLifecycle.writeLock();
            fence.lock();
            try {
                if (incrementalAuthorityGateway != null) {
                    io.rowweave.incremental.IncrementalAuthorityRecord record =
                            incrementalAuthorityGateway.activeRecordForRun(
                                    req.runId());
                    if (record != null) {
                        io.rowweave.incremental.FrontierRecoveryToken
                                frontierToken = incrementalAuthorityGateway
                                .frontierOwnership(record.feedId());
                        RecoveryToken runToken = incrementalAuthorityGateway
                                .runOwnership(req.runId());
                        if (frontierToken == null || runToken == null) {
                            throw new IllegalStateException(
                                    "incremental cancellation ownership unavailable");
                        }
                        return RpcMessage.runCancellationResult(
                                incrementalAuthorityGateway
                                        .requestUserCancellation(
                                                frontierToken, runToken,
                                                req.runId(),
                                                req.cancelRequestId()));
                    }
                }
                return RpcMessage.runCancellationResult(
                        runCancellationAuthority.requestUserCancellation(
                                req.runId(), req.cancelRequestId()));
            } finally {
                fence.unlock();
            }
        } catch (RuntimeException unavailable) {
            return RpcMessage.runCancellationFailure(
                    req.runId(), "run cancellation unavailable");
        }
    }

    private RunDeadlineResult evaluateRunDeadline(String runId) {
        if (runDeadlineAuthority == null) {
            throw new IllegalStateException(
                    "run deadline unavailable: authority capability missing");
        }
        java.util.concurrent.locks.Lock fence = runDispatchLifecycle.writeLock();
        fence.lock();
        try {
            return runDeadlineAuthority.evaluateDeadline(runId);
        } finally {
            fence.unlock();
        }
    }

    private static boolean deadlineStopsExecution(RunDeadlineResult result) {
        return result.disposition() == RunDeadlineResult.Disposition.ACCEPTED
                || result.disposition()
                == RunDeadlineResult.Disposition.ALREADY_REQUESTED
                || result.disposition()
                == RunDeadlineResult.Disposition.ALREADY_TERMINAL
                || result.disposition() == RunDeadlineResult.Disposition.RETIRING;
    }

    private RpcMessage handleRunEventsQuery(RpcMessage req) {
        if (runStateStore == null) {
            return RpcMessage.runEventsPageFailure(
                    req.runId(), "run event query unavailable: authority capability missing");
        }
        try {
            int publicLimit = req.eventLimit();
            List<ExecutionEvent> authorityPage = null;
            RunExecutionEventPage page = null;
            for (int read = 0; read < RUN_EVENT_QUERY_MAX_READS; read++) {
                authorityPage = runStateStore.events(
                        req.runId(), req.eventAfterSequence(), publicLimit + 1);
                if (!authorityPage.isEmpty()) break;
                RunSnapshot snapshot = runStateStore.get(req.runId());
                if (snapshot == null) {
                    page = RunExecutionEventPage.notFound(req.eventAfterSequence());
                    break;
                }
                if (snapshot.revision() <= req.eventAfterSequence()) {
                    page = RunExecutionEventPage.found(
                            req.eventAfterSequence(), java.util.Collections.emptyList(), false);
                    break;
                }
            }
            if (page == null && authorityPage != null && !authorityPage.isEmpty()) {
                int resultCount = Math.min(publicLimit, authorityPage.size());
                req.reserveResponseResidentBytes(64L + 128L * resultCount);
                List<RunExecutionEvent> projected = new java.util.ArrayList<>(resultCount);
                for (int i = 0; i < resultCount; i++) {
                    projected.add(authorityPage.get(i).publicView());
                }
                page = RunExecutionEventPage.found(
                        req.eventAfterSequence(), projected, authorityPage.size() > publicLimit);
            }
            if (page == null) {
                throw new IllegalStateException(
                        "run event authority view did not converge");
            }
            byte[] encoded = RunExecutionEventPageCodec.encode(
                    page, req::reserveResponseResidentBytes);
            return RpcMessage.runEventsPage(req.runId(), encoded);
        } catch (RunHistoryRetiringException retiring) {
            byte[] encoded = RunExecutionEventPageCodec.encode(
                    RunExecutionEventPage.retiring(req.eventAfterSequence()),
                    req::reserveResponseResidentBytes);
            return RpcMessage.runEventsPage(req.runId(), encoded);
        } catch (RuntimeException unavailable) {
            return RpcMessage.runEventsPageFailure(
                    req.runId(), "run event query unavailable");
        }
    }

    private RpcMessage handleBatchSinkOutcomeQuery(RpcMessage req) {
        String runId;
        try {
            runId = new RunAttemptFence(req.runId(), "query", 1L).runId();
        } catch (RuntimeException invalid) {
            return RpcMessage.batchSinkOutcome(
                    "invalid-run-query",
                    io.rowweave.commit.BatchSinkOutcome.State
                            .CORRUPT_OR_CONFLICT.name(),
                    0L, null, "INVALID_RUN_ID");
        }
        try {
            io.rowweave.commit.BatchSinkOutcome outcome =
                    projectBatchSinkOutcome(runId, req);
            byte[] identity = outcome.identityDigest();
            if (identity != null) req.reserveResponseResidentBytes(identity.length);
            return RpcMessage.batchSinkOutcome(
                    runId, outcome.state().name(),
                    outcome.generation() == null ? 0L : outcome.generation(),
                    identity, outcome.detailCode());
        } catch (RuntimeException failure) {
            return RpcMessage.batchSinkOutcome(
                    runId,
                    io.rowweave.commit.BatchSinkOutcome.State
                            .CORRUPT_OR_CONFLICT.name(),
                    0L, null, "OUTCOME_PROJECTION_FAILED");
        }
    }

    private RpcMessage handleQualityGateDecisionQuery(RpcMessage req) {
        if (runStateStore == null || stageAssignmentAuthority == null) {
            return RpcMessage.qualityGateDecisionFailure(
                    req.runId(),
                    "quality decision query unavailable: authority capability missing");
        }
        try {
            RunSnapshot snapshot = runStateStore.get(req.runId());
            if (snapshot == null || snapshot.currentAttempt() == null) {
                return RpcMessage.qualityGateDecisionNotFound(req.runId());
            }
            AttemptSnapshot attempt = snapshot.currentAttempt();
            io.rowweave.QualityGateDecision decision =
                    stageAssignmentAuthority.qualityGateDecision(
                            new RunAttemptFence(snapshot.runId(),
                                    attempt.attemptId(), attempt.fenceToken()));
            if (decision == null) {
                return RpcMessage.qualityGateDecisionNotFound(req.runId());
            }
            req.reserveResponseResidentBytes(
                    io.rowweave.QualityGateDecisionCodec.MAX_FRAME_BYTES);
            byte[] encoded = io.rowweave.QualityGateDecisionCodec.encode(decision);
            return RpcMessage.qualityGateDecision(req.runId(), encoded);
        } catch (RuntimeException unavailable) {
            return RpcMessage.qualityGateDecisionFailure(
                    req.runId(), "quality decision query unavailable");
        }
    }

    private io.rowweave.commit.BatchSinkOutcome projectBatchSinkOutcome(
            String runId, RpcMessage request) {
        if (runStateStore == null || artifactRepository == null
                || stageAssignmentAuthority == null) {
            throw new IllegalStateException("durable outcome authority is unavailable");
        }
        RunSnapshot snapshot = runStateStore.get(runId);
        if (snapshot == null) return io.rowweave.commit.BatchSinkOutcome.notFound();
        byte[] artifactBytes = artifactRepository.get(
                snapshot.artifactId(), request::reserveResidentBytes);
        if (artifactBytes == null) {
            throw new IllegalStateException("durable outcome artifact is unavailable");
        }
        PlanArtifact artifact = PlanArtifactCodec.decode(
                artifactBytes, request::reserveResidentBytes);
        requireArtifactDeployment(artifact);
        PlanGraph graph;
        if (artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            PortableBatchPlan plan = PortablePlanArtifact
                    .preflightDurableValidated(
                            artifact,
                            deploymentRuntime.portableBindingVerifier(),
                            request::reserveResidentBytes);
            graph = PortablePlanCompiler.projectMetadata(
                    plan, deploymentRuntime.portableBindingVerifier());
        } else {
            PlanArtifactValidator.validateForCurrentRuntime(artifact);
            io.rowweave.plan.PlanGraphEnvelopeScanner.Scan providerScan =
                    io.rowweave.plan.PlanGraphEnvelopeScanner.scanDurable(
                            artifact.payload(), request::reserveResidentBytes);
            io.rowweave.plan.DurableSinkProviderPlanPreflight.validate(
                    providerScan,
                    deploymentRuntime.durableSinkProviderRegistry());
            graph = LegacyPlanArtifactAdapter.unwrapValidated(
                    artifact, objectTrustContext,
                    request::reserveResidentBytes)
                    .restoreBuiltinManifestOutputs();
            io.rowweave.plan.OutputCommitValidator.validateDurable(graph);
        }

        return projectBatchSinkOutcome(snapshot, graph);
    }

    private io.rowweave.commit.BatchSinkOutcome projectBatchSinkOutcome(
            RunSnapshot snapshot, PlanGraph graph) {
        io.rowweave.BatchSinkDescriptor.CapabilityProfile profile = null;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            io.rowweave.BatchSinkDescriptor.CapabilityProfile current =
                    node.batchSinkDescriptor().capabilityProfile();
            if (profile != null && profile != current) {
                throw new IllegalStateException("durable Sink outcome profile drift");
            }
            profile = current;
        }
        if (profile == io.rowweave.BatchSinkDescriptor.CapabilityProfile
                .SIDE_EFFECT_FREE_V1) {
            if (snapshot.status() == io.rowweave.run.RunStatus.SUCCEEDED) {
                return io.rowweave.commit.BatchSinkOutcome.of(
                        io.rowweave.commit.BatchSinkOutcome.State.COMMITTED,
                        null, null, "SIDE_EFFECT_FREE_COMMITTED");
            }
            if (snapshot.status() == io.rowweave.run.RunStatus.FAILED
                    || snapshot.status() == io.rowweave.run.RunStatus.CANCELED) {
                return io.rowweave.commit.BatchSinkOutcome.of(
                        io.rowweave.commit.BatchSinkOutcome.State.ABORTED,
                        null, null, "SIDE_EFFECT_FREE_ABORTED");
            }
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.PREPARING,
                    null, null, "SIDE_EFFECT_FREE_RUNNING");
        }
        if (profile != io.rowweave.BatchSinkDescriptor.CapabilityProfile
                .MANIFEST_ATOMIC_GROUP_V1) {
            throw new IllegalStateException("unsupported durable Sink outcome profile");
        }
        if (containsUnprojectedProviderOutput(graph)) {
            if (testProviderAuthorityFactory == null
                    || snapshot.currentAttempt() == null) {
                throw new IllegalStateException(
                        "custom durable Sink outcome authority is unavailable");
            }
            AttemptSnapshot attempt = snapshot.currentAttempt();
            RunAttemptFence fence = new RunAttemptFence(snapshot.runId(),
                    attempt.attemptId(), attempt.fenceToken());
            StageAssignmentSnapshot evidence =
                    stageAssignmentAuthority.assignmentSnapshot(fence);
            if (evidence == null || !hasProviderLocatorClosure(evidence)) {
                return testProviderAuthorityFactory.query(
                        fence, attempt.publicationId());
            }
            java.util.List<io.rowweave.ProviderBatchSink> outputs =
                    providerOutputs(graph);
            try (DurableSinkAuthorityAdapterFactory.Reservation reservation =
                         testProviderAuthorityFactory.reserve(
                                 fence, attempt.publicationId(), outputs)) {
                java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                        providerLocators(evidence);
                DurableSinkCommitCoordinator coordinator =
                        new DurableSinkCommitCoordinator(
                                reservation.bind(locators),
                                (action, decisionDigest) ->
                                        executeRecoveredProviderGroupControl(
                                                graph, evidence,
                                                attempt.publicationId(),
                                                reservation.generation(), action,
                                                decisionDigest,
                                                deadlineAfterMillis(
                                                        outputCommitTimeoutMs)));
                return coordinator.query();
            }
        }
        return projectManifestBatchSinkOutcome(snapshot, graph);
    }

    private io.rowweave.commit.BatchSinkOutcome projectManifestBatchSinkOutcome(
            RunSnapshot snapshot, PlanGraph graph) {
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (attempt == null) {
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.PREPARING,
                    null, null, "AWAITING_ATTEMPT");
        }
        RunAttemptFence fence = new RunAttemptFence(
                snapshot.runId(), attempt.attemptId(), attempt.fenceToken());
        boolean qualityBounded = containsQualityGateOutput(graph);
        io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                qualityBounded
                        ? stageAssignmentAuthority.qualityGateCommitJournal(fence)
                        : null;
        ManifestCommitJournalSnapshot journal = qualityBounded ? null
                : stageAssignmentAuthority.manifestCommitJournal(fence);
        io.rowweave.run.BatchSinkAbortRecord abort =
                stageAssignmentAuthority.batchSinkAbort(fence);
        if (abort != null) {
            if (!attempt.publicationId().equals(abort.publicationId())) {
                throw new IllegalStateException("manifest abort publication identity mismatch");
            }
            if (qualityBounded
                    ? qualityJournal != null && qualityJournal.intent() != null
                    : journal != null && journal.intent() != null) {
                throw new IllegalStateException(
                        "manifest authority contains conflicting commit and abort decisions");
            }
            return io.rowweave.commit.BatchSinkOutcome.of(
                    abort.state() == io.rowweave.run.BatchSinkAbortRecord.State.ABORTED
                            ? io.rowweave.commit.BatchSinkOutcome.State.ABORTED
                            : io.rowweave.commit.BatchSinkOutcome.State.ABORT_DECIDED,
                    abort.generation(), abort.identityDigest(),
                    abort.state() == io.rowweave.run.BatchSinkAbortRecord.State.ABORTED
                            ? "MANIFEST_ABORT_COMPLETED"
                            : "MANIFEST_ABORT_DECIDED");
        }
        if (qualityBounded ? qualityJournal == null : journal == null) {
            return projectTerminalManifestWithoutJournal(snapshot, graph);
        }
        byte[] identity = qualityBounded
                ? sinkGroupIdentity(qualityJournal) : sinkGroupIdentity(journal);
        ManifestCommitIntent intent = qualityBounded
                ? qualityJournal.intent() : journal.intent();
        if (intent == null) {
            Long generation = qualityBounded
                    ? firstManifestGeneration(qualityJournal)
                    : firstManifestGeneration(journal);
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.PREPARING,
                    generation, identity,
                    snapshot.status().isTerminal()
                            ? "ABORT_DECISION_PENDING"
                            : "AWAITING_COMMIT_DECISION");
        }
        StageAssignmentSnapshot evidence =
                stageAssignmentAuthority.assignmentSnapshot(fence);
        if (evidence != null && hasProviderLocatorClosure(evidence)) {
            io.rowweave.commit.CommitManifest intended = qualityBounded
                    ? qualityJournal.intendedManifest() : journal.intendedManifest();
            io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
            if (!output.domainId().equals(intent.domainId())
                    || !attempt.publicationId().equals(intent.publicationId())) {
                throw new IllegalStateException(
                        "manifest outcome intent identity mismatch");
            }
            ManifestDurableSinkAuthorityAdapter adapter =
                    manifestAuthorityProjection(intent, intended);
            DurableSinkCommitCoordinator coordinator =
                    new DurableSinkCommitCoordinator(adapter,
                            (action, decisionDigest) ->
                                    executeRecoveredProviderGroupControl(
                                            graph, evidence,
                                            attempt.publicationId(),
                                            intent.generation(), action,
                                            decisionDigest,
                                            deadlineAfterMillis(
                                                    outputCommitTimeoutMs)));
            io.rowweave.commit.BatchSinkOutcome observed = coordinator.query();
            if (observed.state()
                    == io.rowweave.commit.BatchSinkOutcome.State.COMMITTED) {
                return io.rowweave.commit.BatchSinkOutcome.of(
                        observed.state(), observed.generation(),
                        identity == null ? intent.manifestHash() : identity,
                        "MANIFEST_VISIBLE_VERIFIED");
            }
            if (observed.state()
                    == io.rowweave.commit.BatchSinkOutcome.State.COMMIT_DECIDED) {
                return io.rowweave.commit.BatchSinkOutcome.of(
                        observed.state(), observed.generation(),
                        identity == null ? intent.manifestHash() : identity,
                        "MANIFEST_INTENT_DURABLE");
            }
            return observed;
        }
        if (containsProviderSinkOutput(graph)) {
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.COMMIT_DECIDED,
                    intent.generation(),
                    identity == null ? intent.manifestHash() : identity,
                    "MANIFEST_PROVIDER_LOCATOR_PENDING");
        }
        // Compatibility only: pre-R4 journals did not persist provider locators.
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        io.rowweave.commit.ManifestFileStore store =
                new io.rowweave.commit.ManifestFileStore(
                        java.nio.file.Paths.get(output.domainPath()));
        if (!store.domainId().equals(intent.domainId())
                || !attempt.publicationId().equals(intent.publicationId())) {
            throw new IllegalStateException("manifest outcome intent identity mismatch");
        }
        io.rowweave.commit.ManifestCommitOutcome observed =
                store.queryOutcome(intent.publicationId(), intent.generation());
        if (observed.state()
                == io.rowweave.commit.ManifestCommitOutcome.State.COMMITTED) {
            if (!java.security.MessageDigest.isEqual(
                    observed.manifestHash(), intent.manifestHash())) {
                throw new IllegalStateException("visible manifest conflicts with intent");
            }
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.COMMITTED,
                    intent.generation(), identity == null ? intent.manifestHash() : identity,
                    "MANIFEST_VISIBLE_VERIFIED");
        }
        if (observed.state()
                == io.rowweave.commit.ManifestCommitOutcome.State.PREPARING) {
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.COMMIT_DECIDED,
                    intent.generation(), identity == null ? intent.manifestHash() : identity,
                    "MANIFEST_INTENT_DURABLE");
        }
        throw new IllegalStateException("manifest outcome conflicts with durable intent");
    }

    private io.rowweave.commit.BatchSinkOutcome projectTerminalManifestWithoutJournal(
            RunSnapshot snapshot, PlanGraph graph) {
        io.rowweave.run.RunResultSummary summary = snapshot.resultSummary();
        if (snapshot.status() != io.rowweave.run.RunStatus.SUCCEEDED
                || summary == null
                || summary.publicationKind()
                != io.rowweave.run.RunResultSummary.PublicationKind.MANIFEST) {
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.PREPARING,
                    null, null, snapshot.status().isTerminal()
                            ? "ABORT_DECISION_PENDING" : "AWAITING_MANIFEST_JOURNAL");
        }
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        io.rowweave.commit.ManifestCommitOutcome observed =
                new io.rowweave.commit.ManifestFileStore(
                        java.nio.file.Paths.get(output.domainPath()))
                        .queryOutcome(snapshot.currentAttempt().publicationId(),
                                summary.manifestGeneration());
        if (observed.state()
                != io.rowweave.commit.ManifestCommitOutcome.State.COMMITTED
                || !java.security.MessageDigest.isEqual(
                observed.manifestHash(), summary.manifestSha256())) {
            throw new IllegalStateException("terminal manifest summary is not externally visible");
        }
        return io.rowweave.commit.BatchSinkOutcome.of(
                io.rowweave.commit.BatchSinkOutcome.State.COMMITTED,
                summary.manifestGeneration(),
                summary.sinkIdentityDigest() == null
                        ? summary.manifestSha256() : summary.sinkIdentityDigest(),
                "MANIFEST_VISIBLE_VERIFIED");
    }

    private static Long firstManifestGeneration(ManifestCommitJournalSnapshot journal) {
        for (int i = 0; i < journal.assignments().manifest().assignmentCount(); i++) {
            ManifestMemberReceipt receipt = journal.member(i);
            if (receipt != null) return receipt.generation();
        }
        return null;
    }

    private static Long firstManifestGeneration(
            io.rowweave.run.QualityGateCommitJournalSnapshot journal) {
        for (ManifestMemberReceipt receipt : journal.members()) {
            if (receipt != null) return receipt.generation();
        }
        return null;
    }

    private static byte[] sinkGroupIdentity(ManifestCommitJournalSnapshot journal) {
        try {
            java.security.MessageDigest digest =
                    java.security.MessageDigest.getInstance("SHA-256");
            boolean found = false;
            for (int i = 0; i < journal.assignments().manifest().assignmentCount(); i++) {
                ManifestMemberReceipt receipt = journal.member(i);
                if (receipt != null && receipt.hasPreparedLocator()) {
                    digest.update(receipt.preparedLocator()
                            .attemptIdentity().identityDigest());
                    found = true;
                }
            }
            return found ? digest.digest() : null;
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static byte[] sinkGroupIdentity(
            io.rowweave.run.QualityGateCommitJournalSnapshot journal) {
        try {
            java.security.MessageDigest digest =
                    java.security.MessageDigest.getInstance("SHA-256");
            boolean found = false;
            for (ManifestMemberReceipt receipt : journal.members()) {
                if (receipt != null && receipt.hasPreparedLocator()) {
                    digest.update(receipt.preparedLocator()
                            .attemptIdentity().identityDigest());
                    found = true;
                }
            }
            return found ? digest.digest() : null;
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private RpcMessage handleAssignmentAcceptance(RpcMessage request) {
        if (stageAssignmentAuthority == null) {
            return RpcMessage.jobResult(false, 0,
                    "assignment acceptance unavailable: authority capability missing");
        }
        final StageAssignmentToken token;
        byte[] digest = request.stageCommandDigest();
        try {
            token = StageAssignmentManifestCodec.decodeToken(
                    request.stageAssignmentTokenBytes());
            if (digest == null || digest.length != 32) {
                throw new IllegalArgumentException("command digest must be SHA-256");
            }
        } catch (RuntimeException invalid) {
            return RpcMessage.jobResult(false, 0,
                    "assignment acceptance rejected: invalid envelope");
        }
        StageOwner owner = stageWorker.get(request.stageId());
        if (owner == null || owner.assignmentToken == null
                || !owner.assignmentToken.equals(token)
                || !owner.workerId.equals(request.workerId())
                || !owner.endpoint.incarnationId().equals(request.incarnationId())
                || !owner.runAttemptFence.equals(request.runAttemptFence())
                || !java.security.MessageDigest.isEqual(owner.commandDigest, digest)) {
            return RpcMessage.jobResult(false, 0,
                    "assignment acceptance rejected: assignment identity mismatch");
        }
        try {
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    token, digest, System.currentTimeMillis());
            StageAcceptanceDecision decision;
            if (owner.frontierToken != null) {
                decision = incrementalAuthorityGateway.acceptAssignment(
                        owner.frontierToken, owner.recoveryToken, acceptance);
            } else if (owner.recoveryToken == null) {
                decision = stageAssignmentAuthority.accept(acceptance);
            } else if (stageAssignmentAuthority
                    instanceof RecoveryFencedStageAssignmentAuthority) {
                decision = ((RecoveryFencedStageAssignmentAuthority)
                        stageAssignmentAuthority).acceptRecovered(
                        owner.recoveryToken, acceptance);
            } else {
                return RpcMessage.jobResult(false, 0,
                        "assignment acceptance unavailable: recovery-fenced authority missing");
            }
            if (decision.code() == StageAcceptanceDecision.Code.ACCEPTED
                    || decision.code() == StageAcceptanceDecision.Code.ALREADY_ACCEPTED) {
                return RpcMessage.ack();
            }
            if (decision.code() == StageAcceptanceDecision.Code.UNAVAILABLE) {
                return RpcMessage.jobResult(false, 0,
                        "assignment acceptance unavailable: " + decision.code());
            }
            return RpcMessage.jobResult(false, 0,
                    "assignment acceptance rejected: " + decision.code());
        } catch (RuntimeException unavailable) {
            return RpcMessage.jobResult(false, 0,
                    "assignment acceptance unavailable: " + unavailable.getMessage());
        }
    }

    /**
     * Create the run and its reference inside the artifact publication guard once preflight has passed, then create the attempt and claim
     * it for dispatch.
     */
    private RpcMessage handleDurableSubmit(RpcMessage req) {
        if (runStateStore == null) {
            return RpcMessage.durableJobResult(false, 0, "run state authority unavailable",
                    io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE);
        }
        RunSubmission submission = null;
        java.util.concurrent.atomic.AtomicBoolean acceptanceMayHaveStarted =
                new java.util.concurrent.atomic.AtomicBoolean();
        try {
            deploymentRuntime.requireAdmissionOpen();
            requireRecoveryInventoryReady();
            RunSubmissionCodec.Decoded decoded = RunSubmissionCodec.decode(
                    req.planGraphBytes(), req::reserveResidentBytes);
            RunSubmissionPolicyValidator.requireSupported(decoded.submission());
            if (decoded.submission().deadlineEpochMillis() > 0L
                    && runDeadlineAuthority == null) {
                throw new IllegalStateException(
                        "run deadline authority capability unavailable");
            }
            byte[] artifactBytes = decoded.artifactBytes();
            PlanArtifact artifact = PlanArtifactCodec.decode(
                    artifactBytes, req::reserveResidentBytes);
            requireArtifactDeployment(artifact);
            boolean portable = artifact.payloadFormat()
                    == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1;
            PlanGraph graph;
            java.util.List<String> requiredWorkerCapabilities =
                    java.util.Collections.emptyList();
            if (portable) {
                PortableBatchPlan plan = PortablePlanArtifact
                        .preflightDurableValidated(
                                artifact,
                                deploymentRuntime.portableBindingVerifier(),
                                req::reserveResidentBytes);
                graph = PortablePlanCompiler.projectMetadata(
                        plan, deploymentRuntime.portableBindingVerifier());
                requiredWorkerCapabilities = plan.requiredCapabilities();
            } else {
                PlanArtifactValidator.validateForCurrentRuntime(artifact);
                io.rowweave.plan.PlanGraphEnvelopeScanner.Scan providerScan =
                        io.rowweave.plan.PlanGraphEnvelopeScanner.scanDurable(
                                artifact.payload(), req::reserveResidentBytes);
                io.rowweave.plan.DurableSinkProviderPlanPreflight.validate(
                        providerScan,
                        deploymentRuntime.durableSinkProviderRegistry());
                graph = LegacyPlanArtifactAdapter.unwrapValidated(
                        artifact, objectTrustContext,
                        req::reserveResidentBytes)
                        .restoreBuiltinManifestOutputs();
            }
            OutputDestinationValidator.validate(graph);
            ClusterOutputValidator.validate(graph);
            if (!portable) {
                io.rowweave.plan.OutputCommitValidator.validateDurable(graph);
            }
            ClusterPlanCapabilityValidator.validate(graph);
            if (!portable) {
                io.rowweave.plan.BatchSourcePlanValidator.validateDurable(
                        graph, objectTrustContext);
            }
            ExecutionGraph capacityGraph = portable
                    ? (requiresPortableStageLifecycle(
                            requiredWorkerCapabilities)
                    ? ExecutionGraphBuilder.buildWithSinkBoundary(graph)
                    : portableWholeExecutionGraph(graph))
                    : ExecutionGraphBuilder.buildWithSinkBoundary(graph);
            DurableStageAssignmentPlan.requireCapacity(StageAssignment.assign(
                    capacityGraph, graph.parallelism(),
                    java.util.Collections.singletonList("capacity-only")));
            if (stageAssignmentAuthority == null) {
                throw new IllegalStateException(
                        "stage assignment authority capability unavailable");
            }
            if (decoded.submission().maxAttempts() > 1
                    && wholeAttemptRetryAuthority == null) {
                throw new IllegalStateException(
                        "whole-attempt retry authority capability unavailable: "
                                + "run and assignment authority must be the same instance");
            }
            if (artifactLifecycleRepository == null
                    || artifactReferenceAuthority == null) {
                throw new IllegalStateException(
                        "artifact lifecycle capability unavailable: repository, "
                                + "reference authority, and run state authority must match");
            }
            submission = decoded.submission();
            String seed = sha256Hex(req.planGraphBytes());
            RunSubmission guardedSubmission = submission;
            RunSnapshot created = artifactLifecycleRepository.publishReferenced(
                    artifactBytes, req::reserveResidentBytes, storedArtifactId -> {
                        if (!artifact.artifactId().equals(storedArtifactId)) {
                            throw new IllegalStateException(
                                    "artifact repository identity mismatch: expected="
                                            + artifact.artifactId() + " actual="
                                            + storedArtifactId);
                        }
                        acceptanceMayHaveStarted.set(true);
                        return createDurableRun(
                                guardedSubmission, artifact, seed);
                    });
            DurableRunExecution execution = claimDurableDispatch(
                    submission, created, seed);
            if (execution == null) {
                return durableResultFromAuthority(submission.runId(),
                        "run already accepted; authoritative outcome follows", req);
            }
            if (!locallyExecutingDurableRuns.add(submission.runId())) {
                throw new IllegalStateException(
                        "durable run already executes on this Master");
            }
            try {
                execution.takeResidentLease(req.residentLease(),
                        req.transferResidentOwner(
                                io.rowweave.cluster.rpc.ControlResidentOwner.RUN_OWNED));
                RpcMessage result = executeSubmittedGraph(
                        graph, submission.publicationId(), execution, req, null,
                        portable ? artifactBytes : null,
                        requiredWorkerCapabilities);
                return durableResultFromAuthority(
                        submission.runId(), result.errorMessage(), req);
            } finally {
                execution.releaseResidentLease();
                locallyExecutingDurableRuns.remove(submission.runId());
                if (recoveryInventory != null) recoveryInventory.signal();
            }
        } catch (DurableRunAcceptanceRejectedException rejected) {
            return RpcMessage.durableJobResult(false, 0,
                    "submit failed: " + rejected.getMessage(),
                    io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE);
        } catch (io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted) {
            return !acceptanceMayHaveStarted.get() || submission == null
                    ? RpcMessage.durableJobResult(false, 0, exhausted.getMessage(),
                    io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE)
                    : durableResultAfterPublicationFailure(
                            submission.runId(), exhausted.getMessage(), req);
        } catch (Exception failure) {
            String detail = "submit failed: " + failure.getMessage();
            return !acceptanceMayHaveStarted.get() || submission == null
                    ? RpcMessage.durableJobResult(false, 0, detail,
                    io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE)
                    : durableResultAfterPublicationFailure(submission.runId(), detail, req);
        }
    }

    private RpcMessage handleIncrementalSubmit(RpcMessage request) {
        if (incrementalAuthorityGateway == null
                || artifactLifecycleRepository == null) {
            return RpcMessage.jobResult(false, 0,
                    "incremental authority unavailable");
        }
        io.rowweave.incremental.IncrementalRunSubmission submission = null;
        final String[] reservedRunId = {null};
        try {
            submission = io.rowweave.incremental.IncrementalRunSubmissionCodec
                    .decode(request.incrementalSubmissionBytes(),
                            request::reserveResidentBytes);
            long actionDeadline = deadlineAfterMillis(
                    submission.sourceContract().actionBudget()
                            .actionTimeoutMillis());
            io.rowweave.incremental.IncrementalIntervalPreparationDecision prepared =
                    prepareIncrementalSubmissionControl(
                            submission, actionDeadline,
                            request::reserveResidentBytes, runId -> {
                                if (!locallyExecutingDurableRuns.add(runId)) {
                                    throw new IllegalStateException(
                                            "incremental run already executes on this Master");
                                }
                                reservedRunId[0] = runId;
                            });
            if (prepared.code()
                    != io.rowweave.incremental.IncrementalIntervalPreparationDecision
                    .Code.CLAIMED) {
                return RpcMessage.jobResult(true, 0, null);
            }
            IncrementalExecutionClaim claim =
                    claimIncrementalExecutionControl(
                            submission, prepared,
                            request::reserveResidentBytes);
            DurableRunExecution execution = new DurableRunExecution(claim);
            if (!execution.runId.equals(reservedRunId[0])) {
                throw new IllegalStateException(
                        "incremental run reservation differs before execution");
            }
            try {
                execution.takeResidentLease(request.residentLease(),
                        request.transferResidentOwner(
                                io.rowweave.cluster.rpc.ControlResidentOwner.RUN_OWNED));
                DurableRunExecution attempt = execution;
                while (true) {
                    RpcMessage result = executeSubmittedGraph(
                            claim.graph(), attempt.publicationId,
                            attempt, request, null);
                    if (result.success()) return result;
                    DurableRunExecution retry = claimIncrementalRetry(
                            attempt, claim.graph(), actionDeadline);
                    if (retry == null) return result;
                    attempt = retry;
                }
            } finally {
                execution.releaseResidentLease();
            }
        } catch (Exception failure) {
            return RpcMessage.jobResult(false, 0,
                    "incremental submit failed: " + failure.getMessage());
        } finally {
            if (reservedRunId[0] != null) {
                locallyExecutingDurableRuns.remove(reservedRunId[0]);
                if (recoveryInventory != null) recoveryInventory.signal();
            }
        }
    }

    private DurableRunExecution claimIncrementalRetry(
            DurableRunExecution execution, PlanGraph graph,
            long actionDeadlineNanos) {
        RunSnapshot observed = incrementalAuthorityGateway.runSnapshot(
                execution.runId);
        AttemptSnapshot observedAttempt = observed == null
                ? null : observed.currentAttempt();
        if (observed == null || observedAttempt == null) {
            throw new IllegalStateException(
                    "incremental retry Run authority disappeared");
        }
        if (!execution.attemptId.equals(observedAttempt.attemptId())
                || execution.fenceToken != observedAttempt.fenceToken()) {
            throw new IllegalStateException(
                    "incremental retry attempt identity changed");
        }
        execution.snapshot = observed;
        AttemptStatus observedStatus = observedAttempt.status();
        if (observed.status().isTerminal()
                || observedStatus == AttemptStatus.CANCELING
                || observedStatus == AttemptStatus.COMMITTING
                || observedStatus == AttemptStatus.RECONCILING
                || observedStatus.isTerminal()) {
            return null;
        }
        StageAssignmentSnapshot evidence = execution.assignmentSnapshot();
        if (evidence != null) {
            evidence = reconcileIncrementalAssignmentsUntilStable(
                    execution, evidence, actionDeadlineNanos);
            completeIncrementalRetryAbort(
                    execution, graph, evidence, actionDeadlineNanos);
        }
        WholeAttemptRetryDecision scheduled = incrementalAuthorityGateway
                .scheduleRetry(execution.frontierToken,
                        execution.recoveryToken, execution.fence());
        retryRecoveryMetrics.record(scheduled);
        if (scheduled.snapshot() != null) {
            execution.snapshot = scheduled.snapshot();
        }
        switch (scheduled.code()) {
            case SCHEDULED:
            case ALREADY_SCHEDULED:
                break;
            case NOT_RETRYABLE:
                execution.failFromTerminalEvidence();
                return null;
            case EXHAUSTED:
            case TOO_LATE:
            case RETIRING:
                return null;
            case CLOSURE_PENDING:
                throw new IllegalStateException(
                        "incremental retry closure is incomplete");
            case STALE_RUN:
            case STALE_RECOVERY_OWNER:
                throw new IllegalStateException(
                        "incremental retry ownership changed: "
                                + scheduled.code());
            case NOT_FOUND:
            case UNAVAILABLE:
            default:
                throw new IllegalStateException(
                        "incremental retry authority unavailable: "
                                + scheduled.code());
        }

        waitUntilIncrementalRetryDue(execution, actionDeadlineNanos);
        RetryAttemptClaim claimed = incrementalAuthorityGateway.claimDueRetry(
                execution.frontierToken, execution.recoveryToken,
                execution.runId);
        if (claimed.code() != RetryAttemptClaim.Code.CLAIMED
                && claimed.code() != RetryAttemptClaim.Code.ALREADY_CLAIMED) {
            throw new IllegalStateException(
                    "incremental retry claim rejected: " + claimed.code());
        }
        io.rowweave.incremental.IncrementalAuthorityRecord record =
                incrementalAuthorityGateway.record(
                        execution.incrementalRequest.feedId());
        io.rowweave.incremental.IncrementalActiveInterval active =
                record == null ? null : record.activeInterval();
        RunSnapshot snapshot = incrementalAuthorityGateway.runSnapshot(
                execution.runId);
        io.rowweave.run.RetryAttemptIdentity identity = claimed.identity();
        AttemptSnapshot attempt = snapshot == null
                ? null : snapshot.currentAttempt();
        if (record == null || active == null || snapshot == null
                || identity == null || attempt == null
                || !execution.incrementalRequest.intervalId().equals(
                        active.identity().intervalId())
                || !execution.incrementalRequest.artifactId().equals(
                        active.artifactId())
                || !execution.incrementalRequest.capturedTarget().equals(
                        active.capturedTarget())
                || !identity.attemptId().equals(attempt.attemptId())
                || !identity.publicationId().equals(attempt.publicationId())
                || identity.fenceToken() != attempt.fenceToken()
                || !identity.attemptId().equals(active.fence().attemptId())
                || identity.fenceToken() != active.fence().fenceToken()) {
            throw new IllegalStateException(
                    "incremental retry changed the frozen interval");
        }
        return new DurableRunExecution(
                record, active, execution.frontierToken,
                execution.recoveryToken, snapshot);
    }

    private void completeIncrementalRetryAbort(
            DurableRunExecution execution, PlanGraph graph,
            StageAssignmentSnapshot evidence, long actionDeadlineNanos) {
        io.rowweave.run.BatchSinkAbortRecord abort =
                execution.batchSinkAbort();
        if (abort == null
                || abort.state()
                == io.rowweave.run.BatchSinkAbortRecord.State.ABORTED) {
            return;
        }
        ManifestCommitJournalSnapshot journal =
                execution.manifestCommitJournal();
        if (journal == null
                || !journal.assignments().manifest().fence().equals(
                        evidence.manifest().fence())
                || journal.intent() != null) {
            throw new IllegalStateException(
                    "incremental retry manifest journal is not abortable");
        }
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        io.rowweave.commit.ManifestFileStore store =
                new io.rowweave.commit.ManifestFileStore(
                        java.nio.file.Paths.get(output.domainPath()));
        if (!store.domainId().equals(abort.commitDomainId())
                || abort.assignmentGeneration()
                != evidence.manifest().generation()
                || !abort.publicationId().equals(execution.publicationId)
                || store.exact(abort.generation()) != null) {
            throw new IllegalStateException(
                    "incremental retry abort identity is unsafe");
        }
        MasterElection election = leadership;
        if (election == null || election.termToken() == 0L
                || !incrementalAuthorityGateway.currentlyOwns(
                        execution.frontierToken, execution.recoveryToken)) {
            throw reconciliationUnavailable(evidence,
                    "incremental retry abort ownership unavailable", null);
        }
        long rollbackDeadline = Math.min(actionDeadlineNanos,
                deadlineAfterMillis(outputCommitTimeoutMs));
        String groupId = "manifest:" + execution.publicationId;
        for (int index = evidence.manifest().assignmentCount() - 1;
             index >= 0; index--) {
            io.rowweave.run.StageAssignmentRecord assignment =
                    evidence.manifest().assignment(index);
            StageAssignmentAcceptance acceptance = evidence.acceptance(index);
            if (!isManifestOutputRole(assignment.role())
                    || acceptance == null) {
                continue;
            }
            RegistryWorker worker = endpointOf(assignment.workerId());
            if (worker == null
                    || !assignment.host().equals(worker.host())
                    || assignment.rpcPort() != worker.port()
                    || !assignment.workerIncarnationId().equals(
                            worker.incarnationId())) {
                continue;
            }
            StageOwner owner = new StageOwner(
                    assignment.workerId(), worker, execution.fence(),
                    acceptance.token(), acceptance.commandDigest(),
                    execution.recoveryToken, execution.frontierToken);
            RpcMessage rollback = RpcMessage.rollbackPreparedOutput(
                    assignment.workerId(), assignment.runtimeStageId(),
                    groupId, election.self(), election.termToken(),
                    assignment.workerIncarnationId(), execution.fence());
            try {
                callManifestControl(owner, assignment.runtimeStageId(),
                        rollback, RpcMessage.Type.ACK, rollbackDeadline,
                        response -> null);
            } catch (RuntimeException unavailable) {
                throw reconciliationUnavailable(evidence,
                        "incremental retry output rollback failed", unavailable);
            }
        }
        io.rowweave.run.BatchSinkAbortDecision completed =
                execution.recordBatchSinkAbort(
                        abort.completed(abort.decidedAtMillis()));
        if (completed.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code.RECORDED
                && completed.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code
                .ALREADY_RECORDED) {
            throw new IllegalStateException(
                    "incremental retry abort completion rejected: "
                            + completed.code());
        }
    }

    private StageAssignmentSnapshot reconcileIncrementalAssignmentsUntilStable(
            DurableRunExecution execution, StageAssignmentSnapshot evidence,
            long actionDeadlineNanos) {
        while (true) {
            try {
                return reconcileIncrementalAssignments(execution, evidence);
            } catch (io.rowweave.run.RunStateStoreUnavailableException unavailable) {
                if (actionDeadlineNanos - System.nanoTime() <= 0L) {
                    throw unavailable;
                }
                StageAssignmentSnapshot refreshed =
                        execution.assignmentSnapshot();
                if (refreshed != null) evidence = refreshed;
                try {
                    Thread.sleep(25L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    unavailable.addSuppressed(interrupted);
                    throw unavailable;
                }
            }
        }
    }

    private void waitUntilIncrementalRetryDue(
            DurableRunExecution execution, long actionDeadlineNanos) {
        while (true) {
            io.rowweave.run.RetryReadyEvaluation readiness =
                    incrementalAuthorityGateway.evaluateRetryReady(
                            execution.frontierToken, execution.recoveryToken,
                            execution.runId);
            if (readiness.code()
                    == io.rowweave.run.RetryReadyEvaluation.Code.DUE) {
                return;
            }
            if (readiness.code()
                    != io.rowweave.run.RetryReadyEvaluation.Code.NOT_DUE) {
                throw new IllegalStateException(
                        "incremental retry readiness rejected: "
                                + readiness.code());
            }
            long remainingNanos = actionDeadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new IllegalStateException(
                        "incremental retry exceeded its action deadline");
            }
            long waitMillis = Math.max(1L, Math.min(50L,
                    readiness.notBeforeMillis()
                            - readiness.observedAtMillis()));
            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "incremental retry wait was interrupted", interrupted);
            }
        }
    }

    private RpcMessage durableResultAfterPublicationFailure(
            String runId, String detail, RpcMessage request) {
        io.rowweave.run.DurableExecutionEvidenceView view = null;
        final RunSnapshot snapshot;
        if (!durableExecutionEvidenceProviderPresent) {
            try {
                snapshot = runStateStore.get(runId);
            } catch (RuntimeException authorityUnavailable) {
                return RpcMessage.durableJobResult(false, 0, detail,
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
        } else {
            try {
                view = requireDurableExecutionEvidenceAuthority().executionEvidence(runId);
                if (view == null) {
                    throw new IllegalStateException(
                            "coherent execution evidence authority returned null");
                }
                snapshot = view.snapshot();
            } catch (RuntimeException authorityUnavailable) {
                return RpcMessage.durableJobResult(false, 0,
                        durableAuthorityFailureDetail(detail, authorityUnavailable),
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
        }
        if (snapshot == null) {
            return RpcMessage.durableJobResult(false, 0, detail,
                    io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE);
        }
        return durableResult(snapshot, detail, view, request);
    }

    private RpcMessage durableResultFromAuthority(
            String runId, String detail, RpcMessage request) {
        io.rowweave.run.DurableExecutionEvidenceView view = null;
        final RunSnapshot snapshot;
        if (!durableExecutionEvidenceProviderPresent) {
            try {
                snapshot = runStateStore.get(runId);
            } catch (RuntimeException authorityUnavailable) {
                return RpcMessage.durableJobResult(false, 0,
                        detail == null ? "run authority unavailable" : detail,
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
        } else {
            try {
                view = requireDurableExecutionEvidenceAuthority().executionEvidence(runId);
                if (view == null) {
                    throw new IllegalStateException(
                            "coherent execution evidence authority returned null");
                }
                snapshot = view.snapshot();
            } catch (RuntimeException authorityUnavailable) {
                return RpcMessage.durableJobResult(false, 0,
                        durableAuthorityFailureDetail(detail, authorityUnavailable),
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
        }
        if (snapshot == null) {
            return RpcMessage.durableJobResult(false, 0,
                    detail == null ? "accepted run outcome is unknown" : detail,
                    io.rowweave.run.DurableRunDisposition.UNKNOWN);
        }
        return durableResult(snapshot, detail, view, request);
    }

    private static String durableAuthorityFailureDetail(
            String detail, RuntimeException failure) {
        String message = failure.getMessage();
        String authorityDetail = message == null || message.isEmpty()
                ? "run evidence authority unavailable"
                : "run evidence authority unavailable: " + message;
        return detail == null || detail.isEmpty()
                ? authorityDetail : detail + "; " + authorityDetail;
    }

    private io.rowweave.run.DurableExecutionEvidenceAuthority
    requireDurableExecutionEvidenceAuthority() {
        if (durableExecutionEvidenceAuthority == null) {
            throw new IllegalStateException(
                    "coherent execution evidence authority provider returned null");
        }
        return durableExecutionEvidenceAuthority;
    }

    private RpcMessage durableResult(
            RunSnapshot snapshot, String detail,
            io.rowweave.run.DurableExecutionEvidenceView evidence,
            RpcMessage request) {
        if (snapshot == null) {
            return RpcMessage.durableJobResult(false, 0,
                    detail == null ? "accepted run outcome is unknown" : detail,
                    io.rowweave.run.DurableRunDisposition.UNKNOWN);
        }
        if (snapshot.status() == io.rowweave.run.RunStatus.SUCCEEDED) {
            io.rowweave.run.RunResultSummary summary = snapshot.resultSummary();
            if (summary == null) {
                return RpcMessage.durableJobResult(false, 0,
                        "succeeded run is missing result summary",
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
            try {
                if (request != null) {
                    request.reserveResponseResidentBytes(
                            io.rowweave.plan.ExecutionTelemetrySummaryCodec
                                    .MAX_MATERIALIZED_RESIDENT_BYTES);
                }
                return RpcMessage.durableJobResultWithTelemetrySummary(
                        true, summary.rowsOut(), null,
                        io.rowweave.run.DurableRunDisposition.SUCCEEDED,
                        DurableExecutionTelemetryResolver.resolve(evidence));
            } catch (RuntimeException | LinkageError unavailableTelemetry) {
                return RpcMessage.durableJobResult(true, summary.rowsOut(), null,
                        io.rowweave.run.DurableRunDisposition.SUCCEEDED);
            }
        }
        if (snapshot.status() == io.rowweave.run.RunStatus.FAILED
                || snapshot.status() == io.rowweave.run.RunStatus.CANCELED) {
            return RpcMessage.durableJobResult(false, 0,
                    detail == null ? "run reached terminal failure" : detail,
                    io.rowweave.run.DurableRunDisposition.TERMINAL_FAILED);
        }
        return RpcMessage.durableJobResult(false, 0,
                detail == null ? "run remains non-terminal" : detail,
                io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL);
    }

    private RunSnapshot createDurableRun(
            RunSubmission submission, PlanArtifact artifact, String seed) {
        RunDecision created = runStateStore.apply(RunCommand.createRun(
                durableCommandId(seed, "create-run"), submission.runId(),
                artifact.definitionId(), artifact.artifactId(), submission.maxAttempts(),
                submission.deadlineEpochMillis(), submission.submittedAtMillis()));
        if (created.code() != RunDecision.Code.ACCEPTED
                && created.code() != RunDecision.Code.ALREADY_APPLIED) {
            throw new DurableRunAcceptanceRejectedException(
                    "create run rejected: " + created.code());
        }
        RunSnapshot run = created.snapshot();
        boolean identityMismatch = !artifact.definitionId().equals(run.definitionId())
                || !artifact.artifactId().equals(run.artifactId())
                || submission.maxAttempts() != run.maxAttempts()
                || submission.deadlineEpochMillis() != run.deadlineEpochMillis()
                || submission.submittedAtMillis() != run.createdAtMillis();
        if (identityMismatch && created.code() == RunDecision.Code.ALREADY_APPLIED) {
            throw new DurableRunAcceptanceRejectedException(
                    "run identity conflicts with durable snapshot");
        }
        if (identityMismatch) {
            throw new IllegalStateException("run identity conflicts with durable snapshot");
        }
        return run;
    }

    private DurableRunExecution claimDurableDispatch(
            RunSubmission submission, RunSnapshot run, String seed) {
        if (submission.deadlineEpochMillis() > 0L
                && deadlineStopsExecution(evaluateRunDeadline(submission.runId()))) {
            return null;
        }
        RunDecision attempted = runStateStore.apply(RunCommand.createAttempt(
                durableCommandId(seed, "create-attempt"), submission.runId(), 1L,
                submission.attemptId(), submission.publicationId(),
                submission.submittedAtMillis()));
        if (attempted.code() != RunDecision.Code.ACCEPTED
                && attempted.code() != RunDecision.Code.ALREADY_APPLIED) {
            if (completeUnassignedUserCancellation(submission, seed)) {
                return null;
            }
            RunSnapshot observed = runStateStore.get(submission.runId());
            if (observed != null && observed.status().isTerminal()
                    && observed.currentAttempt() == null) {
                return null;
            }
            throw new IllegalStateException("create attempt rejected: " + attempted.code());
        }
        AttemptSnapshot attempt = attempted.snapshot().currentAttempt();
        if (attempt == null || !submission.attemptId().equals(attempt.attemptId())
                || !submission.publicationId().equals(attempt.publicationId())) {
            throw new IllegalStateException("attempt identity conflicts with durable snapshot");
        }

        if (submission.deadlineEpochMillis() > 0L
                && deadlineStopsExecution(evaluateRunDeadline(submission.runId()))) {
            completeUnassignedTimeout(submission, seed);
            return null;
        }

        RunDecision dispatch = runStateStore.apply(RunCommand.advanceAttempt(
                durableCommandId(seed, "dispatch"), submission.runId(), 2L,
                submission.attemptId(), attempt.fenceToken(), AttemptStatus.DISPATCHING,
                false, submission.submittedAtMillis(), null, null));
        if (dispatch.code() == RunDecision.Code.ALREADY_APPLIED) {
            completeUnassignedUserCancellation(submission, seed);
            return null; // receipt replay does not prove this handler owns execution.
        }
        if (dispatch.code() != RunDecision.Code.ACCEPTED) {
            if (completeUnassignedUserCancellation(submission, seed)) {
                return null;
            }
            if (completeUnassignedTimeout(submission, seed)) {
                return null;
            }
            throw new IllegalStateException("dispatch claim rejected: " + dispatch.code());
        }
        return new DurableRunExecution(submission, seed, dispatch.snapshot());
    }

    private boolean completeUnassignedUserCancellation(
            RunSubmission submission, String seed) {
        RunSnapshot observed = runStateStore.get(submission.runId());
        AttemptSnapshot attempt = observed == null ? null : observed.currentAttempt();
        if (attempt == null || !submission.attemptId().equals(attempt.attemptId())
                || !submission.publicationId().equals(attempt.publicationId())
                || attempt.status() != AttemptStatus.CANCELING
                || attempt.cancellationCause() != CancellationCause.USER) {
            return false;
        }
        RunAttemptFence fence = new RunAttemptFence(
                observed.runId(), attempt.attemptId(), attempt.fenceToken());
        StageAssignmentSnapshot assignments =
                stageAssignmentAuthority.assignmentSnapshot(fence);
        if (assignments != null && !assignments.allNone()) {
            return false;
        }
        return new DurableRunExecution(submission, seed, observed)
                .completeUserCancellationIfProven();
    }

    private boolean completeUnassignedTimeout(
            RunSubmission submission, String seed) {
        RunSnapshot observed = runStateStore.get(submission.runId());
        AttemptSnapshot attempt = observed == null ? null : observed.currentAttempt();
        if (attempt == null || !submission.attemptId().equals(attempt.attemptId())
                || !submission.publicationId().equals(attempt.publicationId())
                || attempt.status() != AttemptStatus.CANCELING
                || attempt.cancellationCause() != CancellationCause.TIMEOUT) {
            return false;
        }
        RunAttemptFence fence = new RunAttemptFence(
                observed.runId(), attempt.attemptId(), attempt.fenceToken());
        StageAssignmentSnapshot assignments =
                stageAssignmentAuthority.assignmentSnapshot(fence);
        if (assignments != null && !assignments.allNone()) return false;
        return new DurableRunExecution(submission, seed, observed)
                .completeTimeoutIfProven();
    }

    /** Recovers only an attempt in READY or CREATED that the authority can show had not been dispatched. */
    private InheritedRunRecovery.Result recoverInheritedRun(
            InheritedRunRecovery.Candidate candidate) {
        if (!isActiveLeader()) {
            return InheritedRunRecovery.Result.retry("LEADERSHIP_LOST");
        }
        RunSnapshot snapshot = candidate.snapshot();
        io.rowweave.incremental.IncrementalAuthorityRecord active =
                incrementalAuthorityGateway == null ? null
                        : incrementalAuthorityGateway.activeRecordForRun(
                                snapshot.runId());
        PlanArtifact artifact = candidate.artifact();
        requireArtifactDeployment(artifact);
        boolean portable = active == null && artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1;
        PlanGraph graph;
        java.util.List<String> requiredWorkerCapabilities =
                java.util.Collections.emptyList();
        if (portable) {
            PortableBatchPlan plan = PortablePlanArtifact
                    .preflightDurableValidated(
                            artifact,
                            deploymentRuntime.portableBindingVerifier(),
                            candidate::reserveResident);
            graph = PortablePlanCompiler.projectMetadata(
                    plan, deploymentRuntime.portableBindingVerifier());
            requiredWorkerCapabilities = plan.requiredCapabilities();
        } else {
            io.rowweave.plan.PlanGraphEnvelopeScanner.Scan providerScan =
                    io.rowweave.plan.PlanGraphEnvelopeScanner.scanDurable(
                            artifact.payload(), candidate::reserveResident);
            io.rowweave.plan.DurableSinkProviderPlanPreflight.validate(
                    providerScan,
                    deploymentRuntime.durableSinkProviderRegistry());
            graph = (active == null
                    ? LegacyPlanArtifactAdapter.unwrapValidated(
                            artifact, objectTrustContext,
                            candidate::reserveResident)
                    : LegacyPlanArtifactAdapter.unwrapIncrementalValidated(
                            artifact, objectTrustContext,
                            candidate::reserveResident))
                    .restoreBuiltinManifestOutputs();
        }
        OutputDestinationValidator.validate(graph);
        ClusterOutputValidator.validate(graph);
        if (!portable) {
            io.rowweave.plan.OutputCommitValidator.validateDurable(graph);
        }
        ClusterPlanCapabilityValidator.validate(graph);

        if (active != null) {
            return recoverIncrementalRun(candidate, graph, active);
        }
        if (snapshot.deadlineEpochMillis() > 0L) {
            RunDeadlineResult deadline = evaluateRunDeadline(snapshot.runId());
            if (deadline.disposition() == RunDeadlineResult.Disposition.NOT_FOUND) {
                return InheritedRunRecovery.Result.retry("DEADLINE_AUTHORITY_MISSING");
            }
            if (deadline.disposition() == RunDeadlineResult.Disposition.RETIRING
                    || deadline.disposition()
                    == RunDeadlineResult.Disposition.ALREADY_TERMINAL) {
                return InheritedRunRecovery.Result.resolved();
            }
            snapshot = runStateStore.get(snapshot.runId());
            if (snapshot == null) {
                return InheritedRunRecovery.Result.retry("DEADLINE_AUTHORITY_MISSING");
            }
            if (snapshot.status().isTerminal()) {
                return InheritedRunRecovery.Result.resolved();
            }
        }
        InheritedRunRecovery.Eligibility eligibility =
                InheritedRunRecovery.eligibility(snapshot);
        if (eligibility == InheritedRunRecovery.Eligibility.RECONCILE_CANCELLATION) {
            return snapshot.currentAttempt().cancellationCause()
                    == CancellationCause.TIMEOUT
                    ? reconcileInheritedTimeout(candidate, graph)
                    : reconcileInheritedUserCancellation(candidate, graph);
        }
        if (eligibility == InheritedRunRecovery.Eligibility.RECONCILE_IN_FLIGHT) {
            try {
                return reconcileInheritedInFlight(candidate, graph);
            } catch (UserCancellationWonRecoveryRace won) {
                return reconcileInheritedUserCancellation(candidate, graph);
            } catch (TimeoutWonRecoveryRace won) {
                return reconcileInheritedTimeout(candidate, graph);
            }
        }
        // Legacy in-flight evidence remains reconciliation-only. Executable READY/CREATED
        // recovery must pass the same exact-source admission as a new durable submit before
        // worker lookup or any attempt/create/dispatch mutation.
        if (!portable) {
            io.rowweave.plan.BatchSourcePlanValidator.validateDurable(
                    graph, objectTrustContext);
        }
        // Done before any create or dispatch mutation, so retrying an inventory read cannot consume the
        // attempt's budget.
        if (schedulableWorkers(requiredWorkerCapabilities(
                graph, requiredWorkerCapabilities)).isEmpty()) {
            return InheritedRunRecovery.Result.retry("NO_SCHEDULABLE_WORKER");
        }
        InheritedRunRecovery.Identity identity;
        if (eligibility == InheritedRunRecovery.Eligibility.EXECUTE_READY) {
            if (snapshot.attemptCount() > 0) {
                if (wholeAttemptRetryAuthority == null) {
                    return InheritedRunRecovery.Result.retry(
                            "RETRY_AUTHORITY_UNAVAILABLE");
                }
                RetryAttemptClaim claimed = wholeAttemptRetryAuthority
                        .claimDueRetryRecovered(candidate.token(), snapshot.runId());
                if (claimed.code() == RetryAttemptClaim.Code.NOT_DUE) {
                    return InheritedRunRecovery.Result.retry("RETRY_NOT_DUE");
                }
                if (claimed.code() == RetryAttemptClaim.Code.STALE_RECOVERY_OWNER) {
                    return InheritedRunRecovery.Result.retry("LOST_OWNERSHIP");
                }
                if (claimed.code() == RetryAttemptClaim.Code.NOT_FOUND
                        || claimed.code() == RetryAttemptClaim.Code.UNAVAILABLE) {
                    return InheritedRunRecovery.Result.retry(
                            "RETRY_AUTHORITY_UNAVAILABLE");
                }
                if (claimed.code() == RetryAttemptClaim.Code.TOO_LATE
                        || claimed.code() == RetryAttemptClaim.Code.RETIRING) {
                    return InheritedRunRecovery.Result.resolved();
                }
                if (claimed.code() != RetryAttemptClaim.Code.CLAIMED
                        && claimed.code()
                        != RetryAttemptClaim.Code.ALREADY_CLAIMED) {
                    return InheritedRunRecovery.Result.retry(
                            "RETRY_AUTHORITY_CHANGED");
                }
                snapshot = claimed.snapshot();
                identity = InheritedRunRecovery.identityFor(snapshot);
                requireRecoveryAttemptIdentity(snapshot, identity,
                        AttemptStatus.CREATED, "claim recovery retry attempt");
            } else {
                identity = InheritedRunRecovery.identityFor(snapshot);
                RunDecision created = applyRecovered(candidate.token(),
                        RunCommand.createAttempt(
                                identity.commandId("create-attempt"), snapshot.runId(),
                                snapshot.revision(), identity.attemptId(),
                                identity.publicationId(), identity.eventTimeMillis()));
                InheritedRunRecovery.Result race = classifyRecoveryMutationRace(
                        candidate, graph, snapshot, created);
                if (race != null) return race;
                snapshot = acceptedRecoveryMutation(
                        created, "create recovery attempt");
                requireRecoveryAttemptIdentity(snapshot, identity,
                        AttemptStatus.CREATED, "create recovery attempt");
            }
        } else {
            identity = InheritedRunRecovery.identityFor(snapshot);
            requireRecoveryAttemptIdentity(snapshot, identity,
                    AttemptStatus.CREATED, "resume created attempt");
        }

        RunDecision dispatched = applyRecovered(candidate.token(),
                RunCommand.advanceAttempt(
                        identity.commandId("dispatch"), snapshot.runId(),
                        snapshot.revision(), identity.attemptId(),
                        identity.fenceToken(), AttemptStatus.DISPATCHING,
                        false, identity.eventTimeMillis(), null, null));
        InheritedRunRecovery.Result race = classifyRecoveryMutationRace(
                candidate, graph, snapshot, dispatched);
        if (race != null) return race;
        snapshot = acceptedRecoveryMutation(dispatched, "dispatch recovery attempt");
        requireRecoveryAttemptIdentity(snapshot, identity,
                AttemptStatus.DISPATCHING, "dispatch recovery attempt");

        DurableRunExecution execution = new DurableRunExecution(
                identity, candidate.token(), snapshot);
        execution.takeResidentLease(candidate.residentLease(),
                candidate.transferResidentToRun());
        try {
            byte[] portableArtifactBytes = portable
                    ? PlanArtifactCodec.encode(
                            artifact, candidate::reserveResident)
                    : null;
            executeSubmittedGraph(
                    graph, identity.publicationId(), execution, null, null,
                    portableArtifactBytes, requiredWorkerCapabilities);
        } finally {
            execution.releaseResidentLease();
        }
        RunSnapshot authoritative = runStateStore.get(snapshot.runId());
        if (authoritative != null && authoritative.status().isTerminal()) {
            return InheritedRunRecovery.Result.resolved();
        }
        AttemptSnapshot attempt = authoritative == null
                ? null : authoritative.currentAttempt();
        return InheritedRunRecovery.Result.pending(attempt == null
                ? "RECOVERY_AUTHORITY_MISSING"
                : "ATTEMPT_" + attempt.status().name());
    }

    private InheritedRunRecovery.Result classifyRecoveryMutationRace(
            InheritedRunRecovery.Candidate candidate, PlanGraph graph,
            RunSnapshot expected, RunDecision decision) {
        if (decision.code() == RunDecision.Code.ACCEPTED
                || decision.code() == RunDecision.Code.ALREADY_APPLIED) {
            return null;
        }
        RunSnapshot current = runStateStore.get(expected.runId());
        if (current == null) {
            return InheritedRunRecovery.Result.retry("RECOVERY_AUTHORITY_MISSING");
        }
        if (current.status().isTerminal()) {
            return InheritedRunRecovery.Result.resolved();
        }
        if (isSameUserCancellation(expected, current)) {
            return reconcileInheritedUserCancellation(candidate, graph);
        }
        if (isSameTimeoutCancellation(expected, current)) {
            return reconcileInheritedTimeout(candidate, graph);
        }
        return null;
    }

    private InheritedRunRecovery.Result reconcileInheritedUserCancellation(
            InheritedRunRecovery.Candidate candidate, PlanGraph graph) {
        RunSnapshot snapshot = runStateStore.get(candidate.snapshot().runId());
        AttemptSnapshot attempt = snapshot == null ? null : snapshot.currentAttempt();
        if (snapshot == null || snapshot.status() != io.rowweave.run.RunStatus.CANCELING
                || attempt == null || attempt.status() != AttemptStatus.CANCELING
                || attempt.cancellationCause() != CancellationCause.USER) {
            return snapshot != null && snapshot.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.retry("CANCELLATION_AUTHORITY_CHANGED");
        }
        RunAttemptFence fence = new RunAttemptFence(
                snapshot.runId(), attempt.attemptId(), attempt.fenceToken());
        StageAssignmentSnapshot evidence =
                stageAssignmentAuthority.assignmentSnapshot(fence);
        if (evidence == null) {
            RunSnapshot canceled = cancelRecoveredAttempt(candidate.token(), snapshot);
            return canceled.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending("CANCELLATION_NO_ASSIGNMENTS");
        }
        if (!fence.equals(evidence.manifest().fence())
                || !snapshot.artifactId().equals(evidence.manifest().artifactId())) {
            throw new IllegalStateException(
                    "cancellation assignment identity mismatch");
        }
        if (containsManifestOutput(graph)) {
            boolean qualityBounded = evidence.manifest().hasQualityGateBinding();
            io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                    qualityBounded
                            ? stageAssignmentAuthority.qualityGateCommitJournal(fence)
                            : null;
            ManifestCommitJournalSnapshot journal = qualityBounded ? null
                    : stageAssignmentAuthority.manifestCommitJournal(fence);
            if (qualityBounded ? qualityJournal == null : journal == null) {
                throw new IllegalStateException(
                        "manifest cancellation journal is missing");
            }
            if (qualityBounded
                    ? qualityJournal.intent() != null : journal.intent() != null) {
                return InheritedRunRecovery.Result.pending(
                        "CANCELLATION_MANIFEST_INTENT_PRESENT");
            }
            // A durable abort has to precede the reconcile call, because reconciling on the worker cancels the
            // stage still active there and rolls back whatever output it had prepared.
            decideRecoveredManifestAbort(
                    candidate.token(), snapshot, graph, evidence, "USER_CANCELED");
        }
        try {
            evidence = reconcileDepartedAssignments(candidate.token(), evidence, graph);
        } catch (PreparedLocatorRecoveryFailedException locatorMissing) {
            // Cancellation already persisted the abort decision above. A
            // successful terminal without its exact locator cannot be turned
            // into a synthetic LOST terminal, but it also cannot delay the
            // requested terminal cancellation indefinitely.
            evidence = locatorMissing.evidence();
        }
        int unsettled = 0;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            if (evidence.acceptance(i) != null && evidence.terminal(i) == null) {
                unsettled++;
            }
        }
        if (unsettled > 0) {
            return InheritedRunRecovery.Result.pending(
                    "CANCELLATION_ASSIGNMENTS_UNSETTLED_" + unsettled);
        }
        snapshot = runStateStore.get(snapshot.runId());
        if (snapshot == null) {
            return InheritedRunRecovery.Result.retry(
                    "CANCELLATION_AUTHORITY_MISSING");
        }
        if (snapshot.status().isTerminal()) {
            return InheritedRunRecovery.Result.resolved();
        }
        RunSnapshot canceled = cancelRecoveredAttempt(candidate.token(), snapshot);
        return canceled.status().isTerminal()
                ? InheritedRunRecovery.Result.resolved()
                : InheritedRunRecovery.Result.pending("CANCELLATION_NOT_TERMINAL");
    }

    private InheritedRunRecovery.Result reconcileInheritedTimeout(
            InheritedRunRecovery.Candidate candidate, PlanGraph graph) {
        RunSnapshot snapshot = runStateStore.get(candidate.snapshot().runId());
        AttemptSnapshot attempt = snapshot == null ? null : snapshot.currentAttempt();
        if (snapshot == null || snapshot.status() != io.rowweave.run.RunStatus.CANCELING
                || attempt == null || attempt.status() != AttemptStatus.CANCELING
                || attempt.cancellationCause() != CancellationCause.TIMEOUT) {
            return snapshot != null && snapshot.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.retry("TIMEOUT_AUTHORITY_CHANGED");
        }
        RunAttemptFence fence = new RunAttemptFence(
                snapshot.runId(), attempt.attemptId(), attempt.fenceToken());
        StageAssignmentSnapshot evidence =
                stageAssignmentAuthority.assignmentSnapshot(fence);
        if (evidence == null) {
            requireTimeoutMayHaveNoAssignmentEvidence(snapshot);
            RunSnapshot timedOut = timeoutRecoveredAttempt(candidate.token(), snapshot);
            return timedOut.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending("TIMEOUT_NO_ASSIGNMENTS");
        }
        if (!fence.equals(evidence.manifest().fence())
                || !snapshot.artifactId().equals(evidence.manifest().artifactId())) {
            throw new IllegalStateException("timeout assignment identity mismatch");
        }
        if (containsManifestOutput(graph)) {
            boolean qualityBounded = evidence.manifest().hasQualityGateBinding();
            io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                    qualityBounded
                            ? stageAssignmentAuthority.qualityGateCommitJournal(fence)
                            : null;
            ManifestCommitJournalSnapshot journal = qualityBounded ? null
                    : stageAssignmentAuthority.manifestCommitJournal(fence);
            if (qualityBounded ? qualityJournal == null : journal == null) {
                throw new IllegalStateException("manifest timeout journal is missing");
            }
            if (qualityBounded
                    ? qualityJournal.intent() != null : journal.intent() != null) {
                return InheritedRunRecovery.Result.pending(
                        "TIMEOUT_MANIFEST_INTENT_PRESENT");
            }
            decideRecoveredManifestAbort(
                    candidate.token(), snapshot, graph, evidence,
                    io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE);
        }
        try {
            evidence = reconcileDepartedAssignments(candidate.token(), evidence, graph);
        } catch (PreparedLocatorRecoveryFailedException locatorMissing) {
            // Timeout recovery follows the same fail-closed rule as user
            // cancellation: durable abort exists before reconciliation.
            evidence = locatorMissing.evidence();
        }
        int unsettled = 0;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            if (evidence.acceptance(i) != null && evidence.terminal(i) == null) {
                unsettled++;
            }
        }
        if (unsettled > 0) {
            return InheritedRunRecovery.Result.pending(
                    "TIMEOUT_ASSIGNMENTS_UNSETTLED_" + unsettled);
        }
        snapshot = runStateStore.get(snapshot.runId());
        if (snapshot == null) {
            return InheritedRunRecovery.Result.retry("TIMEOUT_AUTHORITY_MISSING");
        }
        if (snapshot.status().isTerminal()) {
            return InheritedRunRecovery.Result.resolved();
        }
        RunSnapshot timedOut = timeoutRecoveredAttempt(candidate.token(), snapshot);
        return timedOut.status().isTerminal()
                ? InheritedRunRecovery.Result.resolved()
                : InheritedRunRecovery.Result.pending("TIMEOUT_NOT_TERMINAL");
    }

    private void requireTimeoutMayHaveNoAssignmentEvidence(RunSnapshot snapshot) {
        List<ExecutionEvent> events = runStateStore.events(
                snapshot.runId(), snapshot.revision() - 1L, 1);
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (events.size() != 1) {
            throw new IllegalStateException(
                    "timeout transition event is missing");
        }
        ExecutionEvent event = events.get(0);
        if (event.sequence() != snapshot.revision()
                || event.type() != ExecutionEvent.Type.TIMEOUT_REQUESTED
                || !snapshot.runId().equals(event.runId())
                || !attempt.attemptId().equals(event.attemptId())
                || attempt.fenceToken() != event.fenceToken()
                || event.attemptTo() != AttemptStatus.CANCELING
                || !io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE.equals(
                event.reasonCode())) {
            throw new IllegalStateException(
                    "timeout transition evidence is inconsistent");
        }
        if (event.attemptFrom() != AttemptStatus.CREATED
                && event.attemptFrom() != AttemptStatus.DISPATCHING) {
            throw new IllegalStateException(
                    "running timeout is missing its assignment manifest");
        }
    }

    private RunSnapshot cancelRecoveredAttempt(
            RecoveryToken token, RunSnapshot snapshot) {
        AttemptSnapshot attempt = snapshot.currentAttempt();
        RunDecision decision = applyRecovered(token, RunCommand.advanceAttempt(
                InheritedRunRecovery.inFlightCommandId(snapshot, "user-canceled"),
                snapshot.runId(), snapshot.revision(), attempt.attemptId(),
                attempt.fenceToken(), AttemptStatus.CANCELED, false,
                snapshot.updatedAtMillis(), "USER_CANCEL", null));
        if (decision.code() == RunDecision.Code.STALE_RECOVERY_OWNER
                || decision.code() == RunDecision.Code.STALE_REVISION) {
            RunSnapshot current = runStateStore.get(snapshot.runId());
            if (current != null && current.status().isTerminal()) {
                return current;
            }
            throw new io.rowweave.run.RunStateStoreUnavailableException(
                    "cancel-recovery-" + snapshot.runId(),
                    "recovery ownership changed while completing cancellation", null);
        }
        return acceptedRecoveryMutation(decision,
                "complete recovered user cancellation");
    }

    private RunSnapshot timeoutRecoveredAttempt(
            RecoveryToken token, RunSnapshot snapshot) {
        AttemptSnapshot attempt = snapshot.currentAttempt();
        RunDecision decision = applyRecovered(token, RunCommand.advanceAttempt(
                InheritedRunRecovery.inFlightCommandId(snapshot, "deadline-timed-out"),
                snapshot.runId(), snapshot.revision(), attempt.attemptId(),
                attempt.fenceToken(), AttemptStatus.TIMED_OUT, false,
                snapshot.updatedAtMillis(),
                io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE, null));
        if (decision.code() == RunDecision.Code.STALE_RECOVERY_OWNER
                || decision.code() == RunDecision.Code.STALE_REVISION) {
            RunSnapshot current = runStateStore.get(snapshot.runId());
            if (current != null && current.status().isTerminal()) return current;
            throw new io.rowweave.run.RunStateStoreUnavailableException(
                    "timeout-recovery-" + snapshot.runId(),
                    "recovery ownership changed while completing timeout", null);
        }
        return acceptedRecoveryMutation(decision,
                "complete recovered deadline timeout");
    }

    private InheritedRunRecovery.Result reconcileInheritedInFlight(
            InheritedRunRecovery.Candidate candidate, PlanGraph graph) {
        RunSnapshot snapshot = candidate.snapshot();
        RunSnapshot current = runStateStore.get(snapshot.runId());
        if (isSameUserCancellation(snapshot, current)) {
            return reconcileInheritedUserCancellation(candidate, graph);
        }
        if (isSameTimeoutCancellation(snapshot, current)) {
            return reconcileInheritedTimeout(candidate, graph);
        }
        if (current == null || current.status().isTerminal()) {
            return current == null
                    ? InheritedRunRecovery.Result.retry("RECOVERY_AUTHORITY_MISSING")
                    : InheritedRunRecovery.Result.resolved();
        }
        if (current.revision() != snapshot.revision()) {
            return InheritedRunRecovery.Result.retry("RECOVERY_AUTHORITY_CHANGED");
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        RunAttemptFence fence = new RunAttemptFence(
                snapshot.runId(), attempt.attemptId(), attempt.fenceToken());
        StageAssignmentSnapshot evidence = stageAssignmentAuthority.assignmentSnapshot(fence);
        if (evidence == null) {
            throw new IllegalStateException("in-flight run is missing assignment manifest");
        }
        if (!fence.equals(evidence.manifest().fence())
                || !snapshot.artifactId().equals(evidence.manifest().artifactId())) {
            throw new IllegalStateException("in-flight assignment identity mismatch");
        }
        try {
            evidence = reconcileDepartedAssignments(candidate.token(), evidence, graph);
        } catch (PreparedLocatorRecoveryFailedException locatorMissing) {
            current = runStateStore.get(snapshot.runId());
            if (current == null) {
                return InheritedRunRecovery.Result.retry(
                        "RECOVERY_AUTHORITY_MISSING");
            }
            if (current.status().isTerminal()) {
                return InheritedRunRecovery.Result.resolved();
            }
            if (containsUnprojectedProviderOutput(graph)) {
                decideRecoveredProviderAbort(current, graph,
                        "PREPARED_LOCATOR_RECOVERY_FAILED");
            } else {
                decideRecoveredManifestAbort(
                        candidate.token(), current, graph,
                        locatorMissing.evidence(),
                        "PREPARED_LOCATOR_RECOVERY_FAILED");
            }
            RunSnapshot failed = failRecoveredAttempt(
                    candidate.token(), current,
                    "PREPARED_LOCATOR_RECOVERY_FAILED");
            return failed.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending(
                            "PREPARED_LOCATOR_RECOVERY_FAILED");
        }
        current = runStateStore.get(snapshot.runId());
        if (isSameUserCancellation(snapshot, current)) {
            return reconcileInheritedUserCancellation(candidate, graph);
        }
        if (isSameTimeoutCancellation(snapshot, current)) {
            return reconcileInheritedTimeout(candidate, graph);
        }
        if (current == null || current.status().isTerminal()) {
            return current == null
                    ? InheritedRunRecovery.Result.retry("RECOVERY_AUTHORITY_MISSING")
                    : InheritedRunRecovery.Result.resolved();
        }
        if (current.revision() != snapshot.revision()) {
            return InheritedRunRecovery.Result.retry("RECOVERY_AUTHORITY_CHANGED");
        }
        int unsettled = 0;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            if (evidence.acceptance(i) != null && evidence.terminal(i) == null) {
                unsettled++;
            }
        }
        if (unsettled > 0) {
            return InheritedRunRecovery.Result.pending(
                    "ASSIGNMENTS_UNSETTLED_" + unsettled);
        }
        boolean allSucceeded = true;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            StageTerminalReceipt terminal = evidence.terminal(i);
            if (evidence.acceptance(i) == null || terminal == null
                    || terminal.outcome() != StageTerminalOutcome.SUCCEEDED) {
                allSucceeded = false;
                break;
            }
        }
        if (containsUnprojectedProviderOutput(graph)) {
            if (!allSucceeded) {
                decideRecoveredProviderAbort(
                        snapshot, graph, "STAGE_TERMINAL_FAILURE");
                RunSnapshot failed = failRecoveredAttempt(
                        candidate.token(), snapshot, "STAGE_TERMINAL");
                return failed.status().isTerminal()
                        ? InheritedRunRecovery.Result.resolved()
                        : InheritedRunRecovery.Result.pending(
                                "PROVIDER_ABORT_RECONCILIATION");
            }
            return reconcileRecoveredProvider(
                    candidate.token(), snapshot, graph, evidence);
        }
        if (containsManifestOutput(graph)) {
            if (!allSucceeded) {
                io.rowweave.run.BatchSinkAbortRecord abort =
                        decideRecoveredManifestAbort(
                        candidate.token(), snapshot, graph, evidence,
                        "STAGE_TERMINAL_FAILURE");
                if (snapshot.maxAttempts() > 1
                        && isWorkerLostRetryClosure(evidence)) {
                    abort = completeRecoveredManifestAbort(
                            candidate.token(), snapshot, graph, evidence, abort);
                    if (abort.state()
                            == io.rowweave.run.BatchSinkAbortRecord.State.ABORTED) {
                        InheritedRunRecovery.Result retry = scheduleRecoveredRetry(
                                candidate.token(), snapshot);
                        if (retry != null) return retry;
                    }
                }
                RunSnapshot failed = failRecoveredAttempt(
                        candidate.token(), snapshot, "STAGE_TERMINAL");
                return failed.status().isTerminal()
                        ? InheritedRunRecovery.Result.resolved()
                        : InheritedRunRecovery.Result.pending("MANIFEST_ROLLBACK_RECONCILIATION");
            }
            return reconcileRecoveredManifest(candidate.token(), snapshot, graph, evidence);
        }
        if (!allSucceeded) {
            InheritedRunRecovery.Result retry = scheduleRecoveredRetry(
                    candidate.token(), snapshot);
            if (retry != null) return retry;
        }
        snapshot = allSucceeded
                ? succeedRecoveredAttempt(candidate.token(), snapshot,
                io.rowweave.run.RunResultSummary.discardFrom(evidence))
                : failRecoveredAttempt(candidate.token(), snapshot, "STAGE_TERMINAL");
        return snapshot.status().isTerminal()
                ? InheritedRunRecovery.Result.resolved()
                : InheritedRunRecovery.Result.pending(
                "ATTEMPT_" + snapshot.currentAttempt().status().name());
    }

    private InheritedRunRecovery.Result recoverIncrementalRun(
            InheritedRunRecovery.Candidate candidate, PlanGraph graph,
            io.rowweave.incremental.IncrementalAuthorityRecord record) {
        io.rowweave.incremental.IncrementalActiveInterval active =
                record.activeInterval();
        RunSnapshot snapshot = incrementalAuthorityGateway.runSnapshot(
                candidate.snapshot().runId());
        AttemptSnapshot attempt = snapshot == null
                ? null : snapshot.currentAttempt();
        if (active == null || snapshot == null || attempt == null
                || !active.fence().runId().equals(snapshot.runId())
                || !active.fence().attemptId().equals(attempt.attemptId())
                || active.fence().fenceToken() != attempt.fenceToken()
                || !active.artifactId().equals(snapshot.artifactId())) {
            throw new IllegalStateException(
                    "incremental recovery authority identity mismatch");
        }
        io.rowweave.run.RecoveryOwner owner = incrementalRecoveryOwner();
        io.rowweave.incremental.FrontierRecoveryToken previous =
                incrementalAuthorityGateway.frontierOwnership(record.feedId());
        long expectedEpoch = previous == null
                ? 0L : previous.ownershipEpoch();
        io.rowweave.incremental.FrontierOwnershipClaim claimed =
                incrementalAuthorityGateway.claimOwnership(
                        record.feedId(), record.revision(), expectedEpoch, owner);
        if (claimed.code()
                != io.rowweave.incremental.FrontierOwnershipClaim.Code.ACQUIRED
                && claimed.code()
                != io.rowweave.incremental.FrontierOwnershipClaim.Code.REPLAYED) {
            return InheritedRunRecovery.Result.retry(
                    "INCREMENTAL_FRONTIER_OWNERSHIP_CHANGED");
        }
        io.rowweave.incremental.FrontierRecoveryToken frontierToken =
                claimed.token();
        if (!owner.equals(candidate.token().owner())
                || !incrementalAuthorityGateway.currentlyOwns(
                        frontierToken, candidate.token())) {
            return InheritedRunRecovery.Result.retry(
                    "INCREMENTAL_DUAL_OWNERSHIP_CHANGED");
        }
        DurableRunExecution execution = new DurableRunExecution(
                record, active, frontierToken, candidate.token(), snapshot);
        if (attempt.status() == AttemptStatus.CANCELING) {
            StageAssignmentSnapshot evidence = execution.assignmentSnapshot();
            if (evidence != null) {
                reconcileIncrementalAssignments(execution, evidence);
            }
            boolean resolved = attempt.cancellationCause()
                    == CancellationCause.TIMEOUT
                    ? execution.completeTimeoutIfProven()
                    : execution.completeUserCancellationIfProven();
            return resolved
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending(
                            "INCREMENTAL_CANCELLATION_EVIDENCE_INCOMPLETE");
        }
        if (attempt.status() != AttemptStatus.COMMITTING) {
            return InheritedRunRecovery.Result.pending(
                    "INCREMENTAL_" + attempt.status().name());
        }
        ManifestCommitJournalSnapshot journal =
                incrementalAuthorityGateway.manifestCommitJournal(
                        active.fence());
        if (journal == null || !journal.allExpectedMembersSealed()
                || journal.intent() == null) {
            return InheritedRunRecovery.Result.pending(
                    "INCREMENTAL_COMMIT_EVIDENCE_INCOMPLETE");
        }
        io.rowweave.commit.CommitManifest intended =
                journal.intendedManifest();
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        io.rowweave.commit.ManifestFileStore store =
                new io.rowweave.commit.ManifestFileStore(
                        java.nio.file.Paths.get(output.domainPath()));
        io.rowweave.commit.CommitManifest visible =
                store.exact(journal.intent().generation());
        if (visible == null) {
            return InheritedRunRecovery.Result.pending(
                    "INCREMENTAL_PUBLICATION_NOT_VISIBLE");
        }
        if (!intended.equals(visible)
                || !attempt.publicationId().equals(visible.attemptId())) {
            throw new IllegalStateException(
                    "incremental visible publication conflicts with intent");
        }
        try (ManifestJobCommit manifest = new ManifestJobCommit(
                graph, store, attempt.publicationId(), visible.generation(),
                java.util.Collections.emptyMap(), null)) {
            execution.markSucceeded(
                    execution.resultSummary(manifest), manifest);
        }
        return InheritedRunRecovery.Result.resolved();
    }

    private StageAssignmentSnapshot reconcileIncrementalAssignments(
            DurableRunExecution execution, StageAssignmentSnapshot evidence) {
        boolean changed = false;
        for (int index = 0;
             index < evidence.manifest().assignmentCount(); index++) {
            StageAssignmentAcceptance acceptance = evidence.acceptance(index);
            if (acceptance == null || evidence.terminal(index) != null) {
                continue;
            }
            io.rowweave.run.StageAssignmentRecord assignment =
                    evidence.manifest().assignment(index);
            RegistryWorker worker = endpointOf(assignment.workerId());
            boolean exactWorker = worker != null
                    && assignment.host().equals(worker.host())
                    && assignment.rpcPort() == worker.port()
                    && assignment.workerIncarnationId().equals(
                            worker.incarnationId());
            StageTerminalReceipt terminal;
            if (exactWorker) {
                final RpcMessage response;
                try {
                    MasterElection election = leadership;
                    if (election == null || election.termToken() == 0L) {
                        throw new IllegalStateException(
                                "active incremental recovery term is unavailable");
                    }
                    RpcMessage request = RpcMessage.reconcileDurableStage(
                            assignment.workerId(),
                            StageAssignmentManifestCodec.encodeToken(
                                    acceptance.token()),
                            acceptance.commandDigest(), election.self(),
                            election.termToken(),
                            assignment.workerIncarnationId(),
                            evidence.manifest().fence());
                    response = secureInbound(rpcCache.call(
                            assignment.host(), assignment.rpcPort(),
                            secureOutbound(request)));
                } catch (RuntimeException unavailable) {
                    throw reconciliationUnavailable(
                            evidence,
                            "incremental Worker reconciliation RPC failed",
                            unavailable);
                }
                if (response != null
                        && response.type()
                        == RpcMessage.Type.DURABLE_STAGE_RECONCILED
                        && "TERMINAL".equals(
                                response.reconciliationOutcome())) {
                    try {
                        terminal = StageAssignmentManifestCodec
                                .decodeTerminalReceipt(
                                        response
                                        .reconciledTerminalReceiptBytes())
                                .withTelemetry(decodeStageTelemetry(response));
                    } catch (RuntimeException malformed) {
                        throw new IllegalStateException(
                                "Worker returned malformed incremental terminal",
                                malformed);
                    }
                    if (!terminal.token().equals(acceptance.token())
                            || !java.security.MessageDigest.isEqual(
                                    terminal.commandDigest(),
                                    acceptance.commandDigest())) {
                        throw new IllegalStateException(
                                "incremental terminal reconciliation identity mismatch");
                    }
                } else if (response != null
                        && response.type()
                        == RpcMessage.Type.DURABLE_STAGE_RECONCILED
                        && "ABSENT".equals(
                                response.reconciliationOutcome())) {
                    terminal = incrementalLostTerminal(
                            acceptance, assignment);
                } else {
                    throw reconciliationUnavailable(
                            evidence,
                            "incremental Worker reconciliation is not terminal",
                            null);
                }
            } else {
                terminal = incrementalLostTerminal(acceptance, assignment);
            }
            StageTerminalDecision decision =
                    execution.recordStageTerminal(terminal);
            if (decision.code() != StageTerminalDecision.Code.RECORDED
                    && decision.code()
                    != StageTerminalDecision.Code.ALREADY_RECORDED) {
                throw reconciliationUnavailable(
                        evidence,
                        "incremental terminal reconciliation rejected: "
                                + decision.code(), null);
            }
            changed = true;
        }
        if (!changed) return evidence;
        StageAssignmentSnapshot refreshed = execution.assignmentSnapshot();
        if (refreshed == null) {
            throw new IllegalStateException(
                    "incremental assignment evidence disappeared after reconciliation");
        }
        return refreshed;
    }

    private static StageTerminalReceipt incrementalLostTerminal(
            StageAssignmentAcceptance acceptance,
            io.rowweave.run.StageAssignmentRecord assignment) {
        byte[] failureDigest = sha256(("worker-lost\0"
                + assignment.workerId() + '\0'
                + assignment.workerIncarnationId())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return StageTerminalReceipt.lost(
                acceptance.token(), acceptance.commandDigest(),
                "WORKER_LOST", failureDigest, 0L);
    }

    private InheritedRunRecovery.Result scheduleRecoveredRetry(
            RecoveryToken token, RunSnapshot snapshot) {
        if (snapshot.maxAttempts() <= 1) return null;
        if (wholeAttemptRetryAuthority == null) {
            return InheritedRunRecovery.Result.retry(
                    "RETRY_AUTHORITY_UNAVAILABLE");
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        WholeAttemptRetryDecision decision = wholeAttemptRetryAuthority
                .scheduleRetryRecovered(token, new RunAttemptFence(
                        snapshot.runId(), attempt.attemptId(),
                        attempt.fenceToken()));
        retryRecoveryMetrics.record(decision);
        switch (decision.code()) {
            case SCHEDULED:
            case ALREADY_SCHEDULED:
                return InheritedRunRecovery.Result.pending("RETRY_SCHEDULED");
            case EXHAUSTED:
            case RETIRING:
                return InheritedRunRecovery.Result.resolved();
            case CLOSURE_PENDING:
                return InheritedRunRecovery.Result.pending(
                        "RETRY_CLOSURE_PENDING");
            case STALE_RUN:
                return InheritedRunRecovery.Result.retry(
                        "RETRY_AUTHORITY_CHANGED");
            case STALE_RECOVERY_OWNER:
                return InheritedRunRecovery.Result.retry("LOST_OWNERSHIP");
            case NOT_FOUND:
            case UNAVAILABLE:
                return InheritedRunRecovery.Result.retry(
                        "RETRY_AUTHORITY_UNAVAILABLE");
            case TOO_LATE:
                RunSnapshot current = runStateStore.get(snapshot.runId());
                return current != null && current.status().isTerminal()
                        ? InheritedRunRecovery.Result.resolved()
                        : InheritedRunRecovery.Result.pending(
                        "RETRY_AUTHORITY_CHANGED");
            case NOT_RETRYABLE:
            default:
                return null;
        }
    }

    private static boolean isWorkerLostRetryClosure(
            StageAssignmentSnapshot evidence) {
        boolean lost = false;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            if (evidence.acceptance(i) == null) {
                if (evidence.terminal(i) != null) return false;
                continue;
            }
            StageTerminalReceipt terminal = evidence.terminal(i);
            if (terminal == null
                    || terminal.outcome() == StageTerminalOutcome.FAILED) {
                return false;
            }
            if (terminal.outcome() == StageTerminalOutcome.LOST) lost = true;
        }
        return lost;
    }

    private static boolean isSameUserCancellation(
            RunSnapshot expected, RunSnapshot observed) {
        return isSameCancellation(expected, observed, CancellationCause.USER);
    }

    private static boolean isSameTimeoutCancellation(
            RunSnapshot expected, RunSnapshot observed) {
        return isSameCancellation(expected, observed, CancellationCause.TIMEOUT);
    }

    private static boolean isSameCancellation(
            RunSnapshot expected, RunSnapshot observed, CancellationCause cause) {
        if (expected == null || observed == null
                || observed.status() != io.rowweave.run.RunStatus.CANCELING) {
            return false;
        }
        AttemptSnapshot expectedAttempt = expected.currentAttempt();
        AttemptSnapshot observedAttempt = observed.currentAttempt();
        return expectedAttempt != null && observedAttempt != null
                && expectedAttempt.attemptId().equals(observedAttempt.attemptId())
                && expectedAttempt.fenceToken() == observedAttempt.fenceToken()
                && observedAttempt.status() == AttemptStatus.CANCELING
                && observedAttempt.cancellationCause() == cause;
    }

    private InheritedRunRecovery.Result reconcileRecoveredProvider(
            RecoveryToken token, RunSnapshot snapshot, PlanGraph graph,
            StageAssignmentSnapshot evidence) {
        if (testProviderAuthorityFactory == null) {
            throw new IllegalStateException(
                    "custom durable Sink recovery authority is unavailable");
        }
        if (!containsProviderSinkOutput(graph)) {
            decideRecoveredProviderAbort(
                    snapshot, graph, "PREPARED_LOCATOR_RECOVERY_FAILED");
            RunSnapshot failed = failRecoveredAttempt(
                    token, snapshot, "PREPARED_LOCATOR_RECOVERY_FAILED");
            return failed.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending(
                            "PREPARED_LOCATOR_RECOVERY_FAILED");
        }
        if (snapshot.currentAttempt().status() == AttemptStatus.RUNNING) {
            snapshot = advanceRecovered(token, snapshot,
                    AttemptStatus.COMMITTING, "provider-committing", null);
        }
        if (snapshot.currentAttempt().status() != AttemptStatus.COMMITTING) {
            throw new IllegalStateException(
                    "provider recovery requires COMMITTING attempt");
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        java.util.List<io.rowweave.ProviderBatchSink> outputs =
                providerOutputs(graph);
        try (DurableSinkAuthorityAdapterFactory.Reservation reservation =
                     testProviderAuthorityFactory.reserve(
                             evidence.manifest().fence(),
                             attempt.publicationId(), outputs)) {
            java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                    providerLocators(evidence);
            DurableSinkCommitCoordinator coordinator =
                    new DurableSinkCommitCoordinator(
                            reservation.bind(locators),
                            (action, decisionDigest) ->
                                    executeRecoveredProviderGroupControl(
                                            graph, evidence,
                                            attempt.publicationId(),
                                            reservation.generation(), action,
                                            decisionDigest,
                                            deadlineAfterMillis(
                                                    outputCommitTimeoutMs)));
            io.rowweave.commit.BatchSinkOutcome outcome =
                    coordinator.publishAndFinalize();
            if (outcome.generation() == null
                    || outcome.identityDigest() == null) {
                throw new IllegalStateException(
                        "recovered provider publication lacks durable identity");
            }
            snapshot = succeedRecoveredAttempt(token, snapshot,
                    io.rowweave.run.RunResultSummary.manifestFrom(
                            evidence, outcome.generation(),
                            outcome.identityDigest(), outcome.identityDigest()));
        }
        return snapshot.status().isTerminal()
                ? InheritedRunRecovery.Result.resolved()
                : InheritedRunRecovery.Result.pending("PROVIDER_COMMITTING");
    }

    private void decideRecoveredProviderAbort(
            RunSnapshot snapshot, PlanGraph graph, String reasonCode) {
        if (testProviderAuthorityFactory == null
                || snapshot == null || snapshot.currentAttempt() == null) {
            throw new IllegalStateException(
                    "custom durable Sink recovery authority is unavailable");
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        RunAttemptFence fence = new RunAttemptFence(snapshot.runId(),
                attempt.attemptId(), attempt.fenceToken());
        try (DurableSinkAuthorityAdapterFactory.Reservation reservation =
                     testProviderAuthorityFactory.reserve(
                             fence, attempt.publicationId(),
                             providerOutputs(graph))) {
            reservation.bind(java.util.Collections.emptyList())
                    .requireAbortDecision(reasonCode);
        }
    }

    private InheritedRunRecovery.Result reconcileRecoveredManifest(
            RecoveryToken token, RunSnapshot snapshot, PlanGraph graph,
            StageAssignmentSnapshot evidence) {
        if (!(stageAssignmentAuthority instanceof RecoveryFencedStageAssignmentAuthority)) {
            throw new IllegalStateException(
                    "recovery-fenced manifest authority unavailable");
        }
        RecoveryFencedStageAssignmentAuthority recoveryAuthority =
                (RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority;
        boolean qualityBounded = evidence.manifest().hasQualityGateBinding();
        ManifestCommitJournalSnapshot journal = null;
        io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal = null;
        if (qualityBounded) {
            qualityJournal = stageAssignmentAuthority.qualityGateCommitJournal(
                    evidence.manifest().fence());
        } else {
            journal = stageAssignmentAuthority.manifestCommitJournal(
                    evidence.manifest().fence());
        }
        if (qualityBounded
                ? qualityJournal == null
                || !qualityJournal.fence().equals(evidence.manifest().fence())
                || qualityJournal.assignmentGeneration()
                != evidence.manifest().generation()
                : journal == null
                || !journal.assignments().manifest().fence().equals(
                evidence.manifest().fence())
                || journal.assignments().manifest().generation()
                != evidence.manifest().generation()) {
            throw new IllegalStateException("manifest commit journal identity mismatch");
        }
        if (qualityBounded) {
            qualityJournal = recoverMissingQualityManifestMembers(
                    token, snapshot, graph, evidence, qualityJournal);
        } else {
            journal = recoverMissingManifestMembers(
                    token, snapshot, graph, evidence, journal);
        }
        if (qualityBounded
                ? !qualityJournal.allMembersSealed()
                : !journal.allExpectedMembersSealed()) {
            decideRecoveredManifestAbort(
                    token, snapshot, graph, evidence,
                    "MANIFEST_MEMBERS_INCOMPLETE");
            RunSnapshot failed = failRecoveredAttempt(
                    token, snapshot, "MANIFEST_MEMBERS_INCOMPLETE");
            return failed.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending("MANIFEST_MEMBERS_INCOMPLETE");
        }
        if (snapshot.currentAttempt().status() == AttemptStatus.RUNNING) {
            snapshot = advanceRecovered(token, snapshot, AttemptStatus.COMMITTING,
                    "manifest-committing", null);
        }
        if (snapshot.currentAttempt().status() != AttemptStatus.COMMITTING) {
            throw new IllegalStateException(
                    "manifest recovery requires COMMITTING attempt");
        }
        if (qualityBounded && qualityJournal.intent() == null) {
            io.rowweave.QualityGatePolicy policy = qualityGatePolicy(graph);
            byte[] expectedPolicy = qualityJournal.binding().policyDigest();
            if (!java.security.MessageDigest.isEqual(
                    expectedPolicy,
                    io.rowweave.QualityGatePolicyCodec.digest(policy))) {
                throw new IllegalStateException(
                        "recovered Artifact quality policy does not match assignment root");
            }
            io.rowweave.QualityGateDecision qualityDecision =
                    qualityJournal.decision();
            if (qualityDecision == null) {
                io.rowweave.QualityPreparedEvidence prepared =
                        io.rowweave.QualityPreparedEvidenceReader.read(
                                graph, qualityJournal,
                                io.rowweave.QualityEvidenceReadGuard.until(
                                        deadlineAfterMillis(outputCommitTimeoutMs),
                                        () -> !isActiveLeader()
                                                || !recoveryOwnership.current(token)));
                qualityDecision = io.rowweave.QualityGateDecision.evaluate(
                        snapshot.artifactId(), evidence.manifest().fence(),
                        snapshot.currentAttempt().publicationId(), policy,
                        prepared);
                io.rowweave.QualityGateDecisionResult recorded =
                        recordRecoveredQualityDecision(
                                recoveryAuthority, token, qualityDecision);
                if (recorded.code()
                        != io.rowweave.QualityGateDecisionResult.Code.RECORDED
                        && recorded.code()
                        != io.rowweave.QualityGateDecisionResult.Code.ALREADY_RECORDED) {
                    if (recorded.code()
                            == io.rowweave.QualityGateDecisionResult.Code.UNAVAILABLE
                            || recorded.code()
                            == io.rowweave.QualityGateDecisionResult.Code.RECOVERY_OWNED
                            || recorded.code()
                            == io.rowweave.QualityGateDecisionResult.Code.STALE_RUN) {
                        throw reconciliationUnavailable(evidence,
                                "quality decision recovery owner changed", null);
                    }
                    throw new IllegalStateException(
                            "recovered quality decision rejected: "
                                    + recorded.code());
                }
                qualityDecision = recorded.decision();
                qualityJournal = qualityJournal.withQualityDecision(
                        qualityDecision);
                if (!qualityDecision.equals(qualityJournal.decision())) {
                    throw new IllegalStateException(
                            "recovered quality decision disappeared");
                }
            } else {
                requireRecoveredQualityDecision(
                        snapshot, evidence, expectedPolicy, qualityDecision);
            }
            if (qualityDecision.outcome()
                    == io.rowweave.QualityGateDecision.Outcome.FAIL) {
                io.rowweave.run.BatchSinkAbortRecord abort =
                        decideRecoveredManifestAbort(
                                token, snapshot, graph, evidence,
                                "QUALITY_GATE_REJECTED");
                abort = completeRecoveredManifestAbort(
                        token, snapshot, graph, evidence, abort);
                if (abort.state()
                        != io.rowweave.run.BatchSinkAbortRecord.State.ABORTED) {
                    return InheritedRunRecovery.Result.pending(
                            "QUALITY_GATE_ABORT_PENDING");
                }
                RunSnapshot failed = failRecoveredAttempt(
                        token, snapshot, "QUALITY_GATE_REJECTED");
                return failed.status().isTerminal()
                        ? InheritedRunRecovery.Result.resolved()
                        : InheritedRunRecovery.Result.pending(
                        "QUALITY_GATE_REJECTED");
            }
        }
        ManifestCommitIntent currentIntent = qualityBounded
                ? qualityJournal.intent() : journal.intent();
        if (currentIntent == null) {
            ManifestCommitIntent requestedIntent = qualityBounded
                    ? qualityJournal.buildIntent() : journal.buildIntent();
            ManifestIntentDecision decision = recoveryAuthority.commitManifestIntentRecovered(
                    token, requestedIntent);
            if (decision.code() != ManifestIntentDecision.Code.COMMITTED
                    && decision.code() != ManifestIntentDecision.Code.ALREADY_COMMITTED) {
                if (decision.code() == ManifestIntentDecision.Code.UNAVAILABLE) {
                    throw reconciliationUnavailable(evidence,
                            "manifest intent authority unavailable", null);
                }
                throw new IllegalStateException(
                        "manifest intent rejected: " + decision.code());
            }
            if (!requestedIntent.equals(decision.intent())) {
                throw new IllegalStateException(
                        "manifest intent authority acknowledged different bytes");
            }
            if (qualityBounded) {
                qualityJournal = qualityJournal.withIntent(decision.intent());
            } else {
                journal = journal.withIntent(decision.intent());
            }
            currentIntent = decision.intent();
        }
        if (currentIntent == null) {
            throw new IllegalStateException("durable manifest intent disappeared");
        }
        io.rowweave.commit.CommitManifest intended = qualityBounded
                ? qualityJournal.intendedManifest() : journal.intendedManifest();
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        if (!output.domainId().equals(currentIntent.domainId())
                || !output.domainId().equals(intended.domainId())) {
            throw new IllegalStateException("manifest recovery domain identity mismatch");
        }
        if (!hasProviderLocatorClosure(evidence)) {
            // Compatibility only: pre-R4 journals sealed Manifest members
            // without provider locators. New assignments never enter here.
            io.rowweave.commit.ManifestFileStore legacyStore =
                    new io.rowweave.commit.ManifestFileStore(
                            java.nio.file.Paths.get(output.domainPath()));
            io.rowweave.commit.CommitManifest observed =
                    legacyStore.exact(currentIntent.generation());
            if (observed == null) {
                if (!isActiveLeader() || !recoveryOwnership.current(token)) {
                    throw reconciliationUnavailable(evidence,
                            "manifest recovery owner is no longer current", null);
                }
                beforeManifestPublishHook.run();
                if (!isActiveLeader() || !recoveryOwnership.current(token)) {
                    throw reconciliationUnavailable(evidence,
                            "manifest recovery owner changed before publish", null);
                }
                legacyStore.publish(intended);
                afterManifestPublishHook.run();
                observed = legacyStore.exact(currentIntent.generation());
            }
            if (!intended.equals(observed)) {
                throw new IllegalStateException(
                        "published manifest conflicts with durable intent");
            }
            snapshot = succeedRecoveredAttempt(token, snapshot,
                    io.rowweave.run.RunResultSummary.manifestFrom(
                            evidence, observed.generation(),
                            sha256(observed.encode()),
                            qualityBounded ? sinkGroupIdentity(qualityJournal)
                                    : sinkGroupIdentity(journal)));
            return snapshot.status().isTerminal()
                    ? InheritedRunRecovery.Result.resolved()
                    : InheritedRunRecovery.Result.pending(
                            "MANIFEST_COMMITTING");
        }
        if (!hasProviderLocatorClosure(evidence)) {
            throw reconciliationUnavailable(evidence,
                    "manifest recovery lacks provider locator closure", null);
        }
        if (!isActiveLeader() || !recoveryOwnership.current(token)) {
            throw reconciliationUnavailable(evidence,
                    "manifest recovery owner is no longer current", null);
        }
        final ManifestCommitIntent durableIntent = currentIntent;
        final String durablePublicationId =
                snapshot.currentAttempt().publicationId();
        ManifestDurableSinkAuthorityAdapter adapter =
                manifestAuthorityProjection(durableIntent, intended);
        DurableSinkCommitCoordinator coordinator =
                new DurableSinkCommitCoordinator(adapter,
                        (action, decisionDigest) ->
                                executeRecoveredProviderGroupControl(
                                        graph, evidence,
                                        durablePublicationId,
                                        durableIntent.generation(), action,
                                        decisionDigest,
                                        deadlineAfterMillis(
                                                outputCommitTimeoutMs)));
        beforeManifestPublishHook.run();
        if (!isActiveLeader() || !recoveryOwnership.current(token)) {
            throw reconciliationUnavailable(evidence,
                    "manifest recovery owner changed before publish", null);
        }
        io.rowweave.commit.BatchSinkOutcome published =
                coordinator.publishAndFinalize();
        afterManifestPublishHook.run();
        if (published.state()
                != io.rowweave.commit.BatchSinkOutcome.State.COMMITTED
                || published.generation() == null
                || published.generation() != durableIntent.generation()
                || published.identityDigest() == null
                || !java.security.MessageDigest.isEqual(
                published.identityDigest(), durableIntent.manifestHash())) {
            throw new IllegalStateException(
                    "published manifest conflicts with durable intent");
        }
        snapshot = succeedRecoveredAttempt(token, snapshot,
                io.rowweave.run.RunResultSummary.manifestFrom(
                        evidence, durableIntent.generation(),
                        durableIntent.manifestHash(),
                        qualityBounded ? sinkGroupIdentity(qualityJournal)
                                : sinkGroupIdentity(journal)));
        return snapshot.status().isTerminal()
                ? InheritedRunRecovery.Result.resolved()
                : InheritedRunRecovery.Result.pending("MANIFEST_COMMITTING");
    }

    private io.rowweave.QualityGateDecisionResult
    recordRecoveredQualityDecision(
            RecoveryFencedStageAssignmentAuthority recoveryAuthority,
            RecoveryToken token,
            io.rowweave.QualityGateDecision decision) {
        try {
            return recoveryAuthority.recordQualityGateDecisionRecovered(
                    token, decision);
        } catch (RuntimeException uncertain) {
            io.rowweave.QualityGateDecision observed;
            try {
                observed = stageAssignmentAuthority.qualityGateDecision(
                        decision.fence());
            } catch (RuntimeException readFailure) {
                readFailure.addSuppressed(uncertain);
                throw readFailure;
            }
            if (decision.equals(observed)) {
                return io.rowweave.QualityGateDecisionResult
                        .alreadyRecorded(observed);
            }
            throw uncertain;
        }
    }

    private static void requireRecoveredQualityDecision(
            RunSnapshot snapshot, StageAssignmentSnapshot evidence,
            byte[] expectedPolicy,
            io.rowweave.QualityGateDecision decision) {
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (!snapshot.artifactId().equals(decision.artifactId())
                || !evidence.manifest().fence().equals(decision.fence())
                || !attempt.publicationId().equals(decision.publicationId())
                || !java.security.MessageDigest.isEqual(
                expectedPolicy, decision.policyDigest())) {
            throw new IllegalStateException(
                    "recovered quality decision identity mismatch");
        }
    }

    private io.rowweave.run.BatchSinkAbortRecord decideRecoveredManifestAbort(
            RecoveryToken token, RunSnapshot snapshot, PlanGraph graph,
            StageAssignmentSnapshot evidence, String reasonCode) {
        if (!(stageAssignmentAuthority instanceof RecoveryFencedStageAssignmentAuthority)) {
            throw new IllegalStateException(
                    "recovery-fenced manifest authority unavailable");
        }
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        java.nio.file.Path domain = java.nio.file.Paths.get(output.domainPath());
        AttemptSnapshot attempt = snapshot.currentAttempt();
        Long generation = manifestCommitAuthority.find(domain, attempt.publicationId());
        if (generation == null) {
            throw new IllegalStateException(
                    "running manifest attempt lost its durable generation reservation");
        }
        RunAttemptFence fence = evidence.manifest().fence();
        io.rowweave.run.BatchSinkAbortRecord existing =
                stageAssignmentAuthority.batchSinkAbort(fence);
        byte[] abortIdentity = recoveredSinkGroupIdentity(
                evidence.manifest(), graph);
        if ("QUALITY_GATE_REJECTED".equals(reasonCode)) {
            io.rowweave.QualityGateDecision qualityDecision =
                    stageAssignmentAuthority.qualityGateDecision(fence);
            if (qualityDecision == null
                    || qualityDecision.outcome()
                    != io.rowweave.QualityGateDecision.Outcome.FAIL) {
                throw new IllegalStateException(
                        "quality abort requires one retained FAIL decision");
            }
            abortIdentity = io.rowweave.QualityGateDecisionCodec.digest(
                    qualityDecision);
        }
        io.rowweave.run.BatchSinkAbortRecord decided = existing == null
                ? io.rowweave.run.BatchSinkAbortRecord.decided(
                        fence, evidence.manifest().generation(),
                        io.rowweave.BatchSinkDescriptor.CapabilityProfile
                                .MANIFEST_ATOMIC_GROUP_V1,
                        new io.rowweave.commit.ManifestFileStore(domain).domainId(),
                        generation, attempt.publicationId(),
                        abortIdentity,
                        reasonCode, attempt.updatedAtMillis())
                : existing;
        io.rowweave.run.BatchSinkAbortDecision decision =
                ((RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority)
                        .recordBatchSinkAbortRecovered(token, decided);
        if (decision.code() != io.rowweave.run.BatchSinkAbortDecision.Code.RECORDED
                && decision.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code.ALREADY_RECORDED) {
            if (decision.code()
                    == io.rowweave.run.BatchSinkAbortDecision.Code.RECOVERY_OWNED
                    || decision.code()
                    == io.rowweave.run.BatchSinkAbortDecision.Code.STALE_RUN
                    || decision.code()
                    == io.rowweave.run.BatchSinkAbortDecision.Code.UNAVAILABLE) {
                throw reconciliationUnavailable(evidence,
                        "manifest abort recovery owner changed", null);
            }
            throw new IllegalStateException(
                    "recovered manifest abort rejected: " + decision.code());
        }
        return decision.record();
    }

    private io.rowweave.run.BatchSinkAbortRecord completeRecoveredManifestAbort(
            RecoveryToken token, RunSnapshot snapshot, PlanGraph graph,
            StageAssignmentSnapshot evidence,
            io.rowweave.run.BatchSinkAbortRecord abort) {
        if (abort.state() == io.rowweave.run.BatchSinkAbortRecord.State.ABORTED) {
            return abort;
        }
        boolean qualityBounded = evidence.manifest().hasQualityGateBinding();
        io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                qualityBounded ? stageAssignmentAuthority.qualityGateCommitJournal(
                        evidence.manifest().fence()) : null;
        ManifestCommitJournalSnapshot journal = qualityBounded ? null
                : stageAssignmentAuthority.manifestCommitJournal(
                        evidence.manifest().fence());
        if (qualityBounded
                ? qualityJournal == null || !qualityJournal.fence().equals(
                evidence.manifest().fence())
                : journal == null
                || !journal.assignments().manifest().fence().equals(
                evidence.manifest().fence())) {
            throw new IllegalStateException(
                    "manifest commit journal identity mismatch before retry abort");
        }
        if (qualityBounded
                ? qualityJournal.intent() != null : journal.intent() != null) {
            throw new IllegalStateException(
                    "durable manifest commit is already decided; retry is forbidden");
        }
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        io.rowweave.commit.ManifestFileStore store =
                new io.rowweave.commit.ManifestFileStore(
                        java.nio.file.Paths.get(output.domainPath()));
        if (!store.domainId().equals(abort.commitDomainId())
                || abort.assignmentGeneration()
                != evidence.manifest().generation()
                || !abort.publicationId().equals(
                snapshot.currentAttempt().publicationId())) {
            throw new IllegalStateException(
                    "manifest abort identity mismatch before retry");
        }
        if (store.exact(abort.generation()) != null) {
            throw new IllegalStateException(
                    "manifest is already visible; retry is forbidden");
        }
        rollbackRecoveredManifestOutputs(
                token, snapshot, evidence,
                "manifest:" + snapshot.currentAttempt().publicationId());
        io.rowweave.run.BatchSinkAbortDecision completion =
                ((RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority)
                        .recordBatchSinkAbortRecovered(
                                token, abort.completed(abort.decidedAtMillis()));
        if (completion.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code.RECORDED
                && completion.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code.ALREADY_RECORDED) {
            if (completion.code()
                    == io.rowweave.run.BatchSinkAbortDecision.Code.RECOVERY_OWNED
                    || completion.code()
                    == io.rowweave.run.BatchSinkAbortDecision.Code.STALE_RUN
                    || completion.code()
                    == io.rowweave.run.BatchSinkAbortDecision.Code.UNAVAILABLE) {
                throw reconciliationUnavailable(evidence,
                        "manifest abort completion owner changed", null);
            }
            throw new IllegalStateException(
                    "recovered manifest abort completion rejected: "
                            + completion.code());
        }
        return completion.record();
    }

    private void rollbackRecoveredManifestOutputs(
            RecoveryToken token, RunSnapshot snapshot,
            StageAssignmentSnapshot evidence, String groupId) {
        MasterElection election = leadership;
        if (election == null || election.termToken() == 0L
                || !recoveryOwnership.current(token)) {
            throw reconciliationUnavailable(evidence,
                    "manifest rollback leader ownership unavailable", null);
        }
        long deadline = deadlineAfterMillis(outputCommitTimeoutMs);
        for (int i = evidence.manifest().assignmentCount() - 1; i >= 0; i--) {
            io.rowweave.run.StageAssignmentRecord record =
                    evidence.manifest().assignment(i);
            StageAssignmentAcceptance acceptance = evidence.acceptance(i);
            if (!isManifestOutputRole(record.role()) || acceptance == null) {
                continue;
            }
            RegistryWorker current = endpointOf(record.workerId());
            if (current == null
                    || !record.host().equals(current.host())
                    || record.rpcPort() != current.port()
                    || !record.workerIncarnationId().equals(
                    current.incarnationId())) {
                continue;
            }
            StageOwner owner = new StageOwner(
                    record.workerId(), current, evidence.manifest().fence(),
                    acceptance.token(), acceptance.commandDigest(), token);
            RpcMessage rollback = RpcMessage.rollbackPreparedOutput(
                    record.workerId(), record.runtimeStageId(), groupId,
                    election.self(), election.termToken(),
                    record.workerIncarnationId(), evidence.manifest().fence());
            try {
                callManifestControl(owner, record.runtimeStageId(), rollback,
                        RpcMessage.Type.ACK, deadline, response -> null);
            } catch (RuntimeException unavailable) {
                throw reconciliationUnavailable(evidence,
                        "live manifest output rollback failed for stage "
                                + record.runtimeStageId(), unavailable);
            }
        }
    }

    private static byte[] recoveredSinkGroupIdentity(
            io.rowweave.run.StageAssignmentManifest manifest, PlanGraph graph) {
        try {
            java.security.MessageDigest digest =
                    java.security.MessageDigest.getInstance("SHA-256");
            boolean found = false;
            for (io.rowweave.run.StageAssignmentRecord record : manifest.assignments()) {
                if (record.role() == io.rowweave.run.StageAssignmentRole.SINK
                        || record.role()
                        == io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH) {
                    digest.update(DurableStageAssignmentPlan
                            .sinkAttemptIdentity(manifest, record, graph)
                            .identityDigest());
                    found = true;
                }
            }
            if (!found) {
                throw new IllegalStateException(
                        "manifest recovery assignment has no Sink identity");
            }
            return digest.digest();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private ManifestCommitJournalSnapshot recoverMissingManifestMembers(
            RecoveryToken token, RunSnapshot snapshot, PlanGraph graph,
            StageAssignmentSnapshot evidence, ManifestCommitJournalSnapshot journal) {
        if (journal.allExpectedMembersSealed() || journal.intent() != null) return journal;
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        java.nio.file.Path domain = java.nio.file.Paths.get(output.domainPath());
        Long generation = manifestCommitAuthority.find(
                domain, snapshot.currentAttempt().publicationId());
        if (generation == null) return journal;
        String groupId = "manifest:" + snapshot.currentAttempt().publicationId();
        long deadline = deadlineAfterMillis(outputCommitTimeoutMs);
        RecoveryFencedStageAssignmentAuthority recoveryAuthority =
                (RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            if (journal.member(i) != null
                    || !isManifestOutputRole(
                    evidence.manifest().assignment(i).role())) continue;
            StageAssignmentAcceptance acceptance = evidence.acceptance(i);
            StageTerminalReceipt terminal = evidence.terminal(i);
            if (acceptance == null || terminal == null || !terminal.succeeded()) continue;
            io.rowweave.run.StageAssignmentRecord record = evidence.manifest().assignment(i);
            RegistryWorker current = endpointOf(record.workerId());
            if (current == null || !record.host().equals(current.host())
                    || record.rpcPort() != current.port()
                    || !record.workerIncarnationId().equals(current.incarnationId())) {
                continue;
            }
            MasterElection election = leadership;
            if (election == null || election.termToken() == 0L) {
                throw reconciliationUnavailable(evidence,
                        "manifest member recovery leader term unavailable", null);
            }
            StageOwner owner = new StageOwner(record.workerId(), current,
                    evidence.manifest().fence(), acceptance.token(),
                    acceptance.commandDigest(), token);
            RpcMessage install = RpcMessage.installManifestMember(
                    record.workerId(), record.runtimeStageId(), groupId,
                    snapshot.currentAttempt().publicationId(), generation,
                    election.self(), election.termToken(), record.workerIncarnationId(),
                    evidence.manifest().fence());
            io.rowweave.commit.CommitManifest.Member member = callManifestControl(
                    owner, record.runtimeStageId(), install,
                    RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, deadline,
                    response -> io.rowweave.commit.ManifestCommitWire.decodeMember(
                            response.commitPayload()));
            RpcMessage seal = RpcMessage.sealManifestMember(
                    record.workerId(), record.runtimeStageId(), groupId,
                    snapshot.currentAttempt().publicationId(), generation,
                    election.self(), election.termToken(), record.workerIncarnationId(),
                    evidence.manifest().fence());
            callManifestControl(owner, record.runtimeStageId(), seal,
                    RpcMessage.Type.ACK, deadline, response -> null);
            ManifestMemberReceipt receipt = ManifestMemberReceipt.sealed(
                    acceptance.token(), acceptance.commandDigest(), output.domainId(),
                    generation, snapshot.currentAttempt().publicationId(), member, 0L,
                    preparedSinkLocator(
                            DurableStageAssignmentPlan.sinkAttemptIdentity(
                                    evidence.manifest(), record, graph),
                            output.domainId(), member, 0L));
            ManifestMemberDecision decision = recoveryAuthority.recordManifestMemberRecovered(
                    token, receipt);
            if (decision.code() != ManifestMemberDecision.Code.RECORDED
                    && decision.code() != ManifestMemberDecision.Code.ALREADY_RECORDED) {
                if (decision.code() == ManifestMemberDecision.Code.UNAVAILABLE) {
                    throw reconciliationUnavailable(evidence,
                            "manifest member journal unavailable", null);
                }
                throw new IllegalStateException(
                        "manifest member journal rejected: " + decision.code());
            }
            journal = journal.assignments().manifest().hasQualityGateBinding()
                    ? journal.withMember(decision.receipt())
                    : stageAssignmentAuthority.manifestCommitJournal(
                    evidence.manifest().fence());
            if (journal == null) {
                throw new IllegalStateException(
                        "manifest journal disappeared after member receipt");
            }
        }
        validateRecoveredManifestMembers(graph, journal);
        return journal;
    }

    private io.rowweave.run.QualityGateCommitJournalSnapshot
    recoverMissingQualityManifestMembers(
            RecoveryToken token, RunSnapshot snapshot, PlanGraph graph,
            StageAssignmentSnapshot evidence,
            io.rowweave.run.QualityGateCommitJournalSnapshot journal) {
        if (journal.allMembersSealed() || journal.intent() != null) return journal;
        io.rowweave.ManifestFileSink output = singleManifestOutput(graph);
        java.nio.file.Path domain = java.nio.file.Paths.get(output.domainPath());
        Long generation = manifestCommitAuthority.find(
                domain, snapshot.currentAttempt().publicationId());
        if (generation == null) return journal;
        String groupId = "manifest:" + snapshot.currentAttempt().publicationId();
        long deadline = deadlineAfterMillis(outputCommitTimeoutMs);
        RecoveryFencedStageAssignmentAuthority recoveryAuthority =
                (RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority;
        for (QualityGateAssignmentBinding.MemberBinding binding
                : journal.binding().members()) {
            int ordinal = binding.physicalOrdinal();
            if (journal.memberForOrdinal(ordinal) != null) continue;
            StageAssignmentAcceptance acceptance = evidence.acceptance(ordinal);
            StageTerminalReceipt terminal = evidence.terminal(ordinal);
            if (acceptance == null || terminal == null || !terminal.succeeded()) continue;
            io.rowweave.run.StageAssignmentRecord record =
                    evidence.manifest().assignment(ordinal);
            RegistryWorker current = endpointOf(record.workerId());
            if (current == null || !record.host().equals(current.host())
                    || record.rpcPort() != current.port()
                    || !record.workerIncarnationId().equals(current.incarnationId())) {
                continue;
            }
            MasterElection election = leadership;
            if (election == null || election.termToken() == 0L) {
                throw reconciliationUnavailable(evidence,
                        "quality member recovery leader term unavailable", null);
            }
            StageOwner owner = new StageOwner(record.workerId(), current,
                    evidence.manifest().fence(), acceptance.token(),
                    acceptance.commandDigest(), token);
            RpcMessage install = RpcMessage.installManifestMember(
                    record.workerId(), record.runtimeStageId(), groupId,
                    snapshot.currentAttempt().publicationId(), generation,
                    election.self(), election.termToken(),
                    record.workerIncarnationId(), evidence.manifest().fence());
            io.rowweave.commit.CommitManifest.Member member = callManifestControl(
                    owner, record.runtimeStageId(), install,
                    RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, deadline,
                    response -> io.rowweave.commit.ManifestCommitWire.decodeMember(
                            response.commitPayload()));
            RpcMessage seal = RpcMessage.sealManifestMember(
                    record.workerId(), record.runtimeStageId(), groupId,
                    snapshot.currentAttempt().publicationId(), generation,
                    election.self(), election.termToken(),
                    record.workerIncarnationId(), evidence.manifest().fence());
            callManifestControl(owner, record.runtimeStageId(), seal,
                    RpcMessage.Type.ACK, deadline, response -> null);
            ManifestMemberReceipt receipt = ManifestMemberReceipt.sealed(
                    acceptance.token(), acceptance.commandDigest(), output.domainId(),
                    generation, snapshot.currentAttempt().publicationId(), member, 0L,
                    preparedSinkLocator(
                            DurableStageAssignmentPlan.sinkAttemptIdentity(
                                    evidence.manifest(), record, graph),
                            output.domainId(), member, 0L));
            ManifestMemberDecision decision =
                    recoveryAuthority.recordManifestMemberRecovered(token, receipt);
            if (decision.code() != ManifestMemberDecision.Code.RECORDED
                    && decision.code()
                    != ManifestMemberDecision.Code.ALREADY_RECORDED) {
                if (decision.code() == ManifestMemberDecision.Code.UNAVAILABLE) {
                    throw reconciliationUnavailable(evidence,
                            "quality member journal unavailable", null);
                }
                throw new IllegalStateException(
                        "quality member journal rejected: " + decision.code());
            }
            journal = stageAssignmentAuthority.qualityGateCommitJournal(
                    evidence.manifest().fence());
            if (journal == null) {
                throw new IllegalStateException(
                        "quality journal disappeared after member receipt");
            }
        }
        validateRecoveredQualityManifestMembers(graph, journal);
        return journal;
    }

    private static void validateRecoveredQualityManifestMembers(
            PlanGraph graph,
            io.rowweave.run.QualityGateCommitJournalSnapshot journal) {
        if (!journal.allMembersSealed()) return;
        java.util.Map<String, String> expected = new java.util.LinkedHashMap<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof io.rowweave.ManifestFileSink) {
                io.rowweave.ManifestFileSink output =
                        (io.rowweave.ManifestFileSink) node.output();
                expected.put(output.memberName(),
                        io.rowweave.commit.SchemaFingerprint.of(output.getSchema()));
            }
        }
        java.util.Map<String, String> actual = new java.util.LinkedHashMap<>();
        for (ManifestMemberReceipt receipt : journal.members()) {
            actual.put(receipt.member().name(),
                    receipt.member().schemaFingerprint());
        }
        if (!expected.equals(actual)) {
            throw new IllegalStateException(
                    "recovered quality member set/schema does not match PlanGraph");
        }
    }

    private static boolean isManifestOutputRole(
            io.rowweave.run.StageAssignmentRole role) {
        return role == io.rowweave.run.StageAssignmentRole.SINK
                || role == io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH;
    }

    private static void validateRecoveredManifestMembers(
            PlanGraph graph, ManifestCommitJournalSnapshot journal) {
        java.util.Map<String, String> expected = new java.util.LinkedHashMap<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof io.rowweave.ManifestFileSink) {
                io.rowweave.ManifestFileSink output =
                        (io.rowweave.ManifestFileSink) node.output();
                expected.put(output.memberName(),
                        io.rowweave.commit.SchemaFingerprint.of(output.getSchema()));
            }
        }
        java.util.Map<String, String> actual = new java.util.LinkedHashMap<>();
        for (int i = 0; i < journal.assignments().manifest().assignmentCount(); i++) {
            ManifestMemberReceipt receipt = journal.member(i);
            if (receipt != null) {
                actual.put(receipt.member().name(), receipt.member().schemaFingerprint());
            }
        }
        if (journal.allExpectedMembersSealed() && !expected.equals(actual)) {
            throw new IllegalStateException(
                    "recovered manifest member set/schema does not match PlanGraph");
        }
    }

    private static io.rowweave.ManifestFileSink singleManifestOutput(PlanGraph graph) {
        io.rowweave.ManifestFileSink selected = null;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            if (!(node.output() instanceof io.rowweave.ManifestFileSink)) {
                throw new IllegalStateException(
                        "durable manifest recovery cannot mix output families");
            }
            io.rowweave.ManifestFileSink output =
                    (io.rowweave.ManifestFileSink) node.output();
            if (selected != null && !selected.domainId().equals(output.domainId())) {
                throw new IllegalStateException(
                        "durable manifest recovery spans multiple domains");
            }
            selected = output;
        }
        if (selected == null) {
            throw new IllegalStateException("manifest recovery has no ManifestFileSink");
        }
        return selected;
    }

    private StageAssignmentSnapshot reconcileDepartedAssignments(
            RecoveryToken recoveryToken, StageAssignmentSnapshot evidence,
            PlanGraph graph) {
        if (!(stageAssignmentAuthority instanceof RecoveryFencedStageAssignmentAuthority)) {
            throw new IllegalStateException(
                    "recovery-fenced stage assignment authority unavailable");
        }
        RecoveryFencedStageAssignmentAuthority recoveryAuthority =
                (RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority;
        boolean changed = false;
        boolean providerSink = containsProviderSinkOutput(graph);
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            StageAssignmentAcceptance acceptance = evidence.acceptance(i);
            io.rowweave.run.StageAssignmentRecord record =
                    evidence.manifest().assignment(i);
            StageTerminalReceipt recordedTerminal = evidence.terminal(i);
            boolean terminalWithoutPrepared = providerSink
                    && acceptance != null
                    && recordedTerminal != null
                    && recordedTerminal.succeeded()
                    && evidence.preparedSink(i) == null
                    && (record.role()
                    == io.rowweave.run.StageAssignmentRole.SINK
                    || record.role()
                    == io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH);
            if (acceptance == null
                    || recordedTerminal != null && !terminalWithoutPrepared) {
                continue;
            }
            RegistryWorker current = endpointOf(record.workerId());
            boolean exactWorker = current != null
                    && record.host().equals(current.host())
                    && record.rpcPort() == current.port()
                    && record.workerIncarnationId().equals(current.incarnationId());
            if (exactWorker) {
                RpcMessage response;
                try {
                    MasterElection election = leadership;
                    if (election == null || election.termToken() == 0L) {
                        throw new IllegalStateException("active recovery leader term is unavailable");
                    }
                    RpcMessage request = RpcMessage.reconcileDurableStage(
                            record.workerId(),
                            StageAssignmentManifestCodec.encodeToken(acceptance.token()),
                            acceptance.commandDigest(), election.self(), election.termToken(),
                            record.workerIncarnationId(), evidence.manifest().fence());
                    response = secureInbound(rpcCache.call(
                            record.host(), record.rpcPort(), secureOutbound(request)));
                } catch (RuntimeException unavailable) {
                    if (terminalWithoutPrepared) {
                        throw preparedLocatorRecoveryFailed(evidence,
                                "exact Worker locator replay RPC failed",
                                unavailable);
                    }
                    throw reconciliationUnavailable(evidence,
                            "exact Worker reconciliation RPC failed", unavailable);
                }
                if (response != null
                        && response.type() == RpcMessage.Type.DURABLE_STAGE_RECONCILED
                        && "TERMINAL".equals(response.reconciliationOutcome())) {
                    StageTerminalReceipt terminal;
                    try {
                        terminal = StageAssignmentManifestCodec.decodeTerminalReceipt(
                                response.reconciledTerminalReceiptBytes());
                    } catch (RuntimeException malformed) {
                        throw new IllegalStateException(
                                "Worker returned malformed terminal reconciliation", malformed);
                    }
                    if (!terminal.token().equals(acceptance.token())
                            || !java.security.MessageDigest.isEqual(
                            terminal.commandDigest(), acceptance.commandDigest())) {
                        throw new IllegalStateException(
                                "Worker terminal reconciliation identity mismatch");
                    }
                    byte[] preparedBytes = response.preparedSinkReceiptBytes();
                    if (terminalWithoutPrepared) {
                        if (preparedBytes == null) {
                            throw preparedLocatorRecoveryFailed(evidence,
                                    "terminal-without-prepared Worker replay omitted locator", null);
                        }
                        PreparedSinkReceipt prepared;
                        try {
                            prepared = PreparedSinkReceiptCodec.decode(preparedBytes);
                        } catch (RuntimeException malformed) {
                            throw new IllegalStateException(
                                    "Worker returned malformed prepared Sink reconciliation",
                                    malformed);
                        }
                        if (!prepared.token().equals(acceptance.token())
                                || !java.security.MessageDigest.isEqual(
                                prepared.commandDigest(), acceptance.commandDigest())) {
                            throw new IllegalStateException(
                                    "Worker prepared Sink reconciliation identity mismatch");
                        }
                        PreparedSinkDecision preparedDecision =
                                recoveryAuthority.recordPreparedSinkRecovered(
                                        recoveryToken, prepared);
                        if (preparedDecision.code()
                                != PreparedSinkDecision.Code.RECORDED
                                && preparedDecision.code()
                                != PreparedSinkDecision.Code.ALREADY_RECORDED) {
                            throw preparedLocatorRecoveryFailed(evidence,
                                    "prepared Sink locator replay rejected: "
                                            + preparedDecision.code(), null);
                        }
                    }
                    terminal = terminal.withTelemetry(decodeStageTelemetry(response));
                    changed = true;
                    continue;
                }
                if (response != null
                        && response.type() == RpcMessage.Type.DURABLE_STAGE_RECONCILED
                        && "ABSENT".equals(response.reconciliationOutcome())) {
                    RegistryWorker after = endpointOf(record.workerId());
                    if (!isActiveLeader() || after == null || !sameEndpoint(current, after)) {
                        if (terminalWithoutPrepared) {
                            throw preparedLocatorRecoveryFailed(evidence,
                                    "Worker identity changed during locator replay", null);
                        }
                        throw reconciliationUnavailable(evidence,
                                "Worker identity changed during ABSENT reconciliation", null);
                    }
                    if (terminalWithoutPrepared) {
                        throw preparedLocatorRecoveryFailed(evidence,
                                "exact Worker no longer retains prepared locator", null);
                    }
                    recordLost(recoveryAuthority, recoveryToken, evidence,
                            acceptance, record);
                    changed = true;
                    continue;
                }
                if (terminalWithoutPrepared) {
                    throw preparedLocatorRecoveryFailed(evidence,
                            "exact Worker locator replay is not terminal", null);
                }
                throw reconciliationUnavailable(evidence,
                        "exact Worker reconciliation is not terminal: type="
                                + (response == null ? "null" : response.type())
                                + " outcome=" + (response == null ? null
                                : response.reconciliationOutcome())
                                + " error=" + (response == null ? null
                                : response.errorMessage()), null);
            }
            if (terminalWithoutPrepared) {
                throw preparedLocatorRecoveryFailed(evidence,
                        "exact Worker is unavailable for prepared locator replay", null);
            }
            recordLost(recoveryAuthority, recoveryToken, evidence, acceptance, record);
            changed = true;
        }
        if (!changed) return evidence;
        StageAssignmentSnapshot refreshed = stageAssignmentAuthority.assignmentSnapshot(
                evidence.manifest().fence());
        if (refreshed == null) {
            throw new IllegalStateException(
                    "assignment evidence disappeared after terminal reconciliation");
        }
        return refreshed;
    }

    private void recordLost(RecoveryFencedStageAssignmentAuthority recoveryAuthority,
                            RecoveryToken recoveryToken,
                            StageAssignmentSnapshot evidence,
                            StageAssignmentAcceptance acceptance,
                            io.rowweave.run.StageAssignmentRecord record) {
        byte[] failureDigest = sha256(("worker-lost\0" + record.workerId()
                + '\0' + record.workerIncarnationId())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StageTerminalReceipt lost = StageTerminalReceipt.lost(
                acceptance.token(), acceptance.commandDigest(),
                "WORKER_LOST", failureDigest, 0L);
        StageTerminalDecision decision = recoveryAuthority.recordTerminalRecovered(
                recoveryToken, lost);
        if (decision.code() != StageTerminalDecision.Code.RECORDED
                && decision.code() != StageTerminalDecision.Code.ALREADY_RECORDED) {
            if (decision.code() == StageTerminalDecision.Code.UNAVAILABLE
                    || decision.code() == StageTerminalDecision.Code.RECOVERY_OWNED
                    || decision.code() == StageTerminalDecision.Code.STALE_RUN) {
                throw new io.rowweave.run.RunStateStoreUnavailableException(
                        "record-lost-" + evidence.manifest().fence().runId(),
                        "terminal authority or recovery ownership unavailable "
                                + "while recording LOST", null);
            }
            throw new IllegalStateException(
                    "LOST terminal reconciliation rejected: " + decision.code());
        }
    }

    private static io.rowweave.run.RunStateStoreUnavailableException reconciliationUnavailable(
            StageAssignmentSnapshot evidence, String message, Throwable cause) {
        return new io.rowweave.run.RunStateStoreUnavailableException(
                "reconcile-stage-" + evidence.manifest().fence().runId(), message, cause);
    }

    private static PreparedLocatorRecoveryFailedException
    preparedLocatorRecoveryFailed(
            StageAssignmentSnapshot evidence, String message, Throwable cause) {
        return new PreparedLocatorRecoveryFailedException(evidence, message, cause);
    }

    /** Exact locator replay exhausted its one bounded Worker authority path. */
    private static final class PreparedLocatorRecoveryFailedException
            extends IllegalStateException {
        private final StageAssignmentSnapshot evidence;

        private PreparedLocatorRecoveryFailedException(
                StageAssignmentSnapshot evidence, String message,
                Throwable cause) {
            super(message, cause);
            this.evidence = evidence;
        }

        private StageAssignmentSnapshot evidence() { return evidence; }
    }

    private RunSnapshot succeedRecoveredAttempt(RecoveryToken token, RunSnapshot snapshot,
                                                 io.rowweave.run.RunResultSummary summary) {
        AttemptStatus status = snapshot.currentAttempt().status();
        if (status == AttemptStatus.DISPATCHING) {
            snapshot = advanceRecovered(token, snapshot, AttemptStatus.RUNNING,
                    "evidence-running", null);
            status = AttemptStatus.RUNNING;
        }
        if (status == AttemptStatus.RUNNING) {
            snapshot = advanceRecovered(token, snapshot, AttemptStatus.COMMITTING,
                    "evidence-committing", null);
            status = AttemptStatus.COMMITTING;
        }
        if (status == AttemptStatus.COMMITTING) {
            snapshot = advanceRecovered(token, snapshot, AttemptStatus.SUCCEEDED,
                    "evidence-succeeded", null, summary);
        }
        return snapshot;
    }

    private RunSnapshot failRecoveredAttempt(RecoveryToken token, RunSnapshot snapshot,
                                             String reason) {
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (attempt.status() == AttemptStatus.COMMITTING) {
            RunDecision reconciling = applyRecovered(token, RunCommand.requestCancellation(
                    InheritedRunRecovery.inFlightCommandId(snapshot, "failure-reconciling"),
                    snapshot.runId(), snapshot.revision(), attempt.attemptId(),
                    attempt.fenceToken(), CancellationCause.FENCE,
                    snapshot.updatedAtMillis(), reason,
                    "durable terminal evidence requires failure"));
            snapshot = acceptedRecoveryMutation(reconciling,
                    "enter failed commit reconciliation");
        }
        return advanceRecovered(token, snapshot, AttemptStatus.FAILED,
                "evidence-failed", reason);
    }

    private RunSnapshot advanceRecovered(RecoveryToken token, RunSnapshot snapshot,
                                         AttemptStatus target, String phase,
                                         String reason) {
        return advanceRecovered(token, snapshot, target, phase, reason, null);
    }

    private RunSnapshot advanceRecovered(RecoveryToken token, RunSnapshot snapshot,
                                         AttemptStatus target, String phase,
                                         String reason,
                                         io.rowweave.run.RunResultSummary summary) {
        AttemptSnapshot attempt = snapshot.currentAttempt();
        RunDecision decision = applyRecovered(token, RunCommand.advanceAttempt(
                InheritedRunRecovery.inFlightCommandId(snapshot, phase),
                snapshot.runId(), snapshot.revision(), attempt.attemptId(),
                attempt.fenceToken(), target, false, snapshot.updatedAtMillis(),
                reason, reason == null ? null : "durable recovery evidence", summary));
        if (decision.code() != RunDecision.Code.ACCEPTED
                && decision.code() != RunDecision.Code.ALREADY_APPLIED) {
            RunSnapshot current = runStateStore.get(snapshot.runId());
            if (isSameUserCancellation(snapshot, current)) {
                throw new UserCancellationWonRecoveryRace();
            }
            if (isSameTimeoutCancellation(snapshot, current)) {
                throw new TimeoutWonRecoveryRace();
            }
        }
        return acceptedRecoveryMutation(decision, "advance recovered in-flight attempt");
    }

    private static final class UserCancellationWonRecoveryRace
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class TimeoutWonRecoveryRace extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private RunDecision applyRecovered(RecoveryToken token, RunCommand command) {
        return ((RecoveryFencedRunStateStore) runStateStore)
                .applyRecovered(token, command);
    }

    private static RunSnapshot acceptedRecoveryMutation(
            RunDecision decision, String operation) {
        if (decision.code() != RunDecision.Code.ACCEPTED
                && decision.code() != RunDecision.Code.ALREADY_APPLIED) {
            throw new IllegalStateException(operation + " rejected: " + decision.code());
        }
        return decision.snapshot();
    }

    private static void requireRecoveryAttemptIdentity(
            RunSnapshot snapshot, InheritedRunRecovery.Identity identity,
            AttemptStatus expected, String operation) {
        AttemptSnapshot attempt = snapshot == null ? null : snapshot.currentAttempt();
        if (snapshot == null || !identity.runId().equals(snapshot.runId())
                || attempt == null || !identity.attemptId().equals(attempt.attemptId())
                || !identity.publicationId().equals(attempt.publicationId())
                || identity.fenceToken() != attempt.fenceToken()
                || attempt.status() != expected) {
            throw new IllegalStateException(operation + " snapshot mismatch");
        }
    }

    private final class DurableRunExecution {
        final String runId;
        final String attemptId;
        final String publicationId;
        final String commandSeed;
        final long eventTimeMillis;
        final RecoveryToken recoveryToken;
        final io.rowweave.incremental.IncrementalAuthorityGateway incrementalAuthority;
        final io.rowweave.incremental.FrontierRecoveryToken frontierToken;
        final io.rowweave.incremental.IncrementalClaimRequest incrementalRequest;
        final InheritedRunRecovery.Identity recoveryIdentity;
        final long fenceToken;
        RunSnapshot snapshot;
        boolean running;
        int sinkLocatorClosureCount;
        DurableStageAssignmentPlan assignmentPlan;
        io.rowweave.cluster.rpc.ControlResidentLease residentLease;
        io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken residentOwner;
        long deadlineWakeupNanos = Long.MIN_VALUE;

        DurableRunExecution(RunSubmission submission, String commandSeed, RunSnapshot snapshot) {
            this.runId = submission.runId();
            this.attemptId = submission.attemptId();
            this.publicationId = submission.publicationId();
            this.commandSeed = commandSeed;
            this.eventTimeMillis = submission.submittedAtMillis();
            this.recoveryToken = null;
            this.incrementalAuthority = null;
            this.frontierToken = null;
            this.incrementalRequest = null;
            this.recoveryIdentity = null;
            this.snapshot = snapshot;
            this.fenceToken = snapshot.currentAttempt().fenceToken();
        }

        DurableRunExecution(InheritedRunRecovery.Identity identity,
                            RecoveryToken recoveryToken, RunSnapshot snapshot) {
            this.runId = identity.runId();
            this.attemptId = identity.attemptId();
            this.publicationId = identity.publicationId();
            this.commandSeed = null;
            this.eventTimeMillis = identity.eventTimeMillis();
            this.recoveryToken = recoveryToken;
            this.incrementalAuthority = null;
            this.frontierToken = null;
            this.incrementalRequest = null;
            this.recoveryIdentity = identity;
            this.snapshot = snapshot;
            this.fenceToken = snapshot.currentAttempt().fenceToken();
        }

        DurableRunExecution(IncrementalExecutionClaim claim) {
            this.runId = claim.request().runId();
            this.attemptId = claim.request().attemptId();
            this.publicationId = claim.request().publicationId();
            this.commandSeed = claim.request().intervalId();
            this.eventTimeMillis = claim.request().eventTimeMillis();
            this.recoveryToken = claim.runToken();
            this.incrementalAuthority = incrementalAuthorityGateway;
            this.frontierToken = claim.frontierToken();
            this.incrementalRequest = claim.request();
            this.recoveryIdentity = null;
            this.snapshot = claim.runSnapshot();
            this.fenceToken = claim.request().firstAttemptFence().fenceToken();
        }

        DurableRunExecution(
                io.rowweave.incremental.IncrementalAuthorityRecord record,
                io.rowweave.incremental.IncrementalActiveInterval active,
                io.rowweave.incremental.FrontierRecoveryToken frontierToken,
                RecoveryToken runToken, RunSnapshot snapshot) {
            AttemptSnapshot attempt = snapshot.currentAttempt();
            this.runId = snapshot.runId();
            this.attemptId = attempt.attemptId();
            this.publicationId = attempt.publicationId();
            this.commandSeed = active.identity().intervalId();
            this.eventTimeMillis = snapshot.createdAtMillis();
            this.recoveryToken = runToken;
            this.incrementalAuthority = incrementalAuthorityGateway;
            this.frontierToken = frontierToken;
            this.incrementalRequest = new io.rowweave.incremental
                    .IncrementalClaimRequest(
                    record.feedId(), active.capturedTarget(),
                    record.frontierContractDigest(), active.artifactId(),
                    attempt.attemptId(), attempt.publicationId(),
                    snapshot.maxAttempts(), snapshot.deadlineEpochMillis(),
                    snapshot.createdAtMillis());
            this.recoveryIdentity = null;
            this.snapshot = snapshot;
            this.fenceToken = active.fence().fenceToken();
        }

        boolean incremental() { return incrementalAuthority != null; }

        String jobId() { return runId; }

        RunAttemptFence fence() {
            return new RunAttemptFence(runId, attemptId, fenceToken);
        }

        void takeResidentLease(
                io.rowweave.cluster.rpc.ControlResidentLease lease,
                io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken owner) {
            if ((lease == null) != (owner == null) || residentLease != null) {
                throw new IllegalStateException("invalid durable run resident handoff");
            }
            residentLease = lease;
            residentOwner = owner;
        }

        void releaseResidentLease() {
            io.rowweave.cluster.rpc.ControlResidentLease lease = residentLease;
            io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken owner = residentOwner;
            residentLease = null;
            residentOwner = null;
            if (lease != null) lease.release(owner);
        }

        void publishAssignments(DurableStageAssignmentPlan planned) {
            StageManifestPublication publication;
            if (incremental()) {
                publication = incrementalAuthority.publishAssignments(
                        frontierToken, recoveryToken, planned.manifest());
            } else if (recoveryToken == null) {
                publication = stageAssignmentAuthority.publish(planned.manifest());
            } else {
                if (!(stageAssignmentAuthority
                        instanceof RecoveryFencedStageAssignmentAuthority)) {
                    throw new IllegalStateException(
                            "recovery-fenced stage assignment authority unavailable");
                }
                publication = ((RecoveryFencedStageAssignmentAuthority)
                        stageAssignmentAuthority).publishRecovered(
                        recoveryToken, planned.manifest());
            }
            if (publication.code() != StageManifestPublication.Code.PUBLISHED
                    && publication.code()
                    != StageManifestPublication.Code.ALREADY_PUBLISHED) {
                throw new IllegalStateException("stage assignment manifest rejected: "
                        + publication.code());
            }
            assignmentPlan = planned.withPublishedSnapshot(publication.snapshot());
        }

        ManifestMemberDecision recordManifestMember(ManifestMemberReceipt receipt) {
            ManifestMemberDecision decision;
            if (incremental()) {
                decision = incrementalAuthority.recordManifestMember(
                        frontierToken, recoveryToken, receipt);
            } else if (recoveryToken == null) {
                decision = stageAssignmentAuthority.recordManifestMember(receipt);
            } else {
                if (!(stageAssignmentAuthority
                        instanceof RecoveryFencedStageAssignmentAuthority)) {
                    throw new IllegalStateException(
                            "recovery-fenced manifest authority unavailable");
                }
                decision = ((RecoveryFencedStageAssignmentAuthority)
                        stageAssignmentAuthority).recordManifestMemberRecovered(
                        recoveryToken, receipt);
            }
            if (decision.code() == ManifestMemberDecision.Code.RECORDED
                    || decision.code()
                    == ManifestMemberDecision.Code.ALREADY_RECORDED) {
                sinkLocatorClosureCount++;
            }
            return decision;
        }

        ManifestIntentDecision commitManifestIntent(ManifestCommitIntent intent) {
            if (incremental()) {
                return incrementalAuthority.commitManifestIntent(
                        frontierToken, recoveryToken, intent);
            }
            if (recoveryToken == null) {
                return stageAssignmentAuthority.commitManifestIntent(intent);
            }
            if (!(stageAssignmentAuthority
                    instanceof RecoveryFencedStageAssignmentAuthority)) {
                throw new IllegalStateException(
                        "recovery-fenced manifest authority unavailable");
            }
            return ((RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority)
                    .commitManifestIntentRecovered(recoveryToken, intent);
        }

        StageAcceptanceDecision acceptAssignment(
                StageAssignmentAcceptance acceptance) {
            if (!incremental()) {
                throw new IllegalStateException(
                        "joint assignment acceptance requires incremental authority");
            }
            return incrementalAuthority.acceptAssignment(
                    frontierToken, recoveryToken, acceptance);
        }

        StageTerminalDecision recordStageTerminal(
                StageTerminalReceipt receipt) {
            if (!incremental()) {
                throw new IllegalStateException(
                        "joint terminal evidence requires incremental authority");
            }
            return incrementalAuthority.recordStageTerminal(
                    frontierToken, recoveryToken, receipt);
        }

        PreparedSinkDecision recordPreparedSink(
                PreparedSinkReceipt receipt) {
            if (!incremental()) {
                throw new IllegalStateException(
                        "joint prepared evidence requires incremental authority");
            }
            return incrementalAuthority.recordPreparedSink(
                    frontierToken, recoveryToken, receipt);
        }

        io.rowweave.QualityGateDecisionResult recordQualityGateDecision(
                io.rowweave.QualityGateDecision decision) {
            try {
                if (recoveryToken == null) {
                    return stageAssignmentAuthority
                            .recordQualityGateDecision(decision);
                }
                if (!(stageAssignmentAuthority
                        instanceof RecoveryFencedStageAssignmentAuthority)) {
                    throw new IllegalStateException(
                            "recovery-fenced quality authority unavailable");
                }
                return ((RecoveryFencedStageAssignmentAuthority)
                        stageAssignmentAuthority)
                        .recordQualityGateDecisionRecovered(
                                recoveryToken, decision);
            } catch (RuntimeException uncertain) {
                io.rowweave.QualityGateDecision observed;
                try {
                    observed = stageAssignmentAuthority
                            .qualityGateDecision(fence());
                } catch (RuntimeException readFailure) {
                    readFailure.addSuppressed(uncertain);
                    throw readFailure;
                }
                if (decision.equals(observed)) {
                    return io.rowweave.QualityGateDecisionResult
                            .alreadyRecorded(observed);
                }
                throw uncertain;
            }
        }

        io.rowweave.run.BatchSinkAbortDecision recordBatchSinkAbort(
                io.rowweave.run.BatchSinkAbortRecord record) {
            if (incremental()) {
                return incrementalAuthority.recordBatchSinkAbort(
                        frontierToken, recoveryToken, record);
            }
            if (recoveryToken == null) {
                return stageAssignmentAuthority.recordBatchSinkAbort(record);
            }
            if (!(stageAssignmentAuthority
                    instanceof RecoveryFencedStageAssignmentAuthority)) {
                throw new IllegalStateException(
                        "recovery-fenced manifest authority unavailable");
            }
            return ((RecoveryFencedStageAssignmentAuthority) stageAssignmentAuthority)
                    .recordBatchSinkAbortRecovered(recoveryToken, record);
        }

        void markDispatchingIfIncremental() {
            if (incremental()) {
                snapshot = advanceIncremental(
                        AttemptStatus.DISPATCHING, "cluster-dispatch");
                requireAttemptStatus(AttemptStatus.DISPATCHING,
                        "mark dispatching");
            }
        }

        void markRunning() {
            if (deadlineTimeoutRequested(true)) {
                throw new IllegalStateException(
                        "run deadline reached before RUNNING");
            }
            // When the outcome of writing to the authority cannot be confirmed, nothing may follow it by writing
            // FAILED from a DISPATCHING snapshot taken before.
            running = true;
            if (incremental()) {
                snapshot = advanceIncremental(
                        AttemptStatus.RUNNING, "cluster-running");
                return;
            }
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("running"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.RUNNING, false, eventTimeMillis, null, null),
                    AttemptStatus.RUNNING, null, "mark running");
        }

        void markCommitting() {
            if (deadlineTimeoutRequested(true)) {
                throw new IllegalStateException(
                        "run deadline reached before COMMITTING");
            }
            if (incremental()) {
                snapshot = advanceIncremental(
                        AttemptStatus.COMMITTING, "cluster-committing");
                requireAttemptStatus(AttemptStatus.COMMITTING,
                        "mark committing");
                return;
            }
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("committing"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.COMMITTING, false, eventTimeMillis, null, null),
                    AttemptStatus.COMMITTING, null, "mark committing");
            requireAttemptStatus(AttemptStatus.COMMITTING, "mark committing");
        }

        void markSucceeded(
                io.rowweave.run.RunResultSummary summary,
                ManifestJobCommit manifestCommit) {
            if (incremental()) {
                if (manifestCommit == null || manifestCommit.store == null) {
                    throw new IllegalStateException(
                            "incremental success requires a Manifest publication");
                }
                io.rowweave.incremental.IncrementalAuthorityRecord record =
                        incrementalAuthority.record(
                                incrementalRequest.feedId());
                io.rowweave.incremental.IncrementalActiveInterval active =
                        record == null ? null : record.activeInterval();
                io.rowweave.incremental.VerifiedIncrementalJointCommitProof proof =
                        io.rowweave.incremental.IncrementalJointCommitProofs
                                .verify(active, incrementalAuthority,
                                        manifestCommit.store, frontierToken,
                                        recoveryToken);
                io.rowweave.incremental.IncrementalAuthorityCommandKind kind =
                        incrementalRequest.capturedTarget().mode()
                        == io.rowweave.incremental.IncrementalRunMode.BACKFILL_V1
                        ? io.rowweave.incremental
                        .IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                        : io.rowweave.incremental
                        .IncrementalAuthorityCommandKind.COMMIT;
                io.rowweave.incremental.IncrementalAuthorityCommand command =
                        new io.rowweave.incremental.IncrementalAuthorityCommand(
                                kind, incrementalRequest.feedId(),
                                "cluster-terminal-"
                                        + incrementalRequest.intervalId(),
                                record.revision(), record.feedIncarnation(),
                                incrementalRequest.intervalId(), active.fence(),
                                proof.proofDigest());
                io.rowweave.incremental.IncrementalAuthorityDecision decision =
                        incrementalAuthority.jointSuccess(
                                frontierToken, recoveryToken, command, proof);
                if (decision.code()
                        != io.rowweave.incremental.IncrementalAuthorityDecision
                        .Code.ACCEPTED
                        && decision.code()
                        != io.rowweave.incremental.IncrementalAuthorityDecision
                        .Code.ALREADY_APPLIED) {
                    throw new IllegalStateException(
                            "incremental joint success rejected: "
                                    + decision.code());
                }
                snapshot = decision.runSnapshot();
                requireAttemptStatus(
                        AttemptStatus.SUCCEEDED, "mark incremental succeeded");
                return;
            }
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("succeeded"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.SUCCEEDED, false, eventTimeMillis, null, null, summary),
                    AttemptStatus.SUCCEEDED, summary, "mark succeeded");
            requireAttemptStatus(AttemptStatus.SUCCEEDED, "mark succeeded");
        }

        StageAssignmentSnapshot assignmentSnapshot() {
            return incremental()
                    ? incrementalAuthority.assignmentSnapshot(fence())
                    : stageAssignmentAuthority.assignmentSnapshot(fence());
        }

        ManifestCommitJournalSnapshot manifestCommitJournal() {
            return incremental()
                    ? incrementalAuthority.manifestCommitJournal(fence())
                    : stageAssignmentAuthority.manifestCommitJournal(fence());
        }

        io.rowweave.run.BatchSinkAbortRecord batchSinkAbort() {
            return incremental()
                    ? incrementalAuthority.batchSinkAbort(fence())
                    : stageAssignmentAuthority.batchSinkAbort(fence());
        }

        io.rowweave.run.RunResultSummary resultSummary(ManifestJobCommit manifestCommit) {
            StageAssignmentSnapshot evidence = assignmentSnapshot();
            if (evidence == null) {
                throw new IllegalStateException("successful run lost assignment evidence");
            }
            if (manifestCommit == null) {
                return io.rowweave.run.RunResultSummary.discardFrom(evidence);
            }
            if (manifestCommit.genericProvider()) {
                io.rowweave.commit.BatchSinkOutcome outcome =
                        manifestCommit.providerOutcome;
                if (outcome == null
                        || outcome.state()
                        != io.rowweave.commit.BatchSinkOutcome.State.COMMITTED
                        || outcome.generation() == null
                        || outcome.identityDigest() == null) {
                    throw new IllegalStateException(
                            "successful provider Run lacks publication evidence");
                }
                int closure = 0;
                StageAssignmentSnapshot assignmentEvidence =
                        assignmentSnapshot();
                for (int i = 0; i < assignmentEvidence.manifest()
                        .assignmentCount(); i++) {
                    if (assignmentEvidence.preparedSink(i) != null) closure++;
                }
                sinkLocatorClosureCount = closure;
                return io.rowweave.run.RunResultSummary.manifestFrom(
                        evidence, outcome.generation(),
                        outcome.identityDigest(), outcome.identityDigest());
            }
            io.rowweave.commit.CommitManifest manifest =
                    manifestCommit.store.exact(manifestCommit.generation);
            if (manifest == null || !publicationId.equals(manifest.attemptId())) {
                throw new IllegalStateException(
                        "successful run lacks exact manifest publication evidence");
            }
            boolean qualityBounded = assignmentPlan != null
                    && assignmentPlan.manifest().hasQualityGateBinding();
            io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                    qualityBounded ? stageAssignmentAuthority
                            .qualityGateCommitJournal(fence()) : null;
            ManifestCommitJournalSnapshot journal = qualityBounded ? null
                    : manifestCommitJournal();
            byte[] sinkIdentity = qualityBounded
                    ? qualityJournal == null ? null
                    : sinkGroupIdentity(qualityJournal)
                    : journal == null ? null : sinkGroupIdentity(journal);
            if (sinkIdentity == null) {
                throw new IllegalStateException(
                        "successful manifest run lacks Sink group identity evidence");
            }
            sinkLocatorClosureCount = qualityBounded
                    ? qualityJournal.memberCount() : journal.memberCount();
            return io.rowweave.run.RunResultSummary.manifestFrom(
                    evidence, manifest.generation(), sha256(manifest.encode()),
                    sinkIdentity);
        }

        void failBeforeDispatch() {
            if (running || snapshot.currentAttempt().status() != AttemptStatus.DISPATCHING) {
                return;
            }
            if (incremental()) return;
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("pre-dispatch-failed"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.FAILED, false, eventTimeMillis,
                    "PRE_DISPATCH", null), AttemptStatus.FAILED, null,
                    "fail before dispatch");
        }

        boolean schedulePreDispatchRetry() {
            if (incremental()) {
                return true;
            }
            if (snapshot.maxAttempts() <= 1
                    || wholeAttemptRetryAuthority == null) {
                return false;
            }
            try {
                WholeAttemptRetryDecision decision = wholeAttemptRetryAuthority
                        .scheduleRetry(fence());
                retryRecoveryMetrics.record(decision);
                if (decision.snapshot() != null) {
                    snapshot = decision.snapshot();
                }
                switch (decision.code()) {
                    case SCHEDULED:
                    case ALREADY_SCHEDULED:
                    case EXHAUSTED:
                    case UNAVAILABLE:
                    case CLOSURE_PENDING:
                    case STALE_RUN:
                    case STALE_RECOVERY_OWNER:
                        return true;
                    default:
                        return false;
                }
            } catch (RuntimeException unavailable) {
                LOG.warn("whole-attempt retry scheduling unavailable run={} attempt={}",
                        runId, attemptId, unavailable);
                return true;
            }
        }

        /**
         * Running out of local resident memory is a deterministic fail-stop, not uncertainty about an assignment's outcome. Once the
         * attempt has reached RUNNING, the authority must be told FAILED, because leaving a budget refusal as RUNNING would strand a
         * durable run there forever.
         */
        void failResidentExhaustion() {
            AttemptStatus status = snapshot.currentAttempt().status();
            if (status == AttemptStatus.DISPATCHING) {
                failBeforeDispatch();
                return;
            }
            if (status != AttemptStatus.RUNNING) return;
            if (incremental()) return;
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("resident-budget-failed"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.FAILED, false, eventTimeMillis,
                    io.rowweave.cluster.rpc.ControlResidentBudgetExceededException.CODE,
                    "control resident budget exhausted"), AttemptStatus.FAILED, null,
                    "fail resident exhaustion");
        }

        void failFromTerminalEvidence() {
            if (snapshot.currentAttempt().status() != AttemptStatus.RUNNING) return;
            StageAssignmentSnapshot evidence =
                    assignmentSnapshot();
            if (evidence == null) return;
            StageTerminalReceipt failure = null;
            for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
                StageTerminalReceipt terminal = evidence.terminal(i);
                if (terminal != null && !terminal.succeeded()) {
                    failure = terminal;
                    break;
                }
            }
            if (failure == null) return;
            if (incremental()) {
                closeIncrementalAbort(
                        AttemptStatus.FAILED,
                        "cluster-stage-terminal-failed");
                return;
            }
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("stage-terminal-failed"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.FAILED, false, eventTimeMillis,
                    "STAGE_TERMINAL_FAILURE", failure.failureCategory()),
                    AttemptStatus.FAILED, null,
                    "fail from durable terminal evidence");
        }

        void failPrepublicationCommit() {
            if (snapshot.currentAttempt().status() != AttemptStatus.COMMITTING) return;
            if (incremental()) {
                closeIncrementalAbort(
                        AttemptStatus.FAILED,
                        "cluster-prepublication-failed");
                return;
            }
            snapshot = transitionAuthoritatively(RunCommand.requestCancellation(
                    commandId("commit-prepublication-reconciling"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    CancellationCause.FENCE, eventTimeMillis,
                    "COMMIT_PREPUBLICATION_FAILED",
                    "durable manifest was not published"),
                    AttemptStatus.RECONCILING, null,
                    "enter pre-publication reconciliation");
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("commit-prepublication-failed"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.FAILED, false, eventTimeMillis,
                    "COMMIT_PREPUBLICATION_FAILED", null),
                    AttemptStatus.FAILED, null,
                    "fail pre-publication commit");
        }

        private void closeIncrementalAbort(
                AttemptStatus terminalStatus, String commandPrefix) {
            io.rowweave.incremental.VerifiedIncrementalAbortClosureProof proof =
                    incrementalAuthority.verifyAbortClosure(
                            frontierToken, recoveryToken, fence(), terminalStatus);
            io.rowweave.incremental.IncrementalAuthorityRecord record =
                    incrementalAuthority.record(incrementalRequest.feedId());
            io.rowweave.incremental.IncrementalActiveInterval active =
                    record == null ? null : record.activeInterval();
            if (active == null || !fence().equals(active.fence())) {
                throw new IllegalStateException(
                        "incremental abort lost the active interval");
            }
            io.rowweave.incremental.IncrementalAuthorityCommand close =
                    new io.rowweave.incremental.IncrementalAuthorityCommand(
                            io.rowweave.incremental
                                    .IncrementalAuthorityCommandKind
                                    .CLOSE_ABORTED_RUN,
                            incrementalRequest.feedId(),
                            commandPrefix + '-' + incrementalRequest.intervalId(),
                            record.revision(), record.feedIncarnation(),
                            incrementalRequest.intervalId(), active.fence(),
                            proof.proofDigest());
            io.rowweave.incremental.IncrementalAuthorityDecision closed =
                    incrementalAuthority.closeAborted(
                            frontierToken, recoveryToken, close, proof);
            if (closed.code()
                    != io.rowweave.incremental.IncrementalAuthorityDecision
                    .Code.ACCEPTED
                    && closed.code()
                    != io.rowweave.incremental.IncrementalAuthorityDecision
                    .Code.ALREADY_APPLIED) {
                throw new IllegalStateException(
                        "incremental abort closure rejected: "
                                + closed.code());
            }
            snapshot = closed.runSnapshot();
            requireAttemptStatus(terminalStatus,
                    "close incremental aborted Run");
        }

        void failQualityGateRejected() {
            if (snapshot.currentAttempt().status() != AttemptStatus.COMMITTING) return;
            snapshot = transitionAuthoritatively(RunCommand.requestCancellation(
                    commandId("quality-gate-reconciling"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    CancellationCause.FENCE, eventTimeMillis,
                    "QUALITY_GATE_REJECTED", "quality gate rejected publication"),
                    AttemptStatus.RECONCILING, null,
                    "enter quality rejection reconciliation");
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("quality-gate-failed"), runId,
                    snapshot.revision(), attemptId, fence().fenceToken(),
                    AttemptStatus.FAILED, false, eventTimeMillis,
                    "QUALITY_GATE_REJECTED", "quality gate rejected publication"),
                    AttemptStatus.FAILED, null,
                    "fail quality-gated publication");
        }

        boolean userCancellationRequested() {
            RunSnapshot observed = incremental()
                    ? incrementalAuthority.runSnapshot(runId)
                    : runStateStore.get(runId);
            if (observed == null) {
                throw new IllegalStateException(
                        "active durable Run disappeared during cancellation check");
            }
            AttemptSnapshot attempt = observed.currentAttempt();
            if (!runId.equals(observed.runId()) || attempt == null
                    || !attemptId.equals(attempt.attemptId())
                    || attempt.fenceToken() != fenceToken) {
                throw new IllegalStateException(
                        "active durable Run identity changed during cancellation check");
            }
            snapshot = observed;
            return (attempt.status() == AttemptStatus.CANCELING
                    && attempt.cancellationCause() == CancellationCause.USER)
                    || observed.status() == io.rowweave.run.RunStatus.CANCELED;
        }

        /**
         * Every accepted entry has a frozen terminal state; for an entry not accepted, the dispatch fence proves it absent.
         */
        boolean completeUserCancellationIfProven() {
            if (!userCancellationRequested()) return false;
            if (snapshot.status() == io.rowweave.run.RunStatus.CANCELED) return true;
            StageAssignmentSnapshot evidence =
                    assignmentSnapshot();
            if (evidence != null) {
                for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
                    if (evidence.acceptance(i) != null && evidence.terminal(i) == null) {
                        return false;
                    }
                }
                boolean qualityBounded = evidence.manifest()
                        .hasQualityGateBinding();
                io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                        qualityBounded
                                ? stageAssignmentAuthority
                                .qualityGateCommitJournal(fence()) : null;
                ManifestCommitJournalSnapshot journal = qualityBounded ? null
                        : manifestCommitJournal();
                if (qualityBounded
                        ? qualityJournal != null && qualityJournal.intent() != null
                        : journal != null && journal.intent() != null) {
                    return false;
                }
            }
            if (incremental()) {
                closeIncrementalAbort(
                        AttemptStatus.CANCELED,
                        "cluster-user-canceled");
                return true;
            }
            long terminalTime = Math.max(
                    snapshot.updatedAtMillis(), System.currentTimeMillis());
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("user-canceled"), runId,
                    snapshot.revision(), attemptId, fenceToken,
                    AttemptStatus.CANCELED, false, terminalTime,
                    "USER_CANCEL", null), AttemptStatus.CANCELED, null,
                    "complete user cancellation");
            requireAttemptStatus(AttemptStatus.CANCELED,
                    "complete user cancellation");
            return true;
        }

        boolean deadlineTimeoutRequested(boolean forceAuthorityProbe) {
            if (snapshot.deadlineEpochMillis() == 0L) return false;
            if (!forceAuthorityProbe && deadlineWakeupNanos != Long.MIN_VALUE
                    && System.nanoTime() - deadlineWakeupNanos < 0L) {
                return false;
            }
            RunDeadlineResult result = incremental()
                    ? incrementalAuthority.evaluateDeadline(
                            frontierToken, recoveryToken, runId)
                    : evaluateRunDeadline(runId);
            if (result.disposition() == RunDeadlineResult.Disposition.NOT_DUE) {
                scheduleDeadlineWakeup(result);
                return false;
            }
            deadlineWakeupNanos = Long.MAX_VALUE;
            refreshDeadlineSnapshot();
            return timeoutObserved();
        }

        private void scheduleDeadlineWakeup(RunDeadlineResult result) {
            long remainingMillis = result.deadlineEpochMillis()
                    - result.observedAuthorityTimeMillis();
            long maxDelayNanos = Long.MAX_VALUE / 4L;
            long delayNanos = remainingMillis > maxDelayNanos / 1_000_000L
                    ? maxDelayNanos : remainingMillis * 1_000_000L;
            long now = System.nanoTime();
            long candidate = now + delayNanos;
            deadlineWakeupNanos = candidate < now ? Long.MAX_VALUE : candidate;
        }

        private void refreshDeadlineSnapshot() {
            RunSnapshot observed = incremental()
                    ? incrementalAuthority.runSnapshot(runId)
                    : runStateStore.get(runId);
            if (observed == null) {
                throw new IllegalStateException(
                        "active durable Run disappeared during deadline check");
            }
            AttemptSnapshot attempt = observed.currentAttempt();
            if (!runId.equals(observed.runId()) || attempt == null
                    || !attemptId.equals(attempt.attemptId())
                    || attempt.fenceToken() != fenceToken) {
                throw new IllegalStateException(
                        "active durable Run identity changed during deadline check");
            }
            snapshot = observed;
        }

        private boolean timeoutObserved() {
            AttemptSnapshot attempt = snapshot.currentAttempt();
            return attempt != null
                    && ((attempt.status() == AttemptStatus.CANCELING
                    && attempt.cancellationCause() == CancellationCause.TIMEOUT)
                    || attempt.status() == AttemptStatus.TIMED_OUT);
        }

        /**
         * Every accepted entry has a frozen terminal state; for an entry not accepted, the dispatch fence proves it absent.
         */
        boolean completeTimeoutIfProven() {
            refreshDeadlineSnapshot();
            if (snapshot.status() == io.rowweave.run.RunStatus.FAILED
                    && snapshot.currentAttempt().status()
                    == AttemptStatus.TIMED_OUT) {
                return true;
            }
            AttemptSnapshot attempt = snapshot.currentAttempt();
            if (attempt.status() != AttemptStatus.CANCELING
                    || attempt.cancellationCause() != CancellationCause.TIMEOUT) {
                return false;
            }
            StageAssignmentSnapshot evidence =
                    assignmentSnapshot();
            if (evidence != null) {
                for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
                    if (evidence.acceptance(i) != null && evidence.terminal(i) == null) {
                        return false;
                    }
                }
                boolean qualityBounded = evidence.manifest()
                        .hasQualityGateBinding();
                io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                        qualityBounded
                                ? stageAssignmentAuthority
                                .qualityGateCommitJournal(fence()) : null;
                ManifestCommitJournalSnapshot journal = qualityBounded ? null
                        : manifestCommitJournal();
                if (qualityBounded
                        ? qualityJournal != null && qualityJournal.intent() != null
                        : journal != null && journal.intent() != null) return false;
            }
            if (incremental()) {
                closeIncrementalAbort(
                        AttemptStatus.TIMED_OUT,
                        "cluster-deadline-timed-out");
                return true;
            }
            snapshot = transitionAuthoritatively(RunCommand.advanceAttempt(
                    commandId("deadline-timed-out"), runId,
                    snapshot.revision(), attemptId, fenceToken,
                    AttemptStatus.TIMED_OUT, false, snapshot.updatedAtMillis(),
                    io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE, null),
                    AttemptStatus.TIMED_OUT, null, "complete deadline timeout");
            requireAttemptStatus(AttemptStatus.TIMED_OUT,
                    "complete deadline timeout");
            return true;
        }

        private String commandId(String phase) {
            return recoveryIdentity == null
                    ? durableCommandId(commandSeed, phase)
                    : recoveryIdentity.commandId(phase);
        }

        private RunDecision mutate(RunCommand command) {
            if (incremental()) {
                throw new IllegalStateException(
                        "incremental Run mutation requires the joint gateway");
            }
            return recoveryToken == null
                    ? runStateStore.apply(command)
                    : applyRecovered(recoveryToken, command);
        }

        private RunSnapshot advanceIncremental(
                AttemptStatus target, String commandPrefix) {
            io.rowweave.incremental.IncrementalAuthorityRecord record =
                    incrementalAuthority.record(incrementalRequest.feedId());
            RunSnapshot current = incrementalAuthority.runSnapshot(runId);
            if (record == null || record.activeInterval() == null
                    || current == null || current.currentAttempt() == null
                    || !fence().equals(record.activeInterval().fence())) {
                throw new IllegalStateException(
                        "incremental authority disappeared during progress");
            }
            long eventTime = current.updatedAtMillis() == Long.MAX_VALUE
                    ? Long.MAX_VALUE : current.updatedAtMillis() + 1L;
            io.rowweave.incremental.IncrementalAttemptProgressCommand command =
                    new io.rowweave.incremental.IncrementalAttemptProgressCommand(
                            incrementalRequest.feedId(),
                            incrementalRequest.intervalId(), record.revision(),
                            current.revision(), fence(), target,
                            commandPrefix + '-' + incrementalRequest.intervalId()
                                    + '-' + attemptId,
                            eventTime);
            RunDecision decision = incrementalAuthority.advanceProgress(
                    frontierToken, recoveryToken, command);
            if (decision.code() != RunDecision.Code.ACCEPTED
                    && decision.code() != RunDecision.Code.ALREADY_APPLIED) {
                throw new IllegalStateException(
                        "incremental Run progress rejected: "
                                + decision.code());
            }
            return decision.snapshot();
        }

        private RunSnapshot acceptedOrReplayed(RunDecision decision, String operation) {
            if (decision.code() != RunDecision.Code.ACCEPTED
                    && decision.code() != RunDecision.Code.ALREADY_APPLIED) {
                throw new IllegalStateException(operation + " rejected: " + decision.code());
            }
            return decision.snapshot();
        }

        private RunSnapshot transitionAuthoritatively(
                RunCommand command, AttemptStatus expected,
                io.rowweave.run.RunResultSummary expectedSummary,
                String operation) {
            try {
                return acceptedOrReplayed(mutate(command), operation);
            } catch (RuntimeException uncertain) {
                final RunSnapshot observed;
                try {
                    observed = runStateStore.get(runId);
                } catch (RuntimeException readFailure) {
                    readFailure.addSuppressed(uncertain);
                    throw readFailure;
                }
                AttemptSnapshot attempt = observed == null ? null : observed.currentAttempt();
                if (attempt != null && runId.equals(observed.runId())
                        && attemptId.equals(attempt.attemptId())
                        && attempt.fenceToken() == fenceToken
                        && attempt.status() == expected
                        && sameSummary(expectedSummary, observed.resultSummary())) {
                    return observed;
                }
                throw uncertain;
            }
        }

        private boolean sameSummary(io.rowweave.run.RunResultSummary expected,
                                    io.rowweave.run.RunResultSummary observed) {
            if (expected == observed) return true;
            if (expected == null || observed == null) return false;
            return expected.rowsOut() == observed.rowsOut()
                    && expected.publicationKind() == observed.publicationKind()
                    && java.util.Objects.equals(expected.manifestGeneration(),
                    observed.manifestGeneration())
                    && java.util.Arrays.equals(expected.manifestSha256(),
                    observed.manifestSha256())
                    && java.util.Arrays.equals(expected.sinkIdentityDigest(),
                    observed.sinkIdentityDigest());
        }

        private void requireAttemptStatus(AttemptStatus expected, String operation) {
            AttemptSnapshot attempt = snapshot.currentAttempt();
            if (!runId.equals(snapshot.runId()) || attempt == null
                    || !attemptId.equals(attempt.attemptId())
                    || attempt.fenceToken() != fenceToken
                    || attempt.status() != expected) {
                throw new IllegalStateException(operation + " replay snapshot mismatch");
            }
        }
    }

    private static String durableCommandId(String seed, String phase) {
        return "run-submit-v1-" + phase + '-' + seed;
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(value & 0xf, 16));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hexPrefix(byte[] bytes, int byteCount) {
        if (bytes == null || byteCount < 0 || bytes.length < byteCount) {
            throw new IllegalArgumentException("invalid digest prefix request");
        }
        StringBuilder hex = new StringBuilder(byteCount * 2);
        for (int index = 0; index < byteCount; index++) {
            int value = bytes[index] & 0xff;
            hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
            hex.append(Character.forDigit(value & 0xf, 16));
        }
        return hex.toString();
    }

    private RpcMessage handleManifestOutcomeQuery(RpcMessage req) {
        try {
            java.nio.file.Path domain = java.nio.file.Paths.get(req.commitDomainPath());
            io.rowweave.commit.ManifestFileStore store =
                    new io.rowweave.commit.ManifestFileStore(domain);
            if (!store.domainId().equals(req.commitDomainId())) {
                throw new IllegalArgumentException("manifest commit domain identity mismatch");
            }
            Long generation = manifestCommitAuthority.find(store.domainPath(), req.attemptId());
            io.rowweave.commit.ManifestCommitOutcome outcome =
                    store.queryOutcome(req.attemptId(), generation);
            req.reserveResponseResidentBytes(io.rowweave.commit.CommitManifest.SHA256_BYTES);
            return RpcMessage.manifestOutcome(
                    outcome.state().name(), req.attemptId(),
                    outcome.generation() == null ? 0L : outcome.generation(),
                    outcome.manifestHash(), outcome.detail());
        } catch (RuntimeException failure) {
            return RpcMessage.manifestOutcome(
                    io.rowweave.commit.ManifestCommitOutcome.State.CORRUPT_OR_CONFLICT.name(),
                    req.attemptId(), 0L, null, failure.getMessage());
        }
    }

    /**
     * A departure arriving late from an older generation is acknowledged as a no-op. For the generation that matches, scheduling stops
     * seeing it first, and only then is an in-flight barrier released.
     */
    private void handleWorkerDeparture(RpcMessage req) {
        boolean departed = false;
        // The registry may be a remote one, so a lookup by key is completed outside the lock that guards the
        // worker's lifecycle as a whole.
        RegistryWorker endpoint = endpointOf(req.workerId());
        synchronized (workerLifecycleLock) {
            if (!isActiveLeader()) {
                return;
            }
            WorkerInfo current = workers.get(req.workerId());
            boolean localMatches = current != null
                    && current.port == req.port()
                    && current.dataPort == req.dataPort()
                    && java.util.Objects.equals(current.host, req.host())
                    && java.util.Objects.equals(current.incarnationId, req.incarnationId());
            boolean registryMatches = endpoint == null || (endpoint.port() == req.port()
                    && endpoint.dataPort() == req.dataPort()
                    && java.util.Objects.equals(endpoint.host(), req.host())
                    && java.util.Objects.equals(endpoint.incarnationId(), req.incarnationId()));
            if (!localMatches || !registryMatches) {
                return;
            }

            workers.remove(req.workerId(), current);
            if (!registry.workerRegistrationOwnedByWorker()) {
                registry.deregister(req.workerId());
            }
            RegistryWorker departedEndpoint = new RegistryWorker(
                    req.workerId(), req.host(), req.port(), req.slots(), req.dataPort(),
                    req.incarnationId(), req.securityMetadata(),
                    current.dataPlaneRegistration);
            quarantineWorkerLocked(req.workerId(), departedEndpoint, true);
            failStagesOnWorker(req.workerId(), departedEndpoint,
                    new IllegalStateException("worker departed: " + req.workerId()));
            departed = true;
        }
        if (departed) {
            try {
                onWorkerLost.accept(req.workerId());
            } catch (Throwable callbackFailure) {
                LOG.warn("worker departure callback failed worker={}", req.workerId(), callbackFailure);
            }
        }
    }

    private boolean beginSubmit() {
        synchronized (submitLifecycle) {
            if (closed) {
                return false;
            }
            activeSubmits++;
            return true;
        }
    }

    private void endSubmit() {
        synchronized (submitLifecycle) {
            activeSubmits--;
            submitLifecycle.notifyAll();
        }
    }

    private RpcMessage unavailableResult() {
        return closed ? closingResult() : RpcMessage.jobResult(false, 0, "not active master leader");
    }

    private static RpcMessage closingResult() {
        return RpcMessage.jobResult(false, 0, MASTER_CLOSING);
    }

    private final AtomicLong stageSeq = new AtomicLong(
            java.util.concurrent.ThreadLocalRandom.current().nextLong(1L, 1L << 30) << 32);
    // stage id to the tracker of that stage's asynchronous report: an assignment acknowledges at once, and
    // the stage report arrives later over a connection of its own.
    private final Map<Long, StageTracker> stageTrackers = new ConcurrentHashMap<>();
    // stage id to the port tracker of a stage that listens, since a port report arrives asynchronously and
    // the barrier is stepped through layer by layer.
    private final Map<Long, StagePortTracker> portTrackers = new ConcurrentHashMap<>();
    /**
     * A stage's owner is recorded against the generation of an endpoint, not merely against a worker id, which a replacement may have taken
     * over.
     */
    private static final class StageOwner {
        final String workerId;
        final RegistryWorker endpoint;
        final RunAttemptFence runAttemptFence;
        final StageAssignmentToken assignmentToken;
        final byte[] commandDigest;
        final RecoveryToken recoveryToken;
        final io.rowweave.incremental.FrontierRecoveryToken frontierToken;

        StageOwner(String workerId, RegistryWorker endpoint, RunAttemptFence runAttemptFence,
                   StageAssignmentToken assignmentToken, byte[] commandDigest,
                   RecoveryToken recoveryToken) {
            this(workerId, endpoint, runAttemptFence, assignmentToken,
                    commandDigest, recoveryToken, null);
        }

        StageOwner(String workerId, RegistryWorker endpoint, RunAttemptFence runAttemptFence,
                   StageAssignmentToken assignmentToken, byte[] commandDigest,
                   RecoveryToken recoveryToken,
                   io.rowweave.incremental.FrontierRecoveryToken frontierToken) {
            this.workerId = workerId;
            this.endpoint = endpoint;
            this.runAttemptFence = java.util.Objects.requireNonNull(
                    runAttemptFence, "runAttemptFence");
            this.assignmentToken = assignmentToken;
            this.commandDigest = commandDigest == null ? null : commandDigest.clone();
            this.recoveryToken = recoveryToken;
            this.frontierToken = frontierToken;
        }
    }

    private final Map<Long, StageOwner> stageWorker = new ConcurrentHashMap<>();

    /** Observation for tests: how many stage trackers the master still holds terminal responsibility for. */
    int activeStageTrackerCount() {
        return stageTrackers.size();
    }

    /** Observation for tests: how many stage-to-worker assignments have not been cleaned up. */
    int activeStageAssignmentCount() {
        return stageWorker.size();
    }

    /**
     * One stage's latch for an asynchronous report. It is hung after the assignment and released when the stage report arrives. It may be
     * attached to a {@link JobBarrier}, so that arrival notifies the barrier, counting a success or short-circuiting on a failure.
     */
    private static final class StageTracker {
        private final int logicalStage;
        private final int instanceIndex;
        private final StageCommand.Role role;
        private final int parallelism;
        private final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        private volatile boolean success;
        private volatile long inputRows;
        private volatile long rowCount;
        private volatile String error;
        private volatile io.rowweave.plan.TelemetryCoverage telemetryCoverage =
                io.rowweave.plan.TelemetryCoverage.NOT_EXECUTED;
        private volatile long telemetryPeak;
        private volatile long telemetryAcquired;
        private volatile long telemetryPressure;
        private volatile long telemetryRuns;
        private volatile long telemetryWritten;
        private volatile long telemetrySpillPeak;
        private volatile boolean done;
        private volatile boolean workerTerminal;
        private volatile JobBarrier barrier;
        private final java.util.concurrent.atomic.AtomicBoolean notified =
                new java.util.concurrent.atomic.AtomicBoolean(false);

        StageTracker(int logicalStage, int instanceIndex,
                     StageCommand.Role role, int parallelism) {
            if (logicalStage < 0 || instanceIndex < 0 || role == null
                    || parallelism < 1 || instanceIndex >= parallelism) {
                throw new IllegalArgumentException("invalid stage telemetry identity");
            }
            this.logicalStage = logicalStage;
            this.instanceIndex = instanceIndex;
            this.role = role;
            this.parallelism = parallelism;
        }

        /**
         * Attaches the barrier; if this stage is already finished it feeds one report through at once, which is idempotent because a
         * compare-and-set guards the notification.
         */
        synchronized void attachBarrier(JobBarrier b) {
            this.barrier = b;
            if (done) {
                notifyBarrierOnce();
            }
        }

        synchronized boolean onReport(boolean ok, long inputs, long rows, String err,
                                      io.rowweave.plan.StageExecutionTelemetry stageTelemetry,
                                      Runnable firstTerminalAction) {
            return complete(ok, inputs, rows, err, true, stageTelemetry,
                    firstTerminalAction);
        }

        synchronized void fail(String err) {
            complete(false, 0L, 0L, err, false,
                    io.rowweave.plan.StageExecutionTelemetry.notExecuted(), null);
        }

        private boolean complete(boolean ok, long inputs, long rows, String err,
                                 boolean fromWorker,
                                 io.rowweave.plan.StageExecutionTelemetry stageTelemetry,
                                 Runnable firstTerminalAction) {
            if (done) {
                return false;  // the first terminal report wins, whether a loss, a real report or a duplicate arrived first
            }
            this.success = ok;
            this.inputRows = inputs;
            this.rowCount = rows;
            this.error = err;
            io.rowweave.plan.StageExecutionTelemetry frozen = stageTelemetry == null
                    ? io.rowweave.plan.StageExecutionTelemetry.unavailable()
                    : stageTelemetry;
            this.telemetryCoverage = frozen.coverage();
            this.telemetryPeak = frozen.managedPeakReservedBytes();
            this.telemetryAcquired = frozen.managedAcquiredBytes();
            this.telemetryPressure = frozen.pressureSpillCount();
            this.telemetryRuns = frozen.spillRunCount();
            this.telemetryWritten = frozen.spillLogicalBytesWritten();
            this.telemetrySpillPeak = frozen.spillPeakReservedBytes();
            this.workerTerminal = fromWorker;
            this.done = true;
            try {
                if (firstTerminalAction != null) firstTerminalAction.run();
            } finally {
                // The accepted terminal monitor projection must be visible before either
                // waiter can publish FINISHED and clear stageRefs.
                latch.countDown();
                if (barrier != null) {
                    notifyBarrierOnce();
                }
            }
            return true;
        }

        boolean hasWorkerTerminal() {
            return workerTerminal;
        }

        boolean hasSuccessfulWorkerTerminal() {
            return workerTerminal && success;
        }

        synchronized EphemeralExecutionTelemetryAggregator.Physical telemetrySnapshot() {
            io.rowweave.plan.StageExecutionTelemetry telemetry;
            if (telemetryCoverage == io.rowweave.plan.TelemetryCoverage.COMPLETE) {
                telemetry = io.rowweave.plan.StageExecutionTelemetry.complete(
                        telemetryPeak, telemetryAcquired, telemetryPressure,
                        telemetryRuns, telemetryWritten, telemetrySpillPeak);
            } else if (telemetryCoverage
                    == io.rowweave.plan.TelemetryCoverage.NOT_EXECUTED) {
                telemetry = io.rowweave.plan.StageExecutionTelemetry.notExecuted();
            } else {
                telemetry = io.rowweave.plan.StageExecutionTelemetry.unavailable();
            }
            return new EphemeralExecutionTelemetryAggregator.Physical(
                    logicalStage, instanceIndex, role, parallelism,
                    inputRows, workerTerminal, telemetry);
        }

        private void notifyBarrierOnce() {
            if (notified.compareAndSet(false, true)) {
                barrier.onStageDone(success, error);
            }
        }

        boolean await(long timeoutMs) throws InterruptedException {
            return latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Fail-fast reinforcement: a single latch covering every stage of one job that **short-circuits on the
     * first failure**, instead of accumulating a 60s timeout stage by stage. It also releases when every
     * stage succeeds, and one deadline for the whole job is the backstop so the wait cannot hang. Each
     * arriving STAGE_REPORT feeds {@link #onStageDone}.
     */
    private static final class JobBarrier {
        private final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicInteger remaining;
        private volatile boolean failed;
        private volatile String firstError;

        JobBarrier(int stageCount) {
            this.remaining = new java.util.concurrent.atomic.AtomicInteger(stageCount);
        }

        void onStageDone(boolean ok, String err) {
            if (!ok) {
                if (!failed) {
                    failed = true;
                    firstError = err;
                }
                done.countDown();  // the first failure short-circuits the wait
                return;
            }
            if (remaining.decrementAndGet() == 0) {
                done.countDown();  // every stage succeeded
            }
        }

        /**
         * Waits against one deadline for the whole job. False means it timed out, which is the backstop against hanging; {@code failed()}
         * and {@code firstError()} say what happened.
         */
        boolean await(long timeoutMs) throws InterruptedException {
            return done.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        boolean failed() { return failed; }
        String firstError() { return firstError; }
    }

    /** one monotonic origin, with execution before terminal cleanup. */
    private static final class EphemeralSubmitDeadline {
        private final EphemeralSubmitTimeoutPolicy.Budget budget;
        private final long startedNanos;
        private final long executionDeadlineNanos;
        private final long terminalDeadlineNanos;

        private EphemeralSubmitDeadline(EphemeralSubmitTimeoutPolicy.Budget budget) {
            this.budget = java.util.Objects.requireNonNull(budget, "submit timeout budget");
            this.startedNanos = System.nanoTime();
            this.executionDeadlineNanos = addMillisSaturated(
                    startedNanos, budget.executionTimeoutMs());
            this.terminalDeadlineNanos = addMillisSaturated(
                    executionDeadlineNanos, budget.cleanupTimeoutMs());
        }

        static EphemeralSubmitDeadline start(EphemeralSubmitTimeoutPolicy.Budget budget) {
            return new EphemeralSubmitDeadline(budget);
        }

        boolean executionExpired() {
            return executionDeadlineNanos - System.nanoTime() <= 0L;
        }

        long executionWaitMillis(long upperBoundMillis) {
            long remaining = executionDeadlineNanos - System.nanoTime();
            if (remaining <= 0L) return 0L;
            long roundedUp = java.util.concurrent.TimeUnit.NANOSECONDS
                    .toMillis(remaining - 1L) + 1L;
            return Math.min(upperBoundMillis, roundedUp);
        }

        long elapsedMillis() {
            long elapsed = System.nanoTime() - startedNanos;
            return elapsed <= 0L ? 0L
                    : java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(elapsed);
        }

        long executionTimeoutMillis() {
            return budget.executionTimeoutMs();
        }

        long terminalDeadlineNanos() {
            return terminalDeadlineNanos;
        }

        long executionDeadlineNanos() {
            return executionDeadlineNanos;
        }
    }

    /**
     * How long, in milliseconds, stage setup may take. It bounds only a port report and the binding of a listening port, never how long
     * data takes to flow through a healthy worker.
     */
    private static final long STAGE_SETUP_TIMEOUT_MS = Long.getLong("rowweave.cluster.stageTimeoutMs", 60_000L);

    /**
     * Turns the cut graph into a {@link PhysicalPlan}. A WHOLE_GRAPH, which has no shuffle, becomes one worker running the entire graph:
     * the assignment is asynchronous and acknowledges at once, and the wait is for the stage report, failing fast on a timeout. There is no
     * shuffle on this path, so the layer-by-layer barrier orchestration is not used.
     */
    private RpcMessage handleSubmit(RpcMessage req) {
        EphemeralSubmitDeadline submitDeadline = EphemeralSubmitDeadline.start(
                ephemeralSubmitTimeoutPolicy.resolve(req.ephemeralSubmitTimeouts()));
        final PlanGraph graph;
        final byte[] portableArtifactBytes;
        final java.util.List<String> requiredWorkerCapabilities;
        try {
            deploymentRuntime.requireAdmissionOpen();
            if (req.type() == RpcMessage.Type.SUBMIT_PLAN_ARTIFACT) {
                PlanArtifact inProcessArtifact = req.inProcessPlanArtifact();
                byte[] artifactBytes = req.planGraphBytes();
                PlanArtifact artifact = inProcessArtifact != null
                        ? inProcessArtifact
                        : PlanArtifactCodec.decode(
                                artifactBytes, req::reserveResidentBytes);
                requireArtifactDeployment(artifact);
                if (artifact.payloadFormat()
                        == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
                    PortableBatchPlan plan = PortablePlanArtifact
                            .preflightValidated(
                                    artifact,
                                    deploymentRuntime.portableBindingVerifier(),
                                    req::reserveResidentBytes);
                    graph = PortablePlanCompiler.projectMetadata(
                            plan, deploymentRuntime.portableBindingVerifier());
                    portableArtifactBytes = artifactBytes;
                    requiredWorkerCapabilities = plan.requiredCapabilities();
                } else {
                    if (inProcessArtifact != null) {
                        graph = LegacyPlanArtifactAdapter.unwrapValidated(artifact);
                    } else {
                        PlanArtifactValidator.validateForCurrentRuntime(artifact);
                        graph = LegacyPlanArtifactAdapter.unwrapValidated(
                                artifact, objectTrustContext,
                                req::reserveResidentBytes);
                    }
                    portableArtifactBytes = null;
                    requiredWorkerCapabilities = java.util.Collections.emptyList();
                }
            } else {
                graph = PlanGraph.fromBytes(
                        req.planGraphBytes(), objectTrustContext, req::reserveResidentBytes);
                portableArtifactBytes = null;
                requiredWorkerCapabilities = java.util.Collections.emptyList();
            }
            io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(graph);
            OutputDestinationValidator.validate(graph);  // validated here so a raw RPC cannot step around the client, and before any worker is found or admitted
            ClusterOutputValidator.validate(graph);
            io.rowweave.plan.OutputCommitValidator.validate(graph);
            ClusterPlanCapabilityValidator.validate(graph);
        } catch (io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted) {
            return RpcMessage.jobResult(false, 0, exhausted.getMessage());
        } catch (Exception e) {
            return RpcMessage.jobResult(false, 0, "submit failed: " + e.getMessage());
        }
        return executeSubmittedGraph(
                graph, req.attemptId(), null, req, submitDeadline,
                portableArtifactBytes, requiredWorkerCapabilities);
    }

    private void requireArtifactDeployment(PlanArtifact artifact) {
        deploymentRuntime.requireAdmissionOpen();
        PlanArtifactDeploymentBinding.requireMatches(
                artifact, deploymentSetId,
                controlSecurityProfile == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS);
    }

    private RpcMessage executeSubmittedGraph(
            PlanGraph graph, String publicationId, DurableRunExecution durable,
            RpcMessage residentRequest, EphemeralSubmitDeadline ephemeralDeadline) {
        return executeSubmittedGraph(
                graph, publicationId, durable, residentRequest,
                ephemeralDeadline, null,
                java.util.Collections.emptyList());
    }

    private RpcMessage executeSubmittedGraph(
            PlanGraph graph, String publicationId, DurableRunExecution durable,
            RpcMessage residentRequest, EphemeralSubmitDeadline ephemeralDeadline,
            byte[] portableArtifactBytes,
            java.util.Collection<String> requiredWorkerCapabilities) {
        java.util.List<String> effectiveWorkerCapabilities =
                requiredWorkerCapabilities(
                        graph, requiredWorkerCapabilities);
        // Workers are read from the registry, which several masters share, so the new leader after a failover
        // still sees them.
        java.util.List<RegistryWorker> live = schedulableWorkers(
                effectiveWorkerCapabilities);
        RegistryWorker target = live.stream().findFirst().orElse(null);
        if (target == null) {
            if (durable != null) {
                if (durable.userCancellationRequested()) {
                    durable.completeUserCancellationIfProven();
                } else if (!durable.schedulePreDispatchRetry()) {
                    durable.failBeforeDispatch();
                }
            }
            return RpcMessage.jobResult(false, 0, "no registered worker");
        }
        try {
            // A graph with several sinks assembled whole: the builder roots the graph at every sink and a shared
            // upstream broadcasts, so it is computed once. Each SINK stage writes through its own output.
            boolean portableStageLifecycle = portableArtifactBytes != null
                    && requiresPortableStageLifecycle(
                            effectiveWorkerCapabilities);
            ExecutionGraph eg = portableArtifactBytes == null
                    ? (durable != null || containsManifestOutput(graph)
                    ? ExecutionGraphBuilder.buildWithSinkBoundary(graph)
                    : ExecutionGraphBuilder.build(graph))
                    : portableStageLifecycle
                    ? ExecutionGraphBuilder.buildWithSinkBoundary(graph)
                    : portableWholeExecutionGraph(graph);
            java.util.List<String> workerIds = new java.util.ArrayList<>();
            for (RegistryWorker rw : live) {
                workerIds.add(rw.workerId());
            }
            String jobId = durable == null
                    ? "job-" + stageSeq.incrementAndGet() : durable.jobId();
            if (durable == null && portableArtifactBytes == null
                    && residentRequest != null
                    && residentRequest.inProcessPlanArtifact() != null) {
                inProcessLegacyJobs.add(jobId);
            }
            RunAttemptFence runAttemptFence = durable == null
                    ? new RunAttemptFence(jobId, "attempt-" + jobId, 1L)
                    : durable.fence();
            int desiredP = graph.parallelism();  // parallelism is a property of the job, so this is what was asked for
            int effectiveP = desiredP;  // after a fallback this can be below what was asked for; by default they are equal

            // Resource admission takes a job's slots as a gang across the whole job, and it is on by default. When
            // starting at lower parallelism is allowed, the desired value is held for its grace and then the highest
            // that currently fits free slots is taken, all against one deadline.
            if (admissionEnabled) {
                try {
                    effectiveP = admitWithDowngrade(
                            jobId, eg, desiredP, workerIds, target.workerId(),
                            ephemeralDeadline == null ? 0L
                                    : ephemeralDeadline.executionDeadlineNanos());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (durable != null) {
                        if (durable.userCancellationRequested()) {
                            durable.completeUserCancellationIfProven();
                        } else {
                            durable.failBeforeDispatch();
                        }
                    }
                    pushJobCompleteError(jobId, "admission interrupted");
                    clearStageRefs(jobId);
                    return RpcMessage.jobResult(false, 0, "submit failed: admission interrupted");
                }
                if (effectiveP <= 0) {
                    if (ephemeralDeadline != null
                            && ephemeralDeadline.executionExpired()) {
                        return settleEphemeralTimeout(
                                java.util.Collections.emptyList(),
                                java.util.Collections.emptyList(),
                                null, null, ephemeralDeadline);
                    }
                    // Nothing fits even at parallelism one, or with that fallback off the desired parallelism cannot be
                    // met: fail, carrying the diagnosis.
                    java.util.Map<String, Integer> demand = slotDemandOf(StageAssignment.assign(eg, desiredP, workerIds), target.workerId());
                    String diag = insufficientSlotsDiagnostic(demand, slotLedger.snapshot());
                    pushJobCompleteError(jobId, "insufficient slots");
                    clearStageRefs(jobId);
                    if (durable != null) {
                        if (durable.userCancellationRequested()) {
                            durable.completeUserCancellationIfProven();
                        } else {
                            durable.failBeforeDispatch();
                        }
                    }
                    return RpcMessage.jobResult(false, 0, "submit failed: insufficient cluster slots " + diag);
                }
                if (effectiveP < desiredP) {
                    LOG.warn("job {} downgraded parallelism due to slot pressure: desired P={} -> effective P={}", jobId, desiredP, effectiveP);
                }
            }

            // Assign at the effective parallelism, for which admission has already occupied the slots. With the
            // fallback off, the effective value is the desired one.
            ManifestJobCommit manifestCommit = null;
            try {
                PhysicalPlan plan = StageAssignment.assign(eg, effectiveP, workerIds);
                this.lastPhysicalPlan = plan;  // observation for tests, which is what proves the fan-out is real

                if (durable != null) {
                    DurableStageAssignmentPlan assignments =
                            durable.incremental()
                                    ? DurableStageAssignmentPlan.createIncremental(
                                    durable.fence(), durable.snapshot.artifactId(), graph,
                                    eg, plan, live, stageSeq::incrementAndGet)
                                    : DurableStageAssignmentPlan.create(
                                    durable.fence(), durable.snapshot.artifactId(), graph,
                                    eg, plan, live, stageSeq::incrementAndGet);
                    durable.publishAssignments(assignments);
                    durable.markDispatchingIfIncremental();
                }
                // Staging an output has to come after the durable physical intent; the older path keeps its original order.
                manifestCommit = openManifestJobCommit(
                        graph, publicationId,
                        durable == null ? null : durable.fence());
                if (durable != null) {
                    // Only once the intent is complete and the output staged may the job be reported RUNNING or any stage
                    // be assigned.
                    durable.markRunning();
                    runAttemptFence = durable.fence();
                }
                // After assigning, push a snapshot of the job view to the monitor, carrying both the desired and the
                // effective parallelism so a fallback is visible.
                pushJobSubmit(jobId, graph, eg, plan, desiredP, effectiveP,
                        durable != null);

                RpcMessage result = plan.isWholeGraph()
                        ? runWholeGraphAsync(graph, plan, target, manifestCommit, runAttemptFence,
                                durable == null ? null : durable.assignmentPlan,
                                durable == null ? null : durable.recoveryToken,
                                durable,
                                residentRequest, ephemeralDeadline,
                                portableArtifactBytes)
                        // The general multi-stage scheduler takes every shuffle shape: a single keyed exchange, a binary join, a
                        // chain of several hops, an aggregate after a join, a chain of breakers. It really fans out at any
                        // parallelism at or above one. The monitoring job id travels with it so each stage can push updates.
                        : runDagAsync(graph, eg, plan, live, workerIds, jobId, manifestCommit,
                                runAttemptFence, durable, residentRequest,
                                ephemeralDeadline, portableArtifactBytes);
                if (durable != null && result.success()) {
                    durable.markSucceeded(
                            durable.resultSummary(manifestCommit), manifestCommit);
                } else if (durable != null && !durable.incremental()) {
                    durable.failFromTerminalEvidence();
                }
                pushBatchSinkUpdate(jobId, graph, durable);
                pushJobComplete(jobId, result, durable);
                clearStageRefs(jobId);
                return result;
            } catch (Exception execErr) {
                io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted =
                        residentExhaustion(execErr);
                if (durable != null) {
                    if (durable.userCancellationRequested()) {
                        Throwable cleanupFailure = manifestCommit == null ? null
                                : rollbackOutputsBeforePublication(
                                "manifest:" + manifestCommit.attemptId,
                                java.util.Collections.emptyList(), manifestCommit, durable,
                                deadlineAfterMillis(outputCommitTimeoutMs), "USER_CANCELED");
                        if (cleanupFailure == null) {
                            durable.completeUserCancellationIfProven();
                        }
                    } else if (durable.deadlineTimeoutRequested(true)) {
                        settleDeadlineAfterExecutionFailure(
                                manifestCommit, durable);
                    } else if (exhausted == null) {
                        durable.failBeforeDispatch();
                    } else {
                        durable.failResidentExhaustion();
                    }
                }
                pushBatchSinkUpdate(jobId, graph, durable);
                // A failure while running, such as an interruption caused by a worker being lost, is recorded as FAILED.
                // Recording only on the normal return would leave a failed job displayed as RUNNING.
                pushJobCompleteError(jobId, execErr.getMessage());
                clearStageRefs(jobId);
                return RpcMessage.jobResult(false, 0, exhausted == null
                        ? "submit failed: " + execErr.getMessage()
                        : exhausted.getMessage());
            } finally {
                try {
                    if (manifestCommit != null) {
                        manifestCommit.close();
                    }
                } finally {
                    // Release has to cover every terminal state, the normal return and the inner handler alike. With
                    // admission off, releasing is harmless and idempotent, since nothing was occupied.
                    slotLedger.release(jobId);
                }
            }
        } catch (Exception e) {
            io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted =
                    residentExhaustion(e);
            if (durable != null) {
                if (durable.userCancellationRequested()) {
                    durable.completeUserCancellationIfProven();
                } else if (durable.deadlineTimeoutRequested(true)) {
                    settleDeadlineAfterExecutionFailure(null, durable);
                } else if (exhausted == null) {
                    durable.failBeforeDispatch();
                } else {
                    durable.failResidentExhaustion();
                }
            }
            return RpcMessage.jobResult(false, 0, exhausted == null
                    ? "submit failed: " + e.getMessage()
                    : exhausted.getMessage());
        }
    }

    private static ExecutionGraph portableWholeExecutionGraph(PlanGraph graph) {
        java.util.List<Integer> members = new java.util.ArrayList<>(graph.nodeCount());
        for (int index = 0; index < graph.nodeCount(); index++) {
            members.add(index);
        }
        Stage stage = new Stage(
                0, members, java.util.Collections.emptyList(), true,
                graph.node(0).typeId(), 0);
        return new ExecutionGraph(java.util.Collections.singletonList(stage));
    }

    private static boolean requiresPortableStageLifecycle(
            java.util.Collection<String> requiredCapabilities) {
        if (requiredCapabilities == null) return false;
        for (String capability : requiredCapabilities) {
            if (!PortableBatchPlan.BATCH_CAPABILITY.equals(capability)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Derives execution requirements from the materialized graph as well as
     * from a portable plan declaration. Raw and legacy artifacts can contain
     * MAP_PARTITION too, and those paths emit the same v9 fixed-routing
     * commands, so they must never bypass v9 placement admission.
     */
    private static java.util.List<String> requiredWorkerCapabilities(
            PlanGraph graph,
            java.util.Collection<String> declaredCapabilities) {
        java.util.LinkedHashSet<String> required =
                new java.util.LinkedHashSet<>();
        if (declaredCapabilities != null) {
            required.addAll(declaredCapabilities);
        }
        for (int index = 0; index < graph.nodeCount(); index++) {
            if ("mapPartition".equals(graph.node(index).typeId())) {
                required.add(PortableBatchPlan.MAP_PARTITION_CAPABILITY);
                break;
            }
        }
        return java.util.Collections.unmodifiableList(
                new java.util.ArrayList<>(required));
    }

    private void settleDeadlineAfterExecutionFailure(
            ManifestJobCommit manifestCommit, DurableRunExecution durable) {
        Throwable cleanupFailure = manifestCommit == null ? null
                : rollbackOutputsBeforePublication(
                "manifest:" + manifestCommit.attemptId,
                java.util.Collections.emptyList(), manifestCommit, durable,
                deadlineAfterMillis(outputCommitTimeoutMs),
                io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE);
        if (cleanupFailure == null) durable.completeTimeoutIfProven();
    }

    private static io.rowweave.cluster.rpc.ControlResidentBudgetExceededException
    residentExhaustion(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof io.rowweave.cluster.rpc.ControlResidentBudgetExceededException) {
                return (io.rowweave.cluster.rpc.ControlResidentBudgetExceededException) current;
            }
        }
        return null;
    }

    // Collecting run observations. This is advisory: every call is fenced so a monitor cannot affect a job.

    /**
     * Triggers a refresh of the worker snapshot. A cluster that stays up calls it periodically, so the worker view is not tied to someone
     * submitting a job.
     */
    public void refreshWorkerMonitor() { syncWorkersToMonitor(); }

    /**
     * Pushes the current worker snapshot, the endpoints the registry knows together with what this master's own heartbeats prove alive, to
     * the monitor. Without this the worker view is empty.
     */
    private void syncWorkersToMonitor() {
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP) {
            return;
        }
        try {
            java.util.List<io.rowweave.monitor.WorkerView> ws = new java.util.ArrayList<>();
            for (RegistryWorker rw : registry.liveWorkers()) {
                if (MASTER_LEADER_ROLE_ID.equals(rw.workerId())) {
                    continue;  // a role record from an election is not a worker
                }
                WorkerInfo local = workers.get(rw.workerId());
                boolean alive = !quarantinedWorkers.containsKey(rw.workerId())
                        && (controlSecurityProfile
                                == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS
                                ? local != null && sameEndpoint(local, rw)
                                && workerLivenessPauseGuard.isAlive(local.heartbeat)
                                : local == null || workerLivenessPauseGuard.isAlive(local.heartbeat));
                ws.add(new io.rowweave.monitor.WorkerView(
                        rw.workerId(), rw.host(), rw.port(), rw.slots(), alive));
            }
            ((io.rowweave.monitor.InMemoryJobMonitor) monitor2).setWorkers(ws);
        } catch (RuntimeException ignored) {
            // Advisory: anything other than the in-memory collector, or any exception, is skipped rather than
            // affecting the job.
        }
    }

    /**
     * Merges what the registry discovered with this master's own liveness and quarantine into the one set of candidates placement may use.
     */
    private java.util.List<RegistryWorker> schedulableWorkers() {
        return schedulableWorkers(java.util.Collections.emptyList());
    }

    private java.util.List<RegistryWorker> schedulableWorkers(
            java.util.Collection<String> requiredCapabilities) {
        if (requiredCapabilities == null) {
            throw new IllegalArgumentException(
                    "required worker capabilities must not be null");
        }
        boolean negotiatedOnly = requiresPortableStageLifecycle(
                requiredCapabilities);
        if (negotiatedOnly && protocolHello != null
                && !protocolHello.runtimeCapabilities()
                .containsAll(requiredCapabilities)) {
            return java.util.Collections.emptyList();
        }
        boolean fixedRoutingRequired = requiredCapabilities.contains(
                PortableBatchPlan.MAP_PARTITION_CAPABILITY);
        if (fixedRoutingRequired && protocolHello != null
                && !protocolHello.stageCommandVersions().contains(
                StageCommandCodecV2.FIXED_ROUTING_VERSION)) {
            return java.util.Collections.emptyList();
        }
        StoreFormatHeader activeWriterHeader = upgradeAuthorityStore == null
                ? null : upgradeAuthorityStore.read().activeHeader();
        java.util.List<RegistryWorker> result = new java.util.ArrayList<>();
        for (RegistryWorker worker : registry.liveWorkers()) {
            if (MASTER_LEADER_ROLE_ID.equals(worker.workerId())
                    || quarantinedWorkers.containsKey(worker.workerId())
                    || (securityMetadata != null
                    && !securityMetadata.equals(worker.securityMetadata()))) {
                continue;
            }
            WorkerInfo local = workers.get(worker.workerId());
            if (local != null && !workerLivenessPauseGuard.isAlive(local.heartbeat)) {
                continue;  // a stale check between sweeps, so a worker is not treated as alive on stale evidence
            }
            if (activeWriterHeader != null && protocolHello == null) {
                continue;
            }
            if (negotiatedOnly && protocolHello == null
                    && !inProcessEndpoint(worker)) {
                // Local pseudo-deployment roles share this exact code image
                // and receive direct StageCommand objects. A remote pre-v1
                // Worker has no equivalent post-baseline compatibility proof.
                continue;
            }
            if (protocolHello != null) {
                if (local == null || local.protocolHello == null
                        || worker.protocolHello() == null
                        || !local.protocolHello.equals(worker.protocolHello())) {
                    continue;
                }
                try {
                    // Recompute for every candidate so a stale registry record or a
                    // replacement Master cannot reuse a previous connection decision.
                    io.rowweave.cluster.rpc.ProtocolNegotiator.negotiate(
                            protocolHello, worker.protocolHello());
                } catch (io.rowweave.cluster.rpc.ProtocolNegotiationException rejected) {
                    continue;
                }
                if (activeWriterHeader != null) {
                    try {
                        io.rowweave.upgrade.WriterRevisionGate.evaluate(
                                activeWriterHeader, java.util.Arrays.asList(
                                        protocolHello, worker.protocolHello()));
                    } catch (io.rowweave.upgrade.WriterRevisionRejectedException rejected) {
                        continue;
                    }
                }
                if (!worker.protocolHello().runtimeCapabilities()
                        .containsAll(requiredCapabilities)) {
                    continue;
                }
                if (fixedRoutingRequired
                        && !worker.protocolHello().stageCommandVersions()
                        .contains(StageCommandCodecV2.FIXED_ROUTING_VERSION)) {
                    continue;
                }
            }
            if (controlSecurityProfile
                    == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS
                    && (local == null || !sameEndpoint(local, worker)
                    || local.dataPlaneRegistration == null)) {
                continue; // registry data identity is only a candidate until this leader authenticates REGISTER.
            }
            result.add(worker);
        }
        return result;
    }

    private void pushJobSubmit(String jobId, PlanGraph graph, ExecutionGraph eg,
                               PhysicalPlan plan, int desiredP, int effectiveP,
                               boolean durable) {
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP) {
            return;  // the fast path that costs nothing when no observer is present
        }
        syncWorkersToMonitor();  // refreshes the worker view as a job is submitted
        try {
            io.rowweave.monitor.JobView.Builder jb = io.rowweave.monitor.JobView.builder(jobId, jobId)
                    .parallelism(desiredP, effectiveP)
                    .batchSink(batchSinkMonitorView(
                            graph, durable, 0, null));
            StringBuilder nodes = new StringBuilder();
            StringBuilder edges = new StringBuilder();
            java.util.List<Stage> stages = eg.stages();
            for (Stage st : stages) {
                String role = st.isSink() ? "SINK" : (st.inputStages().isEmpty() ? "SOURCE" : "STAGE");
                String opChain = opChainOf(graph, st);
                java.util.List<PhysicalPlan.Instance> insts = plan.instancesOf(st.index());
                int par = Math.max(1, insts.size());
                String wid = insts.isEmpty() ? null : insts.get(0).workerId();
                io.rowweave.monitor.StageView.Builder sv = io.rowweave.monitor.StageView.builder(st.index())
                        .role(role).state("READY").opChain(opChain).workerId(wid)  // a thread may not have started and no data has arrived yet, so the state is READY
                        .parallelism(par)  // the parallelism of the stage
                        .startTime(0);  // the start time is recorded only when the first row arrives
                SourceMonitorSummary sourceSummary = sourceMonitorSummary(graph, st);
                sourceSummary.apply(sv);
                for (int in : st.inputStages()) {
                    sv.addInput(in, edgeChannelType(graph, stages, st, in, plan));
                }
                for (int ii = 0; ii < par; ii++) {  // at first every instance is READY, waiting for data
                    sv.addInstance(new io.rowweave.monitor.StageView.InstanceView(
                            ii, ii < insts.size() ? insts.get(ii).workerId() : wid, "READY", 0));
                }
                jb.addStage(sv.build());
                if (nodes.length() > 0) nodes.append(',');
                nodes.append("{\"id\":").append(st.index()).append(",\"role\":\"").append(role)
                        .append("\",\"op\":\"").append(opChain).append("\"}");
                for (int in : st.inputStages()) {
                    if (edges.length() > 0) edges.append(',');
                    edges.append("{\"from\":").append(in).append(",\"to\":").append(st.index())
                         .append(",\"channel\":\"").append(edgeChannelType(graph, stages, st, in, plan)).append("\"}");
                }
            }
            jb.dagJson("{\"nodes\":[" + nodes + "],\"edges\":[" + edges + "]}");
            monitor2.onJobSubmit(jb.build());
        } catch (RuntimeException ignored) {
            // Advisory: an exception while collecting observations never reaches the job.
        }
    }

    private void pushJobComplete(
            String jobId, RpcMessage result, DurableRunExecution durable) {
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP) {
            return;
        }
        try {
            boolean ok = result.type() == RpcMessage.Type.JOB_RESULT && result.success();
            io.rowweave.plan.ExecutionReport report = ok
                    ? executionReportForMonitor(result, durable) : null;
            monitor2.onJobComplete(jobId, ok ? "FINISHED" : "FAILED",
                    ok ? null : result.errorMessage(), report);
        } catch (RuntimeException ignored) {
        }
    }

    private io.rowweave.plan.ExecutionReport executionReportForMonitor(
            RpcMessage result, DurableRunExecution durable) {
        long rowsOut = result.rowCount();
        if (durable == null) {
            try {
                return result.executionTelemetrySummary().toExecutionReport(rowsOut);
            } catch (RuntimeException | LinkageError unavailableTelemetry) {
                return io.rowweave.plan.ExecutionTelemetrySummary.unavailable()
                        .toExecutionReport(rowsOut);
            }
        }
        RunSnapshot snapshot = durable.snapshot;
        io.rowweave.run.RunResultSummary decimal = snapshot.resultSummary();
        if (decimal != null) rowsOut = decimal.rowsOut();
        try {
            io.rowweave.run.DurableExecutionEvidenceView evidence =
                    durableExecutionEvidenceAuthority == null ? null
                            : durableExecutionEvidenceAuthority.executionEvidence(
                                    durable.runId);
            if (evidence != null && evidence.snapshot() != null
                    && evidence.snapshot().resultSummary() != null) {
                rowsOut = evidence.snapshot().resultSummary().rowsOut();
            }
            return DurableExecutionTelemetryResolver.resolve(evidence)
                    .toExecutionReport(rowsOut);
        } catch (RuntimeException | LinkageError unavailableTelemetry) {
            return io.rowweave.plan.ExecutionTelemetrySummary.unavailable()
                    .toExecutionReport(rowsOut);
        }
    }

    private void pushJobCompleteError(String jobId, String error) {
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP) {
            return;
        }
        try {
            monitor2.onJobComplete(jobId, "FAILED", error);
        } catch (RuntimeException ignored) {
        }
    }

    private void pushBatchSinkUpdate(
            String jobId, PlanGraph graph, DurableRunExecution durable) {
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP
                || durable == null) {
            return;
        }
        try {
            boolean qualityBounded = durable.assignmentPlan.manifest()
                    .hasQualityGateBinding();
            io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                    qualityBounded ? stageAssignmentAuthority
                            .qualityGateCommitJournal(durable.fence()) : null;
            ManifestCommitJournalSnapshot journal = qualityBounded ? null
                    : stageAssignmentAuthority.manifestCommitJournal(durable.fence());
            int locatorClosureCount = qualityBounded
                    ? qualityJournal == null ? durable.sinkLocatorClosureCount
                    : qualityJournal.memberCount()
                    : journal == null ? durable.sinkLocatorClosureCount
                    : journal.memberCount();
            io.rowweave.commit.BatchSinkOutcome outcome =
                    projectBatchSinkOutcome(durable.snapshot, graph);
            monitor2.onBatchSinkUpdate(jobId, batchSinkMonitorView(
                    graph, true, locatorClosureCount, outcome.state().name()));
        } catch (RuntimeException ignored) {
            // advisory:authority or connector projection failures never affect execution.
        }
    }

    private static io.rowweave.monitor.BatchSinkView batchSinkMonitorView(
            PlanGraph graph, boolean durable, int locatorClosureCount,
            String outcomeState) {
        java.util.List<String> profiles = new java.util.ArrayList<>();
        java.util.List<String> digestPrefixes = new java.util.ArrayList<>();
        String firstDomain = null;
        boolean crossDomain = false;
        for (int index = 0; index < graph.nodeCount(); index++) {
            PlanNode node = graph.node(index);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            io.rowweave.BatchSinkDescriptor descriptor =
                    node.batchSinkDescriptor();
            profiles.add(descriptor.capabilityProfile().name());
            digestPrefixes.add(hexPrefix(
                    io.rowweave.BatchSinkDescriptorCodec.digest(descriptor), 8));
            if (firstDomain == null) {
                firstDomain = descriptor.commitDomainId();
            } else if (!firstDomain.equals(descriptor.commitDomainId())) {
                crossDomain = true;
            }
        }
        String domainDisposition = profiles.isEmpty()
                ? io.rowweave.monitor.BatchSinkView.DOMAIN_NONE
                : (crossDomain
                ? io.rowweave.monitor.BatchSinkView.DOMAIN_CROSS
                : io.rowweave.monitor.BatchSinkView.DOMAIN_SAME);
        return io.rowweave.monitor.BatchSinkView.of(
                profiles.size(), profiles, domainDisposition, digestPrefixes,
                locatorClosureCount,
                durable && outcomeState == null
                        ? io.rowweave.commit.BatchSinkOutcome.State.PREPARING.name()
                        : outcomeState);
    }

    /**
     * Applies a progress report to one instance: rows handled in, rows produced, and state, where a stage that is running is RUNNING and
     * one still waiting for data is READY.
     */
    private void pushStageProgress(RpcMessage req) {
        pushInstance(req.stageId(), req.success() ? "RUNNING" : "READY", req.inputRows(), req.rowCount(), null);
    }

    /**
     * Applies a stage report, which is the instance's terminal state, FINISHED or FAILED, together with the rows it handled and the rows it
     * produced.
     */
    private void pushStageUpdate(RpcMessage req) {
        pushInstance(req.stageId(), req.success() ? "FINISHED" : "FAILED", req.inputRows(), req.rowCount(),
                (!req.success()) ? req.errorMessage() : null);
    }

    /**
     * Records the state of one instance of a stage and rebuilds the snapshot of the stage as a whole. The stage's row count is the sum over
     * instances; its state is FINISHED only once every instance finished, FAILED if any failed and RUNNING otherwise; the instance list
     * carries each one's details.
     */
    private void pushInstance(long stageId, String state, long inRows, long outRows, String error) {
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP) return;
        try {
            StageRef ref = stageRefs.get(stageId);
            if (ref == null) return;
            String key = ref.monitorJobId + "#" + ref.stageIndex;
            InstanceState[] arr = stageInstances.computeIfAbsent(key, k -> {
                InstanceState[] a = new InstanceState[Math.max(1, ref.parallelism)];
                for (int i = 0; i < a.length; i++) a[i] = new InstanceState();
                return a;
            });
            int idx = Math.min(ref.instanceIndex, arr.length - 1);
            long now = System.currentTimeMillis();
            // A start time is recorded only on the first transition into RUNNING, which is when the first row
            // arrives. Waiting in READY does not count.
            if (arr[idx].startTime == 0 && ("RUNNING".equals(state) || "FINISHED".equals(state) || "FAILED".equals(state))) {
                arr[idx].startTime = now;
            }
            arr[idx].state = state; arr[idx].rowCount = outRows; arr[idx].inputRows = inRows; arr[idx].worker = ref.workerId;
            if ("FINISHED".equals(state) || "FAILED".equals(state)) arr[idx].endTime = now;

            long aggOut = 0, aggIn = 0; boolean anyFail = false, allDone = true, anyRunning = false;
            long minStart = Long.MAX_VALUE, maxEnd = 0;  // a stage's span runs from the earliest instance start to the latest instance end
            for (InstanceState is : arr) {
                aggOut += is.rowCount; aggIn += is.inputRows;
                if ("FAILED".equals(is.state)) anyFail = true;
                if ("RUNNING".equals(is.state)) anyRunning = true;
                if (!"FINISHED".equals(is.state) && !"FAILED".equals(is.state)) allDone = false;
                if (is.startTime > 0) minStart = Math.min(minStart, is.startTime);
                maxEnd = Math.max(maxEnd, is.endTime);
            }
            // A stage is FAILED if any instance failed, FINISHED if all reached a terminal state, RUNNING if any is
            // processing, and READY while all are still waiting.
            String stageState = anyFail ? "FAILED" : (allDone ? "FINISHED" : (anyRunning ? "RUNNING" : "READY"));
            boolean terminal = anyFail || allDone;
            long stageStart = (minStart == Long.MAX_VALUE) ? 0 : minStart;  // when the first instance saw its first row, or zero meaning processing has not started
            long stageEnd = terminal ? maxEnd : 0;  // a stage has ended only once every instance did, which is the latest of them

            io.rowweave.monitor.StageView.Builder sv = io.rowweave.monitor.StageView.builder(ref.stageIndex)
                    .role(ref.role).opChain(ref.opChain).workerId(ref.workerId)
                    .state(stageState).outputRows(aggOut).inputRows(aggIn).parallelism(arr.length)
                    .startTime(stageStart).endTime(stageEnd);
            ref.sourceSummary.apply(sv);
            if (ref.inputs != null) {
                for (int ci = 0; ci < ref.inputs.size(); ci++) {
                    String ch = (ref.inputChannels != null && ci < ref.inputChannels.size()) ? ref.inputChannels.get(ci) : "remote";
                    sv.addInput(ref.inputs.get(ci), ch);
                }
            }
            if (error != null) sv.error(error);
            for (int i = 0; i < arr.length; i++) {
                sv.addInstance(new io.rowweave.monitor.StageView.InstanceView(
                        i, arr[i].worker, arr[i].state, arr[i].rowCount, arr[i].startTime, arr[i].endTime, arr[i].inputRows));
            }
            monitor2.onStageUpdate(ref.monitorJobId, sv.build());
        } catch (RuntimeException ignored) {
        }
    }

    /**
     * Drops the stage references of one monitored job once it has finished, so a long-lived master does not accumulate them.
     */
    private void clearStageRefs(String monitorJobId) {
        inProcessLegacyJobs.remove(monitorJobId);
        if (monitor2 == io.rowweave.monitor.JobMonitorRegistry.NOOP) {
            return;
        }
        stageRefs.values().removeIf(r -> r.monitorJobId.equals(monitorJobId));
        stageInstances.keySet().removeIf(k -> k.startsWith(monitorJobId + "#"));
    }

    /**
     * The operator chain of a stage, as type ids in order from nearest the source to nearest the sink, read from the plan's member nodes.
     */
    private static String opChainOf(PlanGraph graph, Stage st) {
        StringBuilder sb = new StringBuilder();
        for (int idx : st.members()) {
            if (sb.length() > 0) sb.append('>');
            sb.append(graph.node(idx).typeId());
        }
        return sb.toString();
    }

    /** Frozen descriptor summary for monitor only; the full source identity never leaves the plan. */
    private static SourceMonitorSummary sourceMonitorSummary(PlanGraph graph, Stage stage) {
        java.security.MessageDigest digest;
        try {
            digest = java.security.MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
        digest.update("rowweave.source-monitor.v1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        int splitCount = 0;
        boolean exact = false;
        boolean singleAttempt = false;
        for (int nodeIndex : stage.members()) {
            PlanNode node = graph.node(nodeIndex);
            if (node.kind() != PlanNode.Kind.SOURCE
                    || node.sourceSplitBindings().isEmpty()) {
                continue;
            }
            updateDigestInt(digest, nodeIndex);
            updateDigestInt(digest, node.sourceSplitBindings().size());
            splitCount = Math.addExact(splitCount, node.sourceSplitBindings().size());
            for (io.rowweave.BatchSourceSplitBinding binding
                    : node.sourceSplitBindings()) {
                byte[] frame = io.rowweave.BatchSourceSplitDescriptorCodec.encode(
                        binding.descriptor());
                updateDigestInt(digest, frame.length);
                digest.update(frame);
                if (binding.descriptor().replayGuarantee()
                        == io.rowweave.BatchSourceSplitDescriptor.ReplayGuarantee
                        .EXACT_ORDERED_FROM_START) {
                    exact = true;
                } else {
                    singleAttempt = true;
                }
            }
        }
        if (splitCount == 0) return SourceMonitorSummary.NONE;
        String replayMode = exact && singleAttempt
                ? io.rowweave.monitor.StageView.SOURCE_REPLAY_MIXED
                : (exact ? io.rowweave.monitor.StageView.SOURCE_REPLAY_EXACT
                : io.rowweave.monitor.StageView.SOURCE_REPLAY_SINGLE_ATTEMPT);
        return new SourceMonitorSummary(
                splitCount, replayMode, digestPrefix(digest.digest()));
    }

    private static void updateDigestInt(java.security.MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static String digestPrefix(byte[] digest) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] prefix = new char[16];
        for (int i = 0; i < 8; i++) {
            prefix[i * 2] = digits[(digest[i] >>> 4) & 0x0f];
            prefix[i * 2 + 1] = digits[digest[i] & 0x0f];
        }
        return new String(prefix);
    }

    private static String firstWorkerOf(PhysicalPlan plan, int logicalStage) {
        java.util.List<PhysicalPlan.Instance> insts = plan.instancesOf(logicalStage);
        return insts.isEmpty() ? null : insts.get(0).workerId();
    }

    /**
     * Observation for tests: the plan built for the last submission, so a test can assert the instance count really fans out rather than
     * collapsing to one.
     */
    private volatile PhysicalPlan lastPhysicalPlan;
    private volatile ShuffleTransportPlan lastShuffleTransportPlan;
    private volatile ShuffleGrantPlan lastShuffleGrantPlan;

    public PhysicalPlan lastPhysicalPlan() {
        return lastPhysicalPlan;
    }

    public ShuffleTransportPlan lastShuffleTransportPlan() {
        return lastShuffleTransportPlan;
    }

    public ShuffleGrantPlan lastShuffleGrantPlan() {
        return lastShuffleGrantPlan;
    }

    private long runtimeStageId(DurableStageAssignmentPlan durableAssignments,
                                int logicalStage, int instanceIndex) {
        return durableAssignments == null
                ? stageSeq.incrementAndGet()
                : durableAssignments.runtimeStageId(logicalStage, instanceIndex);
    }

    /**
     * The general scheduler for a graph of many stages, including a chain of any number of hops. Stages are assigned in reverse topological
     * order, since the graph lists them with the source first and the sink last, so a stage's downstream is always assigned before it and
     * its endpoints are already known. At parallelism one each shuffle edge is a single connection, with no partitioning.
     *
     * <p>Role mapping: a SINK stage becomes the sink command, which reads an inbound, runs a fragment ending in the write and hands rows to
     * the Sink; an intermediate stage becomes the keyed-operator command, which reads an inbound, runs a fragment and connects onwards to
     * one downstream; a stage headed by a source becomes the source-stage command, which reads its Source, runs a fragment and connects to
     * one downstream.
     */
    private RpcMessage runDagAsync(PlanGraph graph, ExecutionGraph eg, PhysicalPlan plan,
                                   java.util.List<RegistryWorker> plannedWorkers,
                                   java.util.List<String> workerIds, String monitorJobId,
                                   ManifestJobCommit manifestCommit,
                                   RunAttemptFence runAttemptFence,
                                   DurableRunExecution durable,
                                   RpcMessage residentRequest,
                                   EphemeralSubmitDeadline ephemeralDeadline,
                                   byte[] portableArtifactBytes) throws Exception {
        requireActiveLeader();
        SecureShuffleBindings secureShuffle = allAuthenticatedMux(plannedWorkers)
                ? SecureShuffleBindings.create(eg, plan, plannedWorkers,
                        runAttemptFence, monitorJobId, objectReuse)
                : null;
        DagAssignmentContext assignmentContext =
                new DagAssignmentContext(plannedWorkers, runAttemptFence,
                        durable == null ? null : durable.assignmentPlan,
                        durable == null ? null : durable.publicationId,
                        manifestCommit == null ? 0L : manifestCommit.generation,
                        durable == null ? null : durable.recoveryToken,
                        durable == null ? null : durable.frontierToken,
                        residentRequest, ephemeralDeadline, secureShuffle,
                        graph, eg, plan, portableArtifactBytes);
        this.lastShuffleTransportPlan = secureShuffle == null
                ? null : secureShuffle.transportPlan;
        this.lastShuffleGrantPlan = secureShuffle == null
                ? null : secureShuffle.grantPlan;
        DurableStageAssignmentPlan durableAssignments =
                durable == null ? null : durable.assignmentPlan;
        // The credit window that bounds backpressure is measured in bytes on the wire.
        int window = 1 << 20;  // a budget of one megabyte of bytes on the wire
        java.util.List<Stage> stages = eg.stages();
        // A stage and one of its inbound edges, keyed together, to the listening endpoints of every instance of
        // that edge. A join stage has two, left as 0 and right as 1, each with one endpoint per instance; a stage
        // with one input, and a sink, have edge 0 alone. An upstream stage chooses by which edge it is to its
        // downstream, and takes the partition keys that go with it.
        java.util.Map<Long, java.util.List<io.rowweave.sched.net.ShuffleEndpoint>> inboundEps =
                new java.util.HashMap<>();
        // Alongside the endpoints, the transport descriptor of every instance on each inbound edge: which worker
        // it is on, its socket endpoint, which is absent when it is reachable only locally, and its local
        // rendezvous key. An upstream builds its per-partition targets from this, choosing local for an instance
        // on its own worker and a socket for any other. Only a sink or a keyed stage with a single inbound edge
        // is filled in; a join or union still takes the socket path it always did.
        java.util.Map<Long, java.util.List<InboundInstance>> inboundInsts = new java.util.HashMap<>();
        java.util.List<Long> allJobIds = new java.util.ArrayList<>();
        // A graph with several sinks gets one tracker per sink and their row counts summed; with one sink this
        // is a list of one.
        java.util.List<StageTracker> sinkReports = new java.util.ArrayList<>();
        java.util.List<Long> sinkStageIds = new java.util.ArrayList<>();
        String commitGroupId = manifestCommit != null
                ? "manifest:" + manifestCommit.attemptId
                : (durable != null ? "durable:" + monitorJobId
                        : (sinkNodeCount(graph) > 1 ? monitorJobId : null));
        // Serializing a job's user objects stays single-threaded, the way it is on the local path; calls made
        // outside the lock still overlap.
        Object serializationMutex = new Object();
        boolean fullMuxBatch = secureShuffle == null
                && workerIds.stream().allMatch(assignmentContext::muxOn);
        boolean canonicalizeSinkOutput = residentRequest == null
                || residentRequest.inProcessCanonicalizeOutput() == null
                || residentRequest.inProcessCanonicalizeOutput();
        java.util.List<PendingStageAssignment> deferredMuxAssignments = new java.util.ArrayList<>();
        java.util.List<java.util.List<PendingStageAssignment>> deferredSecureWaves =
                new java.util.ArrayList<>();
        AssignmentProfile assignmentProfile = new AssignmentProfile();
        try {

        // A timing probe behind rowweave.profile.timing splits the wall clock between assigning, with its
        // barriers, and waiting for the job, which names where the time goes. It costs nothing while the
        // property is unset.
        boolean profTiming = Boolean.getBoolean("rowweave.profile.timing");
        long assignStart = profTiming ? System.nanoTime() : 0;
        // Reverse topological order, from near the sink back towards the source, which is what guarantees that
        // when a stage is assigned its downstream already is, so its endpoints are known.
        boolean monitorOn = monitor2 != io.rowweave.monitor.JobMonitorRegistry.NOOP;
        for (int si = stages.size() - 1; si >= 0; si--) {
            Stage stage = stages.get(si);
            java.util.List<PhysicalPlan.Instance> myInstances = plan.instancesOf(stage.index());
            PlanNode head = graph.node(stage.members().get(0));
            int jobIdsBefore = allJobIds.size();  // where the range of stage ids this assignment begins
            // A binary node with two keyed inbound edges: a join partitions on a numerically normalised key, a
            // coGroup on the keys as declared, which are scale sensitive. A union has no key, concatenates every
            // input into one instance, and runs as a single instance.
            boolean isJoinStage = head.kind() == PlanNode.Kind.BINARY && "join".equals(head.typeId());
            boolean isCoGroupStage = head.kind() == PlanNode.Kind.BINARY && "coGroup".equals(head.typeId());
            boolean isUnionStage = head.kind() == PlanNode.Kind.BINARY && "union".equals(head.typeId());
            if (head.kind() == PlanNode.Kind.BINARY && !ClusterPlanCapabilityValidator.supportsBinary(head)) {
                // The normal entry point already pre-checks in a region with no side effects; where that check is
                // repeated here, a refusal still has to reach the same cancellation and cleanup.
                throw ClusterPlanCapabilityValidator.unsupportedBinary(head);
            }

            if (stage.isSink()) {
                DataSchema inMeta = stageInMeta(graph, stage);
                int upstreamInstances = upstreamInstanceCount(stage, stages, plan);
                // For a SINK stage, the member that is the sink is the last one, since members run source towards sink.
                int sinkNodeIdx = stage.members().get(stage.members().size() - 1);
                PlanNode sinkNode = graph.node(sinkNodeIdx);
                boolean publicationRequiresCanonicalOrder =
                        io.rowweave.plan.TypedQualityPublicationPlanValidator
                                .isPublicationSinkRole(
                                        sinkNode.param(PlanNode.OPERATOR_UID_PARAM));
                // Deterministic output: M upstream instances fan into one sink, and the cross-process
                // arrival order is not deterministic, so a total-order OrderBy tail canonicalizes it
                // (unless the sink stage already carries an explicit orderBy/top). The quality publication
                // protocol always forces the canonical tail, and a local performance hint on an ordinary
                // Sink may never weaken its byte-stable member contract.
                PlanGraph frag = (canonicalizeSinkOutput || publicationRequiresCanonicalOrder)
                        && needsCanonicalize(graph, stage)
                        ? appendOrderByAllFields(stageFragment(graph, stage, inMeta), inMeta)
                        : stageFragment(graph, stage, inMeta);
                // In a graph with several sinks, each SINK stage writes through the output of its own SINK node rather
                // than one shared graph root. With a single sink that root is exactly what it degenerates to.
                io.rowweave.Sink output = sinkNode.output();
                long jobId = runtimeStageId(durableAssignments, stage.index(),
                        myInstances.get(0).instanceIndex());
                allJobIds.add(jobId);
                // Assigning in reverse topological order would visit the sinks in reverse; inserting at the front
                // restores the order the plan gives them, which is stable.
                sinkStageIds.add(0, jobId);
                StageTracker tracker = new StageTracker(
                        stage.index(), 0, StageCommand.Role.SINK, myInstances.size());
                stageTrackers.put(jobId, tracker);
                sinkReports.add(0, tracker);  // keeps the same stable order as the sinks themselves
                // One instance of a sink's inbound, where an upstream on the same worker is reached locally. The local
                // count is how many of the upstream's instances share the sink's worker.
                String sinkWorker = myInstances.get(0).workerId();
                int sinkLocalConns = secureShuffle == null
                        ? (objectReuse ? coLocatedUpstream(
                                graph, stages, stage, 0, plan, sinkWorker) : 0)
                        : secureShuffle.localProducerCount(stage.index(), 0, 0);
                String sinkLocalKey = sinkLocalConns > 0
                        ? (secureShuffle == null
                                ? localKey(monitorJobId, stage.index(), 0, 0)
                                : secureShuffle.localKey(stage.index(), 0, 0))
                        : null;
                io.rowweave.Sink workerOutput = durableAssignments != null
                        && output instanceof io.rowweave.ManifestFileSink
                        ? ((io.rowweave.ManifestFileSink) output)
                        .toProviderSink() : output;
                StageCommand sinkCmd = StageCommand.sink(
                        inMeta, window, upstreamInstances, frag, workerOutput);
                if (commitGroupId != null) {
                    sinkCmd.withCommitGroupId(commitGroupId);
                }
                if (output instanceof io.rowweave.ManifestFileSink) {
                    if (manifestCommit == null) {
                        throw new IllegalStateException("manifest sink missing job commit context");
                    }
                    sinkCmd.withManifestCommitContext(
                            manifestCommit.attemptId, manifestCommit.generation);
                }
                if (sinkLocalConns > 0) {
                    sinkCmd.withLocalInbound(sinkLocalConns, sinkLocalKey);
                }
                if (secureShuffle != null
                        && !secureShuffle.authorizations(stage.index(), 0).isEmpty()) {
                    sinkCmd.withInboundAuthorizations(
                            secureShuffle.authorizations(stage.index(), 0));
                }
                // A listening port is only needed when some upstream producer arrives over a socket. When all are local
                // the socket count is zero, the worker binds nothing, so there is no port to report and the assignment
                // must not wait for one.
                boolean sinkHasSocket = (upstreamInstances - sinkLocalConns) > 0;
                io.rowweave.sched.net.ShuffleEndpoint sinkEp = null;
                if (sinkHasSocket && assignmentContext.muxOn(sinkWorker)) {
                    // Multiplexing: a routing key is generated, handed to the downstream so it can register, the endpoint is
                    // built from the worker's data port and that key, and the stage is assigned at once rather than waiting
                    // for a port report.
                    java.util.UUID tid = secureShuffle == null
                            ? java.util.UUID.randomUUID()
                            : secureShuffle.targetId(stage.index(), 0, 0);
                    sinkCmd.withInboundTargetId(tid);
                    scheduleAssignments(java.util.Collections.singletonList(
                                    assignmentContext.pending(stage.index(), 0,
                                            sinkWorker, jobId, sinkCmd)),
                            serializationMutex, fullMuxBatch, deferredMuxAssignments,
                            deferredSecureWaves, secureShuffle != null, assignmentProfile);
                    RegistryWorker sinkEndpoint = assignmentContext.worker(sinkWorker);
                    sinkEp = secureShuffle == null
                            ? muxEndpoint(sinkEndpoint, tid)
                            : secureShuffle.safeEndpoint(stage.index(), 0, 0);
                    inboundEps.put(edgeKey(stage.index(), 0), java.util.Collections.singletonList(sinkEp));
                } else if (sinkHasSocket) {
                    int port = assignListening(assignmentContext.worker(sinkWorker), jobId,
                            assignmentContext.bind(sinkCmd, jobId), assignmentProfile,
                            assignmentContext);
                    sinkEp = new io.rowweave.sched.net.ShuffleEndpoint(
                            assignmentContext.worker(sinkWorker).host(), port);
                    inboundEps.put(edgeKey(stage.index(), 0), java.util.Collections.singletonList(sinkEp));
                } else {
                    scheduleAssignments(java.util.Collections.singletonList(
                                    assignmentContext.pending(stage.index(), 0,
                                            sinkWorker, jobId, sinkCmd)),
                            serializationMutex, fullMuxBatch, deferredMuxAssignments,
                            deferredSecureWaves, secureShuffle != null, assignmentProfile);
                }
                inboundInsts.put(edgeKey(stage.index(), 0), java.util.Collections.singletonList(
                        new InboundInstance(sinkWorker, sinkEp, sinkLocalKey)));
            } else if (isJoinStage || isCoGroupStage) {
                // A binary join or coGroup stage in the middle of a graph: two inbound edges fan in, and its output is
                // partitioned by the grouping keys of the stage below. A coGroup carries its user function.
                assignJoinStage(graph, stage, head, stages, plan, myInstances, inboundEps, inboundInsts,
                        allJobIds, window, monitorJobId, serializationMutex,
                        fullMuxBatch, deferredMuxAssignments, deferredSecureWaves,
                        assignmentProfile, assignmentContext,
                        durableAssignments, secureShuffle);
            } else if (isUnionStage) {
                // A union is one instance with no key, concatenating every input into one output. Its single inbound
                // accepts connections from all upstream instances on both sides, and a pass-through fragment fans out
                // downstream. Producers on the same worker as the union are reached locally, which means one local
                // count and both edges mapped to the same inbound instance.
                assignUnionStage(graph, stage, stages, plan, inboundEps, inboundInsts, allJobIds, window,
                        monitorJobId, serializationMutex, fullMuxBatch, deferredMuxAssignments, assignmentProfile,
                        deferredSecureWaves, assignmentContext, durableAssignments, secureShuffle);
            } else {
                // The endpoints this stage fans out to downstream, and the keys that partition into them; the downstream
                // decides the fan-out strategy, not the producer.
                DownstreamFanout down = downstreamFanout(graph, stage, stages, inboundEps, plan);
                DataSchema inMeta = stageInMeta(graph, stage);
                DataSchema outMeta = graph.node(stage.members().get(stage.members().size() - 1)).outMeta();
                int upstreamInstances = upstreamInstanceCount(stage, stages, plan);
                PlanGraph frag = stageFragment(graph, stage, inMeta);
                java.util.List<io.rowweave.sched.net.ShuffleEndpoint> myEps = new java.util.ArrayList<>();
                java.util.List<InboundInstance> myInsts = new java.util.ArrayList<>();
                java.util.List<PendingStageAssignment> assignmentWave = new java.util.ArrayList<>();
                java.util.List<String> keyedWorkers = new java.util.ArrayList<>();
                java.util.List<String> keyedLocalKeys = new java.util.ArrayList<>();
                java.util.List<io.rowweave.sched.net.ShuffleEndpoint> keyedKnownEndpoints = new java.util.ArrayList<>();
                java.util.List<StagePortTracker> keyedPortTrackers = new java.util.ArrayList<>();
                // For a stage headed by a source, the splits of that source are shared out round-robin across its R
                // instances, so reading a source is itself parallel. The same sharding the local path uses, giving byte
                // for byte identical output; R is the instance count, the smaller of the parallelism and the split count.
                java.util.List<java.util.List<io.rowweave.BatchSourceSplitBinding>> srcShards =
                        stage.inputStages().isEmpty()
                        ? io.rowweave.plan.InputSharding.shardSourceBindings(
                                sourceOf(graph, stage).sourceSplitBindings(), myInstances.size())
                        : null;
                int srcIdx = 0;
                for (PhysicalPlan.Instance inst : myInstances) {
                    long jobId = runtimeStageId(durableAssignments,
                            stage.index(), inst.instanceIndex());
                    allJobIds.add(jobId);
                    stageTrackers.put(jobId, new StageTracker(
                            stage.index(), inst.instanceIndex(),
                            stage.inputStages().isEmpty()
                                    ? StageCommand.Role.SOURCE_STAGE
                                    : StageCommand.Role.KEYED_OP,
                            myInstances.size()));
                    // This instance's per-partition targets on the worker it runs on, local for an instance of the
                    // downstream that shares its worker and a socket for any other. Absent means the socket path alone.
                    java.util.List<ShuffleTarget> targets = secureShuffle == null
                            ? (objectReuse ? downTargetsFor(
                                    graph, stage, stages, inboundInsts, inst.workerId()) : null)
                            : secureDownTargetsFor(stage, inst.instanceIndex(), stages,
                                    plan, secureShuffle);
                    // An ordinary producer builds a group for every pair of consumer stage and edge, exactly as a binary
                    // does. Whether a row is broadcast must not depend on the row-reuse setting, which decides only whether
                    // a target is local or a socket, never whether a consumer edge exists.
                    java.util.List<StageCommand.BroadcastGroup> bgroups = broadcastGroupsForConsumers(
                            graph, stage, stages, inboundEps, inboundInsts, plan,
                            inst.workerId(), inst.instanceIndex(), secureShuffle);
                    int fixedDownPartition = fixedPartitionForFirstConsumer(
                            graph, stage, stages, plan,
                            inst.instanceIndex());
                    if (stage.inputStages().isEmpty()) {
                        // A stage headed by a source: instance i reads the subset of splits assigned to it, concatenated lazily,
                        // runs the fragment and fans out downstream. Each instance reads only its own subset, which both stops
                        // a row being read by everyone and parallelizes reading a source.
                        java.util.List<io.rowweave.BatchSourceSplitBinding> myShard =
                                srcShards.get(srcIdx++);
                        io.rowweave.Source input =
                                new io.rowweave.AssignedSourceBatch(myShard);
                        StageCommand srcCmd = StageCommand.sourceStageFanout(
                                inMeta, outMeta, window, input, frag,
                                down.endpoints, down.keys,
                                down.normalized);
                        if (fixedDownPartition >= 0) {
                            srcCmd.withDownFixedPartition(
                                    fixedDownPartition,
                                    targets == null
                                            ? down.endpoints.size()
                                            : targets.size());
                        }
                        if (durableAssignments != null) {
                            srcCmd.withSourceSplitAssignment(
                                    durableAssignments.sourceAssignment(jobId));
                        }
                        if (targets != null) {
                            srcCmd.withDownTargets(targets).withObjectReuse(objectReuse);
                        }
                        if (bgroups != null) {
                            srcCmd.withBroadcastGroups(bgroups).withObjectReuse(objectReuse);  // a fan-out broadcast to every group
                        }
                        assignmentWave.add(assignmentContext.pending(
                                stage.index(), inst.instanceIndex(),
                                inst.workerId(), jobId, srcCmd));
                    } else {
                        // A keyed stage in the middle of a graph, with P instances: its inbound accepts the upstream's
                        // connections, it runs its fragment, and its output is partitioned by the keys of the stage below. Of
                        // this instance's inbound, the producers on the same worker are local and the rest arrive over sockets.
                        int localConns = secureShuffle == null
                                ? (objectReuse ? coLocatedUpstream(
                                        graph, stages, stage, 0, plan, inst.workerId()) : 0)
                                : secureShuffle.localProducerCount(
                                        stage.index(), 0, inst.instanceIndex());
                        String myLocalKey = localConns > 0
                                ? (secureShuffle == null
                                        ? localKey(monitorJobId, stage.index(),
                                                inst.instanceIndex(), 0)
                                        : secureShuffle.localKey(stage.index(), 0,
                                                inst.instanceIndex()))
                                : null;
                        StageCommand keyedCmd = StageCommand.keyedOpFanout(
                                inMeta, outMeta, window, upstreamInstances,
                                frag, down.endpoints, down.keys,
                                down.normalized);
                        if (fixedDownPartition >= 0) {
                            keyedCmd.withDownFixedPartition(
                                    fixedDownPartition,
                                    targets == null
                                            ? down.endpoints.size()
                                            : targets.size());
                        }
                        if (localConns > 0) {
                            keyedCmd.withLocalInbound(localConns, myLocalKey);
                        }
                        if (targets != null) {
                            keyedCmd.withDownTargets(targets).withObjectReuse(objectReuse);
                        }
                        if (bgroups != null) {
                            keyedCmd.withBroadcastGroups(bgroups).withObjectReuse(objectReuse);  // a fan-out broadcast to every group
                        }
                        if (secureShuffle != null
                                && !secureShuffle.authorizations(
                                        stage.index(), inst.instanceIndex()).isEmpty()) {
                            keyedCmd.withInboundAuthorizations(
                                    secureShuffle.authorizations(
                                            stage.index(), inst.instanceIndex()));
                        }
                        boolean hasSocket = (upstreamInstances - localConns) > 0;
                        io.rowweave.sched.net.ShuffleEndpoint ep = null;
                        StagePortTracker tracker = null;
                        if (hasSocket && assignmentContext.muxOn(inst.workerId())) {
                            // Multiplexing: the routing key travels with the assignment, the endpoint is the muxed one, and the
                            // stage is assigned at once rather than waiting for a port report.
                            java.util.UUID tid = secureShuffle == null
                                    ? java.util.UUID.randomUUID()
                                    : secureShuffle.targetId(
                                            stage.index(), 0, inst.instanceIndex());
                            keyedCmd.withInboundTargetId(tid);
                            RegistryWorker keyedEndpoint = assignmentContext.worker(inst.workerId());
                            ep = secureShuffle == null
                                    ? muxEndpoint(keyedEndpoint, tid)
                                    : secureShuffle.safeEndpoint(
                                            stage.index(), 0, inst.instanceIndex());
                        } else if (hasSocket) {
                            tracker = new StagePortTracker(true, false);
                            portTrackers.put(jobId, tracker);
                        }
                        keyedPortTrackers.add(tracker);
                        assignmentWave.add(assignmentContext.pending(
                                stage.index(), inst.instanceIndex(),
                                inst.workerId(), jobId, keyedCmd));
                        keyedWorkers.add(inst.workerId());
                        keyedLocalKeys.add(myLocalKey);
                        keyedKnownEndpoints.add(ep);
                    }
                }
                scheduleAssignments(assignmentWave, serializationMutex, fullMuxBatch,
                        deferredMuxAssignments, deferredSecureWaves,
                        secureShuffle != null, assignmentProfile);
                if (!stage.inputStages().isEmpty()) {
                    for (int i = 0; i < keyedWorkers.size(); i++) {
                        io.rowweave.sched.net.ShuffleEndpoint ep = keyedKnownEndpoints.get(i);
                        StagePortTracker tracker = keyedPortTrackers.get(i);
                        if (tracker != null) {
                            long t = System.nanoTime();
                            int port = tracker.await(
                                    assignmentContext.setupWaitMillis());
                            assignmentProfile.portWaitNanos.addAndGet(System.nanoTime() - t);
                            ep = new io.rowweave.sched.net.ShuffleEndpoint(
                                    assignmentContext.worker(keyedWorkers.get(i)).host(), port);
                        }
                        if (ep != null) {
                            myEps.add(ep);
                        }
                        myInsts.add(new InboundInstance(
                                keyedWorkers.get(i), ep, keyedLocalKeys.get(i)));
                    }
                }
                if (!myEps.isEmpty()) {
                    inboundEps.put(edgeKey(stage.index(), 0), myEps);
                }
                if (!myInsts.isEmpty()) {
                    inboundInsts.put(edgeKey(stage.index(), 0), myInsts);
                }
            }
            // Every stage id assigned for this stage maps to its reference, carrying the instance index and the
            // parallelism, so a stage report or a progress report can find the instance and push an update.
            if (monitorOn) {
                String role = stage.isSink() ? "SINK" : (stage.inputStages().isEmpty() ? "SOURCE" : "STAGE");
                String opChain = opChainOf(graph, stage);
                java.util.List<PhysicalPlan.Instance> insts = plan.instancesOf(stage.index());
                int par = insts.size();
                long startTime = System.currentTimeMillis();
                java.util.List<String> chans = new java.util.ArrayList<>();
                for (int in : stage.inputStages()) {
                    chans.add(edgeChannelType(graph, stages, stage, in, plan));
                }
                int inst = 0;
                SourceMonitorSummary sourceSummary = sourceMonitorSummary(graph, stage);
                for (int k = jobIdsBefore; k < allJobIds.size(); k++) {
                    String iwid = inst < insts.size() ? insts.get(inst).workerId() : (par > 0 ? insts.get(0).workerId() : null);
                    stageRefs.put(allJobIds.get(k),
                            new StageRef(monitorJobId, stage.index(), role, opChain,
                                    stage.inputStages(), iwid, startTime, inst, par,
                                    chans, sourceSummary));
                    inst++;
                }
            }
        }
        if (secureShuffle != null) {
            secureShuffle.assertCommandsComplete();
            for (java.util.List<PendingStageAssignment> wave : deferredSecureWaves) {
                dispatchAssignments(wave, serializationMutex, assignmentProfile);
            }
        } else if (fullMuxBatch) {
            dispatchAssignments(deferredMuxAssignments, serializationMutex, assignmentProfile);
        }
        if (profTiming) {
            long assignMs = (System.nanoTime() - assignStart) / 1_000_000;
            long portWaitMs = assignmentProfile.portWaitNanos.get() / 1_000_000;
            long serMs = assignmentProfile.serializeNanos.get() / 1_000_000;
            long dispatchMs = assignMs - portWaitMs;  // dispatch on its own, serializing and calling, is the total less the wait
            long awaitStart = System.nanoTime();
            RpcMessage r = awaitJob(
                    allJobIds, sinkReports, sinkStageIds, commitGroupId, manifestCommit,
                    durable, ephemeralDeadline, residentRequest);
            long awaitMs = (System.nanoTime() - awaitStart) / 1_000_000;
            LOG.debug("prof stages={} assign+barrier={}ms [dispatch={}ms(serialize={}ms) portWait={}ms] awaitJob(data)={}ms",
                    stages.size(), assignMs, dispatchMs, serMs, portWaitMs, awaitMs);
            return r;
        }
        return awaitJob(allJobIds, sinkReports, sinkStageIds, commitGroupId, manifestCommit,
                durable, ephemeralDeadline, residentRequest);
        } catch (Throwable failure) {
            // A failure at planning, at serialization or partway through dispatch has to converge every stage
            // already registered before it, so that none is left orphaned.
            if (ephemeralDeadline != null
                    && ephemeralDeadline.executionExpired()) {
                RpcMessage timeout = settleEphemeralTimeout(
                        allJobIds, sinkStageIds, commitGroupId,
                        manifestCommit, ephemeralDeadline);
                cleanupStageTracking(allJobIds);
                return timeout;
            }
            boolean userCanceled = durable != null
                    && durable.userCancellationRequested();
            boolean deadlineReached = durable != null && !userCanceled
                    && durable.deadlineTimeoutRequested(true);
            String abortReason = userCanceled ? "USER_CANCELED"
                    : deadlineReached
                    ? io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE
                    : "ASSIGNMENT_DISPATCH_FAILED";
            long cleanupDeadline = ephemeralDeadline == null
                    ? deadlineAfterMillis(outputCommitTimeoutMs)
                    : ephemeralDeadline.terminalDeadlineNanos();
            Throwable cleanupFailure = rollbackOutputsBeforePublication(
                    commitGroupId, sinkStageIds, manifestCommit, durable,
                    cleanupDeadline, abortReason);
            if (cleanupFailure != null && cleanupFailure != failure) {
                failure.addSuppressed(cleanupFailure);
            }
            StageCancellationCoordinator.Result cancellation =
                    cancelPendingStages(allJobIds, cleanupDeadline);
            if (userCanceled) {
                boolean terminal = cleanupFailure == null && cancellation.complete()
                        && durable.completeUserCancellationIfProven();
                cleanupStageTracking(allJobIds);
                return RpcMessage.jobResult(false, 0, terminal
                        ? "run canceled by user"
                        : "run cancellation accepted but terminal evidence is incomplete");
            }
            if (deadlineReached) {
                boolean terminal = cleanupFailure == null && cancellation.complete()
                        && durable.completeTimeoutIfProven();
                cleanupStageTracking(allJobIds);
                return RpcMessage.jobResult(false, 0, terminal
                        ? "run deadline reached"
                        : "run deadline accepted but terminal evidence is incomplete");
            }
            cleanupStageTracking(allJobIds);
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            throw (Error) failure;
        }
    }

    /**
     * Admission with downgrade, using gang admission for the whole job. Returns the effective P, that is,
     * the P of the tier that was acquired successfully; 0 means no tier was feasible and the submission
     * fails. One single deadline bounds the total wait to admitTimeoutMs, so grace and downgrade retries
     * do not stack.
     *
     * <p><b>Downgrade off</b> (downgradeEnabled=false): only the demand for the desired P is acquired,
     * which is the plain behaviour — if it does not fit, wait until the timeout and return 0.
     * <p><b>Downgrade on</b>: the candidates are P = desired down to 1, each with its own per-worker demand,
     * highest first. Phase 1 waits min(graceMs, budget) for the desired P, hoping another job releases
     * enough slots to free the full P; acquiring it means effective = desired. If the grace expires without
     * that, phase 2 calls acquireBestFit to take the highest feasible tier atomically with the remaining
     * budget, which removes the race. Lowering P leaves the row multiset independent of P, because the sink
     * tail is a single canonicalizing instance, so a downgrade cannot change results.
     */
    private int admitWithDowngrade(String jobId, ExecutionGraph eg, int desiredP,
                                   java.util.List<String> workerIds, String target,
                                   long requestDeadlineNanos) throws InterruptedException {
        long deadlineNanos = deadlineAfterMillis(admitTimeoutMs);
        if (requestDeadlineNanos > 0L) {
            deadlineNanos = Math.min(deadlineNanos, requestDeadlineNanos);
        }
        java.util.Map<String, Integer> desiredDemand =
                ParallelismDowngrade.perWorkerDemand(eg, desiredP, workerIds, target);

        if (!downgradeEnabled) {
            // With the fallback off, the older meaning holds: only the desired parallelism is ever occupied, and if
            // the slots are short the caller waits until the timeout.
            return slotLedger.acquire(
                    jobId, desiredDemand, remainMs(deadlineNanos)) ? desiredP : 0;
        }

        // First phase: the desired parallelism waits out its grace, never past the overall deadline.
        long grace = Math.min(downgradeGraceMs, remainMs(deadlineNanos));
        if (grace > 0 && slotLedger.acquire(jobId, desiredDemand, grace)) {
            return desiredP;
        }
        // Second phase: the candidates, each worker's demand for parallelism from the desired value down to one,
        // ordered high to low, with the highest feasible tier taken atomically against the budget left.
        java.util.List<java.util.Map<String, Integer>> candidates = new java.util.ArrayList<>();
        for (int p = desiredP; p >= 1; p--) {
            candidates.add(ParallelismDowngrade.perWorkerDemand(eg, p, workerIds, target));
        }
        int idx = slotLedger.acquireBestFit(jobId, candidates, remainMs(deadlineNanos));
        return idx < 0 ? 0 : desiredP - idx;  // index 0 is the desired value and a higher index is a lower tier, so the parallelism asked for minus the index
    }

    private static long remainMs(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return 0L;
        }
        return java.util.concurrent.TimeUnit.NANOSECONDS
                .toMillis(remainingNanos - 1L) + 1L;
    }

    /**
     * What a job asks of each worker is the number of stage instances placed there, since an instance holds one thread and one slot. The
     * whole-graph fast path, which runs the graph on one worker and whose instance list may be incomplete, asks for a single slot on its
     * target.
     */
    private static java.util.Map<String, Integer> slotDemandOf(PhysicalPlan plan, String wholeGraphWorker) {
        java.util.Map<String, Integer> demand = new java.util.HashMap<>();
        if (plan.isWholeGraph()) {
            demand.merge(wholeGraphWorker, 1, Integer::sum);
            return demand;
        }
        for (PhysicalPlan.Instance inst : plan.instances()) {
            demand.merge(inst.workerId(), 1, Integer::sum);
        }
        return demand;
    }

    /**
     * Diagnostic string for an admission refusal: per-worker "need=X free=Y cap=Z" plus total demand
     * against total free slots. It lets the caller tell at a glance whether resources really ran short
     * (free < need) or the demand was computed wrong (need far off the stage instance count).
     */
    private static String insufficientSlotsDiagnostic(java.util.Map<String, Integer> demand,
                                                      java.util.Map<String, int[]> snapshot) {
        int totalNeed = 0, totalFree = 0;
        StringBuilder sb = new StringBuilder("(per-worker:");
        for (java.util.Map.Entry<String, Integer> e : demand.entrySet()) {
            int need = e.getValue();
            int[] fc = snapshot.getOrDefault(e.getKey(), new int[]{0, 0});
            int free = fc[0], cap = fc[1];
            totalNeed += need;
            totalFree += Math.max(0, free);
            sb.append(' ').append(e.getKey())
              .append("[need=").append(need).append(" free=").append(free).append(" cap=").append(cap).append(']');
        }
        sb.append("; totalDemand=").append(totalNeed).append(" totalFree=").append(totalFree).append(')');
        return sb.toString();
    }

    /** A stage index and an edge index combined into one key: the stage in the high half, the edge in the low. */
    private static long edgeKey(int stageIndex, int edgeIndex) {
        return ((long) stageIndex << 32) | (edgeIndex & 0xffffffffL);
    }

    private static int sinkNodeCount(PlanGraph graph) {
        int count = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK) {
                count++;
            }
        }
        return count;
    }

    private static boolean containsManifestOutput(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK
                    && graph.node(i).output() instanceof io.rowweave.ManifestFileSink) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsProviderSinkOutput(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() != PlanNode.Kind.SINK) continue;
            Object output = graph.node(i).output();
            if (output instanceof io.rowweave.ProviderBatchSink
                    || "true".equals(graph.node(i).param(
                    PlanGraph.DURABLE_PROVIDER_BINDING_PARAM))) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsUnprojectedProviderOutput(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK
                    && graph.node(i).output()
                    instanceof io.rowweave.ProviderBatchSink) {
                return true;
            }
        }
        return false;
    }

    private static java.util.List<io.rowweave.ProviderBatchSink>
    providerOutputs(PlanGraph graph) {
        java.util.List<io.rowweave.ProviderBatchSink> outputs =
                new java.util.ArrayList<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output()
                    instanceof io.rowweave.ProviderBatchSink) {
                outputs.add((io.rowweave.ProviderBatchSink) node.output());
            } else if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof io.rowweave.ManifestFileSink) {
                outputs.add(((io.rowweave.ManifestFileSink) node.output())
                        .toProviderSink());
            }
        }
        if (outputs.isEmpty()) {
            throw new IllegalStateException(
                    "durable Sink graph has no provider output");
        }
        return outputs;
    }

    private static boolean hasProviderLocatorClosure(
            StageAssignmentSnapshot evidence) {
        boolean found = false;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            io.rowweave.run.StageAssignmentRole role =
                    evidence.manifest().assignment(i).role();
            if (role != io.rowweave.run.StageAssignmentRole.SINK
                    && role != io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH) {
                continue;
            }
            found = true;
            if (evidence.preparedSink(i) == null) return false;
        }
        return found;
    }

    private static java.util.List<io.rowweave.run.PreparedSinkLocator>
    providerLocators(StageAssignmentSnapshot evidence) {
        java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                new java.util.ArrayList<>();
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            io.rowweave.run.StageAssignmentRole role =
                    evidence.manifest().assignment(i).role();
            if (role != io.rowweave.run.StageAssignmentRole.SINK
                    && role != io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH) {
                continue;
            }
            PreparedSinkReceipt prepared = evidence.preparedSink(i);
            if (prepared == null) {
                throw new IllegalStateException(
                        "provider locator closure is incomplete");
            }
            locators.add(prepared.locator());
        }
        if (locators.isEmpty()) {
            throw new IllegalStateException(
                    "provider locator closure is empty");
        }
        return locators;
    }

    private static boolean containsQualityGateOutput(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK
                    && graph.node(i).output()
                    instanceof io.rowweave.ManifestFileSink
                    && ((io.rowweave.ManifestFileSink) graph.node(i).output())
                    .hasQualityGateBinding()) {
                return true;
            }
        }
        return false;
    }

    /**
     * The lease this master holds, together with a stable attempt and generation, until the whole group reaches a terminal state.
     */
    private final class ManifestJobCommit implements AutoCloseable {
        final PlanGraph graph;
        final io.rowweave.commit.ManifestFileStore store;
        final String attemptId;
        final long generation;
        final java.util.Map<String, String> expectedSchemas;
        final io.rowweave.commit.ManifestCommitAuthority.Lease lease;
        final DurableSinkAuthorityAdapterFactory.Reservation providerReservation;
        volatile io.rowweave.commit.BatchSinkOutcome providerOutcome;

        ManifestJobCommit(PlanGraph graph,
                          io.rowweave.commit.ManifestFileStore store, String attemptId,
                          long generation, java.util.Map<String, String> expectedSchemas,
                          io.rowweave.commit.ManifestCommitAuthority.Lease lease) {
            this.graph = graph;
            this.store = store;
            this.attemptId = attemptId;
            this.generation = generation;
            this.expectedSchemas = expectedSchemas;
            this.lease = lease;
            this.providerReservation = null;
        }

        ManifestJobCommit(
                PlanGraph graph, String attemptId, long generation,
                DurableSinkAuthorityAdapterFactory.Reservation reservation) {
            this.graph = graph;
            this.store = null;
            this.attemptId = attemptId;
            this.generation = generation;
            this.expectedSchemas = java.util.Collections.emptyMap();
            this.lease = null;
            this.providerReservation = reservation;
        }

        @Override
        public void close() {
            if (lease != null) lease.close();
            if (providerReservation != null) providerReservation.close();
        }

        boolean genericProvider() { return providerReservation != null; }
    }

    private ManifestJobCommit openManifestJobCommit(
            PlanGraph graph, String wireAttemptId, RunAttemptFence fence) {
        java.util.List<io.rowweave.ManifestFileSink> outputs = new java.util.ArrayList<>();
        java.util.List<io.rowweave.ProviderBatchSink> providerOutputs =
                new java.util.ArrayList<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof io.rowweave.ManifestFileSink) {
                outputs.add((io.rowweave.ManifestFileSink) node.output());
            } else if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof io.rowweave.ProviderBatchSink) {
                providerOutputs.add(
                        (io.rowweave.ProviderBatchSink) node.output());
            }
        }
        if (outputs.isEmpty() && providerOutputs.isEmpty()) {
            return null;
        }
        String attemptId = wireAttemptId == null || wireAttemptId.isEmpty()
                ? java.util.UUID.randomUUID().toString() : wireAttemptId;
        if (outputs.isEmpty()) {
            if (fence == null || testProviderAuthorityFactory == null) {
                throw new IllegalStateException(
                        "custom durable Sink authority is unavailable");
            }
            DurableSinkAuthorityAdapterFactory.Reservation reservation =
                    testProviderAuthorityFactory.reserve(
                            fence, attemptId, providerOutputs);
            if (reservation == null || reservation.generation() <= 0L) {
                throw new IllegalStateException(
                        "custom durable Sink authority reservation failed");
            }
            return new ManifestJobCommit(
                    graph, attemptId, reservation.generation(), reservation);
        }
        io.rowweave.commit.ManifestFileStore store = new io.rowweave.commit.ManifestFileStore(
                java.nio.file.Paths.get(outputs.get(0).domainPath()));
        java.util.Map<String, String> expected = new java.util.LinkedHashMap<>();
        for (io.rowweave.ManifestFileSink output : outputs) {
            if (!store.domainId().equals(output.domainId())) {
                throw new IllegalArgumentException(
                        "manifest sink domain identity mismatch: identity changed");
            }
            String previous = expected.put(output.memberName(),
                    io.rowweave.commit.SchemaFingerprint.of(output.getSchema()));
            if (previous != null) {
                throw new IllegalArgumentException("duplicate manifest member: " + output.memberName());
            }
        }
        io.rowweave.commit.ManifestCommitAuthority.Lease lease =
                manifestCommitAuthority.tryAcquire(store.domainPath(), attemptId);
        if (lease == null) {
            throw new IllegalStateException(
                    "manifest commit domain already has an active writer: " + store.domainId());
        }
        try {
            long generation = manifestCommitAuthority.reserve(store.domainPath(), attemptId);
            return new ManifestJobCommit(
                    graph, store, attemptId, generation, expected, lease);
        } catch (Throwable failure) {
            try {
                lease.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throwUnchecked(failure);
            throw new AssertionError("unreachable");
        }
    }

    /**
     * Assigns a binary join stage in the middle of a graph, with P instances and two inbound edges. Each instance listens on two ports, one
     * per side, and the join's output is partitioned by the grouping keys of the stage below. The ports are recorded as the stage's edge 0
     * and edge 1, so the stage upstream on the left reaches the first and the one on the right the second.
     */
    private void assignJoinStage(PlanGraph graph, Stage stage, PlanNode join, java.util.List<Stage> stages,
                                 PhysicalPlan plan, java.util.List<PhysicalPlan.Instance> myInstances,
                                 java.util.Map<Long, java.util.List<io.rowweave.sched.net.ShuffleEndpoint>> inboundEps,
                                 java.util.Map<Long, java.util.List<InboundInstance>> inboundInsts,
                                 java.util.List<Long> allJobIds, int window, String monitorJobId,
                                 Object serializationMutex, boolean fullMuxBatch,
                                 java.util.List<PendingStageAssignment> deferredMuxAssignments,
                                 java.util.List<java.util.List<PendingStageAssignment>> deferredSecureWaves,
                                 AssignmentProfile assignmentProfile,
                                 DagAssignmentContext assignmentContext,
                                 DurableStageAssignmentPlan durableAssignments,
                                 SecureShuffleBindings secureShuffle) throws Exception {
        if (stage.members().size() != 1) {
            throw new IllegalStateException("join stage supports only one member (pass-through after join is not yet supported): "
                    + stage.members());
        }
        DataSchema leftMeta = graph.node(join.child(0)).outMeta();
        DataSchema rightMeta = graph.node(join.child(1)).outMeta();
        DataSchema joinOutMeta = join.outMeta();
        String[] leftKeys = join.listParam("leftKeys").toArray(new String[0]);
        String[] rightKeys = join.listParam("rightKeys").toArray(new String[0]);
        boolean isCoGroup = "coGroup".equals(join.typeId());
        String joinType = isCoGroup ? null : join.param("joinType");
        io.rowweave.ops.join.JoinPhysicalStrategy joinStrategy = isCoGroup ? null
                : io.rowweave.ops.join.JoinPhysicalStrategy.fromPlanHint(join.param("joinHint"));
        io.rowweave.CoGroupFunction coGroupFn = isCoGroup
                ? (io.rowweave.CoGroupFunction) join.userFunction() : null;
        // The producer count on each inbound edge of a binary node is the instance count of the stage above it.
        int leftInbound = plan.instancesOf(stage.inputStages().get(0)).size();
        int rightInbound = plan.instancesOf(stage.inputStages().get(1)).size();
        DownstreamFanout down = downstreamFanout(graph, stage, stages, inboundEps, plan);

        java.util.List<io.rowweave.sched.net.ShuffleEndpoint> leftEps = new java.util.ArrayList<>();
        java.util.List<io.rowweave.sched.net.ShuffleEndpoint> rightEps = new java.util.ArrayList<>();
        java.util.List<InboundInstance> leftInsts = new java.util.ArrayList<>();
        java.util.List<InboundInstance> rightInsts = new java.util.ArrayList<>();
        java.util.List<PendingStageAssignment> assignmentWave = new java.util.ArrayList<>();
        java.util.List<String> instanceWorkers = new java.util.ArrayList<>();
        java.util.List<String> leftLocalKeys = new java.util.ArrayList<>();
        java.util.List<String> rightLocalKeys = new java.util.ArrayList<>();
        java.util.List<io.rowweave.sched.net.ShuffleEndpoint> knownLeft = new java.util.ArrayList<>();
        java.util.List<io.rowweave.sched.net.ShuffleEndpoint> knownRight = new java.util.ArrayList<>();
        java.util.List<StagePortTracker> listening = new java.util.ArrayList<>();
        for (PhysicalPlan.Instance inst : myInstances) {
            long jobId = runtimeStageId(durableAssignments,
                    stage.index(), inst.instanceIndex());
            allJobIds.add(jobId);
            stageTrackers.put(jobId, new StageTracker(
                    stage.index(), inst.instanceIndex(),
                    StageCommand.Role.JOIN_CONSUMER, myInstances.size()));
            java.util.List<ShuffleTarget> targets = secureShuffle == null
                    ? (objectReuse ? downTargetsFor(
                            graph, stage, stages, inboundInsts, inst.workerId()) : null)
                    : secureDownTargetsFor(stage, inst.instanceIndex(), stages,
                            plan, secureShuffle);
            StageCommand cmd = isCoGroup
                    ? StageCommand.joinConsumerFanout(
                            leftMeta, rightMeta, joinOutMeta, window, leftInbound, rightInbound,
                            leftKeys, rightKeys, joinType, down.endpoints, down.keys, down.normalized)
                    : StageCommand.joinConsumerFanout(
                            leftMeta, rightMeta, joinOutMeta, window, leftInbound, rightInbound,
                            leftKeys, rightKeys, joinType, joinStrategy,
                            down.endpoints, down.keys, down.normalized);
            if (isCoGroup) {
                cmd.withCoGroupFn(coGroupFn);
            }
            // Producers on the same worker are reached locally, on edge 0 for the left and edge 1 for the right, and
            // because the local key carries the edge the two are separate by construction.
            int leftLocal = objectReuse ? coLocatedUpstream(graph, stages, stage, 0, plan, inst.workerId()) : 0;
            int rightLocal = objectReuse ? coLocatedUpstream(graph, stages, stage, 1, plan, inst.workerId()) : 0;
            if (secureShuffle != null) {
                leftLocal = secureShuffle.localProducerCount(
                        stage.index(), 0, inst.instanceIndex());
                rightLocal = secureShuffle.localProducerCount(
                        stage.index(), 1, inst.instanceIndex());
            }
            String leftLocalKey = leftLocal > 0
                    ? (secureShuffle == null
                            ? localKey(monitorJobId, stage.index(), inst.instanceIndex(), 0)
                            : secureShuffle.localKey(stage.index(), 0, inst.instanceIndex()))
                    : null;
            String rightLocalKey = rightLocal > 0
                    ? (secureShuffle == null
                            ? localKey(monitorJobId, stage.index(), inst.instanceIndex(), 1)
                            : secureShuffle.localKey(stage.index(), 1, inst.instanceIndex()))
                    : null;
            if (leftLocal > 0) cmd.withLocalInbound(leftLocal, leftLocalKey);
            if (rightLocal > 0) cmd.withLocalInboundRight(rightLocal, rightLocalKey);
            if (targets != null) cmd.withDownTargets(targets).withObjectReuse(objectReuse);
            // A binary node's output may have more than one consumer. Every consumer's endpoints and targets have to
            // be carried into the fan-out, otherwise the remaining sinks would wait forever for rows and for EOS,
            // which is why each consumption edge gets its own broadcast group.
            java.util.List<StageCommand.BroadcastGroup> broadcastGroups = broadcastGroupsForConsumers(
                    graph, stage, stages, inboundEps, inboundInsts, plan,
                    inst.workerId(), inst.instanceIndex(), secureShuffle);
            if (broadcastGroups != null) {
                cmd.withBroadcastGroups(broadcastGroups).withObjectReuse(objectReuse);
            }
            if (secureShuffle != null
                    && !secureShuffle.authorizations(
                            stage.index(), inst.instanceIndex()).isEmpty()) {
                cmd.withInboundAuthorizations(secureShuffle.authorizations(
                        stage.index(), inst.instanceIndex()));
            }
            boolean leftHasSocket = (leftInbound - leftLocal) > 0;
            boolean rightHasSocket = (rightInbound - rightLocal) > 0;
            io.rowweave.sched.net.ShuffleEndpoint leftEp = null, rightEp = null;
            if ((leftHasSocket || rightHasSocket) && assignmentContext.muxOn(inst.workerId())) {
                // Multiplexing: one routing key per inbound edge, left as edge 0 and right as edge 1, the endpoints are
                // the muxed ones, and the stage is assigned at once rather than waiting for a port report.
                java.util.UUID leftTid = leftHasSocket
                        ? (secureShuffle == null ? java.util.UUID.randomUUID()
                                : secureShuffle.targetId(
                                        stage.index(), 0, inst.instanceIndex()))
                        : null;
                java.util.UUID rightTid = rightHasSocket
                        ? (secureShuffle == null ? java.util.UUID.randomUUID()
                                : secureShuffle.targetId(
                                        stage.index(), 1, inst.instanceIndex()))
                        : null;
                cmd.withInboundTargetIds(leftTid, rightTid);
                RegistryWorker joinEndpoint = assignmentContext.worker(inst.workerId());
                if (leftHasSocket) leftEp = secureShuffle == null
                        ? muxEndpoint(joinEndpoint, leftTid)
                        : secureShuffle.safeEndpoint(
                                stage.index(), 0, inst.instanceIndex());
                if (rightHasSocket) rightEp = secureShuffle == null
                        ? muxEndpoint(joinEndpoint, rightTid)
                        : secureShuffle.safeEndpoint(
                                stage.index(), 1, inst.instanceIndex());
                listening.add(null);
            } else if (leftHasSocket || rightHasSocket) {
                StagePortTracker tracker = new StagePortTracker(leftHasSocket, rightHasSocket);
                portTrackers.put(jobId, tracker);
                listening.add(tracker);
            } else {
                listening.add(null);
            }
            assignmentWave.add(assignmentContext.pending(
                    stage.index(), inst.instanceIndex(),
                    inst.workerId(), jobId, cmd));
            instanceWorkers.add(inst.workerId());
            leftLocalKeys.add(leftLocalKey);
            rightLocalKeys.add(rightLocalKey);
            knownLeft.add(leftEp);
            knownRight.add(rightEp);
        }
        scheduleAssignments(assignmentWave, serializationMutex, fullMuxBatch,
                deferredMuxAssignments, deferredSecureWaves,
                secureShuffle != null, assignmentProfile);
        for (int i = 0; i < instanceWorkers.size(); i++) {
            io.rowweave.sched.net.ShuffleEndpoint leftEp = knownLeft.get(i);
            io.rowweave.sched.net.ShuffleEndpoint rightEp = knownRight.get(i);
            StagePortTracker tracker = listening.get(i);
            if (tracker != null) {
                long t = System.nanoTime();
                int[] ports = tracker.awaitBoth(
                        assignmentContext.setupWaitMillis());
                assignmentProfile.portWaitNanos.addAndGet(System.nanoTime() - t);
                if (ports[0] > 0) {
                    leftEp = new io.rowweave.sched.net.ShuffleEndpoint(
                            assignmentContext.worker(instanceWorkers.get(i)).host(), ports[0]);
                }
                if (ports[1] > 0) {
                    rightEp = new io.rowweave.sched.net.ShuffleEndpoint(
                            assignmentContext.worker(instanceWorkers.get(i)).host(), ports[1]);
                }
            }
            if (leftEp != null) leftEps.add(leftEp);
            if (rightEp != null) rightEps.add(rightEp);
            leftInsts.add(new InboundInstance(instanceWorkers.get(i), leftEp, leftLocalKeys.get(i)));
            rightInsts.add(new InboundInstance(instanceWorkers.get(i), rightEp, rightLocalKeys.get(i)));
        }
        if (!leftEps.isEmpty()) inboundEps.put(edgeKey(stage.index(), 0), leftEps);
        if (!rightEps.isEmpty()) inboundEps.put(edgeKey(stage.index(), 1), rightEps);
        inboundInsts.put(edgeKey(stage.index(), 0), leftInsts);
        inboundInsts.put(edgeKey(stage.index(), 1), rightInsts);
    }

    /**
     * Assigns a union stage in the middle of a graph: one instance, no key, concatenating every input into one output. A single inbound
     * channel accepts all connections from both sides, the left being input 0 and the right input 1, so its producer count is the instances
     * above on both added together, and a pass-through fragment fans out downstream. Both inbound edges share that one endpoint, because a
     * union has no key and so no left or right.
     *
     * <p>The distinct part of this shape is that two edges converge on one channel. Producers on the same worker are reached locally
     * through one registered count, the sum over both sides on that worker, under a single key for edge 0, and the count is sockets plus
     * local. Both edge keys then map to that same inbound instance with the same local key, which mirrors the two edges sharing one
     * endpoint, so an upstream on either side finds the same local channel and the latch for EOS is neither one short nor one over.
     */
    private void assignUnionStage(PlanGraph graph, Stage stage, java.util.List<Stage> stages,
                                  PhysicalPlan plan,
                                  java.util.Map<Long, java.util.List<io.rowweave.sched.net.ShuffleEndpoint>> inboundEps,
                                  java.util.Map<Long, java.util.List<InboundInstance>> inboundInsts,
                                  java.util.List<Long> allJobIds, int window, String monitorJobId,
                                  Object serializationMutex, boolean fullMuxBatch,
                                  java.util.List<PendingStageAssignment> deferredMuxAssignments,
                                  AssignmentProfile assignmentProfile,
                                  java.util.List<java.util.List<PendingStageAssignment>> deferredSecureWaves,
                                  DagAssignmentContext assignmentContext,
                                  DurableStageAssignmentPlan durableAssignments,
                                  SecureShuffleBindings secureShuffle) throws Exception {
        PlanNode union = graph.node(stage.members().get(0));
        DataSchema meta = union.outMeta();  // both sides of a union have the same schema, so what comes in is what goes out
        int inbound = upstreamInstanceCount(stage, stages, plan);  // the instances above on both sides, since both converge on one channel
        DownstreamFanout down = downstreamFanout(graph, stage, stages, inboundEps, plan);
        String unionWorker = plan.instancesOf(stage.index()).get(0).workerId();
        // Producers on the same worker are reached locally. Both inbound edges converge on one channel, so the
        // local count is that sum over the two sides, registered under the single key for edge 0.
        int localConns = secureShuffle == null
                ? (objectReuse
                        ? coLocatedUpstream(graph, stages, stage, 0, plan, unionWorker)
                          + coLocatedUpstream(graph, stages, stage, 1, plan, unionWorker)
                        : 0)
                : secureShuffle.localProducerCount(stage.index(), 0, 0)
                  + secureShuffle.localProducerCount(stage.index(), 1, 0);
        String localKey = null;
        if (localConns > 0) {
            localKey = secureShuffle == null
                    ? localKey(monitorJobId, stage.index(), 0, 0)
                    : secureShuffle.localKey(stage.index(), 0, 0);
            if (localKey == null && secureShuffle != null) {
                localKey = secureShuffle.localKey(stage.index(), 1, 0);
            }
        }
        boolean hasSocket = (inbound - localConns) > 0;
        // A fragment that is a placeholder source and nothing else is an identity concatenation, and it reuses
        // the fan-out machinery of a keyed stage.
        PlanGraph frag = new PlanGraph(java.util.Collections.singletonList(
                PlanNode.placeholderSource(meta)));
        long jobId = runtimeStageId(durableAssignments, stage.index(), 0);
        allJobIds.add(jobId);
        stageTrackers.put(jobId, new StageTracker(
                stage.index(), 0, StageCommand.Role.KEYED_OP, 1));
        // Downstream instances on the same worker are reached locally, exactly as for a join; absent means the
        // socket path alone.
        java.util.List<ShuffleTarget> targets = secureShuffle == null
                ? (objectReuse ? downTargetsFor(
                        graph, stage, stages, inboundInsts, unionWorker) : null)
                : secureDownTargetsFor(stage, 0, stages, plan, secureShuffle);
        StageCommand cmd = StageCommand.keyedOpFanout(meta, meta, window, inbound, frag, down.endpoints, down.keys, down.normalized);
        if (localConns > 0) {
            cmd.withLocalInbound(localConns, localKey);
        }
        if (targets != null) {
            cmd.withDownTargets(targets).withObjectReuse(objectReuse);
        }
        // A union is a binary producer just as a join or a coGroup is, so its output goes to every consumption
        // edge rather than only the first downstream.
        java.util.List<StageCommand.BroadcastGroup> broadcastGroups = broadcastGroupsForConsumers(
                graph, stage, stages, inboundEps, inboundInsts, plan,
                unionWorker, 0, secureShuffle);
        if (broadcastGroups != null) {
            cmd.withBroadcastGroups(broadcastGroups).withObjectReuse(objectReuse);
        }
        if (secureShuffle != null
                && !secureShuffle.authorizations(stage.index(), 0).isEmpty()) {
            cmd.withInboundAuthorizations(
                    secureShuffle.authorizations(stage.index(), 0));
        }
        io.rowweave.sched.net.ShuffleEndpoint ep = null;
        if (hasSocket && assignmentContext.muxOn(unionWorker)) {
            // Multiplexing: a union's single inbound accepts every upstream on both sides, so one routing key is
            // enough, the endpoint is the muxed one, and the stage is assigned at once rather than waiting for a
            // port report.
            java.util.UUID tid = secureShuffle == null
                    ? java.util.UUID.randomUUID()
                    : firstNonNull(
                            secureShuffle.targetId(stage.index(), 0, 0),
                            secureShuffle.targetId(stage.index(), 1, 0));
            cmd.withInboundTargetId(tid);
            scheduleAssignments(java.util.Collections.singletonList(
                            assignmentContext.pending(
                                    stage.index(), 0, unionWorker, jobId, cmd)),
                    serializationMutex, fullMuxBatch, deferredMuxAssignments,
                    deferredSecureWaves, secureShuffle != null, assignmentProfile);
            RegistryWorker unionEndpoint = assignmentContext.worker(unionWorker);
            ep = secureShuffle == null
                    ? muxEndpoint(unionEndpoint, tid)
                    : firstNonNull(
                            secureShuffle.safeEndpoint(stage.index(), 0, 0),
                            secureShuffle.safeEndpoint(stage.index(), 1, 0));
            java.util.List<io.rowweave.sched.net.ShuffleEndpoint> eps = java.util.Collections.singletonList(ep);
            inboundEps.put(edgeKey(stage.index(), 0), eps);
            inboundEps.put(edgeKey(stage.index(), 1), eps);
        } else if (hasSocket) {
            int port = assignListening(assignmentContext.worker(unionWorker), jobId,
                    assignmentContext.bind(cmd, jobId), assignmentProfile, assignmentContext);
            ep = new io.rowweave.sched.net.ShuffleEndpoint(
                    assignmentContext.worker(unionWorker).host(), port);
            java.util.List<io.rowweave.sched.net.ShuffleEndpoint> eps = java.util.Collections.singletonList(ep);
            inboundEps.put(edgeKey(stage.index(), 0), eps);  // both inbound edges share the one endpoint, because a union has no key and no left or right
            inboundEps.put(edgeKey(stage.index(), 1), eps);
        } else {
            scheduleAssignments(java.util.Collections.singletonList(
                            assignmentContext.pending(
                                    stage.index(), 0, unionWorker, jobId, cmd)),
                    serializationMutex, fullMuxBatch, deferredMuxAssignments,
                    deferredSecureWaves, secureShuffle != null, assignmentProfile);
        }
        // The two edge keys map to one inbound instance: same worker, same endpoint, same local key.
        InboundInstance ii = new InboundInstance(unionWorker, ep, localKey);
        inboundInsts.put(edgeKey(stage.index(), 0), java.util.Collections.singletonList(ii));
        inboundInsts.put(edgeKey(stage.index(), 1), java.util.Collections.singletonList(ii));
    }

    /**
     * Every pair of consumer stage and edge that consumes this stage, which is the list of subpartitions of its output. This is
     * deliberately not a set of consumers: a consumer that references this stage on two inbound edges, which is what a self-join is, yields
     * two pairs rather than one being deduplicated away. That is the meaning of a subpartition, and it is what makes a self-join receive
     * its second input, where taking only the first edge would have dropped rows.
     */
    private static java.util.List<int[]> consumerEdgesOf(Stage stage, java.util.List<Stage> stages) {
        java.util.List<int[]> pairs = new java.util.ArrayList<>();
        for (Stage s : stages) {
            java.util.List<Integer> ins = s.inputStages();
            for (int e = 0; e < ins.size(); e++) {
                if (ins.get(e) == stage.index()) {
                    pairs.add(new int[]{s.index(), e});  // one consumer referencing this stage on several edges yields several subpartitions, which is a self-join
                }
            }
        }
        return pairs;
    }

    /**
     * Turns every consumption edge of a producer stage into a broadcast group on the worker side. Deduplicate by edge rather than by
     * consumer, so a self-join or a re-join keeps both of its input edges, and each group carries the downstream's own partitioning rule
     * and sends its EOS independently on close.
     *
     * <p>With a single consumption edge this returns null, which keeps the fast path a stage with one downstream has always had. If a
     * downstream has registered no mixed socket and local targets the call fails outright, rather than falling back to a first-consumer
     * path that would silently under-send.
     */
    private java.util.List<StageCommand.BroadcastGroup> broadcastGroupsForConsumers(
            PlanGraph graph, Stage producer, java.util.List<Stage> stages,
            java.util.Map<Long, java.util.List<io.rowweave.sched.net.ShuffleEndpoint>> inboundEps,
            java.util.Map<Long, java.util.List<InboundInstance>> inboundInsts,
            PhysicalPlan plan, String producerWorker, int producerInstance,
            SecureShuffleBindings secureShuffle) {
        java.util.List<int[]> edges = consumerEdgesOf(producer, stages);
        if (edges.size() <= 1) {
            return null;
        }
        java.util.List<StageCommand.BroadcastGroup> groups = new java.util.ArrayList<>(edges.size());
        for (int[] pair : edges) {
            Stage consumer = stages.get(pair[0]);
            int edge = pair[1];
            java.util.List<ShuffleTarget> targets = secureShuffle == null
                    ? downTargetsForConsumer(
                            consumer, edge, inboundInsts, producerWorker)
                    : secureShuffle.targets(producer.index(), producerInstance,
                            consumer.index(), edge,
                            plan.instancesOf(consumer.index()).size());
            if (targets == null) {
                throw new IllegalStateException("fan-out consumer targets unavailable: producer="
                        + producer.index() + " consumer=" + consumer.index() + " edge=" + edge);
            }
            DownstreamFanout fanout = fanoutForConsumer(graph, consumer, edge, inboundEps, plan);
            int fixedPartition = fixedPartitionForConsumer(
                    graph, producer, consumer, plan,
                    producerInstance);
            StageCommand.BroadcastGroup group =
                    new StageCommand.BroadcastGroup(
                            targets, fanout.keys, fanout.normalized,
                            producer.blockingFanout());
            if (fixedPartition >= 0) {
                group.withFixedPartition(
                        fixedPartition, targets.size());
            }
            groups.add(group);
        }
        return groups;
    }

    private java.util.List<ShuffleTarget> secureDownTargetsFor(
            Stage producer, int producerInstance,
            java.util.List<Stage> stages, PhysicalPlan plan,
            SecureShuffleBindings secureShuffle) {
        for (Stage consumer : stages) {
            for (int edge = 0; edge < consumer.inputStages().size(); edge++) {
                if (consumer.inputStages().get(edge) == producer.index()) {
                    return secureShuffle.targets(producer.index(), producerInstance,
                            consumer.index(), edge,
                            plan.instancesOf(consumer.index()).size());
                }
            }
        }
        return null;
    }

    /**
     * What this stage needs in order to fan out downstream: the endpoints of every instance of the stage below, the keys that partition
     * into them, and whether those are normalised numerically, which a join does.
     */
    private static final class DownstreamFanout {
        final java.util.List<io.rowweave.sched.net.ShuffleEndpoint> endpoints;
        final String[] keys;  // the grouping keys of the downstream; unused when it has one instance, since then everything is sent to it, so they may be empty
        final boolean normalized;  // normalised numerically when the downstream is a join, so a match does not depend on scale; false otherwise

        DownstreamFanout(java.util.List<io.rowweave.sched.net.ShuffleEndpoint> eps, String[] keys, boolean normalized) {
            this.endpoints = eps;
            this.keys = keys;
            this.normalized = normalized;
        }
    }

    /**
     * The stage below decides how it is fed. Find the downstream stage that consumes this one and which of its edges this is, and take the
     * endpoints recorded for that stage and edge. One instance below means everything goes to it; P instances partitioned by key means the
     * output is spread across them by that stage's grouping keys, which for a join's edge 0 are its left keys and for edge 1 its right
     * keys, normalised numerically.
     */
    private DownstreamFanout downstreamFanout(
            PlanGraph graph, Stage stage, java.util.List<Stage> stages,
            java.util.Map<Long, java.util.List<io.rowweave.sched.net.ShuffleEndpoint>> inboundEps, PhysicalPlan plan) {
        for (Stage s : stages) {
            int edge = s.inputStages().indexOf(stage.index());
            if (edge < 0) {
                continue;
            }
            return fanoutForConsumer(graph, s, edge, inboundEps, plan);  // the consumer that comes first, which is all a stage with one downstream needs
        }
        throw new IllegalStateException("stage " + stage.index() + " has no downstream (not a sink and has no consumer)");
    }

    /**
     * What this stage needs in order to feed one named consumer, where the edge says which of that consumer's inbound edges this stage is.
     * Lifted out of the loop over downstream stages so a broadcast can be built one consumer at a time.
     */
    private DownstreamFanout fanoutForConsumer(
            PlanGraph graph, Stage s, int edge,
            java.util.Map<Long, java.util.List<io.rowweave.sched.net.ShuffleEndpoint>> inboundEps, PhysicalPlan plan) {
        java.util.List<io.rowweave.sched.net.ShuffleEndpoint> eps = inboundEps.get(edgeKey(s.index(), edge));
        int downCount = plan.instancesOf(s.index()).size();
        if (eps == null) {
            eps = java.util.Collections.emptyList();
        }
        if (downCount == 1) {
            return new DownstreamFanout(eps, new String[0], false);
        }
        PlanNode downHead = graph.node(s.members().get(0));
        if (downHead.kind() == PlanNode.Kind.BINARY) {
            if ("union".equals(downHead.typeId())) {
                return new DownstreamFanout(eps, new String[0], false);
            }
            String keyParam = edge == 0 ? "leftKeys" : "rightKeys";
            boolean norm = !"coGroup".equals(downHead.typeId());
            return new DownstreamFanout(eps, downHead.listParam(keyParam).toArray(new String[0]), norm);
        }
        DataSchema childMeta = graph.node(downHead.child(0)).outMeta();
        return new DownstreamFanout(eps, keyedKeyFields(childMeta, downHead), false);
    }

    private int fixedPartitionForFirstConsumer(
            PlanGraph graph, Stage producer, java.util.List<Stage> stages,
            PhysicalPlan plan, int producerInstance) {
        for (Stage consumer : stages) {
            for (int edge = 0; edge < consumer.inputStages().size(); edge++) {
                if (consumer.inputStages().get(edge) == producer.index()) {
                    return fixedPartitionForConsumer(
                            graph, producer, consumer, plan,
                            producerInstance);
                }
            }
        }
        return -1;
    }

    /**
     * MAP_PARTITION is a physical partition boundary, not a keyed shuffle.
     * Producer task i therefore keeps all of its rows on consumer task i.
     * Every producer still opens/closes every bounded channel so the existing
     * multi-producer EOS lifecycle remains unchanged.
     */
    private int fixedPartitionForConsumer(
            PlanGraph graph, Stage producer, Stage consumer,
            PhysicalPlan plan, int producerInstance) {
        PlanNode head = graph.node(consumer.members().get(0));
        if (!"mapPartition".equals(head.typeId())) {
            return -1;
        }
        int producers = plan.instancesOf(producer.index()).size();
        int consumers = plan.instancesOf(consumer.index()).size();
        if (producers != consumers) {
            throw new IllegalStateException(
                    "mapPartition requires one physical consumer per upstream partition: producer="
                            + producers + " consumer=" + consumers);
        }
        return producerInstance;
    }

    /**
     * How to reach one instance of one inbound edge of a downstream stage: which worker it is on, which decides locality, its socket
     * endpoint for a different worker, and its local rendezvous key for the same one. An upstream compares this against the worker it is
     * itself on.
     */
    private static final class InboundInstance {
        final String workerId;
        final io.rowweave.sched.net.ShuffleEndpoint endpoint;  // may be absent: an inbound reached only locally has no socket to bind
        final String localKey;

        InboundInstance(String workerId, io.rowweave.sched.net.ShuffleEndpoint endpoint, String localKey) {
            this.workerId = workerId;
            this.endpoint = endpoint;
            this.localKey = localKey;
        }
    }

    /**
     * The local rendezvous key is the job, the downstream stage, the instance and the edge, which is how a downstream registering an
     * inbound and an upstream awaiting it agree. A worker's registry outlives one job, so the key has to be unique per job, which is what
     * the monitoring job id is for.
     */
    private static String localKey(String monitorJobId, int downStage, int downInstance, int edge) {
        return "L" + monitorJobId + "#" + downStage + "#" + downInstance + "#" + edge;
    }

    private static <T> T firstNonNull(T first, T second) {
        return first != null ? first : second;
    }

    /**
     * How many producers of one inbound edge of a downstream stage, that is the upstream stage at that input position, sit on the same
     * worker and can be reached locally. An upstream that is a join or a union still fans out over sockets and has no local targets wired,
     * so it must never be counted as local: the downstream would then think everything was local and discard the socket endpoints while the
     * binary still sent over sockets, leaving it with no partitions to reach.
     */
    private static int coLocatedUpstream(PlanGraph graph, java.util.List<Stage> stages, Stage downStage,
                                         int edge, PhysicalPlan plan, String downWorker) {
        if (edge < 0 || edge >= downStage.inputStages().size()) {
            return 0;
        }
        int up = downStage.inputStages().get(edge);
        if (!supportsLocalEgress(graph, stageByIndex(stages, up))) {
            return 0;  // an upstream that is a join or union arrives over sockets, so it is not counted as local
        }
        int c = 0;
        for (PhysicalPlan.Instance inst : plan.instancesOf(up)) {
            if (downWorker.equals(inst.workerId())) {
                c++;
            }
        }
        return c;
    }

    /**
     * How one inbound edge, from the upstream stage at that position to the named downstream, is actually carried, which is what colours an
     * edge in the monitor. With row reuse off, or where the upstream is a join or union that cannot take a local egress, the answer is
     * remote. Otherwise the pairs of producer and consumer instances are checked for sharing a worker: all of them local, none, or mixed.
     * The colouring is per edge, not per instance.
     */
    private String edgeChannelType(PlanGraph graph, java.util.List<Stage> stages, Stage downStage,
                                   int upIndex, PhysicalPlan plan) {
        // When the upstream is a BLOCKING fork (it materializes subpartitions and its disk writes are not
        // bounded by downstream backpressure), this inbound edge is "materialized" and the monitor
        // highlights it as a flow-breaking buffer point, which is exactly where spill and memory are
        // consumed in a workload that must not OOM. This check runs ahead of the local/remote decision.
        Stage up = stageByIndex(stages, upIndex);
        if (up != null && up.blockingFanout()) {
            return "materialized";
        }
        if (!objectReuse || !supportsLocalEgress(graph, up)) {
            return "remote";
        }
        java.util.List<PhysicalPlan.Instance> ups = plan.instancesOf(upIndex);
        java.util.List<PhysicalPlan.Instance> downs = plan.instancesOf(downStage.index());
        boolean anyLocal = false, anyRemote = false;
        for (PhysicalPlan.Instance u : ups) {
            for (PhysicalPlan.Instance d : downs) {
                if (u.workerId().equals(d.workerId())) {
                    anyLocal = true;
                } else {
                    anyRemote = true;
                }
            }
        }
        if (anyLocal && anyRemote) {
            return "mixed";
        }
        return anyLocal ? "local" : "remote";
    }

    private static Stage stageByIndex(java.util.List<Stage> stages, int index) {
        for (Stage s : stages) {
            if (s.index() == index) {
                return s;
            }
        }
        throw new IllegalStateException("no stage with index " + index);
    }

    /**
     * Whether this stage can take a local egress, that is whether per-partition targets were wired for it. A stage headed by a source and a
     * keyed stage can, and so can join, coGroup and union.
     */
    private static boolean supportsLocalEgress(PlanGraph graph, Stage stage) {
        PlanNode head = graph.node(stage.members().get(0));
        if (head.kind() != PlanNode.Kind.BINARY) {
            return true;
        }
        return ClusterPlanCapabilityValidator.supportsBinary(head);  // the one place that says what a cluster supports, shared with the capability whitelist
    }

    /**
     * The per-partition targets for one instance of an upstream stage, on the worker named, along one inbound edge of its downstream,
     * ordered exactly as the downstream's endpoints are, so partition i means instance i. An instance on the same worker is reached through
     * its local key and any other through its socket endpoint. Where no descriptor was registered, which is the socket path a join or union
     * still takes, this returns null and the plain endpoint list is used.
     */
    private java.util.List<ShuffleTarget> downTargetsFor(
            PlanGraph graph, Stage stage, java.util.List<Stage> stages,
            java.util.Map<Long, java.util.List<InboundInstance>> inboundInsts, String upWorkerId) {
        for (Stage s : stages) {
            int edge = s.inputStages().indexOf(stage.index());
            if (edge < 0) {
                continue;
            }
            return downTargetsForConsumer(s, edge, inboundInsts, upWorkerId);  // the consumer that comes first, which is all a stage with one downstream needs
        }
        return null;
    }

    /**
     * The per-partition targets for one inbound edge of one named consumer of this stage, taken from the same computation so a broadcast
     * can build them group by group.
     */
    private java.util.List<ShuffleTarget> downTargetsForConsumer(
            Stage s, int edge, java.util.Map<Long, java.util.List<InboundInstance>> inboundInsts, String upWorkerId) {
        java.util.List<InboundInstance> insts = inboundInsts.get(edgeKey(s.index(), edge));
        if (insts == null) {
            return null;  // no descriptor registered for the downstream, which is a join or union, so the socket path applies
        }
        java.util.List<ShuffleTarget> targets = new java.util.ArrayList<>(insts.size());
        for (InboundInstance ii : insts) {
            if (ii.workerId.equals(upWorkerId) && ii.localKey != null) {
                targets.add(ShuffleTarget.local(ii.localKey));
            } else {
                targets.add(ShuffleTarget.socket(ii.endpoint));
            }
        }
        return targets;
    }

    /**
     * How many instances on that inbound edge of the downstream share the worker named, which is the number of local targets that upstream
     * instance will have.
     */
    private static int localTargetCount(java.util.List<InboundInstance> insts, String upWorkerId) {
        int c = 0;
        if (insts != null) {
            for (InboundInstance ii : insts) {
                if (ii.localKey != null && ii.workerId.equals(upWorkerId)) {
                    c++;
                }
            }
        }
        return c;
    }

    /**
     * The producer count on this stage's inbound edge, which is the instance count of the stage above summed over its inputs, and zero for
     * a stage with none.
     */
    private static int upstreamInstanceCount(Stage stage, java.util.List<Stage> stages, PhysicalPlan plan) {
        int sum = 0;
        for (int up : stage.inputStages()) {
            sum += plan.instancesOf(up).size();
        }
        return sum;
    }

    /**
     * The schema of the rows arriving on this stage's inbound: the input schema of its first member, which is the source's own output for a
     * stage headed by a source and the output of the stage above otherwise.
     */
    private static DataSchema stageInMeta(PlanGraph graph, Stage stage) {
        int firstMember = stage.members().get(0);
        PlanNode first = graph.node(firstMember);
        if (first.kind() == PlanNode.Kind.SOURCE) {
            return first.outMeta();
        }
        // Where a stage's first member is a breaker, that member's child, which is the last node of the stage
        // above, carries the schema this stage receives.
        return graph.node(first.child(0)).outMeta();
    }

    private static PlanNode sourceOf(PlanGraph graph, Stage stage) {
        int firstMember = stage.members().get(0);
        PlanNode first = graph.node(firstMember);
        if (first.kind() != PlanNode.Kind.SOURCE) {
            throw new IllegalStateException("first member of a source-headed stage is not SOURCE: " + first.typeId());
        }
        return first;
    }

    /**
     * Rebuilds a stage's chain of members into a fragment that can be executed: a placeholder source carrying the inbound schema at index
     * 0, then each member's operator wired to the node before it in the fragment. For a stage headed by a source the source member is
     * skipped, since the placeholder stands in for it. At parallelism one a fragment is a linear run of a single source and unary
     * operators, which executes directly.
     */
    private static PlanGraph stageFragment(PlanGraph graph, Stage stage, DataSchema inMeta) {
        java.util.List<PlanNode> nodes = new java.util.ArrayList<>();
        PlanNode placeholder = PlanNode.placeholderSource(inMeta);
        if (stage.inputStages().isEmpty()) {
            int sourceIndex = stage.members().get(0);
            if (io.rowweave.plan.SourcePositionPlanValidator
                    .sourceFeedsPositionConsumer(graph, sourceIndex)) {
                String sourceUid = sourceOf(graph, stage)
                        .param(PlanNode.OPERATOR_UID_PARAM);
                if (sourceUid != null) {
                    placeholder = placeholder.withParam(
                            PlanNode.OPERATOR_UID_PARAM, sourceUid);
                }
            }
        } else {
            int firstMember = stage.members().get(0);
            PlanNode first = graph.node(firstMember);
            if (io.rowweave.plan.ObjectStorageQualityPlanValidator.isRoute(first)
                    && first.childCount() == 1) {
                PlanNode qualitySource = graph.node(first.child(0));
                String sourceUid = qualitySource.param(PlanNode.OPERATOR_UID_PARAM);
                if (sourceUid != null) {
                    placeholder = placeholder.withParam(
                            PlanNode.OPERATOR_UID_PARAM, sourceUid);
                }
            }
        }
        if (stage.isSink()) {
            int sinkIndex = stage.members().get(stage.members().size() - 1);
            String sinkRole = graph.node(sinkIndex).param(PlanNode.OPERATOR_UID_PARAM);
            if (io.rowweave.plan.TypedQualityPublicationPlanValidator
                    .isPublicationSinkRole(sinkRole)) {
                placeholder = placeholder.withParam(
                        io.rowweave.plan.TypedQualityPublicationPlanValidator
                                .WORKER_SINK_ROLE_PARAM,
                        sinkRole).withParam(
                        io.rowweave.plan.TypedQualityPublicationPlanValidator
                                .WORKER_SINK_BINDING_PARAM,
                        graph.node(sinkIndex).batchSinkDescriptor().bindingIdentity());
            }
        }
        nodes.add(placeholder);  // the placeholder source at index 0 stands for a real one, keeping its logical identity for a stage headed by a source
        int prevIdx = 0;
        for (int memberIdx : stage.members()) {
            PlanNode m = graph.node(memberIdx);
            if (m.kind() == PlanNode.Kind.SOURCE) {
                continue;  // the placeholder source takes the place of the real one
            }
            if (m.kind() == PlanNode.Kind.SINK) {
                continue;  // the sink node is not part of the fragment, because the output a SINK stage writes travels with the stage command
            }
            nodes.add(rebuildUnaryOnChild(m, prevIdx));
            prevIdx = nodes.size() - 1;
        }
        return new PlanGraph(nodes);
    }

    /**
     * Whether a SINK stage needs its output put into a single total order. Rows arriving from several instances of the stage above do not
     * arrive in a determined order, so unless the stage's own chain already ends in an operator that orders, an explicit sort or a limit, a
     * total-order sort is appended to the end. That is what makes the result agree with the local path, which produces a single stream.
     */
    private static boolean needsCanonicalize(PlanGraph graph, Stage stage) {
        // The members of a SINK stage are the chain, an optional sort and finally the sink. The one to ask about
        // is the last that is not the sink.
        for (int i = stage.members().size() - 1; i >= 0; i--) {
            PlanNode m = graph.node(stage.members().get(i));
            if (m.kind() == PlanNode.Kind.SINK) {
                continue;
            }
            String t = m.typeId();
            return !("orderBy".equals(t) || "top".equals(t));  // the chain already ends in an explicit order, so nothing is appended
        }
        return true;  // a bare sink with no operators, so a total order is appended
    }

    /**
     * Appends to the end of a fragment a stable total-order sort over every field, which turns a nondeterministic arrival order into one
     * that is identical for everyone. The sort's fields are built from the fragment's output schema, the columns the rows being sorted
     * actually have, and not from the inbound schema: an operator in the stage can change the shape of a row, an aggregate over the whole
     * input turning several columns into one, and building the sort against the inbound schema would then reference columns that are not
     * there.
     */
    private static PlanGraph appendOrderByAllFields(PlanGraph frag, DataSchema meta) {
        java.util.List<PlanNode> nodes = new java.util.ArrayList<>();
        for (int i = 0; i < frag.nodeCount(); i++) {
            nodes.add(frag.node(i));
        }
        int prevIdx = frag.rootIndex();
        DataSchema outMeta = frag.root().outMeta();
        java.util.List<String> sort = new java.util.ArrayList<>();
        for (int i = 0; i < outMeta.getFieldCount(); i++) {
            sort.add(io.rowweave.plan.PlanOps.encodeSortField(new io.rowweave.ops.SortField(
                    i, io.rowweave.ops.SortField.Direction.ASC, io.rowweave.ops.SortField.NullOrder.NULLS_LAST)));
        }
        nodes.add(PlanNode.unary("orderBy", outMeta, prevIdx).listParam("sort", sort).build());
        return new PlanGraph(nodes);
    }

    /**
     * Copies a unary node with its child pointing at a new index, carrying over its parameters, its list parameters and its user function.
     */
    private static PlanNode rebuildUnaryOnChild(PlanNode n, int newChildIdx) {
        if (n.userFunction() != null) {
            return PlanNode.unaryFnWithParams(n.typeId(), n.outMeta(), newChildIdx,
                    n.params(), n.listParams(), n.userFunction());
        }
        PlanNode.Builder b = PlanNode.unary(n.typeId(), n.outMeta(), newChildIdx);
        for (Map.Entry<String, String> e : n.params().entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, List<String>> e : n.listParams().entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
        return b.build();
    }

    /**
     * Fail-fast aggregate wait: attach {@link JobBarrier} to every stage tracker, so the first failure
     * short-circuits instead of a timeout accumulating per stage, all successes release it, and one job
     * deadline is the backstop against hanging. On success the rowCount is taken from the sink tracker, and
     * the trackers are cleared in a finally block.
     */
    /**
     * Waiting for a terminal state has two different authorities on time. An ephemeral submission is bounded by a deadline in the request,
     * measured monotonically. For a durable run the backend's deadline decides the product's terminal state, and where no compatible hard
     * limit was set on the job, waiting stays driven by whether the workers are alive and making progress. The barrier is therefore polled
     * in short rounds, each round checking the deadline or cancellation authority that applies and the heartbeats of the workers involved.
     */
    private RpcMessage awaitJob(java.util.List<Long> all, java.util.List<StageTracker> sinkReports)
            throws InterruptedException {
        return awaitJob(all, sinkReports, java.util.Collections.emptyList(),
                null, null, null, null, null);
    }

    private RpcMessage awaitJob(
            java.util.List<Long> all, java.util.List<StageTracker> sinkReports,
            java.util.List<Long> sinkStageIds, String commitGroupId,
            ManifestJobCommit manifestCommit, DurableRunExecution durable,
            EphemeralSubmitDeadline ephemeralDeadline,
            RpcMessage residentRequest) throws InterruptedException {
        JobBarrier barrier = new JobBarrier(all.size());
        for (long id : all) {
            stageTrackers.get(id).attachBarrier(barrier);
        }
        // The set of workers this job involves, looked up from what was recorded per stage when it was assigned.
        java.util.Map<String, StageOwner> jobWorkers = new java.util.LinkedHashMap<>();
        for (long id : all) {
            StageOwner owner = stageWorker.get(id);
            if (owner != null) {
                jobWorkers.putIfAbsent(owner.workerId, owner);
            }
        }
        long hardLimitMs = ephemeralDeadline == null
                ? Long.getLong("rowweave.cluster.jobHardLimitMs", 0L) : 0L;
        long start = System.currentTimeMillis();
        try {
            while (true) {
                long waitMillis = ephemeralDeadline == null ? HEARTBEAT_POLL_MS
                        : ephemeralDeadline.executionWaitMillis(HEARTBEAT_POLL_MS);
                if (waitMillis == 0L) {
                    return settleEphemeralTimeout(all, sinkStageIds, commitGroupId,
                            manifestCommit, ephemeralDeadline);
                }
                boolean completed = barrier.await(waitMillis);
                if (durable != null && durable.userCancellationRequested()) {
                    return settleActiveUserCancellation(
                            all, sinkStageIds, commitGroupId, manifestCommit, durable);
                }
                if (durable != null && durable.deadlineTimeoutRequested(false)) {
                    return settleActiveTimeout(
                            all, sinkStageIds, commitGroupId, manifestCommit, durable);
                }
                if (completed) {
                    break;  // done, either success or the first failure that short-circuited it
                }
                if (ephemeralDeadline != null
                        && ephemeralDeadline.executionExpired()) {
                    return settleEphemeralTimeout(all, sinkStageIds, commitGroupId,
                            manifestCommit, ephemeralDeadline);
                }
                // Not finished: are all the workers involved still alive. If any has gone quiet it is either dead or
                // wedged, so fail fast.
                String deadWorker = firstDeadWorker(jobWorkers.values());
                if (deadWorker != null) {
                    long cleanupDeadline = deadlineAfterMillis(outputCommitTimeoutMs);
                    Throwable rollbackFailure = rollbackOutputsBeforePublication(
                            commitGroupId, sinkStageIds, manifestCommit, durable,
                            cleanupDeadline, "WORKER_LOST");
                    StageCancellationCoordinator.Result cancellation =
                            cancelPendingStages(all, cleanupDeadline);
                    String error = "worker lost during job (heartbeat stale): " + deadWorker;
                    if (rollbackFailure != null) {
                        error += "; output rollback failed: " + describeFailure(rollbackFailure);
                    }
                    if (!cancellation.complete()) {
                        error += "; stage cancellation evidence incomplete";
                    }
                    return RpcMessage.jobResult(false, 0, error);
                }
                if (hardLimitMs > 0 && System.currentTimeMillis() - start > hardLimitMs) {
                    long cleanupDeadline = deadlineAfterMillis(outputCommitTimeoutMs);
                    Throwable rollbackFailure = rollbackOutputsBeforePublication(
                            commitGroupId, sinkStageIds, manifestCommit, durable,
                            cleanupDeadline, "JOB_HARD_LIMIT");
                    StageCancellationCoordinator.Result cancellation =
                            cancelPendingStages(all, cleanupDeadline);
                    String error = "job exceeded hard limit " + hardLimitMs + "ms";
                    if (rollbackFailure != null) {
                        error += "; output rollback failed: " + describeFailure(rollbackFailure);
                    }
                    if (!cancellation.complete()) {
                        error += "; stage cancellation evidence incomplete";
                    }
                    return RpcMessage.jobResult(false, 0, error);
                }
                if (commitGroupId != null) {
                    long renewalDeadline = deadlineAfterMillis(HEARTBEAT_POLL_MS);
                    if (ephemeralDeadline != null) {
                        renewalDeadline = Math.min(renewalDeadline,
                                ephemeralDeadline.executionDeadlineNanos());
                    }
                    renewPreparedOutputsWhileWaiting(
                            commitGroupId, sinkStageIds, renewalDeadline);
                }
                // The workers are all alive and the job is not finished, which is a large job still making progress:
                // keep waiting rather than condemning it.
            }
            if (durable != null && durable.deadlineTimeoutRequested(true)) {
                return settleActiveTimeout(
                        all, sinkStageIds, commitGroupId, manifestCommit, durable);
            }
            if (barrier.failed()) {
                long cleanupDeadline = deadlineAfterMillis(outputCommitTimeoutMs);
                Throwable rollbackFailure = rollbackOutputsBeforePublication(
                        commitGroupId, sinkStageIds, manifestCommit, durable,
                        cleanupDeadline, "STAGE_FAILED");
                StageCancellationCoordinator.Result cancellation =
                        cancelPendingStages(all, cleanupDeadline);
                String error = barrier.firstError() != null ? barrier.firstError() : "stage failed";
                if (rollbackFailure != null) {
                    error += "; output rollback failed: " + describeFailure(rollbackFailure);
                }
                if (!cancellation.complete()) {
                    error += "; stage cancellation evidence incomplete";
                }
                return RpcMessage.jobResult(false, 0,
                        error);
            }
            if (commitGroupId != null) {
                if (ephemeralDeadline != null
                        && ephemeralDeadline.executionExpired()) {
                    return settleEphemeralTimeout(
                            all, sinkStageIds, commitGroupId,
                            manifestCommit, ephemeralDeadline);
                }
                if (durable != null) {
                    try {
                        durable.markCommitting();
                    } catch (Throwable barrierFailure) {
                        if (durable.userCancellationRequested()) {
                            return settleActiveUserCancellation(
                                    all, sinkStageIds, commitGroupId,
                                    manifestCommit, durable);
                        }
                        if (durable.deadlineTimeoutRequested(true)) {
                            return settleActiveTimeout(
                                    all, sinkStageIds, commitGroupId,
                                    manifestCommit, durable);
                        }
                        Throwable cleanupFailure = rollbackOutputsBeforePublication(
                                commitGroupId, sinkStageIds, manifestCommit, durable,
                                deadlineAfterMillis(outputCommitTimeoutMs),
                                "COMMIT_BARRIER_FAILED");
                        if (cleanupFailure != null && cleanupFailure != barrierFailure) {
                            barrierFailure.addSuppressed(cleanupFailure);
                        }
                        return RpcMessage.jobResult(false, 0, describeFailure(barrierFailure));
                    }
                }
                long outputControlDeadline =
                        deadlineAfterMillis(outputCommitTimeoutMs);
                long commitDeadline = outputControlDeadline;
                long cleanupDeadline = outputControlDeadline;
                if (ephemeralDeadline != null) {
                    // TC5: publication may consume only the execution window; rollback/finalize
                    // may use cleanup grace, still capped by the Sink control sub-budget.
                    commitDeadline = Math.min(
                            outputControlDeadline,
                            ephemeralDeadline.executionDeadlineNanos());
                    cleanupDeadline = Math.min(
                            outputControlDeadline,
                            ephemeralDeadline.terminalDeadlineNanos());
                }
                try {
                    if (manifestCommit == null) {
                        coordinatePreparedOutputs(
                                commitGroupId, sinkStageIds,
                                commitDeadline, cleanupDeadline);
                    } else {
                        coordinateManifestOutputs(
                                commitGroupId, sinkStageIds, manifestCommit, durable,
                                commitDeadline, cleanupDeadline);
                    }
                } catch (Throwable commitFailure) {
                    if (ephemeralDeadline != null
                            && ephemeralDeadline.executionExpired()) {
                        return settleEphemeralTimeout(
                                all, sinkStageIds, commitGroupId,
                                manifestCommit, ephemeralDeadline);
                    }
                    return RpcMessage.jobResult(false, 0, describeFailure(commitFailure));
                }
            }
            long totalRows = 0;  // the row counts of every sink summed, which with one sink is that sink alone
            for (StageTracker t : sinkReports) {
                totalRows = Math.addExact(totalRows, t.rowCount);
            }
            if (durable == null) {
                return ephemeralJobResult(totalRows, all, residentRequest);
            }
            return RpcMessage.jobResult(true, totalRows, null);
        } catch (InterruptedException interrupted) {
            long cleanupDeadline = deadlineAfterMillis(outputCommitTimeoutMs);
            Throwable cleanupFailure = rollbackOutputsBeforePublication(
                    commitGroupId, sinkStageIds, manifestCommit, durable,
                    cleanupDeadline, "MASTER_INTERRUPTED");
            if (cleanupFailure != null && cleanupFailure != interrupted) {
                interrupted.addSuppressed(cleanupFailure);
            }
            StageCancellationCoordinator.Result cancellation =
                    cancelPendingStages(all, cleanupDeadline);
            if (!cancellation.complete()) {
                interrupted.addSuppressed(new IllegalStateException(
                        "stage cancellation evidence incomplete"));
            }
            throw interrupted;
        } finally {
            cleanupStageTracking(all);
        }
    }

    private RpcMessage settleActiveUserCancellation(
            java.util.List<Long> all, java.util.List<Long> sinkStageIds,
            String commitGroupId, ManifestJobCommit manifestCommit,
            DurableRunExecution durable) {
        long cleanupDeadline = deadlineAfterMillis(outputCommitTimeoutMs);
        Throwable cleanupFailure = rollbackOutputsBeforePublication(
                commitGroupId, sinkStageIds, manifestCommit, durable,
                cleanupDeadline, "USER_CANCELED");
        // Aborting a manifest has to precede the rollback on the worker, because cancelling a stage there is what
        // rolls back the output that stage had prepared.
        StageCancellationCoordinator.Result cancellation =
                cancelPendingStages(all, cleanupDeadline);
        boolean terminal = cleanupFailure == null
                && durable.completeUserCancellationIfProven();
        String error = terminal
                ? "run canceled by user"
                : "run cancellation accepted but terminal evidence is incomplete";
        if (cleanupFailure != null) {
            error += "; output rollback failed: " + describeFailure(cleanupFailure);
        }
        return RpcMessage.jobResult(false, 0, error);
    }

    private RpcMessage settleActiveTimeout(
            java.util.List<Long> all, java.util.List<Long> sinkStageIds,
            String commitGroupId, ManifestJobCommit manifestCommit,
            DurableRunExecution durable) {
        long cleanupDeadline = deadlineAfterMillis(outputCommitTimeoutMs);
        Throwable cleanupFailure = rollbackOutputsBeforePublication(
                commitGroupId, sinkStageIds, manifestCommit, durable,
                cleanupDeadline, io.rowweave.run.RunDeadlineAuthorityKernel.REASON_CODE);
        // Durable abort must precede Worker cancellation/rollback.
        StageCancellationCoordinator.Result cancellation =
                cancelPendingStages(all, cleanupDeadline);
        boolean terminal = cleanupFailure == null
                && durable.completeTimeoutIfProven();
        String error = terminal
                ? "run deadline reached"
                : "run deadline accepted but terminal evidence is incomplete";
        if (cleanupFailure != null) {
            error += "; output rollback failed: "
                    + describeFailure(cleanupFailure);
        }
        return RpcMessage.jobResult(false, 0, error);
    }

    private RpcMessage settleEphemeralTimeout(
            java.util.List<Long> all, java.util.List<Long> sinkStageIds,
            String commitGroupId, ManifestJobCommit manifestCommit,
            EphemeralSubmitDeadline deadline) {
        int nonterminal = 0;
        for (long stageId : all) {
            StageTracker tracker = stageTrackers.get(stageId);
            if (tracker != null && !tracker.hasWorkerTerminal()) nonterminal++;
        }
        Throwable cleanupFailure = rollbackOutputsBeforePublication(
                commitGroupId, sinkStageIds, manifestCommit, null,
                deadline.terminalDeadlineNanos(), EPHEMERAL_EXECUTION_TIMEOUT);
        StageCancellationCoordinator.Result cancellation = cancelPendingStages(
                all, deadline.terminalDeadlineNanos());
        String category = cleanupFailure == null && cancellation.complete()
                ? EPHEMERAL_EXECUTION_TIMEOUT
                : EPHEMERAL_TIMEOUT_CLEANUP_INCOMPLETE;
        String diagnostic = category
                + " configuredMs=" + deadline.executionTimeoutMillis()
                + " elapsedMs=" + deadline.elapsedMillis()
                + " stages=" + all.size()
                + " nonterminal=" + nonterminal;
        return RpcMessage.jobResult(false, 0, diagnostic);
    }

    private void coordinatePreparedOutputs(
            String groupId, java.util.List<Long> sinkStageIds,
            long commitDeadline, long cleanupDeadline) {
        try {
            for (long stageId : sinkStageIds) {
                sendOutputControl(groupId, stageId,
                        RpcMessage.Type.COMMIT_PREPARED_OUTPUT, commitDeadline);
            }
            // All publish ACKs before this check are the ordinary-Sink success
            // linearization point. Until then every member remains compensatable.
            requireDeadlineRemaining(commitDeadline, "output commit");
        } catch (Throwable publishFailure) {
            rollbackPreparedOutputs(
                    groupId, sinkStageIds, publishFailure, cleanupDeadline);
            throwUnchecked(publishFailure);
        }

        Throwable finalizeFailure = null;
        for (long stageId : sinkStageIds) {
            try {
                sendOutputControl(groupId, stageId,
                        RpcMessage.Type.FINALIZE_PREPARED_OUTPUT, cleanupDeadline);
            } catch (Throwable failure) {
                if (finalizeFailure == null) {
                    finalizeFailure = failure;
                } else {
                    finalizeFailure.addSuppressed(failure);
                }
            }
        }
        if (finalizeFailure != null) {
            throw new IllegalStateException("outputs published but finalize failed", finalizeFailure);
        }
    }

    private void coordinateManifestOutputs(
            String groupId, java.util.List<Long> sinkStageIds, ManifestJobCommit context,
            DurableRunExecution durable, long commitDeadline, long cleanupDeadline) {
        if (context.genericProvider()) {
            try {
                coordinateGenericProviderOutputs(
                        groupId, sinkStageIds, context, durable,
                        commitDeadline);
            } catch (Throwable commitFailure) {
                try {
                    if (resolveNormalGenericProviderFailure(
                            groupId, sinkStageIds, context, durable,
                            cleanupDeadline)) {
                        return;
                    }
                } catch (Throwable resolutionFailure) {
                    if (resolutionFailure != commitFailure) {
                        resolutionFailure.addSuppressed(commitFailure);
                    }
                    throwUnchecked(resolutionFailure);
                    return;
                }
                throwUnchecked(commitFailure);
            }
            return;
        }
        java.util.List<io.rowweave.commit.CommitManifest.Member> members = new java.util.ArrayList<>();
        try {
            for (long stageId : sinkStageIds) {
                PreparedSinkReceipt prepared = preparedSinkReceipt(
                        durable, stageId);
                members.add(prepared == null
                        ? sendManifestInstall(
                        groupId, stageId, context, commitDeadline)
                        : io.rowweave.commit.ManifestCommitWire.decodeMember(
                        prepared.locator().payload()));
            }
            requireDeadlineRemaining(commitDeadline, "manifest output commit");
            validateManifestMembers(context, members);
        } catch (Throwable installFailure) {
            Throwable rollbackFailure =
                    abortDurableManifestOutputs(
                            groupId, sinkStageIds, context, durable, cleanupDeadline,
                            "MANIFEST_INSTALL_FAILED");
            if (rollbackFailure == null && durable != null) {
                try {
                    durable.failPrepublicationCommit();
                } catch (Throwable terminalFailure) {
                    installFailure.addSuppressed(terminalFailure);
                }
            } else if (rollbackFailure != null) {
                installFailure.addSuppressed(rollbackFailure);
            }
            throwUnchecked(installFailure);
        }

        io.rowweave.commit.CommitManifest intended = new io.rowweave.commit.CommitManifest(
                context.store.domainId(), context.generation, context.attemptId, members);
        if (durable != null) {
            coordinateDurableManifestPublication(
                    groupId, sinkStageIds, context, members, intended,
                    commitDeadline, durable);
            if (!allProviderPrepared(durable, sinkStageIds)) {
                try {
                    finalizeManifestOutputs(groupId, sinkStageIds, cleanupDeadline);
                } catch (RuntimeException cleanupFailure) {
                    // Visibility and durable intent are already authoritative. Finalize only releases
                    // bounded Worker cleanup state and must not downgrade a committed Run.
                    LOG.warn("durable manifest finalize deferred after publication run={} publication={}",
                            durable.runId, context.attemptId, cleanupFailure);
                }
            }
            return;
        }
        try {
            io.rowweave.plan.LocalQualityGate.enforce(
                    context.graph, members, context.attemptId,
                    context.generation, commitDeadline);
            beforeManifestPublishHook.run();
            requireDeadlineRemaining(commitDeadline, "manifest output commit");
            sendManifestPublish(groupId, sinkStageIds.get(0), context,
                    intended.encode(), commitDeadline);
        } catch (Throwable publishFailure) {
            io.rowweave.commit.CommitManifest observed;
            try {
                observed = context.store.exact(context.generation);
            } catch (Throwable corrupt) {
                publishFailure.addSuppressed(corrupt);
                throwUnchecked(publishFailure);
                return;
            }
            if (!intended.equals(observed)) {
                if (observed == null) {
                    rollbackPreparedOutputs(
                            groupId, sinkStageIds, publishFailure, cleanupDeadline);
                } else {
                    publishFailure.addSuppressed(new IllegalStateException(
                            "manifest generation conflict; preserving immutable data"));
                }
                throwUnchecked(publishFailure);
            }
            // An identical manifest means the commit happened, so losing the acknowledgement of a publish must not
            // lead to a rollback.
        }
        afterManifestPublishHook.run();

        finalizeManifestOutputs(groupId, sinkStageIds, cleanupDeadline);
    }

    private void coordinateGenericProviderOutputs(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context, DurableRunExecution durable,
            long deadline) {
        if (durable == null || !allProviderPrepared(durable, sinkStageIds)) {
            throw new IllegalStateException(
                    "generic provider publication requires durable locator closure");
        }
        java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                new java.util.ArrayList<>(sinkStageIds.size());
        for (long stageId : sinkStageIds) {
            locators.add(preparedSinkReceipt(durable, stageId).locator());
        }
        DurableSinkAuthorityAdapter adapter =
                context.providerReservation.bind(locators);
        DurableSinkCommitCoordinator coordinator =
                new DurableSinkCommitCoordinator(adapter,
                        (action, decisionDigest) -> executeProviderGroupControl(
                                groupId, sinkStageIds, context, durable,
                                action, decisionDigest, deadline));
        beforeManifestPublishHook.run();
        context.providerOutcome = coordinator.publishAndFinalize(
                beforeDurableDecisionHookForTesting,
                afterDurableDecisionHookForTesting);
        afterManifestPublishHook.run();
    }

    private boolean resolveNormalGenericProviderFailure(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context, DurableRunExecution durable,
            long deadline) {
        java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                new java.util.ArrayList<>(sinkStageIds.size());
        for (long stageId : sinkStageIds) {
            PreparedSinkReceipt prepared = preparedSinkReceipt(durable, stageId);
            if (prepared == null) {
                DurableSinkAuthorityAdapter adapter =
                        context.providerReservation.bind(
                                java.util.Collections.emptyList());
                adapter.requireAbortDecision(
                        "PROVIDER_PREPUBLICATION_FAILED");
                durable.failPrepublicationCommit();
                return false;
            }
            locators.add(prepared.locator());
        }
        DurableSinkAuthorityAdapter adapter =
                context.providerReservation.bind(locators);
        io.rowweave.commit.BatchSinkOutcome authority =
                adapter.authorityOutcome();
        DurableSinkCommitCoordinator coordinator =
                new DurableSinkCommitCoordinator(adapter,
                        (action, decisionDigest) ->
                                executeProviderGroupControl(
                                        groupId, sinkStageIds, context,
                                        durable, action, decisionDigest,
                                        deadline));
        if (authority.state()
                == io.rowweave.commit.BatchSinkOutcome.State.COMMIT_DECIDED
                || authority.state()
                == io.rowweave.commit.BatchSinkOutcome.State.COMMITTED) {
            context.providerOutcome = coordinator.publishAndFinalize();
            return true;
        }
        if (authority.state()
                == io.rowweave.commit.BatchSinkOutcome.State.PREPARING
                || authority.state()
                == io.rowweave.commit.BatchSinkOutcome.State.NOT_FOUND) {
            context.providerOutcome = coordinator.abort(
                    "PROVIDER_PREPUBLICATION_FAILED");
            durable.failPrepublicationCommit();
            return false;
        }
        throw new IllegalStateException(
                "provider failure resolution reached incompatible authority state: "
                        + authority.state());
    }

    private static void requireDeadlineRemaining(long deadlineNanos, String operation) {
        if (deadlineNanos - System.nanoTime() <= 0L) {
            throw new IllegalStateException(operation + " deadline exceeded");
        }
    }

    private void coordinateDurableManifestPublication(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context,
            java.util.List<io.rowweave.commit.CommitManifest.Member> members,
            io.rowweave.commit.CommitManifest intended, long deadline,
            DurableRunExecution durable) {
        if (stageAssignmentAuthority == null) {
            throw new IllegalStateException("durable manifest authority unavailable");
        }
        io.rowweave.commit.CommitManifest effectiveIntended = intended;
        try {
            boolean qualityBounded = durable.assignmentPlan.manifest()
                    .hasQualityGateBinding();
            for (int i = 0; i < sinkStageIds.size(); i++) {
                long stageId = sinkStageIds.get(i);
                PreparedSinkReceipt prepared = preparedSinkReceipt(
                        durable, stageId);
                final ManifestMemberReceipt receipt;
                if (prepared != null) {
                    io.rowweave.run.PreparedSinkLocator locator =
                            prepared.locator();
                    receipt = ManifestMemberReceipt.sealed(
                            prepared.token(), prepared.commandDigest(),
                            context.store.domainId(), context.generation,
                            context.attemptId, members.get(i),
                            locator.preparedAtMillis(), locator);
                } else {
                    StageOwner owner = requiredCurrentOutputOwner(stageId);
                    sendManifestSeal(groupId, stageId, context, deadline);
                    if (owner.assignmentToken == null
                            || owner.commandDigest == null) {
                        throw new IllegalStateException(
                                "durable manifest output lacks assignment identity: stageId="
                                        + stageId);
                    }
                    receipt = ManifestMemberReceipt.sealed(
                            owner.assignmentToken, owner.commandDigest,
                            context.store.domainId(), context.generation,
                            context.attemptId, members.get(i), 0L,
                            preparedSinkLocator(
                                    durable.assignmentPlan
                                            .sinkAttemptIdentity(stageId),
                                    context.store.domainId(), members.get(i), 0L));
                }
                ManifestMemberDecision decision = durable.recordManifestMember(receipt);
                if (decision.code() != ManifestMemberDecision.Code.RECORDED
                        && decision.code() != ManifestMemberDecision.Code.ALREADY_RECORDED) {
                    int ordinal = receipt.token().physicalOrdinal();
                    ManifestMemberReceipt existing;
                    if (qualityBounded) {
                        io.rowweave.run.QualityGateCommitJournalSnapshot observed =
                                stageAssignmentAuthority.qualityGateCommitJournal(
                                        durable.fence());
                        existing = observed == null ? null
                                : observed.memberForOrdinal(ordinal);
                    } else {
                        ManifestCommitJournalSnapshot observed =
                                durable.manifestCommitJournal();
                        existing = observed == null || ordinal < 0
                                || ordinal >= observed.assignments().manifest()
                                .assignmentCount() ? null : observed.member(ordinal);
                    }
                    throw new IllegalStateException(
                            "manifest member journal rejected: " + decision.code()
                                    + " stageId=" + stageId
                                    + " ordinal=" + ordinal
                                    + " member=" + members.get(i).name()
                                    + " existingMember=" + (existing == null
                                    ? "none" : existing.member().name()));
                }
            }
            if (durable.incremental()) {
                effectiveIntended = installIncrementalControlMember(
                                groupId, context, members, deadline, durable);
            }
            io.rowweave.run.ManifestCommitJournalSnapshot journal = null;
            io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal = null;
            if (qualityBounded) {
                qualityJournal = stageAssignmentAuthority
                        .qualityGateCommitJournal(durable.fence());
            } else {
                journal = durable.manifestCommitJournal();
            }
            if (qualityBounded
                    ? qualityJournal == null || !qualityJournal.allMembersSealed()
                    : journal == null || !journal.allExpectedMembersSealed()) {
                throw new IllegalStateException("manifest member journal is incomplete");
            }
            if (!effectiveIntended.equals(qualityBounded
                    ? qualityJournal.intendedManifest()
                    : journal.intendedManifest())) {
                throw new IllegalStateException("manifest journal conflicts with installed members");
            }
            if (qualityBounded) {
                io.rowweave.QualityGatePolicy policy = qualityGatePolicy(context.graph);
                io.rowweave.QualityEvidenceReadGuard guard =
                        io.rowweave.QualityEvidenceReadGuard.until(
                                deadline, durable::userCancellationRequested);
                io.rowweave.QualityPreparedEvidence evidence =
                        io.rowweave.QualityPreparedEvidenceReader.read(
                                context.graph, qualityJournal, guard);
                io.rowweave.QualityGateDecision qualityDecision =
                        io.rowweave.QualityGateDecision.evaluate(
                                durable.snapshot.artifactId(), durable.fence(),
                                durable.publicationId, policy, evidence);
                io.rowweave.QualityGateDecisionResult recorded =
                        durable.recordQualityGateDecision(qualityDecision);
                if (recorded.code()
                        != io.rowweave.QualityGateDecisionResult.Code.RECORDED
                        && recorded.code()
                        != io.rowweave.QualityGateDecisionResult.Code.ALREADY_RECORDED) {
                    throw new IllegalStateException(
                            "quality decision rejected: " + recorded.code());
                }
                qualityDecision = recorded.decision();
                qualityJournal = qualityJournal.withQualityDecision(
                        qualityDecision);
                if (!qualityDecision.equals(qualityJournal.decision())) {
                    throw new IllegalStateException(
                            "durable quality decision disappeared");
                }
                if (qualityDecision.outcome()
                        == io.rowweave.QualityGateDecision.Outcome.FAIL) {
                    Throwable cleanupFailure = abortDurableManifestOutputs(
                            groupId, sinkStageIds, context, durable, deadline,
                            "QUALITY_GATE_REJECTED");
                    if (cleanupFailure != null) {
                        throw new IllegalStateException(
                                "quality rejection cleanup could not be proven",
                                cleanupFailure);
                    }
                    durable.failQualityGateRejected();
                    throw new QualityGateRejectedException();
                }
            }
            ManifestCommitIntent requestedIntent = qualityBounded
                    ? qualityJournal.buildIntent() : journal.buildIntent();
            beforeDurableDecisionHookForTesting.run();
            ManifestIntentDecision intentDecision =
                    durable.commitManifestIntent(requestedIntent);
            if (intentDecision.code() != ManifestIntentDecision.Code.COMMITTED
                    && intentDecision.code() != ManifestIntentDecision.Code.ALREADY_COMMITTED) {
                throw new IllegalStateException(
                        "manifest intent rejected: " + intentDecision.code());
            }
            if (!requestedIntent.equals(intentDecision.intent())) {
                throw new IllegalStateException(
                        "manifest intent authority acknowledged different bytes");
            }
            afterDurableDecisionHookForTesting.run();
            if (qualityBounded) {
                qualityJournal = qualityJournal.withIntent(intentDecision.intent());
            } else {
                journal = journal.withIntent(intentDecision.intent());
            }
            if (!effectiveIntended.equals(qualityBounded
                    ? qualityJournal.intendedManifest()
                    : journal.intendedManifest())) {
                throw new IllegalStateException("durable manifest intent disappeared");
            }

            beforeManifestPublishHook.run();
            final io.rowweave.commit.CommitManifest publicationManifest =
                    effectiveIntended;
            if (durable.incremental()) {
                publishIncrementalManifest(
                        groupId, context, members, publicationManifest,
                        deadline, durable);
            } else if (allProviderPrepared(durable, sinkStageIds)) {
                ManifestDurableSinkAuthorityAdapter adapter =
                        new ManifestDurableSinkAuthorityAdapter(
                                intentDecision.intent(), publicationManifest,
                                () -> projectManifestProviderAuthorityOutcome(
                                        context, publicationManifest));
                DurableSinkCommitCoordinator coordinator =
                        new DurableSinkCommitCoordinator(adapter,
                                (action, decisionDigest) ->
                                        executeProviderGroupControl(
                                                groupId, sinkStageIds, context,
                                                durable, action, decisionDigest,
                                                deadline));
                coordinator.publishAndFinalize();
            } else {
                // Pre-R4 recovery fixtures may still carry only the sealed Manifest journal.
                // New durable assignments always take the generic provider control path above.
                publishIntendedManifest(context, publicationManifest);
            }
            afterManifestPublishHook.run();
        } catch (QualityGateRejectedException rejected) {
            throw rejected;
        } catch (Throwable commitFailure) {
            try {
                if (resolveNormalDurableManifestFailure(
                        groupId, sinkStageIds, context, effectiveIntended,
                        deadline, durable)) {
                    return;
                }
            } catch (Throwable resolutionFailure) {
                if (resolutionFailure != commitFailure) {
                    resolutionFailure.addSuppressed(commitFailure);
                }
                throwUnchecked(resolutionFailure);
                return;
            }
            throwUnchecked(commitFailure);
        }
    }

    private io.rowweave.commit.CommitManifest
    installIncrementalControlMember(
            String groupId, ManifestJobCommit context,
            java.util.List<io.rowweave.commit.CommitManifest.Member> dataMembers,
            long deadline, DurableRunExecution durable) {
        io.rowweave.incremental.IncrementalAuthorityRecord authorityRecord =
                durable.incrementalAuthority.record(
                        durable.incrementalRequest.feedId());
        io.rowweave.incremental.IncrementalActiveInterval active =
                authorityRecord == null ? null : authorityRecord.activeInterval();
        if (active == null || !durable.fence().equals(active.fence())) {
            throw new IllegalStateException(
                    "incremental control member lost the active interval");
        }
        byte[] controlFrame = io.rowweave.incremental
                .IncrementalControlMemberCodec.encodeForPublication(
                        durable.incrementalRequest.feedId(),
                        authorityRecord.definitionDigest(), active,
                        incrementalPlanDescriptor(context.graph),
                        context.attemptId, context.generation, dataMembers);
        io.rowweave.commit.CommitManifest.Member expectedMember =
                new io.rowweave.commit.CommitManifest.Member(
                        io.rowweave.incremental.IncrementalControlMemberCodec
                                .MEMBER_NAME,
                        "data/" + context.generation + "/"
                                + io.rowweave.incremental
                                .IncrementalControlMemberCodec.MEMBER_NAME
                                + ".data",
                        io.rowweave.incremental.IncrementalControlMemberCodec
                                .SCHEMA_FINGERPRINT,
                        controlFrame.length, sha256(controlFrame));
        java.util.List<io.rowweave.commit.CommitManifest.Member> allMembers =
                new java.util.ArrayList<>(dataMembers);
        allMembers.add(expectedMember);
        io.rowweave.commit.CommitManifest intended =
                new io.rowweave.commit.CommitManifest(
                        context.store.domainId(), context.generation,
                        context.attemptId, allMembers);
        byte[] installCommand = IncrementalControlMemberInstallCodec.encode(
                context.store.domainPath().toString(), controlFrame,
                intended.encode());
        byte[] commandDigest = sha256(installCommand);

        io.rowweave.run.StageAssignmentManifest assignmentManifest =
                durable.assignmentPlan.manifest();
        io.rowweave.run.StageAssignmentRecord controlAssignment =
                incrementalControlAssignment(assignmentManifest);
        StageAssignmentSnapshot assignmentSnapshot =
                durable.assignmentSnapshot();
        StageAssignmentToken token = assignmentSnapshot.token(
                controlAssignment.physicalOrdinal());
        long evidenceTime = Math.max(1L, durable.eventTimeMillis);
        StageAcceptanceDecision accepted = durable.acceptAssignment(
                new StageAssignmentAcceptance(
                        token, commandDigest, evidenceTime));
        if (accepted.code() != StageAcceptanceDecision.Code.ACCEPTED
                && accepted.code()
                != StageAcceptanceDecision.Code.ALREADY_ACCEPTED) {
            throw new IllegalStateException(
                    "incremental control assignment rejected: "
                            + accepted.code());
        }

        RegistryWorker endpoint = endpointOf(controlAssignment.workerId());
        if (endpoint == null
                || !controlAssignment.host().equals(endpoint.host())
                || controlAssignment.rpcPort() != endpoint.port()
                || !controlAssignment.workerIncarnationId().equals(
                endpoint.incarnationId())) {
            throw new IllegalStateException(
                    "incremental control Worker generation changed");
        }
        StageOwner owner = new StageOwner(
                endpoint.workerId(), endpoint, durable.fence(), token,
                commandDigest, durable.recoveryToken, durable.frontierToken);
        RpcMessage request;
        MasterElection election = leadership;
        if (election == null) {
            request = RpcMessage.installIncrementalControlMember(
                    endpoint.workerId(), controlAssignment.runtimeStageId(),
                    groupId, context.attemptId, context.generation,
                    installCommand, endpoint.incarnationId(), durable.fence());
        } else {
            long term = election.termToken();
            if (term == 0L) {
                throw new IllegalStateException("not active master leader");
            }
            request = RpcMessage.installIncrementalControlMember(
                    endpoint.workerId(), controlAssignment.runtimeStageId(),
                    groupId, context.attemptId, context.generation,
                    installCommand, election.self(), term,
                    endpoint.incarnationId(), durable.fence());
        }
        io.rowweave.commit.CommitManifest.Member member = callManifestControl(
                owner, controlAssignment.runtimeStageId(), request,
                RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, deadline,
                response -> io.rowweave.commit.ManifestCommitWire.decodeMember(
                        response.commitPayload()));
        if (!expectedMember.equals(member)) {
            throw new IllegalStateException(
                    "incremental Worker installed a different control member");
        }

        StageTerminalDecision terminal = durable.recordStageTerminal(
                StageTerminalReceipt.succeeded(
                        token, commandDigest, 0L, 0L, evidenceTime));
        if (terminal.code() != StageTerminalDecision.Code.RECORDED
                && terminal.code()
                != StageTerminalDecision.Code.ALREADY_RECORDED) {
            throw new IllegalStateException(
                    "incremental control terminal rejected: "
                            + terminal.code());
        }
        io.rowweave.BatchSinkDescriptor descriptor =
                incrementalControlDescriptor(context.store);
        io.rowweave.run.SinkAttemptIdentity identity =
                io.rowweave.run.SinkAttemptIdentity.forAssignment(
                        assignmentManifest, controlAssignment,
                        io.rowweave.incremental.IncrementalControlMemberCodec
                                .MEMBER_NAME,
                        descriptor);
        io.rowweave.run.PreparedSinkLocator locator = preparedSinkLocator(
                identity, context.store.domainId(), member, evidenceTime);
        PreparedSinkDecision prepared = durable.recordPreparedSink(
                new PreparedSinkReceipt(token, commandDigest, locator));
        if (prepared.code() != PreparedSinkDecision.Code.RECORDED
                && prepared.code()
                != PreparedSinkDecision.Code.ALREADY_RECORDED) {
            throw new IllegalStateException(
                    "incremental control locator rejected: "
                            + prepared.code());
        }
        ManifestMemberDecision recorded = durable.recordManifestMember(
                ManifestMemberReceipt.sealed(
                        token, commandDigest, context.store.domainId(),
                        context.generation, context.attemptId, member,
                        evidenceTime, locator));
        if (recorded.code() != ManifestMemberDecision.Code.RECORDED
                && recorded.code()
                != ManifestMemberDecision.Code.ALREADY_RECORDED) {
            throw new IllegalStateException(
                    "incremental control member rejected: "
                            + recorded.code());
        }
        return intended;
    }

    private void publishIncrementalManifest(
            String groupId, ManifestJobCommit context,
            java.util.List<io.rowweave.commit.CommitManifest.Member> dataMembers,
            io.rowweave.commit.CommitManifest intended, long deadline,
            DurableRunExecution durable) {
        io.rowweave.incremental.IncrementalAuthorityRecord authorityRecord =
                durable.incrementalAuthority.record(
                        durable.incrementalRequest.feedId());
        io.rowweave.incremental.IncrementalActiveInterval active =
                authorityRecord == null ? null : authorityRecord.activeInterval();
        if (active == null || !durable.fence().equals(active.fence())) {
            throw new IllegalStateException(
                    "incremental publication lost the active interval");
        }
        byte[] controlFrame = io.rowweave.incremental
                .IncrementalControlMemberCodec.encodeForPublication(
                        durable.incrementalRequest.feedId(),
                        authorityRecord.definitionDigest(), active,
                        incrementalPlanDescriptor(context.graph),
                        context.attemptId, context.generation, dataMembers);
        byte[] command = IncrementalControlMemberInstallCodec.encode(
                context.store.domainPath().toString(), controlFrame,
                intended.encode());
        byte[] commandDigest = sha256(command);
        io.rowweave.run.StageAssignmentRecord assignment =
                incrementalControlAssignment(
                        durable.assignmentPlan.manifest());
        StageAssignmentSnapshot snapshot = durable.assignmentSnapshot();
        StageAssignmentAcceptance acceptance = snapshot.acceptance(
                assignment.physicalOrdinal());
        if (acceptance == null || !java.security.MessageDigest.isEqual(
                commandDigest, acceptance.commandDigest())) {
            throw new IllegalStateException(
                    "incremental publication command was not accepted");
        }
        RegistryWorker endpoint = endpointOf(assignment.workerId());
        if (endpoint == null
                || !assignment.host().equals(endpoint.host())
                || assignment.rpcPort() != endpoint.port()
                || !assignment.workerIncarnationId().equals(
                endpoint.incarnationId())) {
            throw new IllegalStateException(
                    "incremental publication Worker generation changed");
        }
        StageOwner owner = new StageOwner(
                endpoint.workerId(), endpoint, durable.fence(),
                acceptance.token(), commandDigest, durable.recoveryToken,
                durable.frontierToken);
        RpcMessage request;
        MasterElection election = leadership;
        if (election == null) {
            request = RpcMessage.publishIncrementalManifest(
                    endpoint.workerId(), assignment.runtimeStageId(), groupId,
                    context.attemptId, context.generation, command,
                    endpoint.incarnationId(), durable.fence());
        } else {
            long term = election.termToken();
            if (term == 0L) {
                throw new IllegalStateException("not active master leader");
            }
            request = RpcMessage.publishIncrementalManifest(
                    endpoint.workerId(), assignment.runtimeStageId(), groupId,
                    context.attemptId, context.generation, command,
                    election.self(), term, endpoint.incarnationId(),
                    durable.fence());
        }
        callManifestControl(owner, assignment.runtimeStageId(), request,
                RpcMessage.Type.ACK, deadline, response -> null);
    }

    private static io.rowweave.run.StageAssignmentRecord
    incrementalControlAssignment(
            io.rowweave.run.StageAssignmentManifest manifest) {
        io.rowweave.run.StageAssignmentRecord selected = null;
        for (io.rowweave.run.StageAssignmentRecord candidate
                : manifest.assignments()) {
            if (candidate.role()
                    != io.rowweave.run.StageAssignmentRole.SYSTEM_CONTROL) {
                continue;
            }
            if (selected != null) {
                throw new IllegalStateException(
                        "incremental assignment has multiple control members");
            }
            selected = candidate;
        }
        if (selected == null) {
            throw new IllegalStateException(
                    "incremental assignment lacks its control member");
        }
        return selected;
    }

    private static io.rowweave.incremental.IncrementalPlanDescriptor
    incrementalPlanDescriptor(PlanGraph graph) {
        io.rowweave.incremental.IncrementalPlanDescriptor selected = null;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SOURCE
                    || !node.hasIncrementalPlanDescriptor()) continue;
            if (selected != null) {
                throw new IllegalStateException(
                        "incremental graph has multiple plan descriptors");
            }
            selected = node.incrementalPlanDescriptor();
        }
        if (selected == null) {
            throw new IllegalStateException(
                    "incremental graph has no plan descriptor");
        }
        return selected;
    }

    private static io.rowweave.BatchSinkDescriptor
    incrementalControlDescriptor(
            io.rowweave.commit.ManifestFileStore store) {
        return new io.rowweave.BatchSinkDescriptor(
                io.rowweave.BatchSinkDescriptor.CURRENT_VERSION,
                "rowweave.manifest-file", 1,
                lowercaseHex(sha256(io.rowweave.incremental
                        .IncrementalControlMemberCodec.SCHEMA_FINGERPRINT
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                store.domainId() + "#member:"
                        + io.rowweave.incremental
                        .IncrementalControlMemberCodec.MEMBER_NAME,
                store.domainId(),
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1);
    }

    private static String lowercaseHex(byte[] value) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xff;
            encoded[i * 2] = digits[unsigned >>> 4];
            encoded[i * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(encoded);
    }

    private PreparedSinkReceipt preparedSinkReceipt(
            DurableRunExecution durable, long stageId) {
        if (durable == null || durable.assignmentPlan == null
                || stageAssignmentAuthority == null) return null;
        StageAssignmentToken token = durable.assignmentPlan.token(stageId);
        if (token == null) return null;
        StageAssignmentSnapshot snapshot =
                durable.assignmentSnapshot();
        if (snapshot == null
                || token.physicalOrdinal() < 0
                || token.physicalOrdinal()
                >= snapshot.manifest().assignmentCount()) return null;
        PreparedSinkReceipt prepared = snapshot.preparedSink(
                token.physicalOrdinal());
        if (prepared != null && !prepared.token().equals(token)) {
            throw new IllegalStateException(
                    "prepared Sink authority identity changed");
        }
        return prepared;
    }

    private boolean allProviderPrepared(
            DurableRunExecution durable, java.util.List<Long> sinkStageIds) {
        if (durable == null || sinkStageIds.isEmpty()) return false;
        for (long stageId : sinkStageIds) {
            if (preparedSinkReceipt(durable, stageId) == null) return false;
        }
        return true;
    }

    private static io.rowweave.QualityGatePolicy qualityGatePolicy(
            PlanGraph graph) {
        io.rowweave.QualityGatePolicy policy = null;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK
                    || !(node.output() instanceof io.rowweave.ManifestFileSink)) {
                continue;
            }
            io.rowweave.ManifestFileSink output =
                    (io.rowweave.ManifestFileSink) node.output();
            if (!output.hasQualityGateBinding()) continue;
            io.rowweave.QualityGatePolicy candidate = output.qualityGatePolicy();
            if (policy != null && !policy.equals(candidate)) {
                throw new IllegalStateException(
                        "quality Sink policies differ after admission");
            }
            policy = candidate;
        }
        if (policy == null) {
            throw new IllegalStateException(
                    "quality assignment has no policy in Artifact graph");
        }
        return policy;
    }

    private static final class QualityGateRejectedException
            extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        QualityGateRejectedException() {
            super("QUALITY_GATE_REJECTED");
        }
    }

    /**
     * Resolves a normal-path commit exception from shared authority. {@code true} means the
     * irreversible intent was completed to visibility; {@code false} means non-publication was
     * proven, rollback completed, and the Run was made terminal FAILED.
     */
    private boolean resolveNormalDurableManifestFailure(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context,
            io.rowweave.commit.CommitManifest intended, long deadline,
            DurableRunExecution durable) {
        boolean qualityBounded = durable.assignmentPlan.manifest()
                .hasQualityGateBinding();
        io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                qualityBounded ? stageAssignmentAuthority
                        .qualityGateCommitJournal(durable.fence()) : null;
        ManifestCommitJournalSnapshot journal = qualityBounded ? null
                : durable.manifestCommitJournal();
        if (qualityBounded ? qualityJournal == null : journal == null) {
            throw new IllegalStateException("manifest commit journal unavailable during resolution");
        }
        ManifestCommitIntent intent = qualityBounded
                ? qualityJournal.intent() : journal.intent();
        if (intent != null) {
            if (qualityBounded
                    ? !qualityJournal.allMembersSealed()
                    || !intended.equals(qualityJournal.intendedManifest())
                    : !journal.allExpectedMembersSealed()
                    || !intended.equals(journal.intendedManifest())) {
                throw new IllegalStateException(
                        "durable manifest intent conflicts during resolution");
            }
            if (durable.incremental()) {
                publishIncrementalManifest(
                        groupId, context,
                        incrementalDataMembers(intended), intended,
                        deadline, durable);
            } else if (allProviderPrepared(durable, sinkStageIds)) {
                ManifestDurableSinkAuthorityAdapter adapter =
                        manifestAuthorityProjection(intent, intended);
                new DurableSinkCommitCoordinator(adapter,
                        (action, decisionDigest) ->
                                executeProviderGroupControl(
                                        groupId, sinkStageIds, context,
                                        durable, action, decisionDigest,
                                        deadline))
                        .publishAndFinalize();
            } else {
                // Compatibility only: pre-R4 journals have no provider locators.
                publishIntendedManifest(context, intended);
            }
            return true;
        }

        io.rowweave.commit.CommitManifest observed =
                context.store.exact(context.generation);
        if (observed != null) {
            throw new IllegalStateException(
                    "manifest is visible without durable commit intent");
        }
        Throwable rollbackFailure =
                abortDurableManifestOutputs(
                        groupId, sinkStageIds, context, durable, deadline,
                        "MANIFEST_PREPUBLICATION_FAILED");
        if (rollbackFailure != null) {
            throw new IllegalStateException(
                    "pre-publication rollback could not be proven", rollbackFailure);
        }
        durable.failPrepublicationCommit();
        return false;
    }

    private static java.util.List<io.rowweave.commit.CommitManifest.Member>
    incrementalDataMembers(
            io.rowweave.commit.CommitManifest manifest) {
        java.util.List<io.rowweave.commit.CommitManifest.Member> members =
                new java.util.ArrayList<>();
        for (io.rowweave.commit.CommitManifest.Member member
                : manifest.members()) {
            if (!io.rowweave.incremental.IncrementalControlMemberCodec
                    .MEMBER_NAME.equals(member.name())) {
                members.add(member);
            }
        }
        if (members.isEmpty()) {
            throw new IllegalStateException(
                    "incremental Manifest has no user data members");
        }
        return members;
    }

    /**
     * Establishes the irreversible abort decision before asking Workers to delete staged data.
     * A durable commit intent and an abort decision are mutually exclusive at the authority CAS.
     */
    private Throwable abortDurableManifestOutputs(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context, DurableRunExecution durable,
            long deadline, String reasonCode) {
        if (durable == null || context == null) {
            return rollbackPreparedOutputs(groupId, sinkStageIds, null, deadline);
        }
        if (context.genericProvider()) {
            try {
                java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                        new java.util.ArrayList<>();
                boolean closure = !sinkStageIds.isEmpty();
                for (long stageId : sinkStageIds) {
                    PreparedSinkReceipt prepared = preparedSinkReceipt(
                            durable, stageId);
                    if (prepared == null) {
                        closure = false;
                        break;
                    }
                    locators.add(prepared.locator());
                }
                DurableSinkAuthorityAdapter adapter =
                        context.providerReservation.bind(
                                closure ? locators
                                        : java.util.Collections.emptyList());
                if (!closure) {
                    adapter.requireAbortDecision(reasonCode);
                    return null;
                }
                DurableSinkCommitCoordinator coordinator =
                        new DurableSinkCommitCoordinator(adapter,
                                (action, decisionDigest) ->
                                        executeProviderGroupControl(
                                                groupId, sinkStageIds, context,
                                                durable, action, decisionDigest,
                                                deadline));
                context.providerOutcome = coordinator.abort(reasonCode);
                return null;
            } catch (Throwable failure) {
                return failure;
            }
        }
        boolean qualityBounded = durable.assignmentPlan.manifest()
                .hasQualityGateBinding();
        io.rowweave.run.QualityGateCommitJournalSnapshot qualityJournal =
                qualityBounded ? stageAssignmentAuthority
                        .qualityGateCommitJournal(durable.fence()) : null;
        ManifestCommitJournalSnapshot journal = qualityBounded ? null
                : durable.manifestCommitJournal();
        if (qualityBounded ? qualityJournal == null : journal == null) {
            throw new IllegalStateException(
                    "manifest commit journal unavailable before abort decision");
        }
        if (qualityBounded
                ? qualityJournal.intent() != null : journal.intent() != null) {
            throw new IllegalStateException(
                    "durable manifest commit is already decided; rollback is forbidden");
        }
        io.rowweave.run.BatchSinkAbortRecord existing =
                durable.batchSinkAbort();
        byte[] abortIdentity = durable.assignmentPlan.sinkGroupIdentity();
        if ("QUALITY_GATE_REJECTED".equals(reasonCode)
                && qualityBounded
                && qualityJournal.decision() != null
                && qualityJournal.decision().outcome()
                == io.rowweave.QualityGateDecision.Outcome.FAIL) {
            abortIdentity = io.rowweave.QualityGateDecisionCodec.digest(
                    qualityJournal.decision());
        }
        io.rowweave.run.BatchSinkAbortRecord decided = existing == null
                ? io.rowweave.run.BatchSinkAbortRecord.decided(
                        durable.fence(), durable.assignmentPlan.manifest().generation(),
                        io.rowweave.BatchSinkDescriptor.CapabilityProfile
                                .MANIFEST_ATOMIC_GROUP_V1,
                        context.store.domainId(), context.generation, context.attemptId,
                        abortIdentity, reasonCode,
                        durable.eventTimeMillis)
                : existing;
        io.rowweave.run.BatchSinkAbortDecision decision =
                durable.recordBatchSinkAbort(decided);
        if (decision.code() != io.rowweave.run.BatchSinkAbortDecision.Code.RECORDED
                && decision.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code.ALREADY_RECORDED) {
            throw new IllegalStateException(
                    "manifest abort decision rejected: " + decision.code());
        }
        decided = decision.record();
        if (decided.state() == io.rowweave.run.BatchSinkAbortRecord.State.ABORTED) {
            return null;
        }
        if (allProviderPrepared(durable, sinkStageIds)) {
            final io.rowweave.run.BatchSinkAbortRecord abortDecision = decided;
            DurableSinkAuthorityAdapter adapter =
                    new DurableSinkAuthorityAdapter() {
                        @Override
                        public byte[] requireCommitDecision() {
                            throw new IllegalStateException(
                                    "manifest abort is already decided");
                        }

                        @Override
                        public io.rowweave.commit.BatchSinkOutcome
                        authorityOutcome() {
                            return io.rowweave.commit.BatchSinkOutcome.of(
                                    io.rowweave.commit.BatchSinkOutcome.State
                                            .ABORT_DECIDED,
                                    abortDecision.generation(),
                                    abortDecision.identityDigest(),
                                    abortDecision.reasonCode());
                        }

                        @Override
                        public byte[] requireAbortDecision(String requestedReason) {
                            if (!abortDecision.reasonCode().equals(requestedReason)) {
                                throw new IllegalStateException(
                                        "manifest abort reason conflicts with authority");
                            }
                            return abortDecision.identityDigest();
                        }

                        @Override
                        public void completeAbort() {
                            io.rowweave.run.BatchSinkAbortDecision completion =
                                    durable.recordBatchSinkAbort(
                                            abortDecision.completed(
                                                    abortDecision
                                                            .decidedAtMillis()));
                            if (completion.code()
                                    != io.rowweave.run.BatchSinkAbortDecision.Code
                                    .RECORDED
                                    && completion.code()
                                    != io.rowweave.run.BatchSinkAbortDecision.Code
                                    .ALREADY_RECORDED) {
                                throw new IllegalStateException(
                                        "manifest abort completion rejected: "
                                                + completion.code());
                            }
                        }
                    };
            try {
                new DurableSinkCommitCoordinator(adapter,
                        (action, decisionDigest) ->
                                executeProviderGroupControl(
                                        groupId, sinkStageIds, context,
                                        durable, action, decisionDigest,
                                        deadline))
                        .abort(reasonCode);
                return null;
            } catch (Throwable failure) {
                return failure;
            }
        }
        Throwable rollbackFailure =
                rollbackPreparedOutputs(groupId, sinkStageIds, null, deadline);
        if (rollbackFailure != null) {
            return rollbackFailure;
        }
        io.rowweave.run.BatchSinkAbortDecision completion =
                durable.recordBatchSinkAbort(decided.completed(decided.decidedAtMillis()));
        if (completion.code() != io.rowweave.run.BatchSinkAbortDecision.Code.RECORDED
                && completion.code()
                != io.rowweave.run.BatchSinkAbortDecision.Code.ALREADY_RECORDED) {
            return new IllegalStateException(
                    "manifest abort completion rejected: " + completion.code());
        }
        return null;
    }

    private Throwable rollbackOutputsBeforePublication(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context, DurableRunExecution durable,
            long deadline, String reasonCode) {
        try {
            return abortDurableManifestOutputs(
                    groupId, sinkStageIds, context, durable, deadline, reasonCode);
        } catch (Throwable failure) {
            return failure;
        }
    }

    private static void publishIntendedManifest(
            ManifestJobCommit context,
            io.rowweave.commit.CommitManifest intended) {
        io.rowweave.commit.CommitManifest observed =
                context.store.exact(context.generation);
        if (observed == null) {
            context.store.publish(intended);
            observed = context.store.exact(context.generation);
        }
        if (!intended.equals(observed)) {
            throw new IllegalStateException(
                    "published manifest conflicts with durable intent");
        }
    }

    private static io.rowweave.commit.BatchSinkOutcome
            projectManifestProviderAuthorityOutcome(
            ManifestJobCommit context,
            io.rowweave.commit.CommitManifest intended) {
        io.rowweave.commit.CommitManifest visible =
                context.store.exact(context.generation);
        if (visible == null) {
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.COMMIT_DECIDED,
                    context.generation, sha256(intended.encode()), null);
        }
        if (!intended.equals(visible)) {
            return io.rowweave.commit.BatchSinkOutcome.of(
                    io.rowweave.commit.BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                    context.generation, null, "PROVIDER_OUTCOME_CONFLICT");
        }
        return io.rowweave.commit.BatchSinkOutcome.of(
                io.rowweave.commit.BatchSinkOutcome.State.COMMITTED,
                context.generation, sha256(intended.encode()), null);
    }

    private static ManifestDurableSinkAuthorityAdapter
            manifestAuthorityProjection(
            ManifestCommitIntent intent,
            io.rowweave.commit.CommitManifest intended) {
        return new ManifestDurableSinkAuthorityAdapter(
                intent, intended,
                () -> io.rowweave.commit.BatchSinkOutcome.of(
                        io.rowweave.commit.BatchSinkOutcome.State.COMMIT_DECIDED,
                        intent.generation(), intent.manifestHash(),
                        "MANIFEST_INTENT_DURABLE"));
    }

    private io.rowweave.commit.BatchSinkOutcome executeProviderGroupControl(
            String groupId, java.util.List<Long> sinkStageIds,
            ManifestJobCommit context, DurableRunExecution durable,
            ProviderPreparedGroupCommandCodec.Action action,
            byte[] decisionDigest, long deadline) {
        java.util.List<io.rowweave.ProviderBatchSink> outputs =
                new java.util.ArrayList<>();
        for (int i = 0; i < context.graph.nodeCount(); i++) {
            PlanNode node = context.graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            if (node.output() instanceof io.rowweave.ManifestFileSink) {
                outputs.add(((io.rowweave.ManifestFileSink) node.output())
                        .toProviderSink());
            } else if (node.output() instanceof io.rowweave.ProviderBatchSink) {
                outputs.add((io.rowweave.ProviderBatchSink) node.output());
            }
        }
        java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                new java.util.ArrayList<>(sinkStageIds.size());
        for (long stageId : sinkStageIds) {
            PreparedSinkReceipt prepared = preparedSinkReceipt(durable, stageId);
            if (prepared == null) {
                throw new IllegalStateException(
                        "provider locator closure is incomplete: stageId=" + stageId);
            }
            locators.add(prepared.locator());
        }
        if (outputs.size() != locators.size()) {
            throw new IllegalStateException(
                    "provider output/locator closure count mismatch");
        }
        byte[] encoded = ProviderPreparedGroupCommandCodec.encode(
                new ProviderPreparedGroupCommandCodec.Command(
                        action, outputs, locators, decisionDigest));
        long stageId = sinkStageIds.get(0);
        StageOwner owner = requiredCurrentOutputOwner(stageId);
        MasterElection election = leadership;
        RpcMessage request;
        if (election == null) {
            request = RpcMessage.executeProviderGroup(
                    owner.workerId, stageId, groupId, context.attemptId,
                    context.generation, encoded,
                    owner.endpoint.incarnationId(), owner.runAttemptFence);
        } else {
            long term = election.termToken();
            if (term == 0L) throw new IllegalStateException("not active master leader");
            request = RpcMessage.executeProviderGroup(
                    owner.workerId, stageId, groupId, context.attemptId,
                    context.generation, encoded, election.self(), term,
                    owner.endpoint.incarnationId(), owner.runAttemptFence);
        }
        return callManifestControl(owner, stageId, request,
                RpcMessage.Type.PROVIDER_GROUP_RESULT, deadline,
                RpcMessage::providerGroupOutcome);
    }

    /**
     * route one idempotent read-only source action to an exact
     * live Worker generation. A retry re-resolves the registry so replacement
     * Workers can take over without accepting a stale incarnation.
     */
    IncrementalSourceActionResult executeIncrementalSourceActionControl(
            IncrementalSourceActionCommand command, long deadlineNanos) {
        if (command == null) {
            throw new IllegalArgumentException(
                    "incremental source action command is required");
        }
        byte[] encoded = IncrementalSourceActionCommandCodec.encode(command);
        try (io.rowweave.internal.IncrementalSourceControlLedger.Lease ignored =
                     incrementalSourceControlLedger.acquireLarge(
                             command.contract().actionBudget()
                                     .maxLargeResidentBytes(), deadlineNanos)) {
            return executeAdmittedIncrementalSourceActionControl(
                    command, deadlineNanos, encoded);
        }
    }

    private IncrementalSourceActionResult
    executeAdmittedIncrementalSourceActionControl(
            IncrementalSourceActionCommand command, long deadlineNanos,
            byte[] encoded) {
        Throwable firstFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            if (!isActiveLeader()) {
                throw new IllegalStateException(
                        "incremental source action requires active Master leadership",
                        firstFailure);
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new IllegalStateException(
                        "incremental source action deadline exceeded",
                        firstFailure);
            }
            RegistryWorker worker = schedulableWorkers().stream()
                    .findFirst().orElse(null);
            if (worker == null) {
                throw new IllegalStateException(
                        "incremental source action has no registered Worker",
                        firstFailure);
            }
            MasterElection election = leadership;
            Endpoint leader;
            long term;
            if (election == null) {
                leader = new Endpoint(
                        controlSecurity.advertisedHost(), port());
                term = 1L;
            } else {
                term = election.termToken();
                if (term == 0L) {
                    throw new IllegalStateException(
                            "incremental source action has no active leader term");
                }
                leader = election.self();
            }
            RpcMessage request = RpcMessage.executeIncrementalSourceAction(
                    worker.workerId(), encoded, leader, term,
                    worker.incarnationId());
            int readTimeoutMillis = (int) Math.min(Integer.MAX_VALUE,
                    Math.max(1L, java.util.concurrent.TimeUnit.NANOSECONDS
                            .toMillis(remainingNanos)));
            io.rowweave.cluster.rpc.RpcTransport transport =
                    io.rowweave.cluster.rpc.RpcTransports.connect(
                            worker.host(), worker.port(),
                            Math.min(5_000, readTimeoutMillis),
                            readTimeoutMillis, controlSecurity, protocolHello);
            try {
                IncrementalSourceActionResult result = transport.callScoped(
                        secureOutbound(request), response -> {
                            response = secureInbound(response);
                            if (response == null
                                    || response.type()
                                    != RpcMessage.Type
                                    .INCREMENTAL_SOURCE_ACTION_RESULT) {
                                throw new IllegalStateException(
                                        "Worker rejected incremental source action: "
                                                + (response == null ? "null response"
                                                : response.errorMessage()));
                            }
                            return IncrementalSourceActionResultCodec.decode(
                                    response.incrementalSourceActionResultBytes(),
                                    response::reserveResidentBytes);
                        });
                requireSourceActionResultBinding(command, result);
                return result;
            } catch (RuntimeException | Error failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    failure.addSuppressed(firstFailure);
                    throw failure;
                }
            } finally {
                transport.close();
            }
        }
        throwUnchecked(firstFailure);
        throw new AssertionError("unreachable");
    }

    /**
     * Capture-to-claim half of the cluster path. It is deliberately separate
     * from the RPC handler until physical dispatch also carries both authority
     * tokens through every Worker receipt.
     */
    io.rowweave.incremental.IncrementalIntervalPreparationDecision
    prepareIncrementalSubmissionControl(
            io.rowweave.incremental.IncrementalRunSubmission submission,
            long actionDeadlineNanos,
            java.util.function.LongConsumer reservation) {
        return prepareIncrementalSubmissionControl(
                submission, actionDeadlineNanos, reservation,
                ignored -> { });
    }

    private io.rowweave.incremental.IncrementalIntervalPreparationDecision
    prepareIncrementalSubmissionControl(
            io.rowweave.incremental.IncrementalRunSubmission submission,
            long actionDeadlineNanos,
            java.util.function.LongConsumer reservation,
            java.util.function.Consumer<String> pendingRunReservation) {
        if (submission == null || reservation == null
                || pendingRunReservation == null) {
            throw new IllegalArgumentException(
                    "incremental submission and reservation are required");
        }
        if (!isActiveLeader()) {
            throw new IllegalStateException(
                    "incremental preparation requires active Master leadership");
        }
        if (incrementalAuthorityGateway == null
                || artifactLifecycleRepository == null) {
            throw new IllegalStateException(
                    "incremental authority or Artifact lifecycle unavailable");
        }
        deploymentRuntime.requireAdmissionOpen();
        PlanArtifact artifact = submission.templateArtifact();
        requireArtifactDeployment(artifact);
        PlanArtifactValidator.validateForCurrentRuntime(artifact);
        PlanGraph template = LegacyPlanArtifactAdapter
                .unwrapIncrementalValidated(
                        artifact, objectTrustContext, reservation);
        io.rowweave.incremental.IncrementalDefinition derived =
                io.rowweave.incremental.IncrementalPlanTemplateFingerprint
                        .derive(submission.definition().definitionId(),
                                template, objectTrustContext, reservation);
        if (!derived.equals(submission.definition())) {
            throw new IllegalArgumentException(
                    "incremental definition differs from template");
        }
        io.rowweave.incremental.IncrementalSourceProviderContract admitted =
                deploymentRuntime.incrementalSourceProviderRegistry()
                        .requireContract(
                                submission.sourceContract().providerKey());
        if (!admitted.equals(submission.sourceContract())) {
            throw new IllegalArgumentException(
                    "incremental source contract differs from signed deployment");
        }
        OutputDestinationValidator.validate(template);
        ClusterOutputValidator.validate(template);
        io.rowweave.plan.OutputCommitValidator.validateDurable(template);
        ClusterPlanCapabilityValidator.validate(template);

        io.rowweave.incremental.IncrementalAuthorityRecord record =
                incrementalAuthorityGateway.record(submission.feedId());
        if (record == null) {
            throw new IllegalArgumentException(
                    "incremental feed is not initialized");
        }
        io.rowweave.run.RecoveryOwner owner = incrementalRecoveryOwner();
        io.rowweave.incremental.FrontierRecoveryToken observed =
                incrementalAuthorityGateway.frontierOwnership(
                        submission.feedId());
        long expectedEpoch = observed == null
                ? 0L : observed.ownershipEpoch();
        io.rowweave.incremental.FrontierOwnershipClaim ownership =
                incrementalAuthorityGateway.claimOwnership(
                        submission.feedId(), record.revision(),
                        expectedEpoch, owner);
        if (ownership.code()
                != io.rowweave.incremental.FrontierOwnershipClaim.Code.ACQUIRED
                && ownership.code()
                != io.rowweave.incremental.FrontierOwnershipClaim.Code.REPLAYED) {
            throw new IllegalStateException(
                    "incremental frontier ownership rejected: "
                            + ownership.code());
        }
        io.rowweave.incremental.IncrementalSourceActions sourceActions =
                new io.rowweave.incremental.IncrementalSourceActions() {
                    @Override
                    public io.rowweave.incremental.CapturedTargetReceipt capture(
                            io.rowweave.incremental.IncrementalCaptureRequest request,
                            long deadlineNanos) {
                        return executeIncrementalSourceActionControl(
                                IncrementalSourceActionCommand.capture(
                                        submission.sourceContract(),
                                        submission.sourceConfiguration(), request),
                                deadlineNanos).capturedTarget();
                    }

                    @Override
                    public io.rowweave.incremental.IncrementalMaterializationResponse
                    materialize(
                            io.rowweave.incremental.CapturedTargetReceipt captured,
                            long deadlineNanos,
                            java.util.function.LongConsumer ignoredReservation) {
                        return executeIncrementalSourceActionControl(
                                IncrementalSourceActionCommand.materialize(
                                        submission.sourceContract(),
                                        submission.sourceConfiguration(), captured),
                                deadlineNanos).materialization();
                    }
                };
        io.rowweave.incremental.IncrementalIntervalPreparer preparer =
                io.rowweave.incremental.IncrementalIntervalPreparer.forCluster(
                        submission.definition(), template,
                        submission.sourceContract(),
                        submission.sourceConfiguration(), sourceActions,
                        artifactLifecycleRepository,
                        incrementalAuthorityGateway, objectTrustContext,
                        io.rowweave.artifact.PlanArtifactDeploymentBinding
                                .deploymentSetId(artifact),
                        pendingRunReservation);
        if (submission.mode()
                == io.rowweave.incremental.IncrementalRunMode.BACKFILL_V1) {
            return preparer.prepareBackfill(
                    submission.feedId(), ownership.token(),
                    submission.expectedBackfillOrdinal(),
                    submission.backfillFrom(), submission.backfillTarget(),
                    submission.requestIdentity(), submission.attemptId(),
                    submission.publicationId(), submission.maxAttempts(),
                    submission.deadlineEpochMillis(),
                    submission.submittedAtMillis(), actionDeadlineNanos,
                    reservation);
        }
        return preparer.prepareNormal(
                submission.feedId(), ownership.token(),
                submission.requestIdentity(), submission.mode(),
                submission.attemptId(), submission.publicationId(),
                submission.maxAttempts(), submission.deadlineEpochMillis(),
                submission.submittedAtMillis(), actionDeadlineNanos,
                reservation);
    }

    static final class IncrementalExecutionClaim {
        private final String artifactId;
        private final PlanGraph graph;
        private final io.rowweave.incremental.IncrementalClaimRequest request;
        private final io.rowweave.run.RunSnapshot runSnapshot;
        private final io.rowweave.incremental.FrontierRecoveryToken frontierToken;
        private final io.rowweave.run.RecoveryToken runToken;

        private IncrementalExecutionClaim(
                String artifactId, PlanGraph graph,
                io.rowweave.incremental.IncrementalClaimRequest request,
                io.rowweave.run.RunSnapshot runSnapshot,
                io.rowweave.incremental.FrontierRecoveryToken frontierToken,
                io.rowweave.run.RecoveryToken runToken) {
            this.artifactId = artifactId;
            this.graph = graph;
            this.request = request;
            this.runSnapshot = runSnapshot;
            this.frontierToken = frontierToken;
            this.runToken = runToken;
        }

        String artifactId() { return artifactId; }
        PlanGraph graph() { return graph; }
        io.rowweave.incremental.IncrementalClaimRequest request() {
            return request;
        }
        io.rowweave.run.RunSnapshot runSnapshot() { return runSnapshot; }
        io.rowweave.incremental.FrontierRecoveryToken frontierToken() {
            return frontierToken;
        }
        io.rowweave.run.RecoveryToken runToken() { return runToken; }
    }

    /**
     * Converts a frozen interval claim into an executable graph only while the
     * same Master jointly owns the frontier and Run authorities.
     */
    IncrementalExecutionClaim claimIncrementalExecutionControl(
            io.rowweave.incremental.IncrementalRunSubmission submission,
            io.rowweave.incremental.IncrementalIntervalPreparationDecision prepared,
            java.util.function.LongConsumer reservation) {
        if (submission == null || prepared == null || reservation == null) {
            throw new IllegalArgumentException(
                    "incremental execution claim fields are required");
        }
        if (!isActiveLeader()) {
            throw new IllegalStateException(
                    "incremental execution claim requires active Master leadership");
        }
        if (incrementalAuthorityGateway == null || artifactRepository == null) {
            throw new IllegalStateException(
                    "incremental authority or Artifact repository unavailable");
        }
        if (prepared.code()
                != io.rowweave.incremental.IncrementalIntervalPreparationDecision
                .Code.CLAIMED
                || prepared.claimRequest() == null
                || prepared.runSnapshot() == null) {
            throw new IllegalArgumentException(
                    "incremental interval is not executable");
        }
        io.rowweave.incremental.IncrementalClaimRequest request =
                prepared.claimRequest();
        if (!submission.feedId().equals(request.feedId())
                || !prepared.artifactId().equals(request.artifactId())) {
            throw new IllegalArgumentException(
                    "incremental execution claim differs from submission");
        }
        io.rowweave.incremental.IncrementalAuthorityRecord record =
                incrementalAuthorityGateway.record(submission.feedId());
        io.rowweave.incremental.IncrementalActiveInterval active =
                record == null ? null : record.activeInterval();
        if (record == null || record.state()
                != io.rowweave.incremental.IncrementalFrontierAuthorityState.ACTIVE
                || active == null
                || !request.intervalId().equals(active.identity().intervalId())
                || !request.capturedTarget().equals(active.capturedTarget())
                || !prepared.artifactId().equals(active.artifactId())
                || !request.firstAttemptFence().equals(active.fence())) {
            throw new IllegalStateException(
                    "incremental interval authority differs before execution");
        }
        io.rowweave.run.RunSnapshot run =
                incrementalAuthorityGateway.runSnapshot(request.runId());
        if (!sameIncrementalRunAuthority(run, prepared.runSnapshot(), active)) {
            throw new IllegalStateException(
                    "incremental Run authority differs before execution");
        }
        io.rowweave.run.RecoveryOwner owner = incrementalRecoveryOwner();
        io.rowweave.incremental.FrontierRecoveryToken frontierToken =
                incrementalAuthorityGateway.frontierOwnership(
                        submission.feedId());
        if (frontierToken == null || !owner.equals(frontierToken.owner())) {
            throw new IllegalStateException(
                    "incremental frontier ownership was replaced");
        }
        io.rowweave.run.RecoveryToken previous =
                incrementalAuthorityGateway.runOwnership(request.runId());
        long expectedEpoch = previous == null ? 0L : previous.ownershipEpoch();
        io.rowweave.run.RecoveryToken runToken;
        try {
            io.rowweave.run.RecoveryClaim claim =
                    incrementalAuthorityGateway.claimRunOwnership(
                            frontierToken, request.runId(), run.revision(),
                            expectedEpoch, owner);
            if (claim.code() != io.rowweave.run.RecoveryClaim.Code.ACQUIRED
                    && claim.code()
                    != io.rowweave.run.RecoveryClaim.Code.REPLAYED) {
                throw new IllegalStateException(
                        "incremental Run ownership rejected: " + claim.code());
            }
            runToken = claim.token();
        } catch (RuntimeException unknown) {
            io.rowweave.run.RecoveryToken confirmed =
                    incrementalAuthorityGateway.runOwnership(request.runId());
            if (!sameRunOwner(confirmed, request.runId(), owner)
                    || !incrementalAuthorityGateway.currentlyOwns(
                    frontierToken, confirmed)) {
                throw unknown;
            }
            runToken = confirmed;
        }
        if (!sameRunOwner(runToken, request.runId(), owner)
                || !incrementalAuthorityGateway.currentlyOwns(
                frontierToken, runToken)) {
            throw new IllegalStateException(
                    "incremental dual authority ownership was lost");
        }

        byte[] encodedArtifact = artifactRepository.get(
                prepared.artifactId(), reservation);
        if (encodedArtifact == null) {
            throw new IllegalStateException(
                    "incremental Artifact is unavailable");
        }
        PlanArtifact artifact = PlanArtifactCodec.decode(
                encodedArtifact, reservation);
        if (!prepared.artifactId().equals(artifact.artifactId())
                || !io.rowweave.incremental.IncrementalPlanArtifactMaterializer
                .definitionIdentity(submission.definition().fingerprint())
                .equals(artifact.definitionId())) {
            throw new IllegalArgumentException(
                    "incremental Artifact identity differs");
        }
        requireArtifactDeployment(artifact);
        PlanArtifactValidator.validateForCurrentRuntime(artifact);
        PlanGraph graph = LegacyPlanArtifactAdapter.unwrapIncrementalValidated(
                artifact, objectTrustContext, reservation)
                .restoreBuiltinManifestOutputs();
        return new IncrementalExecutionClaim(
                prepared.artifactId(), graph, request, run,
                frontierToken, runToken);
    }

    StageAssignmentSnapshot publishIncrementalAssignmentsControl(
            IncrementalExecutionClaim claim) {
        if (claim == null) {
            throw new IllegalArgumentException(
                    "incremental execution claim is required");
        }
        if (!isActiveLeader()
                || !incrementalAuthorityGateway.currentlyOwns(
                claim.frontierToken(), claim.runToken())) {
            throw new IllegalStateException(
                    "incremental dual authority ownership was lost");
        }
        java.util.List<RegistryWorker> live = schedulableWorkers(
                requiredWorkerCapabilities(
                        claim.graph(), java.util.Collections.emptyList()));
        if (live.isEmpty()) {
            throw new IllegalStateException(
                    "incremental execution has no schedulable Worker");
        }
        ExecutionGraph executionGraph =
                ExecutionGraphBuilder.buildWithSinkBoundary(claim.graph());
        java.util.List<String> workerIds = new java.util.ArrayList<>();
        for (RegistryWorker worker : live) {
            workerIds.add(worker.workerId());
        }
        PhysicalPlan physicalPlan = StageAssignment.assign(
                executionGraph, claim.graph().parallelism(), workerIds);
        DurableStageAssignmentPlan planned =
                DurableStageAssignmentPlan.createIncremental(
                        claim.request().firstAttemptFence(), claim.artifactId(),
                        claim.graph(), executionGraph, physicalPlan, live,
                        stageSeq::incrementAndGet);
        StageManifestPublication publication =
                incrementalAuthorityGateway.publishAssignments(
                        claim.frontierToken(), claim.runToken(),
                        planned.manifest());
        if (publication.code() != StageManifestPublication.Code.PUBLISHED
                && publication.code()
                != StageManifestPublication.Code.ALREADY_PUBLISHED) {
            throw new IllegalStateException(
                    "incremental assignment manifest rejected: "
                            + publication.code());
        }
        return publication.snapshot();
    }

    private static boolean sameIncrementalRunAuthority(
            io.rowweave.run.RunSnapshot current,
            io.rowweave.run.RunSnapshot prepared,
            io.rowweave.incremental.IncrementalActiveInterval active) {
        return current != null && prepared != null
                && current.currentAttempt() != null
                && current.runId().equals(prepared.runId())
                && current.definitionId().equals(prepared.definitionId())
                && current.artifactId().equals(prepared.artifactId())
                && current.revision() == prepared.revision()
                && current.currentAttempt().attemptNumber()
                == active.attemptOrdinal()
                && current.currentAttempt().attemptId().equals(
                active.fence().attemptId())
                && current.currentAttempt().fenceToken()
                == active.fence().fenceToken();
    }

    private static boolean sameRunOwner(
            io.rowweave.run.RecoveryToken token, String runId,
            io.rowweave.run.RecoveryOwner owner) {
        return token != null && runId.equals(token.runId())
                && owner.equals(token.owner());
    }

    private io.rowweave.run.RecoveryOwner incrementalRecoveryOwner() {
        MasterElection election = leadership;
        if (election == null) {
            return new io.rowweave.run.RecoveryOwner(
                    new Endpoint(controlSecurity.advertisedHost(), port()).toString(),
                    1L);
        }
        long term = election.termToken();
        if (!election.isLeader() || term == 0L) {
            throw new IllegalStateException(
                    "incremental authority has no active leader term");
        }
        return new io.rowweave.run.RecoveryOwner(
                election.self().toString(), term);
    }

    private static void requireSourceActionResultBinding(
            IncrementalSourceActionCommand command,
            IncrementalSourceActionResult result) {
        if (command.action() == IncrementalSourceActionCommand.Action.CAPTURE) {
            if (result.kind() != IncrementalSourceActionResult.Kind.CAPTURED
                    || result.capturedTarget() == null
                    || !java.security.MessageDigest.isEqual(
                    command.configuration().contractDigest(),
                    result.capturedTarget().sourceContractDigest())) {
                throw new IllegalArgumentException(
                        "incremental capture result binding differs");
            }
            return;
        }
        if (result.kind() != IncrementalSourceActionResult.Kind.MATERIALIZED
                || result.materialization() == null
                || !command.contract().providerKey().equals(
                result.materialization().providerKey())
                || !command.capturedTarget().equals(
                result.materialization().capturedTarget())) {
            throw new IllegalArgumentException(
                    "incremental materialization result binding differs");
        }
    }

    private io.rowweave.commit.BatchSinkOutcome
    executeRecoveredProviderGroupControl(
            PlanGraph graph, StageAssignmentSnapshot evidence,
            String publicationId, long publicationGeneration,
            ProviderPreparedGroupCommandCodec.Action action,
            byte[] decisionDigest, long deadline) {
        java.util.List<io.rowweave.ProviderBatchSink> outputs =
                providerOutputs(graph);
        java.util.List<io.rowweave.run.PreparedSinkLocator> locators =
                providerLocators(evidence);
        if (outputs.size() != locators.size()) {
            throw new IllegalStateException(
                    "provider output/locator closure count mismatch");
        }
        int ownerOrdinal = -1;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            io.rowweave.run.StageAssignmentRole role =
                    evidence.manifest().assignment(i).role();
            if (role == io.rowweave.run.StageAssignmentRole.SINK
                    || role == io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH) {
                ownerOrdinal = i;
                break;
            }
        }
        if (ownerOrdinal < 0 || evidence.acceptance(ownerOrdinal) == null) {
            throw new IllegalStateException(
                    "provider recovery control owner is unavailable");
        }
        io.rowweave.run.StageAssignmentRecord record =
                evidence.manifest().assignment(ownerOrdinal);
        RegistryWorker current = endpointOf(record.workerId());
        boolean exact = current != null
                && record.host().equals(current.host())
                && record.rpcPort() == current.port()
                && record.workerIncarnationId().equals(
                        current.incarnationId());
        if (!exact) {
            current = null;
            for (RegistryWorker candidate : registry.liveWorkers()) {
                if (candidate.slots() > 0
                        && candidate.port() > 0
                        && candidate.incarnationId() != null
                        && !candidate.incarnationId().isEmpty()) {
                    current = candidate;
                    break;
                }
            }
        }
        if (current == null) {
            throw reconciliationUnavailable(evidence,
                    "provider recovery has no admitted Worker for rebuild",
                    null);
        }
        StageAssignmentAcceptance acceptance =
                evidence.acceptance(ownerOrdinal);
        StageOwner owner = new StageOwner(
                current.workerId(), current, evidence.manifest().fence(),
                acceptance.token(), acceptance.commandDigest(), null);
        byte[] encoded = ProviderPreparedGroupCommandCodec.encode(
                new ProviderPreparedGroupCommandCodec.Command(
                        action, outputs, locators, decisionDigest));
        String groupId = "provider-recovery:" + publicationId;
        MasterElection election = leadership;
        RpcMessage request;
        if (election == null) {
            request = RpcMessage.executeProviderGroup(
                    owner.workerId, record.runtimeStageId(), groupId,
                    publicationId, publicationGeneration, encoded,
                    owner.endpoint.incarnationId(), owner.runAttemptFence);
        } else {
            long term = election.termToken();
            if (term == 0L) {
                throw new IllegalStateException("not active master leader");
            }
            request = RpcMessage.executeProviderGroup(
                    owner.workerId, record.runtimeStageId(), groupId,
                    publicationId, publicationGeneration, encoded,
                    election.self(), term, owner.endpoint.incarnationId(),
                    owner.runAttemptFence);
        }
        return callManifestControl(owner, record.runtimeStageId(), request,
                RpcMessage.Type.PROVIDER_GROUP_RESULT, deadline,
                RpcMessage::providerGroupOutcome);
    }

    private static io.rowweave.run.PreparedSinkLocator preparedSinkLocator(
            io.rowweave.run.SinkAttemptIdentity identity, String domainId,
            io.rowweave.commit.CommitManifest.Member member,
            long preparedAtMillis) {
        return new io.rowweave.run.PreparedSinkLocator(
                io.rowweave.run.PreparedSinkLocator.CURRENT_VERSION,
                identity,
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                domainId,
                io.rowweave.commit.ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), preparedAtMillis);
    }

    private void finalizeManifestOutputs(
            String groupId, java.util.List<Long> sinkStageIds, long deadline) {
        Throwable finalizeFailure = null;
        for (long stageId : sinkStageIds) {
            try {
                sendOutputControl(groupId, stageId, RpcMessage.Type.FINALIZE_PREPARED_OUTPUT, deadline);
            } catch (Throwable failure) {
                if (finalizeFailure == null) {
                    finalizeFailure = failure;
                } else {
                    finalizeFailure.addSuppressed(failure);
                }
            }
        }
        if (finalizeFailure != null) {
            throw new IllegalStateException("manifest committed but finalize failed", finalizeFailure);
        }
    }

    private static void validateManifestMembers(
            ManifestJobCommit context,
            java.util.List<io.rowweave.commit.CommitManifest.Member> members) {
        java.util.Map<String, String> actual = new java.util.LinkedHashMap<>();
        for (io.rowweave.commit.CommitManifest.Member member : members) {
            String previous = actual.put(member.name(), member.schemaFingerprint());
            if (previous != null) {
                throw new IllegalStateException("duplicate installed manifest member: " + member.name());
            }
            String expectedPath = "data/" + context.generation + "/" + member.name() + ".data";
            if (!expectedPath.equals(member.relativeDataPath())) {
                throw new IllegalStateException("installed manifest member path mismatch: " + member.name());
            }
        }
        if (!context.expectedSchemas.equals(actual)) {
            throw new IllegalStateException("installed manifest member set/schema does not match PlanGraph");
        }
    }

    private io.rowweave.commit.CommitManifest.Member sendManifestInstall(
            String groupId, long stageId, ManifestJobCommit context, long deadline) {
        StageOwner owner = requiredCurrentOutputOwner(stageId);
        RpcMessage command;
        MasterElection election = leadership;
        if (election == null) {
            command = RpcMessage.installManifestMember(
                    owner.workerId, stageId, groupId, context.attemptId,
                    context.generation, owner.endpoint.incarnationId(), owner.runAttemptFence);
        } else {
            long term = election.termToken();
            if (term == 0L) {
                throw new IllegalStateException("not active master leader");
            }
            command = RpcMessage.installManifestMember(
                    owner.workerId, stageId, groupId, context.attemptId, context.generation,
                    election.self(), term, owner.endpoint.incarnationId(), owner.runAttemptFence);
        }
        return callManifestControl(owner, stageId, command,
                RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, deadline,
                response -> io.rowweave.commit.ManifestCommitWire.decodeMember(
                        response.commitPayload()));
    }

    private void sendManifestPublish(
            String groupId, long stageId, ManifestJobCommit context,
            byte[] manifest, long deadline) {
        StageOwner owner = requiredCurrentOutputOwner(stageId);
        RpcMessage command;
        MasterElection election = leadership;
        if (election == null) {
            command = RpcMessage.publishManifestGroup(
                    owner.workerId, stageId, groupId, context.attemptId,
                    context.generation, manifest, owner.endpoint.incarnationId(), owner.runAttemptFence);
        } else {
            long term = election.termToken();
            if (term == 0L) {
                throw new IllegalStateException("not active master leader");
            }
            command = RpcMessage.publishManifestGroup(
                    owner.workerId, stageId, groupId, context.attemptId, context.generation,
                    manifest, election.self(), term, owner.endpoint.incarnationId(), owner.runAttemptFence);
        }
        callManifestControl(owner, stageId, command, RpcMessage.Type.ACK, deadline,
                response -> null);
    }

    private void sendManifestSeal(
            String groupId, long stageId, ManifestJobCommit context, long deadline) {
        StageOwner owner = requiredCurrentOutputOwner(stageId);
        RpcMessage command;
        MasterElection election = leadership;
        if (election == null) {
            command = RpcMessage.sealManifestMember(
                    owner.workerId, stageId, groupId, context.attemptId,
                    context.generation, owner.endpoint.incarnationId(),
                    owner.runAttemptFence);
        } else {
            long term = election.termToken();
            if (term == 0L) throw new IllegalStateException("not active master leader");
            command = RpcMessage.sealManifestMember(
                    owner.workerId, stageId, groupId, context.attemptId,
                    context.generation, election.self(), term,
                    owner.endpoint.incarnationId(), owner.runAttemptFence);
        }
        callManifestControl(owner, stageId, command, RpcMessage.Type.ACK, deadline,
                response -> null);
    }

    private StageOwner requiredCurrentOutputOwner(long stageId) {
        StageOwner owner = stageWorker.get(stageId);
        if (owner == null) {
            throw new IllegalStateException("prepared output owner missing: stageId=" + stageId);
        }
        RegistryWorker current = endpointOf(owner.workerId);
        if (current == null || !sameEndpoint(owner.endpoint, current)) {
            throw new IllegalStateException(
                    "prepared output worker generation changed: worker=" + owner.workerId
                            + " stageId=" + stageId);
        }
        return owner;
    }

    private <T> T callManifestControl(
            StageOwner owner, long stageId, RpcMessage command,
            RpcMessage.Type expected, long deadlineNanos,
            java.util.function.Function<RpcMessage, T> consumer) {
        Throwable firstFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new IllegalStateException("manifest output commit deadline exceeded", firstFailure);
            }
            int readTimeoutMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L,
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            io.rowweave.cluster.rpc.RpcTransport transport =
                    io.rowweave.cluster.rpc.RpcTransports.connect(
                            owner.endpoint.host(), owner.endpoint.port(),
                            Math.min(5_000, readTimeoutMs), readTimeoutMs,
                            controlSecurity, protocolHello);
            try {
                return transport.callScoped(secureOutbound(command), response -> {
                    response = secureInbound(response);
                    if (response == null || response.type() != expected) {
                        throw new IllegalStateException("worker " + owner.workerId
                                + " rejected manifest control for stage " + stageId + ": "
                                + (response == null ? "null response" : response.errorMessage()));
                    }
                    return consumer.apply(response);
                });
            } catch (RuntimeException | Error failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    failure.addSuppressed(firstFailure);
                    throw failure;
                }
            } finally {
                transport.close();
            }
        }
        throwUnchecked(firstFailure);
        throw new AssertionError("unreachable");
    }

    private Throwable rollbackPreparedOutputs(
            String groupId, java.util.List<Long> sinkStageIds, Throwable primary) {
        return rollbackPreparedOutputs(
                groupId, sinkStageIds, primary, deadlineAfterMillis(outputCommitTimeoutMs));
    }

    private Throwable rollbackPreparedOutputs(
            String groupId, java.util.List<Long> sinkStageIds, Throwable primary, long deadline) {
        if (groupId == null) {
            return primary;
        }
        Throwable failure = primary;
        for (int i = sinkStageIds.size() - 1; i >= 0; i--) {
            try {
                sendOutputControl(
                        groupId, sinkStageIds.get(i), RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT, deadline);
            } catch (Throwable rollbackFailure) {
                if (failure == null) {
                    failure = rollbackFailure;
                } else if (failure != rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
        }
        return failure;
    }

    private void renewPreparedOutputsWhileWaiting(
            String groupId, java.util.List<Long> sinkStageIds, long deadlineNanos) {
        for (long stageId : sinkStageIds) {
            if (deadlineNanos - System.nanoTime() <= 0L) return;
            StageTracker tracker = stageTrackers.get(stageId);
            if (tracker == null || !tracker.hasSuccessfulWorkerTerminal()) {
                continue;
            }
            StageOwner owner = stageWorker.get(stageId);
            if (owner == null) {
                continue;
            }
            RegistryWorker current = endpointOf(owner.workerId);
            if (current == null || !sameEndpoint(owner.endpoint, current)) {
                continue;
            }
            try {
                RpcMessage command;
                MasterElection election = leadership;
                if (election == null) {
                    command = RpcMessage.renewPreparedOutput(owner.workerId, stageId, groupId,
                            owner.endpoint.incarnationId(), owner.runAttemptFence);
                } else {
                    long term = election.termToken();
                    if (term == 0L) {
                        continue;
                    }
                    command = RpcMessage.renewPreparedOutput(owner.workerId, stageId, groupId,
                            election.self(), term, owner.endpoint.incarnationId(),
                            owner.runAttemptFence);
                }
                sendPreparedRenewalUntil(owner, command, deadlineNanos);
            } catch (RuntimeException renewalFailure) {
                // The next wait cycle retries; worker TTL still bounds abandoned state.
                LOG.debug("prepared Sink renewal failed stageId={} worker={}",
                        stageId, owner.workerId, renewalFailure);
            }
        }
    }

    private void sendPreparedRenewalUntil(
            StageOwner owner, RpcMessage command, long deadlineNanos) {
        java.util.concurrent.atomic.AtomicBoolean aborted =
                new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.atomic.AtomicReference<io.rowweave.cluster.rpc.RpcTransport> active =
                new java.util.concurrent.atomic.AtomicReference<>();
        cancellationCoordinator.runControlUntil(() -> {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L || aborted.get()) return;
            int timeoutMs = (int) Math.max(1L, Math.min(HEARTBEAT_POLL_MS,
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining)));
            // Short-lived transport avoids another Job's cancel/reconcile monitor. All blocking
            // connect/hello/write work stays on the bounded pool, with close as the timeout abort.
            try (io.rowweave.cluster.rpc.RpcTransport transport =
                         io.rowweave.cluster.rpc.RpcTransports.connect(
                                 owner.endpoint.host(), owner.endpoint.port(), timeoutMs, timeoutMs,
                                 controlSecurity, protocolHello)) {
                active.set(transport);
                if (!aborted.get() && deadlineNanos - System.nanoTime() > 0L) {
                    transport.send(secureOutbound(command));
                }
            } finally {
                active.set(null);
            }
        }, () -> {
            aborted.set(true);
            io.rowweave.cluster.rpc.RpcTransport transport = active.getAndSet(null);
            if (transport != null) transport.close();
        }, deadlineNanos);
    }

    private void sendOutputControl(
            String groupId, long stageId, RpcMessage.Type type, long deadlineNanos) {
        StageOwner owner = stageWorker.get(stageId);
        if (owner == null) {
            if (type == RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT) {
                return;  // a sink that was never assigned has no state on a worker to roll back
            }
            throw new IllegalStateException("prepared output owner missing: stageId=" + stageId);
        }
        RegistryWorker current = endpointOf(owner.workerId);
        if (current == null || !sameEndpoint(owner.endpoint, current)) {
            throw new IllegalStateException(
                    "prepared output worker generation changed: worker=" + owner.workerId + " stageId=" + stageId);
        }
        RpcMessage command;
        MasterElection election = leadership;
        if (election == null) {
            command = outputControlMessage(type, owner.workerId, stageId, groupId,
                    owner.endpoint.incarnationId(), owner.runAttemptFence);
        } else {
            long term = election.termToken();
            if (term == 0L) {
                throw new IllegalStateException("not active master leader");
            }
            command = outputControlMessage(type, owner.workerId, stageId, groupId,
                    election.self(), term, owner.endpoint.incarnationId(), owner.runAttemptFence);
        }
        Throwable firstFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                IllegalStateException timeout = new IllegalStateException(
                        "output commit deadline exceeded after " + outputCommitTimeoutMs + "ms");
                if (firstFailure != null) {
                    timeout.addSuppressed(firstFailure);
                }
                throw timeout;
            }
            long remainingMs = Math.max(1L,
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos));
            int readTimeoutMs = (int) Math.min(Integer.MAX_VALUE, remainingMs);
            int connectTimeoutMs = Math.min(5_000, readTimeoutMs);
            io.rowweave.cluster.rpc.RpcTransport transport =
                    io.rowweave.cluster.rpc.RpcTransports.connect(
                            owner.endpoint.host(), owner.endpoint.port(), connectTimeoutMs,
                            readTimeoutMs, controlSecurity, protocolHello);
            try {
                RpcMessage response = secureInbound(transport.call(secureOutbound(command)));
                if (response == null || response.type() != RpcMessage.Type.ACK) {
                    throw new IllegalStateException("worker " + owner.workerId + " rejected " + type
                            + " for stage " + stageId + ": "
                            + (response == null ? "null response" : response.errorMessage()));
                }
                return;
            } catch (RuntimeException | Error failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    failure.addSuppressed(firstFailure);
                    throw failure;
                }
            } finally {
                transport.close();
            }
        }
        throwUnchecked(firstFailure);
    }

    private static RpcMessage outputControlMessage(
            RpcMessage.Type type, String workerId, long stageId, String groupId,
            String incarnationId, RunAttemptFence runAttemptFence) {
        if (type == RpcMessage.Type.COMMIT_PREPARED_OUTPUT) {
            return RpcMessage.commitPreparedOutput(
                    workerId, stageId, groupId, incarnationId, runAttemptFence);
        }
        if (type == RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT) {
            return RpcMessage.rollbackPreparedOutput(
                    workerId, stageId, groupId, incarnationId, runAttemptFence);
        }
        if (type == RpcMessage.Type.FINALIZE_PREPARED_OUTPUT) {
            return RpcMessage.finalizePreparedOutput(
                    workerId, stageId, groupId, incarnationId, runAttemptFence);
        }
        throw new IllegalArgumentException("not an output control type: " + type);
    }

    private static RpcMessage outputControlMessage(
            RpcMessage.Type type, String workerId, long stageId, String groupId,
            Endpoint leader, long term, String incarnationId,
            RunAttemptFence runAttemptFence) {
        if (type == RpcMessage.Type.COMMIT_PREPARED_OUTPUT) {
            return RpcMessage.commitPreparedOutput(
                    workerId, stageId, groupId, leader, term, incarnationId, runAttemptFence);
        }
        if (type == RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT) {
            return RpcMessage.rollbackPreparedOutput(
                    workerId, stageId, groupId, leader, term, incarnationId, runAttemptFence);
        }
        if (type == RpcMessage.Type.FINALIZE_PREPARED_OUTPUT) {
            return RpcMessage.finalizePreparedOutput(
                    workerId, stageId, groupId, leader, term, incarnationId, runAttemptFence);
        }
        throw new IllegalArgumentException("not an output control type: " + type);
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    private static long deadlineAfterMillis(long timeoutMs) {
        return addMillisSaturated(System.nanoTime(), timeoutMs);
    }

    private static long addMillisSaturated(long originNanos, long timeoutMs) {
        long delta = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            return Math.addExact(originNanos, delta);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static String describeFailure(Throwable failure) {
        StringBuilder description = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (description.length() > 0) {
                description.append(" <- ");
            }
            description.append(current.getClass().getSimpleName());
            if (current.getMessage() != null) {
                description.append(": ").append(current.getMessage());
            }
        }
        return description.toString();
    }

    private void cleanupStageTracking(java.util.List<Long> stageIds) {
        for (long id : stageIds) {
            stageTrackers.remove(id);
            portTrackers.remove(id);
            stageWorker.remove(id);
        }
    }

    private static io.rowweave.plan.StageExecutionTelemetry decodeStageTelemetry(
            RpcMessage report) {
        try {
            report.reserveResidentBytes(
                    io.rowweave.plan.StageExecutionTelemetryCodec
                            .MAX_MATERIALIZED_RESIDENT_BYTES);
            return report.stageExecutionTelemetry();
        } catch (RuntimeException | LinkageError malformedTelemetry) {
            return io.rowweave.plan.StageExecutionTelemetry.unavailable();
        }
    }

    private RpcMessage ephemeralJobResult(
            long totalRows, java.util.List<Long> stageIds, RpcMessage residentRequest) {
        try {
            if (residentRequest != null) {
                residentRequest.reserveResponseResidentBytes(
                        io.rowweave.plan.ExecutionTelemetrySummaryCodec
                                .MAX_MATERIALIZED_RESIDENT_BYTES);
            }
            Iterable<EphemeralExecutionTelemetryAggregator.Physical> physical = () ->
                    new java.util.Iterator<EphemeralExecutionTelemetryAggregator.Physical>() {
                        private final java.util.Iterator<Long> ids = stageIds.iterator();
                        @Override public boolean hasNext() { return ids.hasNext(); }
                        @Override public EphemeralExecutionTelemetryAggregator.Physical next() {
                            StageTracker tracker = stageTrackers.get(ids.next());
                            return tracker == null ? null : tracker.telemetrySnapshot();
                        }
                    };
            return RpcMessage.jobResultWithTelemetrySummary(
                    true, totalRows, null,
                    EphemeralExecutionTelemetryAggregator.aggregate(physical));
        } catch (RuntimeException | LinkageError unavailableTelemetry) {
            return RpcMessage.jobResult(true, totalRows, null);
        }
    }

    /**
     * How often, in milliseconds, to poll: the short wait on a barrier and the rhythm of checking whether the workers are alive.
     */
    private static final long HEARTBEAT_POLL_MS = 500L;

    /**
     * Where a report is consumed, the generation of the stage's owner is verified against what this master holds locally, because the hot
     * path of reports is not a place to go reading a registry.
     */
    private boolean stageMessageCurrent(RpcMessage req) {
        synchronized (workerLifecycleLock) {
            StageOwner owner = stageWorker.get(req.stageId());
            if (owner == null || !java.util.Objects.equals(owner.workerId, req.workerId())
                    || !java.util.Objects.equals(owner.endpoint.incarnationId(), req.incarnationId())
                    || !owner.runAttemptFence.equals(req.runAttemptFence())) {
                return false;
            }
            WorkerInfo current = workers.get(req.workerId());
            return current != null && sameEndpoint(current, owner.endpoint);
        }
    }

    /** Records a worker as alive; any message from it is the evidence. */
    private void beatWorker(String workerId) {
        if (workerId == null) {
            return;
        }
        WorkerInfo w = workers.get(workerId);
        if (w != null) {
            w.heartbeat.beat();
        }
    }

    /**
     * The first worker this job involves that has gone quiet, by heartbeat, or null if all are alive. The map of heartbeats records only
     * workers this master has heard from, so immediately after a failover the new leader may not have heard from one yet, since it will
     * register again on its next heartbeat. Where the registry has that worker, it is treated as alive: the registry is the source of truth
     * about whether a worker exists, and a local heartbeat is only evidence that this master saw one, so its absence is not death. This is
     * what stops a failover from condemning registered workers.
     */
    private String firstDeadWorker(java.util.Collection<StageOwner> jobWorkers) {
        for (StageOwner owner : jobWorkers) {
            String wid = owner.workerId;
            WorkerInfo worker = workers.get(wid);
            RegistryWorker endpoint = endpointOf(wid);
            boolean endpointCurrent = endpoint != null
                    && sameEndpoint(owner.endpoint, endpoint)
                    && worker != null && sameEndpoint(worker, endpoint);
            boolean heartbeatAlive = endpointCurrent
                    && workerLivenessPauseGuard.isAlive(worker.heartbeat);
            if (!heartbeatAlive) {
                if (TRACE_HEARTBEATS) {
                    LOG.warn("heartbeat trace worker-lost worker={} registered={} endpointCurrent={} heartbeatAlive={}",
                            wid, worker != null, endpointCurrent, heartbeatAlive);
                }
                return wid;
            }
        }
        return null;
    }

    private static final class AssignmentProfile {
        final java.util.concurrent.atomic.AtomicLong portWaitNanos = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong serializeNanos = new java.util.concurrent.atomic.AtomicLong();
    }

    /**
     * An immutable assignment built by the planning thread. A dispatcher only serializes it and makes the calls concurrently; it never
     * touches the graph maps.
     */
    private static final class PendingStageAssignment {
        final String workerId;
        final RegistryWorker expectedEndpoint;
        final long stageId;
        final StageCommand command;
        final StageAssignmentToken assignmentToken;
        final RecoveryToken recoveryToken;
        final io.rowweave.incremental.FrontierRecoveryToken frontierToken;
        final RpcMessage residentRequest;
        final long executionDeadlineNanos;

        PendingStageAssignment(String workerId, RegistryWorker expectedEndpoint,
                               long stageId, StageCommand command,
                               StageAssignmentToken assignmentToken,
                               RecoveryToken recoveryToken,
                               io.rowweave.incremental.FrontierRecoveryToken frontierToken,
                               RpcMessage residentRequest,
                               long executionDeadlineNanos) {
            this.workerId = workerId;
            this.expectedEndpoint = expectedEndpoint;
            this.stageId = stageId;
            this.command = command;
            this.assignmentToken = assignmentToken;
            this.recoveryToken = recoveryToken;
            this.frontierToken = frontierToken;
            this.residentRequest = residentRequest;
            this.executionDeadlineNanos = executionDeadlineNanos;
        }
    }

    /** Builds the only mux endpoint form permitted by the destination's registered profile. */
    private static io.rowweave.sched.net.ShuffleEndpoint muxEndpoint(
            RegistryWorker worker, java.util.UUID targetId) {
        io.rowweave.cluster.rpc.DataPlaneRegistration registration =
                worker.dataPlaneRegistration();
        if (registration == null) {
            return io.rowweave.sched.net.ShuffleEndpoint.mux(
                    worker.host(), worker.dataPort(), targetId);
        }
        io.rowweave.cluster.rpc.RpcSecurityMetadata metadata =
                worker.securityMetadata();
        if (metadata == null || worker.incarnationId() == null) {
            throw new IllegalStateException(
                    "authenticated Worker endpoint is missing exact generation metadata");
        }
        io.rowweave.sched.net.WorkerDataIdentity identity =
                io.rowweave.sched.net.WorkerDataIdentity.authenticated(
                        worker.workerId(), worker.incarnationId(),
                        registration.protocol(), metadata.deploymentSetId(),
                        metadata.authzSetId(), registration.certificateFingerprint());
        return io.rowweave.sched.net.ShuffleEndpoint.muxAuthenticated(
                worker.host(), worker.dataPort(), targetId, identity);
    }

    private static final class DagAssignmentContext {
        private final java.util.Map<String, RegistryWorker> workers = new java.util.HashMap<>();
        private final RunAttemptFence runAttemptFence;
        private final DurableStageAssignmentPlan durableAssignments;
        private final String publicationId;
        private final long publicationGeneration;
        private final RecoveryToken recoveryToken;
        private final io.rowweave.incremental.FrontierRecoveryToken frontierToken;
        private final RpcMessage residentRequest;
        private final EphemeralSubmitDeadline ephemeralDeadline;
        private final SecureShuffleBindings secureShuffle;
        private final PlanGraph graph;
        private final java.util.Map<Integer, Stage> stages;
        private final PhysicalPlan physicalPlan;
        private final byte[] portableArtifactBytes;

        DagAssignmentContext(java.util.List<RegistryWorker> snapshot,
                             RunAttemptFence runAttemptFence,
                             DurableStageAssignmentPlan durableAssignments,
                             String publicationId,
                             long publicationGeneration,
                             RecoveryToken recoveryToken,
                             io.rowweave.incremental.FrontierRecoveryToken frontierToken,
                             RpcMessage residentRequest,
                             EphemeralSubmitDeadline ephemeralDeadline,
                             SecureShuffleBindings secureShuffle,
                             PlanGraph graph, ExecutionGraph executionGraph,
                             PhysicalPlan physicalPlan,
                             byte[] portableArtifactBytes) {
            for (RegistryWorker worker : snapshot) {
                workers.put(worker.workerId(), worker);
            }
            this.runAttemptFence = java.util.Objects.requireNonNull(
                    runAttemptFence, "runAttemptFence");
            this.durableAssignments = durableAssignments;
            this.publicationId = publicationId;
            this.publicationGeneration = publicationGeneration;
            this.recoveryToken = recoveryToken;
            this.frontierToken = frontierToken;
            this.residentRequest = residentRequest;
            this.ephemeralDeadline = ephemeralDeadline;
            this.secureShuffle = secureShuffle;
            this.graph = java.util.Objects.requireNonNull(graph, "graph");
            this.stages = new java.util.HashMap<>();
            for (Stage stage : executionGraph.stages()) {
                this.stages.put(stage.index(), stage);
            }
            this.physicalPlan = java.util.Objects.requireNonNull(
                    physicalPlan, "physicalPlan");
            this.portableArtifactBytes = portableArtifactBytes;
        }

        RegistryWorker worker(String workerId) {
            RegistryWorker worker = workers.get(workerId);
            if (worker == null) {
                throw new IllegalStateException("worker absent from job assignment snapshot: " + workerId);
            }
            return worker;
        }

        PendingStageAssignment pending(int logicalStage, int instanceIndex,
                                       String workerId, long stageId,
                                       StageCommand command) {
            if (portableArtifactBytes != null) {
                Stage stage = stages.get(logicalStage);
                if (stage == null) {
                    throw new IllegalStateException(
                            "portable assignment references an unknown stage");
                }
                int instanceCount = physicalPlan.instancesOf(
                        logicalStage).size();
                command.withPortableStageArtifact(
                        portableArtifactBytes,
                        new PortableStageSpec(
                                logicalStage, stage.members(), instanceIndex,
                                instanceCount,
                                appendedSinkCanonicalization(stage, command)));
            }
            StageCommand bound = bind(command, stageId);
            if (secureShuffle != null) {
                secureShuffle.acceptCommand(
                        logicalStage, instanceIndex, workerId, bound);
            }
            return new PendingStageAssignment(workerId, worker(workerId), stageId, bound,
                    token(stageId), recoveryToken, frontierToken, residentRequest,
                    executionDeadlineNanos());
        }

        long executionDeadlineNanos() {
            return ephemeralDeadline == null
                    ? 0L : ephemeralDeadline.executionDeadlineNanos();
        }

        long setupWaitMillis() {
            return ephemeralDeadline == null
                    ? STAGE_SETUP_TIMEOUT_MS
                    : ephemeralDeadline.executionWaitMillis(
                            STAGE_SETUP_TIMEOUT_MS);
        }

        StageAssignmentToken token(long stageId) {
            return durableAssignments == null ? null : durableAssignments.token(stageId);
        }

        RecoveryToken recoveryToken() { return recoveryToken; }

        StageCommand bind(StageCommand command, long stageId) {
            StageCommand bound = command.withRunAttemptFence(runAttemptFence);
            if (durableAssignments != null
                    && bound.output() != null
                    && bound.output().getClass()
                    == io.rowweave.ProviderBatchSink.class) {
                io.rowweave.run.SinkAttemptIdentity identity =
                        durableAssignments.sinkAttemptIdentity(stageId);
                bound.withDurableSinkContext(identity, publicationId,
                        publicationGeneration);
            }
            return bound;
        }

        boolean muxOn(String workerId) {
            return MUX_ENABLED && worker(workerId).dataPort() > 0;
        }

        private boolean appendedSinkCanonicalization(
                Stage stage, StageCommand command) {
            if (!stage.isSink() || command.sinkFragment() == null
                    || !"orderBy".equals(
                    command.sinkFragment().root().typeId())) {
                return false;
            }
            for (int member : stage.members()) {
                PlanNode node = graph.node(member);
                if (node.kind() != PlanNode.Kind.SINK
                        && ("orderBy".equals(node.typeId())
                        || "top".equals(node.typeId()))) {
                    return false;
                }
            }
            return true;
        }

    }

    private static boolean allAuthenticatedMux(
            java.util.List<RegistryWorker> workers) {
        if (!MUX_ENABLED || workers == null || workers.isEmpty()) return false;
        for (RegistryWorker worker : workers) {
            io.rowweave.cluster.rpc.DataPlaneRegistration registration =
                    worker.dataPlaneRegistration();
            if (worker.dataPort() <= 0 || registration == null
                    || registration.protocol()
                    != io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
                return false;
            }
        }
        return true;
    }

    /** Exact per-attempt route/grant closure consumed by command assembly only. */
    private static final class SecureShuffleBindings {
        final ShuffleTransportPlan transportPlan;
        final ShuffleGrantPlan grantPlan;
        final StageCommandShuffleClosure commandClosure;

        private SecureShuffleBindings(ShuffleTransportPlan transportPlan,
                                      ShuffleGrantPlan grantPlan,
                                      StageCommandShuffleClosure commandClosure) {
            this.transportPlan = transportPlan;
            this.grantPlan = grantPlan;
            this.commandClosure = commandClosure;
        }

        static SecureShuffleBindings create(
                ExecutionGraph graph, PhysicalPlan physicalPlan,
                java.util.List<RegistryWorker> workers,
                RunAttemptFence fence, String jobIdentity,
                boolean objectReuse) {
            java.util.Map<String, RegistryWorker> byId = new java.util.LinkedHashMap<>();
            for (RegistryWorker worker : workers) {
                if (byId.put(worker.workerId(), worker) != null) {
                    throw new IllegalArgumentException("duplicate Worker in assignment snapshot");
                }
            }
            java.security.SecureRandom random = new java.security.SecureRandom();
            ShuffleTransportPlan transport = ShuffleTransportPlan.create(
                    graph, physicalPlan, byId, fence, jobIdentity,
                    objectReuse, random);
            ShuffleGrantPlan grants = ShuffleGrantPlan.create(transport, random);
            return new SecureShuffleBindings(transport, grants,
                    StageCommandShuffleClosure.create(
                            graph, physicalPlan, transport, grants));
        }

        void acceptCommand(int logicalStage, int instanceIndex,
                           String workerId, StageCommand command) {
            commandClosure.accept(
                    logicalStage, instanceIndex, workerId, command);
        }

        void assertCommandsComplete() {
            commandClosure.assertComplete();
        }

        ShuffleTransportPlan.Route route(
                int consumerStage, int edge, int consumerInstance,
                int producerStage, int producerInstance) {
            ShuffleTransportPlan.Route route = transportPlan.route(
                    consumerStage, edge, consumerInstance,
                    producerStage, producerInstance);
            if (route == null) {
                throw new IllegalStateException("secure shuffle route is missing");
            }
            return route;
        }

        int localProducerCount(int consumerStage, int edge, int consumerInstance) {
            int count = 0;
            for (ShuffleTransportPlan.Route route : transportPlan.routes()) {
                ShuffleTransportPlan.RouteKey key = route.key();
                if (key.consumerStage() == consumerStage
                        && key.consumerEdge() == edge
                        && key.consumerInstance() == consumerInstance
                        && route.isLocal()) count++;
            }
            return count;
        }

        String localKey(int consumerStage, int edge, int consumerInstance) {
            for (ShuffleTransportPlan.Route route : transportPlan.routes()) {
                ShuffleTransportPlan.RouteKey key = route.key();
                if (key.consumerStage() == consumerStage
                        && key.consumerEdge() == edge
                        && key.consumerInstance() == consumerInstance
                        && route.isLocal()) return route.localKey();
            }
            return null;
        }

        java.util.UUID targetId(int consumerStage, int edge, int consumerInstance) {
            io.rowweave.sched.net.ShuffleInboundAuthorization authorization =
                    grantPlan.consumerAuthorization(
                            consumerStage, edge, consumerInstance);
            return authorization == null ? null : authorization.targetId();
        }

        java.util.List<io.rowweave.sched.net.ShuffleInboundAuthorization>
        authorizations(int consumerStage, int consumerInstance) {
            return grantPlan.consumerAuthorizations(consumerStage, consumerInstance);
        }

        io.rowweave.sched.net.ShuffleEndpoint safeEndpoint(
                int consumerStage, int edge, int consumerInstance) {
            for (ShuffleTransportPlan.Route route : transportPlan.routes()) {
                ShuffleTransportPlan.RouteKey key = route.key();
                if (key.consumerStage() == consumerStage
                        && key.consumerEdge() == edge
                        && key.consumerInstance() == consumerInstance
                        && !route.isLocal()) {
                    return io.rowweave.sched.net.ShuffleEndpoint.muxAuthenticated(
                            route.destinationHost(), route.destinationDataPort(),
                            route.targetId(), route.destinationIdentity());
                }
            }
            return null;
        }

        java.util.List<ShuffleTarget> targets(
                int producerStage, int producerInstance,
                int consumerStage, int edge, int consumerInstances) {
            java.util.List<ShuffleTarget> targets =
                    new java.util.ArrayList<>(consumerInstances);
            for (int consumerInstance = 0;
                 consumerInstance < consumerInstances; consumerInstance++) {
                ShuffleTransportPlan.Route route = route(
                        consumerStage, edge, consumerInstance,
                        producerStage, producerInstance);
                if (route.isLocal()) {
                    targets.add(ShuffleTarget.local(route.localKey()));
                } else {
                    io.rowweave.sched.net.ShuffleEndpoint endpoint =
                            grantPlan.producerEndpoint(route.key());
                    if (endpoint == null) {
                        throw new IllegalStateException(
                                "secure shuffle producer grant is missing");
                    }
                    targets.add(ShuffleTarget.socket(endpoint));
                }
            }
            return targets;
        }
    }

    private void dispatchAssignments(java.util.List<PendingStageAssignment> assignments,
                                     Object serializationMutex,
                                     AssignmentProfile assignmentProfile) {
        if (assignments.isEmpty()) {
            return;
        }
        java.util.List<java.util.concurrent.Callable<Void>> calls = new java.util.ArrayList<>(assignments.size());
        for (PendingStageAssignment pending : assignments) {
            calls.add(() -> {
                boolean direct = inProcessDirectAssignment(
                        pending.expectedEndpoint, pending.command,
                        pending.assignmentToken);
                byte[] blob = null;
                if (!direct) {
                    synchronized (serializationMutex) {
                        blob = serialize(pending.command, assignmentProfile,
                                pending.residentRequest);
                    }
                }
                assignTo(pending.workerId, pending.expectedEndpoint, pending.stageId,
                        blob, direct ? pending.command : null,
                        pending.command.runAttemptFence(), pending.assignmentToken,
                        pending.recoveryToken, pending.frontierToken,
                        pending.executionDeadlineNanos);
                return null;
            });
        }
        assignmentDispatcher.invokeAll(calls);
    }

    private void scheduleAssignments(java.util.List<PendingStageAssignment> wave,
                                     Object serializationMutex,
                                     boolean fullMuxBatch,
                                     java.util.List<PendingStageAssignment> deferredMuxAssignments,
                                     java.util.List<java.util.List<PendingStageAssignment>> deferredSecureWaves,
                                     boolean secureBatch,
                                     AssignmentProfile assignmentProfile) {
        if (secureBatch) {
            deferredSecureWaves.add(java.util.Collections.unmodifiableList(
                    new java.util.ArrayList<>(wave)));
        } else if (fullMuxBatch) {
            deferredMuxAssignments.addAll(wave);
        } else {
            dispatchAssignments(wave, serializationMutex, assignmentProfile);
        }
    }

    /**
     * Assigns a stage that listens, which acknowledges at once, and waits for its port report to learn the ports actually bound, one layer
     * at a time.
     */
    private int assignListening(RegistryWorker expected, long stageId, StageCommand cmd,
                                AssignmentProfile assignmentProfile,
                                DagAssignmentContext assignmentContext) throws Exception {
        StagePortTracker pt = new StagePortTracker(true, false);
        portTrackers.put(stageId, pt);
        boolean direct = inProcessDirectAssignment(
                expected, cmd, assignmentContext.token(stageId));
        assignTo(expected.workerId(), expected, stageId,
                direct ? null : serialize(
                        cmd, assignmentProfile, assignmentContext.residentRequest),
                direct ? cmd : null,
                cmd.runAttemptFence(), assignmentContext.token(stageId),
                assignmentContext.recoveryToken(),
                assignmentContext.frontierToken,
                assignmentContext.executionDeadlineNanos());
        long t = System.nanoTime();
        int p = pt.await(assignmentContext.setupWaitMillis());
        assignmentProfile.portWaitNanos.addAndGet(System.nanoTime() - t);
        return p;
    }

    /**
     * Resolves a worker's endpoint from the registry, which is where registration is recorded; null for an id it has never seen.
     */
    private RegistryWorker endpointOf(String workerId) {
        return registry.worker(workerId);
    }

    /**
     * Assigns any stage to a worker, acknowledging at once and taking execution and ports back asynchronously. Records which worker owns
     * each stage, which is what a cancellation looks up.
     */
    private void assignTo(String workerId, RegistryWorker expected, long stageId,
                          byte[] blob, StageCommand inProcessCommand,
                          RunAttemptFence runAttemptFence,
                          StageAssignmentToken assignmentToken,
                          RecoveryToken recoveryToken,
                          io.rowweave.incremental.FrontierRecoveryToken frontierToken,
                          long executionDeadlineNanos) {
        probeDeadlineBeforeAssignment(
                runAttemptFence, assignmentToken, frontierToken, recoveryToken);
        java.util.concurrent.locks.Lock fence = assignmentLifecycle.readLock();
        fence.lock();
        try {
            requireActiveLeader();
            byte[] digest = assignmentToken == null ? null : sha256(blob);
            RegistryWorker w;
            java.util.concurrent.locks.Lock runFence = runDispatchLifecycle.readLock();
            runFence.lock();
            try {
                requireDurableRunDispatchable(
                        runAttemptFence, assignmentToken,
                        frontierToken, recoveryToken);
                w = claimStageAssignment(workerId, stageId, expected,
                        runAttemptFence, assignmentToken, digest,
                        recoveryToken, frontierToken);
            } finally {
                runFence.unlock();
            }
            RpcMessage assignment = assignmentMessage(
                    workerId, stageId, blob, inProcessCommand,
                    w.incarnationId(), runAttemptFence,
                    assignmentToken);
            // Reuses the long-lived connection rather than connecting, calling and closing per stage. A call that
            // throws drops the connection and the attempt is made once more against a new one.
            RpcMessage response = executionDeadlineNanos == 0L
                    ? secureInbound(assignmentLanes.call(
                            w.host(), w.port(), secureOutbound(assignment),
                            () -> stageAssignmentCurrent(workerId, stageId, w)))
                    : callAssignmentBeforeDeadline(
                            workerId, stageId, w, assignment,
                            executionDeadlineNanos);
            requireAssignAck(response, workerId, stageId);
        } finally {
            fence.unlock();
        }
    }

    /** Ephemeral ASSIGN keeps the existing one retry, but both attempts share one request deadline. */
    private RpcMessage callAssignmentBeforeDeadline(
            String workerId, long stageId, RegistryWorker worker,
            RpcMessage assignment, long deadlineNanos) {
        Throwable firstFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new IllegalStateException(
                        "ephemeral assignment deadline exceeded", firstFailure);
            }
            int readTimeoutMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L,
                    java.util.concurrent.TimeUnit.NANOSECONDS
                            .toMillis(remainingNanos - 1L) + 1L));
            io.rowweave.cluster.rpc.RpcTransport transport =
                    io.rowweave.cluster.rpc.RpcTransports.connect(
                            worker.host(), worker.port(),
                            Math.min(5_000, readTimeoutMs), readTimeoutMs,
                            controlSecurity, protocolHello);
            try {
                return secureInbound(transport.call(secureOutbound(assignment)));
            } catch (RuntimeException | Error failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else if (failure != firstFailure) {
                    firstFailure.addSuppressed(failure);
                }
                if (attempt > 0
                        || !stageAssignmentCurrent(workerId, stageId, worker)) {
                    throwUnchecked(firstFailure);
                }
            } finally {
                transport.close();
            }
        }
        throwUnchecked(firstFailure);
        throw new AssertionError("unreachable");
    }

    /**
     * Registering the owner of an assignment and a worker's departure linearise at the same point. If the departure wins, this dispatch
     * fails before touching the network; if the claim wins, the departure will find the tracker and release it. The whole-graph fast path
     * additionally passes the endpoint chosen while planning, so a replacement generation of the same id cannot pick up an old plan.
     */
    private RegistryWorker claimStageAssignment(String workerId, long stageId, RegistryWorker expected,
                                                RunAttemptFence runAttemptFence,
                                                StageAssignmentToken assignmentToken,
                                                byte[] commandDigest,
                                                RecoveryToken recoveryToken,
                                                io.rowweave.incremental.FrontierRecoveryToken frontierToken) {
        // The registry snapshot is taken outside the lock and the full generation re-checked inside it, so a
        // slow lookup cannot freeze a registration or a heartbeat.
        RegistryWorker endpoint = endpointOf(workerId);
        synchronized (workerLifecycleLock) {
            requireActiveLeader();
            WorkerInfo current = workers.get(workerId);
            if (current == null || endpoint == null || quarantinedWorkers.containsKey(workerId)
                    || !sameEndpoint(current, endpoint)
                    || (expected != null && !sameEndpoint(expected, endpoint))) {
                throw new IllegalStateException("worker endpoint departed before assign: " + workerId);
            }
            stageWorker.put(stageId, new StageOwner(workerId, endpoint, runAttemptFence,
                    assignmentToken, commandDigest, recoveryToken,
                    frontierToken));
            return endpoint;
        }
    }

    private void requireDurableRunDispatchable(
            RunAttemptFence fence, StageAssignmentToken assignmentToken,
            io.rowweave.incremental.FrontierRecoveryToken frontierToken,
            RecoveryToken recoveryToken) {
        if (assignmentToken == null) return;
        if (frontierToken != null) {
            RunSnapshot run = incrementalAuthorityGateway.runSnapshot(
                    fence.runId());
            AttemptSnapshot attempt = run == null ? null : run.currentAttempt();
            if (recoveryToken == null
                    || !incrementalAuthorityGateway.currentlyOwns(
                    frontierToken, recoveryToken)
                    || attempt == null
                    || !fence.attemptId().equals(attempt.attemptId())
                    || fence.fenceToken() != attempt.fenceToken()
                    || attempt.status() != AttemptStatus.RUNNING) {
                throw new IllegalStateException(
                        "incremental Run is no longer dispatchable: "
                                + fence.runId());
            }
            return;
        }
        if (runStateStore == null) {
            throw new IllegalStateException("durable Run authority unavailable before assign");
        }
        RunSnapshot run = runStateStore.get(fence.runId());
        AttemptSnapshot attempt = run == null ? null : run.currentAttempt();
        if (attempt == null || !fence.attemptId().equals(attempt.attemptId())
                || fence.fenceToken() != attempt.fenceToken()
                || attempt.status() != AttemptStatus.RUNNING) {
            throw new IllegalStateException(
                    "durable Run is no longer dispatchable: " + fence.runId());
        }
    }

    private void probeDeadlineBeforeAssignment(
            RunAttemptFence fence, StageAssignmentToken assignmentToken,
            io.rowweave.incremental.FrontierRecoveryToken frontierToken,
            RecoveryToken recoveryToken) {
        if (assignmentToken == null) return;
        if (frontierToken != null) {
            if (recoveryToken == null
                    || !incrementalAuthorityGateway.currentlyOwns(
                    frontierToken, recoveryToken)) {
                throw new IllegalStateException(
                        "incremental authority unavailable before deadline probe");
            }
            RunSnapshot run = incrementalAuthorityGateway.runSnapshot(
                    fence.runId());
            if (run == null) {
                throw new IllegalStateException(
                        "incremental Run authority unavailable before deadline probe");
            }
            return;
        }
        if (runStateStore == null) {
            throw new IllegalStateException(
                    "durable Run authority unavailable before deadline probe");
        }
        RunSnapshot run = runStateStore.get(fence.runId());
        if (run != null && run.deadlineEpochMillis() > 0L
                && deadlineStopsExecution(evaluateRunDeadline(fence.runId()))) {
            throw new IllegalStateException(
                    "durable Run deadline prevents assignment: " + fence.runId());
        }
    }

    private static boolean sameEndpoint(WorkerInfo worker, RegistryWorker endpoint) {
        return worker.port == endpoint.port()
                && worker.dataPort == endpoint.dataPort()
                && java.util.Objects.equals(worker.host, endpoint.host())
                && java.util.Objects.equals(worker.incarnationId, endpoint.incarnationId())
                && java.util.Objects.equals(worker.securityMetadata, endpoint.securityMetadata())
                && java.util.Objects.equals(worker.dataPlaneRegistration,
                        endpoint.dataPlaneRegistration());
    }

    private static boolean sameEndpoint(RegistryWorker left, RegistryWorker right) {
        return left.port() == right.port()
                && left.dataPort() == right.dataPort()
                && java.util.Objects.equals(left.host(), right.host())
                && java.util.Objects.equals(left.incarnationId(), right.incarnationId())
                && java.util.Objects.equals(left.securityMetadata(), right.securityMetadata())
                && java.util.Objects.equals(left.dataPlaneRegistration(),
                        right.dataPlaneRegistration());
    }

    /**
     * Permission to reconnect after the first assignment call failed. Once a worker has departed or been replaced, its older generation
     * must not be brought back.
     */
    private boolean stageAssignmentCurrent(String workerId, long stageId, RegistryWorker claimed) {
        // Local state first filters a request that is obviously stale, so a pointless read is avoided; and a
        // read, when one is needed, is never taken while holding the lifecycle lock.
        synchronized (workerLifecycleLock) {
            if (!stageAssignmentLocallyCurrentLocked(workerId, stageId, claimed)) {
                return false;
            }
        }
        RegistryWorker endpoint = endpointOf(workerId);
        synchronized (workerLifecycleLock) {
            if (!stageAssignmentLocallyCurrentLocked(workerId, stageId, claimed)) {
                return false;
            }
            WorkerInfo current = workers.get(workerId);
            return current != null && endpoint != null
                    && sameEndpoint(current, endpoint) && sameEndpoint(claimed, endpoint);
        }
    }

    private boolean stageAssignmentLocallyCurrentLocked(String workerId, long stageId,
                                                         RegistryWorker claimed) {
        StageOwner owner = stageWorker.get(stageId);
        if (owner == null || !workerId.equals(owner.workerId)
                || !sameEndpoint(owner.endpoint, claimed)
                || quarantinedWorkers.containsKey(workerId)) {
            return false;
        }
        WorkerInfo current = workers.get(workerId);
        return current != null && sameEndpoint(current, claimed);
    }

    private RpcMessage assignmentMessage(String workerId, long stageId, byte[] blob,
                                         StageCommand inProcessCommand,
                                         String incarnationId,
                                         RunAttemptFence runAttemptFence,
                                         StageAssignmentToken assignmentToken) {
        requireActiveLeader();
        byte[] encodedToken = assignmentToken == null ? null
                : StageAssignmentManifestCodec.encodeToken(assignmentToken);
        if (inProcessCommand != null && encodedToken != null) {
            throw new IllegalArgumentException(
                    "durable assignment requires canonical command bytes");
        }
        MasterElection election = leadership;
        if (election == null) {
            if (inProcessCommand != null) {
                return RpcMessage.assignStageInProcess(
                        workerId, stageId, inProcessCommand,
                        incarnationId, runAttemptFence);
            }
            return encodedToken == null
                    ? RpcMessage.assignStage(workerId, stageId, blob, incarnationId, runAttemptFence)
                    : RpcMessage.assignStage(workerId, stageId, blob, incarnationId,
                    runAttemptFence, encodedToken);
        }
        long term = election.termToken();
        if (term == 0L) {
            throw new IllegalStateException("not active master leader");
        }
        if (inProcessCommand != null) {
            return RpcMessage.assignStageInProcess(
                    workerId, stageId, inProcessCommand,
                    election.self(), term, incarnationId, runAttemptFence);
        }
        return RpcMessage.assignStage(
                workerId, stageId, blob, election.self(), term, incarnationId,
                runAttemptFence, encodedToken);
    }

    private boolean inProcessDirectAssignment(
            RegistryWorker worker, StageCommand command,
            StageAssignmentToken assignmentToken) {
        return assignmentToken == null && command != null
                && command.portableArtifactBytesUnsafe() == null
                && command.runAttemptFence() != null
                && inProcessLegacyJobs.contains(command.runAttemptFence().runId())
                && inProcessEndpoint(worker);
    }

    private static boolean inProcessEndpoint(RegistryWorker worker) {
        return worker != null
                && io.rowweave.cluster.rpc.InProcessRpcRegistry.isRegistered(
                worker.host(), worker.port());
    }

    private static void requireAssignAck(RpcMessage response, String workerId, long stageId) {
        if (response == null || response.type() != RpcMessage.Type.ACK) {
            String error = response != null ? response.errorMessage() : "null response";
            throw new IllegalStateException("worker " + workerId + " rejected stage " + stageId + ": " + error);
        }
    }

    // Multiplexing is on by default. When it is, and the target worker reported a data port above zero, the
    // endpoint is that host, that port and a routing key; the key travels with the assignment so the
    // downstream registers under it, and the stage is assigned at once because the endpoint is already
    // known. Otherwise the older path binds and waits for a port report.
    private static final boolean MUX_ENABLED = ShuffleTransportConfig.muxEnabled();

    /**
     * Cancels stages whose terminal state is unproven concurrently, grouped by the exact generation of the worker that owns them, against
     * one deadline.
     */
    private StageCancellationCoordinator.Result cancelPendingStages(
            java.util.List<Long> all, long deadlineNanos) {
        java.util.Map<String, java.util.List<StageCancellationCoordinator.CancelCall>> callsByWorker =
                new java.util.LinkedHashMap<>();
        java.util.Map<String, StageOwner> ownerByWorker = new java.util.LinkedHashMap<>();
        for (long id : all) {
            StageTracker t = stageTrackers.get(id);
            if (t != null && t.hasWorkerTerminal()) {
                continue;  // a stage may be skipped only once the worker confirmed its terminal state; a failure this master synthesized still owes the worker a cancellation
            }
            StageOwner owner = stageWorker.get(id);
            if (owner == null) {
                continue;
            }
            if (!stageCancellationLocallyCurrent(owner.workerId, id, owner.endpoint)) {
                continue;  // that exact generation has departed, so a stage of it is never cancelled against a replacement that took the same id
            }
            String workerKey = owner.workerId + '\u0000' + owner.endpoint.host()
                    + '\u0000' + owner.endpoint.port() + '\u0000'
                    + owner.endpoint.incarnationId();
            ownerByWorker.putIfAbsent(workerKey, owner);
            callsByWorker.computeIfAbsent(workerKey, ignored -> new java.util.ArrayList<>())
                    .add(() -> cancelExactStage(id, owner));
        }

        java.util.List<StageCancellationCoordinator.WorkerBatch> batches =
                new java.util.ArrayList<>(callsByWorker.size());
        for (java.util.Map.Entry<String, java.util.List<StageCancellationCoordinator.CancelCall>>
                entry : callsByWorker.entrySet()) {
            StageOwner owner = ownerByWorker.get(entry.getKey());
            batches.add(StageCancellationCoordinator.WorkerBatch.of(
                    entry.getKey(), entry.getValue(), failure -> {
                        boolean quarantined = quarantineLocalWorkerGeneration(
                                owner.workerId, owner.endpoint);
                        LOG.warn("cancel confirmation incomplete; {} worker={} incarnation={}",
                                quarantined ? "worker quarantined" : "stale generation ignored",
                                owner.workerId, owner.endpoint.incarnationId(), failure);
                    }));
        }
        try {
            return cancellationCoordinator.cancel(batches, deadlineNanos);
        } catch (IllegalArgumentException capacityFailure) {
            for (StageOwner owner : ownerByWorker.values()) {
                quarantineLocalWorkerGeneration(owner.workerId, owner.endpoint);
            }
            LOG.warn("stage cancellation rejected before dispatch; workers={} stages={}",
                    ownerByWorker.size(), all.size(), capacityFailure);
            return StageCancellationCoordinator.Result.incompleteBeforeDispatch(
                    all.size(), ownerByWorker.size());
        }
    }

    private void cancelExactStage(long stageId, StageOwner owner) {
        if (!stageCancellationLocallyCurrent(
                owner.workerId, stageId, owner.endpoint)) {
            throw new IllegalStateException("stage cancellation owner changed");
        }
        RpcMessage response = secureInbound(rpcCache.call(
                owner.endpoint.host(), owner.endpoint.port(), secureOutbound(
                        RpcMessage.cancelStage(owner.workerId, stageId,
                                owner.endpoint.incarnationId(), owner.runAttemptFence)),
                () -> stageAssignmentCurrent(
                        owner.workerId, stageId, owner.endpoint)));
        requireCancelAck(response, owner.workerId, stageId);
    }

    private boolean stageCancellationLocallyCurrent(
            String workerId, long stageId, RegistryWorker endpoint) {
        synchronized (workerLifecycleLock) {
            return stageAssignmentLocallyCurrentLocked(
                    workerId, stageId, endpoint);
        }
    }

    /** Cancellation deadline path: exact local generation fence, with no registry I/O after expiry. */
    private boolean quarantineLocalWorkerGeneration(
            String workerId, RegistryWorker endpoint) {
        synchronized (workerLifecycleLock) {
            WorkerInfo current = workers.get(workerId);
            if (current == null || !sameEndpoint(current, endpoint)) {
                return false;
            }
            quarantineWorkerLocked(workerId, endpoint);
            return true;
        }
    }

    /**
     * An acknowledgement for a cancellation is proof the worker really ended the stage, so a failure cannot be reclaimed as though it had
     * succeeded.
     */
    private static void requireCancelAck(RpcMessage response, String workerId, long stageId) {
        if (response == null || response.type() != RpcMessage.Type.ACK) {
            String error = response != null ? response.errorMessage() : "null response";
            throw new IllegalStateException(
                    "worker " + workerId + " rejected cancellation for stage " + stageId + ": " + error);
        }
    }

    /**
     * A call that failed outside the lock may quarantine a generation only while it is still the current identity; a failure arriving late
     * evicts only the connection it came from.
     */
    private boolean quarantineWorker(String workerId, RegistryWorker endpoint) {
        boolean quarantined = false;
        // A slow read from the registry must not block the point at which a registration, a heartbeat or a
        // departure is linearised.
        RegistryWorker registered = endpointOf(workerId);
        synchronized (workerLifecycleLock) {
            WorkerInfo current = workers.get(workerId);
            boolean localMatches = current == null || sameEndpoint(current, endpoint);
            boolean registryMatches = registered == null || sameEndpoint(registered, endpoint);
            if ((current != null || registered != null) && localMatches && registryMatches) {
                quarantineWorkerLocked(workerId, endpoint);
                quarantined = true;
            }
        }
        if (!quarantined) {
            // When a replacement has already taken over, only retire the previous-generation transport, and
            // never release the new ledger entry by workerId.
            assignmentLanes.invalidate(endpoint.host(), endpoint.port());
            rpcCache.invalidate(endpoint.host(), endpoint.port());
        }
        return quarantined;
    }

    private void quarantineWorkerLocked(String workerId, RegistryWorker endpoint) {
        quarantineWorkerLocked(workerId, endpoint, false);
    }

    private void quarantineWorkerLocked(String workerId, RegistryWorker endpoint, boolean departureFenced) {
        quarantinedWorkers.put(workerId,
                new QuarantinedWorker(endpoint.host(), endpoint.port(), endpoint.slots(), endpoint.dataPort(),
                        endpoint.incarnationId(), departureFenced));
        slotLedger.releaseWorker(workerId);
        assignmentLanes.invalidate(endpoint.host(), endpoint.port());
        rpcCache.invalidate(endpoint.host(), endpoint.port());
    }

    void quarantineWorker(String workerId) {
        RegistryWorker endpoint = endpointOf(workerId);
        if (endpoint == null) {
            throw new IllegalArgumentException("unknown worker " + workerId);
        }
        quarantineWorker(workerId, endpoint);
    }

    boolean workerQuarantined(String workerId) {
        return quarantinedWorkers.containsKey(workerId);
    }

    private static String[] keyedKeyFields(DataSchema childMeta, PlanNode keyed) {
        List<String> keys = keyed.listParam("keys");
        if (!keys.isEmpty()) {
            return keys.toArray(new String[0]);
        }
        String[] all = new String[childMeta.getFieldCount()];
        for (int i = 0; i < all.length; i++) {
            all[i] = childMeta.getField(i).getName();
        }
        return all;
    }

    /**
     * A whole graph on a single worker: the assignment is asynchronous, and the wait for a terminal state is the same one the general DAG
     * uses.
     */
    private RpcMessage runWholeGraphAsync(
            PlanGraph graph, PhysicalPlan physicalPlan, RegistryWorker target,
            ManifestJobCommit manifestCommit,
            RunAttemptFence runAttemptFence,
            DurableStageAssignmentPlan durableAssignments,
            RecoveryToken recoveryToken,
            DurableRunExecution durable,
            RpcMessage residentRequest,
            EphemeralSubmitDeadline ephemeralDeadline,
            byte[] portableArtifactBytes) throws Exception {
        requireActiveLeader();
        if (manifestCommit != null) {
            throw new IllegalStateException(
                    "manifest cluster output requires staged group protocol");
        }
        PlanGraph workerGraph = graph;
        if (durableAssignments != null
                && graph.root().kind() == PlanNode.Kind.SINK
                && graph.root().output()
                instanceof io.rowweave.ManifestFileSink) {
            workerGraph = graph.replaceRootSinkOutput(
                    ((io.rowweave.ManifestFileSink) graph.root().output())
                            .toProviderSink());
        }
        StageCommand wholeGraphCommand = portableArtifactBytes == null
                ? StageCommand.wholeGraph(workerGraph)
                : StageCommand.wholePortableArtifact(portableArtifactBytes);
        if (durableAssignments != null) {
            long plannedStageId = durableAssignments.runtimeStageId(
                    physicalPlan.instances().get(0).logicalStage(), 0);
            wholeGraphCommand.withSourceSplitAssignment(
                    durableAssignments.sourceAssignment(plannedStageId));
            if (wholeGraphCommand.providerOutputProxy() != null) {
                io.rowweave.run.SinkAttemptIdentity identity =
                        durableAssignments.sinkAttemptIdentity(plannedStageId);
                wholeGraphCommand.withDurableSinkContext(
                        identity, durable.publicationId,
                        identity.assignmentGeneration());
            }
        }
        wholeGraphCommand.withRunAttemptFence(runAttemptFence);
        long stageId = durableAssignments == null
                ? stageSeq.incrementAndGet()
                : durableAssignments.runtimeStageId(
                        physicalPlan.instances().get(0).logicalStage(), 0);
        PhysicalPlan.Instance wholeInstance = physicalPlan.instances().get(0);
        StageTracker tracker = new StageTracker(
                wholeInstance.logicalStage(), 0, StageCommand.Role.WHOLE_GRAPH, 1);
        stageTrackers.put(stageId, tracker);
        java.util.List<Long> stageIds = java.util.Collections.singletonList(stageId);
        try {
            StageAssignmentToken assignmentToken = durableAssignments == null
                    ? null : durableAssignments.token(stageId);
            boolean direct = inProcessDirectAssignment(
                    target, wholeGraphCommand, assignmentToken);
            byte[] blob = direct ? null
                    : serialize(wholeGraphCommand, null, residentRequest);
            probeDeadlineBeforeAssignment(
                    runAttemptFence, assignmentToken,
                    durable == null ? null : durable.frontierToken,
                    recoveryToken);
            java.util.concurrent.locks.Lock fence = assignmentLifecycle.readLock();
            fence.lock();
            try {
                requireActiveLeader();
                byte[] commandDigest = assignmentToken == null ? null : sha256(blob);
                RegistryWorker claimed;
                java.util.concurrent.locks.Lock runFence =
                        runDispatchLifecycle.readLock();
                runFence.lock();
                try {
                    requireDurableRunDispatchable(
                            runAttemptFence, assignmentToken,
                            durable == null ? null : durable.frontierToken,
                            recoveryToken);
                    claimed = claimStageAssignment(
                            target.workerId(), stageId, target, runAttemptFence,
                            assignmentToken, commandDigest, recoveryToken,
                            durable == null ? null : durable.frontierToken);
                } finally {
                    runFence.unlock();
                }
                RpcMessage assignment = assignmentMessage(
                        target.workerId(), stageId, blob,
                        direct ? wholeGraphCommand : null,
                        claimed.incarnationId(), runAttemptFence,
                        assignmentToken);
                RpcMessage response = ephemeralDeadline == null
                        ? secureInbound(assignmentLanes.call(
                                claimed.host(), claimed.port(), secureOutbound(assignment),
                                () -> stageAssignmentCurrent(
                                        target.workerId(), stageId, claimed)))
                        : callAssignmentBeforeDeadline(
                                target.workerId(), stageId, claimed, assignment,
                                ephemeralDeadline.executionDeadlineNanos());
                requireAssignAck(response, target.workerId(), stageId);
            } finally {
                fence.unlock();
            }
            RpcMessage result = awaitJob(
                    stageIds, java.util.Collections.singletonList(tracker),
                    java.util.Collections.emptyList(), null, null, durable,
                    ephemeralDeadline, residentRequest);
            if (portableArtifactBytes != null
                    && durable != null && result.success()) {
                durable.markCommitting();
            }
            return result;
        } catch (Throwable failure) {
            // Waiting for the job already covers a terminal state, a failure and an interruption; this converges a
            // serialization or assignment failure raised before entering it, and calling it twice is harmless.
            if (ephemeralDeadline != null
                    && ephemeralDeadline.executionExpired()) {
                RpcMessage timeout = settleEphemeralTimeout(
                        stageIds, java.util.Collections.emptyList(),
                        null, null, ephemeralDeadline);
                cleanupStageTracking(stageIds);
                return timeout;
            }
            cancelPendingStages(stageIds, ephemeralDeadline == null
                    ? deadlineAfterMillis(outputCommitTimeoutMs)
                    : ephemeralDeadline.terminalDeadlineNanos());
            cleanupStageTracking(stageIds);
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            throw (Error) failure;
        }
    }

    /**
     * Turns a stage command into the explicit envelope version 2, so the top level no longer relies on serialization of Java objects.
     */
    byte[] serialize(StageCommand cmd) {
        return serialize(cmd, null, null);
    }

    private byte[] serialize(StageCommand cmd, AssignmentProfile assignmentProfile) {
        return serialize(cmd, assignmentProfile, null);
    }

    private byte[] serialize(StageCommand cmd, AssignmentProfile assignmentProfile,
                             RpcMessage residentRequest) {
        long t = System.nanoTime();
        try {
            java.util.function.LongConsumer reservation = residentRequest == null
                    ? ignored -> { } : residentRequest::reserveResidentBytes;
            return StageCommandCodecV2.encode(cmd, objectTrustContext, reservation);
        } catch (Exception e) {
            throw new RuntimeException("serialize StageCommand failed", e);
        } finally {
            if (assignmentProfile != null) {
                assignmentProfile.serializeNanos.addAndGet(System.nanoTime() - t);
            }
        }
    }

    private void failActiveStageTracking(RuntimeException cause) {
        for (StageTracker tracker : stageTrackers.values()) {
            tracker.fail(cause.getMessage());
        }
        for (StagePortTracker tracker : portTrackers.values()) {
            tracker.fail(cause);
        }
    }

    private int awaitActiveSubmits() {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(closeDrainMs);
        synchronized (submitLifecycle) {
            while (activeSubmits > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    break;
                }
                try {
                    java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(submitLifecycle, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return activeSubmits;
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

    @Override
    public synchronized void close() {
        java.util.concurrent.locks.Lock assignmentFence = assignmentLifecycle.writeLock();
        assignmentFence.lock();
        try {
            // A registration that has entered its critical section is allowed to finish; otherwise the closed flag
            // is re-checked and the registration refused.
            synchronized (workerLifecycleLock) {
                synchronized (submitLifecycle) {
                    if (closed) {
                        return;
                    }
                    closed = true;
                }
            }
        } finally {
            assignmentFence.unlock();
        }

        Throwable failure = null;
        if (artifactJanitor != null) {
            failure = runCloseStep(failure, artifactJanitor::close);
        }
        if (terminalRunJanitor != null) {
            failure = runCloseStep(failure, terminalRunJanitor::close);
        }
        if (recoveryInventory != null) {
            failure = runCloseStep(failure, recoveryInventory::close);
        }
        failure = runCloseStep(failure, () -> stopHeartbeatMonitor(true));
        failure = runCloseStep(failure, () ->
                failActiveStageTracking(new IllegalStateException("master is closing")));
        // Stop accepting new assignments first. An owner already registered still has its connection, so it can
        // send a cancellation and wait for the worker's terminal acknowledgement.
        failure = runCloseStep(failure, assignmentDispatcher::close);
        int remaining = awaitActiveSubmits();
        if (remaining > 0) {
            LOG.warn("master close drain timed out after {}ms: activeSubmits={} trackers={} ports={} assignments={}",
                    closeDrainMs, remaining, stageTrackers.size(), portTrackers.size(), stageWorker.size());
        }
        failure = runCloseStep(failure, cancellationCoordinator::close);
        failure = runCloseStep(failure, assignmentLanes::close);
        failure = runCloseStep(failure, rpcCache::shutdown);
        failure = runCloseStep(failure, rpcEndpoint::close);
        if (ownsDeploymentRuntime) {
            failure = runCloseStep(failure, deploymentRuntime::close);
        }
        if (failure != null) {
            rethrowCloseFailure(failure);
        }
    }
}
