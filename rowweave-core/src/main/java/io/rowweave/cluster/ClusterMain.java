package io.rowweave.cluster;

import io.rowweave.mem.GlobalMemoryPool;
import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The single process entry point for a cluster node. It reads {@link ClusterConfig} from system
 * properties to decide which roles to start, brings up a MasterServer, a WorkerServer or both,
 * configures memory, prints a readiness line on stdout so whoever launched the process can confirm
 * it, and then stays alive until it is killed.
 *
 * <p>A standalone process must declare the full envelope of heap, direct, metaspace, code cache,
 * stack, CPU and process limits. A command that sets the role properties without that envelope is
 * refused before any role resource exists. There is no CLI library here, only system properties. The
 * readiness line is {@code MASTER_PORT=<port>} for a master and {@code WORKER_READY=<id> port=<port>}
 * for a worker, and the launcher parses it rather than guessing with a sleep.
 */
public final class ClusterMain {

    private ClusterMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        // Must be the first statement before any RowWeave configuration or TLS context; atomically freezes the TLS and JVM capacity proof.
        ProcessBootstrapEnvelope processEnvelope =
                TransportSecurityBootstrap.bootstrapProcess();
        ClusterConfig cfg = ClusterConfig.fromSystemProperties(processEnvelope);
        if (!cfg.masterEnabled() && !cfg.workerEnabled()) {
            log().error("ClusterMain: set -Drowweave.master.enable=true or -Drowweave.worker.enable=true");
            System.exit(2);
            return;
        }
        StartupCleanup cleanup = new StartupCleanup();
        try {
            // The capacity gate has passed, so deployment material may be read and probe threads
            // created from here on.
            io.rowweave.deployment.DeploymentRuntime deploymentRuntime =
                    cfg.loadDeploymentRuntime(processEnvelope.tlsEnvelope());
            cleanup.push(deploymentRuntime::close);
            // MTLS never falls back to plaintext, and this is asserted before the memory pool, the
            // artifact repository and the registry session, so no role resource is ever created
            // against a transport that was supposed to be secure.
            deploymentRuntime.controlSecurity().requireImplementedTransport();
            run(cfg, deploymentRuntime, cleanup);
        } catch (InterruptedException interrupted) {
            cleanup.closeInto(interrupted);
            throw interrupted;
        } catch (RuntimeException | Error failure) {
            cleanup.closeInto(failure);
            throw failure;
        }
    }

    private static void run(ClusterConfig cfg,
                            io.rowweave.deployment.DeploymentRuntime deploymentRuntime,
                            StartupCleanup cleanup)
            throws InterruptedException {

        // The Worker uses only the frozen memory/slots already validated against the master ledger, and never re-reads a mutable System Property.
        if (cfg.workerEnabled()) {
            io.rowweave.mem.WorkerConfig.configureProcessSnapshot(
                    cfg.memoryBytes(), cfg.workerSlots(),
                    cfg.workerMaxStageThreads());
            GlobalMemoryPool.configure(cfg.memoryBytes());
        }

        // The master collects run state into an InMemoryJobMonitor, which the monitor HTTP service in
        // the same process reads in order to render it.
        final io.rowweave.monitor.InMemoryJobMonitor jobMonitor =
                new io.rowweave.monitor.InMemoryJobMonitor(
                        Integer.getInteger("rowweave.monitor.history", 200));
        // The registry is chosen by rowweave.registry. The default, inprocess, is a single master with
        // no failover; zk gives several masters high availability. Core does not depend on an
        // implementation: the registry is found through the ServiceRegistryBootstrap SPI. The file
        // repository is initialised before a registry that may already own background threads, so a
        // repository that fails to configure cannot leak a registry session.
        final io.rowweave.artifact.ArtifactRepository configuredArtifactRepository =
                cfg.masterEnabled() ? masterArtifactRepository(cfg) : null;
        final io.rowweave.cluster.registry.ServiceRegistry masterRegistry = cfg.masterEnabled()
                ? io.rowweave.cluster.registry.ServiceRegistryBootstrap.create(
                        cfg.registryKind(), cfg.zkConnectString())
                : null;
        if (masterRegistry != null) cleanup.push(() -> closeRegistry(masterRegistry));
        // MasterServer holds no parallelism: it travels with the submitted job graph and is unrelated
        // to worker slots. Passing worker slots to the master as a parallelism was a category error and
        // is gone; slots configure only the WorkerServer below.
        final MasterServer master = !cfg.masterEnabled() ? null
                : new MasterServer(
                        cfg.heartbeatStalenessMs(), cfg.masterPort(), masterRegistry, jobMonitor,
                        configuredArtifactRepository, deploymentRuntime);
        if (master != null) cleanup.push(master::close);
        // With a registry the master joins the election and publishes its endpoint, so workers and
        // clients resolve the current leader through RegistryMasterLocator and follow a failover.
        if (master != null) {
            if (cfg.zkEnabled()) {
                MasterElection election = new MasterElection(masterRegistry,
                        new Endpoint(deploymentRuntime.controlSecurity().advertisedHost(),
                                master.port()),
                        master.securityMetadata());
                cleanup.push(election::close);
                master.bindLeadership(election);
                election.start();
                log().info("master joined election (registry={}, leader={})", cfg.registryKind(), election.isLeader());
            }
            startFixedMasterLeadershipServices(master, cfg);
            // Readiness protocol: MASTER_PORT= must go to stdout, because the launcher parses it line
            // by line. Readiness markers must stay independent of the application logger.
            System.out.println("MASTER_PORT=" + master.port());
            System.out.flush();
            log().info("master started port={}", master.port());
        }

        // The monitor HTTP service is optional and starts only when the rowweave-monitor jar is on the
        // classpath; it is discovered through an SPI, so core does not depend on it. Enable it with
        // -Drowweave.monitor.enable=true and choose the port with rowweave.monitor.port, where the
        // default 0 means an ephemeral one. With no leader resolver the local path does not redirect.
        if (master != null && Boolean.getBoolean("rowweave.monitor.enable")) {
            io.rowweave.monitor.MonitorLauncher launcher = io.rowweave.monitor.MonitorBootstrap.discover();
            if (launcher != null) {
                cleanup.push(launcher::stop);
                int mport = launcher.start(jobMonitor,
                        Integer.getInteger("rowweave.monitor.port", 0), null,
                        cfg.monitorIoThreads());
                System.out.println("MONITOR_PORT=" + mport);
                System.out.flush();
                log().info("monitor HTTP started port={}", mport);
            } else {
                log().warn("rowweave.monitor.enable=true but no MonitorLauncher implementation is on the classpath; monitoring is disabled");
            }
        }

        // With a registry the worker keeps a session of its own: it uses it to find the leader through
        // RegistryMasterLocator, and WorkerServer holds its own ephemeral resource lease in it rather
        // than letting the master's session write it on the worker's behalf.
        if (cfg.workerEnabled()) {
            String workerId = "w-" + Long.toHexString(System.nanoTime());
            File tmp = new File(System.getProperty("java.io.tmpdir"),
                    "rowweave-worker-" + workerId);
            tmp.mkdirs();
            // With a registry the worker always resolves the current leader through it. Even when this
            // process also runs a master, that master may be a standby, and registering with it directly
            // would register with someone who is not the leader. The worker's registry is a second
            // session against the same ensemble; without a registry it uses the local master or the
            // remote one it was configured with.
            final io.rowweave.cluster.registry.ServiceRegistry workerRegistry = cfg.zkEnabled()
                    ? io.rowweave.cluster.registry.ServiceRegistryBootstrap.create(
                            cfg.registryKind(), cfg.zkConnectString())
                    : null;
            if (workerRegistry != null) {
                cleanup.push(() -> closeRegistry(workerRegistry));
            }
            MasterLocator workerLocator = workerMasterLocator(
                    cfg, workerRegistry, master != null ? master.port() : -1,
                    deploymentRuntime.controlSecurity().securityMetadata());
            WorkerServer worker = new WorkerServer(
                    workerId, cfg.workerSlots(), workerLocator, tmp,
                    deploymentRuntime, cfg.workerMaxStageThreads());
            cleanup.push(worker::close);
            worker.startHeartbeat(Math.max(1000, cfg.heartbeatStalenessMs() / 3));  // heartbeat runs on its own schedule once started
            System.out.println("WORKER_READY=" + workerId + " port=" + worker.port());
            System.out.flush();
        }

        // Graceful shutdown: on SIGTERM or Ctrl-C the roles are closed and their sockets, threads and
        // registry connections released.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Throwable closeFailure = cleanup.closeInto(null);
            if (closeFailure != null) {
                closeFailure.printStackTrace(System.err);
            }
            System.out.println("SHUTDOWN");
            System.out.flush();
        }, "rowweave-cluster-shutdown"));

        // Stay alive until the process is killed; the shutdown hook does the graceful part.
        new CountDownLatch(1).await();
    }

    /**
     * Decides which {@link MasterLocator} a worker uses, kept apart so the decision is testable.
     * <ul>
     * <li><b>With a registry</b>: always {@link RegistryMasterLocator}, which finds the current leader
     * in the registry. This holds even when the process has a master of its own, because that master
     * may be a standby and registering with it directly would register with a follower.</li>
     * <li><b>Without a registry</b>: when this process has a master, the worker connects to it on
     * {@code 127.0.0.1} at that port; a worker alone connects to the remote host and port named by
     * the worker master properties.</li>
     * </ul>
     *
     * @param workerRegistry the worker's own registry session, when a registry is in use; null
     *                       otherwise.
     * @param localMasterPort the port of the master in this process, or -1 when there is none.
     */
    static MasterLocator workerMasterLocator(ClusterConfig cfg,
            io.rowweave.cluster.registry.ServiceRegistry workerRegistry,
            int localMasterPort,
            io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata) {
        if (cfg.zkEnabled()) {
            return new RegistryMasterLocator(workerRegistry,
                    securityMetadata);
        }
        String host = localMasterPort >= 0 ? "127.0.0.1" : cfg.masterHost();
        int port = localMasterPort >= 0 ? localMasterPort : cfg.workerMasterPort();
        return new FixedMasterLocator(new Endpoint(host, port));
    }

    /**
     * A production master with no registry takes fixed leadership explicitly; with a registry the
     * leadership services may start only once the election has been acquired.
     */
    static void startFixedMasterLeadershipServices(MasterServer master,
                                                   ClusterConfig cfg) {
        if (master == null || cfg == null) {
            throw new IllegalArgumentException("master and config are required");
        }
        if (!cfg.zkEnabled()) {
            master.startFixedLeadershipServices();
        }
    }

    /**
     * Large artifacts never go into the registry. A production master builds its durable repository
     * only from a directory that was explicitly shared.
     */
    static io.rowweave.artifact.ArtifactRepository masterArtifactRepository(ClusterConfig cfg) {
        String directory = cfg.masterArtifactDirectory();
        return directory == null ? null
                : new io.rowweave.artifact.FileArtifactRepository(
                        java.nio.file.Paths.get(directory));
    }

    /**
     * A ServiceRegistry may be AutoCloseable, as the ZooKeeper implementation is; the in-process
     * registry is not, so closing it is skipped.
     */
    private static void closeRegistry(
            io.rowweave.cluster.registry.ServiceRegistry registry) throws Exception {
        if (registry instanceof AutoCloseable) {
            ((AutoCloseable) registry).close();
        }
    }

    @FunctionalInterface
    private interface CloseAction {
        void close() throws Exception;
    }

    /**
     * Every resource created during startup is pushed onto this stack, so a failure while starting and
     * a shutdown later reach the same end state by unwinding in reverse.
     */
    private static final class StartupCleanup {
        private final Deque<CloseAction> actions = new ArrayDeque<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        private synchronized void push(CloseAction action) {
            if (closed.get()) {
                throw new IllegalStateException("cluster startup cleanup is already closed");
            }
            actions.push(action);
        }

        private synchronized Throwable closeInto(Throwable primary) {
            if (!closed.compareAndSet(false, true)) return primary;
            Throwable result = primary;
            while (!actions.isEmpty()) {
                try {
                    actions.pop().close();
                } catch (Throwable closeFailure) {
                    if (result == null) result = closeFailure;
                    else if (result != closeFailure) result.addSuppressed(closeFailure);
                }
            }
            return result;
        }
    }

    /**
     * The logging implementation and any provider it loads are initialised only after the TLS
     * bootstrap, so a rejected envelope cannot leave a logging backend behind.
     */
    private static org.slf4j.Logger log() {
        return LoggerHolder.LOG;
    }

    private static final class LoggerHolder {
        private static final org.slf4j.Logger LOG =
                org.slf4j.LoggerFactory.getLogger(ClusterMain.class);
    }
}
