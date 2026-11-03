package io.rowweave;

/**
 * The entry factory of the RowWeave engine: it creates an {@link ExecutionEnvironment}.
 */
public final class RowWeave {

    private RowWeave() {
    }

    public static ExecutionEnvironment createEnvironment(String name) {
        return new ExecutionEnvironment(name, name);
    }

    public static ExecutionEnvironment createEnvironment(String jobName, String jobTitle) {
        return new ExecutionEnvironment(jobName, jobTitle);
    }

    public static StreamSession createStreamSession(String name) {
        return new StreamSession(name, name);
    }

    public static StreamSession createStreamSession(String jobName, String jobTitle) {
        return new StreamSession(jobName, jobTitle);
    }

    public static StreamSession createStreamSession(
            String jobName, String jobTitle, StreamJobClient client) {
        if (client == null) throw new IllegalArgumentException("StreamJobClient is required");
        return new StreamSession(jobName, jobTitle, client);
    }

    /** Creates the cluster-first Stream product client for a Stream Master endpoint. */
    public static StreamJobClient createRealClusterStreamJobClient(
            String masterHost, int masterPort) {
        return new io.rowweave.monitor.MonitoredStreamJobClient(
                new io.rowweave.stream.cluster.RealClusterStreamJobClient(
                        masterHost, masterPort),
                io.rowweave.monitor.MonitorRuntime.shared());
    }

    /** Creates a Stream client using an explicit LOOPBACK or mTLS identity. */
    public static StreamJobClient createRealClusterStreamJobClient(
            String masterHost, int masterPort,
            io.rowweave.cluster.rpc.ControlSecurityConfig security) {
        return new io.rowweave.monitor.MonitoredStreamJobClient(
                new io.rowweave.stream.cluster.RealClusterStreamJobClient(
                        masterHost, masterPort, security),
                io.rowweave.monitor.MonitorRuntime.shared());
    }

    /** Creates an HA Stream client that discovers the active Master per call. */
    public static StreamJobClient createRegistryStreamJobClient(
            io.rowweave.cluster.registry.ServiceRegistry registry,
            io.rowweave.cluster.rpc.ControlSecurityConfig security) {
        return new io.rowweave.monitor.MonitoredStreamJobClient(
                new io.rowweave.stream.cluster.RealClusterStreamJobClient(
                        registry, security),
                io.rowweave.monitor.MonitorRuntime.shared());
    }

    /** Starts one independent Stream Worker from public SDKs. */
    public static StreamWorkerRuntime startStreamWorker(
            io.rowweave.stream.api.StreamWorkerConfig config,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamSourceProvider> sourceProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamSinkProvider> eagerSinkProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamTransactionalSinkProvider>
                    transactionalSinkProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamFunctionProvider>
                    functionProviders) {
        return StreamWorkerRuntime.start(config, sourceProviders,
                eagerSinkProviders, transactionalSinkProviders,
                functionProviders);
    }

    /** Starts a signed Stream Worker; the caller retains deployment-set ownership. */
    public static StreamWorkerRuntime startStreamWorker(
            io.rowweave.stream.api.StreamWorkerConfig config,
            io.rowweave.deployment.VerifiedDeploymentSet deployments) {
        return StreamWorkerRuntime.startSigned(config, deployments);
    }

    /** Injects a Worker-local secret resolver for signed connector opens. */
    public static StreamWorkerRuntime startStreamWorker(
            io.rowweave.stream.api.StreamWorkerConfig config,
            io.rowweave.deployment.VerifiedDeploymentSet deployments,
            io.rowweave.stream.spi.StreamSecretResolver resolver) {
        if (resolver == null) {
            throw new IllegalArgumentException(
                    "Stream secret resolver is required");
        }
        return StreamWorkerRuntime.startSigned(config, deployments, resolver);
    }

    /** Starts one active Stream Master for a durable cluster root. */
    public static StreamMasterRuntime startStreamMaster(
            java.nio.file.Path clusterStateRoot,
            java.util.Collection<io.rowweave.stream.api.StreamWorkerEndpoint>
                    workers) {
        return StreamMasterRuntime.start(clusterStateRoot, workers);
    }

    /** Starts a Stream Master on an explicit LOOPBACK or mTLS endpoint. */
    public static StreamMasterRuntime startStreamMaster(
            java.nio.file.Path clusterStateRoot,
            java.util.Collection<io.rowweave.stream.api.StreamWorkerEndpoint>
                    workers,
            io.rowweave.cluster.rpc.ControlSecurityConfig security,
            int port) {
        return StreamMasterRuntime.start(
                clusterStateRoot, workers, security, port);
    }

    /** Starts a Master with metadata-only admission from caller-owned signed deployments. */
    public static StreamMasterRuntime startStreamMaster(
            java.nio.file.Path clusterStateRoot,
            java.util.Collection<io.rowweave.stream.api.StreamWorkerEndpoint>
                    workers,
            io.rowweave.cluster.rpc.ControlSecurityConfig security,
            int port,
            io.rowweave.deployment.VerifiedDeploymentSet deployments) {
        return StreamMasterRuntime.start(
                clusterStateRoot, workers, security, port, deployments);
    }

    /** Starts a Stream Master whose placement reads a live Worker directory. */
    static StreamMasterRuntime startStreamMaster(
            java.nio.file.Path clusterStateRoot,
            io.rowweave.stream.cluster.StreamWorkerDirectory workers,
            io.rowweave.cluster.rpc.ControlSecurityConfig security,
            int port) {
        return StreamMasterRuntime.start(
                clusterStateRoot, workers, security, port);
    }

    /** Starts a Stream Master whose admission follows the named registry role. */
    static StreamMasterRuntime startHaStreamMaster(
            java.nio.file.Path clusterStateRoot,
            io.rowweave.stream.cluster.StreamWorkerDirectory workers,
            io.rowweave.cluster.rpc.ControlSecurityConfig security,
            int port,
            io.rowweave.cluster.registry.ServiceRegistry electionRegistry) {
        return StreamMasterRuntime.startHa(clusterStateRoot, workers,
                security, port, electionRegistry);
    }

    /** Starts an HA Stream Master with a shared durable product authority. */
    static StreamMasterRuntime startHaStreamMaster(
            java.nio.file.Path clusterStateRoot,
            io.rowweave.stream.cluster.StreamWorkerDirectory workers,
            io.rowweave.cluster.rpc.ControlSecurityConfig security,
            int port,
            io.rowweave.cluster.registry.ServiceRegistry electionRegistry,
            io.rowweave.stream.cluster.StreamProductJobStateStore
                    productJobStateStore) {
        return StreamMasterRuntime.startHa(clusterStateRoot, workers,
                security, port, electionRegistry, productJobStateStore);
    }

    /** Starts an HA Stream Master with shared metadata and payload storage. */
    static StreamMasterRuntime startHaStreamMaster(
            java.nio.file.Path clusterStateRoot,
            io.rowweave.stream.cluster.StreamWorkerDirectory workers,
            io.rowweave.cluster.rpc.ControlSecurityConfig security,
            int port,
            io.rowweave.cluster.registry.ServiceRegistry electionRegistry,
            io.rowweave.stream.cluster.StreamProductJobStateStore
                    productJobStateStore,
            java.nio.file.Path sharedCheckpointRoot) {
        return StreamMasterRuntime.startHa(clusterStateRoot, workers,
                security, port, electionRegistry, productJobStateStore,
                sharedCheckpointRoot);
    }

    /** Creates an owned no-listener Local Stream runtime using product roles. */
    public static StreamJobClient createLocalStreamJobClient(
            java.nio.file.Path stateRoot,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamSourceProvider> sourceProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamSinkProvider> sinkProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamFunctionProvider> functionProviders) {
        return new io.rowweave.monitor.MonitoredStreamJobClient(
                io.rowweave.stream.cluster.LocalStreamRuntimeFactory.create(
                        stateRoot, sourceProviders, sinkProviders,
                        functionProviders),
                io.rowweave.monitor.MonitorRuntime.shared());
    }

    /** Creates Local Stream with both eager and checkpoint-transactional Sinks. */
    public static StreamJobClient createLocalStreamJobClient(
            java.nio.file.Path stateRoot,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamSourceProvider> sourceProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamSinkProvider> eagerSinkProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamTransactionalSinkProvider>
                    transactionalSinkProviders,
            java.util.Collection<? extends
                    io.rowweave.stream.spi.StreamFunctionProvider> functionProviders) {
        return new io.rowweave.monitor.MonitoredStreamJobClient(
                io.rowweave.stream.cluster.LocalStreamRuntimeFactory.create(
                        stateRoot, sourceProviders, eagerSinkProviders,
                        transactionalSinkProviders, functionProviders),
                io.rowweave.monitor.MonitorRuntime.shared());
    }
}
