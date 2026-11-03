package io.rowweave;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.registry.ServiceRegistryBootstrap;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ControlSecurityProfile;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.stream.api.StreamClusterConfig;
import io.rowweave.stream.api.StreamWorkerEndpoint;
import io.rowweave.stream.cluster.RegistryStreamWorkerDirectory;
import io.rowweave.stream.cluster.StreamProductJobStateStore;
import io.rowweave.stream.cluster.StreamProductJobStateStoreProvider;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamSecretResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owned Master-only, Worker-only or combined Stream cluster process roles. */
public final class StreamClusterRuntime implements AutoCloseable {
    private final StreamMasterRuntime master;
    private final List<StreamWorkerRuntime> workers;
    private final List<StreamWorkerRegistryLease> leases;
    private final ServiceRegistry masterRegistry;
    private final ServiceRegistry workerRegistry;
    private final ControlSecurityProfile securityProfile;
    private final DeploymentRuntime deploymentRuntime;
    private final AtomicBoolean closed = new AtomicBoolean();

    private StreamClusterRuntime(StreamMasterRuntime master,
            List<StreamWorkerRuntime> workers,
            List<StreamWorkerRegistryLease> leases,
            ServiceRegistry masterRegistry, ServiceRegistry workerRegistry,
            ControlSecurityProfile securityProfile,
            DeploymentRuntime deploymentRuntime) {
        this.master = master;
        this.workers = workers;
        this.leases = leases;
        this.masterRegistry = masterRegistry;
        this.workerRegistry = workerRegistry;
        this.securityProfile = securityProfile;
        this.deploymentRuntime = deploymentRuntime;
    }

    public static StreamClusterRuntime start(StreamClusterConfig config,
            Collection<? extends StreamSourceProvider> sourceProviders,
            Collection<? extends StreamSinkProvider> sinkProviders,
            Collection<? extends StreamTransactionalSinkProvider>
                    transactionalSinkProviders,
            Collection<? extends StreamFunctionProvider> functionProviders) {
        if (config == null || sourceProviders == null || sinkProviders == null
                || transactionalSinkProviders == null
                || functionProviders == null) {
            throw new IllegalArgumentException(
                    "Stream cluster configuration and providers are required");
        }
        return start(config, sourceProviders, sinkProviders,
                transactionalSinkProviders, functionProviders, null, null);
    }

    /** Takes ownership of one already verified signed deployment runtime. */
    static StreamClusterRuntime startSigned(StreamClusterConfig config,
            DeploymentRuntime deploymentRuntime,
            StreamSecretResolver secretResolver) {
        if (config == null || deploymentRuntime == null) {
            throw new IllegalArgumentException(
                    "Stream cluster configuration and deployment runtime are required");
        }
        return start(config,
                Collections.<StreamSourceProvider>emptyList(),
                Collections.<StreamSinkProvider>emptyList(),
                Collections.<StreamTransactionalSinkProvider>emptyList(),
                Collections.<StreamFunctionProvider>emptyList(),
                deploymentRuntime, secretResolver);
    }

    private static StreamClusterRuntime start(StreamClusterConfig config,
            Collection<? extends StreamSourceProvider> sourceProviders,
            Collection<? extends StreamSinkProvider> sinkProviders,
            Collection<? extends StreamTransactionalSinkProvider>
                    transactionalSinkProviders,
            Collection<? extends StreamFunctionProvider> functionProviders,
            DeploymentRuntime deploymentRuntime,
            StreamSecretResolver secretResolver) {
        ControlSecurityConfig security = deploymentRuntime == null
                ? config.loadControlSecurity()
                : deploymentRuntime.controlSecurity();
        StreamConnectorProviderRegistry connectorRegistry =
                deploymentRuntime == null ? null
                        : deploymentRuntime.streamConnectorProviderRegistry();
        ServiceRegistry masterRegistry = null;
        ServiceRegistry workerRegistry = null;
        List<StreamWorkerRuntime> workers = new ArrayList<>();
        List<StreamWorkerRegistryLease> leases = new ArrayList<>();
        StreamMasterRuntime master = null;
        try {
            if (config.zkEnabled()) {
                masterRegistry = config.masterEnabled()
                        ? ServiceRegistryBootstrap.create(
                        config.registryKind(), config.zkConnectString()) : null;
                workerRegistry = config.workerEnabled()
                        ? ServiceRegistryBootstrap.create(
                        config.registryKind(), config.zkConnectString()) : null;
            } else if (config.masterEnabled() && config.workerEnabled()) {
                masterRegistry = new InProcessRegistry();
                workerRegistry = masterRegistry;
            }
            if (config.workerEnabled()) {
                io.rowweave.stream.api.StreamWorkerConfig workerConfig =
                        config.workerConfig().withControlEndpoint(
                                security, config.workerPort());
                StreamWorkerRuntime worker = deploymentRuntime == null
                        ? RowWeave.startStreamWorker(workerConfig,
                        sourceProviders, sinkProviders,
                        transactionalSinkProviders, functionProviders)
                        : deploymentRuntime.startStreamWorker(
                        workerConfig, secretResolver);
                workers.add(worker);
                if (workerRegistry != null) {
                    leases.add(worker.register(workerRegistry));
                }
            }
            if (config.masterEnabled()) {
                if (masterRegistry != null) {
                    RegistryStreamWorkerDirectory directory =
                            new RegistryStreamWorkerDirectory(
                                    masterRegistry, security);
                    if (config.zkEnabled()) {
                        if (!(masterRegistry instanceof
                                StreamProductJobStateStoreProvider)) {
                            throw new IllegalStateException(
                                    "external Stream registry lacks product authority");
                        }
                        io.rowweave.artifact.ArtifactRepository artifacts =
                                new io.rowweave.artifact.FileArtifactRepository(
                                        config.artifactRoot());
                        StreamProductJobStateStore productJobs =
                                ((StreamProductJobStateStoreProvider)
                                        masterRegistry)
                                        .streamProductJobStateStore(artifacts);
                        master = StreamMasterRuntime.startHa(
                                config.stateRoot().resolve("master"),
                                directory, security, config.masterPort(),
                                masterRegistry, productJobs,
                                config.artifactRoot().resolve(
                                        "stream-checkpoints"),
                                connectorRegistry);
                    } else {
                        master = StreamMasterRuntime.start(
                                config.stateRoot().resolve("master"),
                                directory, security, config.masterPort(),
                                connectorRegistry);
                    }
                } else {
                    master = StreamMasterRuntime.start(
                            config.stateRoot().resolve("master"),
                            config.fixedWorkers(), security,
                            config.masterPort(), connectorRegistry);
                }
            }
            return new StreamClusterRuntime(master,
                    Collections.unmodifiableList(workers),
                    Collections.unmodifiableList(leases), masterRegistry,
                    workerRegistry, config.controlEndpoint().profile(),
                    deploymentRuntime);
        } catch (RuntimeException | Error failure) {
            closeAll(master, leases, workers, masterRegistry, workerRegistry,
                    deploymentRuntime, failure);
            throw failure;
        }
    }

    public List<StreamWorkerEndpoint> workerEndpoints() {
        requireOpen();
        List<StreamWorkerEndpoint> result = new ArrayList<>();
        for (StreamWorkerRuntime worker : workers) {
            result.add(worker.endpoint());
        }
        return Collections.unmodifiableList(result);
    }

    public StreamWorkerEndpoint masterEndpoint() {
        requireOpen();
        if (master == null) {
            throw new IllegalStateException("Stream Master role is disabled");
        }
        return new StreamWorkerEndpoint(master.host(), master.port());
    }

    public StreamJobClient createLoopbackClient() {
        requireOpen();
        if (master == null) {
            throw new IllegalStateException("Stream Master role is disabled");
        }
        if (securityProfile != ControlSecurityProfile.LOOPBACK) {
            throw new IllegalStateException(
                    "mTLS cluster clients require an independent CLIENT identity");
        }
        return master.createJobClient();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Throwable failure = closeAll(master, leases, workers,
                masterRegistry, workerRegistry, deploymentRuntime, null);
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "Stream cluster shutdown failed", failure);
        }
    }

    private void requireOpen() {
        if (closed.get()) throw new IllegalStateException(
                "Stream cluster runtime is closed");
    }

    private static Throwable closeAll(StreamMasterRuntime master,
            List<StreamWorkerRegistryLease> leases,
            List<StreamWorkerRuntime> workers,
            ServiceRegistry masterRegistry, ServiceRegistry workerRegistry,
            DeploymentRuntime deploymentRuntime,
            Throwable primary) {
        Throwable result = primary;
        result = close(master, result);
        for (int index = leases.size() - 1; index >= 0; index--) {
            result = close(leases.get(index), result);
        }
        for (int index = workers.size() - 1; index >= 0; index--) {
            result = close(workers.get(index), result);
        }
        if (workerRegistry != masterRegistry) {
            result = close(workerRegistry, result);
        }
        result = close(masterRegistry, result);
        return close(deploymentRuntime, result);
    }

    private static Throwable close(Object value, Throwable primary) {
        if (!(value instanceof AutoCloseable)) return primary;
        try {
            ((AutoCloseable) value).close();
            return primary;
        } catch (Throwable failure) {
            if (primary == null) return failure;
            if (primary != failure) primary.addSuppressed(failure);
            return primary;
        }
    }
}
