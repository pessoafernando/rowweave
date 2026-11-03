package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.api.StreamWorkerEndpoint;
import io.rowweave.stream.cluster.StreamRemoteWorkerServer;
import io.rowweave.stream.cluster.StreamWorkerCapabilityProfile;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.PublicStreamSinkAdapter;
import io.rowweave.stream.runtime.PublicStreamSourceAdapter;
import io.rowweave.stream.runtime.PublicTransactionalStreamSinkAdapter;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSecretResolver;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.stream.cluster.RegistryStreamWorkerDirectory;

/** Owned public lifecycle for one independently deployed Stream Worker. */
public final class StreamWorkerRuntime implements AutoCloseable {
    private final StreamRemoteWorkerServer delegate;
    private final StreamWorkerEndpoint endpoint;
    private final StreamWorkerCapabilityProfile profile;
    private final io.rowweave.cluster.rpc.ControlSecurityConfig security;
    private final AtomicBoolean closed = new AtomicBoolean();

    private StreamWorkerRuntime(StreamRemoteWorkerServer delegate,
            String advertisedHost, StreamWorkerCapabilityProfile profile,
            io.rowweave.cluster.rpc.ControlSecurityConfig security) {
        this.delegate = delegate;
        this.endpoint = new StreamWorkerEndpoint(
                advertisedHost, delegate.port());
        this.profile = profile;
        this.security = security;
    }

    static StreamWorkerRuntime start(StreamWorkerConfig config,
            Collection<? extends StreamSourceProvider> sourceProviders,
            Collection<? extends StreamSinkProvider> eagerSinkProviders,
            Collection<? extends io.rowweave.stream.spi
                    .StreamTransactionalSinkProvider>
                    transactionalSinkProviders,
            Collection<? extends StreamFunctionProvider> functionProviders) {
        if (config == null || sourceProviders == null
                || eagerSinkProviders == null
                || transactionalSinkProviders == null
                || functionProviders == null) {
            throw new IllegalArgumentException(
                    "Stream Worker configuration and providers are required");
        }
        config.controlSecurity().requireImplementedTransport();
        config.controlSecurity().requireLocalRole(
                io.rowweave.cluster.rpc.ControlRole.WORKER);
        Map<String, DurableStreamSourceProvider> sources =
                new LinkedHashMap<>();
        for (StreamSourceProvider provider : sourceProviders) {
            DurableStreamSourceProvider adapted =
                    PublicStreamSourceAdapter.adapt(provider);
            putUnique(sources, adapted.providerKey(), adapted, "source");
        }
        Map<String, EagerStreamSinkProvider> eagerSinks =
                new LinkedHashMap<>();
        for (StreamSinkProvider provider : eagerSinkProviders) {
            EagerStreamSinkProvider adapted =
                    PublicStreamSinkAdapter.adapt(provider);
            putUnique(eagerSinks, adapted.providerKey(), adapted, "Sink");
        }
        Map<String, TransactionalStreamSinkProvider> transactionalSinks =
                new LinkedHashMap<>();
        for (io.rowweave.stream.spi.StreamTransactionalSinkProvider provider
                : transactionalSinkProviders) {
            TransactionalStreamSinkProvider adapted =
                    PublicTransactionalStreamSinkAdapter.adapt(provider);
            if (eagerSinks.containsKey(adapted.providerKey())) {
                throw new IllegalArgumentException(
                        "Stream Worker Sink provider key is duplicated");
            }
            putUnique(transactionalSinks, adapted.providerKey(), adapted,
                    "Sink");
        }
        Map<String, StreamFunctionProvider> functions =
                new LinkedHashMap<>();
        for (StreamFunctionProvider provider : functionProviders) {
            if (provider == null || provider.deploymentSetId() == null
                    || provider.deploymentSetId().isEmpty()
                    || functions.put(provider.deploymentSetId(), provider)
                    != null) {
                throw new IllegalArgumentException(
                        "Stream Worker function deployment is invalid or duplicated");
            }
        }
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                config.workerId(), UUID.randomUUID().toString(), 3, 1,
                capabilities(functions.keySet()), config.taskSlots(),
                config.channelCapacity(), config.maximumFrameBytes(),
                config.maximumResidentBytes(), config.maximumThreads(),
                config.maximumTimerCount(), config.maximumWindowCount(),
                config.maximumCheckpointBytes(),
                config.maximumSpillDiskBytes(),
                config.maximumFileDescriptorCount());
        io.rowweave.stream.cluster.StreamFunctionResolver resolver =
                new io.rowweave.stream.cluster.StreamFunctionResolver() {
                    @Override public Serializable resolve(
                            FunctionDeploymentReference reference) {
                        return StreamWorkerRuntime.resolve(functions, reference);
                    }

                    @Override public boolean supportsAsyncReplay(
                            FunctionDeploymentReference reference,
                            io.rowweave.stream.api.StreamAsyncReplaySpec replay) {
                        StreamFunctionProvider provider = functions.get(
                                reference.deploymentSetId());
                        return provider != null
                                && provider.supportsAsyncReplay(
                                reference, replay);
                    }
                };
        StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, sources, eagerSinks, transactionalSinks,
                resolver,
                config.clusterStateRoot().resolve("checkpoints"),
                config.controlSecurity(), config.controlPort());
        return new StreamWorkerRuntime(server,
                config.controlSecurity().advertisedHost(), profile,
                config.controlSecurity());
    }

    static StreamWorkerRuntime startSigned(StreamWorkerConfig config,
            VerifiedDeploymentSet deployments) {
        return startSigned(config, deployments, null);
    }

    static StreamWorkerRuntime startSigned(StreamWorkerConfig config,
            VerifiedDeploymentSet deployments,
            StreamSecretResolver resolver) {
        if (config == null || deployments == null) {
            throw new IllegalArgumentException(
                    "Stream Worker configuration and deployments are required");
        }
        config.controlSecurity().requireImplementedTransport();
        config.controlSecurity().requireLocalRole(
                io.rowweave.cluster.rpc.ControlRole.WORKER);
        deployments.requireAdmissionOpen();
        List<String> features = capabilities(
                java.util.Collections.singletonList(
                        deployments.deploymentSetId()));
        features.add(PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                config.workerId(), UUID.randomUUID().toString(), 3, 1,
                features, config.taskSlots(), config.channelCapacity(),
                config.maximumFrameBytes(), config.maximumResidentBytes(),
                config.maximumThreads(), config.maximumTimerCount(),
                config.maximumWindowCount(),
                config.maximumCheckpointBytes(),
                config.maximumSpillDiskBytes(),
                config.maximumFileDescriptorCount());
        StreamRemoteWorkerServer server = StreamRemoteWorkerServer.signed(
                profile, deployments, resolver,
                config.clusterStateRoot().resolve("checkpoints"),
                config.controlSecurity(), config.controlPort());
        return new StreamWorkerRuntime(server,
                config.controlSecurity().advertisedHost(), profile,
                config.controlSecurity());
    }

    public StreamWorkerEndpoint endpoint() {
        return endpoint;
    }

    /** Registers this exact incarnation; closing the lease removes only it. */
    public StreamWorkerRegistryLease register(ServiceRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("Stream registry is required");
        }
        if (closed.get()) {
            throw new IllegalStateException("Stream Worker is closed");
        }
        RegistryStreamWorkerDirectory.register(registry, endpoint.host(),
                endpoint.port(), profile, security.securityMetadata());
        return new StreamWorkerRegistryLease(registry, profile);
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true)) delegate.close();
    }

    private static Serializable resolve(
            Map<String, StreamFunctionProvider> functions,
            FunctionDeploymentReference reference) {
        StreamFunctionProvider provider = functions.get(
                reference.deploymentSetId());
        if (provider == null) {
            throw new IllegalArgumentException(
                    "Stream function deployment is unavailable");
        }
        Serializable value = provider.resolve(reference);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Stream function provider returned null");
        }
        return value;
    }

    private static List<String> capabilities(
            Collection<String> deploymentSetIds) {
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_V2_CAPABILITY,
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY,
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.FLAT_MAP_CAPABILITY,
                PortableStreamPlan.UNION_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY,
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY,
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY,
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY,
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY,
                PortableStreamPlan.DYNAMIC_SPLIT_DISCOVERY_CAPABILITY,
                PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY,
                StreamWorkerCapabilityProfile.PROGRESS_CAPABILITY));
        for (String deploymentSetId : deploymentSetIds) {
            capabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                    + deploymentSetId);
        }
        return capabilities;
    }

    private static <T> void putUnique(
            Map<String, T> values, String key, T value, String kind) {
        if (values.put(key, value) != null) {
            throw new IllegalArgumentException(
                    "Stream Worker " + kind + " provider key is duplicated");
        }
    }
}
