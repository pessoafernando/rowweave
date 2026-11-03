package io.rowweave.deployment;

import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.cluster.ProcessTlsEnvelope;
import io.rowweave.cluster.rpc.ControlResidentConfig;
import io.rowweave.cluster.rpc.DataPlaneSecurityContext;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ControlSecurityProfile;
import io.rowweave.cluster.rpc.ProtocolHelloConfig;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.plan.ObjectTrustContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** One immutable, verified deployment identity for a Client/Master/Worker process. */
public final class DeploymentRuntime implements AutoCloseable {
    public static final String DEPLOYMENT_COUNT_PROPERTY = "rowweave.deployment.count";
    public static final String MANIFEST_PROPERTY_PREFIX = "rowweave.deployment.manifest.";
    public static final String SIGNER_COUNT_PROPERTY = "rowweave.deployment.signer.count";
    public static final String SIGNER_ID_PROPERTY_PREFIX = "rowweave.deployment.signer.id.";
    public static final String SIGNER_PATH_PROPERTY_PREFIX = "rowweave.deployment.signer.path.";
    public static final String TRUST_ROOT_PROPERTY = "rowweave.deployment.trustRoot";
    public static final String PROBE_INTERVAL_PROPERTY = "rowweave.deployment.probeIntervalMs";

    private static final int MAX_DEPLOYMENTS = 4096;
    private static final long DEFAULT_PROBE_INTERVAL_MILLIS = 30_000L;
    private static final long MAX_PROBE_INTERVAL_MILLIS = 86_400_000L;

    private final VerifiedDeploymentSet deployments;
    private final ControlSecurityConfig controlSecurity;
    private final DataPlaneConfig dataPlaneConfig;
    private final io.rowweave.sched.net.DataPlaneHandshakeAdmission dataPlaneHandshakeAdmission;
    private final DataPlaneSecurityContext dataPlaneSecurityContext;
    private final ObjectTrustContext objectTrustContext;
    private final ProtocolHelloV1 protocolHello;
    private final DurableBatchSinkProviderRegistry durableSinkProviderRegistry;
    private final IncrementalSourceProviderRegistry incrementalSourceProviderRegistry;
    private final PortableRuntimeBindingRegistry portableBindingVerifier;
    private final StreamConnectorProviderRegistry streamConnectorProviderRegistry;
    private final ScheduledExecutorService probeExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();

    private DeploymentRuntime(VerifiedDeploymentSet deployments,
                              ControlSecurityConfig controlSecurity,
                              DataPlaneConfig dataPlaneConfig,
                              ProcessTlsEnvelope processTlsEnvelope,
                              ScheduledExecutorService probeExecutor,
                              ProtocolHelloV1 protocolHello) {
        this(deployments, controlSecurity, dataPlaneConfig,
                processTlsEnvelope, probeExecutor, null, null, protocolHello);
    }

    private DeploymentRuntime(VerifiedDeploymentSet deployments,
                              ControlSecurityConfig controlSecurity,
                              DataPlaneConfig dataPlaneConfig,
                              ProcessTlsEnvelope processTlsEnvelope,
                              ScheduledExecutorService probeExecutor,
                              DurableBatchSinkProviderRegistry providerRegistry,
                              IncrementalSourceProviderRegistry sourceProviderRegistry,
                              ProtocolHelloV1 protocolHello) {
        this.deployments = deployments;
        boolean workerRuntime = dataPlaneConfig != null;
        this.durableSinkProviderRegistry = providerRegistry == null
                ? DurableBatchSinkProviderRegistry.scan(
                deployments, !workerRuntime
                        ? DurableBatchSinkProviderRegistry.Role.STATIC_ONLY
                        : DurableBatchSinkProviderRegistry.Role.WORKER)
                : providerRegistry;
        this.incrementalSourceProviderRegistry = sourceProviderRegistry == null
                ? IncrementalSourceProviderRegistry.scan(
                deployments, !workerRuntime
                        ? IncrementalSourceProviderRegistry.Role.STATIC_ONLY
                        : IncrementalSourceProviderRegistry.Role.WORKER)
                : sourceProviderRegistry;
        this.portableBindingVerifier = PortableRuntimeBindingRegistry.scan(
                deployments, PortableRuntimeBindingRegistry.Role.STATIC_ONLY,
                null, 0L);
        this.streamConnectorProviderRegistry =
                StreamConnectorProviderRegistry.scan(deployments,
                        StreamConnectorProviderRegistry.Role.STATIC_ONLY);
        this.controlSecurity = controlSecurity.withDeploymentSetId(deployments.deploymentSetId());
        this.dataPlaneConfig = dataPlaneConfig;
        io.rowweave.sched.net.DataPlaneHandshakeAdmission handshakeAdmission = null;
        DataPlaneSecurityContext securityContext = null;
        if (dataPlaneConfig != null
                && dataPlaneConfig.protocol()
                == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1) {
            ProcessTlsEnvelope.DataPlaneOwner owner =
                    processTlsEnvelope.claimDataPlaneOwner(dataPlaneConfig);
            handshakeAdmission = owner.admission();
            try {
                securityContext = this.controlSecurity.dataPlaneSecurityContext(
                        processTlsEnvelope, dataPlaneConfig.handshakeTimeoutMillis(), owner);
            } catch (RuntimeException | Error failure) {
                handshakeAdmission.close();
                throw failure;
            }
        }
        this.dataPlaneHandshakeAdmission = handshakeAdmission;
        this.dataPlaneSecurityContext = securityContext;
        this.objectTrustContext = this.controlSecurity.profile() == ControlSecurityProfile.MTLS
                ? ObjectTrustContext.mtlsDeployments(deployments)
                : ObjectTrustContext.loopbackCluster();
        this.protocolHello = protocolHello;
        this.probeExecutor = probeExecutor;
    }

    /** Package-local signed-fixture seam; production admission policy never reaches it. */
    static DeploymentRuntime forTesting(
            VerifiedDeploymentSet deployments,
            DurableBatchSinkProviderRegistry providerRegistry,
            boolean workerRuntime) {
        return forTesting(deployments, providerRegistry, null, workerRuntime);
    }

    /** Package-local signed-fixture seam for an independent source registry. */
    static DeploymentRuntime forTesting(
            VerifiedDeploymentSet deployments,
            DurableBatchSinkProviderRegistry providerRegistry,
            IncrementalSourceProviderRegistry sourceProviderRegistry,
            boolean workerRuntime) {
        Properties properties = new Properties();
        properties.putAll(System.getProperties());
        ControlSecurityConfig security =
                ControlSecurityConfig.fromProperties(properties);
        DataPlaneConfig dataPlane = workerRuntime
                ? DataPlaneConfig.fromProperties(properties, security,
                Runtime.getRuntime().maxMemory()) : null;
        return new DeploymentRuntime(deployments, security, dataPlane,
                null, null, providerRegistry, sourceProviderRegistry,
                ProtocolHelloConfig.fromProperties(properties));
    }

    /** Control-only runtime: does not resolve Worker shuffle configuration and neither creates nor consumes a data-plane owner. */
    public static DeploymentRuntime load(Properties properties,
                                         ControlSecurityConfig controlSecurity,
                                         long maxHeapBytes) {
        return load(properties, controlSecurity, maxHeapBytes, null, false);
    }

    /**
     * A Worker runtime creates the data plane; under MTLS it must carry the proof produced by the
     * earliest process bootstrap and freeze that same configuration snapshot.
     */
    public static DeploymentRuntime load(Properties properties,
                                         ControlSecurityConfig controlSecurity,
                                         long maxHeapBytes,
                                         ProcessTlsEnvelope processTlsEnvelope) {
        return load(properties, controlSecurity, maxHeapBytes,
                processTlsEnvelope, true, null, null);
    }

    /** materialize only from the already validated frozen config. */
    public static DeploymentRuntime loadFrozen(
            Properties properties,
            ControlSecurityConfig controlSecurity,
            long maxHeapBytes,
            DataPlaneConfig dataPlaneConfig,
            DeploymentRuntimeConfig runtimeConfig,
            ProcessTlsEnvelope processTlsEnvelope) {
        if (maxHeapBytes <= 0L) {
            throw new IllegalArgumentException("frozen max heap must be positive");
        }
        if (runtimeConfig == null) {
            throw new IllegalArgumentException(
                    "frozen deployment runtime config is required");
        }
        boolean workerDataPlaneEnabled = dataPlaneConfig != null;
        return load(properties, controlSecurity, maxHeapBytes, processTlsEnvelope,
                workerDataPlaneEnabled, dataPlaneConfig, runtimeConfig);
    }

    private static DeploymentRuntime load(Properties properties,
                                          ControlSecurityConfig controlSecurity,
                                          long maxHeapBytes,
                                          ProcessTlsEnvelope processTlsEnvelope,
                                          boolean workerDataPlaneEnabled) {
        return load(properties, controlSecurity, maxHeapBytes,
                processTlsEnvelope, workerDataPlaneEnabled, null, null);
    }

    private static DeploymentRuntime load(Properties properties,
                                          ControlSecurityConfig controlSecurity,
                                          long maxHeapBytes,
                                          ProcessTlsEnvelope processTlsEnvelope,
                                          boolean workerDataPlaneEnabled,
                                          DataPlaneConfig resolvedDataPlaneConfig,
                                          DeploymentRuntimeConfig resolvedRuntimeConfig) {
        if (properties == null || controlSecurity == null) {
            throw new IllegalArgumentException("deployment properties and control security are required");
        }
        Properties startupProperties = new Properties();
        startupProperties.putAll(properties);
        if (workerDataPlaneEnabled
                && controlSecurity.profile() == ControlSecurityProfile.MTLS) {
            if (processTlsEnvelope == null) {
                throw new IllegalStateException(
                        "MTLS Worker runtime requires current process bootstrap proof");
            }
            processTlsEnvelope.requireCurrentBootstrap();
            processTlsEnvelope.requireCertifiedForMtls();
            processTlsEnvelope.requireConfigurationMatch(startupProperties);
            controlSecurity.requireProcessConfiguration(processTlsEnvelope);
            controlSecurity.requireLocalRole(io.rowweave.cluster.rpc.ControlRole.WORKER);
        }
        DataPlaneConfig dataPlaneConfig = resolvedDataPlaneConfig != null
                ? resolvedDataPlaneConfig : workerDataPlaneEnabled
                ? DataPlaneConfig.fromProperties(
                        startupProperties, controlSecurity, maxHeapBytes)
                : null;
        int deploymentCount = count(startupProperties, DEPLOYMENT_COUNT_PROPERTY, MAX_DEPLOYMENTS);
        int signerCount = count(startupProperties, SIGNER_COUNT_PROPERTY, MAX_DEPLOYMENTS);
        requireExactIndexes(startupProperties, MANIFEST_PROPERTY_PREFIX, deploymentCount);
        requireExactIndexes(startupProperties, SIGNER_ID_PROPERTY_PREFIX, signerCount);
        requireExactIndexes(startupProperties, SIGNER_PATH_PROPERTY_PREFIX, signerCount);
        Path trustRoot = null;
        Map<String, Path> signerPaths = new LinkedHashMap<>();
        if (deploymentCount > 0) {
            trustRoot = absolutePath(required(startupProperties, TRUST_ROOT_PROPERTY), TRUST_ROOT_PROPERTY);
            if (signerCount < 1) {
                throw new IllegalArgumentException(SIGNER_COUNT_PROPERTY
                        + " must be positive when deployments are configured");
            }
            for (int i = 0; i < signerCount; i++) {
                String id = required(startupProperties, SIGNER_ID_PROPERTY_PREFIX + i);
                Path source = absolutePath(required(startupProperties, SIGNER_PATH_PROPERTY_PREFIX + i),
                        SIGNER_PATH_PROPERTY_PREFIX + i);
                if (signerPaths.put(id, source) != null) {
                    throw new IllegalArgumentException("duplicate deployment signer id: " + id);
                }
            }
        } else if (signerCount != 0 || startupProperties.getProperty(TRUST_ROOT_PROPERTY) != null) {
            throw new IllegalArgumentException("deployment trustRoot/signers require a positive deployment count");
        }

        DeploymentRuntimeConfig runtimeConfig = resolvedRuntimeConfig;
        if (runtimeConfig == null) {
            long controlBytes = ControlResidentConfig.fromProperties(
                    startupProperties, maxHeapBytes).residentBytes();
            runtimeConfig = DeploymentRuntimeConfig.fromProperties(
                    startupProperties, controlBytes, maxHeapBytes);
        }
        DeploymentMemoryBudget budget = runtimeConfig.newBudget();
        VerifiedDeploymentSet verified = null;
        ScheduledExecutorService probe = null;
        try {
            Map<String, PublicKey> signerKeys = deploymentCount == 0
                    ? java.util.Collections.<String, PublicKey>emptyMap()
                    : TrustedSignerRegistry.load(trustRoot, signerPaths).signerKeys();
            List<DeploymentManifest> manifests = new ArrayList<>(deploymentCount);
            for (int i = 0; i < deploymentCount; i++) {
                Path source = absolutePath(required(startupProperties, MANIFEST_PROPERTY_PREFIX + i),
                        MANIFEST_PROPERTY_PREFIX + i);
                TrustedSignerRegistry.requireReadOnlyChild(
                        trustRoot, source, "DEPLOYMENT_MANIFEST_NOT_TRUSTED");
                manifests.add(DeploymentManifestCodec.decode(readManifest(source)));
            }
            verified = VerifiedDeploymentSet.load(manifests, signerKeys, budget);
            if (deploymentCount > 0) {
                long interval = probeInterval(startupProperties);
                final VerifiedDeploymentSet observed = verified;
                probe = Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "rowweave-deployment-source-probe");
                    thread.setDaemon(true);
                    return thread;
                });
                probe.scheduleWithFixedDelay(() -> {
                    try {
                        observed.probeSources();
                    } catch (RuntimeException | Error failure) {
                        observed.closeAdmission();
                    }
                }, interval, interval, TimeUnit.MILLISECONDS);
            }
            return new DeploymentRuntime(verified, controlSecurity, dataPlaneConfig,
                    processTlsEnvelope, probe,
                    ProtocolHelloConfig.fromProperties(startupProperties));
        } catch (RuntimeException | Error failure) {
            if (probe != null) probe.shutdownNow();
            if (verified != null) verified.close();
            throw failure;
        }
    }

    public static DeploymentRuntime fromSystemProperties(ControlSecurityConfig controlSecurity) {
        Properties properties = new Properties();
        properties.putAll(System.getProperties());
        return load(properties, controlSecurity, Runtime.getRuntime().maxMemory());
    }

    /** Only for the LOOPBACK direct-Worker construction path; MTLS is refused before any TLS material is read. */
    public static DeploymentRuntime fromSystemPropertiesForWorker() {
        Properties properties = new Properties();
        properties.putAll(System.getProperties());
        String profile = properties.getProperty(
                ControlSecurityConfig.PROFILE_PROPERTY, "LOOPBACK").trim();
        if ("MTLS".equalsIgnoreCase(profile)) {
            throw new IllegalStateException(
                    "direct MTLS Worker construction is unsupported; use ClusterMain bootstrap");
        }
        ControlSecurityConfig controlSecurity =
                ControlSecurityConfig.fromProperties(properties);
        return load(properties, controlSecurity, Runtime.getRuntime().maxMemory(),
                null, true);
    }

    public ControlSecurityConfig controlSecurity() { return controlSecurity; }

    /** Null only for an explicitly pre-v1 process. */
    public ProtocolHelloV1 protocolHello() { return protocolHello; }

    public DataPlaneConfig requireDataPlaneConfig() {
        if (dataPlaneConfig == null) {
            throw new IllegalStateException(
                    "runtime does not own a Worker data-plane configuration");
        }
        return dataPlaneConfig;
    }

    public DataPlaneSecurityContext requireDataPlaneSecurityContext() {
        if (dataPlaneSecurityContext == null) {
            throw new IllegalStateException(
                    "runtime does not own a certified Worker data-plane TLS context");
        }
        return dataPlaneSecurityContext;
    }

    public io.rowweave.sched.net.DataPlaneHandshakeAdmission
            requireDataPlaneHandshakeAdmission() {
        if (dataPlaneHandshakeAdmission == null) {
            throw new IllegalStateException(
                    "runtime does not own a certified Worker data-plane admission");
        }
        return dataPlaneHandshakeAdmission;
    }

    public ObjectTrustContext objectTrustContext() { return objectTrustContext; }

    public DurableBatchSinkProviderRegistry durableSinkProviderRegistry() {
        return durableSinkProviderRegistry;
    }

    public IncrementalSourceProviderRegistry
            incrementalSourceProviderRegistry() {
        return incrementalSourceProviderRegistry;
    }

    /** Master/Client metadata-only portable verifier; it never loads provider classes. */
    public PortableRuntimeBindingRegistry portableBindingVerifier() {
        return portableBindingVerifier;
    }

    /** Master-safe Stream connector metadata; provider classes stay unloaded. */
    public StreamConnectorProviderRegistry streamConnectorProviderRegistry() {
        requireAdmissionOpen();
        return streamConnectorProviderRegistry;
    }

    /** Starts a Stream Worker over this runtime's immutable signed images. */
    public io.rowweave.StreamWorkerRuntime startStreamWorker(
            io.rowweave.stream.api.StreamWorkerConfig config,
            io.rowweave.stream.spi.StreamSecretResolver resolver) {
        requireAdmissionOpen();
        return resolver == null
                ? io.rowweave.RowWeave.startStreamWorker(config, deployments)
                : io.rowweave.RowWeave.startStreamWorker(
                        config, deployments, resolver);
    }

    /** Worker-only resolver over the same immutable signed deployment set. */
    public PortableRuntimeBindingRegistry portableRuntimeBindings(
            io.rowweave.internal.ProviderActionSupervisor supervisor,
            long actionTimeoutMillis) {
        if (dataPlaneConfig == null) {
            throw new IllegalStateException(
                    "portable runtime bindings require a Worker runtime");
        }
        requireAdmissionOpen();
        return PortableRuntimeBindingRegistry.scan(
                deployments, PortableRuntimeBindingRegistry.Role.WORKER,
                supervisor, actionTimeoutMillis);
    }

    public String deploymentSetId() { return deployments.deploymentSetId(); }

    /**
     * Opens the verified application class path for a client assembling a plan. The returned loader stays
     * owned and closed by this runtime; the call first takes the admission fence, so a new plan is not admitted after a source probe has already failed.
     */
    public ClassLoader applicationClassLoader(String deploymentId) {
        requireAdmissionOpen();
        return deployments.classLoader(deploymentId);
    }

    public void requireAdmissionOpen() { deployments.requireAdmissionOpen(); }

    public boolean probeSources() { return deployments.probeSources(); }

    VerifiedDeploymentSet verifiedDeployments() { return deployments; }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Throwable failure = null;
        if (dataPlaneHandshakeAdmission != null) {
            try {
                dataPlaneHandshakeAdmission.close();
            } catch (Throwable admissionFailure) {
                failure = admissionFailure;
            }
        }
        if (probeExecutor != null) {
            probeExecutor.shutdownNow();
            try {
                if (!probeExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    failure = append(failure, new IllegalStateException(
                            "deployment source probe did not terminate"));
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure = append(failure, new IllegalStateException(
                        "interrupted closing deployment runtime", interrupted));
            }
        }
        try {
            deployments.close();
        } catch (Throwable deploymentFailure) {
            failure = append(failure, deploymentFailure);
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) throw new IllegalStateException(failure);
    }

    private static Throwable append(Throwable current, Throwable next) {
        if (current == null) return next;
        current.addSuppressed(next);
        return current;
    }

    private static int count(Properties properties, String name, int max) {
        String configured = properties.getProperty(name);
        if (configured == null || configured.trim().isEmpty()) return 0;
        String canonical = configured.trim();
        if (!canonical.equals(configured)) {
            throw new IllegalArgumentException(name + " must be canonical decimal");
        }
        try {
            int value = Integer.parseInt(canonical);
            if (value < 0 || value > max || !Integer.toString(value).equals(canonical)) {
                throw new NumberFormatException("out of bounds");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be an integer in [0," + max + ']');
        }
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isEmpty() || !value.equals(value.trim())) {
            throw new IllegalArgumentException(name + " is required and must not contain outer whitespace");
        }
        return value;
    }

    private static void requireExactIndexes(Properties properties, String prefix, int count) {
        for (String name : properties.stringPropertyNames()) {
            if (!name.startsWith(prefix)) continue;
            String suffix = name.substring(prefix.length());
            try {
                int index = Integer.parseInt(suffix);
                if (index < 0 || index >= count || !Integer.toString(index).equals(suffix)) {
                    throw new NumberFormatException("index out of range");
                }
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(
                        "unexpected or non-canonical indexed deployment property: " + name);
            }
        }
    }

    private static Path absolutePath(String value, String name) {
        Path result = Paths.get(value);
        if (!result.isAbsolute() || !result.normalize().equals(result)
                || value.length() > 4096 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be a bounded absolute normalized path");
        }
        return result;
    }

    private static byte[] readManifest(Path source) {
        try (InputStream input = Files.newInputStream(source)) {
            ByteArrayOutputStream output = new ByteArrayOutputStream(1024);
            byte[] chunk = new byte[4096];
            int total = 0;
            while (true) {
                int read = input.read(chunk);
                if (read < 0) break;
                if (read == 0) continue;
                total = Math.addExact(total, read);
                if (total > DeploymentManifestCodec.MAX_MANIFEST_BYTES) {
                    throw new DeploymentValidationException(
                            "INVALID_DEPLOYMENT_MANIFEST", "manifest exceeds byte limit");
                }
                output.write(chunk, 0, read);
            }
            return output.toByteArray();
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new DeploymentValidationException(
                    "INVALID_DEPLOYMENT_MANIFEST", "cannot read deployment manifest", failure);
        }
    }

    private static long probeInterval(Properties properties) {
        String configured = properties.getProperty(PROBE_INTERVAL_PROPERTY);
        if (configured == null || configured.trim().isEmpty()) {
            return DEFAULT_PROBE_INTERVAL_MILLIS;
        }
        try {
            long value = Long.parseLong(configured);
            if (value < 1_000L || value > MAX_PROBE_INTERVAL_MILLIS
                    || !Long.toString(value).equals(configured)) {
                throw new NumberFormatException("out of bounds");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(PROBE_INTERVAL_PROPERTY
                    + " must be canonical milliseconds in [1000,86400000]");
        }
    }
}
