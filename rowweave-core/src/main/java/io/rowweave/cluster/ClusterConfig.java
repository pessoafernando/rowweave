package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ControlEndpointConfig;
import io.rowweave.cluster.rpc.ControlRole;
import io.rowweave.cluster.rpc.ControlResidentConfig;
import io.rowweave.cluster.rpc.ControlSecurityProfile;
import io.rowweave.cluster.rpc.RpcAdmissionConfig;
import io.rowweave.deployment.DeploymentRuntimeConfig;
import io.rowweave.deployment.DeploymentRuntime;

import java.util.Properties;

/**
 * Immutable start-up configuration of a cluster node, parsed from system properties. The dependency
 * surface stays narrow: no CLI or YAML library is pulled in. This type freezes roles and budgets only;
 * it reads no TLS or deployment material and creates no repository, registry, socket or thread.
 *
 * <p>Configuration keys, all under {@code rowweave.*}:
 * <ul>
 * <li>{@code rowweave.master.enable} (default false) starts a MasterServer, which schedules the DAG and accepts jobs.</li>
 * <li>{@code rowweave.worker.enable} (default false) starts a WorkerServer, which forks a child process per stage.</li>
 * <li>{@code rowweave.master.port} (default 0, meaning any free port) pins the port of the master RPC.</li>
 * <li>{@code rowweave.control.securityProfile} (default LOOPBACK) and {@code rowweave.control.host}
 *     (default 127.0.0.1) give the security mode of the control plane and the address it binds and publishes.
 *     mTLS between hosts is the step that follows loopback control.</li>
 * <li>{@code rowweave.control.maxConnections}, {@code listenBacklog}, {@code frameDeadlineMs} and
 *     {@code inFlightFrameBytes} are admission settings, and an illegal combination is rejected before any
 *     role resource is created.</li>
 * <li>{@code rowweave.worker.master.host} (default 127.0.0.1) and {@code rowweave.worker.master.port} say
 *     which master a worker connects to.</li>
 * <li>{@code rowweave.worker.slots} (default 128) is the upper bound on stage instances, and therefore
 *     threads, that a worker may run concurrently, which is what admission counts against. It aligns with
 *     {@link io.rowweave.mem.WorkerConfig#DEFAULT_SLOTS}: a production heap is at least 2G, slots bound
 *     daemon threads rather than CPUs, and the pool is divided by slots for a per-stage share. Note that
 *     slots are not the parallelism P of a job: P is a property of the job and travels with the graph.</li>
 * <li>{@code rowweave.worker.memory} (half of an explicitly set -Xmx by default) is the managed memory of
 *     the process. {@code rowweave.memory.bytes} remains only as a compatibility alias for one release
 *     cycle, and when both are set they must be exactly equal.</li>
 * <li>{@code rowweave.heartbeat.staleness.ms} (default 5000) is what the master waits before calling a worker lost.</li>
 * <li>{@code rowweave.registry} (default inprocess) picks the registry: {@code inprocess} for a single
 *     master with no failover, or {@code zk} for several masters, which discovers ZkServiceRegistry through
 *     SPI and needs rowweave-registry-zk on the classpath.</li>
 * <li>{@code rowweave.zk.connectString} is the ZooKeeper connection string, {@code host:port,...}, used when the registry is {@code zk}.</li>
 * <li>{@code rowweave.master.artifact.dir} is the shared artifact directory for durable submission. Under
 *     ZooKeeper, or any production mode with several masters, every master must point at the same shared
 *     file system, one that supports locking and atomic moves.</li>
 * </ul>
 */
public final class ClusterConfig {

    private static final long MIB = 1024L * 1024L;
    private static final long MASTER_MIN_DIRECT_BYTES = 64L * MIB;
    private static final long WORKER_MIN_DIRECT_BYTES = 256L * MIB;
    private static final long MIN_METASPACE_BYTES = 128L * MIB;
    private static final long MIN_CODE_CACHE_BYTES = 64L * MIB;

    private final boolean masterEnabled;
    private final boolean workerEnabled;
    private final int masterPort;  // 0 means any free port
    private final String masterHost;  // host of the master a worker connects to
    private final int workerMasterPort;  // port of the master a worker connects to
    private final int workerSlots;  // threads, that is admission slots
    private final int workerMaxStageThreads;
    private final long memoryBytes;  // the whole managed pool of a worker, and zero when there is no worker and nothing was configured
    private final long heartbeatStalenessMs;
    private final String registryKind;  // inprocess by default, or zk
    private final String zkConnectString;  // connection string used when the registry kind is zk
    private final String masterArtifactDirectory;
    private final RpcAdmissionConfig rpcAdmission;
    private final ControlEndpointConfig controlEndpoint;
    private final DataPlaneConfig dataPlaneConfig;
    private final Properties startupProperties;
    private final long maxHeapBytes;
    private final DeploymentRuntimeConfig deploymentConfig;
    private final long controlResidentBytes;
    private final long deploymentResidentBytes;
    private final long heapRequiredBytes;
    private final long javaThreadUpperBound;
    private final int monitorIoThreads;

    private ClusterConfig(boolean masterEnabled, boolean workerEnabled, int masterPort, String masterHost,
                          int workerMasterPort, int workerSlots, int workerMaxStageThreads,
                          long memoryBytes, long heartbeatStalenessMs,
                          String registryKind, String zkConnectString,
                          String masterArtifactDirectory,
                          RpcAdmissionConfig rpcAdmission,
                          ControlEndpointConfig controlEndpoint,
                          DataPlaneConfig dataPlaneConfig,
                          Properties startupProperties,
                          long maxHeapBytes,
                          DeploymentRuntimeConfig deploymentConfig,
                          long controlResidentBytes,
                          long deploymentResidentBytes,
                          long heapRequiredBytes,
                          long javaThreadUpperBound,
                          int monitorIoThreads) {
        this.masterEnabled = masterEnabled;
        this.workerEnabled = workerEnabled;
        this.masterPort = masterPort;
        this.masterHost = masterHost;
        this.workerMasterPort = workerMasterPort;
        this.workerSlots = workerSlots;
        this.workerMaxStageThreads = workerMaxStageThreads;
        this.memoryBytes = memoryBytes;
        this.heartbeatStalenessMs = heartbeatStalenessMs;
        this.registryKind = registryKind;
        this.zkConnectString = zkConnectString;
        this.masterArtifactDirectory = masterArtifactDirectory;
        this.rpcAdmission = rpcAdmission;
        this.controlEndpoint = controlEndpoint;
        this.dataPlaneConfig = dataPlaneConfig;
        this.startupProperties = startupProperties;
        this.maxHeapBytes = maxHeapBytes;
        this.deploymentConfig = deploymentConfig;
        this.controlResidentBytes = controlResidentBytes;
        this.deploymentResidentBytes = deploymentResidentBytes;
        this.heapRequiredBytes = heapRequiredBytes;
        this.javaThreadUpperBound = javaThreadUpperBound;
        this.monitorIoThreads = monitorIoThreads;
    }

    /** Parses from a Properties object, where a system property wins over the default. */
    public static ClusterConfig fromProperties(Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("cluster properties must not be null");
        }
        return parsePure(p, Runtime.getRuntime().maxMemory(), null);
    }

    /** pure parse plus aggregate capacity gate; no TLS/deployment resources. */
    static ClusterConfig fromProperties(Properties properties,
                                        JvmCapacityEnvelope capacity) {
        if (capacity == null) {
            throw new IllegalArgumentException("JVM capacity envelope is required");
        }
        ClusterConfig parsed = parsePure(properties, capacity.maxHeapBytes(), capacity);
        requireCertifiedNonHeap(capacity, parsed.masterEnabled(),
                parsed.workerEnabled());
        capacity.requireRssCapacity(parsed.javaThreadUpperBound());
        return parsed;
    }

    private static void requireCertifiedNonHeap(JvmCapacityEnvelope capacity,
                                                boolean masterEnabled,
                                                boolean workerEnabled) {
        if (!masterEnabled && !workerEnabled) return;
        long requiredDirect = workerEnabled
                ? WORKER_MIN_DIRECT_BYTES : MASTER_MIN_DIRECT_BYTES;
        if (capacity.maxDirectMemoryBytes() < requiredDirect) {
            throw certificationFailure("MaxDirectMemorySize is below the certified "
                    + (workerEnabled ? "Worker" : "Master") + " minimum");
        }
        if (capacity.maxMetaspaceBytes() < MIN_METASPACE_BYTES) {
            throw certificationFailure(
                    "MaxMetaspaceSize is below the certified process minimum");
        }
        if (capacity.reservedCodeCacheBytes() < MIN_CODE_CACHE_BYTES) {
            throw certificationFailure(
                    "ReservedCodeCacheSize is below the certified process minimum");
        }
    }

    private static ProcessCapacityException certificationFailure(String message) {
        return new ProcessCapacityException(
                ProcessCapacityException.Code.CERTIFICATION, message);
    }

    /** Parses from system properties, which is where ClusterMain reads its configuration by default. */
    public static ClusterConfig fromSystemProperties() {
        return fromProperties(System.getProperties());
    }

    /** Parses from the system properties frozen at the earliest bootstrap of the process, and applies the heap and base RSS checks. */
    public static ClusterConfig fromSystemProperties(ProcessBootstrapEnvelope processEnvelope) {
        if (processEnvelope == null) {
            throw new IllegalArgumentException("process bootstrap envelope is required");
        }
        Properties current = new Properties();
        current.putAll(System.getProperties());
        processEnvelope.requireCurrentConfiguration(current);
        return fromProperties(current, processEnvelope.capacityEnvelope());
    }

    public boolean masterEnabled() { return masterEnabled; }
    public boolean workerEnabled() { return workerEnabled; }
    public int masterPort() { return masterPort; }
    public String masterHost() { return masterHost; }
    public int workerMasterPort() { return workerMasterPort; }
    public int workerSlots() { return workerSlots; }
    /** Frozen per-assignment internal-thread ceiling certified by . */
    public int workerMaxStageThreads() { return workerMaxStageThreads; }
    public long memoryBytes() { return memoryBytes; }
    public long heartbeatStalenessMs() { return heartbeatStalenessMs; }
    public String registryKind() { return registryKind; }
    public String zkConnectString() { return zkConnectString; }
    public String masterArtifactDirectory() { return masterArtifactDirectory; }
    public ControlSecurityProfile controlProfile() { return controlEndpoint.profile(); }
    public ControlEndpointConfig controlEndpoint() { return controlEndpoint; }
    public DataPlaneConfig dataPlaneConfig() { return dataPlaneConfig; }
    public long controlResidentBytes() { return controlResidentBytes; }
    public long deploymentResidentBytes() { return deploymentResidentBytes; }
    public long heapRequiredBytes() { return heapRequiredBytes; }
    /** Conservative process-wide RowWeave Java-thread upper bound used by RSS preflight. */
    public long javaThreadUpperBound() { return javaThreadUpperBound; }
    /** Explicit monitor Netty I/O size; never inferred after process preflight. */
    public int monitorIoThreads() { return monitorIoThreads; }
    public RpcAdmissionConfig rpcAdmission() { return rpcAdmission; }
    public boolean zkEnabled() { return !"inprocess".equalsIgnoreCase(registryKind); }

    /** Materialize TLS/deployment state only after the aggregate capacity gate succeeded. */
    DeploymentRuntime loadDeploymentRuntime(ProcessTlsEnvelope processTlsEnvelope) {
        Properties properties = new Properties();
        properties.putAll(startupProperties);
        ControlSecurityConfig control = controlProfile() == ControlSecurityProfile.MTLS
                ? ControlSecurityConfig.fromProperties(properties, processTlsEnvelope)
                : ControlSecurityConfig.fromProperties(properties);
        if (masterEnabled) control.requireLocalRole(ControlRole.MASTER);
        if (workerEnabled) control.requireLocalRole(ControlRole.WORKER);
        return workerEnabled
                ? DeploymentRuntime.loadFrozen(properties, control,
                        maxHeapBytes, dataPlaneConfig, deploymentConfig,
                        processTlsEnvelope)
                : DeploymentRuntime.loadFrozen(properties, control,
                        maxHeapBytes, null, deploymentConfig, null);
    }

    private static ClusterConfig parsePure(Properties source, long maxHeapBytes,
                                           JvmCapacityEnvelope capacity) {
        if (source == null) {
            throw new IllegalArgumentException("cluster properties must not be null");
        }
        Properties properties = new Properties();
        properties.putAll(source);
        boolean masterEnabled = ClusterPropertyParser.booleanValue(
                properties, "rowweave.master.enable", false);
        boolean workerEnabled = ClusterPropertyParser.booleanValue(
                properties, "rowweave.worker.enable", false);
        ControlEndpointConfig endpoint = ControlEndpointConfig.fromProperties(properties);
        RpcAdmissionConfig rpcAdmission = RpcAdmissionConfig.fromProperties(properties);
        ControlResidentConfig control;
        DeploymentRuntimeConfig deployment;
        DataPlaneConfig dataPlane;
        try {
            control = ControlResidentConfig.fromProcessProperties(properties);
            deployment = DeploymentRuntimeConfig.fromProcessProperties(properties);
            int activeProcessors = capacity == null
                    ? Runtime.getRuntime().availableProcessors()
                    : capacity.activeProcessorCount();
            dataPlane = workerEnabled
                    ? DataPlaneConfig.fromProperties(properties, endpoint, maxHeapBytes,
                            activeProcessors)
                    : null;
        } catch (ProcessCapacityException capacityFailure) {
            throw capacityFailure;
        } catch (IllegalArgumentException invalidCapacityProperty) {
            if (capacity == null) throw invalidCapacityProperty;
            throw new ProcessCapacityException(
                    ProcessCapacityException.Code.CONFIGURATION,
                    "role capacity configuration is invalid",
                    invalidCapacityProperty);
        }
        long workerMemory = workerMemoryBytes(
                properties, workerEnabled, maxHeapBytes);
        int slots = strictPositiveInt(properties, "rowweave.worker.slots",
                io.rowweave.mem.WorkerConfig.DEFAULT_SLOTS);
        int maxStageThreads = strictPositiveInt(
                properties, "rowweave.worker.maxStageThreads",
                io.rowweave.mem.WorkerConfig.DEFAULT_MAX_STAGE_THREADS);
        int activeProcessors = capacity == null
                ? Runtime.getRuntime().availableProcessors()
                : capacity.activeProcessorCount();
        String registry = trimOr(properties.getProperty("rowweave.registry"), "inprocess");
        int monitorIoThreads = strictPositiveInt(properties,
                ProcessThreadCapacityModel.MONITOR_IO_THREADS,
                Math.max(2, activeProcessors / 2), 64);
        long javaThreads = capacity == null ? 0L
                : ProcessThreadCapacityModel.estimate(
                        properties, masterEnabled, workerEnabled,
                        !"inprocess".equalsIgnoreCase(registry), rpcAdmission,
                        dataPlane, slots, maxStageThreads, activeProcessors)
                        .totalThreads();
        long heapHeadroom = capacity == null ? 0L : capacity.heapHeadroomBytes();
        long heapRequired = heapRequired(control.residentBytes(),
                deployment.deploymentBytes(), workerEnabled, workerMemory,
                dataPlane, heapHeadroom);
        if (capacity != null && heapRequired > maxHeapBytes) {
            throw new ProcessCapacityException(
                    ProcessCapacityException.Code.HEAP_CAPACITY,
                    "aggregate heap budget exceeds declared -Xmx");
        }
        return new ClusterConfig(
                masterEnabled,
                workerEnabled,
                parseInt(properties.getProperty("rowweave.master.port"), 0),
                trimOr(properties.getProperty("rowweave.worker.master.host"), "127.0.0.1"),
                parseInt(properties.getProperty("rowweave.worker.master.port"), 0),
                slots, maxStageThreads,
                workerMemory,
                parseLong(properties.getProperty("rowweave.heartbeat.staleness.ms"), 5000L),
                registry,
                trimOr(properties.getProperty("rowweave.zk.connectString"), ""),
                trimToNull(properties.getProperty("rowweave.master.artifact.dir")),
                rpcAdmission, endpoint, dataPlane, properties, maxHeapBytes, deployment,
                control.residentBytes(), deployment.deploymentBytes(), heapRequired,
                javaThreads, monitorIoThreads);
    }

    private static long workerMemoryBytes(Properties properties,
                                          boolean workerEnabled,
                                          long maxHeapBytes) {
        String canonical = properties.getProperty("rowweave.worker.memory");
        String compatibility = properties.getProperty("rowweave.memory.bytes");
        Long canonicalBytes = canonical == null ? null
                : strictPositiveLong(canonical, "rowweave.worker.memory");
        Long compatibilityBytes = compatibility == null ? null
                : strictPositiveLong(compatibility, "rowweave.memory.bytes");
        if (canonicalBytes != null && compatibilityBytes != null
                && !canonicalBytes.equals(compatibilityBytes)) {
            throw configurationFailure(
                    "rowweave.worker.memory and rowweave.memory.bytes must be exactly equal");
        }
        if (canonicalBytes != null) return canonicalBytes;
        if (compatibilityBytes != null) return compatibilityBytes;
        return workerEnabled ? maxHeapBytes / 2L : 0L;
    }

    private static int strictPositiveInt(Properties properties, String name,
                                         int defaultValue) {
        return strictPositiveInt(properties, name, defaultValue,
                Integer.MAX_VALUE);
    }

    private static int strictPositiveInt(Properties properties, String name,
                                         int defaultValue, int maximum) {
        String configured = properties.getProperty(name);
        if (configured == null) return defaultValue;
        long value = strictPositiveLong(configured, name);
        if (value > maximum) {
            throw configurationFailure(name + " must be in [1," + maximum + ']');
        }
        return (int) value;
    }

    private static long strictPositiveLong(String value, String name) {
        if (value == null || value.isEmpty() || !value.equals(value.trim())
                || value.charAt(0) == '0') {
            throw configurationFailure(name + " must be a positive canonical decimal");
        }
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current < '0' || current > '9') {
                throw configurationFailure(name + " must be a positive canonical decimal");
            }
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0L) throw new NumberFormatException("non-positive");
            return parsed;
        } catch (NumberFormatException invalid) {
            throw new ProcessCapacityException(
                    ProcessCapacityException.Code.CONFIGURATION,
                    name + " is outside the signed long range", invalid);
        }
    }

    private static long heapRequired(long control, long deployment,
                                     boolean workerEnabled, long workerMemory,
                                     DataPlaneConfig dataPlane, long headroom) {
        try {
            long required = Math.addExact(control, deployment);
            if (workerEnabled) required = Math.addExact(required, workerMemory);
            if (workerEnabled && dataPlane != null && dataPlane.muxEnabled()) {
                required = Math.addExact(required,
                        dataPlane.ingressAdmission().ingressResidentBytes());
            }
            if (workerEnabled && dataPlane != null
                    && dataPlane.protocol() == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1) {
                required = Math.addExact(required, dataPlane.handshakeResidentBytes());
            }
            return Math.addExact(required, headroom);
        } catch (ArithmeticException overflow) {
            throw new ProcessCapacityException(
                    ProcessCapacityException.Code.HEAP_CAPACITY,
                    "aggregate heap budget arithmetic overflow", overflow);
        }
    }

    private static ProcessCapacityException configurationFailure(String message) {
        return new ProcessCapacityException(
                ProcessCapacityException.Code.CONFIGURATION, message);
    }

    private static int parseInt(String v, int dflt) {
        return (v == null || v.trim().isEmpty()) ? dflt : Integer.parseInt(v.trim());
    }

    private static long parseLong(String v, long dflt) {
        return (v == null || v.trim().isEmpty()) ? dflt : Long.parseLong(v.trim());
    }

    private static String trimOr(String v, String dflt) {
        if (v == null) {
            return dflt;
        }
        String t = v.trim();
        return t.isEmpty() ? dflt : t;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
