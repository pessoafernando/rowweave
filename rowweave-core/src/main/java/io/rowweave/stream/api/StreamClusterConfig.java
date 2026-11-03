package io.rowweave.stream.api;

import io.rowweave.cluster.rpc.ControlEndpointConfig;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.JvmCapacityEnvelope;
import io.rowweave.cluster.ProcessBootstrapEnvelope;
import io.rowweave.cluster.ProcessTlsEnvelope;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** Pure, immutable role and endpoint configuration for a Stream cluster process. */
public final class StreamClusterConfig {
    private static final int MASTER_INFRASTRUCTURE_THREADS = 7;
    private static final int WORKER_INFRASTRUCTURE_THREADS = 6;
    private static final int EXTERNAL_REGISTRY_THREADS_PER_ROLE = 16;
    private final boolean masterEnabled;
    private final boolean workerEnabled;
    private final Path stateRoot;
    private final int masterPort;
    private final int workerPort;
    private final String workerId;
    private final String registryKind;
    private final String zkConnectString;
    private final Path artifactRoot;
    private final List<StreamWorkerEndpoint> fixedWorkers;
    private final ControlEndpointConfig controlEndpoint;
    private final Properties properties;
    private final ProcessTlsEnvelope processTlsEnvelope;
    private final StreamWorkerConfig workerConfig;
    private final boolean deploymentProbeEnabled;

    private StreamClusterConfig(boolean masterEnabled,
            boolean workerEnabled, Path stateRoot, int masterPort,
            int workerPort, String workerId, String registryKind,
            String zkConnectString, Path artifactRoot,
            List<StreamWorkerEndpoint> fixedWorkers,
            ControlEndpointConfig controlEndpoint, Properties properties,
            ProcessTlsEnvelope processTlsEnvelope,
            StreamWorkerConfig workerConfig,
            boolean deploymentProbeEnabled) {
        this.masterEnabled = masterEnabled;
        this.workerEnabled = workerEnabled;
        this.stateRoot = stateRoot;
        this.masterPort = masterPort;
        this.workerPort = workerPort;
        this.workerId = workerId;
        this.registryKind = registryKind;
        this.zkConnectString = zkConnectString;
        this.artifactRoot = artifactRoot;
        this.fixedWorkers = Collections.unmodifiableList(
                new ArrayList<>(fixedWorkers));
        this.controlEndpoint = controlEndpoint;
        this.properties = properties;
        this.processTlsEnvelope = processTlsEnvelope;
        this.workerConfig = workerConfig;
        this.deploymentProbeEnabled = deploymentProbeEnabled;
    }

    public static StreamClusterConfig fromProperties(Properties source) {
        return fromProperties(source, (ProcessTlsEnvelope) null);
    }

    /** Parses the exact property snapshot captured by process bootstrap. */
    public static StreamClusterConfig fromProperties(Properties source,
            ProcessTlsEnvelope processTlsEnvelope) {
        if (source == null) {
            throw new IllegalArgumentException(
                    "Stream cluster properties are required");
        }
        Properties properties = new Properties();
        properties.putAll(source);
        if (processTlsEnvelope != null) {
            processTlsEnvelope.requireCurrentBootstrap();
            processTlsEnvelope.requireConfigurationMatch(properties);
        }
        boolean master = booleanValue(properties,
                "rowweave.stream.master.enable", false);
        boolean worker = booleanValue(properties,
                "rowweave.stream.worker.enable", false);
        if (!master && !worker) {
            throw new IllegalArgumentException(
                    "at least one Stream cluster role must be enabled");
        }
        String rootText = required(properties, "rowweave.stream.state.dir");
        Path root = Paths.get(rootText).toAbsolutePath().normalize();
        int masterPort = port(properties, "rowweave.stream.master.port", 0);
        int workerPort = port(properties, "rowweave.stream.worker.port", 0);
        String workerId = trim(properties.getProperty(
                "rowweave.stream.worker.id"));
        if (worker && workerId == null) {
            throw new IllegalArgumentException(
                    "Stream Worker identity is required");
        }
        String registry = trim(properties.getProperty("rowweave.registry"));
        if (registry == null) registry = "inprocess";
        String connect = trim(properties.getProperty(
                "rowweave.zk.connectString"));
        boolean zk = !"inprocess".equalsIgnoreCase(registry);
        if (zk && connect == null) {
            throw new IllegalArgumentException(
                    "external Stream registry connect string is required");
        }
        String artifactText = trim(properties.getProperty(
                "rowweave.stream.master.artifact.dir"));
        Path artifactRoot = artifactText == null ? null
                : Paths.get(artifactText).toAbsolutePath().normalize();
        if (zk && master && artifactRoot == null) {
            throw new IllegalArgumentException(
                    "ZooKeeper Stream Master requires a shared Artifact directory");
        }
        List<StreamWorkerEndpoint> fixed = fixedWorkers(
                properties.getProperty("rowweave.stream.master.workers"));
        if (master && !worker && !zk && fixed.isEmpty()) {
            throw new IllegalArgumentException(
                    "direct Master-only mode requires fixed Worker endpoints");
        }
        StreamWorkerConfig workerConfig = worker
                ? StreamWorkerConfig.fromProperties(workerId,
                root.resolve("worker"), properties) : null;
        int deploymentCount = canonicalInt(properties,
                "rowweave.deployment.count", 0, 0, 4_096);
        return new StreamClusterConfig(master, worker, root, masterPort,
                workerPort, workerId, registry, connect, artifactRoot, fixed,
                ControlEndpointConfig.fromProperties(properties), properties,
                processTlsEnvelope, workerConfig, deploymentCount > 0);
    }

    /** Parses the earliest process snapshot and closes the Stream RSS budget. */
    public static StreamClusterConfig fromProperties(Properties source,
            ProcessBootstrapEnvelope processEnvelope) {
        if (processEnvelope == null) {
            throw new IllegalArgumentException(
                    "process bootstrap envelope is required");
        }
        StreamClusterConfig parsed = fromProperties(
                source, processEnvelope.tlsEnvelope());
        parsed.requireProcessCapacity(processEnvelope.capacityEnvelope());
        parsed.requireCertifiedProcessSecurity();
        return parsed;
    }

    public static StreamClusterConfig fromSystemProperties() {
        return fromProperties(System.getProperties());
    }

    public boolean masterEnabled() { return masterEnabled; }
    public boolean workerEnabled() { return workerEnabled; }
    public Path stateRoot() { return stateRoot; }
    public int masterPort() { return masterPort; }
    public int workerPort() { return workerPort; }
    public String workerId() { return workerId; }
    public StreamWorkerConfig workerConfig() {
        if (!workerEnabled) {
            throw new IllegalStateException("Stream Worker role is disabled");
        }
        return workerConfig;
    }
    public String registryKind() { return registryKind; }
    public String zkConnectString() { return zkConnectString; }
    public Path artifactRoot() { return artifactRoot; }
    public boolean zkEnabled() {
        return !"inprocess".equalsIgnoreCase(registryKind);
    }
    public List<StreamWorkerEndpoint> fixedWorkers() { return fixedWorkers; }
    public ControlEndpointConfig controlEndpoint() { return controlEndpoint; }

    /** Conservative upper bound for RowWeave-owned Stream process threads. */
    public long javaThreadUpperBound() {
        long value = masterEnabled ? MASTER_INFRASTRUCTURE_THREADS : 0L;
        if (workerEnabled) {
            value = Math.addExact(value,
                    Math.addExact((long) workerConfig.maximumThreads(),
                            WORKER_INFRASTRUCTURE_THREADS));
        }
        if (zkEnabled()) {
            int roles = (masterEnabled ? 1 : 0) + (workerEnabled ? 1 : 0);
            value = Math.addExact(value, Math.multiplyExact(
                    (long) roles, EXTERNAL_REGISTRY_THREADS_PER_ROLE));
        }
        if (deploymentProbeEnabled) value = Math.addExact(value, 1L);
        return value;
    }

    /** Loads TLS material only after the pure configuration gate succeeds. */
    public ControlSecurityConfig loadControlSecurity() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return controlEndpoint.profile()
                == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS
                && processTlsEnvelope != null
                ? ControlSecurityConfig.fromProperties(
                copy, processTlsEnvelope)
                : ControlSecurityConfig.fromProperties(copy);
    }

    private void requireProcessCapacity(JvmCapacityEnvelope capacity) {
        if (capacity == null) {
            throw new IllegalArgumentException(
                    "JVM capacity envelope is required");
        }
        capacity.requireRssCapacity(javaThreadUpperBound());
    }

    private void requireCertifiedProcessSecurity() {
        boolean externalRole = zkEnabled()
                || masterEnabled != workerEnabled
                || !fixedWorkers.isEmpty();
        if (externalRole && controlEndpoint.profile()
                != io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS) {
            throw new IllegalStateException(
                    "external Stream process roles require MTLS");
        }
    }

    private static boolean booleanValue(Properties properties, String key,
            boolean fallback) {
        String value = trim(properties.getProperty(key));
        if (value == null) return fallback;
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        throw new IllegalArgumentException(key + " must be true or false");
    }

    private static int port(Properties properties, String key, int fallback) {
        String value = trim(properties.getProperty(key));
        if (value == null) return fallback;
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0 || parsed > 65_535) throw new NumberFormatException();
            return parsed;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " is not a valid port");
        }
    }

    private static int canonicalInt(Properties properties, String key,
            int fallback, int minimum, int maximum) {
        String value = properties.getProperty(key);
        if (value == null) return fallback;
        if (value.isEmpty() || !value.equals(value.trim())
                || (value.length() > 1 && value.charAt(0) == '0')) {
            throw new IllegalArgumentException(key + " is not canonical");
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < minimum || parsed > maximum
                    || !Integer.toString(parsed).equals(value)) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " is out of range");
        }
    }

    private static List<StreamWorkerEndpoint> fixedWorkers(String text) {
        String value = trim(text);
        if (value == null) return Collections.emptyList();
        List<StreamWorkerEndpoint> result = new ArrayList<>();
        for (String item : value.split(",", -1)) {
            String endpoint = trim(item);
            if (endpoint == null) {
                throw new IllegalArgumentException(
                        "fixed Stream Worker endpoint is empty");
            }
            int separator = endpoint.lastIndexOf(':');
            if (separator < 1 || separator == endpoint.length() - 1) {
                throw new IllegalArgumentException(
                        "fixed Stream Worker endpoint must be host:port");
            }
            String host = endpoint.substring(0, separator);
            if (host.startsWith("[") && host.endsWith("]")) {
                host = host.substring(1, host.length() - 1);
            }
            int port;
            try {
                port = Integer.parseInt(endpoint.substring(separator + 1));
            } catch (NumberFormatException invalid) {
                throw new IllegalArgumentException(
                        "fixed Stream Worker endpoint port is invalid");
            }
            result.add(new StreamWorkerEndpoint(host, port));
        }
        return result;
    }

    private static String required(Properties properties, String key) {
        String value = trim(properties.getProperty(key));
        if (value == null) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value;
    }

    private static String trim(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
