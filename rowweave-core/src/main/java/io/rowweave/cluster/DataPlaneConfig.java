package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ControlEndpointConfig;
import io.rowweave.cluster.rpc.ControlSecurityProfile;
import io.rowweave.sched.net.DataPlaneHandshakeAdmission;
import io.rowweave.sched.net.ShuffleIngressAdmissionConfig;

import java.net.InetAddress;
import java.util.Properties;

/** Immutable snapshot of the shuffle data plane configuration derived from the control plane profile. */
public final class DataPlaneConfig {

    static final String MAX_HANDSHAKES = "rowweave.shuffle.maxConcurrentHandshakes";
    static final String IO_THREADS = "rowweave.shuffle.ioThreads";
    static final String HANDSHAKE_TIMEOUT_MS = "rowweave.shuffle.handshakeTimeoutMs";
    static final String CONNECT_TIMEOUT_MS = "rowweave.shuffle.connectTimeoutMs";
    static final String HANDSHAKE_RESIDENT_BYTES = "rowweave.shuffle.handshakeResidentBytes";
    static final String AUTH_FRAME_BYTES = "rowweave.shuffle.authFrameBytes";
    static final String FORBIDDEN_SECURITY_PROFILE = "rowweave.shuffle.securityProfile";

    private static final int DEFAULT_MAX_HANDSHAKES = 4;
    private static final long DEFAULT_HANDSHAKE_TIMEOUT_MS = 10_000L;
    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 10_000L;
    private static final long DEFAULT_HANDSHAKE_RESIDENT_BYTES = 24L * 1024L * 1024L;
    private static final int FIXED_AUTH_FRAME_BYTES = 1024;
    private static final long MIB = 1024L * 1024L;

    public enum Protocol {
        LOOPBACK_PLAIN_V0,
        SHUFFLE_MTLS_V1
    }

    private final ControlSecurityProfile controlProfile;
    private final Protocol protocol;
    private final boolean muxEnabled;
    private final InetAddress bindAddress;
    private final int maxConcurrentHandshakes;
    private final int ioThreads;
    private final long handshakeTimeoutMillis;
    private final long connectTimeoutMillis;
    private final long handshakeResidentBytes;
    private final ShuffleIngressAdmissionConfig ingressAdmission;

    private DataPlaneConfig(Properties properties,
                            ControlSecurityProfile controlProfile,
                            InetAddress bindAddress,
                            long maxHeapBytes,
                            boolean aggregateProcessEnvelope,
                            int activeProcessorCount) {
        if (properties == null || controlProfile == null || bindAddress == null) {
            throw new IllegalArgumentException(
                    "properties, control profile and bind address must not be null");
        }
        if (maxHeapBytes <= 0L) {
            throw new IllegalArgumentException("max heap must be positive");
        }
        rejectIndependentSecurityProfile(properties);
        this.controlProfile = controlProfile;
        this.protocol = controlProfile == ControlSecurityProfile.MTLS
                ? Protocol.SHUFFLE_MTLS_V1 : Protocol.LOOPBACK_PLAIN_V0;
        this.muxEnabled = ShuffleTransportConfig.muxEnabled(properties);
        if (controlProfile == ControlSecurityProfile.MTLS && !muxEnabled) {
            throw new IllegalArgumentException("MTLS shuffle requires mux=true");
        }
        if (controlProfile == ControlSecurityProfile.LOOPBACK
                && !bindAddress.isLoopbackAddress()) {
            throw new IllegalArgumentException(
                    "LOOPBACK shuffle bind address must be loopback");
        }
        this.bindAddress = bindAddress;
        this.ingressAdmission = aggregateProcessEnvelope
                ? ShuffleIngressAdmissionConfig.fromProcessProperties(properties)
                : ShuffleIngressAdmissionConfig.fromProperties(properties, maxHeapBytes);
        this.ioThreads = intProperty(properties, IO_THREADS,
                Math.max(2, activeProcessorCount / 2));
        requireRange(IO_THREADS, ioThreads, 1, 64);
        this.maxConcurrentHandshakes = intProperty(
                properties, MAX_HANDSHAKES, DEFAULT_MAX_HANDSHAKES);
        int maximumHandshakes = protocol == Protocol.SHUFFLE_MTLS_V1
                ? DataPlaneHandshakeAdmission.CERTIFIED_MAX_CONCURRENT_HANDSHAKES
                : 32;
        requireRange(MAX_HANDSHAKES, maxConcurrentHandshakes, 2, maximumHandshakes);
        if (protocol == Protocol.SHUFFLE_MTLS_V1
                && maxConcurrentHandshakes > ingressAdmission.maxConnections()) {
            throw new IllegalArgumentException(MAX_HANDSHAKES
                    + " must not exceed rowweave.shuffle.maxConnections");
        }
        this.handshakeTimeoutMillis = longProperty(
                properties, HANDSHAKE_TIMEOUT_MS, DEFAULT_HANDSHAKE_TIMEOUT_MS);
        requireRange(HANDSHAKE_TIMEOUT_MS, handshakeTimeoutMillis, 1_000L, 60_000L);
        this.connectTimeoutMillis = longProperty(
                properties, CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS);
        requireRange(CONNECT_TIMEOUT_MS, connectTimeoutMillis, 1_000L, 60_000L);
        this.handshakeResidentBytes = longProperty(
                properties, HANDSHAKE_RESIDENT_BYTES,
                protocol == Protocol.SHUFFLE_MTLS_V1
                        ? DataPlaneHandshakeAdmission.requiredResidentBytes(
                                maxConcurrentHandshakes)
                        : DEFAULT_HANDSHAKE_RESIDENT_BYTES);
        requireRange(HANDSHAKE_RESIDENT_BYTES, handshakeResidentBytes,
                8L * MIB,
                aggregateProcessEnvelope ? Long.MAX_VALUE : maxHeapBytes / 4L);
        long requiredHandshakeBytes = DataPlaneHandshakeAdmission
                .requiredResidentBytes(maxConcurrentHandshakes);
        if (protocol == Protocol.SHUFFLE_MTLS_V1
                && handshakeResidentBytes < requiredHandshakeBytes) {
            throw new IllegalArgumentException(HANDSHAKE_RESIDENT_BYTES
                    + " is smaller than maxConcurrentHandshakes charge");
        }
        int authFrameBytes = intProperty(
                properties, AUTH_FRAME_BYTES, FIXED_AUTH_FRAME_BYTES);
        if (authFrameBytes != FIXED_AUTH_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    AUTH_FRAME_BYTES + " is fixed at " + FIXED_AUTH_FRAME_BYTES);
        }
        if (protocol == Protocol.SHUFFLE_MTLS_V1) {
            long requiredIngress = ShuffleIngressAdmissionConfig
                    .minimumMtlsV1ResidentBytes(
                            ingressAdmission.maxConnections());
            if (ingressAdmission.ingressResidentBytes() < requiredIngress) {
                throw new IllegalArgumentException(
                        "MTLS ingress resident budget cannot cover connection scratch"
                                + " plus one maximum V1 frame");
            }
        }
    }

    public static DataPlaneConfig fromProperties(Properties properties,
                                                 ControlSecurityConfig controlSecurity,
                                                 long maxHeapBytes) {
        if (controlSecurity == null) {
            throw new IllegalArgumentException("control security must not be null");
        }
        return new DataPlaneConfig(properties, controlSecurity.profile(),
                controlSecurity.bindAddress(), maxHeapBytes, false,
                Runtime.getRuntime().availableProcessors());
    }

    /** Capacity preflight entry: derives data-plane budgets without reading TLS material. */
    public static DataPlaneConfig fromProperties(Properties properties,
                                                 ControlEndpointConfig controlEndpoint,
                                                 long maxHeapBytes) {
        return fromProperties(properties, controlEndpoint, maxHeapBytes,
                Runtime.getRuntime().availableProcessors());
    }

    static DataPlaneConfig fromProperties(Properties properties,
                                          ControlEndpointConfig controlEndpoint,
                                          long maxHeapBytes,
                                          int activeProcessorCount) {
        if (controlEndpoint == null) {
            throw new IllegalArgumentException("control endpoint must not be null");
        }
        if (activeProcessorCount < 1 || activeProcessorCount > 64) {
            throw new IllegalArgumentException(
                    "active processor count must be in [1,64]");
        }
        return new DataPlaneConfig(properties, controlEndpoint.profile(),
                controlEndpoint.bindAddress(), maxHeapBytes, true,
                activeProcessorCount);
    }

    static DataPlaneConfig fromResolvedControlForTesting(
            Properties properties,
            ControlSecurityProfile controlProfile,
            InetAddress bindAddress,
            long maxHeapBytes) {
        return new DataPlaneConfig(
                properties, controlProfile, bindAddress, maxHeapBytes, false,
                Runtime.getRuntime().availableProcessors());
    }

    public Protocol protocol() {
        return protocol;
    }

    public boolean muxEnabled() {
        return muxEnabled;
    }

    public InetAddress bindAddress() {
        return bindAddress;
    }

    public int maxConcurrentHandshakes() {
        return maxConcurrentHandshakes;
    }

    /** Frozen Worker data-plane Netty I/O size in the certified range [1,64]. */
    public int ioThreads() {
        return ioThreads;
    }

    public int inboundHandshakeSlots() {
        return (maxConcurrentHandshakes + 1) / 2;
    }

    public int outboundHandshakeSlots() {
        return maxConcurrentHandshakes / 2;
    }

    public long handshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    public long connectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public long handshakeResidentBytes() {
        return handshakeResidentBytes;
    }

    public int authFrameBytes() {
        return FIXED_AUTH_FRAME_BYTES;
    }

    public ShuffleIngressAdmissionConfig ingressAdmission() {
        return ingressAdmission;
    }

    DataPlaneHandshakeAdmission newHandshakeAdmission() {
        return DataPlaneHandshakeAdmission.create(
                maxConcurrentHandshakes, handshakeResidentBytes);
    }

    public void requireAllowedPeer(InetAddress peerAddress) {
        if (peerAddress == null) {
            throw new IllegalArgumentException("shuffle peer address must not be null");
        }
        if (controlProfile == ControlSecurityProfile.LOOPBACK
                && !peerAddress.isLoopbackAddress()) {
            throw new IllegalArgumentException(
                    "LOOPBACK shuffle rejects non-loopback peer");
        }
    }

    private static void rejectIndependentSecurityProfile(Properties properties) {
        String configured = properties.getProperty(FORBIDDEN_SECURITY_PROFILE);
        if (configured != null && !configured.trim().isEmpty()) {
            throw new IllegalArgumentException(FORBIDDEN_SECURITY_PROFILE
                    + " is forbidden; shuffle security derives from control profile");
        }
    }

    private static int intProperty(Properties properties, String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be an integer", invalid);
        }
    }

    private static long longProperty(Properties properties, String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be a long", invalid);
        }
    }

    private static void requireRange(String key, long value, long minimum, long maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(key + " must be in ["
                    + minimum + ',' + maximum + "] but was " + value);
        }
    }
}
