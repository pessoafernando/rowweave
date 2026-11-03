package io.rowweave.cluster;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import io.rowweave.sched.net.DataPlaneHandshakeAdmission;

/**
 * The verifiable proof of the JSSE build and properties, frozen by the earliest transport
 * security bootstrap of the process, together with the single gate holding worker data plane
 * ownership. The object retains no SSLContext, key, password or manager.
 */
public final class ProcessTlsEnvelope {

    static final String HANDSHAKE_LIMIT_PROPERTY = "jdk.tls.maxHandshakeMessageSize";
    static final String CHAIN_LIMIT_PROPERTY = "jdk.tls.maxCertificateChainLength";

    private final String javaVendor;
    private final String javaRuntimeVersion;
    private final String javaVmName;
    private final String providerName;
    private final String providerClassName;
    private final String providerVersion;
    private final String configuredHandshakeLimit;
    private final String configuredChainLimit;
    private final boolean handshakeLimitCameFromJvmStartup;
    private final boolean chainLimitCameFromJvmStartup;
    private final byte[] configurationDigest;
    private final AtomicBoolean dataPlaneOwnerClaimed = new AtomicBoolean();

    ProcessTlsEnvelope(String javaVendor,
                       String javaRuntimeVersion,
                       String javaVmName,
                       Provider provider,
                       String configuredHandshakeLimit,
                       String configuredChainLimit) {
        this(javaVendor, javaRuntimeVersion, javaVmName, provider,
                configuredHandshakeLimit, configuredChainLimit, System.getProperties());
    }

    ProcessTlsEnvelope(String javaVendor,
                       String javaRuntimeVersion,
                       String javaVmName,
                       Provider provider,
                       String configuredHandshakeLimit,
                       String configuredChainLimit,
                       Properties startupProperties) {
        this.javaVendor = required("java.vendor", javaVendor);
        this.javaRuntimeVersion = required("java.runtime.version", javaRuntimeVersion);
        this.javaVmName = required("java.vm.name", javaVmName);
        if (provider == null) {
            throw new IllegalStateException("certified JSSE provider is unavailable");
        }
        this.providerName = required("JSSE provider name", provider.getName());
        this.providerClassName = provider.getClass().getName();
        this.providerVersion = Double.toString(provider.getVersion());
        this.configuredHandshakeLimit = configuredHandshakeLimit;
        this.configuredChainLimit = configuredChainLimit;
        this.handshakeLimitCameFromJvmStartup = cameFromJvmStartup(
                HANDSHAKE_LIMIT_PROPERTY, configuredHandshakeLimit);
        this.chainLimitCameFromJvmStartup = cameFromJvmStartup(
                CHAIN_LIMIT_PROPERTY, configuredChainLimit);
        this.configurationDigest = configurationDigest(startupProperties);
    }

    public String javaVendor() {
        return javaVendor;
    }

    public String javaRuntimeVersion() {
        return javaRuntimeVersion;
    }

    public String javaVmName() {
        return javaVmName;
    }

    public String providerName() {
        return providerName;
    }

    public String providerClassName() {
        return providerClassName;
    }

    public String providerVersion() {
        return providerVersion;
    }

    public OptionalInt configuredMaxHandshakeMessageBytes() {
        return optionalCanonicalInt(
                HANDSHAKE_LIMIT_PROPERTY, configuredHandshakeLimit, 4_096, 65_536);
    }

    public OptionalInt configuredMaxCertificateChainLength() {
        return optionalCanonicalInt(
                CHAIN_LIMIT_PROPERTY, configuredChainLimit, 1, 10);
    }

    /**
     * Closed gate before the MTLS data plane is enabled; LOOPBACK freezes a snapshot only and
     * does not claim a certified build.
     */
    public void requireCertifiedForMtls() {
        requireNoNettyOverrides();
        requireJvmStartupOrigin(HANDSHAKE_LIMIT_PROPERTY, configuredHandshakeLimit,
                handshakeLimitCameFromJvmStartup);
        requireJvmStartupOrigin(CHAIN_LIMIT_PROPERTY, configuredChainLimit,
                chainLimitCameFromJvmStartup);
        configuredMaxHandshakeMessageBytes();
        configuredMaxCertificateChainLength();
        CertifiedTlsBuildMatrix.requireCertified(this);
        requireCurrentProcessMatch();
    }

    /**
     * Drift in runtime properties, providers or the build may invalidate the proof, but may
     * never rewrite a frozen value.
     */
    public void requireCurrentProcessMatch() {
        requireNoNettyOverrides();
        requireSame("java.vendor", javaVendor, System.getProperty("java.vendor"));
        requireSame("java.runtime.version", javaRuntimeVersion,
                System.getProperty("java.runtime.version"));
        requireSame("java.vm.name", javaVmName, System.getProperty("java.vm.name"));
        requireSame(HANDSHAKE_LIMIT_PROPERTY, configuredHandshakeLimit,
                System.getProperty(HANDSHAKE_LIMIT_PROPERTY));
        requireSame(CHAIN_LIMIT_PROPERTY, configuredChainLimit,
                System.getProperty(CHAIN_LIMIT_PROPERTY));
        Provider current = Security.getProvider("SunJSSE");
        if (current == null
                || !providerName.equals(current.getName())
                || !providerClassName.equals(current.getClass().getName())
                || !providerVersion.equals(Double.toString(current.getVersion()))) {
            throw new IllegalStateException("TLS provider drift after process bootstrap");
        }
    }

    /**
     * Compares irreversible fingerprints of the start-up configuration only; the proof keeps
     * and exports no key or trust password.
     */
    public void requireConfigurationMatch(Properties properties) {
        if (!MessageDigest.isEqual(configurationDigest, configurationDigest(properties))) {
            throw new IllegalStateException(
                    "RowWeave transport/deployment configuration differs from process bootstrap");
        }
    }

    public void requireConfigurationDigest(byte[] candidate) {
        if (candidate == null || !MessageDigest.isEqual(configurationDigest, candidate)) {
            throw new IllegalStateException(
                    "control security configuration differs from process bootstrap");
        }
    }

    public void requireCurrentBootstrap() {
        if (this != TransportSecurityBootstrap.current()) {
            throw new IllegalStateException(
                    "process TLS bootstrap proof is not current");
        }
    }

    /**
     * An attested process may create the data-plane owner for its single Worker runtime exactly once. If
     * constructing the runtime fails after the claim, the process stays fail-closed: it must be restarted
     * rather than minting a second data-plane owner.
     */
    public DataPlaneOwner claimDataPlaneOwner(DataPlaneConfig dataPlaneConfig) {
        if (dataPlaneConfig == null
                || dataPlaneConfig.protocol() != DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1) {
            throw new IllegalArgumentException(
                    "certified data-plane owner requires SHUFFLE_MTLS_V1 config");
        }
        requireCurrentBootstrap();
        requireCertifiedForMtls();
        if (!dataPlaneOwnerClaimed.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "process data-plane admission owner was already claimed");
        }
        return new DataPlaneOwner(dataPlaneConfig.newHandshakeAdmission());
    }

    public static final class DataPlaneOwner {
        private final DataPlaneHandshakeAdmission admission;
        private final AtomicBoolean contextClaimed = new AtomicBoolean();

        private DataPlaneOwner(DataPlaneHandshakeAdmission admission) {
            this.admission = admission;
        }

        public DataPlaneHandshakeAdmission claimContextAdmission() {
            if (!contextClaimed.compareAndSet(false, true)) {
                throw new IllegalStateException(
                        "process data-plane TLS context was already created");
            }
            return admission;
        }

        public DataPlaneHandshakeAdmission admission() {
            return admission;
        }
    }

    /**
     * A fingerprint of origin for a configuration object to hold: irreversible, and without
     * plain property values.
     */
    public static byte[] configurationDigest(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("configuration properties are required");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            List<String> names = new ArrayList<>();
            for (String name : properties.stringPropertyNames()) {
                if (name.startsWith("rowweave.")
                        || name.startsWith("io.netty.")
                        || HANDSHAKE_LIMIT_PROPERTY.equals(name)
                        || CHAIN_LIMIT_PROPERTY.equals(name)) {
                    if (!name.toLowerCase(java.util.Locale.ROOT).contains("password")) {
                        names.add(name);
                    }
                }
            }
            Collections.sort(names);
            for (String name : names) {
                updateLengthPrefixed(digest, name);
                updateLengthPrefixed(digest, properties.getProperty(name));
            }
            return digest.digest();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void updateLengthPrefixed(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static OptionalInt optionalCanonicalInt(
            String name, String configured, int minimum, int maximum) {
        if (configured == null) return OptionalInt.empty();
        if (configured.isEmpty() || !configured.equals(configured.trim())) {
            throw new IllegalArgumentException(name + " must be canonical decimal");
        }
        try {
            int value = Integer.parseInt(configured);
            if (value < minimum || value > maximum
                    || !Integer.toString(value).equals(configured)) {
                throw new NumberFormatException("out of range or noncanonical");
            }
            return OptionalInt.of(value);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(name + " must be in ["
                    + minimum + ',' + maximum + "]", invalid);
        }
    }

    private static boolean cameFromJvmStartup(String name, String configured) {
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        String prefix = "-D" + name + '=';
        int matches = 0;
        String startupValue = null;
        for (String argument : arguments) {
            if (argument.equals("-D" + name)) {
                matches++;
                startupValue = "";
            } else if (argument.startsWith(prefix)) {
                matches++;
                startupValue = argument.substring(prefix.length());
            }
        }
        return matches == 0 ? configured == null
                : matches == 1 && java.util.Objects.equals(configured, startupValue);
    }

    private static void requireNoNettyOverrides() {
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (argument.startsWith("-Dio.netty.")) {
                throw new IllegalStateException(
                        "unsupported io.netty.* startup property for certified MTLS: "
                                + argument.substring(2).split("=", 2)[0]);
            }
        }
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith("io.netty.")) {
                throw new IllegalStateException(
                        "unsupported io.netty.* runtime property for certified MTLS: "
                                + name);
            }
        }
    }

    private static void requireJvmStartupOrigin(
            String name, String configured, boolean cameFromJvmStartup) {
        if (!cameFromJvmStartup) {
            throw new IllegalStateException(name
                    + " must be supplied only by one matching JVM startup -D argument");
        }
        if (configured != null && System.getProperty(name) == null) {
            throw new IllegalStateException(name + " was removed after JVM startup");
        }
    }

    private static String required(String name, String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(name + " is unavailable");
        }
        return value;
    }

    private static void requireSame(String name, String expected, String actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new IllegalStateException(name + " drift after process bootstrap");
        }
    }
}
