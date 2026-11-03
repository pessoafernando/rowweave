package io.rowweave.cluster.rpc;

import io.rowweave.cluster.ProcessTlsEnvelope;

import java.net.InetAddress;
import java.util.Properties;

/** Control plane security profile together with its validated bind and publish configuration. */
public final class ControlSecurityConfig {

    public static final String PROFILE_PROPERTY = "rowweave.control.securityProfile";
    public static final String HOST_PROPERTY = "rowweave.control.host";

    private final ControlSecurityProfile profile;
    private final String advertisedHost;
    private final InetAddress bindAddress;
    private final ControlTlsContext tlsContext;
    private final RpcSecurityMetadata securityMetadata;
    private final byte[] sourceConfigurationDigest;
    private final ProcessTlsEnvelope certifiedOrigin;

    private ControlSecurityConfig(ControlSecurityProfile profile, String advertisedHost,
                                  InetAddress bindAddress, ControlTlsContext tlsContext,
                                  RpcSecurityMetadata securityMetadata,
                                  byte[] sourceConfigurationDigest,
                                  ProcessTlsEnvelope certifiedOrigin) {
        this.profile = profile;
        this.advertisedHost = advertisedHost;
        this.bindAddress = bindAddress;
        this.tlsContext = tlsContext;
        this.securityMetadata = securityMetadata;
        this.sourceConfigurationDigest = sourceConfigurationDigest.clone();
        this.certifiedOrigin = certifiedOrigin;
    }

    public static ControlSecurityConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("control security properties must not be null");
        }
        ControlEndpointConfig endpoint = ControlEndpointConfig.fromProperties(properties);
        ControlTlsContext tls = endpoint.profile() == ControlSecurityProfile.MTLS
                ? ControlTlsContext.load(properties) : null;
        RpcSecurityMetadata metadata = tls == null ? null : RpcSecurityMetadata.of(
                io.rowweave.deployment.VerifiedDeploymentSet.emptyDeploymentSetId(),
                tls.authzPolicy().authzSetId());
        return new ControlSecurityConfig(endpoint.profile(), endpoint.advertisedHost(),
                endpoint.bindAddress(), tls, metadata,
                ProcessTlsEnvelope.configurationDigest(properties), null);
    }

    /** Production entry point for the data plane: attests the build and the full RowWeave start-up snapshot before any TLS material is read or JSSE initialised. */
    public static ControlSecurityConfig fromProperties(
            Properties properties, ProcessTlsEnvelope processTlsEnvelope) {
        if (properties == null) {
            throw new IllegalArgumentException("control security properties must not be null");
        }
        Properties startupProperties = new Properties();
        startupProperties.putAll(properties);
        if (ControlSecurityProfile.parse(startupProperties.getProperty(PROFILE_PROPERTY))
                != ControlSecurityProfile.MTLS) {
            throw new IllegalArgumentException(
                    "certified TLS configuration factory requires MTLS profile");
        }
        if (processTlsEnvelope == null) {
            throw new IllegalStateException(
                    "MTLS requires current process bootstrap proof");
        }
        processTlsEnvelope.requireCurrentBootstrap();
        processTlsEnvelope.requireCertifiedForMtls();
        processTlsEnvelope.requireConfigurationMatch(startupProperties);
        ControlSecurityConfig loaded = fromProperties(startupProperties);
        return new ControlSecurityConfig(
                loaded.profile, loaded.advertisedHost, loaded.bindAddress,
                loaded.tlsContext, loaded.securityMetadata,
                loaded.sourceConfigurationDigest, processTlsEnvelope);
    }

    public static ControlSecurityConfig fromSystemProperties() {
        return fromProperties(System.getProperties());
    }

    public ControlSecurityProfile profile() {
        return profile;
    }

    public String advertisedHost() {
        return advertisedHost;
    }

    public InetAddress bindAddress() {
        return bindAddress;
    }

    ControlTlsContext tlsContext() { return tlsContext; }

    public RpcSecurityMetadata securityMetadata() { return securityMetadata; }

    /** Creates the read-only data plane TLS capability, bound to the attestation of the earliest bootstrap in this process. */
    public DataPlaneSecurityContext dataPlaneSecurityContext(
            ProcessTlsEnvelope processTlsEnvelope, long handshakeTimeoutMillis,
            ProcessTlsEnvelope.DataPlaneOwner dataPlaneOwner) {
        if (profile != ControlSecurityProfile.MTLS || tlsContext == null) {
            throw new IllegalStateException("data-plane TLS requires MTLS control profile");
        }
        if (processTlsEnvelope == null) {
            throw new IllegalStateException(
                    "data-plane TLS requires current process bootstrap proof");
        }
        if (dataPlaneOwner == null) {
            throw new IllegalStateException(
                    "data-plane TLS requires process-owned admission");
        }
        processTlsEnvelope.requireCurrentBootstrap();
        processTlsEnvelope.requireCertifiedForMtls();
        requireProcessConfiguration(processTlsEnvelope);
        return new DataPlaneSecurityContext(
                tlsContext, securityMetadata, handshakeTimeoutMillis,
                dataPlaneOwner.claimContextAdmission(), processTlsEnvelope);
    }

    /** Derives the data REGISTER claim of a worker from the same start-up control identity. */
    public DataPlaneRegistration workerDataRegistration() {
        if (profile == ControlSecurityProfile.LOOPBACK) {
            return DataPlaneRegistration.loopbackPlainV0();
        }
        if (!tlsContext.localRoles().contains(ControlRole.WORKER)) {
            throw new IllegalStateException(
                    "MTLS Worker requires local certificate role WORKER");
        }
        return DataPlaneRegistration.shuffleMtlsV1(
                decodeFingerprint(tlsContext.localFingerprint()));
    }

    /** Replaces the empty built-in set with the identity of an actually verified local set. */
    public ControlSecurityConfig withDeploymentSetId(String deploymentSetId) {
        if (profile != ControlSecurityProfile.MTLS) return this;
        return new ControlSecurityConfig(profile, advertisedHost, bindAddress, tlsContext,
                RpcSecurityMetadata.of(deploymentSetId, tlsContext.authzPolicy().authzSetId()),
                sourceConfigurationDigest, certifiedOrigin);
    }

    public void requireProcessConfiguration(ProcessTlsEnvelope processTlsEnvelope) {
        if (processTlsEnvelope == null) {
            throw new IllegalStateException("process TLS bootstrap proof is required");
        }
        if (certifiedOrigin != processTlsEnvelope) {
            throw new IllegalStateException(
                    "control security configuration lacks certified bootstrap origin");
        }
        processTlsEnvelope.requireConfigurationDigest(sourceConfigurationDigest);
    }

    /** Aligns the cluster topology with the local certificate role before any role resource is created. */
    public void requireLocalRole(ControlRole requiredRole) {
        if (requiredRole == null) {
            throw new IllegalArgumentException("required control role must not be null");
        }
        if (profile == ControlSecurityProfile.MTLS
                && (tlsContext == null
                || !tlsContext.localRoles().contains(requiredRole))) {
            throw new IllegalStateException(
                    "MTLS process topology requires local " + requiredRole + " role");
        }
    }

    /** Configuration loading and transport are both closed here; the call site stays so a future profile cannot fall back silently. */
    public void requireImplementedTransport() {
        if (profile == ControlSecurityProfile.MTLS && tlsContext == null) {
            throw new IllegalStateException("MTLS control transport material is unavailable");
        }
    }

    private static byte[] decodeFingerprint(String hex) {
        if (hex == null || hex.length() != 64) {
            throw new IllegalStateException("local certificate fingerprint is unavailable");
        }
        byte[] decoded = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        for (int i = 0; i < decoded.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalStateException("local certificate fingerprint is invalid");
            }
            decoded[i] = (byte) ((high << 4) | low);
        }
        return decoded;
    }
}
