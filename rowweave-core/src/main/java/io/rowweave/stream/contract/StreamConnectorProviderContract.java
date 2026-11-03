package io.rowweave.stream.contract;

import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable signed metadata for one public Stream connector provider. */
public final class StreamConnectorProviderContract {
    public static final int ABI_VERSION = 1;
    public static final int MAX_CONFIGURATION_BYTES = 1024 * 1024;
    private static final Pattern DEPLOYMENT_ID =
            Pattern.compile("[A-Za-z0-9._-]{1,128}");

    public enum Kind { SOURCE, SINK }
    public enum SourceReplay { EXACT_ORDERED }
    public enum SinkVisibility { EAGER_AT_LEAST_ONCE, CHECKPOINT_TRANSACTIONAL }

    private final String deploymentId;
    private final Kind kind;
    private final String providerKey;
    private final String configurationCodecId;
    private final int maximumConfigurationBytes;
    private final int maximumConcurrentOpens;
    private final long openTimeoutMillis;
    private final SourceReplay sourceReplay;
    private final boolean boundedEndOfInput;
    private final boolean dynamicDiscovery;
    private final boolean residentCdc;
    private final boolean externalCheckpointAcknowledgement;
    private final SinkVisibility sinkVisibility;

    private StreamConnectorProviderContract(
            String deploymentId, Kind kind, String providerKey,
            String configurationCodecId, int maximumConfigurationBytes,
            int maximumConcurrentOpens, long openTimeoutMillis,
            SourceReplay sourceReplay, boolean boundedEndOfInput,
            boolean dynamicDiscovery, boolean residentCdc,
            boolean externalCheckpointAcknowledgement,
            SinkVisibility sinkVisibility) {
        if (deploymentId == null
                || !DEPLOYMENT_ID.matcher(deploymentId).matches()
                || kind == null) {
            throw new IllegalArgumentException("invalid Stream connector identity");
        }
        this.deploymentId = deploymentId;
        this.kind = kind;
        this.providerKey = StreamContractSupport.text(
                providerKey, "connector.providerKey", 256);
        this.configurationCodecId = StreamContractSupport.text(
                configurationCodecId, "connector.configurationCodecId", 256);
        if (maximumConfigurationBytes < 1
                || maximumConfigurationBytes > MAX_CONFIGURATION_BYTES
                || maximumConcurrentOpens < 1
                || maximumConcurrentOpens > 4096
                || openTimeoutMillis < 1L
                || openTimeoutMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid Stream connector resource limits");
        }
        if (kind == Kind.SOURCE) {
            if (sourceReplay == null || sinkVisibility != null
                    || (boundedEndOfInput && dynamicDiscovery)
                    || (externalCheckpointAcknowledgement
                    && !residentCdc)) {
                throw new IllegalArgumentException("invalid Stream source capability set");
            }
        } else if (sinkVisibility == null || sourceReplay != null
                || boundedEndOfInput || dynamicDiscovery || residentCdc
                || externalCheckpointAcknowledgement) {
            throw new IllegalArgumentException("invalid Stream sink capability set");
        }
        this.maximumConfigurationBytes = maximumConfigurationBytes;
        this.maximumConcurrentOpens = maximumConcurrentOpens;
        this.openTimeoutMillis = openTimeoutMillis;
        this.sourceReplay = sourceReplay;
        this.boundedEndOfInput = boundedEndOfInput;
        this.dynamicDiscovery = dynamicDiscovery;
        this.residentCdc = residentCdc;
        this.externalCheckpointAcknowledgement =
                externalCheckpointAcknowledgement;
        this.sinkVisibility = sinkVisibility;
    }

    public static StreamConnectorProviderContract source(
            String deploymentId, String providerKey,
            String configurationCodecId, int maximumConfigurationBytes,
            int maximumConcurrentOpens, long openTimeoutMillis,
            boolean boundedEndOfInput, boolean dynamicDiscovery,
            boolean residentCdc) {
        return source(deploymentId, providerKey, configurationCodecId,
                maximumConfigurationBytes, maximumConcurrentOpens,
                openTimeoutMillis, boundedEndOfInput, dynamicDiscovery,
                residentCdc, false);
    }

    public static StreamConnectorProviderContract source(
            String deploymentId, String providerKey,
            String configurationCodecId, int maximumConfigurationBytes,
            int maximumConcurrentOpens, long openTimeoutMillis,
            boolean boundedEndOfInput, boolean dynamicDiscovery,
            boolean residentCdc,
            boolean externalCheckpointAcknowledgement) {
        return new StreamConnectorProviderContract(
                deploymentId, Kind.SOURCE, providerKey,
                configurationCodecId, maximumConfigurationBytes,
                maximumConcurrentOpens, openTimeoutMillis,
                SourceReplay.EXACT_ORDERED, boundedEndOfInput,
                dynamicDiscovery, residentCdc,
                externalCheckpointAcknowledgement, null);
    }

    public static StreamConnectorProviderContract sink(
            String deploymentId, String providerKey,
            String configurationCodecId, int maximumConfigurationBytes,
            int maximumConcurrentOpens, long openTimeoutMillis,
            SinkVisibility visibility) {
        return new StreamConnectorProviderContract(
                deploymentId, Kind.SINK, providerKey,
                configurationCodecId, maximumConfigurationBytes,
                maximumConcurrentOpens, openTimeoutMillis,
                null, false, false, false, false, visibility);
    }

    public String deploymentId() { return deploymentId; }
    public Kind kind() { return kind; }
    public String providerKey() { return providerKey; }
    public String configurationCodecId() { return configurationCodecId; }
    public int maximumConfigurationBytes() { return maximumConfigurationBytes; }
    public int maximumConcurrentOpens() { return maximumConcurrentOpens; }
    public long openTimeoutMillis() { return openTimeoutMillis; }
    public SourceReplay sourceReplay() { return sourceReplay; }
    public boolean boundedEndOfInput() { return boundedEndOfInput; }
    public boolean dynamicDiscovery() { return dynamicDiscovery; }
    public boolean residentCdc() { return residentCdc; }
    public boolean externalCheckpointAcknowledgement() {
        return externalCheckpointAcknowledgement;
    }
    public SinkVisibility sinkVisibility() { return sinkVisibility; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamConnectorProviderContract)) return false;
        StreamConnectorProviderContract that = (StreamConnectorProviderContract) other;
        return deploymentId.equals(that.deploymentId)
                && kind == that.kind
                && providerKey.equals(that.providerKey)
                && configurationCodecId.equals(that.configurationCodecId)
                && maximumConfigurationBytes == that.maximumConfigurationBytes
                && maximumConcurrentOpens == that.maximumConcurrentOpens
                && openTimeoutMillis == that.openTimeoutMillis
                && sourceReplay == that.sourceReplay
                && boundedEndOfInput == that.boundedEndOfInput
                && dynamicDiscovery == that.dynamicDiscovery
                && residentCdc == that.residentCdc
                && externalCheckpointAcknowledgement
                == that.externalCheckpointAcknowledgement
                && sinkVisibility == that.sinkVisibility;
    }

    @Override public int hashCode() {
        return Objects.hash(deploymentId, kind, providerKey,
                configurationCodecId, maximumConfigurationBytes,
                maximumConcurrentOpens, openTimeoutMillis, sourceReplay,
                boundedEndOfInput, dynamicDiscovery, residentCdc,
                externalCheckpointAcknowledgement, sinkVisibility);
    }
}
