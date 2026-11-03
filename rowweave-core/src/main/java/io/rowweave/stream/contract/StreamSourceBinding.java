package io.rowweave.stream.contract;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Inert fixed-split Stream source contract; contains no live reader or credential. */
public final class StreamSourceBinding implements StreamNodeBinding {
    public enum ReplayGuarantee {
        NONE(0), EXACT_ORDERED(1);

        private final int id;

        ReplayGuarantee(int id) { this.id = id; }
        int wireId() { return id; }

        static ReplayGuarantee fromWire(int id) {
            for (ReplayGuarantee value : values()) {
                if (value.id == id) return value;
            }
            throw StreamContractSupport.failure(
                    "UNKNOWN_REPLAY_GUARANTEE", "source.replay",
                    "unknown replay guarantee");
        }
    }

    private final String providerKey;
    private final String providerContractDigest;
    private final String configurationDigest;
    private final byte[] configurationFrame;
    private final List<StreamSourceSplit> fixedSplits;
    private final StreamSourceDiscoveryDescriptor discoveryDescriptor;
    private final ReplayGuarantee replayGuarantee;

    public StreamSourceBinding(
            String providerKey, String providerContractDigest,
            String configurationDigest, byte[] configurationFrame,
            List<StreamSourceSplit> fixedSplits,
            ReplayGuarantee replayGuarantee) {
        this(providerKey, providerContractDigest, configurationDigest,
                configurationFrame, fixedSplits, null, replayGuarantee);
    }

    public StreamSourceBinding(
            String providerKey, String providerContractDigest,
            String configurationDigest, byte[] configurationFrame,
            StreamSourceDiscoveryDescriptor discoveryDescriptor,
            ReplayGuarantee replayGuarantee) {
        this(providerKey, providerContractDigest, configurationDigest,
                configurationFrame, Collections.<StreamSourceSplit>emptyList(),
                discoveryDescriptor, replayGuarantee);
    }

    private StreamSourceBinding(
            String providerKey, String providerContractDigest,
            String configurationDigest, byte[] configurationFrame,
            List<StreamSourceSplit> fixedSplits,
            StreamSourceDiscoveryDescriptor discoveryDescriptor,
            ReplayGuarantee replayGuarantee) {
        this.providerKey = StreamContractSupport.text(
                providerKey, "source.providerKey", 256);
        this.providerContractDigest = StreamContractSupport.digest(
                providerContractDigest, "source.contractDigest");
        this.configurationDigest = StreamContractSupport.digest(
                configurationDigest, "source.configurationDigest");
        this.configurationFrame = StreamContractSupport.frame(
                configurationFrame, "source.configuration");
        if (!MessageDigest.isEqual(
                this.configurationDigest.getBytes(StandardCharsets.US_ASCII),
                StreamContractSupport.sha256Hex(this.configurationFrame)
                        .getBytes(StandardCharsets.US_ASCII))) {
            throw StreamContractSupport.failure(
                    "CONFIGURATION_DIGEST_MISMATCH", "source.configurationDigest",
                    "source configuration digest does not match its canonical frame");
        }
        if (fixedSplits == null
                || (fixedSplits.isEmpty() == (discoveryDescriptor == null))
                || fixedSplits.size() > 4096) {
            throw StreamContractSupport.failure(
                    "INVALID_SPLIT_SET", "source.splits",
                    "exactly one fixed or dynamic split contract is required");
        }
        List<StreamSourceSplit> copied = new ArrayList<>(fixedSplits.size());
        Set<String> splitIds = new HashSet<>();
        for (StreamSourceSplit split : fixedSplits) {
            if (split == null) {
                throw StreamContractSupport.failure(
                        "INVALID_SPLIT_SET", "source.splits",
                        "source split is required");
            }
            if (!splitIds.add(split.splitId())) {
                throw StreamContractSupport.failure(
                        "DUPLICATE_SPLIT_ID", "source.splits.splitId",
                        "fixed split identities must be unique");
            }
            copied.add(split);
        }
        copied.sort(Comparator.comparing(StreamSourceSplit::splitId));
        this.fixedSplits = Collections.unmodifiableList(copied);
        this.discoveryDescriptor = discoveryDescriptor;
        if (replayGuarantee != ReplayGuarantee.EXACT_ORDERED) {
            throw StreamContractSupport.failure(
                    "SOURCE_NOT_REPLAYABLE", "source.replayGuarantee",
                    "production Stream requires exact ordered replay");
        }
        this.replayGuarantee = replayGuarantee;
    }

    @Override
    public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.SOURCE;
    }

    public String providerKey() { return providerKey; }
    public String providerContractDigest() { return providerContractDigest; }
    public String configurationDigest() { return configurationDigest; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    public List<StreamSourceSplit> fixedSplits() { return fixedSplits; }
    public boolean dynamicDiscovery() { return discoveryDescriptor != null; }
    public StreamSourceDiscoveryDescriptor discoveryDescriptor() {
        if (discoveryDescriptor == null) {
            throw new IllegalStateException(
                    "fixed source has no discovery descriptor");
        }
        return discoveryDescriptor;
    }
    public ReplayGuarantee replayGuarantee() { return replayGuarantee; }
    byte[] configurationFrameUnsafe() { return configurationFrame; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamSourceBinding)) return false;
        StreamSourceBinding that = (StreamSourceBinding) other;
        return providerKey.equals(that.providerKey)
                && providerContractDigest.equals(that.providerContractDigest)
                && configurationDigest.equals(that.configurationDigest)
                && replayGuarantee == that.replayGuarantee
                && Arrays.equals(configurationFrame, that.configurationFrame)
                && fixedSplits.equals(that.fixedSplits)
                && Objects.equals(discoveryDescriptor,
                that.discoveryDescriptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerKey, providerContractDigest,
                configurationDigest, replayGuarantee,
                Arrays.hashCode(configurationFrame), fixedSplits,
                discoveryDescriptor);
    }
}
