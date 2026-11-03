package io.rowweave.stream.contract;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/** Inert Stream Sink provider contract with an explicit visibility guarantee. */
public final class StreamSinkBinding implements StreamNodeBinding {
    public enum VisibilityProfile {
        CHECKPOINT_TRANSACTIONAL(1), EAGER_AT_LEAST_ONCE(2);

        private final int id;

        VisibilityProfile(int id) { this.id = id; }
        int wireId() { return id; }

        static VisibilityProfile fromWire(int id) {
            for (VisibilityProfile value : values()) {
                if (value.id == id) return value;
            }
            throw StreamContractSupport.failure(
                    "UNKNOWN_SINK_VISIBILITY", "sink.visibility",
                    "unknown sink visibility");
        }
    }

    private final String providerKey;
    private final String providerContractDigest;
    private final String configurationDigest;
    private final byte[] configurationFrame;
    private final VisibilityProfile visibilityProfile;
    private final List<PortableStreamPlan.ChangelogMode>
            acceptedChangelogModes;

    public StreamSinkBinding(
            String providerKey, String providerContractDigest,
            String configurationDigest, byte[] configurationFrame,
            VisibilityProfile visibilityProfile) {
        this(providerKey, providerContractDigest, configurationDigest,
                configurationFrame, visibilityProfile, Arrays.asList(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.ChangelogMode.UPSERT));
    }

    public StreamSinkBinding(
            String providerKey, String providerContractDigest,
            String configurationDigest, byte[] configurationFrame,
            VisibilityProfile visibilityProfile,
            List<PortableStreamPlan.ChangelogMode> acceptedChangelogModes) {
        this.providerKey = StreamContractSupport.text(
                providerKey, "sink.providerKey", 256);
        this.providerContractDigest = StreamContractSupport.digest(
                providerContractDigest, "sink.contractDigest");
        this.configurationDigest = StreamContractSupport.digest(
                configurationDigest, "sink.configurationDigest");
        this.configurationFrame = StreamContractSupport.frame(
                configurationFrame, "sink.configuration");
        if (!MessageDigest.isEqual(
                this.configurationDigest.getBytes(StandardCharsets.US_ASCII),
                StreamContractSupport.sha256Hex(this.configurationFrame)
                        .getBytes(StandardCharsets.US_ASCII))) {
            throw StreamContractSupport.failure(
                    "CONFIGURATION_DIGEST_MISMATCH", "sink.configurationDigest",
                    "sink configuration digest does not match its canonical frame");
        }
        if (visibilityProfile == null) {
            throw StreamContractSupport.failure(
                    "INVALID_SINK_VISIBILITY", "sink.visibility",
                    "sink visibility is required");
        }
        this.visibilityProfile = visibilityProfile;
        if (acceptedChangelogModes == null
                || acceptedChangelogModes.isEmpty()) {
            throw StreamContractSupport.failure(
                    "INVALID_SINK_CHANGELOG", "sink.changelogModes",
                    "sink must accept at least one changelog mode");
        }
        EnumSet<PortableStreamPlan.ChangelogMode> unique = EnumSet.noneOf(
                PortableStreamPlan.ChangelogMode.class);
        for (PortableStreamPlan.ChangelogMode mode : acceptedChangelogModes) {
            if (mode == null || !unique.add(mode)) {
                throw StreamContractSupport.failure(
                        "INVALID_SINK_CHANGELOG", "sink.changelogModes",
                        "sink changelog modes must be unique");
            }
        }
        this.acceptedChangelogModes = Collections.unmodifiableList(
                new ArrayList<>(unique));
    }

    @Override
    public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.SINK;
    }

    public String providerKey() { return providerKey; }
    public String providerContractDigest() { return providerContractDigest; }
    public String configurationDigest() { return configurationDigest; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    byte[] configurationFrameUnsafe() { return configurationFrame; }
    public VisibilityProfile visibilityProfile() { return visibilityProfile; }
    public List<PortableStreamPlan.ChangelogMode> acceptedChangelogModes() {
        return acceptedChangelogModes;
    }
    public boolean accepts(PortableStreamPlan.ChangelogMode mode) {
        return acceptedChangelogModes.contains(mode);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamSinkBinding)) return false;
        StreamSinkBinding that = (StreamSinkBinding) other;
        return providerKey.equals(that.providerKey)
                && providerContractDigest.equals(that.providerContractDigest)
                && configurationDigest.equals(that.configurationDigest)
                && visibilityProfile == that.visibilityProfile
                && acceptedChangelogModes.equals(
                that.acceptedChangelogModes)
                && Arrays.equals(configurationFrame, that.configurationFrame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerKey, providerContractDigest,
                configurationDigest, visibilityProfile,
                acceptedChangelogModes, Arrays.hashCode(configurationFrame));
    }
}
