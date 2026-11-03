package io.rowweave.stream.api;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/** Public inert sink provider specification with explicit visibility semantics. */
public final class StreamSinkSpec {
    public enum Visibility { CHECKPOINT_TRANSACTIONAL, EAGER_AT_LEAST_ONCE }

    private final String providerKey;
    private final String providerContractDigest;
    private final byte[] configurationFrame;
    private final Visibility visibility;
    private final List<StreamChangelogMode> acceptedChangelogModes;

    private StreamSinkSpec(String providerKey, String providerContractDigest,
            byte[] configurationFrame, Visibility visibility,
            List<StreamChangelogMode> acceptedChangelogModes) {
        if (providerKey == null || providerContractDigest == null
                || configurationFrame == null || visibility == null
                || acceptedChangelogModes == null
                || acceptedChangelogModes.isEmpty()) {
            throw new IllegalArgumentException("sink fields are required");
        }
        this.providerKey = providerKey;
        this.providerContractDigest = providerContractDigest;
        this.configurationFrame = configurationFrame.clone();
        this.visibility = visibility;
        LinkedHashSet<StreamChangelogMode> unique = new LinkedHashSet<>();
        for (StreamChangelogMode mode : acceptedChangelogModes) {
            if (mode == null || !unique.add(mode)) {
                throw new IllegalArgumentException(
                        "sink changelog capabilities are invalid");
            }
        }
        this.acceptedChangelogModes = Collections.unmodifiableList(
                new ArrayList<>(unique));
    }

    public static StreamSinkSpec eagerAtLeastOnce(String providerKey,
            String providerContractDigest, byte[] configurationFrame) {
        return new StreamSinkSpec(providerKey, providerContractDigest,
                configurationFrame, Visibility.EAGER_AT_LEAST_ONCE,
                legacyModes());
    }

    public static StreamSinkSpec retractEagerAtLeastOnce(String providerKey,
            String providerContractDigest, byte[] configurationFrame) {
        return new StreamSinkSpec(providerKey, providerContractDigest,
                configurationFrame, Visibility.EAGER_AT_LEAST_ONCE,
                Arrays.asList(StreamChangelogMode.APPEND,
                        StreamChangelogMode.UPSERT,
                        StreamChangelogMode.RETRACT));
    }

    public static StreamSinkSpec checkpointTransactional(String providerKey,
            String providerContractDigest, byte[] configurationFrame) {
        return new StreamSinkSpec(providerKey, providerContractDigest,
                configurationFrame, Visibility.CHECKPOINT_TRANSACTIONAL,
                legacyModes());
    }

    public static StreamSinkSpec retractCheckpointTransactional(
            String providerKey, String providerContractDigest,
            byte[] configurationFrame) {
        return new StreamSinkSpec(providerKey, providerContractDigest,
                configurationFrame, Visibility.CHECKPOINT_TRANSACTIONAL,
                Arrays.asList(StreamChangelogMode.APPEND,
                        StreamChangelogMode.UPSERT,
                        StreamChangelogMode.RETRACT));
    }

    public String providerKey() { return providerKey; }
    public String providerContractDigest() { return providerContractDigest; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    public Visibility visibility() { return visibility; }
    public List<StreamChangelogMode> acceptedChangelogModes() {
        return acceptedChangelogModes;
    }

    private static List<StreamChangelogMode> legacyModes() {
        return Arrays.asList(
                StreamChangelogMode.APPEND, StreamChangelogMode.UPSERT);
    }
}
