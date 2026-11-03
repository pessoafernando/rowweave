package io.rowweave.mem.spill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Bounded, non-sensitive snapshot of one spill orphan maintenance pass. */
final class SpillScavengeReport {

    private static final int MAX_SAMPLES = 16;

    private final int inspectedEntries;
    private final int deletedEntries;
    private final long reclaimedBytes;
    private final int liveInstances;
    private final int tooYoungInstances;
    private final int suspiciousInstances;
    private final int failures;
    private final List<String> samples;

    private SpillScavengeReport(Builder builder) {
        this.inspectedEntries = builder.inspectedEntries;
        this.deletedEntries = builder.deletedEntries;
        this.reclaimedBytes = builder.reclaimedBytes;
        this.liveInstances = builder.liveInstances;
        this.tooYoungInstances = builder.tooYoungInstances;
        this.suspiciousInstances = builder.suspiciousInstances;
        this.failures = builder.failures;
        this.samples = Collections.unmodifiableList(new ArrayList<>(builder.samples));
    }

    static SpillScavengeReport empty() {
        return new Builder(0, 0).build();
    }

    int inspectedEntries() {
        return inspectedEntries;
    }

    int deletedEntries() {
        return deletedEntries;
    }

    long reclaimedBytes() {
        return reclaimedBytes;
    }

    int liveInstances() {
        return liveInstances;
    }

    int tooYoungInstances() {
        return tooYoungInstances;
    }

    int suspiciousInstances() {
        return suspiciousInstances;
    }

    int failures() {
        return failures;
    }

    List<String> samples() {
        return samples;
    }

    static final class Builder {
        private final int inspectLimit;
        private final int deleteLimit;
        private int inspectedEntries;
        private int deletedEntries;
        private long reclaimedBytes;
        private int liveInstances;
        private int tooYoungInstances;
        private int suspiciousInstances;
        private int failures;
        private final List<String> samples = new ArrayList<>(MAX_SAMPLES);

        Builder(int inspectLimit, int deleteLimit) {
            this.inspectLimit = inspectLimit;
            this.deleteLimit = deleteLimit;
        }

        boolean canInspect() {
            return inspectedEntries < inspectLimit;
        }

        boolean inspect() {
            if (inspectedEntries >= inspectLimit) return false;
            inspectedEntries++;
            return true;
        }

        boolean canDelete(int count) {
            return count >= 0 && deletedEntries <= deleteLimit - count;
        }

        void deleted(long bytes) {
            deletedEntries++;
            if (bytes > 0) {
                reclaimedBytes = reclaimedBytes > Long.MAX_VALUE - bytes
                        ? Long.MAX_VALUE : reclaimedBytes + bytes;
            }
        }

        void live(String identity) {
            liveInstances++;
            sample(identity, "live");
        }

        void tooYoung(String identity) {
            tooYoungInstances++;
            sample(identity, "grace");
        }

        void suspicious(String identity, String reason) {
            suspiciousInstances++;
            sample(identity, reason);
        }

        void failure(String identity) {
            failures++;
            sample(identity, "io");
        }

        private void sample(String identity, String reason) {
            if (samples.size() >= MAX_SAMPLES) return;
            String safeIdentity = identity != null && identity.matches("[0-9a-f]{32}")
                    ? identity.substring(0, 16) : "unknown";
            samples.add(safeIdentity + ':' + reason);
        }

        SpillScavengeReport build() {
            return new SpillScavengeReport(this);
        }
    }
}
