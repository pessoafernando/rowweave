package io.rowweave.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable observation snapshot for one pipeline stage.
 * role=SOURCE/STAGE/SINK; state=RUNNING/FINISHED/FAILED;
 * opChain=operator typeId chain (source-side first, sink-side last).
 */
public final class StageView {

    public static final String SOURCE_REPLAY_EXACT = "EXACT_ORDERED_FROM_START";
    public static final String SOURCE_REPLAY_SINGLE_ATTEMPT = "SINGLE_ATTEMPT_NO_RECOVERY";
    public static final String SOURCE_REPLAY_MIXED = "MIXED";

    private final int index;
    private final String role;
    private final String state;
    private final String opChain;
    private final long outputRows;
    private final long inputRows;
    private final String workerId;
    private final String error;
    private final List<Integer> inputs;
    /**
     * Channel type per inbound edge, aligned with {@code inputs}.
     * "local" = same-worker direct channel, "remote" = cross-worker socket,
     * "mixed" = fan-out with both types. Absent/empty = treat as remote.
     */
    private final List<String> inputChannels;
    private final long startTime;
    private final long endTime;
    private final int parallelism;
    private final List<InstanceView> instances;
    private final int sourceSplitCount;
    private final String sourceReplayMode;
    private final String sourceDescriptorDigestPrefix;

    /** Observation for one parallel instance of a stage running on a worker. */
    public static final class InstanceView {
        public final int instanceIndex;
        public final String workerId;
        public final String state;
        public final long outputRows;
        public final long startTime;
        public final long endTime;
        public final long inputRows;

        public InstanceView(int instanceIndex, String workerId, String state, long outputRows) {
            this(instanceIndex, workerId, state, outputRows, 0, 0, 0);
        }

        public InstanceView(int instanceIndex, String workerId, String state,
                            long outputRows, long startTime, long endTime) {
            this(instanceIndex, workerId, state, outputRows, startTime, endTime, 0);
        }

        public InstanceView(int instanceIndex, String workerId, String state,
                            long outputRows, long startTime, long endTime, long inputRows) {
            this.instanceIndex = instanceIndex;
            this.workerId = workerId;
            this.state = state;
            this.outputRows = outputRows;
            this.startTime = startTime;
            this.endTime = endTime;
            this.inputRows = inputRows;
        }
    }

    private StageView(Builder builder) {
        this.index = builder.index;
        this.role = builder.role;
        this.state = builder.state;
        this.opChain = builder.opChain;
        this.outputRows = builder.outputRows;
        this.inputRows = builder.inputRows;
        this.workerId = builder.workerId;
        this.error = builder.error;
        this.inputs = Collections.unmodifiableList(new ArrayList<>(builder.inputs));
        this.inputChannels = Collections.unmodifiableList(new ArrayList<>(builder.inputChannels));
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.parallelism = builder.parallelism;
        this.instances = Collections.unmodifiableList(new ArrayList<>(builder.instances));
        this.sourceSplitCount = builder.sourceSplitCount;
        this.sourceReplayMode = builder.sourceReplayMode;
        this.sourceDescriptorDigestPrefix = builder.sourceDescriptorDigestPrefix;
    }

    public int index() { return index; }
    public String role() { return role; }
    public String state() { return state; }
    public String opChain() { return opChain; }
    public long outputRows() { return outputRows; }
    public long inputRows() { return inputRows; }
    public String workerId() { return workerId; }
    public String error() { return error; }
    public List<Integer> inputs() { return inputs; }
    public List<String> inputChannels() { return inputChannels; }
    public long startTime() { return startTime; }
    public long endTime() { return endTime; }
    public int parallelism() { return parallelism; }
    public List<InstanceView> instances() { return instances; }
    public int sourceSplitCount() { return sourceSplitCount; }
    public String sourceReplayMode() { return sourceReplayMode; }
    public String sourceDescriptorDigestPrefix() { return sourceDescriptorDigestPrefix; }

    public static Builder builder(int index) { return new Builder(index); }

    public static final class Builder {
        private final int index;
        private String role = "STAGE";
        private String state = "RUNNING";
        private String opChain = "";
        private long outputRows = 0;
        private long inputRows = 0;
        private String workerId;
        private String error;
        private final List<Integer> inputs = new ArrayList<>();
        private final List<String> inputChannels = new ArrayList<>();
        private long startTime = 0;
        private long endTime = 0;
        private int parallelism = 1;
        private final List<InstanceView> instances = new ArrayList<>();
        private int sourceSplitCount;
        private String sourceReplayMode;
        private String sourceDescriptorDigestPrefix;

        private Builder(int index) { this.index = index; }

        public Builder role(String value) { this.role = value; return this; }
        public Builder state(String value) { this.state = value; return this; }
        public Builder opChain(String value) { this.opChain = value; return this; }
        public Builder outputRows(long value) { this.outputRows = value; return this; }
        public Builder inputRows(long value) { this.inputRows = value; return this; }
        public Builder workerId(String value) { this.workerId = value; return this; }
        public Builder error(String value) { this.error = value; return this; }
        public Builder addInput(int stageIndex) { this.inputs.add(stageIndex); return this; }
        public Builder addInput(int stageIndex, String channel) {
            this.inputs.add(stageIndex);
            this.inputChannels.add(channel);
            return this;
        }
        public Builder startTime(long value) { this.startTime = value; return this; }
        public Builder endTime(long value) { this.endTime = value; return this; }
        public Builder parallelism(int value) { this.parallelism = value; return this; }
        public Builder addInstance(InstanceView instance) { this.instances.add(instance); return this; }

        public Builder sourceSummary(int splitCount, String replayMode,
                                     String descriptorDigestPrefix) {
            if (splitCount < 1) {
                throw new IllegalArgumentException("source split count must be positive");
            }
            if (!SOURCE_REPLAY_EXACT.equals(replayMode)
                    && !SOURCE_REPLAY_SINGLE_ATTEMPT.equals(replayMode)
                    && !SOURCE_REPLAY_MIXED.equals(replayMode)) {
                throw new IllegalArgumentException("unknown source replay mode");
            }
            if (!isLowerHexPrefix(descriptorDigestPrefix)) {
                throw new IllegalArgumentException(
                        "source descriptor digest prefix must be 16 lowercase hex characters");
            }
            this.sourceSplitCount = splitCount;
            this.sourceReplayMode = replayMode;
            this.sourceDescriptorDigestPrefix = descriptorDigestPrefix;
            return this;
        }

        public StageView build() { return new StageView(this); }

        private static boolean isLowerHexPrefix(String value) {
            if (value == null || value.length() != 16) return false;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
            }
            return true;
        }
    }
}
