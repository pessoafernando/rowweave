package io.rowweave.stream.contract;

import java.util.Objects;

/** Fenced identity of one ordered Stream data lane. */
public final class StreamChannelIdentity {
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long assignmentGeneration;
    private final String channelId;
    private final long channelEpoch;

    public StreamChannelIdentity(
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, String channelId, long channelEpoch) {
        this.runId = StreamContractSupport.text(runId, "channel.runId", 256);
        this.attemptId = StreamContractSupport.text(attemptId, "channel.attemptId", 256);
        this.channelId = StreamContractSupport.text(channelId, "channel.channelId", 256);
        if (fenceToken <= 0L || assignmentGeneration <= 0L || channelEpoch <= 0L) {
            throw StreamContractSupport.failure("INVALID_CHANNEL_IDENTITY", "channel",
                    "channel generations must be positive");
        }
        this.fenceToken = fenceToken;
        this.assignmentGeneration = assignmentGeneration;
        this.channelEpoch = channelEpoch;
    }

    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String channelId() { return channelId; }
    public long channelEpoch() { return channelEpoch; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamChannelIdentity)) return false;
        StreamChannelIdentity that = (StreamChannelIdentity) other;
        return fenceToken == that.fenceToken
                && assignmentGeneration == that.assignmentGeneration
                && channelEpoch == that.channelEpoch && runId.equals(that.runId)
                && attemptId.equals(that.attemptId) && channelId.equals(that.channelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId, attemptId, fenceToken,
                assignmentGeneration, channelId, channelEpoch);
    }
}
