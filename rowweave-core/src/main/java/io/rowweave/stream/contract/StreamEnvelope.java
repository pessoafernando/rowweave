package io.rowweave.stream.contract;

import java.util.Arrays;
import java.util.Objects;

/** Immutable Stream ordered data-lane event. */
public final class StreamEnvelope {
    public enum Kind {
        RECORD(1), WATERMARK(2), CHECKPOINT_BARRIER(3), WATERMARK_STATUS(4),
        END_OF_INPUT(5), ASYNC_COMPLETION(6), CANCEL(7), ERROR(8);

        private final int wireId;

        Kind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Kind fromWire(int wireId) {
            for (Kind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_ENVELOPE_KIND",
                    "envelope.kind", "unknown Stream envelope kind");
        }
    }

    public enum WatermarkStatus {
        ACTIVE(1), IDLE(2);

        private final int wireId;

        WatermarkStatus(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static WatermarkStatus fromWire(int wireId) {
            for (WatermarkStatus value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_WATERMARK_STATUS",
                    "envelope.watermarkStatus", "unknown watermark status");
        }
    }

    private final Kind kind;
    private final StreamChannelIdentity channelIdentity;
    private final long sequence;
    private final Long timestamp;
    private final byte[] recordFrame;
    private final CheckpointIdentity checkpointIdentity;
    private final WatermarkStatus watermarkStatus;
    private final long watermarkStatusGeneration;
    private final String detailIdentity;

    private StreamEnvelope(
            Kind kind, StreamChannelIdentity channelIdentity, long sequence,
            Long timestamp, byte[] recordFrame,
            CheckpointIdentity checkpointIdentity,
            WatermarkStatus watermarkStatus, long watermarkStatusGeneration,
            String detailIdentity) {
        if (kind == null || channelIdentity == null || sequence <= 0L) {
            throw StreamContractSupport.failure("INVALID_ENVELOPE", "envelope",
                    "kind, channel and positive sequence are required");
        }
        this.kind = kind;
        this.channelIdentity = channelIdentity;
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.recordFrame = recordFrame == null ? null
                : StreamContractSupport.frame(recordFrame, "envelope.recordFrame");
        this.checkpointIdentity = checkpointIdentity;
        this.watermarkStatus = watermarkStatus;
        this.watermarkStatusGeneration = watermarkStatusGeneration;
        this.detailIdentity = detailIdentity;
    }

    public static StreamEnvelope record(
            StreamChannelIdentity channel, long sequence,
            long eventTimestamp, byte[] recordFrame) {
        return record(channel, sequence, Long.valueOf(eventTimestamp), recordFrame);
    }

    public static StreamEnvelope record(
            StreamChannelIdentity channel, long sequence, byte[] recordFrame) {
        return record(channel, sequence, null, recordFrame);
    }

    private static StreamEnvelope record(
            StreamChannelIdentity channel, long sequence,
            Long eventTimestamp, byte[] recordFrame) {
        if (recordFrame == null || recordFrame.length == 0) {
            throw StreamContractSupport.failure("INVALID_RECORD", "envelope.recordFrame",
                    "record frame must not be empty");
        }
        return new StreamEnvelope(Kind.RECORD, channel, sequence,
                eventTimestamp, recordFrame, null, null, 0L, null);
    }

    public static StreamEnvelope watermark(
            StreamChannelIdentity channel, long sequence, long timestamp) {
        return new StreamEnvelope(Kind.WATERMARK, channel, sequence,
                timestamp, null, null, null, 0L, null);
    }

    public static StreamEnvelope checkpointBarrier(
            StreamChannelIdentity channel, long sequence,
            CheckpointIdentity checkpoint) {
        if (checkpoint == null || channel == null
                || !channel.runId().equals(checkpoint.runId())
                || !channel.attemptId().equals(checkpoint.attemptId())
                || channel.fenceToken() != checkpoint.fenceToken()) {
            throw StreamContractSupport.failure("CHECKPOINT_SCOPE_MISMATCH",
                    "envelope.checkpoint", "checkpoint does not belong to channel attempt");
        }
        return new StreamEnvelope(Kind.CHECKPOINT_BARRIER, channel, sequence,
                null, null, checkpoint, null, 0L, null);
    }

    public static StreamEnvelope watermarkStatus(
            StreamChannelIdentity channel, long sequence,
            WatermarkStatus status, long generation) {
        if (status == null || generation <= 0L) {
            throw StreamContractSupport.failure("INVALID_WATERMARK_STATUS",
                    "envelope.watermarkStatus",
                    "status and positive generation are required");
        }
        return new StreamEnvelope(Kind.WATERMARK_STATUS, channel, sequence,
                null, null, null, status, generation, null);
    }

    public static StreamEnvelope endOfInput(
            StreamChannelIdentity channel, long sequence) {
        return signal(Kind.END_OF_INPUT, channel, sequence, null);
    }

    public static StreamEnvelope asyncCompletion(
            StreamChannelIdentity channel, long sequence, String operationIdentity) {
        if (operationIdentity == null || operationIdentity.isEmpty()) {
            throw StreamContractSupport.failure("INVALID_ASYNC_COMPLETION",
                    "envelope.operationIdentity", "operation identity is required");
        }
        return signal(Kind.ASYNC_COMPLETION, channel, sequence,
                StreamContractSupport.text(operationIdentity,
                        "envelope.operationIdentity", 256));
    }

    public static StreamEnvelope cancel(
            StreamChannelIdentity channel, long sequence) {
        return signal(Kind.CANCEL, channel, sequence, null);
    }

    public static StreamEnvelope error(
            StreamChannelIdentity channel, long sequence, String errorCode) {
        if (errorCode == null || !errorCode.matches("[A-Z][A-Z0-9_]{0,127}")) {
            throw StreamContractSupport.failure("INVALID_ERROR",
                    "envelope.errorCode", "canonical error code is required");
        }
        return signal(Kind.ERROR, channel, sequence, errorCode);
    }

    private static StreamEnvelope signal(
            Kind kind, StreamChannelIdentity channel, long sequence, String detail) {
        return new StreamEnvelope(kind, channel, sequence,
                null, null, null, null, 0L, detail);
    }

    public Kind kind() { return kind; }
    public StreamChannelIdentity channelIdentity() { return channelIdentity; }
    public long sequence() { return sequence; }

    public boolean hasEventTimestamp() {
        return kind == Kind.RECORD && timestamp != null;
    }

    public long timestamp() {
        if ((kind != Kind.RECORD && kind != Kind.WATERMARK) || timestamp == null) {
            throw new IllegalStateException("event has no timestamp");
        }
        return timestamp;
    }

    public byte[] recordFrame() {
        if (kind != Kind.RECORD) throw new IllegalStateException("not a record");
        return recordFrame.clone();
    }

    byte[] recordFrameUnsafe() { return recordFrame; }

    public CheckpointIdentity checkpointIdentity() {
        if (kind != Kind.CHECKPOINT_BARRIER) {
            throw new IllegalStateException("not a checkpoint barrier");
        }
        return checkpointIdentity;
    }

    public WatermarkStatus watermarkStatus() {
        if (kind != Kind.WATERMARK_STATUS) {
            throw new IllegalStateException("not a watermark status");
        }
        return watermarkStatus;
    }

    public long watermarkStatusGeneration() {
        if (kind != Kind.WATERMARK_STATUS) {
            throw new IllegalStateException("not a watermark status");
        }
        return watermarkStatusGeneration;
    }

    public String operationIdentity() {
        if (kind != Kind.ASYNC_COMPLETION) {
            throw new IllegalStateException("not an async completion");
        }
        return detailIdentity;
    }

    public String errorCode() {
        if (kind != Kind.ERROR) throw new IllegalStateException("not an error");
        return detailIdentity;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamEnvelope)) return false;
        StreamEnvelope that = (StreamEnvelope) other;
        return sequence == that.sequence
                && watermarkStatusGeneration == that.watermarkStatusGeneration
                && kind == that.kind && channelIdentity.equals(that.channelIdentity)
                && Objects.equals(timestamp, that.timestamp)
                && Arrays.equals(recordFrame, that.recordFrame)
                && Objects.equals(checkpointIdentity, that.checkpointIdentity)
                && watermarkStatus == that.watermarkStatus
                && Objects.equals(detailIdentity, that.detailIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, channelIdentity, sequence, timestamp,
                Arrays.hashCode(recordFrame), checkpointIdentity, watermarkStatus,
                watermarkStatusGeneration, detailIdentity);
    }
}
