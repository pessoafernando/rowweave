package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelOutput;

/** Aligned, globally sequenced merge from parallel state subtasks to one Sink. */
final class StreamParallelOutputMerger {
    private final StreamChannelOutput target;
    private final Source[] inputs;
    private final long[] watermarks;
    private long sequence;
    private long emittedWatermark = Long.MIN_VALUE;
    private CheckpointIdentity pendingCheckpoint;
    private int checkpointCount;
    private StreamEnvelope.WatermarkStatus pendingStatus;
    private long pendingStatusGeneration;
    private int statusCount;
    private String pendingCompletion;
    private int completionCount;
    private int endCount;
    private int closeCount;
    private boolean terminal;

    StreamParallelOutputMerger(
            StreamChannelOutput target, int parallelism) {
        if (target == null || parallelism < 1) {
            throw new IllegalArgumentException(
                    "parallel output merge is incomplete");
        }
        this.target = target;
        this.inputs = new Source[parallelism];
        this.watermarks = new long[parallelism];
        java.util.Arrays.fill(watermarks, Long.MIN_VALUE);
        for (int index = 0; index < parallelism; index++) {
            inputs[index] = new Source(index);
        }
    }

    StreamChannelOutput input(int subtask) {
        if (subtask < 0 || subtask >= inputs.length) {
            throw new IllegalArgumentException("parallel subtask is invalid");
        }
        return inputs[subtask];
    }

    private synchronized void send(int subtask, StreamEnvelope envelope) {
        if (terminal) return;
        switch (envelope.kind()) {
            case RECORD:
                emit(envelope);
                return;
            case WATERMARK:
                watermarks[subtask] = Math.max(
                        watermarks[subtask], envelope.timestamp());
                long minimum = Long.MAX_VALUE;
                for (long watermark : watermarks) {
                    minimum = Math.min(minimum, watermark);
                }
                if (minimum > emittedWatermark) {
                    emittedWatermark = minimum;
                    emit(StreamEnvelope.watermark(
                            target.channelIdentity(), 1L, minimum));
                }
                return;
            case CHECKPOINT_BARRIER:
                if (pendingCheckpoint == null) {
                    pendingCheckpoint = envelope.checkpointIdentity();
                    checkpointCount = 1;
                } else if (pendingCheckpoint.equals(
                        envelope.checkpointIdentity())) {
                    checkpointCount++;
                } else {
                    throw new IllegalArgumentException(
                            "parallel checkpoint barriers crossed");
                }
                if (checkpointCount == inputs.length) {
                    emit(StreamEnvelope.checkpointBarrier(
                            target.channelIdentity(), 1L,
                            pendingCheckpoint));
                    pendingCheckpoint = null;
                    checkpointCount = 0;
                }
                return;
            case WATERMARK_STATUS:
                alignStatus(envelope);
                return;
            case ASYNC_COMPLETION:
                alignCompletion(envelope);
                return;
            case END_OF_INPUT:
                if (++endCount == inputs.length) emit(
                        StreamEnvelope.endOfInput(
                                target.channelIdentity(), 1L));
                return;
            case CANCEL:
                terminal = true;
                target.send(StreamEnvelope.cancel(
                        target.channelIdentity(), next()));
                return;
            case ERROR:
                terminal = true;
                target.send(StreamEnvelope.error(target.channelIdentity(),
                        next(), envelope.errorCode()));
                return;
            default:
                throw new IllegalArgumentException(
                        "unknown parallel output event");
        }
    }

    private void alignStatus(StreamEnvelope envelope) {
        if (pendingStatus == null) {
            pendingStatus = envelope.watermarkStatus();
            pendingStatusGeneration = envelope.watermarkStatusGeneration();
            statusCount = 1;
        } else if (pendingStatus == envelope.watermarkStatus()
                && pendingStatusGeneration
                == envelope.watermarkStatusGeneration()) {
            statusCount++;
        } else {
            throw new IllegalArgumentException(
                    "parallel watermark statuses crossed");
        }
        if (statusCount == inputs.length) {
            emit(StreamEnvelope.watermarkStatus(target.channelIdentity(),
                    1L, pendingStatus, pendingStatusGeneration));
            pendingStatus = null;
            statusCount = 0;
        }
    }

    private void alignCompletion(StreamEnvelope envelope) {
        if (pendingCompletion == null) {
            pendingCompletion = envelope.operationIdentity();
            completionCount = 1;
        } else if (pendingCompletion.equals(envelope.operationIdentity())) {
            completionCount++;
        } else {
            throw new IllegalArgumentException(
                    "parallel async completions crossed");
        }
        if (completionCount == inputs.length) {
            emit(StreamEnvelope.asyncCompletion(target.channelIdentity(),
                    1L, pendingCompletion));
            pendingCompletion = null;
            completionCount = 0;
        }
    }

    private void emit(StreamEnvelope source) {
        target.send(StreamParallelKeyedOutput.rebind(
                source, target.channelIdentity(), next()));
    }

    private long next() {
        return sequence = Math.addExact(sequence, 1L);
    }

    private final class Source implements StreamChannelOutput {
        private final int subtask;
        private boolean closed;

        private Source(int subtask) { this.subtask = subtask; }

        @Override public StreamChannelIdentity channelIdentity() {
            return target.channelIdentity();
        }

        @Override public void send(StreamEnvelope envelope) {
            StreamParallelOutputMerger.this.send(subtask, envelope);
        }

        @Override public void close() {
            synchronized (StreamParallelOutputMerger.this) {
                if (!closed && ++closeCount == inputs.length) target.close();
                closed = true;
            }
        }

        @Override public void cancel() {
            synchronized (StreamParallelOutputMerger.this) {
                if (!terminal) {
                    terminal = true;
                    target.cancel();
                }
            }
        }

        @Override public void fail(Throwable cause) {
            synchronized (StreamParallelOutputMerger.this) {
                if (!terminal) {
                    terminal = true;
                    target.fail(cause);
                }
            }
        }
    }
}
