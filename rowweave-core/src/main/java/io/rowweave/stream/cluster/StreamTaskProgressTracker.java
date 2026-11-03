package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelMetrics;
import io.rowweave.stream.runtime.StreamChannelOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Transport-neutral observer attached once to every operator task lifecycle. */
final class StreamTaskProgressTracker {
    private volatile StreamControlMessage command;
    private final StreamTaskIdentity identity;
    private final boolean sourceBoundary;
    private final boolean sinkBoundary;
    private final List<StreamChannelOutput> outputs;

    private long sampleSequence;
    private long recordsRead;
    private long recordsWritten;
    private long bytesRead;
    private long bytesWritten;
    private long operatorWatermark = Long.MIN_VALUE;
    private boolean watermarkIdle;

    StreamTaskProgressTracker(StreamControlMessage command,
            StreamTaskIdentity identity, boolean sourceBoundary,
            boolean sinkBoundary,
            List<? extends StreamChannelOutput> outputs) {
        if (command == null || identity == null || outputs == null
                || sourceBoundary && sinkBoundary) {
            throw new IllegalArgumentException(
                    "Stream task progress tracker is incomplete");
        }
        this.command = command;
        this.identity = identity;
        this.sourceBoundary = sourceBoundary;
        this.sinkBoundary = sinkBoundary;
        ArrayList<StreamChannelOutput> copy = new ArrayList<>(outputs.size());
        for (StreamChannelOutput output : outputs) {
            if (output == null) {
                throw new IllegalArgumentException(
                        "Stream task output is required");
            }
            copy.add(output);
        }
        this.outputs = Collections.unmodifiableList(copy);
    }

    StreamChannelOutput observeOutput(StreamChannelOutput output) {
        if (output == null) {
            throw new IllegalArgumentException("Stream task output is required");
        }
        return new StreamChannelOutput() {
            @Override public StreamChannelIdentity channelIdentity() {
                return output.channelIdentity();
            }
            @Override public void send(StreamEnvelope envelope) {
                output.send(envelope);
                recordOutput(envelope);
            }
            @Override public void close() { output.close(); }
            @Override public void cancel() { output.cancel(); }
            @Override public void fail(Throwable cause) { output.fail(cause); }
            @Override public StreamChannelMetrics metrics() {
                return output.metrics();
            }
        };
    }

    synchronized void takeOver(StreamControlMessage replacement) {
        if (replacement == null
                || !command.runId().equals(replacement.runId())
                || !command.attemptId().equals(replacement.attemptId())
                || command.fenceToken() != replacement.fenceToken()
                || command.assignmentGeneration()
                != replacement.assignmentGeneration()
                || replacement.coordinatorTerm()
                <= command.coordinatorTerm()) {
            throw new IllegalArgumentException(
                    "Stream task progress takeover fence is stale");
        }
        command = replacement;
    }

    StreamChannelInput observeInput(StreamChannelInput input) {
        if (input == null) {
            throw new IllegalArgumentException("Stream task input is required");
        }
        return new StreamChannelInput() {
            @Override public StreamChannelIdentity channelIdentity() {
                return input.channelIdentity();
            }
            @Override public StreamEnvelope receive()
                    throws InterruptedException {
                StreamEnvelope envelope = input.receive();
                recordInput(envelope);
                return envelope;
            }
            @Override public StreamEnvelope poll(long timeoutMillis)
                    throws InterruptedException {
                StreamEnvelope envelope = input.poll(timeoutMillis);
                recordInput(envelope);
                return envelope;
            }
            @Override public void cancel() { input.cancel(); }
            @Override public void fail(Throwable cause) { input.fail(cause); }
        };
    }

    synchronized void recordInput(StreamEnvelope envelope) {
        if (envelope == null
                || envelope.kind() != StreamEnvelope.Kind.RECORD) return;
        recordsRead = saturatingAdd(recordsRead, 1L);
        bytesRead = saturatingAdd(bytesRead, envelope.recordFrame().length);
        if (sinkBoundary) {
            recordsWritten = saturatingAdd(recordsWritten, 1L);
            bytesWritten = saturatingAdd(bytesWritten, envelope.recordFrame().length);
        }
    }

    synchronized void recordOutput(StreamEnvelope envelope) {
        if (envelope == null) return;
        if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
            recordsWritten = saturatingAdd(recordsWritten, 1L);
            bytesWritten = saturatingAdd(bytesWritten, envelope.recordFrame().length);
            if (sourceBoundary) {
                recordsRead = saturatingAdd(recordsRead, 1L);
                bytesRead = saturatingAdd(bytesRead, envelope.recordFrame().length);
            }
        } else if (envelope.kind() == StreamEnvelope.Kind.WATERMARK) {
            operatorWatermark = Math.max(
                    operatorWatermark, envelope.timestamp());
            watermarkIdle = false;
        } else if (envelope.kind()
                == StreamEnvelope.Kind.WATERMARK_STATUS) {
            watermarkIdle = envelope.watermarkStatus()
                    == StreamEnvelope.WatermarkStatus.IDLE;
        }
    }

    synchronized StreamTaskProgress snapshot(
            int mailboxHighWaterEvents, long managedStateBytes,
            int timerHighWaterCount, int windowHighWaterCount,
            boolean resourceCoverageComplete) {
        StreamChannelMetrics channels = outputs.isEmpty()
                ? StreamChannelMetrics.available(0L, 0L, 0L, 0L, 0L)
                : StreamChannelMetrics.aggregateOutputs(outputs);
        sampleSequence = saturatingAdd(sampleSequence, 1L);
        return new StreamTaskProgress(command.runId(), command.attemptId(),
                command.fenceToken(), command.coordinatorTerm(),
                command.assignmentGeneration(), identity, sampleSequence,
                resourceCoverageComplete && channels.available(), recordsRead,
                recordsWritten, bytesRead, bytesWritten, operatorWatermark,
                watermarkIdle, channels.currentEvents(),
                channels.currentBytes(), mailboxHighWaterEvents,
                channels.highWaterBytes(), managedStateBytes,
                timerHighWaterCount, windowHighWaterCount,
                channels.blockedNanos());
    }

    private static long saturatingAdd(long left, long right) {
        return left > Long.MAX_VALUE - right
                ? Long.MAX_VALUE : left + right;
    }
}
