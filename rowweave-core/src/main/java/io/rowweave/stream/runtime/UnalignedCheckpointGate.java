package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Mailbox-owned unaligned checkpoint gate.
 *
 * <p>The first barrier snapshots operator state and is forwarded immediately.
 * Until every other input reaches the same barrier, pre-barrier envelopes are
 * both delivered and copied into a bounded channel-state cut for replay.
 */
public final class UnalignedCheckpointGate {
    private final List<StreamChannelIdentity> inputs;
    private final int maximumEventsPerLane;
    private final long maximumBytesPerLane;
    private final long maximumTotalBytes;
    private final int maximumFrameBytes;
    private final Consumer<StreamEnvelope> eventHandler;
    private final Consumer<CheckpointIdentity> snapshotHandler;
    private final Consumer<CheckpointIdentity> barrierHandler;
    private final Consumer<List<StreamChannelState>> completionHandler;
    private final Thread ownerThread;
    private final long[] lastObservedSequences;
    private final long[] snapshotSequences;
    private final long[] barrierSequences;
    private final boolean[] barriered;
    private final List<List<StreamEnvelope>> captured;
    private final long[] capturedBytes;

    private CheckpointIdentity activeCheckpoint;
    private boolean capturing;
    private int barrierCount;
    private long totalCapturedBytes;
    private long lastCompletedSequence;
    private StreamCheckpointException terminalFailure;

    public UnalignedCheckpointGate(
            List<StreamChannelIdentity> inputIdentities,
            int maximumEventsPerLane, long maximumBytesPerLane,
            long maximumTotalBytes, int maximumFrameBytes,
            Consumer<StreamEnvelope> eventHandler,
            Consumer<CheckpointIdentity> snapshotHandler,
            Consumer<CheckpointIdentity> barrierHandler,
            Consumer<List<StreamChannelState>> completionHandler) {
        if (inputIdentities == null || inputIdentities.isEmpty()
                || inputIdentities.size() > PortableStreamPlan.MAX_NODES
                || maximumEventsPerLane < 1
                || maximumBytesPerLane < 1L || maximumTotalBytes < 1L
                || maximumFrameBytes < 1 || eventHandler == null
                || snapshotHandler == null || barrierHandler == null
                || completionHandler == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "bounded input identities, leases and handlers are required");
        }
        List<StreamChannelIdentity> copied = new ArrayList<>(
                inputIdentities.size());
        Set<String> unique = new HashSet<>();
        String runId = null;
        String attemptId = null;
        long fenceToken = -1L;
        for (StreamChannelIdentity identity : inputIdentities) {
            if (identity == null || !unique.add(identity.channelId())) {
                throw failure(
                        StreamCheckpointException.Code.INVALID_CONFIGURATION,
                        "input channel identities must be non-null and unique");
            }
            if (runId == null) {
                runId = identity.runId();
                attemptId = identity.attemptId();
                fenceToken = identity.fenceToken();
            } else if (!runId.equals(identity.runId())
                    || !attemptId.equals(identity.attemptId())
                    || fenceToken != identity.fenceToken()) {
                throw failure(
                        StreamCheckpointException.Code.INVALID_CONFIGURATION,
                        "input channels belong to different attempts");
            }
            copied.add(identity);
        }
        this.inputs = Collections.unmodifiableList(copied);
        this.maximumEventsPerLane = maximumEventsPerLane;
        this.maximumBytesPerLane = maximumBytesPerLane;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumFrameBytes = maximumFrameBytes;
        this.eventHandler = eventHandler;
        this.snapshotHandler = snapshotHandler;
        this.barrierHandler = barrierHandler;
        this.completionHandler = completionHandler;
        this.ownerThread = Thread.currentThread();
        this.lastObservedSequences = new long[inputs.size()];
        this.snapshotSequences = new long[inputs.size()];
        this.barrierSequences = new long[inputs.size()];
        this.barriered = new boolean[inputs.size()];
        this.capturedBytes = new long[inputs.size()];
        this.captured = new ArrayList<>(inputs.size());
        for (int index = 0; index < inputs.size(); index++) {
            captured.add(new ArrayList<StreamEnvelope>());
        }
    }

    public void trigger(CheckpointIdentity checkpoint) {
        checkOwnerAndHealthy();
        if (checkpoint == null || !sameAttempt(checkpoint)) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "checkpoint trigger does not belong to the input attempt",
                    null);
        }
        if (activeCheckpoint != null) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_IN_FLIGHT,
                    "task already has a checkpoint in flight", null);
        }
        if (checkpoint.checkpointSequence() <= lastCompletedSequence) {
            throw poison(
                    StreamCheckpointException.Code.CHECKPOINT_SEQUENCE_REPLAY,
                    "checkpoint sequence was already completed", null);
        }
        activeCheckpoint = checkpoint;
    }

    public void onEnvelope(int inputIndex, StreamEnvelope envelope) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        if (envelope == null
                || !inputs.get(inputIndex).equals(
                envelope.channelIdentity())) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "envelope does not belong to the selected input", null);
        }
        long expected;
        try {
            expected = Math.addExact(lastObservedSequences[inputIndex], 1L);
        } catch (ArithmeticException overflow) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "input sequence overflowed", overflow);
        }
        if (envelope.sequence() != expected) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "input sequence is not contiguous", null);
        }
        if (envelope.kind() == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
            acceptBarrier(inputIndex, envelope);
            lastObservedSequences[inputIndex] = envelope.sequence();
            return;
        }
        if (capturing && !barriered[inputIndex]) {
            capture(inputIndex, envelope);
        }
        invokeEvent(envelope);
        lastObservedSequences[inputIndex] = envelope.sequence();
    }

    public boolean isCapturing() {
        checkOwnerAndHealthy();
        return capturing;
    }

    public CheckpointIdentity activeCheckpoint() {
        checkOwnerAndHealthy();
        return activeCheckpoint;
    }

    public long lastCompletedSequence() {
        checkOwnerAndHealthy();
        return lastCompletedSequence;
    }

    private void acceptBarrier(int inputIndex, StreamEnvelope envelope) {
        if (activeCheckpoint == null) {
            throw poison(StreamCheckpointException.Code.UNEXPECTED_BARRIER,
                    "checkpoint barrier arrived before its trigger", null);
        }
        if (!activeCheckpoint.equals(envelope.checkpointIdentity())) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "checkpoint barrier differs from its trigger", null);
        }
        if (barriered[inputIndex]) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "input delivered a duplicate checkpoint barrier", null);
        }
        if (!capturing) beginCapture();
        barriered[inputIndex] = true;
        barrierSequences[inputIndex] = envelope.sequence();
        barrierCount++;
        if (barrierCount == inputs.size()) completeCapture();
    }

    private void beginCapture() {
        System.arraycopy(lastObservedSequences, 0, snapshotSequences, 0,
                lastObservedSequences.length);
        try {
            snapshotHandler.accept(activeCheckpoint);
            barrierHandler.accept(activeCheckpoint);
        } catch (Throwable callbackFailure) {
            throw poison(StreamCheckpointException.Code.CALLBACK_FAILURE,
                    "unaligned first-barrier callback failed",
                    callbackFailure);
        }
        capturing = true;
    }

    private void capture(int inputIndex, StreamEnvelope envelope) {
        if (envelope.kind() == StreamEnvelope.Kind.CANCEL
                || envelope.kind() == StreamEnvelope.Kind.ERROR
                || envelope.kind() == StreamEnvelope.Kind.END_OF_INPUT) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "terminal input arrived before its checkpoint barrier",
                    null);
        }
        byte[] encoded = StreamEnvelopeCodec.encode(envelope);
        long laneBytes;
        long allBytes;
        try {
            laneBytes = Math.addExact(
                    capturedBytes[inputIndex], encoded.length);
            allBytes = Math.addExact(totalCapturedBytes, encoded.length);
        } catch (ArithmeticException overflow) {
            throw poison(StreamCheckpointException.Code.CHANNEL_STATE_LIMIT,
                    "channel state byte accounting overflowed", overflow);
        }
        if (encoded.length > maximumFrameBytes
                || captured.get(inputIndex).size()
                >= maximumEventsPerLane
                || laneBytes > maximumBytesPerLane
                || allBytes > maximumTotalBytes) {
            throw poison(StreamCheckpointException.Code.CHANNEL_STATE_LIMIT,
                    "unaligned channel state exceeds its resource lease",
                    null);
        }
        captured.get(inputIndex).add(envelope);
        capturedBytes[inputIndex] = laneBytes;
        totalCapturedBytes = allBytes;
    }

    private void completeCapture() {
        List<StreamChannelState> states = new ArrayList<>(inputs.size());
        for (int index = 0; index < inputs.size(); index++) {
            states.add(StreamChannelState.capture(
                    activeCheckpoint, inputs.get(index),
                    snapshotSequences[index], barrierSequences[index],
                    captured.get(index), maximumEventsPerLane,
                    maximumBytesPerLane, maximumFrameBytes));
        }
        try {
            completionHandler.accept(Collections.unmodifiableList(states));
        } catch (Throwable callbackFailure) {
            throw poison(StreamCheckpointException.Code.CALLBACK_FAILURE,
                    "unaligned channel-state callback failed",
                    callbackFailure);
        }
        lastCompletedSequence = activeCheckpoint.checkpointSequence();
        activeCheckpoint = null;
        capturing = false;
        barrierCount = 0;
        totalCapturedBytes = 0L;
        for (int index = 0; index < inputs.size(); index++) {
            barriered[index] = false;
            snapshotSequences[index] = 0L;
            barrierSequences[index] = 0L;
            capturedBytes[index] = 0L;
            captured.get(index).clear();
        }
    }

    private void invokeEvent(StreamEnvelope envelope) {
        try {
            eventHandler.accept(envelope);
        } catch (Throwable callbackFailure) {
            throw poison(StreamCheckpointException.Code.CALLBACK_FAILURE,
                    "Stream event handler failed", callbackFailure);
        }
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        StreamChannelIdentity first = inputs.get(0);
        return first.runId().equals(checkpoint.runId())
                && first.attemptId().equals(checkpoint.attemptId())
                && first.fenceToken() == checkpoint.fenceToken();
    }

    private void checkOwnerAndHealthy() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamCheckpointException.Code.THREAD_OWNERSHIP,
                    "checkpoint gate is owned by its mailbox thread");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private void requireInput(int inputIndex) {
        if (inputIndex < 0 || inputIndex >= inputs.size()) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint input index is outside the task closure");
        }
    }

    private StreamCheckpointException poison(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        StreamCheckpointException failure = cause == null
                ? failure(code, message) : failure(code, message, cause);
        terminalFailure = failure;
        return failure;
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }
}
