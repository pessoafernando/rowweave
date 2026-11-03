package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEnvelope;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Task-local aligned checkpoint gate.
 *
 * <p>The owner stops reading an input as soon as its barrier arrives. Records
 * after that barrier remain in the bounded input channel and consume credit;
 * this gate never materializes an alignment buffer.
 */
public final class AlignedCheckpointGate {
    private final boolean[] blockedInputs;
    private final Consumer<StreamEnvelope> eventHandler;
    private final Consumer<CheckpointIdentity> snapshotHandler;
    private final Consumer<CheckpointIdentity> barrierHandler;
    private final Thread ownerThread;

    private CheckpointIdentity activeCheckpoint;
    private int blockedCount;
    private long lastAlignedSequence;
    private StreamCheckpointException terminalFailure;

    public AlignedCheckpointGate(
            int inputCount,
            Consumer<StreamEnvelope> eventHandler,
            Consumer<CheckpointIdentity> snapshotHandler,
            Consumer<CheckpointIdentity> barrierHandler) {
        if (inputCount < 1 || inputCount > PortableStreamPlan.MAX_NODES
                || eventHandler == null || snapshotHandler == null
                || barrierHandler == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "bounded inputs and checkpoint handlers are required");
        }
        this.blockedInputs = new boolean[inputCount];
        this.eventHandler = eventHandler;
        this.snapshotHandler = snapshotHandler;
        this.barrierHandler = barrierHandler;
        this.ownerThread = Thread.currentThread();
    }

    /** Freezes the exact checkpoint accepted from the task control lane. */
    public void trigger(CheckpointIdentity checkpoint) {
        checkOwnerAndHealthy();
        if (checkpoint == null) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "checkpoint trigger identity is required", null);
        }
        if (activeCheckpoint != null) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_IN_FLIGHT,
                    "task already has a checkpoint in flight", null);
        }
        if (checkpoint.checkpointSequence() <= lastAlignedSequence) {
            throw poison(
                    StreamCheckpointException.Code.CHECKPOINT_SEQUENCE_REPLAY,
                    "checkpoint sequence was already aligned", null);
        }
        activeCheckpoint = checkpoint;
    }

    /** Handles only an envelope already selected from an unblocked input. */
    public void onEnvelope(int inputIndex, StreamEnvelope envelope) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        if (envelope == null) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "Stream envelope is required", null);
        }
        if (blockedInputs[inputIndex]) {
            throw poison(StreamCheckpointException.Code.INPUT_BLOCKED,
                    "barriered input must not be consumed before alignment",
                    null);
        }
        if (envelope.kind() != StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
            invokeEvent(envelope);
            return;
        }
        CheckpointIdentity arrived = envelope.checkpointIdentity();
        if (activeCheckpoint == null) {
            throw poison(StreamCheckpointException.Code.UNEXPECTED_BARRIER,
                    "checkpoint barrier arrived before its trigger", null);
        }
        if (!activeCheckpoint.equals(arrived)) {
            throw poison(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "checkpoint barrier does not match the active trigger",
                    null);
        }
        blockedInputs[inputIndex] = true;
        blockedCount++;
        if (blockedCount == blockedInputs.length) {
            finishAlignment();
        }
    }

    /** The mailbox scheduler must not poll an input while this returns true. */
    public boolean isInputBlocked(int inputIndex) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        return blockedInputs[inputIndex];
    }

    public CheckpointIdentity activeCheckpoint() {
        checkOwnerAndHealthy();
        return activeCheckpoint;
    }

    public long lastAlignedSequence() {
        checkOwnerAndHealthy();
        return lastAlignedSequence;
    }

    private void finishAlignment() {
        CheckpointIdentity checkpoint = activeCheckpoint;
        try {
            snapshotHandler.accept(checkpoint);
            barrierHandler.accept(checkpoint);
        } catch (Throwable callbackFailure) {
            throw poison(StreamCheckpointException.Code.CALLBACK_FAILURE,
                    "aligned checkpoint callback failed", callbackFailure);
        }
        lastAlignedSequence = checkpoint.checkpointSequence();
        activeCheckpoint = null;
        blockedCount = 0;
        Arrays.fill(blockedInputs, false);
    }

    private void invokeEvent(StreamEnvelope envelope) {
        try {
            eventHandler.accept(envelope);
        } catch (Throwable callbackFailure) {
            throw poison(StreamCheckpointException.Code.CALLBACK_FAILURE,
                    "Stream event handler failed", callbackFailure);
        }
    }

    private void checkOwnerAndHealthy() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamCheckpointException.Code.THREAD_OWNERSHIP,
                    "checkpoint gate is owned by its mailbox thread");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private void requireInput(int inputIndex) {
        if (inputIndex < 0 || inputIndex >= blockedInputs.length) {
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
