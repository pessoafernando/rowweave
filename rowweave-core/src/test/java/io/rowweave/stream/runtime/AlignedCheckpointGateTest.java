package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** barrier alignment pauses inputs instead of buffering records. */
class AlignedCheckpointGateTest {
    private static final String A = digest('a');
    private static final String B = digest('b');

    @Test
    void blocksOnlyArrivedInputsAndSnapshotsBeforeForwardingBarrier() {
        List<String> actions = new ArrayList<>();
        AlignedCheckpointGate gate = new AlignedCheckpointGate(
                2,
                envelope -> actions.add("record-" + envelope.recordFrame()[0]),
                checkpoint -> actions.add("snapshot-"
                        + checkpoint.checkpointSequence()),
                checkpoint -> actions.add("forward-"
                        + checkpoint.checkpointSequence()));
        CheckpointIdentity checkpoint = checkpoint(1L);
        gate.trigger(checkpoint);

        gate.onEnvelope(0, record(0, 1L, 10));
        gate.onEnvelope(0, barrier(0, 2L, checkpoint));

        assertTrue(gate.isInputBlocked(0));
        assertFalse(gate.isInputBlocked(1));
        assertEquals(checkpoint, gate.activeCheckpoint());

        gate.onEnvelope(1, record(1, 1L, 20));
        gate.onEnvelope(1, barrier(1, 2L, checkpoint));

        assertEquals(Arrays.asList(
                "record-10", "record-20", "snapshot-1", "forward-1"),
                actions);
        assertFalse(gate.isInputBlocked(0));
        assertFalse(gate.isInputBlocked(1));
        assertNull(gate.activeCheckpoint());
        assertEquals(1L, gate.lastAlignedSequence());
    }

    @Test
    void postBarrierRecordAndCheckpointDriftPoisonWithoutFalseAlignment() {
        CheckpointIdentity checkpoint = checkpoint(1L);
        AtomicReference<CheckpointIdentity> aligned = new AtomicReference<>();
        AlignedCheckpointGate blocked = new AlignedCheckpointGate(
                2, ignored -> { }, aligned::set, ignored -> { });
        blocked.trigger(checkpoint);
        blocked.onEnvelope(0, barrier(0, 1L, checkpoint));

        StreamCheckpointException afterBarrier = assertThrows(
                StreamCheckpointException.class,
                () -> blocked.onEnvelope(0, record(0, 2L, 9)));
        assertEquals(StreamCheckpointException.Code.INPUT_BLOCKED,
                afterBarrier.code());
        assertNull(aligned.get());
        assertSame(afterBarrier, assertThrows(
                StreamCheckpointException.class,
                () -> blocked.onEnvelope(1, barrier(1, 1L, checkpoint))));

        AlignedCheckpointGate drift = new AlignedCheckpointGate(
                2, ignored -> { }, aligned::set, ignored -> { });
        drift.trigger(checkpoint);
        StreamCheckpointException mismatch = assertThrows(
                StreamCheckpointException.class,
                () -> drift.onEnvelope(0, barrier(0, 1L, checkpoint(2L))));
        assertEquals(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                mismatch.code());
        assertNull(aligned.get());
    }

    @Test
    void triggerIsSingleInFlightMonotonicAndOwnedByMailboxThread()
            throws Exception {
        AlignedCheckpointGate gate = new AlignedCheckpointGate(
                1, ignored -> { }, ignored -> { }, ignored -> { });
        CheckpointIdentity first = checkpoint(1L);
        gate.trigger(first);

        assertCode(StreamCheckpointException.Code.CHECKPOINT_IN_FLIGHT,
                () -> gate.trigger(checkpoint(2L)));

        AlignedCheckpointGate completed = new AlignedCheckpointGate(
                1, ignored -> { }, ignored -> { }, ignored -> { });
        completed.trigger(first);
        completed.onEnvelope(0, barrier(0, 1L, first));
        assertCode(StreamCheckpointException.Code.CHECKPOINT_SEQUENCE_REPLAY,
                () -> completed.trigger(first));

        AlignedCheckpointGate owned = new AlignedCheckpointGate(
                1, ignored -> { }, ignored -> { }, ignored -> { });
        AtomicReference<Throwable> crossThread = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                owned.trigger(first);
            } catch (Throwable failure) {
                crossThread.set(failure);
            }
        });
        other.start();
        other.join();
        assertEquals(StreamCheckpointException.Code.THREAD_OWNERSHIP,
                ((StreamCheckpointException) crossThread.get()).code());
    }

    @Test
    void snapshotFailurePoisonsBeforeBarrierForward() {
        RuntimeException snapshotBoom = new RuntimeException("snapshot boom");
        List<String> actions = new ArrayList<>();
        AlignedCheckpointGate gate = new AlignedCheckpointGate(
                1, ignored -> { }, ignored -> { throw snapshotBoom; },
                ignored -> actions.add("forward"));
        CheckpointIdentity checkpoint = checkpoint(1L);
        gate.trigger(checkpoint);

        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class,
                () -> gate.onEnvelope(0, barrier(0, 1L, checkpoint)));

        assertEquals(StreamCheckpointException.Code.CALLBACK_FAILURE,
                failure.code());
        assertSame(snapshotBoom, failure.getCause());
        assertEquals(0, actions.size());
        assertSame(failure, assertThrows(StreamCheckpointException.class,
                () -> gate.isInputBlocked(0)));
    }

    private static StreamEnvelope record(
            int input, long sequence, int value) {
        return StreamEnvelope.record(channel(input), sequence,
                new byte[] {(byte) value});
    }

    private static StreamEnvelope barrier(
            int input, long sequence, CheckpointIdentity checkpoint) {
        return StreamEnvelope.checkpointBarrier(
                channel(input), sequence, checkpoint);
    }

    private static StreamChannelIdentity channel(int input) {
        return new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 1L,
                "input-" + input, 1L);
    }

    private static CheckpointIdentity checkpoint(long sequence) {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", sequence, A, B, 1, 1);
    }

    private static void assertCode(
            StreamCheckpointException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class, action);
        assertEquals(code, failure.code());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
