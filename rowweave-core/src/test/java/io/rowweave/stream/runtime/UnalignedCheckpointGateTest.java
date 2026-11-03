package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** first-barrier snapshots while remaining lanes are logged. */
class UnalignedCheckpointGateTest {
    private static final String A = digest('a');
    private static final String B = digest('b');

    @Test
    void snapshotsAndForwardsAtFirstBarrierThenCompletesChannelCut() {
        List<String> actions = new ArrayList<>();
        AtomicReference<List<StreamChannelState>> completed =
                new AtomicReference<>();
        UnalignedCheckpointGate gate = gate(
                envelope -> actions.add("event-"
                        + envelope.channelIdentity().channelId() + '-'
                        + envelope.sequence()),
                checkpoint -> actions.add("snapshot-"
                        + checkpoint.checkpointSequence()),
                checkpoint -> actions.add("forward-"
                        + checkpoint.checkpointSequence()),
                states -> {
                    actions.add("complete");
                    completed.set(states);
                }, 8, 64 * 1024L, 128 * 1024L);
        CheckpointIdentity checkpoint = checkpoint(1L);

        gate.onEnvelope(0, record(0, 1L, 10));
        gate.onEnvelope(1, record(1, 1L, 20));
        gate.trigger(checkpoint);
        gate.onEnvelope(0, barrier(0, 2L, checkpoint));

        assertTrue(gate.isCapturing());
        assertEquals(Arrays.asList(
                "event-input-0-1", "event-input-1-1",
                "snapshot-1", "forward-1"), actions);

        gate.onEnvelope(0, record(0, 3L, 30));
        gate.onEnvelope(1, record(1, 2L, 40));
        assertNull(completed.get());
        gate.onEnvelope(1, barrier(1, 3L, checkpoint));

        assertFalse(gate.isCapturing());
        assertEquals(Arrays.asList(
                "event-input-0-1", "event-input-1-1",
                "snapshot-1", "forward-1",
                "event-input-0-3", "event-input-1-2", "complete"),
                actions);
        List<StreamChannelState> states = completed.get();
        assertEquals(2, states.size());
        assertEquals(1L, states.get(0).lastConsumedSequence());
        assertEquals(2L, states.get(0).barrierSequence());
        assertEquals(0, states.get(0).retainedEventCredits());
        assertEquals(1L, states.get(1).lastConsumedSequence());
        assertEquals(3L, states.get(1).barrierSequence());
        assertEquals(Arrays.asList(record(1, 2L, 40)),
                states.get(1).envelopes());
        assertEquals(1L, gate.lastCompletedSequence());
    }

    @Test
    void channelStateLimitMinusOneFailsBeforeCapturedRecordIsProcessed() {
        StreamEnvelope captured = record(1, 2L, 40);
        long exact = StreamEnvelopeCodec.encode(captured).length;
        AtomicInteger events = new AtomicInteger();
        UnalignedCheckpointGate exactGate = gate(
                ignored -> events.incrementAndGet(), ignored -> { },
                ignored -> { }, ignored -> { }, 1, exact, exact);
        CheckpointIdentity checkpoint = checkpoint(1L);
        exactGate.onEnvelope(0, record(0, 1L, 1));
        exactGate.onEnvelope(1, record(1, 1L, 2));
        exactGate.trigger(checkpoint);
        exactGate.onEnvelope(0, barrier(0, 2L, checkpoint));
        exactGate.onEnvelope(1, captured);
        exactGate.onEnvelope(1, barrier(1, 3L, checkpoint));
        assertEquals(3, events.get());

        AtomicInteger rejectedEvents = new AtomicInteger();
        UnalignedCheckpointGate rejected = gate(
                ignored -> rejectedEvents.incrementAndGet(), ignored -> { },
                ignored -> { }, ignored -> { }, 1, exact - 1L, exact - 1L);
        rejected.onEnvelope(0, record(0, 1L, 1));
        rejected.onEnvelope(1, record(1, 1L, 2));
        rejected.trigger(checkpoint);
        rejected.onEnvelope(0, barrier(0, 2L, checkpoint));
        assertCode(StreamCheckpointException.Code.CHANNEL_STATE_LIMIT,
                () -> rejected.onEnvelope(1, captured));
        assertEquals(2, rejectedEvents.get());
    }

    @Test
    void mismatchedBarrierAndSnapshotFailurePoisonTheGate() {
        UnalignedCheckpointGate mismatch = gate(
                ignored -> { }, ignored -> { }, ignored -> { },
                ignored -> { }, 1, 1024L, 2048L);
        mismatch.trigger(checkpoint(1L));
        assertCode(StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                () -> mismatch.onEnvelope(
                        0, barrier(0, 1L, checkpoint(2L))));

        RuntimeException boom = new RuntimeException("snapshot boom");
        List<String> forwards = new ArrayList<>();
        UnalignedCheckpointGate callback = gate(
                ignored -> { }, ignored -> { throw boom; },
                ignored -> forwards.add("forward"), ignored -> { },
                1, 1024L, 2048L);
        callback.trigger(checkpoint(1L));
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class,
                () -> callback.onEnvelope(
                        0, barrier(0, 1L, checkpoint(1L))));
        assertEquals(StreamCheckpointException.Code.CALLBACK_FAILURE,
                failure.code());
        assertSame(boom, failure.getCause());
        assertTrue(forwards.isEmpty());
        assertSame(failure, assertThrows(StreamCheckpointException.class,
                callback::isCapturing));
    }

    private static UnalignedCheckpointGate gate(
            java.util.function.Consumer<StreamEnvelope> events,
            java.util.function.Consumer<CheckpointIdentity> snapshots,
            java.util.function.Consumer<CheckpointIdentity> barriers,
            java.util.function.Consumer<List<StreamChannelState>> completed,
            int maximumEvents, long maximumBytesPerLane,
            long maximumTotalBytes) {
        return new UnalignedCheckpointGate(
                Arrays.asList(channel(0), channel(1)), maximumEvents,
                maximumBytesPerLane, maximumTotalBytes,
                StreamEnvelopeCodec.MAX_FRAME_BYTES,
                events, snapshots, barriers, completed);
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
                4L, "coordinator-1", sequence, A, B, 2, 2);
    }

    private static void assertCode(StreamCheckpointException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
