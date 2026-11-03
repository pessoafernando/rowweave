package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one bounded mailbox gate owns two ordered inputs. */
class StreamMultiInputGateTest {

    @Test
    void endPropagatesIdleBeforeBarrierAndRestoresItsGeneration()
            throws Exception {
        Fixture inner = new Fixture();
        inner.left.send(StreamEnvelope.watermark(inner.leftId, 1L, 0L));
        inner.right.send(StreamEnvelope.watermarkStatus(inner.rightId, 1L,
                StreamEnvelope.WatermarkStatus.IDLE, 1L));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                inner.gate.take().envelope().kind());

        CheckpointIdentity cut = checkpoint();
        inner.right.send(StreamEnvelope.checkpointBarrier(
                inner.rightId, 2L, cut));
        inner.left.send(StreamEnvelope.endOfInput(inner.leftId, 2L));
        StreamEnvelope idle = inner.gate.take().envelope();
        assertEquals(StreamEnvelope.Kind.WATERMARK_STATUS, idle.kind());
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                idle.watermarkStatus());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                inner.gate.take().envelope().kind());

        Fixture outer = new Fixture();
        outer.left.send(StreamEnvelope.watermark(outer.leftId, 1L, 0L));
        outer.right.send(StreamEnvelope.watermark(outer.rightId, 1L, 100L));
        assertEquals(0L, outer.gate.take().envelope().timestamp());
        outer.left.send(StreamEnvelope.watermarkStatus(outer.leftId, 2L,
                idle.watermarkStatus(), idle.watermarkStatusGeneration()));
        assertEquals(100L, outer.gate.take().envelope().timestamp(),
                "an upstream UNION END must release the downstream frontier");

        Fixture restored = new Fixture();
        restored.gate.restoreState(inner.gate.snapshotState());
        restored.right.send(StreamEnvelope.watermarkStatus(
                restored.rightId, 1L,
                StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
        StreamEnvelope active = restored.gate.take().envelope();
        assertEquals(StreamEnvelope.Kind.WATERMARK_STATUS, active.kind());
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                active.watermarkStatus());
        assertEquals(idle.watermarkStatusGeneration() + 1L,
                active.watermarkStatusGeneration());
    }

    @Test
    void endCompletingBarrierEmitsDerivedWatermarkBeforeSnapshotCut()
            throws Exception {
        Fixture original = new Fixture();
        original.right.send(StreamEnvelope.watermark(
                original.rightId, 1L, 0L));
        original.left.send(StreamEnvelope.watermark(
                original.leftId, 1L, 100L));
        assertEquals(0L, original.gate.take().envelope().timestamp());

        CheckpointIdentity cut = checkpoint();
        original.left.send(StreamEnvelope.checkpointBarrier(
                original.leftId, 2L, cut));
        original.right.send(StreamEnvelope.endOfInput(
                original.rightId, 2L));
        StreamMultiInputGate.Event advanced = original.gate.take();
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                advanced.envelope().kind());
        assertEquals(100L, advanced.envelope().timestamp());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                original.gate.take().envelope().kind());

        Fixture restored = new Fixture();
        restored.gate.restoreState(original.gate.snapshotState());
        assertTrue(restored.gate.inputEnded(1));
        restored.left.send(StreamEnvelope.watermark(
                restored.leftId, 1L, 100L));
        assertEquals(null, restored.gate.poll(0L),
                "the pre-cut watermark must not replay twice");
        restored.left.send(StreamEnvelope.watermark(
                restored.leftId, 2L, 101L));
        assertEquals(101L, restored.gate.take().envelope().timestamp());
        restored.left.send(StreamEnvelope.endOfInput(restored.leftId, 3L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                restored.gate.take().envelope().kind());
    }

    @Test
    void endCompletesAlignmentWhenFinishedInputCannotSendBarrier()
            throws Exception {
        Fixture fixture = new Fixture();
        CheckpointIdentity checkpoint = checkpoint();
        fixture.left.send(StreamEnvelope.checkpointBarrier(
                fixture.leftId, 1L, checkpoint));
        fixture.right.send(StreamEnvelope.endOfInput(fixture.rightId, 1L));

        StreamMultiInputGate.Event barrier = fixture.gate.take();
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                barrier.envelope().kind());
        assertEquals(checkpoint, barrier.envelope().checkpointIdentity());
        fixture.left.send(StreamEnvelope.endOfInput(fixture.leftId, 2L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                fixture.gate.take().envelope().kind());
    }

    @Test
    void pollsRecordsFairlyAndMergesWatermarksAndEnd() throws Exception {
        Fixture fixture = new Fixture();
        fixture.left.send(record(fixture.leftId, 1L, 1));
        fixture.left.send(record(fixture.leftId, 2L, 2));
        fixture.left.send(StreamEnvelope.watermark(fixture.leftId, 3L, 100L));
        fixture.left.send(StreamEnvelope.endOfInput(fixture.leftId, 4L));
        fixture.right.send(record(fixture.rightId, 1L, 10));
        fixture.right.send(StreamEnvelope.watermark(
                fixture.rightId, 2L, 50L));
        fixture.right.send(StreamEnvelope.endOfInput(fixture.rightId, 3L));

        assertEvent(fixture.gate.take(), 0, StreamEnvelope.Kind.RECORD, 1);
        assertEvent(fixture.gate.take(), 1, StreamEnvelope.Kind.RECORD, 10);
        assertEvent(fixture.gate.take(), 0, StreamEnvelope.Kind.RECORD, 2);
        StreamMultiInputGate.Event watermark = fixture.gate.take();
        assertEquals(-1, watermark.inputIndex());
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                watermark.envelope().kind());
        assertEquals(50L, watermark.envelope().timestamp());
        StreamMultiInputGate.Event advanced = fixture.gate.take();
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                advanced.envelope().kind());
        assertEquals(100L, advanced.envelope().timestamp());
        StreamMultiInputGate.Event end = fixture.gate.take();
        assertEquals(-1, end.inputIndex());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                end.envelope().kind());
    }

    @Test
    void alignedBarrierBlocksPostBarrierDataUntilBothInputsArrive()
            throws Exception {
        Fixture fixture = new Fixture();
        CheckpointIdentity checkpoint = checkpoint();
        fixture.left.send(StreamEnvelope.checkpointBarrier(
                fixture.leftId, 1L, checkpoint));
        fixture.left.send(record(fixture.leftId, 2L, 2));
        fixture.right.send(record(fixture.rightId, 1L, 10));
        fixture.right.send(StreamEnvelope.checkpointBarrier(
                fixture.rightId, 2L, checkpoint));

        assertEvent(fixture.gate.take(), 1, StreamEnvelope.Kind.RECORD, 10);
        StreamMultiInputGate.Event barrier = fixture.gate.take();
        assertEquals(-1, barrier.inputIndex());
        assertEquals(checkpoint, barrier.envelope().checkpointIdentity());
        assertEvent(fixture.gate.take(), 0, StreamEnvelope.Kind.RECORD, 2);
    }

    @Test
    void allIdleAndReactivationProduceOneMergedStatusGeneration()
            throws Exception {
        Fixture fixture = new Fixture();
        fixture.left.send(StreamEnvelope.watermarkStatus(fixture.leftId, 1L,
                StreamEnvelope.WatermarkStatus.IDLE, 1L));
        fixture.right.send(StreamEnvelope.watermarkStatus(fixture.rightId, 1L,
                StreamEnvelope.WatermarkStatus.IDLE, 1L));
        fixture.right.send(StreamEnvelope.watermarkStatus(fixture.rightId, 2L,
                StreamEnvelope.WatermarkStatus.ACTIVE, 2L));

        StreamMultiInputGate.Event idle = fixture.gate.take();
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                idle.envelope().watermarkStatus());
        assertEquals(1L, idle.envelope().watermarkStatusGeneration());
        StreamMultiInputGate.Event active = fixture.gate.take();
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                active.envelope().watermarkStatus());
        assertEquals(2L, active.envelope().watermarkStatusGeneration());
    }

    @Test
    void alignedCutRestoresEndedInputsAndMergedProgress() throws Exception {
        Fixture original = new Fixture();
        original.right.send(StreamEnvelope.watermark(
                original.rightId, 1L, 50L));
        original.left.send(StreamEnvelope.watermark(
                original.leftId, 1L, 100L));
        StreamMultiInputGate.Event initialWatermark = original.gate.take();
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                initialWatermark.envelope().kind());
        assertEquals(50L, initialWatermark.envelope().timestamp());
        original.gate.markEnded(1, StreamEnvelope.endOfInput(
                original.rightId, 2L));
        original.left.send(StreamEnvelope.checkpointBarrier(
                original.leftId, 2L, checkpoint()));

        StreamMultiInputGate.Event watermark = original.gate.take();
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                watermark.envelope().kind());
        assertEquals(100L, watermark.envelope().timestamp());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                original.gate.take().envelope().kind());

        Fixture replacement = new Fixture();
        replacement.gate.restoreState(original.gate.snapshotState());

        assertTrue(replacement.gate.inputEnded(1));
        replacement.left.send(StreamEnvelope.watermark(
                replacement.leftId, 1L, 100L));
        replacement.left.send(StreamEnvelope.endOfInput(
                replacement.leftId, 2L));
        StreamMultiInputGate.Event end = replacement.gate.take();
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                end.envelope().kind());
        assertTrue(replacement.gate.finished());
    }

    @Test
    void rescaleAcceptsCapturedStatusFromLowerGenerationOldTask()
            throws Exception {
        Fixture high = new Fixture();
        high.left.send(StreamEnvelope.watermarkStatus(
                high.leftId, 1L, StreamEnvelope.WatermarkStatus.IDLE, 10L));
        high.right.send(StreamEnvelope.watermarkStatus(
                high.rightId, 1L, StreamEnvelope.WatermarkStatus.IDLE, 10L));
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                high.gate.take().envelope().watermarkStatus());

        Fixture low = new Fixture();
        low.left.send(StreamEnvelope.watermarkStatus(
                low.leftId, 1L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        low.right.send(StreamEnvelope.watermarkStatus(
                low.rightId, 1L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                low.gate.take().envelope().watermarkStatus());

        Fixture target = new Fixture();
        target.gate.restoreState(StreamMultiInputGate.mergeStateForRescale(
                Arrays.asList(high.gate.snapshotState(),
                        low.gate.snapshotState())));
        target.left.send(StreamEnvelope.watermarkStatus(
                target.leftId, 1L,
                StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
        StreamMultiInputGate.Event active = target.gate.take();
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                active.envelope().watermarkStatus());
    }

    @Test
    void ordinaryRecoveryAcceptsNewSourceEpochAfterSavedStatusAndWatermark()
            throws Exception {
        Fixture original = new Fixture();
        original.left.send(StreamEnvelope.watermark(original.leftId, 1L,
                100L));
        original.right.send(StreamEnvelope.watermark(original.rightId, 1L,
                100L));
        assertEquals(100L, original.gate.take().envelope().timestamp());
        original.left.send(StreamEnvelope.watermarkStatus(original.leftId,
                2L, StreamEnvelope.WatermarkStatus.IDLE, 5L));
        original.right.send(StreamEnvelope.watermarkStatus(original.rightId,
                2L, StreamEnvelope.WatermarkStatus.IDLE, 5L));
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                original.gate.take().envelope().watermarkStatus());

        Fixture restored = new Fixture();
        restored.gate.restoreState(original.gate.snapshotState());
        restored.left.send(StreamEnvelope.watermarkStatus(restored.leftId,
                1L, StreamEnvelope.WatermarkStatus.ACTIVE, 1L));
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                restored.gate.take().envelope().watermarkStatus());
        restored.left.send(StreamEnvelope.watermark(restored.leftId,
                2L, 50L));
        restored.right.send(StreamEnvelope.watermark(restored.rightId,
                1L, 50L));
        assertEquals(null, restored.gate.poll(0L),
                "recovered input must not regress the emitted frontier");
        restored.left.send(StreamEnvelope.watermark(restored.leftId,
                3L, 101L));
        restored.right.send(StreamEnvelope.watermark(restored.rightId,
                2L, 101L));
        assertEquals(101L, restored.gate.take().envelope().timestamp());
    }

    @Test
    void recoveredImplicitlyActiveInputPreventsOldIdleFromAdvancingTime()
            throws Exception {
        Fixture original = new Fixture();
        original.left.send(StreamEnvelope.watermark(original.leftId, 1L,
                100L));
        original.right.send(StreamEnvelope.watermark(original.rightId, 1L,
                100L));
        assertEquals(100L, original.gate.take().envelope().timestamp());
        original.left.send(StreamEnvelope.watermarkStatus(original.leftId,
                2L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        assertEquals(null, original.gate.poll(0L));

        Fixture restored = new Fixture();
        restored.gate.restoreState(original.gate.snapshotState());
        restored.right.send(StreamEnvelope.watermark(restored.rightId,
                1L, 200L));
        assertEquals(null, restored.gate.poll(0L),
                "old IDLE must not let B overtake an implicitly active A");
        restored.left.send(record(restored.leftId, 1L, 7));
        assertEvent(restored.gate.take(), 0, StreamEnvelope.Kind.RECORD, 7);
        restored.left.send(StreamEnvelope.watermark(restored.leftId,
                2L, 150L));
        assertEquals(150L, restored.gate.take().envelope().timestamp());
    }

    @Test
    void allIdleRecoveryReactivatesDownstreamBeforeFirstRecord()
            throws Exception {
        Fixture original = new Fixture();
        original.left.send(StreamEnvelope.watermarkStatus(original.leftId,
                1L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        original.right.send(StreamEnvelope.watermarkStatus(original.rightId,
                1L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                original.gate.take().envelope().watermarkStatus());

        Fixture restored = new Fixture();
        restored.gate.restoreState(original.gate.snapshotState());
        restored.left.send(record(restored.leftId, 1L, 9));
        StreamMultiInputGate.Event active = restored.gate.take();
        assertEquals(StreamEnvelope.Kind.WATERMARK_STATUS,
                active.envelope().kind());
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                active.envelope().watermarkStatus());
        assertEquals(2L, active.envelope().watermarkStatusGeneration());
        assertEvent(restored.gate.take(), 0, StreamEnvelope.Kind.RECORD, 9);
    }

    private static void assertEvent(StreamMultiInputGate.Event event,
            int input, StreamEnvelope.Kind kind, int value) {
        assertEquals(input, event.inputIndex());
        assertEquals(kind, event.envelope().kind());
        assertEquals(value, event.envelope().recordFrame()[0] & 0xff);
    }

    private static StreamEnvelope record(StreamChannelIdentity identity,
            long sequence, int value) {
        return StreamEnvelope.record(identity, sequence,
                new byte[] {(byte) value});
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity("definition", digest('a'), "run",
                "attempt", 1L, 1L, "coordinator", 1L, digest('b'),
                digest('c'), 1, 1);
    }

    private static String digest(char value) {
        char[] text = new char[64];
        Arrays.fill(text, value);
        return new String(text);
    }

    private static final class Fixture {
        final StreamChannelIdentity leftId = identity("left");
        final StreamChannelIdentity rightId = identity("right");
        final LocalStreamChannel left = new LocalStreamChannel(
                leftId, 16, 4_096, 65_536L);
        final LocalStreamChannel right = new LocalStreamChannel(
                rightId, 16, 4_096, 65_536L);
        final StreamMultiInputGate gate = new StreamMultiInputGate(
                Arrays.asList(left, right));

        private static StreamChannelIdentity identity(String channel) {
            return new StreamChannelIdentity("run", "attempt", 1L,
                    1L, channel, 1L);
        }
    }
}
