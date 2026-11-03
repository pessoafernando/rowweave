package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one task-local observer covers every runtime shape. */
class StreamTaskProgressTrackerTest {

    @Test
    void sourceCountsOneLogicalRecordWhilePhysicalBroadcastBacklogCountsLanes()
            throws Exception {
        StreamControlMessage command = command();
        LocalStreamChannel first = channel("first");
        LocalStreamChannel second = channel("second");
        StreamParallelBroadcastOutput broadcast =
                new StreamParallelBroadcastOutput(Arrays.asList(first, second));
        StreamTaskProgressTracker tracker = new StreamTaskProgressTracker(
                command, new StreamTaskIdentity("source", 0), true, false,
                Collections.singletonList(broadcast));
        StreamChannelOutput output = tracker.observeOutput(broadcast);

        output.send(StreamEnvelope.record(output.channelIdentity(), 1L,
                new byte[] {1, 2, 3}));
        output.send(StreamEnvelope.watermark(
                output.channelIdentity(), 2L, 100L));
        StreamTaskProgress sample = tracker.snapshot(
                2, 30L, 3, 1, true);

        assertEquals(1L, sample.recordsRead());
        assertEquals(3L, sample.bytesRead());
        assertEquals(1L, sample.recordsWritten());
        assertEquals(4L, sample.backlogEvents());
        assertEquals(first.queuedBytes() + second.queuedBytes(),
                sample.backlogBytes());
        assertEquals(100L, sample.operatorWatermark());
        assertFalse(sample.watermarkIdle());
        assertTrue(sample.complete());
        assertEquals(1L, sample.sampleSequence());
    }

    @Test
    void everyTaskCountsItsOwnInputAndOutput()
            throws Exception {
        LocalStreamChannel lane = channel("sink-input");
        lane.send(StreamEnvelope.record(
                lane.channelIdentity(), 1L, new byte[] {7, 8}));
        StreamTaskProgressTracker sink = new StreamTaskProgressTracker(
                command(), new StreamTaskIdentity("sink", 0), false, true,
                Collections.emptyList());
        StreamChannelInput observed = sink.observeInput(lane);
        observed.receive();
        StreamTaskProgress sinkSample = sink.snapshot(
                0, 0L, 0, 0, true);
        assertEquals(1L, sinkSample.recordsWritten());
        assertEquals(1L, sinkSample.recordsRead());
        assertEquals(2L, sinkSample.bytesWritten());
        assertTrue(sinkSample.complete());

        StreamTaskProgressTracker middle = new StreamTaskProgressTracker(
                command(), new StreamTaskIdentity("map", 0), false, false,
                Collections.emptyList());
        middle.recordInput(StreamEnvelope.record(
                lane.channelIdentity(), 2L, new byte[] {1}));
        middle.recordOutput(StreamEnvelope.record(
                lane.channelIdentity(), 3L, new byte[] {1}));
        StreamTaskProgress middleSample = middle.snapshot(
                0, 0L, 0, 0, false);
        assertEquals(1L, middleSample.recordsRead());
        assertEquals(1L, middleSample.recordsWritten());
        assertFalse(middleSample.complete());
        assertEquals(1L, middleSample.sampleSequence());
        assertEquals(2L, middle.snapshot(
                0, 0L, 0, 0, false).sampleSequence());
    }

    private static StreamControlMessage command() {
        return new StreamControlMessage(StreamControlMessage.Kind.START,
                2L, "coordinator", "run", "attempt", 3L, 4L,
                "worker", 1L, repeat('a', 64), new byte[0]);
    }

    private static LocalStreamChannel channel(String id) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 3L, 1L, id, 1L),
                8, 4_096, 32_768L);
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int index = 0; index < count; index++) result.append(value);
        return result.toString();
    }
}
