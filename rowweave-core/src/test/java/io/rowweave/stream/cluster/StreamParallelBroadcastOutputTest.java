package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelMetrics;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** BROADCAST duplicates each ordered envelope to all subtasks. */
class StreamParallelBroadcastOutputTest {

    @Test
    void eachPartitionReceivesTheRecordAndControlSequence()
            throws Exception {
        LocalStreamChannel first = channel("first");
        LocalStreamChannel second = channel("second");
        StreamParallelBroadcastOutput output =
                new StreamParallelBroadcastOutput(
                        Arrays.asList(first, second));

        output.send(StreamEnvelope.record(first.channelIdentity(), 1L,
                new byte[] {7}));
        output.send(StreamEnvelope.watermark(
                first.channelIdentity(), 2L, 100L));

        StreamChannelMetrics metrics = output.metrics();
        assertEquals(4L, metrics.currentEvents());
        assertEquals(first.queuedBytes() + second.queuedBytes(),
                metrics.currentBytes());
        assertEquals(4L, metrics.highWaterEvents());

        assertLane(first);
        assertLane(second);
    }

    private static void assertLane(LocalStreamChannel lane)
            throws Exception {
        StreamEnvelope record = lane.receive();
        assertEquals(StreamEnvelope.Kind.RECORD, record.kind());
        assertArrayEquals(new byte[] {7}, record.recordFrame());
        assertEquals(lane.channelIdentity(), record.channelIdentity());
        assertEquals(1L, record.sequence());
        StreamEnvelope watermark = lane.receive();
        assertEquals(StreamEnvelope.Kind.WATERMARK, watermark.kind());
        assertEquals(100L, watermark.timestamp());
        assertEquals(2L, watermark.sequence());
    }

    private static LocalStreamChannel channel(String id) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, id, 1L),
                8, 4_096, 32_768L);
    }
}
