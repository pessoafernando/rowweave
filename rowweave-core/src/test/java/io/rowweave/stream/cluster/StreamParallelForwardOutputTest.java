package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelMetrics;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** local FORWARD preserves one-copy records and all controls. */
class StreamParallelForwardOutputTest {

    @Test
    void recordsStaySingleCopyWhileControlsReachEverySubtask()
            throws Exception {
        LocalStreamChannel first = channel("first");
        LocalStreamChannel second = channel("second");
        StreamParallelForwardOutput output = new StreamParallelForwardOutput(
                Arrays.asList(first, second));

        output.send(StreamEnvelope.record(first.channelIdentity(), 1L,
                new byte[] {7}));
        output.send(StreamEnvelope.record(first.channelIdentity(), 2L,
                new byte[] {8}));
        output.send(StreamEnvelope.watermark(
                first.channelIdentity(), 3L, 100L));

        StreamChannelMetrics metrics = output.metrics();
        assertEquals(4L, metrics.currentEvents());
        assertEquals(first.queuedBytes() + second.queuedBytes(),
                metrics.currentBytes());
        assertEquals(4L, metrics.highWaterEvents());

        assertLane(first, (byte) 7);
        assertLane(second, (byte) 8);
    }

    private static void assertLane(LocalStreamChannel lane, byte value)
            throws Exception {
        StreamEnvelope record = lane.receive();
        assertEquals(StreamEnvelope.Kind.RECORD, record.kind());
        assertArrayEquals(new byte[] {value}, record.recordFrame());
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
