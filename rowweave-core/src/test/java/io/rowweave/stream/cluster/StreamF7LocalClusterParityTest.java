package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.BoundedEventTimeTemporalJoin;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 7 local/socket conformance for the bounded UPSERT temporal closure. */
class StreamF7LocalClusterParityTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void localAndSocketLanesProduceTheSameTemporalResultAndResourceProfile()
            throws Exception {
        Outcome local = execute(false, "local");
        Outcome socket = execute(true, "socket");

        assertEquals(Arrays.asList("L:R-low", "L:R-high"), local.output);
        assertEquals(local.output, socket.output);
        assertEquals(local.maximumEntries, socket.maximumEntries);
        assertEquals(local.maximumBytes, socket.maximumBytes);
        assertEquals(3, local.maximumEntries);
        assertEquals(0, local.finalEntries);
        assertEquals(0, socket.finalEntries);
    }

    private Outcome execute(boolean socket, String name) throws Exception {
        StreamChannelIdentity leftIdentity = identity(name + "-left");
        StreamChannelIdentity rightIdentity = identity(name + "-right");
        List<String> output = new ArrayList<>();
        try (Lane left = lane(socket, leftIdentity);
             Lane right = lane(socket, rightIdentity);
             BoundedEventTimeTemporalJoin join =
                     new BoundedEventTimeTemporalJoin(
                             "temporal-join", -10L, 20L,
                             128, 0, 127, 32, 64 * 1024L,
                             1024, 4096,
                             temporaryDirectory.resolve(name),
                             (leftFrame, rightFrame) -> bytes(
                                     text(leftFrame) + ':' + text(rightFrame)),
                             frame -> output.add(text(frame)))) {
            feedRecord(left, join, true, 1L, 100L,
                    StreamUpsertRecord.afterImage(bytes("k"), bytes("L")));
            feedRecord(right, join, false, 1L, 90L,
                    StreamUpsertRecord.afterImage(
                            bytes("k"), bytes("R-low")));
            feedRecord(right, join, false, 2L, 120L,
                    StreamUpsertRecord.afterImage(
                            bytes("k"), bytes("R-high")));

            int maximumEntries = join.maximumObservedEntries();
            long maximumBytes = join.maximumObservedBytes();
            feedWatermark(right, join, false, 3L, 120L);
            feedWatermark(left, join, true, 2L, 130L);
            return new Outcome(output, maximumEntries, maximumBytes,
                    join.stateEntryCount());
        }
    }

    private static void feedRecord(
            Lane lane, BoundedEventTimeTemporalJoin join, boolean left,
            long sequence, long timestamp, StreamUpsertRecord record)
            throws Exception {
        lane.output.send(StreamEnvelope.record(
                lane.identity, sequence, timestamp,
                StreamUpsertRecordCodec.encode(record)));
        StreamEnvelope transported = lane.input.receive();
        StreamUpsertRecord decoded = StreamUpsertRecordCodec.decode(
                transported.recordFrame(), 1024, 4096);
        if (left) {
            join.onLeft(decoded.stableKeyFrame(), transported.timestamp(),
                    decoded.afterImageFrame());
        } else {
            join.onRight(decoded.stableKeyFrame(), transported.timestamp(),
                    decoded.afterImageFrame());
        }
    }

    private static void feedWatermark(
            Lane lane, BoundedEventTimeTemporalJoin join, boolean left,
            long sequence, long watermark) throws Exception {
        lane.output.send(StreamEnvelope.watermark(
                lane.identity, sequence, watermark));
        StreamEnvelope transported = lane.input.receive();
        if (left) join.advanceLeftWatermark(transported.timestamp());
        else join.advanceRightWatermark(transported.timestamp());
    }

    private static Lane lane(
            boolean socket, StreamChannelIdentity identity) throws Exception {
        if (socket) {
            StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                    identity, 4, 4096, 8192L);
            return new Lane(identity, pair.sender(), pair.receiver(), pair);
        }
        LocalStreamChannel channel = new LocalStreamChannel(
                identity, 4, 4096, 8192L);
        return new Lane(identity, channel, channel, null);
    }

    private static StreamChannelIdentity identity(String channel) {
        return new StreamChannelIdentity(
                "run-f7-parity", "attempt-1", 7L, 3L, channel, 1L);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static final class Lane implements AutoCloseable {
        private final StreamChannelIdentity identity;
        private final StreamChannelOutput output;
        private final StreamChannelInput input;
        private final StreamSocketChannel.Pair pair;

        private Lane(
                StreamChannelIdentity identity, StreamChannelOutput output,
                StreamChannelInput input, StreamSocketChannel.Pair pair) {
            this.identity = identity;
            this.output = output;
            this.input = input;
            this.pair = pair;
        }

        @Override
        public void close() {
            if (pair != null) pair.close();
            else output.cancel();
        }
    }

    private static final class Outcome {
        private final List<String> output;
        private final int maximumEntries;
        private final long maximumBytes;
        private final int finalEntries;

        private Outcome(
                List<String> output, int maximumEntries,
                long maximumBytes, int finalEntries) {
            this.output = new ArrayList<>(output);
            this.maximumEntries = maximumEntries;
            this.maximumBytes = maximumBytes;
            this.finalEntries = finalEntries;
        }
    }
}
