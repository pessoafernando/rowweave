package io.rowweave.stream.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.FinalEventTimeDecimalWindow;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceEvent;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/** Isolated event-time window Worker fixture for the public cluster path. */
public final class StreamProductWindowWorkerProcessFixture {
    static final String SOURCE_PROVIDER = "window-source-v1";
    static final String SINK_PROVIDER = "window-sink-v1";
    static final String SOURCE_CONTRACT = digest("window-source-contract");
    static final String SINK_CONTRACT = digest("window-sink-contract");

    private StreamProductWindowWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException(
                    "role, mode and exchange directory required");
        }
        String role = arguments[0];
        String mode = arguments[1];
        Path exchange = Paths.get(arguments[2]);
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                "window-worker-" + role, "window-incarnation-" + role,
                2, 1, Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY,
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY),
                8, 16,
                8_192, 8_388_608L, 32);
        DurableStreamSourceProvider source = "source".equals(role)
                ? new WindowSourceProvider(mode, exchange) : null;
        EagerStreamSinkProvider sink = "sink".equals(role)
                ? new WindowSinkProvider(exchange.resolve("window-result")) : null;
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                source == null ? Collections.emptyMap()
                        : Collections.singletonMap(source.providerKey(), source),
                sink == null ? Collections.emptyMap()
                        : Collections.singletonMap(sink.providerKey(), sink),
                exchange.resolve("window-checkpoint-store"))) {
            Files.write(exchange.resolve("window-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("window-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    static DataSchema inputMeta() {
        return new DataSchema(new Field[] {
                new Field("adaptor", "adaptor", DataType.StringType),
                new Field("eventTime", "eventTime", DataType.TimestampType),
                new Field("amount", "amount", DataType.BigDecimalType)});
    }

    static DataSchema outputMeta() {
        return new DataSchema(new Field[] {
                new Field("adaptor", "adaptor", DataType.StringType),
                new Field("windowStart", "windowStart", DataType.LongType),
                new Field("windowEnd", "windowEnd", DataType.LongType),
                new Field("sum", "sum", DataType.BigDecimalType)});
    }

    private static final class WindowSourceProvider
            implements DurableStreamSourceProvider {
        private final String mode;
        private final Path exchange;
        private WindowSourceProvider(String mode, Path exchange) {
            this.mode = mode;
            this.exchange = exchange;
        }
        @Override public String providerKey() { return SOURCE_PROVIDER; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(inputMeta());
            return new StreamSourceReader() {
                private final CountDownLatch cancelled = new CountDownLatch(1);
                private final Object checkpointWakeupMonitor = new Object();
                private long checkpointWakeupGeneration;
                private int next;
                @Override public StreamSourceRecord next() { return null; }
                @Override public StreamSourceEvent nextEvent() throws Exception {
                    if ("idle".equals(mode)) {
                        if (next++ == 0) {
                            return StreamSourceEvent.watermarkStatus(
                                    io.rowweave.stream.contract.StreamEnvelope
                                            .WatermarkStatus.IDLE, 1L);
                        }
                        cancelled.await();
                        return null;
                    }
                    if ("stall".equals(mode)) {
                        cancelled.await();
                        return null;
                    }
                    if (next == 0) {
                        next++;
                        return record(serializer, 100L,
                                new BigDecimal("1.00000000006"));
                    }
                    if (next == 1) {
                        next++;
                        if ("checkpoint".equals(mode)) {
                            Files.write(exchange.resolve(
                                    "window-before-checkpoint"),
                                    new byte[] {1});
                        }
                        return record(serializer, 200L,
                                new BigDecimal("2.00000000005"));
                    }
                    if (next == 2) {
                        if ("checkpoint".equals(mode)) {
                            while (!Files.exists(exchange.resolve(
                                    "window-release-watermark"))) {
                                synchronized (checkpointWakeupMonitor) {
                                    long observed = checkpointWakeupGeneration;
                                    checkpointWakeupMonitor.wait(10L);
                                    if (checkpointWakeupGeneration
                                            != observed) return null;
                                }
                            }
                        }
                        next++;
                        return StreamSourceEvent.watermark(1_000L);
                    }
                    if ("late".equals(mode) && next == 3) {
                        next++;
                        return record(serializer, 500L, BigDecimal.ONE);
                    }
                    cancelled.await();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }
                @Override public void wakeUp() {
                    synchronized (checkpointWakeupMonitor) {
                        checkpointWakeupGeneration++;
                        checkpointWakeupMonitor.notifyAll();
                    }
                }
                @Override public void cancel() {
                    wakeUp();
                    cancelled.countDown();
                }
                @Override public void close() {
                    wakeUp();
                    cancelled.countDown();
                }
            };
        }
    }

    private static StreamSourceEvent record(
            RowSerializer serializer, long timestamp, BigDecimal amount) {
        DataRow row = new DataRow(3);
        row.set(0, "A");
        row.set(1, new Timestamp(timestamp));
        row.set(2, amount);
        byte[] frame = serializer.serialize(row);
        return StreamSourceEvent.record(
                StreamSourceRecord.of(frame, frame, timestamp));
    }

    private static final class WindowSinkProvider
            implements EagerStreamSinkProvider {
        private final Path result;
        private WindowSinkProvider(Path result) { this.result = result; }
        @Override public String providerKey() { return SINK_PROVIDER; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(outputMeta());
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) throws Exception {
                    DataRow row = serializer.deserialize(recordFrame);
                    Files.write(result, (row.getString(0) + ':'
                            + row.getLong(1) + ':' + row.getLong(2) + ':'
                            + row.getBigDecimal(3).toPlainString())
                            .getBytes(StandardCharsets.US_ASCII));
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    static String stateSerializer() {
        return FinalEventTimeDecimalWindow.DECIMAL_STATE_SERIALIZER;
    }

    static String digest(String value) {
        return StreamProductStatelessWorkerProcessFixture.digestText(value);
    }
}
