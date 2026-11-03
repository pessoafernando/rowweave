package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.StreamWindowContext;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
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
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

/** Independent Worker JVM fixture for generic-window admission. */
public final class StreamProductGenericWindowWorkerProcessFixture {
    static final String SOURCE_PROVIDER = "generic-window-source-v1";
    static final String SINK_PROVIDER = "generic-window-sink-v1";
    static final String SOURCE_CONTRACT =
            StreamGenericWindowProductAdmissionTest.digest("cluster-source");
    static final String SINK_CONTRACT =
            StreamGenericWindowProductAdmissionTest.digest("cluster-sink");
    private static final Object SINK_LOCK = new Object();

    private StreamProductGenericWindowWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "role and exchange directory required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY,
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY,
                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                        + StreamGenericWindowProductAdmissionTest.function()
                        .deploymentSetId()));
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "generic-window-worker-" + role,
                        "generic-window-incarnation-" + role,
                        2, 1, capabilities, 8, 16,
                        8_192, 8_388_608L, 32);
        DurableStreamSourceProvider source = new Source();
        EagerStreamSinkProvider sink = new Sink(exchange);
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                "source".equals(role)
                        ? Collections.singletonMap(
                        source.providerKey(), source)
                        : Collections.emptyMap(),
                "sink".equals(role)
                        ? Collections.singletonMap(sink.providerKey(), sink)
                        : Collections.emptyMap(),
                ignored -> new DecimalAggregate(),
                exchange.resolve("generic-window-checkpoints"))) {
            Files.write(exchange.resolve(
                            "generic-window-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve(
                    "generic-window-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class Source
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return SOURCE_PROVIDER; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamGenericWindowProductAdmissionTest
                                    .inputSchema().toPortableSchema()));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame())
                        .getInt() : 0;

                @Override public StreamSourceRecord next() { return null; }

                @Override public StreamSourceEvent nextEvent() throws Exception {
                    if (next < 2) {
                        DataRow row = new DataRow(3);
                        row.set(0, next == 0 ? "A" : "B");
                        long timestamp = next == 0 ? 10L : 20L;
                        row.set(1, new Timestamp(timestamp));
                        row.set(2, BigDecimal.valueOf(next + 1L));
                        next++;
                        return StreamSourceEvent.record(StreamSourceRecord.of(
                                serializer.serialize(row), position(),
                                timestamp));
                    }
                    if (next == 2) {
                        next++;
                        return StreamSourceEvent.watermark(100L);
                    }
                    wakeups.acquire();
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position();
                }

                private byte[] position() {
                    return ByteBuffer.allocate(Integer.BYTES)
                            .putInt(next).array();
                }

                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class Sink implements EagerStreamSinkProvider {
        private final Path result;

        private Sink(Path exchange) {
            this.result = exchange.resolve("generic-window-result");
        }

        @Override public String providerKey() { return SINK_PROVIDER; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamGenericWindowProductAdmissionTest
                                    .outputSchema().toPortableSchema()));
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] frame) throws Exception {
                    DataRow row = serializer.deserialize(frame);
                    String line = row.getString(0) + ':' + row.getLong(1)
                            + ':' + row.getLong(2) + ':'
                            + row.getBigDecimal(3).toPlainString() + '\n';
                    synchronized (SINK_LOCK) {
                        Files.write(result, line.getBytes(StandardCharsets.UTF_8),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND);
                    }
                }

                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class DecimalAggregate
            implements StreamWindowAggregateFunction {
        @Override public byte[] createAccumulator() {
            return bytes(BigDecimal.ZERO);
        }

        @Override public byte[] add(DataRow value, byte[] accumulator) {
            return bytes(decimal(accumulator).add(value.getBigDecimal(2)));
        }

        @Override public byte[] merge(byte[] left, byte[] right) {
            return bytes(decimal(left).add(decimal(right)));
        }

        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            DataRow row = new DataRow(4);
            row.set(0, context.currentKey().get(0));
            row.set(1, context.windowStart());
            row.set(2, context.windowEnd());
            row.set(3, decimal(accumulator).setScale(
                    10, java.math.RoundingMode.HALF_UP));
            return row;
        }

        private static BigDecimal decimal(byte[] value) {
            return new BigDecimal(new String(value, StandardCharsets.UTF_8));
        }

        private static byte[] bytes(BigDecimal value) {
            return value.toPlainString().getBytes(StandardCharsets.UTF_8);
        }
    }
}
