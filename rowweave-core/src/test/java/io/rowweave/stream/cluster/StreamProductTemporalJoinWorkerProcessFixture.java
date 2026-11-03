package io.rowweave.stream.cluster;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceEvent;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Independent Worker fixture for the public finite temporal join path. */
public final class StreamProductTemporalJoinWorkerProcessFixture {
    static final String LEFT_SOURCE = "temporal-left-v1";
    static final String RIGHT_SOURCE = "temporal-right-v1";
    static final String SINK = "temporal-sink-v1";
    static final String SECOND_SINK = "temporal-second-sink-v1";
    static final String LEFT_CONTRACT = digest("temporal-left-contract");
    static final String RIGHT_CONTRACT = digest("temporal-right-contract");
    static final String SINK_CONTRACT = digest("temporal-sink-contract");
    static final String SECOND_SINK_CONTRACT =
            digest("temporal-second-sink-contract");
    private static final String DEPLOYMENT_SET = digest("temporal-deployment-set");
    private static final String JAR_DIGEST = digest("temporal-function-jar");
    private static final CountDownLatch LEFT_READY = new CountDownLatch(1);

    private StreamProductTemporalJoinWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("role and exchange directory required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY));
        if ("source".equals(role)) {
            capabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                    + DEPLOYMENT_SET);
        }
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                "temporal-worker-" + role, "temporal-incarnation-" + role,
                2, 1, capabilities, 16, 128,
                8_192, 8_388_608L, 256);
        Map<String, DurableStreamSourceProvider> sources = new LinkedHashMap<>();
        if ("source".equals(role)) {
            DurableStreamSourceProvider left = new Source(true);
            DurableStreamSourceProvider right = new Source(false);
            sources.put(left.providerKey(), left);
            sources.put(right.providerKey(), right);
        }
        Map<String, EagerStreamSinkProvider> sinks = new LinkedHashMap<>();
        if ("sink".equals(role)) {
            EagerStreamSinkProvider sink = new Sink(
                    SINK, SINK_CONTRACT,
                    exchange.resolve("temporal-result"));
            sinks.put(sink.providerKey(), sink);
        } else if ("sink-second".equals(role)) {
            EagerStreamSinkProvider sink = new Sink(
                    SECOND_SINK, SECOND_SINK_CONTRACT,
                    exchange.resolve("temporal-second-result"));
            sinks.put(sink.providerKey(), sink);
        }
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, sources, sinks,
                reference -> resolve(reference, exchange),
                exchange.resolve("temporal-checkpoint-store"))) {
            Files.write(exchange.resolve("temporal-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("temporal-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    static FunctionDeploymentReference function() {
        byte[] configuration = new byte[] {4};
        return new FunctionDeploymentReference(
                DEPLOYMENT_SET, "temporal-pair-sum", JAR_DIGEST,
                PairSum.class.getName(), FunctionAbi.CO_GROUP_V1,
                "stream-function-config-v1", digest(configuration),
                configuration, new FunctionResourceProfile(2_048L, 0L, 1));
    }

    private static java.io.Serializable resolve(
            FunctionDeploymentReference reference, Path exchange) {
        Path delay = exchange.resolve("temporal-delay-bootstrap");
        if (Files.exists(delay)) {
            try {
                Files.write(exchange.resolve("temporal-bootstrap-delayed"),
                        new byte[] {1});
                long deadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(5L);
                while (!Files.exists(exchange.resolve(
                        "temporal-release-bootstrap"))
                        && System.nanoTime() < deadline) {
                    Thread.sleep(5L);
                }
            } catch (Exception failure) {
                throw new IllegalStateException(
                        "temporal bootstrap delay failed", failure);
            }
        }
        if (function().equals(reference)) return new PairSum();
        throw new IllegalArgumentException(
                "signed Stream temporal function is unavailable");
    }

    static DataSchema dataSchema() {
        return new DataSchema(new Field[] {
                new Field("adaptor", "adaptor", DataType.StringType),
                new Field("amount", "amount", DataType.BigDecimalType)});
    }

    public static final class PairSum implements CoGroupFunction {
        @Override public void coGroup(Iterable<DataRow> left,
                Iterable<DataRow> right, Collector collector) {
            DataRow leftRow = left.iterator().next();
            DataRow rightRow = right.iterator().next();
            DataRow result = new DataRow(2);
            result.set(0, leftRow.getString(0));
            result.set(1, leftRow.getBigDecimal(1)
                    .add(rightRow.getBigDecimal(1))
                    .setScale(10, RoundingMode.HALF_UP));
            collector.collect(result);
        }

        @Override public DataSchema getResultSchema() { return dataSchema(); }
    }

    private static final class Source implements DurableStreamSourceProvider {
        private final boolean left;

        private Source(boolean left) { this.left = left; }

        @Override public String providerKey() {
            return left ? LEFT_SOURCE : RIGHT_SOURCE;
        }

        @Override public String contractDigest() {
            return left ? LEFT_CONTRACT : RIGHT_CONTRACT;
        }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(dataSchema());
            return new StreamSourceReader() {
                private final CountDownLatch canceled = new CountDownLatch(1);
                private final Semaphore wakeups = new Semaphore(0);
                private int next;

                @Override public StreamSourceRecord next() throws Exception {
                    StreamSourceEvent event = nextEvent();
                    return event == null ? null : event.record();
                }

                @Override public StreamSourceEvent nextEvent() throws Exception {
                    if (left && next == 0) {
                        next++;
                        LEFT_READY.countDown();
                        return event(serializer, "A",
                                new BigDecimal("1.00000000006"), 100L, 0);
                    }
                    if (!left && next < 2) {
                        LEFT_READY.await(5L, TimeUnit.SECONDS);
                        int position = next++;
                        return event(serializer, "A", position == 0
                                ? new BigDecimal("2.00000000005")
                                : new BigDecimal("3.00000000004"),
                                position == 0 ? 90L : 120L, position);
                    }
                    while (true) {
                        if (wakeups.tryAcquire(10L, TimeUnit.MILLISECONDS)) {
                            return null;
                        }
                        if (canceled.getCount() == 0L) return null;
                    }
                }

                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }

                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    wakeups.release();
                    canceled.countDown();
                }
                @Override public void close() {
                    wakeups.release();
                    canceled.countDown();
                }
            };
        }
    }

    private static StreamSourceEvent event(RowSerializer serializer,
            String groupKey, BigDecimal amount, long timestamp, int position) {
        DataRow row = new DataRow(2);
        row.set(0, groupKey);
        row.set(1, amount);
        return StreamSourceEvent.record(StreamSourceRecord.of(
                serializer.serialize(row),
                ByteBuffer.allocate(4).putInt(position).array(), timestamp));
    }

    private static final class Sink implements EagerStreamSinkProvider {
        private final String provider;
        private final String contract;
        private final Path result;
        private Sink(String provider, String contract, Path result) {
            this.provider = provider;
            this.contract = contract;
            this.result = result;
        }
        @Override public String providerKey() { return provider; }
        @Override public String contractDigest() { return contract; }

        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(dataSchema());
            return new EagerStreamSinkWriter() {
                private final StringBuilder output = new StringBuilder();
                private int records;

                @Override public void write(byte[] recordFrame) throws Exception {
                    StreamUpsertRecord upsert = StreamUpsertRecordCodec.decode(
                            recordFrame, 4_096, 4_096);
                    DataRow row = serializer.deserialize(upsert.afterImageFrame());
                    output.append(row.getString(0)).append('=')
                            .append(row.getBigDecimal(1).toPlainString())
                            .append('\n');
                    if (++records == 2) {
                        Files.write(result, output.toString().getBytes(
                                StandardCharsets.US_ASCII));
                    }
                }

                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    static String digest(String value) {
        return StreamProductStatelessWorkerProcessFixture.digestText(value);
    }

    private static String digest(byte[] value) {
        return StreamProductStatelessWorkerProcessFixture.hex(
                StreamProductStatelessWorkerProcessFixture.digest()
                        .digest(value));
    }
}
