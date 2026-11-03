package io.rowweave.stream.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.RetractableStreamAggregateFunction;
import io.rowweave.StreamAggregateFunction;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Independent Worker fixture for the public managed keyed aggregate path. */
public final class StreamProductAggregateWorkerProcessFixture {
    static final String SOURCE_PROVIDER = "aggregate-source-v1";
    static final String SINK_PROVIDER = "aggregate-sink-v1";
    static final String SOURCE_CONTRACT = digest("aggregate-source-contract");
    static final String SINK_CONTRACT = digest("aggregate-sink-contract");
    static final String DEPLOYMENT_SET = digest("aggregate-deployment-set");
    static final String JAR_DIGEST = digest("aggregate-function-jar");

    private StreamProductAggregateWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException(
                    "role, mode and exchange directory required");
        }
        String role = arguments[0];
        String mode = arguments[1];
        Path exchange = Paths.get(arguments[2]);
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY));
        if ("retract".equals(mode)) {
            capabilities.add(
                    PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY);
        }
        boolean sourceRole = role.startsWith("source");
        boolean sinkRole = role.startsWith("sink");
        if (sourceRole) {
            capabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                    + DEPLOYMENT_SET);
        }
            StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                "aggregate-worker-" + role, "aggregate-incarnation-" + role,
                2, 1, capabilities, 16, 128,
                8_192, 8_388_608L, 256);
        DurableStreamSourceProvider source = sourceRole
                ? new AggregateSourceProvider(mode, exchange) : null;
        EagerStreamSinkProvider sink = sinkRole
                ? new AggregateSinkProvider(exchange.resolve(
                        "aggregate-result"), mode) : null;
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                source == null ? Collections.emptyMap()
                        : Collections.singletonMap(source.providerKey(), source),
                sink == null ? Collections.emptyMap()
                        : Collections.singletonMap(sink.providerKey(), sink),
                reference -> resolve(reference),
                exchange.resolve("aggregate-checkpoint-store"))) {
            Files.write(exchange.resolve("aggregate-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("aggregate-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    static FunctionDeploymentReference function() {
        byte[] configuration = new byte[] {4};
        return new FunctionDeploymentReference(
                DEPLOYMENT_SET, "decimal-readout", JAR_DIGEST,
                DecimalReadoutAggregate.class.getName(),
                FunctionAbi.STREAM_AGGREGATE_V1,
                "stream-function-config-v1", digest(configuration),
                configuration, new FunctionResourceProfile(2_048L, 0L, 1));
    }

    static FunctionDeploymentReference retractFunction() {
        byte[] configuration = new byte[] {5};
        return new FunctionDeploymentReference(
                DEPLOYMENT_SET, "retract-decimal-readout", JAR_DIGEST,
                RetractableDecimalReadoutAggregate.class.getName(),
                FunctionAbi.STREAM_RETRACT_AGGREGATE_V2,
                "stream-retract-function-config-v2", digest(configuration),
                configuration, new FunctionResourceProfile(2_048L, 0L, 1));
    }

    private static java.io.Serializable resolve(
            FunctionDeploymentReference reference) {
        if (function().equals(reference)) return new DecimalReadoutAggregate();
        if (retractFunction().equals(reference)) {
            return new RetractableDecimalReadoutAggregate();
        }
        throw new IllegalArgumentException(
                "signed Stream aggregate deployment is unavailable");
    }

    static DataSchema dataSchema() {
        return new DataSchema(new Field[] {
                new Field("adaptor", "adaptor", DataType.StringType),
                new Field("amount", "amount", DataType.BigDecimalType)});
    }

    public static final class DecimalReadoutAggregate
            implements StreamAggregateFunction {
        private static final byte[] EMPTY = new byte[] {0};
        private final RowSerializer serializer = new RowSerializer(dataSchema());

        @Override public byte[] createAccumulator() { return EMPTY.clone(); }

        @Override public byte[] add(DataRow value, byte[] currentAccumulator) {
            DataRow aggregate = new DataRow(2);
            if (currentAccumulator.length == 1
                    && currentAccumulator[0] == EMPTY[0]) {
                aggregate.set(0, value.getString(0));
                aggregate.set(1, value.getBigDecimal(1));
            } else {
                DataRow current = serializer.deserialize(currentAccumulator);
                if (!current.getString(0).equals(value.getString(0))) {
                    throw new IllegalArgumentException(
                            "aggregate state crossed a stable key");
                }
                aggregate.set(0, current.getString(0));
                aggregate.set(1, current.getBigDecimal(1).add(
                        value.getBigDecimal(1)));
            }
            return serializer.serialize(aggregate);
        }

        @Override public DataRow getResult(byte[] accumulator) {
            DataRow current = serializer.deserialize(accumulator);
            DataRow result = new DataRow(2);
            result.set(0, current.getString(0));
            result.set(1, current.getBigDecimal(1).setScale(
                    10, RoundingMode.HALF_UP));
            return result;
        }
    }

    public static final class RetractableDecimalReadoutAggregate
            implements RetractableStreamAggregateFunction {
        @Override public byte[] createAccumulator() {
            return state("", BigDecimal.ZERO, 0L);
        }

        @Override public byte[] add(DataRow value, byte[] accumulator) {
            Readout current = readout(accumulator);
            String groupKey = current.count == 0L
                    ? value.getString(0) : current.groupKey;
            if (!groupKey.equals(value.getString(0))) {
                throw new IllegalArgumentException(
                        "aggregate state crossed a stable key");
            }
            return state(groupKey, current.amount.add(
                    value.getBigDecimal(1)), current.count + 1L);
        }

        @Override public byte[] retract(DataRow value, byte[] accumulator) {
            Readout current = readout(accumulator);
            if (current.count < 1L
                    || !current.groupKey.equals(value.getString(0))) {
                throw new IllegalArgumentException(
                        "aggregate retraction differs from retained state");
            }
            return state(current.groupKey, current.amount.subtract(
                    value.getBigDecimal(1)), current.count - 1L);
        }

        @Override public boolean isEmpty(byte[] accumulator) {
            return readout(accumulator).count == 0L;
        }

        @Override public DataRow getResult(byte[] accumulator) {
            Readout current = readout(accumulator);
            DataRow result = new DataRow(2);
            result.set(0, current.groupKey);
            result.set(1, current.amount.setScale(
                    10, RoundingMode.HALF_UP));
            return result;
        }

        private static byte[] state(
                String groupKey, BigDecimal amount, long count) {
            return (groupKey + '|' + amount.toPlainString() + '|' + count)
                    .getBytes(StandardCharsets.UTF_8);
        }

        private static Readout readout(byte[] state) {
            String text = new String(state, StandardCharsets.UTF_8);
            int first = text.indexOf('|');
            int last = text.lastIndexOf('|');
            return new Readout(text.substring(0, first),
                    new BigDecimal(text.substring(first + 1, last)),
                    Long.parseLong(text.substring(last + 1)));
        }
    }

    private static final class Readout {
        final String groupKey;
        final BigDecimal amount;
        final long count;

        private Readout(String groupKey, BigDecimal amount, long count) {
            this.groupKey = groupKey;
            this.amount = amount;
            this.count = count;
        }
    }

    private static final class AggregateSourceProvider
            implements DurableStreamSourceProvider {
        private final Path exchange;
        private final String mode;

        private AggregateSourceProvider(String mode, Path exchange) {
            this.mode = mode;
            this.exchange = exchange;
        }

        @Override public String providerKey() { return SOURCE_PROVIDER; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(dataSchema());
            return new StreamSourceReader() {
                private final CountDownLatch cancelled = new CountDownLatch(1);
                private final Semaphore checkpointWakeups = new Semaphore(0);
                private int next;

                {
                    if (("rescale".equals(mode)
                            || "restore".equals(mode)
                            || "worker-loss".equals(mode))
                            && request.hasRestoredPosition()) {
                        next = ByteBuffer.wrap(
                                request.restoredPositionFrame()).getInt();
                    }
                }

                @Override public StreamSourceRecord next() throws Exception {
                    if ("retract".equals(mode)) {
                        return retractRecord(serializer);
                    }
                    if ("rescale".equals(mode)
                            || "restore".equals(mode)
                            || "worker-loss".equals(mode)) {
                        return rescaleRecord(serializer,
                                request.hasRestoredPosition());
                    }
                    if ("ttl-pressure".equals(mode)
                            || "backpressure".equals(mode)) {
                        return ttlRecord(serializer);
                    }
                    if (next == 0) return record(serializer, "A",
                            new BigDecimal("1.00000000006"), next++);
                    if (next == 1) return record(serializer, "A",
                            new BigDecimal("2.00000000005"), next++);
                    if (next == 2) {
                        StreamSourceRecord record = record(serializer, "B",
                                new BigDecimal("4.2"), next++);
                        Files.write(exchange.resolve("aggregate-input-ready"),
                                new byte[] {1});
                        return record;
                    }
                    while (true) {
                        if (checkpointWakeups.tryAcquire(
                                10L, TimeUnit.MILLISECONDS)) return null;
                        if (cancelled.getCount() == 0L) return null;
                    }
                }

                private StreamSourceRecord retractRecord(
                        RowSerializer serializer) throws Exception {
                    if (next == 0) return change(serializer, "A",
                            new BigDecimal("1.00000000006"), next++, false);
                    if (next == 1) return change(serializer, "A",
                            new BigDecimal("2.00000000005"), next++, false);
                    if (next == 2) return change(serializer, "A",
                            new BigDecimal("2.00000000005"), next++, true);
                    if (next == 3) {
                        StreamSourceRecord result = change(serializer, "B",
                                new BigDecimal("4.2"), next++, false);
                        Files.write(exchange.resolve("aggregate-input-ready"),
                                new byte[] {1});
                        return result;
                    }
                    return awaitWakeup();
                }

                private StreamSourceRecord rescaleRecord(
                        RowSerializer serializer, boolean restored)
                        throws Exception {
                    int limit = restored ? 4 : 2;
                    if (next < limit) {
                        int position = next++;
                        String groupKey = position % 2 == 0 ? "A" : "B";
                        return record(serializer, groupKey,
                                BigDecimal.valueOf(position + 1L), next);
                    }
                    return awaitWakeup();
                }

                private StreamSourceRecord ttlRecord(RowSerializer serializer)
                        throws Exception {
                    if ("backpressure".equals(mode)) {
                        if (next < 1_000) {
                            int position = next++;
                            Files.write(exchange.resolve(
                                    "aggregate-source-count"),
                                    Integer.toString(next).getBytes(
                                            StandardCharsets.US_ASCII));
                            return record(serializer, "A", BigDecimal.ONE,
                                    position);
                        }
                        return awaitWakeup();
                    }
                    String[] groupKeys = {"A", "B", "C", "A", "D", "E"};
                    String[] amounts = {"1.00000000006", "1", "1",
                            "2.00000000005", "1", "1"};
                    if (next == 3) Thread.sleep(250L);
                    if (next < groupKeys.length) {
                        int position = next++;
                        return record(serializer, groupKeys[position],
                                new BigDecimal(amounts[position]), position);
                    }
                    return awaitWakeup();
                }

                private StreamSourceRecord awaitWakeup() throws Exception {
                    while (true) {
                        if (checkpointWakeups.tryAcquire(
                                10L, TimeUnit.MILLISECONDS)) return null;
                        if (cancelled.getCount() == 0L) return null;
                    }
                }

                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }

                @Override public void wakeUp() { checkpointWakeups.release(); }
                @Override public void cancel() {
                    checkpointWakeups.release();
                    cancelled.countDown();
                }
                @Override public void close() {
                    checkpointWakeups.release();
                    cancelled.countDown();
                }
            };
        }
    }

    private static StreamSourceRecord record(RowSerializer serializer,
            String groupKey, BigDecimal amount, int position) {
        DataRow row = new DataRow(2);
        row.set(0, groupKey);
        row.set(1, amount);
        return StreamSourceRecord.of(serializer.serialize(row),
                ByteBuffer.allocate(4).putInt(position).array());
    }

    private static StreamSourceRecord change(RowSerializer serializer,
            String groupKey, BigDecimal amount, int position,
            boolean delete) {
        DataRow row = new DataRow(2);
        row.set(0, groupKey);
        row.set(1, amount);
        byte[] image = serializer.serialize(row);
        StreamChangeRecord change = delete
                ? StreamChangeRecord.delete(key(groupKey), image)
                : StreamChangeRecord.insert(image);
        return StreamSourceRecord.of(StreamChangeRecordCodec.encode(change),
                ByteBuffer.allocate(4).putInt(position + 1).array());
    }

    private static byte[] key(String groupKey) {
        DataRow key = new DataRow(1);
        key.set(0, groupKey);
        return new RowSerializer(new DataSchema(new Field[] {
                new Field("adaptor", "adaptor", DataType.StringType)}))
                .serialize(key);
    }

    private static final class AggregateSinkProvider
            implements EagerStreamSinkProvider {
        private final Path result;
        private final String mode;
        private final int expectedRecords;
        private final java.util.Map<String, Summary> summaries =
                new java.util.HashMap<>();

        private AggregateSinkProvider(Path result, String mode) {
            this.result = result;
            this.mode = mode;
            this.expectedRecords = "backpressure".equals(mode) ? 1_000
                    : "ttl-pressure".equals(mode) ? 6
                    : "retract".equals(mode) ? 6
                    : "rescale".equals(mode)
                    || "restore".equals(mode)
                    || "worker-loss".equals(mode) ? 2 : 3;
        }
        @Override public String providerKey() { return SINK_PROVIDER; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(dataSchema());
            String attempt = request.inputChannelIdentity().runId() + ':'
                    + request.inputChannelIdentity().attemptId();
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) throws Exception {
                    if ("retract".equals(mode)) {
                        StreamChangeRecord change =
                                StreamChangeRecordCodec.decode(
                                        recordFrame, 4_096, 4_096, 4_096);
                        DataRow row = serializer.deserialize(
                                change.hasBeforeImage()
                                        ? change.beforeImageFrame()
                                        : change.afterImageFrame());
                        record(attempt, change.kind().name() + ':'
                                + row.getString(0) + '='
                                + row.getBigDecimal(1).toPlainString());
                        return;
                    }
                    StreamUpsertRecord upsert = StreamUpsertRecordCodec.decode(
                            recordFrame, 4_096, 4_096);
                    DataRow key = new RowSerializer(new DataSchema(new Field[] {
                            new Field("adaptor", "adaptor", DataType.StringType)
                    })).deserialize(upsert.stableKeyFrame());
                    DataRow row = serializer.deserialize(upsert.afterImageFrame());
                    if (!key.getString(0).equals(row.getString(0))) {
                        throw new IllegalArgumentException(
                                "UPSERT key differs from its after-image");
                    }
                    record(attempt, row);
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }

        private synchronized void record(String attempt, DataRow row)
                throws Exception {
            Summary summary = summaries.computeIfAbsent(attempt,
                    ignored -> new Summary());
            if ("backpressure".equals(mode) && summary.records == 0) {
                Files.write(result.resolveSibling(
                        "aggregate-sink-stalled"), new byte[] {1});
                while (!Files.exists(result.resolveSibling(
                        "aggregate-release-sink"))) Thread.sleep(5L);
            }
            if (!"backpressure".equals(mode)) {
                summary.output.append(row.getString(0)).append('=')
                        .append(row.getBigDecimal(1).toPlainString())
                        .append('\n');
            }
            if (++summary.records == expectedRecords) {
                String completed = "backpressure".equals(mode)
                        ? summary.records + ":"
                        + row.getBigDecimal(1).toPlainString()
                        : summary.output.toString();
                Files.write(result, completed.getBytes(
                        StandardCharsets.US_ASCII));
            }
        }

        private synchronized void record(String attempt, String line)
                throws Exception {
            Summary summary = summaries.computeIfAbsent(attempt,
                    ignored -> new Summary());
            summary.output.append(line).append('\n');
            if (++summary.records == expectedRecords) {
                Files.write(result, summary.output.toString().getBytes(
                        StandardCharsets.US_ASCII));
            }
        }

        private static final class Summary {
            final StringBuilder output = new StringBuilder();
            int records;
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
