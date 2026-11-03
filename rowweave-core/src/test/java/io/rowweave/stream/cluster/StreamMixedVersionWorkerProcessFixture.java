package io.rowweave.stream.cluster;

import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.PublicStreamSinkAdapter;
import io.rowweave.stream.runtime.PublicStreamSourceAdapter;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamDynamicSourceProvider;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSourceEnumeratorContext;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/** Current and frozen-profile Worker JVMs for the N-1/N product gate. */
public final class StreamMixedVersionWorkerProcessFixture {
    public static final String SOURCE = "mixed-version-source-v1";
    public static final String SINK = "mixed-version-sink-v1";
    public static final String SOURCE_CONTRACT = digest("mixed-version-source");
    public static final String SINK_CONTRACT = digest("mixed-version-sink");
    public static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.StringType, false)));

    private StreamMixedVersionWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        boolean previous = "previous-sink".equals(role);
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                role, role + "-incarnation", previous ? 1 : 2, 1,
                previous ? v1Capabilities() : currentCapabilities(),
                8, 32, 8_192, 1_048_576L, 16,
                128, 128, 1_048_576L, 1_048_576L, 128);
        Map<String, io.rowweave.stream.runtime.DurableStreamSourceProvider>
                sources = new LinkedHashMap<>();
        Map<String, io.rowweave.stream.runtime.EagerStreamSinkProvider>
                sinks = new LinkedHashMap<>();
        if (previous) {
            sinks.put(SINK, PublicStreamSinkAdapter.adapt(
                    new FileSink(exchange)));
        } else {
            sources.put(SOURCE, PublicStreamSourceAdapter.adapt(
                    new DynamicSource(exchange)));
        }
        try (StreamRemoteWorkerServer worker = new StreamRemoteWorkerServer(
                profile, sources, sinks,
                exchange.resolve(role + "-checkpoints"))) {
            Files.write(exchange.resolve(role + ".port"),
                    Integer.toString(worker.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("mixed-version-stop"))) {
                Thread.sleep(10L);
            }
        }
    }

    private static List<String> v1Capabilities() {
        return new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY,
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY,
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.FLAT_MAP_CAPABILITY,
                PortableStreamPlan.UNION_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY,
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY,
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY,
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY,
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY));
    }

    private static List<String> currentCapabilities() {
        List<String> values = v1Capabilities();
        values.add(PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY);
        values.add(PortableStreamPlan.DYNAMIC_SPLIT_DISCOVERY_CAPABILITY);
        values.add(PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY);
        values.add(PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY);
        values.add(StreamWorkerCapabilityProfile
                .CHECKPOINT_TRANSFER_CAPABILITY);
        return values;
    }

    private static final class DynamicSource
            implements StreamDynamicSourceProvider {
        private final Path exchange;

        private DynamicSource(Path exchange) { this.exchange = exchange; }

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorContext context) throws Exception {
            Files.write(exchange.resolve("dynamic-enumerator-opened"),
                    new byte[] {1});
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    if (cursor.frame().length == 0) {
                        StreamSplitSpec split = split();
                        return new StreamSplitDiscovery(1L,
                                StreamDiscoveryCursor.of(new byte[] {1}),
                                Collections.singletonList(
                                        StreamSplitChange.add(split, 1L)));
                    }
                    return new StreamSplitDiscovery(1L, cursor,
                            Collections.emptyList());
                }

                @Override public void close() { }
            };
        }

        @Override public StreamSourceReader open(StreamSourceContext context) {
            if (context.configurationFrame()[0] == 9) {
                try {
                    Files.write(exchange.resolve("bounded-source-opened"),
                            new byte[] {1});
                } catch (java.io.IOException failure) {
                    throw new IllegalStateException(failure);
                }
            }
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                                context.restoredPositionFrame()).getInt() : 0;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next == 0) {
                        DataRow row = new DataRow(1);
                        row.set(0, "mixed-v1");
                        next++;
                        return StreamSourceRecord.of(codec.encode(row),
                                position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L,
                                java.util.concurrent.TimeUnit.MILLISECONDS)) {
                            return null;
                        }
                    }
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }

                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
    }

    private static final class FileSink implements StreamSinkProvider {
        private final Path exchange;

        private FileSink(Path exchange) { this.exchange = exchange; }
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> Files.write(
                    exchange.resolve("mixed-version-result"), frame);
        }
    }

    public static StreamSplitSpec split() {
        return new StreamSplitSpec(digest("mixed-version-split"),
                "int32-v1", new byte[] {2});
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(Character.forDigit((one >>> 4) & 0x0f, 16));
                result.append(Character.forDigit(one & 0x0f, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
