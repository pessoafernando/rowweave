package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowSpec;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Runnable Local event-time window checkpoint and exact-restore example. */
public final class StreamLocalEventTimeRecoveryExample {
    private static final String SOURCE = "example-event-source";
    private static final String SINK = "example-event-sink";
    private static final String SOURCE_CONTRACT = digest("example-event-source");
    private static final String SINK_CONTRACT = digest("example-event-sink");
    private static final StreamSchema INPUT_SCHEMA = new StreamSchema(
            Arrays.asList(
                    new StreamField("categoryKey", "categoryKey", "categoryKey",
                            DataType.StringType, false),
                    new StreamField("event-time", "eventTime", "eventTime",
                            DataType.TimestampType, false),
                    new StreamField("quantity", "quantity", "quantity",
                            DataType.BigDecimalType, true)));
    private static final StreamSchema OUTPUT_SCHEMA = new StreamSchema(
            Arrays.asList(
                    new StreamField("categoryKey", "categoryKey", "categoryKey",
                            DataType.StringType, false),
                    new StreamField("window-start", "windowStart",
                            "windowStart", DataType.LongType, false),
                    new StreamField("window-end", "windowEnd", "windowEnd",
                            DataType.LongType, false),
                    new StreamField("total", "total", "total",
                            DataType.BigDecimalType, false)));

    private StreamLocalEventTimeRecoveryExample() { }

    public static List<String> run(Path stateRoot) throws Exception {
        ExampleSource source = new ExampleSource();
        ExampleSink sink = new ExampleSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                stateRoot, Collections.singletonList(source),
                Collections.singletonList(sink), Collections.emptyList())) {
            StreamSession session = session(client);
            StreamJobHandle initial = session.submit(
                    StreamSubmissionId.of("example-event-initial"));
            requireRunning(initial.awaitRunning(5_000L), "initial job");
            source.awaitPendingWindow(5_000L);

            StreamCheckpointHandle checkpoint = initial.triggerCheckpoint(
                    StreamCommandId.of("example-event-window-cut"));
            requireSucceeded(checkpoint.awaitCompletion(5_000L),
                    "window checkpoint");
            requireSucceeded(initial.cancel(StreamCommandId.of(
                            "example-event-initial-cancel"))
                            .awaitCompletion(5_000L), "initial cancel");

            StreamJobHandle restored = client.restore(
                    StreamSubmissionId.of("example-event-restored"),
                    session.toPlanArtifact(), new StreamRestoreRequest(
                            initial.id(), StreamRecoveryPointId.checkpoint(
                            checkpoint.checkpointId()), 1, 128));
            requireRunning(restored.awaitRunning(5_000L), "restored job");
            source.releaseFinalWatermark();
            sink.awaitRows(1, 5_000L);
            requireSucceeded(restored.cancel(StreamCommandId.of(
                            "example-event-restored-cancel"))
                            .awaitCompletion(5_000L), "restored cancel");
            return sink.snapshot();
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "local-event-time-recovery",
                "local-event-time-recovery", client);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(
                5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 4_096L, 16, 16,
                16_384L, 5, 32, 1_048_576L));
        DataStream keyed = session.fromSource(
                        StreamSourceSpec.exactOrderedEventTime(
                                INPUT_SCHEMA, SOURCE, SOURCE_CONTRACT,
                                new byte[] {1}, Collections.singletonList(
                                new StreamSplitSpec(digest("example-event-split"),
                                        "int32-v1", new byte[] {2})),
                                StreamWatermarkPolicy.providerEvents(5_000L)))
                .uid("source")
                .keyBy("categoryKey").uid("category-key");
        keyed.eventTimeDecimalWindow(
                        StreamWindowSpec.tumblingDecimal(
                                "event-time", "quantity", 1_000L,
                                "window-start", "window-end", "total"),
                        OUTPUT_SCHEMA,
                        StreamStateSpec.decimalWindow("category-quantity-window"))
                .uid("category-quantity-window")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static final class ExampleSource implements StreamSourceProvider {
        private final Semaphore pendingWindow = new Semaphore(0);
        private final AtomicBoolean pendingWindowSignaled = new AtomicBoolean();
        private final AtomicBoolean releaseWatermark = new AtomicBoolean();
        private final AtomicReference<Semaphore> activeSignals =
                new AtomicReference<>();

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(INPUT_SCHEMA);
            Semaphore signals = new Semaphore(0);
            activeSignals.set(signals);
            return new StreamSourceReader() {
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                        context.restoredPositionFrame()).getInt() : 0;

                @Override public StreamSourceRecord next() {
                    return null;
                }

                @Override public StreamSourceEvent nextEvent()
                        throws Exception {
                    if (next == 0) {
                        next++;
                        return StreamSourceEvent.record(record(
                                codec, next, 100L,
                                new BigDecimal("1.00000000006")));
                    }
                    if (next == 1) {
                        next++;
                        if (pendingWindowSignaled.compareAndSet(false, true)) {
                            pendingWindow.release();
                        }
                        return StreamSourceEvent.record(record(
                                codec, next, 200L,
                                new BigDecimal("2.00000000005")));
                    }
                    while (!canceled) {
                        if (next == 2 && releaseWatermark.get()) {
                            next++;
                            return StreamSourceEvent.watermark(1_000L);
                        }
                        if (signals.tryAcquire(20L, TimeUnit.MILLISECONDS)) {
                            if (releaseWatermark.get()) continue;
                            return null;
                        }
                    }
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { signals.release(); }
                @Override public void cancel() {
                    canceled = true;
                    signals.release();
                }
                @Override public void close() { cancel(); }
            };
        }

        void awaitPendingWindow(long timeoutMillis)
                throws InterruptedException {
            if (!pendingWindow.tryAcquire(timeoutMillis,
                    TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("pending window timed out");
            }
        }

        void releaseFinalWatermark() {
            releaseWatermark.set(true);
            Semaphore signals = activeSignals.get();
            if (signals != null) signals.release();
        }

        private static StreamSourceRecord record(StreamDataRowCodec codec,
                int position, long timestamp, BigDecimal quantity) {
            DataRow row = new DataRow(3);
            row.set(0, "category-a");
            row.set(1, new Timestamp(timestamp));
            row.set(2, quantity);
            return StreamSourceRecord.of(
                    codec.encode(row), position(position), timestamp);
        }

        private static byte[] position(int value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
    }

    private static final class ExampleSink implements StreamSinkProvider {
        private final List<String> rows = new ArrayList<>();
        private final StreamDataRowCodec codec = new StreamDataRowCodec(OUTPUT_SCHEMA);

        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> {
                DataRow row = codec.decode(frame);
                String value = row.getString(0) + ':' + row.getLong(1)
                        + ':' + row.getLong(2) + ':'
                        + row.getBigDecimal(3).toPlainString();
                synchronized (rows) {
                    rows.add(value);
                    rows.notifyAll();
                }
            };
        }

        void awaitRows(int count, long timeoutMillis)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            synchronized (rows) {
                while (rows.size() < count && System.nanoTime() < deadline) {
                    TimeUnit.NANOSECONDS.timedWait(
                            rows, deadline - System.nanoTime());
                }
                if (rows.size() < count) {
                    throw new IllegalStateException("window output timed out");
                }
            }
        }

        List<String> snapshot() {
            synchronized (rows) {
                return Collections.unmodifiableList(new ArrayList<>(rows));
            }
        }
    }

    private static void requireSucceeded(
            StreamOperationStatus status, String action) {
        if (status != StreamOperationStatus.SUCCEEDED) {
            throw new IllegalStateException(action + " failed: " + status);
        }
    }

    private static void requireRunning(StreamJobStatus status, String action) {
        if (status != StreamJobStatus.RUNNING) {
            throw new IllegalStateException(action + " failed: " + status);
        }
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
