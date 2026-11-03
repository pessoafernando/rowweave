package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamMasterRuntime;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.StreamWorkerRuntime;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.api.StreamUpsertValue;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.api.StreamWorkerEndpoint;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Runnable same-host M+2W resident CDC example using only public API and SPI.
 * Start the two Worker roles first, then invoke the master role.
 */
public final class StreamClusterCdcExample {
    private static final String SOURCE = "example-cdc-source-v1";
    private static final String SINK = "example-cdc-sink-v1";
    private static final String SOURCE_CONTRACT = digest("example-cdc-source");
    private static final String SINK_CONTRACT = digest("example-cdc-sink");
    private static final String SNAPSHOT = digest("example-snapshot-10");
    private static final StreamSchema KEY_SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.StringType, false)));
    private static final StreamSchema ROW_SCHEMA = new StreamSchema(Arrays.asList(
            new StreamField("id", "id", "id", DataType.StringType, false),
            new StreamField("amount", "amount", "amount",
                    DataType.BigDecimalType, false)));

    private StreamClusterCdcExample() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "role and exchange directory are required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        if ("source-worker".equals(role) || "sink-worker".equals(role)) {
            runWorker(role, exchange);
            return;
        }
        if ("master".equals(role)) {
            try {
                for (String value : runMaster(exchange)) {
                    System.out.println(value);
                }
            } finally {
                Files.write(exchange.resolve("shutdown"), new byte[] {1});
            }
            return;
        }
        throw new IllegalArgumentException("unknown role: " + role);
    }

    /** Runs the Master/client flow after both Worker endpoint files exist. */
    public static List<String> runMaster(Path exchange) throws Exception {
        List<StreamWorkerEndpoint> workers = Arrays.asList(
                endpoint(exchange, "source-worker"),
                endpoint(exchange, "sink-worker"));
        List<String> observations = new ArrayList<>();
        try (StreamMasterRuntime master = RowWeave.startStreamMaster(
                exchange.resolve("cluster"), workers);
             StreamJobClient client = master.createJobClient()) {
            StreamSession session = session(client);
            StreamJobHandle initial = session.submit(
                    StreamSubmissionId.of("example-cdc-initial"));
            requireRunning(initial.awaitRunning(5_000L), "initial job");
            observations.add(initial.progress().cdcPhase().name());
            awaitFile(exchange.resolve("initial-log-emitted"), 5_000L);

            StreamSavepointHandle savepoint = initial.stopWithSavepoint(
                    StreamCommandId.of("example-cdc-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            requireSucceeded(savepoint.awaitCompletion(5_000L),
                    "initial savepoint");
            awaitText(exchange.resolve("result"), "A=12.50", 5_000L);
            observations.add(text(exchange.resolve("result")));
            requireStoppedWithSavepoint(
                    initial.awaitTermination(5_000L), "initial job");
            observations.add("SAVEPOINT");

            StreamJobHandle restored = client.restore(
                    StreamSubmissionId.of("example-cdc-restored"),
                    session.toPlanArtifact(), new StreamRestoreRequest(
                            initial.id(), StreamRecoveryPointId.savepoint(
                            savepoint.savepointId()), 1, 128));
            requireRunning(restored.awaitRunning(5_000L), "restored job");
            observations.add(restored.progress().cdcPhase().name());
            awaitFile(exchange.resolve("restored-log-emitted"), 5_000L);
            if (!"A=12.50".equals(text(exchange.resolve("result")))) {
                throw new IllegalStateException(
                        "restored data became visible before checkpoint");
            }
            StreamCheckpointHandle second = restored.triggerCheckpoint(
                    StreamCommandId.of("example-cdc-restored-cut"));
            requireSucceeded(second.awaitCompletion(5_000L),
                    "restored checkpoint");
            awaitText(exchange.resolve("result"), "A=15.00", 5_000L);
            observations.add(text(exchange.resolve("result")));
            requireSucceeded(restored.cancel(StreamCommandId.of(
                            "example-cdc-restored-cancel"))
                            .awaitCompletion(5_000L), "restored cancel");
        }
        return Collections.unmodifiableList(observations);
    }

    private static void runWorker(String role, Path exchange) throws Exception {
        Files.createDirectories(exchange);
        StreamCdcSourceProvider source = "source-worker".equals(role)
                ? new ExampleSource(exchange) : null;
        StreamTransactionalSinkProvider sink = "sink-worker".equals(role)
                ? new ExampleSink(exchange) : null;
        StreamWorkerConfig config = StreamWorkerConfig.defaults(
                role, exchange.resolve("cluster"));
        try (StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                config,
                source == null ? Collections.emptyList()
                        : Collections.singletonList(source),
                Collections.emptyList(),
                sink == null ? Collections.emptyList()
                        : Collections.singletonList(sink),
                Collections.emptyList())) {
            Files.write(exchange.resolve(role + ".port"),
                    Integer.toString(worker.endpoint().port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("shutdown"))) {
                Thread.sleep(20L);
            }
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "cluster-cdc-example", "cluster-cdc-example", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.residentCdc(
                        ROW_SCHEMA, Collections.singletonList("id"),
                        SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        SNAPSHOT, 10L,
                        Collections.singletonList("snapshot-0")))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {2}));
        return session;
    }

    private static final class ExampleSource
            implements StreamCdcSourceProvider {
        private final Path exchange;

        private ExampleSource(Path exchange) {
            this.exchange = exchange;
        }

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) throws Exception {
            Files.write(exchange.resolve("source-open-phases"),
                    (context.phase().name() + ':'
                            + context.lastLogSequence() + '\n').getBytes(
                            StandardCharsets.US_ASCII),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            StreamUpsertCodec codec = new StreamUpsertCodec(
                    KEY_SCHEMA, ROW_SCHEMA);
            boolean restoredLog = context.phase()
                    == StreamCdcSourceContext.Phase.LOG;
            return new StreamCdcSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next;

                @Override public StreamCdcSourceEvent next() throws Exception {
                    if (restoredLog) {
                        if (next++ == 0) {
                            return StreamCdcSourceEvent.logRecord(
                                    context.lastLogSequence() + 1L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "15.00")));
                        }
                        Files.write(exchange.resolve("restored-log-emitted"),
                                new byte[] {1});
                        return awaitWakeup();
                    }
                    switch (next++) {
                        case 0:
                            return StreamCdcSourceEvent.snapshotAfterImage(
                                    "snapshot-0", codec.encodeAfterImage(
                                            key("A"), row("A", "10.00")));
                        case 1:
                            return StreamCdcSourceEvent.snapshotSplitComplete(
                                    "snapshot-0");
                        case 2:
                            return StreamCdcSourceEvent.handoffComplete();
                        case 3:
                            return StreamCdcSourceEvent.logRecord(11L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "12.50")));
                        default:
                            Files.write(exchange.resolve("initial-log-emitted"),
                                    new byte[] {1});
                            return awaitWakeup();
                    }
                }

                private StreamCdcSourceEvent awaitWakeup()
                        throws InterruptedException {
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L, TimeUnit.MILLISECONDS)) {
                            return null;
                        }
                    }
                    return null;
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

    private static final class ExampleSink
            implements StreamTransactionalSinkProvider {
        private final Path exchange;

        private ExampleSink(Path exchange) {
            this.exchange = exchange;
        }

        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            return new StreamTransactionalSinkWriter() {
                private final ByteArrayOutputStream pending =
                        new ByteArrayOutputStream();

                @Override public void write(byte[] frame) throws Exception {
                    pending.write(ByteBuffer.allocate(4)
                            .putInt(frame.length).array());
                    pending.write(frame);
                }

                @Override public byte[] prepare(String identity)
                        throws Exception {
                    Path prepared = prepared(identity);
                    if (!Files.exists(prepared)) {
                        Files.write(prepared, pending.toByteArray());
                    }
                    return digest(identity).getBytes(StandardCharsets.US_ASCII);
                }

                @Override public StreamTransactionStatus query(
                        String identity) {
                    if (Files.exists(visible(identity))) {
                        return StreamTransactionStatus.VISIBLE;
                    }
                    if (Files.exists(prepared(identity))) {
                        return StreamTransactionStatus.PREPARED;
                    }
                    return StreamTransactionStatus.UNKNOWN;
                }

                @Override public void commit(String identity) throws Exception {
                    if (Files.exists(visible(identity))) return;
                    String value = lastValue(Files.readAllBytes(
                            prepared(identity)));
                    Files.write(exchange.resolve("result"),
                            value.getBytes(StandardCharsets.US_ASCII));
                    Files.move(prepared(identity), visible(identity),
                            StandardCopyOption.ATOMIC_MOVE);
                }

                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) throws Exception {
                    Files.deleteIfExists(prepared(identity));
                }
                @Override public void close() { }

                private Path prepared(String identity) {
                    return exchange.resolve("prepared-" + digest(identity));
                }

                private Path visible(String identity) {
                    return exchange.resolve("visible-" + digest(identity));
                }
            };
        }
    }

    private static StreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return new StreamWorkerEndpoint("127.0.0.1", Integer.parseInt(
                text(exchange.resolve(role + ".port"))));
    }

    private static DataRow key(String value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static DataRow row(String id, String amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, new java.math.BigDecimal(amount));
        return row;
    }

    private static String lastValue(byte[] encoded) {
        ByteBuffer input = ByteBuffer.wrap(encoded);
        StreamUpsertCodec codec = new StreamUpsertCodec(KEY_SCHEMA, ROW_SCHEMA);
        StreamUpsertValue last = null;
        while (input.hasRemaining()) {
            int length = input.getInt();
            byte[] frame = new byte[length];
            input.get(frame);
            last = codec.decode(frame);
        }
        if (last == null) {
            throw new IllegalStateException("prepared CDC transaction is empty");
        }
        return last.stableKey().getString(0) + '='
                + last.afterImage().getBigDecimal(1).toPlainString();
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

    private static void requireStoppedWithSavepoint(
            StreamJobStatus status, String action) {
        if (status != StreamJobStatus.STOPPED_WITH_SAVEPOINT) {
            throw new IllegalStateException(action + " failed: " + status);
        }
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!Files.exists(file) && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        if (!Files.exists(file)) {
            throw new IllegalStateException("timed out waiting for " + file);
        }
    }

    private static void awaitText(
            Path file, String expected, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.exists(file) && expected.equals(text(file))) return;
            Thread.sleep(5L);
        }
        throw new IllegalStateException("timed out waiting for " + expected);
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
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
