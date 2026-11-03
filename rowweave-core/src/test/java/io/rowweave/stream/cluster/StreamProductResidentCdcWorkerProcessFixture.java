package io.rowweave.stream.cluster;

import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.PublicStreamSourceAdapter;
import io.rowweave.stream.runtime.PublicTransactionalStreamSinkAdapter;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Independent public CDC Worker used by the resident CDC M+2W cluster gate. */
public final class StreamProductResidentCdcWorkerProcessFixture {
    static final String SOURCE = "public-cluster-cdc-source-v1";
    static final String SINK = "public-cluster-cdc-sink-v1";
    static final String SOURCE_CONTRACT = digest("cluster-cdc-source");
    static final String SINK_CONTRACT = digest("cluster-cdc-sink");
    static final String SNAPSHOT = digest("cluster-snapshot-10");
    static final StreamSchema KEY_SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.StringType, false)));
    static final StreamSchema ROW_SCHEMA = new StreamSchema(Arrays.asList(
            new StreamField("id", "id", "id", DataType.StringType, false),
            new StreamField("amount", "amount", "amount",
                    DataType.BigDecimalType, false)));

    private StreamProductResidentCdcWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "role and exchange directory are required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "cdc-worker-" + role,
                        "cdc-incarnation-" + role, 2, 1,
                        Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                                PortableStreamPlan
                                        .NODE_EXECUTION_CONTRACT_CAPABILITY,
                                PortableStreamPlan.UPSERT_CAPABILITY,
                                PortableStreamPlan.RESIDENT_CDC_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CHECKPOINT_TRANSFER_CAPABILITY),
                        8, 16, 8_192, 8_388_608L, 32);
        StreamCdcSourceProvider source = "source".equals(role)
                ? new FileCdcSource(exchange) : null;
        io.rowweave.stream.spi.StreamTransactionalSinkProvider sink =
                "sink".equals(role) ? new FileUpsertSink(exchange) : null;
        TransactionalStreamSinkProvider adaptedSink = sink == null ? null
                : PublicTransactionalStreamSinkAdapter.adapt(sink);
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                source == null ? Collections.emptyMap()
                        : Collections.singletonMap(source.providerKey(),
                        PublicStreamSourceAdapter.adapt(source)),
                Collections.emptyMap(),
                adaptedSink == null ? Collections.emptyMap()
                        : Collections.singletonMap(
                        adaptedSink.providerKey(), adaptedSink),
                reference -> {
                    throw new UnsupportedOperationException(
                            "CDC fixture has no functions");
                }, exchange.resolve("cdc-checkpoints"))) {
            Files.write(exchange.resolve("cdc-" + role + ".identity"),
                    ManagementFactory.getRuntimeMXBean().getName()
                            .getBytes(StandardCharsets.UTF_8));
            Files.write(exchange.resolve("cdc-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("cdc-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class FileCdcSource
            implements StreamCdcSourceProvider {
        private final Path exchange;

        private FileCdcSource(Path exchange) {
            this.exchange = exchange;
        }

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) throws Exception {
            Files.write(exchange.resolve("cdc-open-phases"),
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
                        if (next == 0) {
                            next++;
                            return StreamCdcSourceEvent.logRecord(
                                    context.lastLogSequence() + 1L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "15.00")));
                        }
                        if (next == 1) {
                            next++;
                            return StreamCdcSourceEvent.logRecord(
                                    context.lastLogSequence() + 2L,
                                    codec.encodeTombstone(key("A")));
                        }
                        Files.write(exchange.resolve(
                                        "cdc-restored-log-emitted"),
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
                            Files.write(exchange.resolve(
                                            "cdc-initial-log-emitted"),
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

    private static final class FileUpsertSink implements
            io.rowweave.stream.spi.StreamTransactionalSinkProvider {
        private final Path exchange;

        private FileUpsertSink(Path exchange) {
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
                    return digest(identity).getBytes(
                            StandardCharsets.US_ASCII);
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
                    byte[] transaction = Files.readAllBytes(
                            prepared(identity));
                    if (transaction.length > 0) {
                        String value = lastValue(transaction);
                        Files.write(exchange.resolve("cdc-result"),
                                value.getBytes(StandardCharsets.US_ASCII));
                    }
                    Files.move(prepared(identity), visible(identity),
                            StandardCopyOption.ATOMIC_MOVE);
                }

                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) throws Exception {
                    Files.deleteIfExists(prepared(identity));
                }
                @Override public void close() { }

                private Path prepared(String identity) {
                    return exchange.resolve("cdc-prepared-" + digest(identity));
                }

                private Path visible(String identity) {
                    return exchange.resolve("cdc-visible-" + digest(identity));
                }
            };
        }

        private static String lastValue(byte[] encoded) {
            ByteBuffer input = ByteBuffer.wrap(encoded);
            io.rowweave.stream.api.StreamDataRowCodec keyCodec =
                    new io.rowweave.stream.api.StreamDataRowCodec(KEY_SCHEMA);
            io.rowweave.stream.api.StreamDataRowCodec rowCodec =
                    new io.rowweave.stream.api.StreamDataRowCodec(ROW_SCHEMA);
            StreamUpsertRecord last = null;
            while (input.hasRemaining()) {
                int length = input.getInt();
                byte[] frame = new byte[length];
                input.get(frame);
                last = StreamUpsertRecordCodec.decode(
                        frame, 4 * 1024 * 1024, 4 * 1024 * 1024);
            }
            if (last == null) throw new IllegalStateException(
                    "prepared CDC transaction is empty");
            DataRow stableKey = keyCodec.decode(last.stableKeyFrame());
            if (!last.hasAfterImage()) {
                return stableKey.getString(0) + "=<deleted>";
            }
            DataRow afterImage = rowCodec.decode(last.afterImageFrame());
            return stableKey.getString(0) + '='
                    + afterImage.getBigDecimal(1).toPlainString();
        }
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

    static String digest(String value) {
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
