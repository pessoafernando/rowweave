package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.PublicStreamSourceAdapter;
import io.rowweave.stream.runtime.PublicTransactionalStreamSinkAdapter;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Semaphore;

/** Independent public-provider Worker used by the transactional M+2W cluster gate. */
public final class StreamProductTransactionalWorkerProcessFixture {
    static final String SOURCE = "public-cluster-exact-source-v1";
    static final String SINK = "public-cluster-exact-sink-v1";
    static final String SOURCE_CONTRACT = digest("cluster-exact-source");
    static final String SINK_CONTRACT = digest("cluster-exact-sink");

    private StreamProductTransactionalWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "role and exchange directory are required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "transactional-worker-" + role,
                        "transactional-incarnation-" + role, 3, 1,
                        Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                                PortableStreamPlan
                                        .NODE_EXECUTION_CONTRACT_CAPABILITY,
                                PortableStreamPlan
                                        .SOURCE_END_OF_INPUT_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CHECKPOINT_TRANSFER_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .PROGRESS_CAPABILITY),
                        8, 16, 8_192, 8_388_608L, 32);
        StreamSourceProvider source = role.startsWith("source")
                ? new ExactSource(exchange) : null;
        io.rowweave.stream.spi.StreamTransactionalSinkProvider sink =
                role.startsWith("sink") ? new FileTransactionalSink(exchange)
                        : null;
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
                            "transactional fixture has no functions");
                }, exchange.resolve(
                        "transactional-checkpoints-" + role))) {
            Files.write(exchange.resolve(
                            "transactional-" + role + ".identity"),
                    ManagementFactory.getRuntimeMXBean().getName()
                            .getBytes(StandardCharsets.UTF_8));
            Files.write(exchange.resolve(
                            "transactional-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve(
                    "transactional-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class ExactSource implements StreamSourceProvider {
        private final Path exchange;
        private ExactSource(Path exchange) { this.exchange = exchange; }
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context)
                throws Exception {
            int restored = context.hasRestoredPosition()
                    ? ByteBuffer.wrap(
                    context.restoredPositionFrame()).getInt() : 0;
            Files.write(exchange.resolve(
                            "transactional-source-open-" + restored),
                    new byte[] {1});
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = restored;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < 2) {
                        byte[] frame = (next++ == 0 ? "one" : "two")
                                .getBytes(StandardCharsets.UTF_8);
                        if (next == 2) {
                            Files.write(exchange.resolve(
                                    "transactional-source-emitted"),
                                    new byte[] {1});
                        }
                        return StreamSourceRecord.of(frame, position(next));
                    }
                    wakeups.acquire();
                    return canceled ? null : null;
                }
                @Override public StreamSourceEvent nextEvent() throws Exception {
                    if (next < 2) return StreamSourceEvent.record(next());
                    if (Files.exists(exchange.resolve(
                            "transactional-bounded-source"))) {
                        if (Files.exists(exchange.resolve(
                                "transactional-pause-before-end"))) {
                            Files.write(exchange.resolve(
                                    "transactional-source-before-end"),
                                    new byte[] {1});
                            while (!canceled && Files.exists(exchange.resolve(
                                    "transactional-pause-before-end"))) {
                                wakeups.tryAcquire(50L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS);
                            }
                            if (canceled) return null;
                        }
                        next++;
                        return StreamSourceEvent.endOfInput();
                    }
                    return StreamSourceReader.super.nextEvent();
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

    private static final class FileTransactionalSink implements
            io.rowweave.stream.spi.StreamTransactionalSinkProvider {
        private final Path exchange;
        private FileTransactionalSink(Path exchange) { this.exchange = exchange; }
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            int subtaskIndex = context.subtaskIndex();
            return new StreamTransactionalSinkWriter() {
                private final java.io.ByteArrayOutputStream pending =
                        new java.io.ByteArrayOutputStream();
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
                @Override public StreamTransactionStatus query(String identity) {
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
                    if (Files.exists(exchange.resolve(
                            "transactional-long-delay-commit"))) {
                        Thread.sleep(1_500L);
                    } else if (Files.exists(exchange.resolve(
                            "transactional-delay-commit"))) {
                        Thread.sleep(300L);
                    }
                    Files.move(prepared(identity), visible(identity),
                            StandardCopyOption.ATOMIC_MOVE);
                    Files.write(exchange.resolve(
                                    "transactional-result-" + subtaskIndex),
                            decode(Files.readAllBytes(visible(identity)))
                                    .getBytes(StandardCharsets.UTF_8));
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

    private static String decode(byte[] frame) {
        ByteBuffer input = ByteBuffer.wrap(frame);
        StringBuilder result = new StringBuilder();
        while (input.hasRemaining()) {
            int length = input.getInt();
            byte[] value = new byte[length];
            input.get(value);
            if (result.length() > 0) result.append(',');
            result.append(new String(value, StandardCharsets.UTF_8));
        }
        return result.toString();
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
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
