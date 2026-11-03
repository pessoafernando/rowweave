package io.rowweave.external;

import io.rowweave.RowWeave;
import io.rowweave.StreamWorkerRuntime;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.Semaphore;

/** External-style Worker process: imports only public API and SPI packages. */
public final class StreamPublicWorkerBootstrapFixture {
    public static final String SOURCE = "bootstrap-source-v1";
    public static final String SINK = "bootstrap-sink-v1";
    public static final String SOURCE_CONTRACT = digest("bootstrap-source");
    public static final String SINK_CONTRACT = digest("bootstrap-sink");

    private StreamPublicWorkerBootstrapFixture() { }

    public static void main(String[] arguments) throws Exception {
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        StreamSourceProvider source = "source".equals(role)
                ? new Source() : null;
        StreamSinkProvider sink = "sink".equals(role)
                ? new Sink(exchange) : null;
        StreamWorkerConfig config = StreamWorkerConfig.defaults(
                "public-" + role, exchange.resolve("cluster"));
        try (StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                config,
                source == null ? Collections.emptyList()
                        : Collections.singletonList(source),
                sink == null ? Collections.emptyList()
                        : Collections.singletonList(sink),
                Collections.emptyList(), Collections.emptyList())) {
            Files.write(exchange.resolve("public-" + role + ".port"),
                    Integer.toString(worker.endpoint().port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("public-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class Source implements StreamSourceProvider {
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                        context.restoredPositionFrame()).getInt() : 0;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next++ == 0) {
                        return StreamSourceRecord.of(
                                "public-bootstrap".getBytes(
                                        StandardCharsets.UTF_8),
                                position(1));
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(Math.min(next, 1));
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

    private static final class Sink implements StreamSinkProvider {
        private final Path exchange;
        private Sink(Path exchange) { this.exchange = exchange; }
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> Files.write(exchange.resolve("public-result"),
                    frame);
        }
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
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
