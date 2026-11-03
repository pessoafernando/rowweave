package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.LocalStreamSinkTask;
import io.rowweave.stream.runtime.LocalStreamSourceTask;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/** Isolated Worker JVM fixture for the 6 M+2W socket gate. */
public final class StreamWorkerProcessFixture {
    private static final int RECORDS = 1_000;
    private static final String SOURCE_DIGEST = repeat('a');
    private static final String SINK_DIGEST = repeat('b');
    private static final String SPLIT_ID = repeat('c');

    private StreamWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("role and exchange directory required");
        }
        Path exchange = Paths.get(arguments[1]);
        Files.write(exchange.resolve(arguments[0] + ".identity"),
                ManagementFactory.getRuntimeMXBean().getName()
                        .getBytes(StandardCharsets.UTF_8));
        if ("sink".equals(arguments[0])) {
            runSink(exchange);
        } else if ("source".equals(arguments[0])) {
            runSource(exchange);
        } else {
            throw new IllegalArgumentException("unknown Stream worker role");
        }
    }

    private static void runSink(Path exchange) throws Exception {
        StreamChannelIdentity identity = identity();
        try (StreamSocketChannel.Listener listener =
                     StreamSocketChannel.bindLoopback(
                             identity, 8, 1_024, 4_096L)) {
            Files.write(exchange.resolve("port"),
                    Integer.toString(listener.localPort())
                            .getBytes(StandardCharsets.US_ASCII));
            StreamSocketChannel.Receiver receiver = listener.accept();
            DigestSinkProvider provider = new DigestSinkProvider();
            LocalStreamSinkTask task = new LocalStreamSinkTask(
                    "worker-b-sink", "sink", sinkBinding(), provider,
                    receiver, ignored -> { });
            task.start();
            provider.writer.complete.await();
            Files.write(exchange.resolve("result"),
                    provider.writer.result().getBytes(StandardCharsets.US_ASCII));
            awaitCancel(exchange);
            task.cancel();
            task.awaitCompletion(2_000L);
            receiver.close();
            if (task.failure() != null) throw new AssertionError(task.failure());
        }
    }

    private static void runSource(Path exchange) throws Exception {
        int port = Integer.parseInt(new String(
                Files.readAllBytes(exchange.resolve("port")),
                StandardCharsets.US_ASCII));
        StreamSocketChannel.Sender sender = StreamSocketChannel.connectLoopback(
                port, identity(), 8, 1_024, 4_096L);
        CountingSourceProvider provider = new CountingSourceProvider();
        StreamSourceBinding binding = sourceBinding();
        LocalStreamSourceTask task = new LocalStreamSourceTask(
                "worker-a-source", "source", binding,
                binding.fixedSplits().get(0), provider, sender);
        task.start();
        provider.reader.emitted.await();
        awaitCancel(exchange);
        task.cancel();
        task.awaitCompletion(2_000L);
        sender.close();
        if (task.failure() != null) throw new AssertionError(task.failure());
    }

    private static void awaitCancel(Path exchange) throws Exception {
        while (!Files.exists(exchange.resolve("cancel"))) {
            Thread.sleep(5L);
        }
    }

    private static StreamChannelIdentity identity() {
        return new StreamChannelIdentity(
                "run-process", "attempt-1", 7L, 3L,
                "source-sink-0", 1L);
    }

    private static StreamSourceBinding sourceBinding() {
        byte[] configuration = new byte[] {1};
        return new StreamSourceBinding(
                "process-source", SOURCE_DIGEST, sha256Hex(configuration),
                configuration, Collections.singletonList(
                        new StreamSourceSplit(
                                SPLIT_ID, "process-position-v1",
                                new byte[] {2})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static StreamSinkBinding sinkBinding() {
        byte[] configuration = new byte[] {3};
        return new StreamSinkBinding(
                "process-sink", SINK_DIGEST, sha256Hex(configuration),
                configuration,
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE);
    }

    private static final class CountingSourceProvider
            implements DurableStreamSourceProvider {
        private final CountingSourceReader reader = new CountingSourceReader();
        @Override public String providerKey() { return "process-source"; }
        @Override public String contractDigest() { return SOURCE_DIGEST; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return reader;
        }
    }

    private static final class CountingSourceReader
            implements StreamSourceReader {
        private final CountDownLatch emitted = new CountDownLatch(1);
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private int next;

        @Override public StreamSourceRecord next() throws Exception {
            if (next < RECORDS) {
                int value = next++;
                if (next == RECORDS) emitted.countDown();
                return StreamSourceRecord.of(
                        new byte[] {(byte) value},
                        new byte[] {(byte) value});
            }
            cancelled.await();
            return null;
        }
        @Override public void cancel() { cancelled.countDown(); }
        @Override public void close() { cancelled.countDown(); }
    }

    private static final class DigestSinkProvider
            implements EagerStreamSinkProvider {
        private final DigestSinkWriter writer = new DigestSinkWriter();
        @Override public String providerKey() { return "process-sink"; }
        @Override public String contractDigest() { return SINK_DIGEST; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return writer;
        }
    }

    private static final class DigestSinkWriter
            implements EagerStreamSinkWriter {
        private final MessageDigest digest;
        private final CountDownLatch complete = new CountDownLatch(1);
        private int count;

        private DigestSinkWriter() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (Exception impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void write(byte[] recordFrame) {
            digest.update(recordFrame);
            count++;
            if (count == RECORDS) complete.countDown();
        }
        @Override public void cancel() { }
        @Override public void close() { }

        private String result() {
            return count + ":" + hex(digest.digest());
        }
    }

    private static String sha256Hex(byte[] value) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
