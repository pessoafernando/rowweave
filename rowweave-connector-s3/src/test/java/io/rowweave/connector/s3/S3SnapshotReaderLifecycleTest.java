package io.rowweave.connector.s3;

import io.rowweave.DataType;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.spi.StreamSourceEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SnapshotReaderLifecycleTest {
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.LongType, false)));

    @Test
    void wakeUpInterruptsDownloadAndNextCallRestartsExactObject(
            @TempDir Path temp) throws Exception {
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(content);
        RestartingClient client = new RestartingClient(descriptor, content);
        S3SnapshotReader reader = new S3SnapshotReader(
                configuration(), descriptor, client, null, temp);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StreamSourceEvent> blocked = executor.submit(reader::nextEvent);
            assertTrue(client.firstRead.await(5, TimeUnit.SECONDS));
            reader.wakeUp();
            assertNull(blocked.get(5, TimeUnit.SECONDS));

            assertEquals(StreamSourceEvent.Kind.RECORD,
                    reader.nextEvent().kind());
            assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                    reader.nextEvent().kind());
            assertEquals(2, client.opens.get());
            assertTrue(isEmpty(temp));
        } finally {
            reader.close();
            executor.shutdownNow();
        }
    }

    @Test
    void cancelUnblocksDownloadAndCleansEveryResource(@TempDir Path temp)
            throws Exception {
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(content);
        RestartingClient client = new RestartingClient(descriptor, content);
        S3SnapshotReader reader = new S3SnapshotReader(
                configuration(), descriptor, client, null, temp);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StreamSourceEvent> blocked = executor.submit(reader::nextEvent);
            assertTrue(client.firstRead.await(5, TimeUnit.SECONDS));
            reader.cancel();
            assertNull(blocked.get(5, TimeUnit.SECONDS));
            assertNull(reader.nextEvent());
            assertTrue(isEmpty(temp));
            assertEquals(1, client.closes.get());
            reader.close();
            assertEquals(1, client.closes.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void midStreamDisconnectRestartsTheWholeFrozenObjectWithinAttemptBound(
            @TempDir Path temp) throws Exception {
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(content);
        DisconnectingClient client = new DisconnectingClient(
                descriptor, content, 1, "connection reset");
        S3SnapshotReader reader = new S3SnapshotReader(
                configuration(2), descriptor, client, null, temp);
        try {
            assertEquals(StreamSourceEvent.Kind.RECORD,
                    reader.nextEvent().kind());
            assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                    reader.nextEvent().kind());
            assertEquals(2, client.opens.get());
            assertEquals(2, client.streamCloses.get());
            assertEquals(1, client.closes.get());
            assertTrue(isEmpty(temp));
        } finally {
            reader.close();
        }
    }

    @Test
    void disconnectExhaustionIsBoundedSanitizedAndCleansEveryAttempt(
            @TempDir Path temp) throws Exception {
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(content);
        DisconnectingClient client = new DisconnectingClient(
                descriptor, content, Integer.MAX_VALUE,
                "connection reset at hidden/object");
        S3SnapshotReader reader = new S3SnapshotReader(
                configuration(2), descriptor, client, null, temp);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, reader::nextEvent);
        assertEquals("S3_IO_FAILURE", failure.getMessage());
        assertFalse(failure.getMessage().contains("hidden/object"));
        assertEquals(2, client.opens.get());
        assertEquals(2, client.streamCloses.get());
        assertEquals(1, client.closes.get());
        assertTrue(isEmpty(temp));
        assertNull(reader.nextEvent());
    }

    @Test
    void frozenMetadataMismatchFailsWithoutRetry(@TempDir Path temp)
            throws Exception {
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(content);
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        S3ObjectClient client = new S3ObjectClient() {
            @Override public S3ObjectStream open(
                    S3ObjectDescriptor ignored) {
                opens.incrementAndGet();
                return new S3ObjectStream(
                        new ByteArrayInputStream(content),
                        descriptor.versionId(), "changed-etag",
                        content.length);
            }
            @Override public void close() { closes.incrementAndGet(); }
        };
        S3SnapshotReader reader = new S3SnapshotReader(
                configuration(3), descriptor, client, null, temp);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, reader::nextEvent);
        assertEquals("S3_OBJECT_CHANGED", failure.getMessage());
        assertEquals(1, opens.get());
        assertEquals(1, closes.get());
        assertTrue(isEmpty(temp));
    }

    private static S3ConnectorConfiguration configuration() {
        return configuration(3);
    }

    private static S3ConnectorConfiguration configuration(
            int maximumAttempts) {
        return S3ConnectorConfiguration.builder()
                .endpoint(java.net.URI.create("https://objects.invalid"))
                .region("us-east-1").bucket("ledger-snapshots")
                .format(S3ObjectFormat.NDJSON_UTF8)
                .credentialsReference("vault/s3")
                .maximumAttempts(maximumAttempts)
                .maximumObjectBytes(1024 * 1024)
                .temporaryDiskBudgetBytes(1024 * 1024).build();
    }

    private static S3ObjectDescriptor descriptor(byte[] content) {
        return S3ObjectDescriptor.versioned(
                "ledger-snapshots", "hidden/object", "version-1", "etag-1",
                content.length, sha256(content),
                S3ObjectFormat.NDJSON_UTF8, SCHEMA);
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder value = new StringBuilder(64);
            for (byte item : digest) value.append(String.format("%02x", item));
            return value.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static boolean isEmpty(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            return !files.findAny().isPresent();
        }
    }

    private static final class RestartingClient implements S3ObjectClient {
        private final S3ObjectDescriptor descriptor;
        private final byte[] content;
        private final CountDownLatch firstRead = new CountDownLatch(1);
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        private RestartingClient(S3ObjectDescriptor descriptor,
                byte[] content) {
            this.descriptor = descriptor;
            this.content = content;
        }

        @Override public S3ObjectStream open(S3ObjectDescriptor ignored) {
            if (opens.incrementAndGet() == 1) {
                BlockingInput input = new BlockingInput(firstRead);
                return new S3ObjectStream(input, descriptor.versionId(),
                        descriptor.etag(), descriptor.contentLength());
            }
            return new S3ObjectStream(new ByteArrayInputStream(content),
                    descriptor.versionId(), descriptor.etag(), content.length);
        }

        @Override public void close() { closes.incrementAndGet(); }
    }

    private static final class BlockingInput extends InputStream {
        private final CountDownLatch entered;
        private boolean closed;

        private BlockingInput(CountDownLatch entered) { this.entered = entered; }

        @Override public synchronized int read() throws IOException {
            entered.countDown();
            while (!closed) {
                try {
                    wait();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted");
                }
            }
            throw new IOException("closed");
        }

        @Override public synchronized int read(byte[] target,
                int offset, int length) throws IOException {
            return read();
        }

        @Override public synchronized void close() {
            closed = true;
            notifyAll();
        }
    }

    private static final class DisconnectingClient
            implements S3ObjectClient {
        private final S3ObjectDescriptor descriptor;
        private final byte[] content;
        private final int failures;
        private final String failureMessage;
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger streamCloses = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        private DisconnectingClient(S3ObjectDescriptor descriptor,
                byte[] content, int failures, String failureMessage) {
            this.descriptor = descriptor;
            this.content = content;
            this.failures = failures;
            this.failureMessage = failureMessage;
        }

        @Override public S3ObjectStream open(
                S3ObjectDescriptor ignored) {
            int attempt = opens.incrementAndGet();
            InputStream input = attempt <= failures
                    ? new DisconnectingInput(content, failureMessage,
                    streamCloses)
                    : new ClosingInput(content, streamCloses);
            return new S3ObjectStream(input, descriptor.versionId(),
                    descriptor.etag(), content.length);
        }

        @Override public void close() { closes.incrementAndGet(); }
    }

    private static final class DisconnectingInput extends InputStream {
        private final byte[] content;
        private final String message;
        private final AtomicInteger closes;
        private int offset;
        private boolean deliveredPrefix;

        private DisconnectingInput(byte[] content, String message,
                AtomicInteger closes) {
            this.content = content;
            this.message = message;
            this.closes = closes;
        }

        @Override public int read() throws IOException {
            byte[] one = new byte[1];
            int count = read(one, 0, 1);
            return count < 0 ? -1 : one[0] & 0xff;
        }

        @Override public int read(byte[] target, int targetOffset,
                int length) throws IOException {
            if (deliveredPrefix) throw new IOException(message);
            int count = Math.min(Math.max(1, content.length / 2), length);
            System.arraycopy(content, offset, target, targetOffset, count);
            offset += count;
            deliveredPrefix = true;
            return count;
        }

        @Override public void close() { closes.incrementAndGet(); }
    }

    private static final class ClosingInput extends ByteArrayInputStream {
        private final AtomicInteger closes;
        private ClosingInput(byte[] content, AtomicInteger closes) {
            super(content);
            this.closes = closes;
        }
        @Override public void close() {
            closes.incrementAndGet();
        }
    }
}
