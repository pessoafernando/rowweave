package io.rowweave;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded object stream validation and row lifecycle. */
class ObjectStorageSnapshotInputExecutorTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));

    @AfterEach
    void resetProvider() {
        DeterministicProvider.reset();
    }

    @Test
    void streamsRowsAndValidatesExactSnapshotAtEof() {
        byte[] content = bytes("1\t12.3400\n2\t\\N\n");
        ObjectStorageSnapshotSource input = input(content, sha256(content),
                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES);
        ObjectStorageSnapshotInputExecutor executor =
                (ObjectStorageSnapshotInputExecutor) SourceExecutorFactory.create(input);

        assertEquals(0, DeterministicProvider.openCount);
        assertThrows(IllegalStateException.class, executor::hasNext);
        executor.open();
        executor.open();
        assertEquals(1, DeterministicProvider.openCount);

        assertTrue(executor.hasNext());
        DataRow first = executor.next();
        assertEquals(1L, first.get(0));
        assertEquals(new BigDecimal("12.3400"), first.get(1));
        assertEquals(4, ((BigDecimal) first.get(1)).scale());

        assertTrue(executor.hasNext());
        DataRow second = executor.next();
        assertNotSame(first, second);
        assertEquals(2L, second.get(0));
        assertEquals(null, second.get(1));
        assertFalse(executor.hasNext(), "EOF must validate length and digest");
        assertEquals(1, DeterministicProvider.closeCount);
        executor.close();
        assertEquals(1, DeterministicProvider.closeCount);
        assertThrows(NoSuchElementException.class, executor::next);
    }

    @Test
    void emptyObjectAndFinalLineWithoutLfAreCanonicalInputs() {
        byte[] empty = new byte[0];
        ObjectStorageSnapshotInputExecutor emptyExecutor = executor(
                input(empty, sha256(empty), ObjectStorageSnapshotSource.MIN_ROW_BYTES));
        emptyExecutor.open();
        assertFalse(emptyExecutor.hasNext());
        assertEquals(1, DeterministicProvider.closeCount);

        DeterministicProvider.reset();
        byte[] noLf = bytes("9\t1.5000000000");
        ObjectStorageSnapshotInputExecutor noLfExecutor = executor(
                input(noLf, sha256(noLf), ObjectStorageSnapshotSource.MIN_ROW_BYTES));
        noLfExecutor.open();
        assertTrue(noLfExecutor.hasNext());
        assertEquals(new BigDecimal("1.5000000000"), noLfExecutor.next().get(1));
        assertFalse(noLfExecutor.hasNext());
        assertEquals(1, DeterministicProvider.closeCount);
    }

    @Test
    void handleMetadataMustMatchBeforeAnyByteIsRead() {
        byte[] content = bytes("1\t1.0\n");
        ObjectStorageSnapshotSource input = input(content, sha256(content),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);

        DeterministicProvider.returnedVersion = "other-version";
        assertOpenRejectedAndClosed(input, "OBJECT_STORAGE_SNAPSHOT_MISMATCH");

        DeterministicProvider.reset();
        DeterministicProvider.returnedLength = content.length + 1L;
        assertOpenRejectedAndClosed(input, "OBJECT_STORAGE_SNAPSHOT_MISMATCH");

        DeterministicProvider.reset();
        DeterministicProvider.returnedProviderType = "other-provider";
        assertOpenRejectedAndClosed(input, "OBJECT_STORAGE_PROVIDER_MISMATCH");

        DeterministicProvider.reset();
        DeterministicProvider.nullStream = true;
        assertOpenRejectedAndClosed(input, "OBJECT_STORAGE_INVALID_HANDLE");

        DeterministicProvider.reset();
        DeterministicProvider.nullHandle = true;
        ObjectStorageSnapshotInputExecutor executor = executor(input);
        IllegalStateException nullHandle = assertThrows(
                IllegalStateException.class, executor::open);
        assertEquals("OBJECT_STORAGE_INVALID_HANDLE", nullHandle.getMessage());
        assertEquals(0, DeterministicProvider.closeCount);
    }

    @Test
    void shortLongDigestUtf8AndRowLimitFailuresCloseTheHandle() {
        byte[] shortBytes = bytes("1\t1.0\n");
        assertReadRejectedAndClosed(
                inputWithExpectedLength(shortBytes, shortBytes.length + 1L,
                        sha256(shortBytes), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_LENGTH_MISMATCH");

        DeterministicProvider.reset();
        byte[] longBytes = bytes("1\t1.0\nextra");
        assertReadRejectedAndClosed(
                inputWithExpectedLength(longBytes, bytes("1\t1.0\n").length,
                        sha256(longBytes), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_LENGTH_MISMATCH");

        DeterministicProvider.reset();
        byte[] digestBytes = bytes("3\t3.0\n");
        assertReadRejectedAndClosed(
                input(digestBytes, repeat('0', 64), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_DIGEST_MISMATCH");

        DeterministicProvider.reset();
        byte[] invalidUtf8 = new byte[]{(byte) 0xc3, (byte) 0x28, (byte) '\n'};
        assertReadRejectedAndClosed(
                input(invalidUtf8, sha256(invalidUtf8), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_ROW_ENCODING_INVALID");

        DeterministicProvider.reset();
        byte[] rawCr = bytes("1\t1.0\r\n");
        assertReadRejectedAndClosed(
                input(rawCr, sha256(rawCr), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_ROW_ENCODING_INVALID");

        DeterministicProvider.reset();
        byte[] invalidRow = bytes("not-a-long\t1.0\n");
        assertReadRejectedAndClosed(
                input(invalidRow, sha256(invalidRow), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_ROW_FORMAT_INVALID");

        DeterministicProvider.reset();
        byte[] oversized = new byte[ObjectStorageSnapshotSource.MIN_ROW_BYTES + 2];
        Arrays.fill(oversized, (byte) '1');
        oversized[oversized.length - 1] = (byte) '\n';
        assertReadRejectedAndClosed(
                input(oversized, sha256(oversized), ObjectStorageSnapshotSource.MIN_ROW_BYTES),
                "OBJECT_STORAGE_ROW_LIMIT_EXCEEDED");
    }

    @Test
    void earlyCloseAndReadFailureReleaseExactlyOneHandle() {
        byte[] content = bytes("1\t1.0\n2\t2.0\n");
        ObjectStorageSnapshotInputExecutor early = executor(
                input(content, sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES));
        early.open();
        assertTrue(early.hasNext());
        early.close();
        early.close();
        assertEquals(1, DeterministicProvider.closeCount);

        DeterministicProvider.reset();
        DeterministicProvider.failReadAfter = 2;
        ObjectStorageSnapshotInputExecutor failing = executor(
                input(content, sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES));
        failing.open();
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, failing::hasNext);
        assertEquals("OBJECT_STORAGE_READ_FAILED", failure.getMessage());
        assertEquals(1, DeterministicProvider.closeCount);

        DeterministicProvider.reset();
        DeterministicProvider.failReadAfter = 2;
        DeterministicProvider.failClose = true;
        ObjectStorageSnapshotInputExecutor closeFailing = executor(
                input(content, sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES));
        closeFailing.open();
        IllegalStateException primary = assertThrows(
                IllegalStateException.class, closeFailing::hasNext);
        assertEquals("OBJECT_STORAGE_READ_FAILED", primary.getMessage());
        assertEquals(1, primary.getSuppressed().length);
        assertEquals("OBJECT_STORAGE_CLOSE_FAILED",
                primary.getSuppressed()[0].getMessage());
    }

    @Test
    void providerCloseInterruptsBlockedReadWithoutLeakingHandle() throws Exception {
        byte[] content = bytes("1\t1.0\n");
        DeterministicProvider.blockRead = true;
        ObjectStorageSnapshotInputExecutor executor = executor(
                input(content, sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES));
        executor.open();
        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> blocked = reader.submit(executor::hasNext);
            assertTrue(DeterministicProvider.readStarted.await(2, TimeUnit.SECONDS));

            executor.close();

            java.util.concurrent.ExecutionException stopped = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> blocked.get(2, TimeUnit.SECONDS));
            assertEquals("OBJECT_STORAGE_READ_FAILED",
                    stopped.getCause().getMessage());
            assertEquals(1, DeterministicProvider.closeCount);
            executor.close();
            assertEquals(1, DeterministicProvider.closeCount);
        } finally {
            executor.close();
            reader.shutdownNow();
        }
    }

    private static void assertOpenRejectedAndClosed(
            ObjectStorageSnapshotSource input, String code) {
        ObjectStorageSnapshotInputExecutor executor = executor(input);
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, executor::open);
        assertEquals(code, failure.getMessage());
        assertEquals(0, DeterministicProvider.readCount);
        assertEquals(1, DeterministicProvider.closeCount);
    }

    private static void assertReadRejectedAndClosed(
            ObjectStorageSnapshotSource input, String code) {
        ObjectStorageSnapshotInputExecutor executor = executor(input);
        executor.open();
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> {
                    while (executor.hasNext()) executor.next();
                });
        assertEquals(code, failure.getMessage());
        assertEquals(1, DeterministicProvider.closeCount);
    }

    private static ObjectStorageSnapshotInputExecutor executor(
            ObjectStorageSnapshotSource input) {
        return (ObjectStorageSnapshotInputExecutor) SourceExecutorFactory.create(input);
    }

    private static ObjectStorageSnapshotSource input(
            byte[] streamBytes, String expectedSha, int maxRowBytes) {
        return inputWithExpectedLength(
                streamBytes, streamBytes.length, expectedSha, maxRowBytes);
    }

    private static ObjectStorageSnapshotSource inputWithExpectedLength(
            byte[] streamBytes, long expectedLength, String expectedSha,
            int maxRowBytes) {
        DeterministicProvider.streamBytes = streamBytes.clone();
        if (DeterministicProvider.returnedLength == Long.MIN_VALUE) {
            DeterministicProvider.returnedLength = expectedLength;
        }
        return new ObjectStorageSnapshotSource(
                META, new DeterministicProvider(), "endpoint-prod", "credential-prod",
                "domain", "domain/ledger.tsv", "version-7", expectedLength,
                expectedSha, maxRowBytes);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            char[] output = new char[digest.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                output[i * 2] = digits[(digest[i] >>> 4) & 0xf];
                output[i * 2 + 1] = digits[digest[i] & 0xf];
            }
            return new String(output);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    public static final class DeterministicProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static byte[] streamBytes = new byte[0];
        static String returnedProviderType = "deterministic";
        static int returnedProviderVersion = 1;
        static String returnedVersion = "version-7";
        static long returnedLength = Long.MIN_VALUE;
        static int failReadAfter = -1;
        static boolean failClose;
        static boolean nullHandle;
        static boolean nullStream;
        static boolean blockRead;
        static CountDownLatch readStarted = new CountDownLatch(1);
        static int openCount;
        static int readCount;
        static int closeCount;

        public DeterministicProvider() { }

        static void reset() {
            streamBytes = new byte[0];
            returnedProviderType = "deterministic";
            returnedProviderVersion = 1;
            returnedVersion = "version-7";
            returnedLength = Long.MIN_VALUE;
            failReadAfter = -1;
            failClose = false;
            nullHandle = false;
            nullStream = false;
            blockRead = false;
            readStarted = new CountDownLatch(1);
            openCount = 0;
            readCount = 0;
            closeCount = 0;
        }

        @Override public String providerType() { return "deterministic"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            openCount++;
            if (nullHandle) return null;
            long length = returnedLength == Long.MIN_VALUE
                    ? request.contentLength() : returnedLength;
            return new TestHandle(returnedProviderType, returnedProviderVersion,
                    returnedVersion, length, streamBytes, failReadAfter,
                    nullStream, failClose, blockRead);
        }
    }

    private static final class TestHandle implements ObjectStorageReadHandle {
        private final String providerType;
        private final int providerVersion;
        private final String version;
        private final long length;
        private final InputStream stream;
        private final boolean failClose;
        private boolean closed;

        private TestHandle(
                String providerType, int providerVersion, String version,
                long length, byte[] content, int failReadAfter,
                boolean nullStream, boolean failClose, boolean blockRead) {
            this.providerType = providerType;
            this.providerVersion = providerVersion;
            this.version = version;
            this.length = length;
            this.stream = nullStream ? null : (blockRead
                    ? new CloseInterruptingInputStream()
                    : new FailingInputStream(content, failReadAfter));
            this.failClose = failClose;
        }

        @Override public String providerType() { return providerType; }
        @Override public int providerVersion() { return providerVersion; }
        @Override public String versionId() { return version; }
        @Override public long contentLength() { return length; }
        @Override public InputStream stream() { return stream; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            DeterministicProvider.closeCount++;
            if (failClose) {
                throw new IllegalStateException("provider leaked domain/ledger.tsv");
            }
            try {
                if (stream != null) stream.close();
            } catch (IOException failure) {
                throw new IllegalStateException(failure);
            }
        }
    }

    private static final class FailingInputStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private final int failAfter;
        private int read;

        private FailingInputStream(byte[] content, int failAfter) {
            this.delegate = new ByteArrayInputStream(content);
            this.failAfter = failAfter;
        }

        @Override
        public int read() throws IOException {
            if (failAfter >= 0 && read >= failAfter) {
                throw new IOException("provider leaked domain/ledger.tsv");
            }
            int value = delegate.read();
            if (value >= 0) {
                read++;
                DeterministicProvider.readCount++;
            }
            return value;
        }
    }

    private static final class CloseInterruptingInputStream extends InputStream {
        private final CountDownLatch closed = new CountDownLatch(1);

        @Override
        public int read() throws IOException {
            DeterministicProvider.readStarted.countDown();
            try {
                if (!closed.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("blocked read was not closed");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("blocked read interrupted", interrupted);
            }
            throw new IOException("closed");
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }
}
