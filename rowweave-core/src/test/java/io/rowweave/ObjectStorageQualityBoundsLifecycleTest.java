package io.rowweave;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded discard/resync and fatal Object lifecycle matrix. */
class ObjectStorageQualityBoundsLifecycleTest {

    private static final DataSchema STRING_META =
            new DataSchema(new Field("value", DataType.StringType));
    private static final String SOURCE_UID = "ledger.object.source";
    private static final int MAX_ROW = ObjectStorageSnapshotSource.MIN_ROW_BYTES;
    private static final long RESYNC_LIMIT = 64L * 1024L * 1024L;

    @BeforeEach
    void resetProviders() {
        ControlledProvider.reset();
        GeneratedProvider.reset();
    }

    @Test
    void exactRowLimitIsValidAndFirstExcessByteRecoversAtLf() {
        byte[] exact = repeat((byte) 'x', MAX_ROW);
        byte[] oversized = repeat((byte) 'y', MAX_ROW + 1);
        byte[] content = concat(exact, bytes("\n"), oversized, bytes("\nok\n"));

        List<DataRow> rows = read(controlledInput(content, content.length, sha256(content)));

        assertEquals(3, rows.size());
        assertEquals(repeatString('x', MAX_ROW), rows.get(0).get(0));
        assertValid(rows.get(0));
        assertRejected(rows.get(1), 1L,
                "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES");
        assertEquals("ok", rows.get(2).get(0));
        assertValid(rows.get(2));
        assertEquals(1, ControlledProvider.closes);
    }

    @Test
    void lfAtExactResyncLimitRecoversWithoutRetainingLargeFixture() {
        long nonLfBytes = MAX_ROW + RESYNC_LIMIT - 1L;
        byte[] suffix = bytes("ok\n");
        ObjectStorageQualitySource input = generatedInput(nonLfBytes, true, suffix);

        List<DataRow> rows = read(input);

        assertEquals(2, rows.size());
        assertRejected(rows.get(0), 0L,
                "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES");
        assertEquals("ok", rows.get(1).get(0));
        assertValid(rows.get(1));
        assertEquals(nonLfBytes + 1L + suffix.length, GeneratedProvider.reads);
        assertEquals(1, GeneratedProvider.closes);
    }

    @Test
    void noLfWithinResyncLimitFailsBeforeReadingBeyondHardCap() {
        long nonLfBytes = MAX_ROW + RESYNC_LIMIT;
        ObjectStorageQualitySource input = generatedInput(nonLfBytes, false, new byte[0]);
        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        executor.open();

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, executor::hasNext);

        assertEquals("OBJECT_STORAGE_RESYNC_LIMIT_EXCEEDED", failure.getMessage());
        assertTrue(GeneratedProvider.reads >= MAX_ROW + RESYNC_LIMIT);
        assertTrue(GeneratedProvider.reads <= MAX_ROW + RESYNC_LIMIT);
        assertEquals(1, GeneratedProvider.closes);
    }

    @Test
    void eofWithinResyncLimitTerminatesOneOversizeRecord() {
        byte[] oversized = repeat((byte) 'z', MAX_ROW + 9);

        List<DataRow> rows = read(controlledInput(
                oversized, oversized.length, sha256(oversized)));

        assertEquals(1, rows.size());
        assertRejected(rows.get(0), 0L,
                "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES");
        assertEquals(1, ControlledProvider.closes);
    }

    @Test
    void invalidOpenAndHandleMetadataFailBeforeAnyByteAndCloseWhenOwned() {
        byte[] content = bytes("ok\n");

        ControlledProvider.openFailure = true;
        assertOpenFailure(controlledInput(content, content.length, sha256(content)),
                "OBJECT_STORAGE_OPEN_FAILED", 0);

        ControlledProvider.reset();
        ControlledProvider.nullHandle = true;
        assertOpenFailure(controlledInput(content, content.length, sha256(content)),
                "OBJECT_STORAGE_INVALID_HANDLE", 0);

        ControlledProvider.reset();
        ControlledProvider.returnedProviderType = "wrong-provider";
        assertOpenFailure(controlledInput(content, content.length, sha256(content)),
                "OBJECT_STORAGE_PROVIDER_MISMATCH", 1);

        ControlledProvider.reset();
        ControlledProvider.returnedVersion = "wrong-version";
        assertOpenFailure(controlledInput(content, content.length, sha256(content)),
                "OBJECT_STORAGE_SNAPSHOT_MISMATCH", 1);

        ControlledProvider.reset();
        ControlledProvider.returnedLength = content.length + 1L;
        assertOpenFailure(controlledInput(content, content.length, sha256(content)),
                "OBJECT_STORAGE_SNAPSHOT_MISMATCH", 1);

        ControlledProvider.reset();
        ControlledProvider.nullStream = true;
        assertOpenFailure(controlledInput(content, content.length, sha256(content)),
                "OBJECT_STORAGE_INVALID_HANDLE", 1);
    }

    @Test
    void lengthDigestReadAndCloseFailuresStayFatalAndRedacted() {
        byte[] content = bytes("ok\n");
        assertReadFailure(controlledInput(
                content, content.length + 1L, sha256(content)),
                "OBJECT_STORAGE_LENGTH_MISMATCH", 1);

        ControlledProvider.reset();
        byte[] longContent = bytes("ok\nextra");
        assertReadFailure(controlledInput(
                longContent, bytes("ok\n").length, sha256(longContent)),
                "OBJECT_STORAGE_LENGTH_MISMATCH", 1);

        ControlledProvider.reset();
        assertReadFailure(controlledInput(
                content, content.length, repeatString('0', 64)),
                "OBJECT_STORAGE_DIGEST_MISMATCH", 1);

        ControlledProvider.reset();
        ControlledProvider.failReadAfter = 1;
        assertReadFailure(controlledInput(
                content, content.length, sha256(content)),
                "OBJECT_STORAGE_READ_FAILED", 1);

        ControlledProvider.reset();
        ControlledProvider.failClose = true;
        assertReadFailure(controlledInput(
                content, content.length, sha256(content)),
                "OBJECT_STORAGE_CLOSE_FAILED", 1);

        ControlledProvider.reset();
        ControlledProvider.failReadAfter = 1;
        ControlledProvider.failClose = true;
        ObjectStorageQualitySource readAndClose = controlledInput(
                content, content.length, sha256(content));
        SourceExecutor<?> executor = SourceExecutorFactory.create(readAndClose);
        executor.open();
        IllegalStateException primary = assertThrows(
                IllegalStateException.class, executor::hasNext);
        assertEquals("OBJECT_STORAGE_READ_FAILED", primary.getMessage());
        assertEquals(1, primary.getSuppressed().length);
        assertEquals("OBJECT_STORAGE_CLOSE_FAILED",
                primary.getSuppressed()[0].getMessage());
        assertFalse(primary.toString().contains("domain/private-ledger"));
    }

    @Test
    void earlyCloseIsIdempotentAndCloseInterruptsBlockedRead() throws Exception {
        byte[] content = bytes("one\ntwo\n");
        SourceExecutor<?> early = SourceExecutorFactory.create(
                controlledInput(content, content.length, sha256(content)));
        early.open();
        assertTrue(early.hasNext());
        early.close();
        early.close();
        assertEquals(1, ControlledProvider.closes);
        IllegalStateException closed = assertThrows(
                IllegalStateException.class, early::hasNext);
        assertEquals("OBJECT_STORAGE_EXECUTOR_CLOSED", closed.getMessage());

        ControlledProvider.reset();
        ControlledProvider.blockRead = true;
        SourceExecutor<?> blocked = SourceExecutorFactory.create(
                controlledInput(content, content.length, sha256(content)));
        blocked.open();
        ExecutorService thread = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> read = thread.submit(blocked::hasNext);
            assertTrue(ControlledProvider.readStarted.await(2, TimeUnit.SECONDS));

            blocked.close();

            ExecutionException stopped = assertThrows(
                    ExecutionException.class, () -> read.get(2, TimeUnit.SECONDS));
            assertEquals("OBJECT_STORAGE_READ_FAILED",
                    stopped.getCause().getMessage());
            assertEquals(1, ControlledProvider.closes);
        } finally {
            blocked.close();
            thread.shutdownNow();
        }
    }

    @Test
    void concurrentCloseCallsReleaseExactlyOneHandle() throws Exception {
        byte[] content = bytes("one\ntwo\n");
        SourceExecutor<?> executor = SourceExecutorFactory.create(
                controlledInput(content, content.length, sha256(content)));
        executor.open();
        ExecutorService threads = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> closes = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                closes.add(threads.submit(() -> {
                    start.await();
                    executor.close();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> close : closes) close.get(2, TimeUnit.SECONDS);
        } finally {
            threads.shutdownNow();
        }

        assertEquals(1, ControlledProvider.closes);
    }

    private static void assertOpenFailure(
            ObjectStorageQualitySource input, String code, int expectedCloses) {
        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, executor::open);
        assertEquals(code, failure.getMessage());
        assertEquals(0, ControlledProvider.reads);
        assertEquals(expectedCloses, ControlledProvider.closes);
        assertFalse(failure.toString().contains("domain/private-ledger"));
    }

    private static void assertReadFailure(
            ObjectStorageQualitySource input, String code, int expectedCloses) {
        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        executor.open();
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> drain(executor));
        assertEquals(code, failure.getMessage());
        assertEquals(expectedCloses, ControlledProvider.closes);
        assertFalse(failure.toString().contains("domain/private-ledger"));
    }

    private static List<DataRow> read(ObjectStorageQualitySource input) {
        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        executor.open();
        try {
            return drain(executor);
        } finally {
            executor.close();
        }
    }

    private static List<DataRow> drain(SourceExecutor<?> executor) {
        List<DataRow> rows = new ArrayList<>();
        while (executor.hasNext()) rows.add(executor.next());
        return rows;
    }

    private static ObjectStorageQualitySource controlledInput(
            byte[] content, long expectedLength, String expectedDigest) {
        ControlledProvider.content = Arrays.copyOf(content, content.length);
        if (ControlledProvider.returnedLength == Long.MIN_VALUE) {
            ControlledProvider.returnedLength = expectedLength;
        }
        return qualityInput(new ControlledProvider(), expectedLength, expectedDigest);
    }

    private static ObjectStorageQualitySource generatedInput(
            long nonLfBytes, boolean lf, byte[] suffix) {
        GeneratedProvider.nonLfBytes = nonLfBytes;
        GeneratedProvider.lf = lf;
        GeneratedProvider.suffix = Arrays.copyOf(suffix, suffix.length);
        long length = nonLfBytes + (lf ? 1L : 0L) + suffix.length;
        return qualityInput(new GeneratedProvider(), length,
                generatedSha256(nonLfBytes, lf, suffix));
    }

    private static ObjectStorageQualitySource qualityInput(
            ObjectStorageSourceProvider provider, long length, String digest) {
        return new ObjectStorageQualitySource(SOURCE_UID, new ObjectStorageSnapshotSource(
                STRING_META, provider, "endpoint-prod", "credential-prod", "domain",
                "domain/private-ledger.tsv", "version-7", length, digest, MAX_ROW));
    }

    private static void assertValid(DataRow row) {
        assertEquals(0, row.get(1));
        assertEquals("", row.get(2));
        assertEquals("", row.get(3));
        assertEquals(0L, row.get(4));
        assertEquals("", row.get(5));
        assertEquals("", row.get(6));
        assertEquals(-1, row.get(7));
        assertEquals("", row.get(8));
    }

    private static void assertRejected(
            DataRow row, long ordinal, String code, String detail) {
        assertEquals(null, row.get(0));
        assertEquals(1, row.get(1));
        assertEquals(SOURCE_UID, row.get(2));
        assertTrue(((String) row.get(3)).matches("[0-9a-f]{64}"));
        assertEquals(ordinal, row.get(4));
        assertEquals(code, row.get(5));
        assertEquals("OBJECT_ROW_DECODE_V1", row.get(6));
        assertEquals(-1, row.get(7));
        assertEquals(detail, row.get(8));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] repeat(byte value, int count) {
        byte[] result = new byte[count];
        Arrays.fill(result, value);
        return result;
    }

    private static String repeatString(char value, int count) {
        char[] result = new char[count];
        Arrays.fill(result, value);
        return new String(result);
    }

    private static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) length = Math.addExact(length, part.length);
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }

    private static String sha256(byte[] content) {
        MessageDigest digest = digest();
        digest.update(content);
        return hex(digest.digest());
    }

    private static String generatedSha256(
            long nonLfBytes, boolean lf, byte[] suffix) {
        MessageDigest digest = digest();
        byte[] block = repeat((byte) 'x', 8192);
        long remaining = nonLfBytes;
        while (remaining > 0L) {
            int count = (int) Math.min(remaining, block.length);
            digest.update(block, 0, count);
            remaining -= count;
        }
        if (lf) digest.update((byte) '\n');
        digest.update(suffix);
        return hex(digest.digest());
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String hex(byte[] digest) {
        StringBuilder result = new StringBuilder(digest.length * 2);
        for (byte value : digest) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    public static final class ControlledProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static byte[] content;
        static String returnedProviderType;
        static int returnedProviderVersion;
        static String returnedVersion;
        static long returnedLength;
        static int failReadAfter;
        static boolean failClose;
        static boolean nullHandle;
        static boolean nullStream;
        static boolean openFailure;
        static boolean blockRead;
        static CountDownLatch readStarted;
        static long reads;
        static int closes;

        public ControlledProvider() { }

        static void reset() {
            content = new byte[0];
            returnedProviderType = "quality-controlled";
            returnedProviderVersion = 1;
            returnedVersion = "version-7";
            returnedLength = Long.MIN_VALUE;
            failReadAfter = -1;
            failClose = false;
            nullHandle = false;
            nullStream = false;
            openFailure = false;
            blockRead = false;
            readStarted = new CountDownLatch(1);
            reads = 0L;
            closes = 0;
        }

        @Override public String providerType() { return "quality-controlled"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            if (openFailure) {
                throw new IllegalStateException("leaked credential-prod");
            }
            if (nullHandle) return null;
            InputStream stream = nullStream ? null : (blockRead
                    ? new BlockingInputStream()
                    : new ControlledInputStream(content, failReadAfter));
            return new TestHandle(
                    returnedProviderType, returnedProviderVersion,
                    returnedVersion, returnedLength, stream, failClose,
                    () -> closes++);
        }
    }

    public static final class GeneratedProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static long nonLfBytes;
        static boolean lf;
        static byte[] suffix;
        static long reads;
        static int closes;

        public GeneratedProvider() { }

        static void reset() {
            nonLfBytes = 0L;
            lf = false;
            suffix = new byte[0];
            reads = 0L;
            closes = 0;
        }

        @Override public String providerType() { return "quality-generated"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            PatternInputStream stream = new PatternInputStream(
                    nonLfBytes, lf, Arrays.copyOf(suffix, suffix.length));
            return new TestHandle(
                    providerType(), providerVersion(), request.versionId(),
                    request.contentLength(), stream, false, () -> closes++);
        }
    }

    private static final class TestHandle implements ObjectStorageReadHandle {
        private final String type;
        private final int version;
        private final String versionId;
        private final long length;
        private final InputStream stream;
        private final boolean failClose;
        private final Runnable onClose;
        private boolean closed;

        private TestHandle(
                String type, int version, String versionId, long length,
                InputStream stream, boolean failClose, Runnable onClose) {
            this.type = type;
            this.version = version;
            this.versionId = versionId;
            this.length = length;
            this.stream = stream;
            this.failClose = failClose;
            this.onClose = onClose;
        }

        @Override public String providerType() { return type; }
        @Override public int providerVersion() { return version; }
        @Override public String versionId() { return versionId; }
        @Override public long contentLength() { return length; }
        @Override public InputStream stream() { return stream; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            onClose.run();
            if (failClose) throw new IllegalStateException("leaked private-ledger");
            try {
                if (stream != null) stream.close();
            } catch (IOException failure) {
                throw new IllegalStateException("leaked private-ledger", failure);
            }
        }
    }

    private static final class ControlledInputStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private final int failAfter;
        private int count;

        private ControlledInputStream(byte[] content, int failAfter) {
            this.delegate = new ByteArrayInputStream(content);
            this.failAfter = failAfter;
        }

        @Override
        public int read() throws IOException {
            if (failAfter >= 0 && count >= failAfter) {
                throw new IOException("leaked domain/private-ledger");
            }
            int value = delegate.read();
            if (value >= 0) {
                count++;
                ControlledProvider.reads++;
            }
            return value;
        }
    }

    private static final class BlockingInputStream extends InputStream {
        private final CountDownLatch closed = new CountDownLatch(1);

        @Override
        public int read() throws IOException {
            ControlledProvider.readStarted.countDown();
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

        @Override public void close() { closed.countDown(); }
    }

    private static final class PatternInputStream extends InputStream {
        private final long nonLfBytes;
        private final boolean lf;
        private final byte[] suffix;
        private long position;

        private PatternInputStream(long nonLfBytes, boolean lf, byte[] suffix) {
            this.nonLfBytes = nonLfBytes;
            this.lf = lf;
            this.suffix = suffix;
        }

        @Override
        public int read() {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            long total = nonLfBytes + (lf ? 1L : 0L) + suffix.length;
            if (position >= total) return -1;
            int count = (int) Math.min((long) length, total - position);
            for (int i = 0; i < count; i++) {
                long current = position + i;
                if (current < nonLfBytes) {
                    target[offset + i] = (byte) 'x';
                } else if (lf && current == nonLfBytes) {
                    target[offset + i] = (byte) '\n';
                } else {
                    int suffixIndex = (int) (current - nonLfBytes - (lf ? 1L : 0L));
                    target[offset + i] = suffix[suffixIndex];
                }
            }
            position += count;
            GeneratedProvider.reads += count;
            return count;
        }
    }
}
