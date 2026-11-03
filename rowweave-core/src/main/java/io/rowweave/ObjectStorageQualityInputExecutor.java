package io.rowweave;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/** one-lookahead exact Object reader for under-limit physical records. */
final class ObjectStorageQualityInputExecutor
        extends SourceExecutor<ObjectStorageQualitySource> {

    private static final int STREAM_BUFFER_BYTES = 64 * 1024;
    private static final long RESYNC_SCAN_LIMIT_BYTES = 64L * 1024L * 1024L;

    private final ObjectStorageSnapshotSource original;
    private final ObjectStorageQualityRecordDecoder decoder;
    private ObjectStorageReadHandle handle;
    private InputStream stream;
    private MessageDigest digest;
    private byte[] recordBuffer;
    private long observedBytes;
    private long nextOrdinal;
    private DataRow nextRow;
    private boolean opened;
    private boolean eofVerified;
    private volatile boolean closed;

    ObjectStorageQualityInputExecutor(ObjectStorageQualitySource input) {
        super(input, input.getSchema());
        this.original = input.originalSource();
        this.decoder = new ObjectStorageQualityRecordDecoder(input);
    }

    @Override
    public void open() {
        if (opened && !closed) return;
        if (closed) throw failure("OBJECT_STORAGE_EXECUTOR_CLOSED");
        ObjectStorageReadHandle openedHandle;
        try {
            openedHandle = original.provider().open(original.readRequest());
        } catch (RuntimeException providerFailure) {
            throw failure("OBJECT_STORAGE_OPEN_FAILED");
        }
        if (openedHandle == null) throw failure("OBJECT_STORAGE_INVALID_HANDLE");
        this.handle = openedHandle;
        try {
            validateHandle(openedHandle);
            InputStream raw = openedHandle.stream();
            if (raw == null) throw failure("OBJECT_STORAGE_INVALID_HANDLE");
            this.stream = new BufferedInputStream(raw, STREAM_BUFFER_BYTES);
            this.digest = sha256();
            this.recordBuffer = new byte[original.maxRowBytes()];
            this.opened = true;
        } catch (Throwable invalid) {
            Throwable primary = sanitize(invalid, "OBJECT_STORAGE_INVALID_HANDLE");
            closeAfterFailure(primary);
            throwUnchecked(primary);
        }
    }

    private void validateHandle(ObjectStorageReadHandle candidate) {
        if (!original.providerType().equals(candidate.providerType())
                || original.providerVersion() != candidate.providerVersion()) {
            throw failure("OBJECT_STORAGE_PROVIDER_MISMATCH");
        }
        ObjectStorageReadRequest expected = original.readRequest();
        if (!expected.versionId().equals(candidate.versionId())
                || expected.contentLength() != candidate.contentLength()) {
            throw failure("OBJECT_STORAGE_SNAPSHOT_MISMATCH");
        }
    }

    @Override
    public boolean hasNext() {
        if (nextRow != null) return true;
        if (eofVerified) return false;
        if (!opened) {
            throw failure(closed
                    ? "OBJECT_STORAGE_EXECUTOR_CLOSED"
                    : "OBJECT_STORAGE_EXECUTOR_NOT_OPEN");
        }
        if (closed) throw failure("OBJECT_STORAGE_EXECUTOR_CLOSED");
        try {
            return prepareNext();
        } catch (Throwable readFailure) {
            Throwable primary = sanitize(readFailure, "OBJECT_STORAGE_READ_FAILED");
            closeAfterFailure(primary);
            throwUnchecked(primary);
            return false;
        }
    }

    private boolean prepareNext() throws IOException {
        int recordLength = 0;
        boolean rowLimitExceeded = false;
        long resyncNonLfBytes = 0L;
        for (;;) {
            int value = stream.read();
            if (value < 0) {
                DataRow finalRow = recordLength == 0 && !rowLimitExceeded
                        ? null : decoder.classify(
                                recordBuffer, recordLength, takeOrdinal(),
                                rowLimitExceeded);
                verifyEof();
                nextRow = finalRow;
                return finalRow != null;
            }
            observedBytes++;
            if (observedBytes > original.readRequest().contentLength()) {
                throw failure("OBJECT_STORAGE_LENGTH_MISMATCH");
            }
            digest.update((byte) value);
            if (value == '\n') {
                nextRow = decoder.classify(
                        recordBuffer, recordLength, takeOrdinal(),
                        rowLimitExceeded);
                return true;
            }
            if (rowLimitExceeded) {
                resyncNonLfBytes++;
                if (resyncNonLfBytes >= RESYNC_SCAN_LIMIT_BYTES) {
                    throw failure("OBJECT_STORAGE_RESYNC_LIMIT_EXCEEDED");
                }
                continue;
            }
            if (recordLength >= recordBuffer.length) {
                Arrays.fill(recordBuffer, 0, recordLength, (byte) 0);
                recordLength = 0;
                rowLimitExceeded = true;
                resyncNonLfBytes = 1L;
                continue;
            }
            recordBuffer[recordLength++] = (byte) value;
        }
    }

    private long takeOrdinal() {
        if (nextOrdinal == Long.MAX_VALUE) {
            throw failure("OBJECT_STORAGE_ORDINAL_OVERFLOW");
        }
        return nextOrdinal++;
    }

    private void verifyEof() {
        if (observedBytes != original.readRequest().contentLength()) {
            throw failure("OBJECT_STORAGE_LENGTH_MISMATCH");
        }
        if (!MessageDigest.isEqual(
                decodeDigest(original.readRequest().contentSha256()), digest.digest())) {
            throw failure("OBJECT_STORAGE_DIGEST_MISMATCH");
        }
        eofVerified = true;
        closeResource(null);
    }

    @Override
    public DataRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Object Storage quality source is exhausted");
        }
        DataRow result = nextRow;
        nextRow = null;
        return result;
    }

    @Override
    public void close() {
        if (closed) return;
        closeResource(null);
    }

    private void closeAfterFailure(Throwable primary) {
        closeResource(primary);
    }

    private synchronized void closeResource(Throwable primary) {
        ObjectStorageReadHandle current = handle;
        handle = null;
        stream = null;
        recordBuffer = null;
        digest = null;
        nextRow = null;
        closed = true;
        if (current == null) return;
        try {
            current.close();
        } catch (Throwable closeFailure) {
            IllegalStateException sanitized = failure("OBJECT_STORAGE_CLOSE_FAILED");
            if (primary != null) primary.addSuppressed(sanitized);
            else throw sanitized;
        }
    }

    private static Throwable sanitize(Throwable failure, String fallbackCode) {
        if (failure instanceof Error) return failure;
        if (failure instanceof ObjectStorageQualityFailure) return failure;
        return failure(fallbackCode);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static byte[] decodeDigest(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int high = Character.digit(value.charAt(i * 2), 16);
            int low = Character.digit(value.charAt(i * 2 + 1), 16);
            bytes[i] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static ObjectStorageQualityFailure failure(String code) {
        return new ObjectStorageQualityFailure(code);
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw failure("OBJECT_STORAGE_READ_FAILED");
    }

    private static final class ObjectStorageQualityFailure
            extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private ObjectStorageQualityFailure(String code) {
            super(code);
        }
    }
}
