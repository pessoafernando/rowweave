package io.rowweave;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;

/** Bounded streaming reader for one exact immutable object snapshot. */
public final class ObjectStorageSnapshotInputExecutor
        extends SourceExecutor<ObjectStorageSnapshotSource> {

    private static final int STREAM_BUFFER_BYTES = 64 * 1024;

    private ObjectStorageReadHandle handle;
    private InputStream stream;
    private MessageDigest digest;
    private byte[] lineBuffer;
    private long observedBytes;
    private DataRow nextRow;
    private boolean opened;
    private boolean eofVerified;
    private boolean closed;

    public ObjectStorageSnapshotInputExecutor(ObjectStorageSnapshotSource input) {
        super(input, input.getSchema());
    }

    @Override
    public void open() {
        if (opened && !closed) return;
        if (closed) {
            throw failure("OBJECT_STORAGE_EXECUTOR_CLOSED");
        }
        ObjectStorageReadHandle openedHandle;
        try {
            openedHandle = input.provider().open(input.readRequest());
        } catch (RuntimeException providerFailure) {
            throw failure("OBJECT_STORAGE_OPEN_FAILED");
        }
        if (openedHandle == null) {
            throw failure("OBJECT_STORAGE_INVALID_HANDLE");
        }
        this.handle = openedHandle;
        try {
            validateHandle(openedHandle);
            InputStream raw = openedHandle.stream();
            if (raw == null) throw failure("OBJECT_STORAGE_INVALID_HANDLE");
            this.stream = new BufferedInputStream(raw, STREAM_BUFFER_BYTES);
            this.digest = sha256();
            this.lineBuffer = new byte[input.maxRowBytes()];
            this.opened = true;
        } catch (Throwable invalid) {
            Throwable primary = sanitize(invalid, "OBJECT_STORAGE_INVALID_HANDLE");
            closeAfterFailure(primary);
            throwUnchecked(primary);
        }
    }

    private void validateHandle(ObjectStorageReadHandle candidate) {
        if (!input.providerType().equals(candidate.providerType())
                || input.providerVersion() != candidate.providerVersion()) {
            throw failure("OBJECT_STORAGE_PROVIDER_MISMATCH");
        }
        ObjectStorageReadRequest expected = input.readRequest();
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
        int lineLength = 0;
        for (;;) {
            int value = stream.read();
            if (value < 0) {
                DataRow finalRow = lineLength == 0 ? null : decodeRow(lineLength);
                verifyEof();
                nextRow = finalRow;
                return finalRow != null;
            }
            observedBytes++;
            if (observedBytes > input.readRequest().contentLength()) {
                throw failure("OBJECT_STORAGE_LENGTH_MISMATCH");
            }
            digest.update((byte) value);
            if (value == '\n') {
                nextRow = decodeRow(lineLength);
                return true;
            }
            if (value == '\r') {
                throw failure("OBJECT_STORAGE_ROW_ENCODING_INVALID");
            }
            if (lineLength >= lineBuffer.length) {
                throw failure("OBJECT_STORAGE_ROW_LIMIT_EXCEEDED");
            }
            lineBuffer[lineLength++] = (byte) value;
        }
    }

    private DataRow decodeRow(int lineLength) {
        final String line;
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(lineBuffer, 0, lineLength));
            line = decoded.toString();
        } catch (CharacterCodingException invalid) {
            throw failure("OBJECT_STORAGE_ROW_ENCODING_INVALID");
        }
        try {
            return FileSink.decodeRow(dataSchema, line);
        } catch (RuntimeException invalid) {
            throw failure("OBJECT_STORAGE_ROW_FORMAT_INVALID");
        }
    }

    private void verifyEof() {
        if (observedBytes != input.readRequest().contentLength()) {
            throw failure("OBJECT_STORAGE_LENGTH_MISMATCH");
        }
        byte[] expected = decodeDigest(input.readRequest().contentSha256());
        if (!MessageDigest.isEqual(expected, digest.digest())) {
            throw failure("OBJECT_STORAGE_DIGEST_MISMATCH");
        }
        eofVerified = true;
        closeResource(null);
    }

    @Override
    public DataRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException("object storage source is exhausted");
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

    private void closeResource(Throwable primary) {
        ObjectStorageReadHandle current = handle;
        handle = null;
        stream = null;
        lineBuffer = null;
        digest = null;
        closed = true;
        if (current == null) return;
        try {
            current.close();
        } catch (Throwable closeFailure) {
            IllegalStateException sanitized = failure("OBJECT_STORAGE_CLOSE_FAILED");
            if (primary != null) {
                primary.addSuppressed(sanitized);
            } else {
                throw sanitized;
            }
        }
    }

    private static Throwable sanitize(Throwable failure, String fallbackCode) {
        if (failure instanceof Error) return failure;
        if (failure instanceof ObjectStorageFailure) return failure;
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

    private static ObjectStorageFailure failure(String code) {
        return new ObjectStorageFailure(code);
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw new IllegalStateException("OBJECT_STORAGE_READ_FAILED");
    }

    /** Marker keeps already-sanitized public failures from being reclassified. */
    private static final class ObjectStorageFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private ObjectStorageFailure(String code) {
            super(code);
        }
    }
}
