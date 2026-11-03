package io.rowweave.connector.s3;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;

/** One-split reader that verifies the complete object before emitting rows. */
final class S3SnapshotReader implements StreamSourceReader {
    private static final int BUFFER_BYTES = 64 * 1024;

    private final S3ConnectorConfiguration configuration;
    private final S3ObjectDescriptor descriptor;
    private final S3ObjectClient client;
    private final Path temporaryDirectory;
    private final StreamDataRowCodec rowCodec;
    private final AtomicBoolean clientClosed = new AtomicBoolean();

    private volatile S3ObjectStream activeObject;
    private volatile S3RecordDecoder decoder;
    private volatile Path temporaryFile;
    private volatile boolean wakeRequested;
    private volatile boolean cancelled;
    private volatile boolean closed;
    private long completedRecords;
    private boolean endEmitted;

    S3SnapshotReader(S3ConnectorConfiguration configuration,
            S3ObjectDescriptor descriptor, S3ObjectClient client,
            S3SourcePosition restored, Path temporaryDirectory) {
        if (configuration == null || descriptor == null || client == null
                || temporaryDirectory == null
                || !Files.isDirectory(temporaryDirectory)
                || !configuration.bucket().equals(descriptor.bucket())
                || configuration.format() != descriptor.format()
                || descriptor.contentLength()
                > configuration.maximumObjectBytes()
                || descriptor.contentLength()
                > configuration.temporaryDiskBudgetBytes()
                || (restored != null
                && !descriptor.splitId().equals(restored.splitId()))) {
            throw new IllegalArgumentException("S3_READER_INVALID");
        }
        this.configuration = configuration;
        this.descriptor = descriptor;
        this.client = client;
        this.temporaryDirectory = temporaryDirectory;
        this.rowCodec = new StreamDataRowCodec(descriptor.schema());
        this.completedRecords = restored == null
                ? 0L : restored.completedRecords();
    }

    @Override public StreamSourceRecord next() throws Exception {
        StreamSourceEvent event = nextEvent();
        return event == null || event.kind() != StreamSourceEvent.Kind.RECORD
                ? null : event.record();
    }

    @Override public StreamSourceEvent nextEvent() throws Exception {
        if (cancelled || closed || endEmitted) return null;
        try {
            if (decoder == null && !prepare()) return null;
            if (cancelled || closed) return null;
            DataRow row = decoder.next();
            if (row == null) {
                finishVerifiedObject();
                endEmitted = true;
                return StreamSourceEvent.endOfInput();
            }
            long nextPosition = Math.addExact(completedRecords, 1L);
            byte[] rowFrame = rowCodec.encode(row);
            completedRecords = nextPosition;
            return StreamSourceEvent.record(StreamSourceRecord.of(
                    rowFrame, checkpointPositionFrame()));
        } catch (Throwable failure) {
            closeAfterFailure(failure);
            if (cancelled) return null;
            throw failure;
        }
    }

    @Override public byte[] checkpointPositionFrame() {
        return S3SourcePositionCodec.encode(new S3SourcePosition(
                descriptor.splitId(), completedRecords));
    }

    private boolean prepare() throws Exception {
        wakeRequested = false;
        boolean downloaded = false;
        for (int attempt = 1;
                attempt <= configuration.maximumAttempts(); attempt++) {
            try {
                downloadAttempt();
                downloaded = true;
                break;
            } catch (InterruptedDownload interrupted) {
                wakeRequested = false;
                return false;
            } catch (StableDownloadFailure stable) {
                throw stable;
            } catch (Exception failure) {
                if (wakeRequested || cancelled || closed) {
                    wakeRequested = false;
                    return false;
                }
                if (isStableResponseFailure(failure)) throw failure;
                if (attempt == configuration.maximumAttempts()) {
                    throw ioFailure();
                }
            }
        }
        if (!downloaded) return false;
        try {
            decoder = openDecoder(temporaryFile);
            for (long skipped = 0; skipped < completedRecords; skipped++) {
                if (decoder.next() == null) {
                    throw new IllegalArgumentException(
                            "S3_POSITION_INVALID");
                }
            }
            return true;
        } catch (IllegalArgumentException | IllegalStateException stable) {
            throw stable;
        } catch (Exception failure) {
            throw ioFailure();
        }
    }

    private void downloadAttempt() throws Exception {
        Path target = Files.createTempFile(
                temporaryDirectory, "rowweave-s3-", ".snapshot");
        temporaryFile = target;
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long count = 0L;
        S3ObjectStream object = null;
        try {
            object = client.open(descriptor);
            if (object == null) {
                throw new StableDownloadFailure(
                        "S3_OBJECT_RESPONSE_INVALID");
            }
            activeObject = object;
            requireExactMetadata(object);
            if (wakeRequested || cancelled || closed) {
                throw new InterruptedDownload();
            }
            byte[] buffer = new byte[BUFFER_BYTES];
            InputStream input = new BufferedInputStream(object.input(),
                    BUFFER_BYTES);
            try (OutputStream output = new BufferedOutputStream(
                         Files.newOutputStream(target,
                                 StandardOpenOption.WRITE), BUFFER_BYTES)) {
                while (true) {
                    int read = input.read(buffer);
                    if (read < 0) break;
                    if (read == 0) continue;
                    count = Math.addExact(count, read);
                    if (count > descriptor.contentLength()
                            || count > configuration.maximumObjectBytes()
                            || count > configuration
                            .temporaryDiskBudgetBytes()) {
                        throw changed();
                    }
                    digest.update(buffer, 0, read);
                    output.write(buffer, 0, read);
                    if (wakeRequested || cancelled || closed) {
                        throw new InterruptedDownload();
                    }
                }
            }
            try {
                closeObject(object);
            } catch (Exception closeFailure) {
                throw new StableDownloadFailure(
                        "S3_RESOURCE_FAILURE");
            }
            object = null;
            activeObject = null;
            if (count != descriptor.contentLength()
                    || !MessageDigest.isEqual(digest.digest(),
                    S3CodecSupport.parseSha256(
                            descriptor.contentSha256(),
                            "S3_OBJECT_CHANGED"))) {
                throw changed();
            }
        } catch (Throwable failure) {
            boolean cleanupFailed = false;
            try {
                closeObject(object);
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
                cleanupFailed = true;
            }
            activeObject = null;
            try {
                deleteTemporaryFile();
            } catch (Throwable deleteFailure) {
                failure.addSuppressed(deleteFailure);
                cleanupFailed = true;
            }
            if (failure instanceof Error) throw (Error) failure;
            if (cleanupFailed) {
                throw new StableDownloadFailure(
                        "S3_RESOURCE_FAILURE");
            }
            throw (Exception) failure;
        }
    }

    private void requireExactMetadata(S3ObjectStream object) {
        if (object.contentLength() != descriptor.contentLength()
                || !descriptor.etag().equals(object.etag())
                || (descriptor.versioned()
                && !descriptor.versionId().equals(object.versionId()))) {
            throw changed();
        }
    }

    private S3RecordDecoder openDecoder(Path path) throws Exception {
        InputStream input = Files.newInputStream(path, StandardOpenOption.READ);
        try {
            if (descriptor.format() == S3ObjectFormat.NDJSON_UTF8) {
                return new S3NdjsonDecoder(input, descriptor.schema(),
                        configuration.maximumRowBytes());
            }
            return new S3CsvDecoder(input, descriptor.schema(),
                    configuration.maximumRowBytes(),
                    configuration.nullLiteral());
        } catch (Throwable failure) {
            try {
                input.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void finishVerifiedObject() throws Exception {
        Throwable failure = null;
        S3RecordDecoder current = decoder;
        decoder = null;
        if (current != null) {
            try {
                current.close();
            } catch (Throwable closeFailure) {
                failure = closeFailure;
            }
        }
        try {
            deleteTemporaryFile();
        } catch (Throwable deleteFailure) {
            if (failure == null) failure = deleteFailure;
            else failure.addSuppressed(deleteFailure);
        }
        try {
            closeClient();
        } catch (Throwable closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (failure != null) {
            throw new IllegalStateException("S3_RESOURCE_FAILURE");
        }
    }

    @Override public void wakeUp() {
        wakeRequested = true;
        closeObjectQuietly(activeObject);
    }

    @Override public void cancel() {
        cancelled = true;
        closeObjectQuietly(activeObject);
        closeDecoderQuietly();
        deleteTemporaryFileQuietly();
        closeClientQuietly();
    }

    @Override public void close() throws Exception {
        if (closed) return;
        closed = true;
        closeObjectQuietly(activeObject);
        Throwable failure = null;
        try {
            S3RecordDecoder current = decoder;
            decoder = null;
            if (current != null) current.close();
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            deleteTemporaryFile();
        } catch (Throwable deleteFailure) {
            if (failure == null) failure = deleteFailure;
            else failure.addSuppressed(deleteFailure);
        }
        try {
            closeClient();
        } catch (Throwable closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (failure != null) {
            throw new IllegalStateException("S3_RESOURCE_FAILURE");
        }
    }

    private void closeAfterFailure(Throwable primary) {
        closed = true;
        closeObjectWithSuppressed(activeObject, primary);
        activeObject = null;
        S3RecordDecoder current = decoder;
        decoder = null;
        if (current != null) {
            try {
                current.close();
            } catch (Throwable closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }
        try {
            deleteTemporaryFile();
        } catch (Throwable deleteFailure) {
            primary.addSuppressed(deleteFailure);
        }
        try {
            closeClient();
        } catch (Throwable closeFailure) {
            primary.addSuppressed(closeFailure);
        }
    }

    private void closeDecoderQuietly() {
        S3RecordDecoder current = decoder;
        decoder = null;
        if (current != null) {
            try { current.close(); } catch (Throwable ignored) { }
        }
    }

    private void closeClient() throws Exception {
        if (clientClosed.compareAndSet(false, true)) client.close();
    }

    private void closeClientQuietly() {
        try { closeClient(); } catch (Throwable ignored) { }
    }

    private void deleteTemporaryFile() throws Exception {
        Path path = temporaryFile;
        if (path != null) {
            Files.deleteIfExists(path);
            if (temporaryFile == path) temporaryFile = null;
        }
    }

    private void deleteTemporaryFileQuietly() {
        try { deleteTemporaryFile(); } catch (Throwable ignored) { }
    }

    private static void closeObject(S3ObjectStream object) throws Exception {
        if (object != null) object.close();
    }

    private static void closeObjectQuietly(S3ObjectStream object) {
        try { closeObject(object); } catch (Throwable ignored) { }
    }

    private static void closeObjectWithSuppressed(S3ObjectStream object,
            Throwable primary) {
        try { closeObject(object); } catch (Throwable failure) {
            primary.addSuppressed(failure);
        }
    }

    private static StableDownloadFailure changed() {
        return new StableDownloadFailure("S3_OBJECT_CHANGED");
    }

    private static IllegalStateException ioFailure() {
        return new IllegalStateException("S3_IO_FAILURE");
    }

    private static boolean isStableResponseFailure(Exception failure) {
        if (!(failure instanceof IllegalArgumentException)
                && !(failure instanceof IllegalStateException)) {
            return false;
        }
        return "S3_OBJECT_RESPONSE_INVALID".equals(failure.getMessage());
    }

    private static final class InterruptedDownload extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private static final class StableDownloadFailure
            extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private StableDownloadFailure(String code) { super(code); }
    }
}
