package io.rowweave.stream.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;
import java.util.Set;

/** Bounded streaming content-addressed payload store for File checkpoints. */
public final class FileCheckpointPayloadStore {
    private static final int COPY_BUFFER_BYTES = 64 * 1024;
    public static final int TRANSFER_CHUNK_BYTES = 64 * 1024;
    private static final Object JVM_TRANSFER_LOCK = new Object();

    enum PublicationStep { PAYLOAD_DURABLE }

    interface PublicationObserver {
        void after(PublicationStep step, Path path);
    }

    private static final PublicationObserver NOOP = (step, path) -> { };

    public static final class StoredPayload {
        private final String digest;
        private final long contentBytes;

        public StoredPayload(String digest, long contentBytes) {
            requireDigest(digest);
            if (contentBytes < 1L) {
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "checkpoint payload size must be positive");
            }
            this.digest = digest;
            this.contentBytes = contentBytes;
        }

        public String digest() { return digest; }
        public long contentBytes() { return contentBytes; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StoredPayload)) return false;
            StoredPayload that = (StoredPayload) other;
            return contentBytes == that.contentBytes
                    && digest.equals(that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(digest, contentBytes);
        }
    }

    private final Path payloadDirectory;
    private final Path transferDirectory;
    private final long maximumPayloadBytes;
    private final PublicationObserver observer;

    public FileCheckpointPayloadStore(Path root, long maximumPayloadBytes) {
        this(root, maximumPayloadBytes, NOOP);
    }

    FileCheckpointPayloadStore(
            Path root, long maximumPayloadBytes,
            PublicationObserver observer) {
        if (root == null || maximumPayloadBytes < 1L || observer == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint payload root and positive bound are required");
        }
        Path normalized = root.toAbsolutePath().normalize();
        ensureDirectory(normalized);
        payloadDirectory = normalized.resolve("payloads");
        transferDirectory = normalized.resolve("payload-transfer");
        this.maximumPayloadBytes = maximumPayloadBytes;
        this.observer = observer;
        ensureDirectory(payloadDirectory);
        ensureDirectory(transferDirectory);
    }

    public StoredPayload store(Path staged) {
        requireRegularFile(staged);
        Path temporary = temporary(payloadDirectory);
        Throwable primary = null;
        try {
            copyBounded(staged, temporary);
            forceFile(temporary);
            long contentBytes = Files.size(temporary);
            String digest = digestFile(temporary);
            Path target = payloadPath(digest);
            boolean created = false;
            try {
                Files.createLink(target, temporary);
                forceDirectory(payloadDirectory);
                created = true;
            } catch (FileAlreadyExistsException collision) {
                verify(target, digest, contentBytes);
            }
            if (created) {
                observer.after(PublicationStep.PAYLOAD_DURABLE, target);
            }
            return new StoredPayload(digest, contentBytes);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint payload publication failed", ioFailure);
        } finally {
            cleanup(temporary, primary);
        }
    }

    public void restore(StoredPayload payload, Path target) {
        if (payload == null || target == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint payload and restore target are required");
        }
        if (payload.contentBytes > maximumPayloadBytes) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload exceeds the restore bound");
        }
        Path source = payloadPath(payload.digest);
        verify(source, payload.digest, payload.contentBytes);
        Path absoluteTarget = target.toAbsolutePath().normalize();
        Path parent = absoluteTarget.getParent();
        if (parent == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "restore target requires a parent directory");
        }
        ensureDirectory(parent);
        Path temporary = temporary(parent);
        Throwable primary = null;
        try {
            copyBounded(source, temporary);
            forceFile(temporary);
            verify(temporary, payload.digest, payload.contentBytes);
            Files.move(temporary, absoluteTarget,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(parent);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "atomic checkpoint payload restore failed", ioFailure);
        } finally {
            cleanup(temporary, primary);
        }
    }

    public void verifyStored(StoredPayload payload) {
        if (payload == null || payload.contentBytes > maximumPayloadBytes) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload exceeds the verification bound");
        }
        verify(payloadPath(payload.digest), payload.digest,
                payload.contentBytes);
    }

    /** Reads one bounded verified chunk for authenticated remote transfer. */
    public byte[] readChunk(StoredPayload payload, long offset,
            int maximumBytes) {
        if (payload == null || offset < 0L
                || offset >= payload.contentBytes
                || maximumBytes < 1
                || maximumBytes > TRANSFER_CHUNK_BYTES) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload chunk request is invalid");
        }
        if (payload.contentBytes > maximumPayloadBytes) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload exceeds the transfer bound");
        }
        Path source = payloadPath(payload.digest);
        verify(source, payload.digest, payload.contentBytes);
        int length = (int) Math.min((long) maximumBytes,
                payload.contentBytes - offset);
        ByteBuffer buffer = ByteBuffer.allocate(length);
        try (FileChannel channel = FileChannel.open(
                source, StandardOpenOption.READ)) {
            channel.position(offset);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) {
                    throw new IOException("checkpoint payload is truncated");
                }
            }
            return buffer.array();
        } catch (IOException failure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint payload chunk read failed", failure);
        }
    }

    /** Appends one idempotent bounded chunk and atomically publishes on completion. */
    public boolean acceptChunk(StoredPayload payload, long offset,
            byte[] chunk) {
        if (payload == null || offset < 0L || chunk == null
                || chunk.length < 1
                || chunk.length > TRANSFER_CHUNK_BYTES
                || payload.contentBytes > maximumPayloadBytes
                || offset > payload.contentBytes - chunk.length) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload transfer chunk is invalid");
        }
        synchronized (JVM_TRANSFER_LOCK) {
            Path target = payloadPath(payload.digest);
            if (Files.exists(target)) {
                verify(target, payload.digest, payload.contentBytes);
                return true;
            }
            Path staged = transferDirectory.resolve(
                    payload.digest + ".part");
            try {
                long current = Files.exists(staged) ? Files.size(staged) : 0L;
                if (offset < current) {
                    if (offset + chunk.length > current
                            || !matches(staged, offset, chunk)) {
                        throw failure(
                                StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                                "checkpoint payload chunk replay differs");
                    }
                    return current == payload.contentBytes;
                }
                if (offset != current) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                            "checkpoint payload chunks are not contiguous");
                }
                try (FileChannel channel = FileChannel.open(staged,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND)) {
                    ByteBuffer value = ByteBuffer.wrap(chunk);
                    while (value.hasRemaining()) channel.write(value);
                    channel.force(true);
                }
                long size = Math.addExact(current, chunk.length);
                if (size != payload.contentBytes) return false;
                verify(staged, payload.digest, payload.contentBytes);
                StoredPayload stored = store(staged);
                if (!payload.equals(stored)) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                            "checkpoint payload transfer identity differs");
                }
                Files.deleteIfExists(staged);
                forceDirectory(transferDirectory);
                return true;
            } catch (StreamCheckpointException failure) {
                throw failure;
            } catch (IOException | ArithmeticException failure) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "checkpoint payload chunk publication failed", failure);
            }
        }
    }

    private static boolean matches(Path path, long offset, byte[] expected)
            throws IOException {
        ByteBuffer actual = ByteBuffer.allocate(expected.length);
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ)) {
            channel.position(offset);
            while (actual.hasRemaining()) {
                if (channel.read(actual) < 0) return false;
            }
        }
        return MessageDigest.isEqual(expected, actual.array());
    }

    Path payloadPath(String digest) {
        requireDigest(digest);
        return payloadDirectory.resolve(digest + ".payload");
    }

    void retainOnly(Set<String> retainedDigests) {
        if (retainedDigests == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "retained checkpoint payloads are required");
        }
        java.util.HashSet<Path> protectedPaths = new java.util.HashSet<>();
        for (String digest : retainedDigests) {
            protectedPaths.add(payloadPath(digest));
        }
        try {
            boolean changed = false;
            try (java.nio.file.DirectoryStream<Path> entries =
                         Files.newDirectoryStream(
                                 payloadDirectory, "*.payload")) {
                for (Path path : entries) {
                    if (!protectedPaths.contains(path)) {
                        changed |= Files.deleteIfExists(path);
                    }
                }
            }
            if (changed) forceDirectory(payloadDirectory);
        } catch (IOException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint payload removal failed", failure);
        }
    }

    long maximumPayloadBytes() {
        return maximumPayloadBytes;
    }

    private void copyBounded(Path source, Path target) throws IOException {
        long before = Files.size(source);
        if (before < 1L || before > maximumPayloadBytes) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload exceeds its configured bound");
        }
        Files.copy(source, target);
        long after = Files.size(target);
        if (after != before || after > maximumPayloadBytes) {
            throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                    "checkpoint payload changed while being copied");
        }
    }

    private void verify(Path path, String digest, long contentBytes) {
        requireRegularFile(path);
        try {
            long size = Files.size(path);
            if (size != contentBytes || size > maximumPayloadBytes
                    || !digest.equals(digestFile(path))) {
                throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                        "checkpoint payload length or digest drifted");
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                    "checkpoint payload is unreadable", ioFailure);
        }
    }

    private String digestFile(Path path) throws IOException {
        MessageDigest digest = sha256();
        long count = 0L;
        byte[] buffer = new byte[COPY_BUFFER_BYTES];
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read == 0) continue;
                count = Math.addExact(count, read);
                if (count > maximumPayloadBytes) {
                    throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                            "checkpoint payload exceeds digest bound");
                }
                digest.update(buffer, 0, read);
            }
        } catch (ArithmeticException overflow) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "checkpoint payload size overflow", overflow);
        }
        return hex(digest.digest());
    }

    private static void requireRegularFile(Path path) {
        if (path == null || !Files.isRegularFile(
                path, LinkOption.NOFOLLOW_LINKS)) {
            throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                    "checkpoint payload must be a regular file");
        }
    }

    private static Path temporary(Path directory) {
        return directory.resolve(
                ".rowweave-stream-payload-" + UUID.randomUUID() + ".tmp");
    }

    private static void cleanup(Path temporary, Throwable primary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException cleanupFailure) {
            if (primary != null) {
                primary.addSuppressed(cleanupFailure);
            } else {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "checkpoint payload temporary cleanup failed",
                        cleanupFailure);
            }
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "checkpoint payload path is not a directory");
            }
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent)) {
                forceDirectory(parent);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint payload directory creation failed", ioFailure);
        }
    }

    private static void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(
                directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
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

    private static void requireDigest(String digest) {
        if (digest == null || !digest.matches("[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "canonical checkpoint payload digest is required");
        }
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }
}
