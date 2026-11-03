package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.StreamSavepoint;
import io.rowweave.stream.contract.StreamSavepointCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Durable immutable named copies of completed Stream checkpoint manifests. */
public final class FileStreamSavepointStore {
    enum PublicationStep { SAVEPOINT_DURABLE }

    interface PublicationObserver {
        void after(PublicationStep step, Path path);
    }

    private static final PublicationObserver NOOP = (step, path) -> { };

    private final Path savepointDirectory;
    private final PublicationObserver observer;

    public FileStreamSavepointStore(Path root) {
        this(root, NOOP);
    }

    FileStreamSavepointStore(Path root, PublicationObserver observer) {
        if (root == null || observer == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "savepoint root and observer are required");
        }
        Path normalized = root.toAbsolutePath().normalize();
        savepointDirectory = normalized.resolve("savepoints");
        this.observer = observer;
        ensureDirectory(normalized);
        ensureDirectory(savepointDirectory);
    }

    public StreamSavepoint create(
            String savepointId, CheckpointIdentity checkpointIdentity,
            StreamCheckpointAuthority checkpointAuthority) {
        if (checkpointIdentity == null || checkpointAuthority == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint identity and authority are required");
        }
        String canonicalId = StreamSavepoint.canonicalId(savepointId);
        Path path = savepointPath(canonicalId);
        StreamSavepoint existing = read(path, canonicalId);
        if (existing != null) {
            if (!checkpointIdentity.equals(
                    existing.checkpointManifest().checkpointIdentity())) {
                throw failure(StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                        "savepoint name cannot be rebound to another checkpoint");
            }
            return existing;
        }
        CheckpointDecision decision = checkpointAuthority.decision(
                checkpointIdentity);
        if (decision == null
                || decision.status() != CheckpointDecision.Status.COMPLETED) {
            throw failure(StreamCheckpointException.Code.CHECKPOINT_NOT_COMPLETED,
                    "savepoint requires an exact completed checkpoint");
        }
        CheckpointManifest manifest = checkpointAuthority.manifest(
                decision.manifestDigest());
        if (manifest == null
                || !checkpointIdentity.equals(manifest.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                    "completed checkpoint has no exact manifest");
        }
        StreamSavepoint proposed = new StreamSavepoint(canonicalId, manifest);
        return publish(proposed);
    }

    public StreamSavepoint create(
            String savepointId, CheckpointIdentity checkpointIdentity,
            FileCheckpointAuthority checkpointAuthority) {
        return create(savepointId, checkpointIdentity,
                (StreamCheckpointAuthority) checkpointAuthority);
    }

    public StreamSavepoint get(String savepointId) {
        String canonicalId = StreamSavepoint.canonicalId(savepointId);
        return read(savepointPath(canonicalId), canonicalId);
    }

    StreamSavepoint publish(StreamSavepoint proposed) {
        if (proposed == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "savepoint is required");
        }
        String canonicalId = StreamSavepoint.canonicalId(
                proposed.savepointId());
        Path path = savepointPath(canonicalId);
        StreamSavepoint existing = read(path, canonicalId);
        if (existing != null) return requireExact(existing, proposed);
        publishImmutable(path, StreamSavepointCodec.encode(proposed));
        observer.after(PublicationStep.SAVEPOINT_DURABLE, path);
        return requireExact(read(path, canonicalId), proposed);
    }

    List<StreamSavepoint> all() {
        List<StreamSavepoint> result = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> entries =
                     Files.newDirectoryStream(
                             savepointDirectory, "*.savepoint")) {
            for (Path path : entries) {
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                            "savepoint metadata must be a regular file");
                }
                long size = Files.size(path);
                if (size < 1L || size > StreamSavepointCodec.MAX_FRAME_BYTES) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                            "savepoint metadata exceeds its bound");
                }
                StreamSavepoint decoded = StreamSavepointCodec.decode(
                        Files.readAllBytes(path));
                if (!path.equals(savepointPath(decoded.savepointId()))) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                            "savepoint identity does not match its durable name");
                }
                result.add(decoded);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                    "savepoint catalog is unreadable", failure);
        }
        result.sort((left, right) -> left.savepointId().compareTo(
                right.savepointId()));
        return Collections.unmodifiableList(result);
    }

    void removeCheckpointReference(String checkpointId) {
        String canonical = StreamSavepoint.canonicalId(checkpointId);
        if (!canonical.startsWith("stream-checkpoint:")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "only retained checkpoint references may be removed");
        }
        try {
            if (Files.deleteIfExists(savepointPath(canonical))) {
                forceDirectory(savepointDirectory);
            }
        } catch (IOException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint reference removal failed", failure);
        }
    }

    Path savepointPath(String savepointId) {
        String canonicalId = StreamSavepoint.canonicalId(savepointId);
        return savepointDirectory.resolve(
                hex(sha256(canonicalId.getBytes(StandardCharsets.UTF_8)))
                        + ".savepoint");
    }

    private static StreamSavepoint requireExact(
            StreamSavepoint existing, StreamSavepoint proposed) {
        if (existing == null || !existing.equals(proposed)) {
            throw failure(StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                    "savepoint name cannot be rebound to another checkpoint");
        }
        return existing;
    }

    private static StreamSavepoint read(Path path, String expectedId) {
        if (!Files.exists(path)) return null;
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                    "savepoint metadata must be a regular file");
        }
        try {
            long size = Files.size(path);
            if (size < 1L || size > StreamSavepointCodec.MAX_FRAME_BYTES) {
                throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                        "savepoint metadata exceeds its bound");
            }
            StreamSavepoint decoded = StreamSavepointCodec.decode(
                    Files.readAllBytes(path));
            if (!expectedId.equals(decoded.savepointId())) {
                throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                        "savepoint identity does not match its durable name");
            }
            return decoded;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                    "savepoint metadata is corrupt", malformed);
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "savepoint metadata read failed", ioFailure);
        }
    }

    private static void publishImmutable(Path target, byte[] frame) {
        Path temporary = target.getParent().resolve(
                ".rowweave-stream-savepoint-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(frame);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try {
                Files.createLink(target, temporary);
                forceDirectory(target.getParent());
            } catch (FileAlreadyExistsException collision) {
                primary = collision;
            }
        } catch (IOException | UnsupportedOperationException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "durable immutable savepoint publication failed", ioFailure);
        } catch (RuntimeException | Error runtimeFailure) {
            primary = runtimeFailure;
            throw runtimeFailure;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException cleanupFailure) {
                if (primary != null) primary.addSuppressed(cleanupFailure);
                else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "savepoint temporary cleanup failed", cleanupFailure);
            }
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "savepoint path is not a directory");
            }
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent)) {
                forceDirectory(parent);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException | UnsupportedOperationException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "savepoint directory is not durable", ioFailure);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(
                directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
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
