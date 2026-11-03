package io.rowweave.pipeline;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.UUID;

/** Shared-filesystem durable CAS substrate for handoff authority records. */
public final class FileVersionedDatasetAuthorityRecordStore
        implements VersionedDatasetAuthorityRecordStore {
    private static final Object[] PROCESS_LOCKS = processLocks();

    private final Path root;
    private final Runnable afterDurableWrite;

    public FileVersionedDatasetAuthorityRecordStore(Path root) {
        this(root, () -> { });
    }

    FileVersionedDatasetAuthorityRecordStore(
            Path root, Runnable afterDurableWrite) {
        if (root == null || afterDurableWrite == null) {
            throw new IllegalArgumentException("root and callback are required");
        }
        this.root = root.toAbsolutePath().normalize();
        this.afterDurableWrite = afterDurableWrite;
        try {
            Files.createDirectories(this.root.resolve("records"));
            Files.createDirectories(this.root.resolve("locks"));
        } catch (IOException failure) {
            throw ioFailure();
        }
    }

    @Override
    public Optional<VersionedDatasetAuthorityRecord> read(
            RecordKind kind, String recordId) {
        Path path = recordPath(kind, recordId);
        if (!Files.exists(path)) return Optional.empty();
        try {
            return Optional.of(new VersionedDatasetAuthorityRecord(readBounded(path)));
        } catch (java.nio.file.NoSuchFileException race) {
            return Optional.empty();
        } catch (IOException failure) {
            throw ioFailure();
        }
    }

    @Override
    public boolean compareAndSet(
            RecordKind kind, String recordId, String expectedVersionToken,
            byte[] replacement) {
        validate(kind, recordId);
        VersionedDatasetAuthorityRecord next =
                new VersionedDatasetAuthorityRecord(replacement);
        Path target = recordPath(kind, recordId);
        Path lockPath = lockPath(kind, recordId);
        Object processLock = PROCESS_LOCKS[
                Math.floorMod((root.toString() + ':' + kind + ':' + recordId).hashCode(),
                        PROCESS_LOCKS.length)];
        synchronized (processLock) {
            try {
                Files.createDirectories(target.getParent());
                Files.createDirectories(lockPath.getParent());
                try (FileChannel lockChannel = FileChannel.open(lockPath,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                     FileLock ignored = lockChannel.lock()) {
                    Optional<VersionedDatasetAuthorityRecord> current = read(kind, recordId);
                    if (expectedVersionToken == null) {
                        if (current.isPresent()) return false;
                    } else if (!current.isPresent()
                            || !expectedVersionToken.equals(
                            current.get().versionToken())) {
                        return false;
                    }
                    writeAtomically(target, next.bytes());
                    afterDurableWrite.run();
                    return true;
                }
            } catch (VersionedDatasetHandoffException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (IOException failure) {
                throw ioFailure();
            }
        }
    }

    private void writeAtomically(Path target, byte[] bytes) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName().toString()
                + "." + UUID.randomUUID().toString() + ".tmp");
        boolean moved = false;
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("atomic authority replacement unavailable");
            }
            moved = true;
            forceDirectory(target.getParent());
        } finally {
            if (!moved) Files.deleteIfExists(temporary);
        }
    }

    private static byte[] readBounded(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < 0L || size > VersionedDatasetHandoffLimits.MAX_RECORD_BYTES) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "authorityRecord",
                        "stored authority record exceeds limit");
            }
            byte[] bytes = new byte[(int) size];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) {
                    throw new IOException("truncated authority record");
                }
            }
            return bytes;
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private Path recordPath(RecordKind kind, String recordId) {
        validate(kind, recordId);
        return root.resolve("records").resolve(kind.name().toLowerCase(java.util.Locale.ROOT))
                .resolve(recordId.substring(0, 2)).resolve(recordId + ".record");
    }

    private Path lockPath(RecordKind kind, String recordId) {
        validate(kind, recordId);
        return root.resolve("locks").resolve(kind.name().toLowerCase(java.util.Locale.ROOT))
                .resolve(recordId.substring(0, 2)).resolve(recordId + ".lock");
    }

    private static void validate(RecordKind kind, String recordId) {
        if (kind == null) throw new IllegalArgumentException("record kind is required");
        VersionedDatasetHandoffSupport.digest(recordId, "authorityRecord.id");
    }

    private static VersionedDatasetHandoffException ioFailure() {
        return new VersionedDatasetHandoffException("AUTHORITY_IO_FAILURE",
                "durable authority storage operation failed");
    }

    private static Object[] processLocks() {
        Object[] locks = new Object[256];
        for (int i = 0; i < locks.length; i++) locks[i] = new Object();
        return locks;
    }
}
