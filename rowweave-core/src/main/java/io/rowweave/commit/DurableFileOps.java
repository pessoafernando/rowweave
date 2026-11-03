package io.rowweave.commit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.LinkOption;
import java.util.UUID;

/** Same-directory atomic rename plus file and directory force; a provider that cannot prove durability must fail closed. */
final class DurableFileOps {

    enum Step {
        TEMP_FILE_FORCED,
        IMMUTABLE_NAME_CREATED,
        DIRECTORY_FORCED_AFTER_CREATE,
        TEMP_NAME_REMOVED,
        DIRECTORY_FORCED_AFTER_CLEANUP
    }

    interface Observer {
        void after(Step step, Path target);
    }

    private static final Observer NOOP = (step, target) -> { };

    private DurableFileOps() {
    }

    static void ensureDirectory(Path directory) {
        try {
            if (Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                return;
            }
            try {
                Files.createDirectories(directory);
            } catch (FileAlreadyExistsException raced) {
                if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                    throw raced;
                }
            }
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("durable directory path is not a directory: " + directory);
            }
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
                forceDirectory(parent);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create durable directory: " + directory, e);
        }
    }

    static void atomicReplace(Path target, byte[] bytes) {
        try {
            atomicWrite(target, bytes, true);
        } catch (FileAlreadyExistsException impossibleWithReplace) {
            throw new IllegalStateException("replace unexpectedly reported target collision: " + target,
                    impossibleWithReplace);
        }
    }

    static void atomicCreate(Path target, byte[] bytes) throws FileAlreadyExistsException {
        atomicCreate(target, bytes, NOOP);
    }

    static void atomicCreate(Path target, byte[] bytes, Observer observer)
            throws FileAlreadyExistsException {
        atomicCreate(target, bytes, observer, "");
    }

    static void atomicCreate(Path target, byte[] bytes, Observer observer, String temporaryTag)
            throws FileAlreadyExistsException {
        Path parent = target.getParent();
        ensureDirectory(parent);
        String tag = temporaryTag == null || temporaryTag.isEmpty() ? "" : temporaryTag + "-";
        if (!tag.matches("[a-zA-Z0-9-]*")) {
            throw new IllegalArgumentException("unsafe durable temporary tag");
        }
        Path temporary = parent.resolve(".rowweave-manifest-" + tag + UUID.randomUUID() + ".tmp");
        Throwable failure = null;
        try {
            writeAndForce(temporary, bytes);
            observer.after(Step.TEMP_FILE_FORCED, target);
            // The behaviour of Java ATOMIC_MOVE when the target already exists is undefined, so it cannot express an immutable create.
            // A same-directory hard link makes one fully forced inode appear as a single atomic directory entry and cannot replace an existing name.
            Files.createLink(target, temporary);
            observer.after(Step.IMMUTABLE_NAME_CREATED, target);
            forceDirectory(parent);
            observer.after(Step.DIRECTORY_FORCED_AFTER_CREATE, target);
            Files.delete(temporary);
            observer.after(Step.TEMP_NAME_REMOVED, target);
            forceDirectory(parent);
            observer.after(Step.DIRECTORY_FORCED_AFTER_CLEANUP, target);
        } catch (FileAlreadyExistsException collision) {
            failure = collision;
            throw collision;
        } catch (IOException | UnsupportedOperationException e) {
            failure = e;
            throw new IllegalStateException(
                    "filesystem does not support required immutable hard-link publication: " + target, e);
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException cleanupFailure) {
                if (failure != null) {
                    failure.addSuppressed(cleanupFailure);
                } else {
                    throw new UncheckedIOException("failed to clean manifest temporary: " + temporary,
                            cleanupFailure);
                }
            }
        }
    }

    /** Installs a forced staging inode as immutable data through a no-replace hard link, then forces the directory. */
    static void installImmutable(Path staged, Path target) throws FileAlreadyExistsException {
        Path parent = target.getParent();
        ensureDirectory(parent);
        try {
            Files.createLink(target, staged);
            forceDirectory(parent);
        } catch (FileAlreadyExistsException collision) {
            throw collision;
        } catch (IOException | UnsupportedOperationException e) {
            throw new IllegalStateException(
                    "filesystem does not support required immutable data installation: " + target, e);
        }
    }

    static void forceFile(Path path) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            throw new IllegalStateException("filesystem does not support required file force: " + path, e);
        }
    }

    private static void atomicWrite(Path target, byte[] bytes, boolean replace)
            throws FileAlreadyExistsException {
        Path parent = target.getParent();
        ensureDirectory(parent);
        Path temporary = parent.resolve(".rowweave-manifest-" + UUID.randomUUID() + ".tmp");
        Throwable failure = null;
        try {
            writeAndForce(temporary, bytes);
            if (replace) {
                Files.move(temporary, target,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            }
            forceDirectory(parent);
        } catch (FileAlreadyExistsException collision) {
            failure = collision;
            throw collision;
        } catch (IOException e) {
            failure = e;
            throw new UncheckedIOException("failed durable atomic write: " + target, e);
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException cleanupFailure) {
                if (failure != null) {
                    failure.addSuppressed(cleanupFailure);
                } else {
                    throw new UncheckedIOException("failed to clean manifest temporary: " + temporary,
                            cleanupFailure);
                }
            }
        }
    }

    private static void writeAndForce(Path path, byte[] bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    static void forceDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            throw new IllegalStateException(
                    "filesystem does not support required directory force: " + directory, e);
        }
    }
}
