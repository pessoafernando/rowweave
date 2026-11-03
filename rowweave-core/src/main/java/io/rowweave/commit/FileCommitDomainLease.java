package io.rowweave.commit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/** In-process plus operating-system exclusive lease for the writer and recovery roles on one host's manifest domain. */
public final class FileCommitDomainLease implements AutoCloseable {

    private static final ConcurrentHashMap<Path, LeaseSlot> JVM_LEASES = new ConcurrentHashMap<>();

    private final Path domainIdentity;
    private final LeaseSlot jvmLease;
    private FileChannel channel;
    private FileLock fileLock;
    private boolean closed;

    private FileCommitDomainLease(
            Path domainIdentity, LeaseSlot jvmLease, FileChannel channel, FileLock fileLock) {
        this.domainIdentity = domainIdentity;
        this.jvmLease = jvmLease;
        this.channel = channel;
        this.fileLock = fileLock;
    }

    /** Returns null when a writer or recovery holder is already active; never blocks. */
    public static FileCommitDomainLease tryAcquire(Path domain) {
        if (domain == null) {
            throw new IllegalArgumentException("domain must not be null");
        }
        Path root = ManifestDomainIdentity.canonicalPath(domain);
        LeaseSlot jvm = JVM_LEASES.compute(root, (identity, existing) -> {
            LeaseSlot slot = existing == null ? new LeaseSlot() : existing;
            slot.references++;
            return slot;
        });
        if (!jvm.permit.tryAcquire()) {
            releaseReference(root, jvm);
            return null;
        }
        FileChannel channel = null;
        try {
            Path authority = root.resolve("authority");
            DurableFileOps.ensureDirectory(authority);
            channel = FileChannel.open(authority.resolve("writer.lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException activeInJvm) {
                lock = null;
            }
            if (lock == null) {
                channel.close();
                jvm.permit.release();
                releaseReference(root, jvm);
                return null;
            }
            return new FileCommitDomainLease(root, jvm, channel, lock);
        } catch (IOException failure) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            jvm.permit.release();
            releaseReference(root, jvm);
            throw new UncheckedIOException("failed to acquire manifest domain lease: " + root, failure);
        } catch (RuntimeException | Error failure) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            jvm.permit.release();
            releaseReference(root, jvm);
            throw failure;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Throwable failure = null;
        FileLock lock = fileLock;
        fileLock = null;
        if (lock != null) {
            try {
                lock.release();
            } catch (Throwable releaseFailure) {
                failure = releaseFailure;
            }
        }
        FileChannel current = channel;
        channel = null;
        if (current != null) {
            try {
                current.close();
            } catch (Throwable closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        jvmLease.permit.release();
        releaseReference(domainIdentity, jvmLease);
        if (failure != null) {
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            throw new RuntimeException("failed to release manifest domain lease", failure);
        }
    }

    private static void releaseReference(Path identity, LeaseSlot expected) {
        JVM_LEASES.compute(identity, (key, current) -> {
            if (current != expected || current.references <= 0) {
                throw new IllegalStateException("manifest JVM lease registry corruption: " + identity);
            }
            current.references--;
            return current.references == 0 ? null : current;
        });
    }

    private static final class LeaseSlot {
        private final Semaphore permit = new Semaphore(1);
        // Read and write only inside the per-key serialised region of ConcurrentHashMap.compute.
        private int references;
    }
}
