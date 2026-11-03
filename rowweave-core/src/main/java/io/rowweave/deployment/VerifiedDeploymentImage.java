package io.rowweave.deployment;

import java.util.Arrays;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;

/** Private immutable compressed image; later slices add the canonical ZIP index and loader. */
final class VerifiedDeploymentImage implements AutoCloseable {
    private final DeploymentManifest manifest;
    private byte[] compressedBytes;
    private final DeploymentMemoryBudget.Lease compressedLease;
    private DeploymentArchiveScanner.Index index;
    private final DeploymentMemoryBudget budget;
    private final DeploymentSourceIdentity sourceIdentity;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycle = new Object();
    private int borrowedEntries;
    private boolean baseReleased;
    private DeploymentClassLoader loader;

    VerifiedDeploymentImage(DeploymentManifest manifest, byte[] compressedBytes,
                            DeploymentMemoryBudget.Lease compressedLease,
                            DeploymentArchiveScanner.Index index,
                            DeploymentMemoryBudget budget,
                            DeploymentSourceIdentity sourceIdentity) {
        this.manifest = manifest;
        this.compressedBytes = compressedBytes;
        this.compressedLease = compressedLease;
        this.index = index;
        this.budget = budget;
        this.sourceIdentity = sourceIdentity;
    }

    DeploymentManifest manifest() { return manifest; }

    boolean compressedContentEquals(byte[] expected) {
        return !closed.get() && Arrays.equals(compressedBytes, expected);
    }

    int entryCount() { return index.entries.size(); }
    java.util.Set<String> classNames() { return index.classNames; }
    boolean hasEntry(String name) {
        synchronized (lifecycle) {
            return !closed.get() && index != null && index.entries.containsKey(name);
        }
    }
    boolean hasClass(String name) {
        DeploymentArchiveScanner.Index current = index;
        return current != null && current.classNames.contains(name);
    }
    int entryExpandedSize(String name) {
        synchronized (lifecycle) {
            if (closed.get() || index == null) return -1;
            DeploymentArchiveScanner.Entry entry = index.entries.get(name);
            return entry == null ? -1 : entry.expandedSize;
        }
    }

    boolean ownsClass(Class<?> type) {
        synchronized (lifecycle) {
            return !closed.get() && loader != null && loader.owns(type);
        }
    }
    long expandedBytes() { return index.expandedBytes; }

    boolean sourceMatches() {
        if (closed.get()) return false;
        try {
            BasicFileAttributes before = Files.readAttributes(manifest.absoluteJarPath(),
                    BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!sourceIdentity.matches(before) || Files.isSymbolicLink(manifest.absoluteJarPath())) {
                return false;
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            DeploymentMemoryBudget.Lease lease = budget.reserve(64L * 1024L);
            try {
                byte[] buffer = new byte[64 * 1024];
                long count = 0L;
                try (InputStream input = Files.newInputStream(manifest.absoluteJarPath())) {
                    for (;;) {
                        int read = input.read(buffer);
                        if (read < 0) break;
                        if (read == 0) continue;
                        count = Math.addExact(count, read);
                        if (count > manifest.jarLength()) return false;
                        digest.update(buffer, 0, read);
                    }
                }
                BasicFileAttributes after = Files.readAttributes(manifest.absoluteJarPath(),
                        BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                return count == manifest.jarLength()
                        && sourceIdentity.matches(after)
                        && MessageDigest.isEqual(manifest.sha256(), digest.digest());
            } finally {
                lease.close();
            }
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            return false;
        }
    }

    String entryDigest(String name) {
        DeploymentArchiveScanner.Entry entry = index.entries.get(name);
        if (entry == null) return null;
        char[] output = new char[entry.sha256.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < entry.sha256.length; i++) {
            output[i * 2] = digits[(entry.sha256[i] >>> 4) & 0xf];
            output[i * 2 + 1] = digits[entry.sha256[i] & 0xf];
        }
        return new String(output);
    }

    DeploymentClassLoader classLoader(ClassLoader parent) {
        synchronized (lifecycle) {
            if (closed.get()) throw new IllegalStateException("deployment image is closed");
            if (loader == null) loader = new DeploymentClassLoader(this, parent);
            return loader;
        }
    }

    BorrowedEntry borrowClass(String binaryName) {
        return borrow(binaryName.replace('.', '/') + ".class");
    }

    BorrowedEntry borrowResource(String name) {
        return borrow(name);
    }

    private BorrowedEntry borrow(String name) {
        final DeploymentArchiveScanner.Entry entry;
        final byte[] image;
        synchronized (lifecycle) {
            if (closed.get()) return null;
            entry = index.entries.get(name);
            if (entry == null) return null;
            image = compressedBytes;
            borrowedEntries++;
        }
        try {
            return new BorrowedEntry(
                    DeploymentArchiveScanner.expandVerified(image, entry, budget), this);
        } catch (RuntimeException | Error failure) {
            releaseBorrow();
            throw failure;
        }
    }

    void closeFromLoader(DeploymentClassLoader source) {
        synchronized (lifecycle) {
            if (loader != source) return;
        }
        close();
    }

    private void releaseBorrow() {
        DeploymentArchiveScanner.Index releaseIndex = null;
        synchronized (lifecycle) {
            if (borrowedEntries <= 0) throw new IllegalStateException("deployment borrow underflow");
            borrowedEntries--;
            if (closed.get() && borrowedEntries == 0 && !baseReleased) {
                releaseIndex = detachBase();
            }
        }
        releaseBase(releaseIndex);
    }

    private DeploymentArchiveScanner.Index detachBase() {
        baseReleased = true;
        compressedBytes = null;
        DeploymentArchiveScanner.Index release = index;
        index = null;
        return release;
    }

    private void releaseBase(DeploymentArchiveScanner.Index releaseIndex) {
        if (releaseIndex != null) {
            releaseIndex.close();
            compressedLease.close();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        DeploymentArchiveScanner.Index releaseIndex = null;
        synchronized (lifecycle) {
            if (loader != null) loader.markClosed();
            if (borrowedEntries == 0 && !baseReleased) releaseIndex = detachBase();
        }
        releaseBase(releaseIndex);
    }

    static final class BorrowedEntry implements AutoCloseable {
        private final DeploymentArchiveScanner.ExpandedEntry expanded;
        private final VerifiedDeploymentImage owner;
        private final AtomicBoolean released = new AtomicBoolean();

        BorrowedEntry(DeploymentArchiveScanner.ExpandedEntry expanded,
                      VerifiedDeploymentImage owner) {
            this.expanded = expanded;
            this.owner = owner;
        }

        byte[] bytes() { return expanded.bytes; }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                expanded.close();
                owner.releaseBorrow();
            }
        }
    }
}
