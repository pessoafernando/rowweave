package io.rowweave.upgrade;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** Crash-durable single-filesystem StoreFormatHeader and UpgradeAuthority. */
public final class FileUpgradeAuthority implements UpgradeAuthorityStore {
    public enum PersistencePoint { AFTER_AUTHORITY_REPLACE_BEFORE_ACK }

    @FunctionalInterface
    interface PersistenceObserver {
        void at(PersistencePoint point);
    }

    private static final int LOCK_STRIPES = 256;
    private static final int MAX_OBJECTS = 100_000;
    private static final int MAX_ENTRIES_PER_DIRECTORY = 4096;
    private static final ReentrantLock[] JVM_LOCKS = new ReentrantLock[LOCK_STRIPES];

    static {
        for (int index = 0; index < JVM_LOCKS.length; index++) {
            JVM_LOCKS[index] = new ReentrantLock();
        }
    }

    private final Path root;
    private final Path namespaces;
    private final Path metadata;
    private final Path headerFile;
    private final Path authorityFile;
    private final Path lockFile;
    private final Path copyEvidenceRoot;
    private final ReentrantLock jvmLock;
    private final PersistenceObserver observer;

    public FileUpgradeAuthority(Path root) {
        this(root, point -> { });
    }

    FileUpgradeAuthority(Path root, PersistenceObserver observer) {
        if (root == null || observer == null) {
            throw new IllegalArgumentException("root and observer are required");
        }
        this.root = root.toAbsolutePath().normalize();
        this.namespaces = this.root.resolve("namespaces");
        this.metadata = this.root.resolve(".rowweave-upgrade-v1");
        this.headerFile = metadata.resolve("store-header.frame");
        this.authorityFile = metadata.resolve("upgrade-authority.frame");
        this.lockFile = metadata.resolve("authority.lock");
        this.copyEvidenceRoot = metadata.resolve("copy-evidence");
        this.jvmLock = JVM_LOCKS[(this.root.hashCode() & Integer.MAX_VALUE)
                % JVM_LOCKS.length];
        this.observer = observer;
        initializeDirectory(this.root, "upgrade root");
        initializeDirectory(namespaces, "namespace root");
        initializeDirectory(metadata, "upgrade metadata root");
        try {
            if (!Files.exists(lockFile, LinkOption.NOFOLLOW_LINKS)) {
                Files.createFile(lockFile);
                forceDirectory(metadata);
            }
            requireRegularFile(lockFile, "authority lock");
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to initialize upgrade lock", failure);
        }
    }

    public UpgradeAuthorityRecord bootstrap(
            StoreFormatHeader sourceHeader, StoreFormatHeader targetHeader,
            long generation, String sourceNamespace, String shadowNamespace,
            UpgradeOwner owner, long rollbackDeadlineEpochMillis) {
        validateNamespace(sourceNamespace);
        validateNamespace(shadowNamespace);
        if (sourceNamespace.equals(shadowNamespace)) {
            throw new IllegalArgumentException("source and shadow namespace must differ");
        }
        if (rollbackDeadlineEpochMillis < 0L) {
            throw new IllegalArgumentException("rollback deadline is invalid");
        }
        return locked(() -> {
            Path source = namespace(sourceNamespace);
            requireDirectory(source, "source namespace");
            Scan sourceScan = scan(source);
            if (Files.exists(authorityFile, LinkOption.NOFOLLOW_LINKS)) {
                UpgradeAuthorityRecord existing = readUnsafe();
                requireBootstrapIdentity(existing, sourceHeader, targetHeader,
                        generation, sourceNamespace, shadowNamespace, owner,
                        sourceScan.digest);
                return existing;
            }
            writeHeaderOnce(sourceHeader);
            String zero = zeroDigest();
            UpgradeAuthorityRecord initial = new UpgradeAuthorityRecord(
                    1, sourceHeader, targetHeader, generation, UpgradePhase.IDLE,
                    sourceNamespace, sourceScan.digest, shadowNamespace,
                    sourceScan.digest, owner, 0L, sourceNamespace, 0L, 1L,
                    0L, 0, zero, zero, rollbackDeadlineEpochMillis, true, false,
                    Collections.<UpgradeCommandEvidence>emptyList());
            writeAuthority(initial, false);
            return initial;
        });
    }

    @Override
    public UpgradeAuthorityRecord read() {
        try {
            return readUnsafe();
        } catch (RuntimeException failure) {
            if (failure instanceof IllegalStateException) throw failure;
            throw new IllegalStateException("upgrade authority is corrupt", failure);
        }
    }

    @Override
    public UpgradeTransition apply(
            UpgradeCommand command, long activeRunCount,
            long activeIntervalCount, long authorityTimeMillis) {
        return locked(() -> {
            UpgradeAuthorityRecord current = readUnsafe();
            UpgradeTransition transition = UpgradeAuthorityKernel.apply(
                    current, command, activeRunCount, activeIntervalCount,
                    authorityTimeMillis);
            if (transition.code() == UpgradeTransition.Code.ACCEPTED) {
                writeAuthority(transition.record(), true);
            }
            return transition;
        });
    }

    public NamespaceVerification copyAndVerifyShadow(UpgradeCommand copyCommand) {
        return locked(() -> copyAndVerifyUnsafe(copyCommand));
    }

    public UpgradeObjectEvidence copyEvidence(
            UpgradeCommand copyCommand, String relativePath) {
        return locked(() -> copyEvidenceUnsafe(copyCommand, relativePath));
    }

    public Path activeNamespacePath() {
        return namespace(read().activeNamespace());
    }

    private NamespaceVerification copyAndVerifyUnsafe(UpgradeCommand command) {
        UpgradeAuthorityRecord current = readUnsafe();
        if (command == null || command.kind() != UpgradeCommand.Kind.BEGIN_COPY) {
            throw new IllegalArgumentException("accepted BEGIN_COPY command is required");
        }
        UpgradeTransition replay = UpgradeAuthorityKernel.apply(
                current, command, 0L, 0L, 0L);
        if (current.phase() != UpgradePhase.COPYING
                || replay.code() != UpgradeTransition.Code.REPLAYED) {
            throw new IllegalStateException("shadow copy is not authorized");
        }
        Path source = namespace(current.sourceNamespace());
        Path shadow = namespace(current.shadowNamespace());
        requireDirectory(source, "source namespace");
        initializeDirectory(shadow, "shadow namespace");
        Scan before = scan(source);
        if (!before.digest.equals(current.sourceDigest())) {
            throw new IllegalStateException("source namespace digest drift");
        }
        long evidenceCount = 0L;
        for (Path relative : before.files) {
            String relativePath = canonicalRelativePath(relative);
            UpgradeObjectEvidence written = copyObject(
                    source, shadow, relative, relativePath, command);
            UpgradeObjectEvidence persisted = copyEvidenceUnsafe(
                    command, relativePath);
            if (!written.equals(persisted)) {
                throw new IllegalStateException("shadow copy evidence drift");
            }
            evidenceCount = Math.addExact(evidenceCount, 1L);
        }
        Scan sourceAfter = scan(source);
        Scan target = scan(shadow);
        if (!sourceAfter.digest.equals(before.digest)
                || !target.digest.equals(current.shadowDigest())
                || sourceAfter.objectCount != target.objectCount) {
            throw new IllegalStateException("shadow semantic verification failed");
        }
        return new NamespaceVerification(sourceAfter.objectCount,
                sourceAfter.shardCount, evidenceCount,
                sourceAfter.digest, target.digest);
    }

    private UpgradeObjectEvidence copyObject(
            Path sourceRoot, Path targetRoot, Path relative,
            String relativePath, UpgradeCommand command) {
        Path source = sourceRoot.resolve(relative).normalize();
        Path target = targetRoot.resolve(relative).normalize();
        if (!source.startsWith(sourceRoot) || !target.startsWith(targetRoot)) {
            throw new IllegalStateException("namespace object escaped its root");
        }
        try {
            Path parent = target.getParent();
            initializeDirectory(parent, "shadow object parent");
            Path temporary = parent.resolve(target.getFileName() + ".copying");
            if (Files.exists(temporary, LinkOption.NOFOLLOW_LINKS)) {
                requireRegularFile(temporary, "shadow temporary object");
                Files.delete(temporary);
            }
            Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
            forceFile(temporary);
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(parent);
            UpgradeObjectEvidence evidence = new UpgradeObjectEvidence(
                    UpgradeObjectEvidence.VERSION_1, command.generation(),
                    relativePath, digestFile(source), digestFile(target),
                    command.commandId(), command.commandDigest());
            writeCopyEvidence(command, evidence);
            return evidence;
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to copy shadow object", failure);
        }
    }

    private UpgradeObjectEvidence copyEvidenceUnsafe(
            UpgradeCommand command, String relativePath) {
        requireCopyCommand(command, readUnsafe());
        Path evidence = copyEvidencePath(command, relativePath);
        try {
            requireRegularFile(evidence, "shadow copy evidence");
            long length = Files.size(evidence);
            if (length <= 0L
                    || length > UpgradeObjectEvidenceCodec.MAX_FRAME_BYTES) {
                throw new IllegalStateException(
                        "shadow copy evidence frame length is invalid");
            }
            UpgradeObjectEvidence decoded = UpgradeObjectEvidenceCodec.decode(
                    Files.readAllBytes(evidence));
            if (decoded.generation() != command.generation()
                    || !decoded.relativePath().equals(relativePath)
                    || !decoded.commandId().equals(command.commandId())
                    || !decoded.commandDigest().equals(command.commandDigest())) {
                throw new IllegalStateException("shadow copy evidence differs");
            }
            return decoded;
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to read shadow copy evidence", failure);
        } catch (IllegalArgumentException corrupt) {
            throw new IllegalStateException("shadow copy evidence is corrupt", corrupt);
        }
    }

    private void writeCopyEvidence(
            UpgradeCommand command, UpgradeObjectEvidence evidence)
            throws IOException {
        Path path = copyEvidencePath(command, evidence.relativePath());
        Path parent = initializeCopyEvidenceDirectories(command, path);
        Path temporary = parent.resolve(path.getFileName() + ".tmp");
        if (Files.exists(temporary, LinkOption.NOFOLLOW_LINKS)) {
            requireRegularFile(temporary, "shadow copy evidence temporary file");
            Files.delete(temporary);
        }
        writeNewForced(temporary, UpgradeObjectEvidenceCodec.encode(evidence));
        Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        forceDirectory(parent);
    }

    private Path initializeCopyEvidenceDirectories(
            UpgradeCommand command, Path evidencePath) {
        initializeDirectory(copyEvidenceRoot, "shadow copy evidence root");
        Path generation = copyEvidenceRoot.resolve(
                Long.toString(command.generation()));
        initializeDirectory(generation, "shadow copy evidence generation");
        Path commandDirectory = generation.resolve(command.commandDigest());
        initializeDirectory(commandDirectory, "shadow copy evidence command");
        Path firstShard = commandDirectory.resolve(
                evidencePath.getParent().getParent().getFileName());
        initializeDirectory(firstShard, "shadow copy evidence first shard");
        Path secondShard = firstShard.resolve(
                evidencePath.getParent().getFileName());
        initializeDirectory(secondShard, "shadow copy evidence second shard");
        if (!secondShard.equals(evidencePath.getParent())) {
            throw new IllegalStateException("shadow copy evidence path differs");
        }
        return secondShard;
    }

    private Path copyEvidencePath(
            UpgradeCommand command, String relativePath) {
        String canonical = canonicalRelativePath(
                root.getFileSystem().getPath(relativePath));
        String identity = UpgradeCodecSupport.sha256Hex(
                canonical.getBytes(StandardCharsets.UTF_8));
        Path commandRoot = copyEvidenceRoot
                .resolve(Long.toString(command.generation()))
                .resolve(command.commandDigest());
        Path path = commandRoot.resolve(identity.substring(0, 2))
                .resolve(identity.substring(2, 4)).resolve(identity + ".frame")
                .normalize();
        if (!path.startsWith(commandRoot)) {
            throw new IllegalArgumentException("copy evidence escaped its root");
        }
        return path;
    }

    private static void requireCopyCommand(
            UpgradeCommand command, UpgradeAuthorityRecord current) {
        if (command == null || command.kind() != UpgradeCommand.Kind.BEGIN_COPY
                || UpgradeAuthorityKernel.apply(current, command, 0L, 0L, 0L)
                .code() != UpgradeTransition.Code.REPLAYED) {
            throw new IllegalStateException("accepted BEGIN_COPY command is required");
        }
    }

    private static String canonicalRelativePath(Path path) {
        if (path == null || path.isAbsolute() || path.getNameCount() == 0
                || !path.normalize().equals(path)) {
            throw new IllegalArgumentException("object relative path is invalid");
        }
        String raw = path.toString();
        if (raw.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("object relative path is invalid");
        }
        String value = raw.replace('\\', '/');
        UpgradeCodecSupport.requireText(value, "object relative path");
        if (value.startsWith("/") || value.endsWith("/")
                || value.contains("//") || value.contains("/../")
                || value.startsWith("../") || value.equals("..")) {
            throw new IllegalArgumentException("object relative path is invalid");
        }
        return value;
    }

    private static String digestFile(Path path) throws IOException {
        MessageDigest digest = sha256();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return hex(digest.digest());
    }

    private UpgradeAuthorityRecord readUnsafe() {
        try {
            requireRegularFile(authorityFile, "upgrade authority");
            long length = Files.size(authorityFile);
            if (length <= 0L || length > UpgradeAuthorityRecordCodec.MAX_FRAME_BYTES) {
                throw new IllegalStateException("upgrade authority frame length is invalid");
            }
            return UpgradeAuthorityRecordCodec.decode(Files.readAllBytes(authorityFile));
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to read upgrade authority", failure);
        } catch (IllegalArgumentException corrupt) {
            throw new IllegalStateException("upgrade authority is corrupt", corrupt);
        }
    }

    private void writeHeaderOnce(StoreFormatHeader header) {
        byte[] encoded = StoreFormatHeaderCodec.encode(header);
        try {
            if (Files.exists(headerFile, LinkOption.NOFOLLOW_LINKS)) {
                requireRegularFile(headerFile, "store header");
                StoreFormatHeader existing = StoreFormatHeaderCodec.decode(
                        Files.readAllBytes(headerFile));
                if (!existing.equals(header)) {
                    throw new IllegalStateException("store header already differs");
                }
                return;
            }
            writeNewForced(headerFile, encoded);
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to write store header", failure);
        }
    }

    private void writeAuthority(UpgradeAuthorityRecord record, boolean notify) {
        byte[] encoded = UpgradeAuthorityRecordCodec.encode(record);
        Path temporary = metadata.resolve("upgrade-authority.tmp");
        try {
            if (Files.exists(temporary, LinkOption.NOFOLLOW_LINKS)) {
                requireRegularFile(temporary, "authority temporary file");
                Files.delete(temporary);
            }
            writeNewForced(temporary, encoded);
            Files.move(temporary, authorityFile, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(metadata);
            if (notify) {
                observer.at(PersistencePoint.AFTER_AUTHORITY_REPLACE_BEFORE_ACK);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to persist upgrade authority", failure);
        }
    }

    private Scan scan(Path namespace) {
        final List<Path> files = new ArrayList<Path>();
        final int[] shards = {0};
        try {
            Files.walkFileTree(namespace, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(
                        Path directory, BasicFileAttributes attributes) {
                    if (attributes.isSymbolicLink()) {
                        throw new IllegalStateException("symbolic-link directory is forbidden");
                    }
                    if (!directory.equals(namespace)
                            && directory.getParent().equals(namespace)) {
                        shards[0] = Math.addExact(shards[0], 1);
                    }
                    requireDirectoryEntryBound(directory);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(
                        Path file, BasicFileAttributes attributes) {
                    if (!attributes.isRegularFile() || attributes.isSymbolicLink()) {
                        throw new IllegalStateException("namespace contains non-regular object");
                    }
                    if (files.size() == MAX_OBJECTS) {
                        throw new IllegalStateException("namespace object limit exceeded");
                    }
                    files.add(namespace.relativize(file));
                    return FileVisitResult.CONTINUE;
                }
            });
            Collections.sort(files, Comparator.comparing(Path::toString));
            MessageDigest digest = sha256();
            byte[] buffer = new byte[64 * 1024];
            for (Path relative : files) {
                byte[] name = relative.toString().replace('\\', '/')
                        .getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(4).putInt(name.length).array());
                digest.update(name);
                Path file = namespace.resolve(relative);
                long length = Files.size(file);
                digest.update(ByteBuffer.allocate(8).putLong(length).array());
                try (InputStream input = Files.newInputStream(file)) {
                    int read;
                    while ((read = input.read(buffer)) != -1) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return new Scan(files, files.size(), shards[0], hex(digest.digest()));
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to scan namespace", failure);
        } catch (ArithmeticException exhausted) {
            throw new IllegalStateException("namespace shard count exhausted", exhausted);
        }
    }

    private static void requireDirectoryEntryBound(Path directory) {
        int entries = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path ignored : stream) {
                entries++;
                if (entries > MAX_ENTRIES_PER_DIRECTORY) {
                    throw new IllegalStateException("directory entry limit exceeded");
                }
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to inspect namespace directory", failure);
        }
    }

    private void requireBootstrapIdentity(
            UpgradeAuthorityRecord record, StoreFormatHeader sourceHeader,
            StoreFormatHeader targetHeader, long generation,
            String sourceNamespace, String shadowNamespace, UpgradeOwner owner,
            String sourceDigest) {
        if (!record.sourceHeader().equals(sourceHeader)
                || !record.targetHeader().equals(targetHeader)
                || record.generation() != generation
                || !record.sourceNamespace().equals(sourceNamespace)
                || !record.shadowNamespace().equals(shadowNamespace)
                || !record.owner().equals(owner)
                || !record.sourceDigest().equals(sourceDigest)) {
            throw new IllegalStateException("existing upgrade bootstrap differs");
        }
    }

    private Path namespace(String name) {
        validateNamespace(name);
        Path resolved = namespaces.resolve(name).normalize();
        if (!resolved.getParent().equals(namespaces)) {
            throw new IllegalArgumentException("namespace escaped its root");
        }
        return resolved;
    }

    private static void validateNamespace(String name) {
        UpgradeCodecSupport.requireText(name, "namespace");
        if (name.equals(".") || name.equals("..") || name.indexOf('/') >= 0
                || name.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("namespace name is invalid");
        }
    }

    private <T> T locked(Work<T> work) {
        jvmLock.lock();
        try (FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            return work.run();
        } catch (IOException failure) {
            throw new UncheckedIOException("upgrade authority lock failed", failure);
        } finally {
            jvmLock.unlock();
        }
    }

    private static void initializeDirectory(Path directory, String label) {
        try {
            if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
                requireDirectory(directory, label);
                return;
            }
            Files.createDirectories(directory);
            requireDirectory(directory, label);
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
                forceDirectory(parent);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to initialize " + label, failure);
        }
    }

    private static void requireDirectory(Path path, String label) {
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(path)) {
            throw new IllegalStateException(label + " is not an owned directory");
        }
    }

    private static void requireRegularFile(Path path, String label) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(path)) {
            throw new IllegalStateException(label + " is not an owned regular file");
        }
    }

    private static void writeNewForced(Path path, byte[] bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        forceDirectory(path.getParent());
    }

    private static void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            encoded[index * 2] = alphabet[value >>> 4];
            encoded[index * 2 + 1] = alphabet[value & 0x0f];
        }
        return new String(encoded);
    }

    private static String zeroDigest() {
        return "0000000000000000000000000000000000000000000000000000000000000000";
    }

    private interface Work<T> {
        T run();
    }

    private static final class Scan {
        private final List<Path> files;
        private final long objectCount;
        private final int shardCount;
        private final String digest;

        private Scan(List<Path> files, long objectCount, int shardCount,
                     String digest) {
            this.files = files;
            this.objectCount = objectCount;
            this.shardCount = shardCount;
            this.digest = digest;
        }
    }
}
