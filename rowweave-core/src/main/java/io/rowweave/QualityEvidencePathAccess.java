package io.rowweave;

import java.io.IOException;
import java.nio.channels.InterruptibleChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.AclEntry;
import java.util.List;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** same-build path capability and stable opened member for evidence reads. */
final class QualityEvidencePathAccess {
    static final int MAX_PATH_COMPONENTS = 256;
    private static final int GROUP_OR_OTHER_WRITE = 0022;
    private static final Set<OpenOption> READ_NOFOLLOW = readNoFollow();
    private static final AccessOps SYSTEM = new SystemAccessOps();

    enum Mode {
        SDS_V1,
        VERIFIED_FILE_V1
    }

    private QualityEvidencePathAccess() {
    }

    static Mode selectMode(Path domain, QualityEvidenceReadGuard guard)
            throws IOException {
        return selectMode(domain, guard, SYSTEM);
    }

    private static Mode selectMode(
            Path domain, QualityEvidenceReadGuard guard, AccessOps ops)
            throws IOException {
        Path canonical = guardedRealPath(domain, guard, ops);
        Path root = canonical.getRoot();
        if (root == null) throw invalid("evidence domain has no filesystem root");
        try (DirectoryStream<Path> stream = guardedNewDirectoryStream(
                root, guard, ops)) {
            if (stream instanceof SecureDirectoryStream) return Mode.SDS_V1;
        }
        if (canonical.getFileSystem() != FileSystems.getDefault()
                || !"file".equals(canonical.getFileSystem().provider().getScheme())) {
            throw invalid("evidence filesystem is not the default file provider");
        }
        FileStore store = guardedGetFileStore(canonical, guard, ops);
        if (!isTrustedLocalFileStoreType(store.type())) {
            throw invalid("evidence filesystem lacks trusted-local capability");
        }
        // Capability requires the primitive Unix identity view, not an opaque fileKey.
        guardedUnixNumber(canonical, "dev", guard, ops);
        guardedUnixNumber(canonical, "ino", guard, ops);
        guardedUnixInt(canonical, "uid", guard, ops);
        guardedUnixInt(canonical, "mode", guard, ops);
        return Mode.VERIFIED_FILE_V1;
    }

    static boolean isTrustedLocalFileStoreType(String type) {
        if (type == null) return false;
        switch (type.toLowerCase(Locale.ROOT)) {
            case "apfs":
            case "hfs":
            case "ext4":
            case "xfs":
            case "btrfs":
            case "overlay":
            case "tmpfs":
                return true;
            default:
                return false;
        }
    }

    static void requireComponentCount(int count) {
        if (count < 1 || count > MAX_PATH_COMPONENTS) {
            throw invalid("evidence path component count exceeds 256");
        }
    }

    static OpenedMember open(
            Path frozenDomain, long generation, String memberName,
            long expectedLength, QualityEvidenceReadGuard guard) throws IOException {
        return open(frozenDomain, generation, memberName, expectedLength,
                guard, SYSTEM);
    }

    static OpenedMember open(
            Path frozenDomain, long generation, String memberName,
            long expectedLength, QualityEvidenceReadGuard guard,
            AccessOps ops) throws IOException {
        if (frozenDomain == null || !frozenDomain.isAbsolute()
                || !frozenDomain.equals(frozenDomain.normalize())) {
            throw invalid("evidence domain identity is noncanonical");
        }
        if (generation <= 0L || memberName == null
                || !memberName.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
            throw invalid("evidence member identity is noncanonical");
        }
        int components = frozenDomain.getNameCount() + 4;
        requireComponentCount(components);
        Path member = frozenDomain.resolve("data")
                .resolve(Long.toString(generation))
                .resolve(memberName + ".data");
        guard.check();
        if (!guardedRealPath(frozenDomain, guard, ops).equals(frozenDomain)) {
            throw invalid("evidence domain differs from its frozen identity");
        }
        int processUid = guardedProcessUid(guard, ops);
        ComponentProof[] before = snapshot(
                member, frozenDomain, processUid, guard, ops);
        Mode mode = selectMode(frozenDomain, guard, ops);
        guard.check();
        SeekableByteChannel channel = mode == Mode.SDS_V1
                ? openSecure(frozenDomain, generation, memberName, guard, ops)
                : openVerified(member, guard, ops);
        InterruptibleChannel interruptible = (InterruptibleChannel) channel;
        try {
            if (guardedChannelSize(channel, guard) != expectedLength) {
                throw invalid("evidence member length differs after open");
            }
            guard.check();
            return new OpenedMember(channel, interruptible, guard, member,
                    frozenDomain, processUid, before, ops);
        } catch (RuntimeException | IOException failure) {
            guard.unregister(interruptible);
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static SeekableByteChannel openSecure(
            Path domain, long generation, String memberName,
            QualityEvidenceReadGuard guard, AccessOps ops) throws IOException {
        Path root = domain.getRoot();
        SecureDirectoryStream<Path> current = requireSecure(
                guardedNewDirectoryStream(root, guard, ops));
        try {
            for (Path component : root.relativize(domain)) {
                current = descend(current, component, guard);
            }
            current = descend(current,
                    domain.getFileSystem().getPath("data"), guard);
            current = descend(current, domain.getFileSystem().getPath(
                    Long.toString(generation)), guard);
            Path leaf = domain.getFileSystem().getPath(memberName + ".data");
            SecureDirectoryStream<Path> leafDirectory = current;
            current = null;
            return openLeaf(leafDirectory, leaf, guard);
        } catch (RuntimeException | IOException failure) {
            closePreserving(current, failure);
            throw failure;
        }
    }

    private static SeekableByteChannel openVerified(
            Path member, QualityEvidenceReadGuard guard, AccessOps ops)
            throws IOException {
        guard.check();
        SeekableByteChannel channel = ops.openByteChannel(member, READ_NOFOLLOW);
        return registerOpened(channel, guard);
    }

    private static SeekableByteChannel openLeaf(
            SecureDirectoryStream<Path> directory, Path leaf,
            QualityEvidenceReadGuard guard) throws IOException {
        SeekableByteChannel channel = null;
        try (SecureDirectoryStream<Path> ignored = directory) {
            guard.check();
            channel = directory.newByteChannel(leaf, READ_NOFOLLOW);
            return registerOpened(channel, guard);
        } catch (RuntimeException | IOException failure) {
            closeRegistered(channel, guard, failure);
            throw failure;
        }
    }

    private static SeekableByteChannel registerOpened(
            SeekableByteChannel channel, QualityEvidenceReadGuard guard)
            throws IOException {
        if (!(channel instanceof InterruptibleChannel)) {
            IllegalStateException failure = invalid(
                    "evidence member channel is not interruptible");
            closePreserving(channel, failure);
            throw failure;
        }
        InterruptibleChannel interruptible = (InterruptibleChannel) channel;
        try {
            guard.register(interruptible);
            guard.check();
            return channel;
        } catch (RuntimeException failure) {
            closeRegistered(channel, guard, failure);
            throw failure;
        }
    }

    private static SecureDirectoryStream<Path> descend(
            SecureDirectoryStream<Path> parent, Path component,
            QualityEvidenceReadGuard guard) throws IOException {
        guard.check();
        DirectoryStream<Path> childRaw = guardedChildDirectoryStream(
                parent, component, guard);
        SecureDirectoryStream<Path> child = requireSecure(childRaw);
        try {
            parent.close();
        } catch (IOException closeFailure) {
            try {
                child.close();
            } catch (IOException childFailure) {
                closeFailure.addSuppressed(childFailure);
            }
            throw closeFailure;
        }
        try {
            guard.check();
            return child;
        } catch (RuntimeException terminated) {
            try {
                child.close();
            } catch (IOException closeFailure) {
                terminated.addSuppressed(closeFailure);
            }
            throw terminated;
        }
    }

    private static SecureDirectoryStream<Path> requireSecure(
            DirectoryStream<Path> stream) {
        if (!(stream instanceof SecureDirectoryStream)) {
            IllegalStateException failure = invalid(
                    "selected SDS capability became unavailable");
            closePreserving(stream, failure);
            throw failure;
        }
        @SuppressWarnings("unchecked")
        SecureDirectoryStream<Path> secure = (SecureDirectoryStream<Path>) stream;
        return secure;
    }

    private static ComponentProof[] snapshot(
            Path member, Path domain, int processUid,
            QualityEvidenceReadGuard guard, AccessOps ops) throws IOException {
        int componentCount = member.getNameCount() + 1;
        requireComponentCount(componentCount);
        ComponentProof[] proof = new ComponentProof[componentCount];
        Path component = member.getRoot();
        int domainComponent = domain.getNameCount();
        long domainDevice = -1L;
        for (int i = 0; i < componentCount; i++) {
            BasicFileAttributes basic = guardedBasicAttributes(
                    component, guard, ops);
            byte kind;
            if (i + 1 == componentCount) {
                if (!basic.isRegularFile()) {
                    throw invalid("evidence member is not a regular file");
                }
                kind = 2;
            } else {
                if (!basic.isDirectory()) {
                    throw invalid("evidence path contains a non-directory component");
                }
                kind = 1;
            }
            long dev = guardedUnixNumber(component, "dev", guard, ops);
            long ino = guardedUnixNumber(component, "ino", guard, ops);
            int uid = guardedUnixInt(component, "uid", guard, ops);
            int mode = guardedUnixInt(component, "mode", guard, ops);
            if (i == domainComponent) domainDevice = dev;
            if (i > domainComponent && dev != domainDevice) {
                throw invalid("evidence member crosses the admitted domain filesystem");
            }
            if ((uid != 0 && uid != processUid) || (mode & GROUP_OR_OTHER_WRITE) != 0) {
                throw invalid("evidence path ownership or permissions are untrusted");
            }
            AclFileAttributeView acl = guardedAclView(component, guard, ops);
            if (acl != null && !guardedAclEntries(acl, guard).isEmpty()) {
                throw invalid("evidence path has an explicit ACL");
            }
            proof[i] = new ComponentProof(dev, ino, uid, mode, kind);
            guard.check();
            if (i + 1 < componentCount) {
                component = component.resolve(member.getName(i));
            }
        }
        if (!guardedRealPath(domain, guard, ops).equals(domain)) {
            throw invalid("evidence domain identity changed during proof");
        }
        return proof;
    }

    private static int systemProcessUid() {
        try {
            Class<?> type = Class.forName("com.sun.security.auth.module.UnixSystem");
            Object system = type.getConstructor().newInstance();
            long uid = ((Number) type.getMethod("getUid").invoke(system)).longValue();
            if (uid < 0L || uid > Integer.MAX_VALUE) {
                throw invalid("current OS UID exceeds integer bound");
            }
            return (int) uid;
        } catch (ReflectiveOperationException unavailable) {
            throw invalid("current OS UID is unavailable", unavailable);
        }
    }

    private static Path guardedRealPath(
            Path path, QualityEvidenceReadGuard guard, AccessOps ops)
            throws IOException {
        guard.check();
        Path result = ops.toRealPath(path);
        guard.check();
        return result;
    }

    private static DirectoryStream<Path> guardedNewDirectoryStream(
            Path path, QualityEvidenceReadGuard guard, AccessOps ops)
            throws IOException {
        guard.check();
        DirectoryStream<Path> stream = ops.newDirectoryStream(path);
        try {
            guard.check();
            return stream;
        } catch (RuntimeException failure) {
            closePreserving(stream, failure);
            throw failure;
        }
    }

    private static DirectoryStream<Path> guardedChildDirectoryStream(
            SecureDirectoryStream<Path> parent, Path component,
            QualityEvidenceReadGuard guard) throws IOException {
        guard.check();
        DirectoryStream<Path> stream = parent.newDirectoryStream(
                component, LinkOption.NOFOLLOW_LINKS);
        try {
            guard.check();
            return stream;
        } catch (RuntimeException failure) {
            closePreserving(stream, failure);
            throw failure;
        }
    }

    private static FileStore guardedGetFileStore(
            Path path, QualityEvidenceReadGuard guard, AccessOps ops)
            throws IOException {
        guard.check();
        FileStore store = ops.fileStore(path);
        guard.check();
        return store;
    }

    private static BasicFileAttributes guardedBasicAttributes(
            Path path, QualityEvidenceReadGuard guard, AccessOps ops)
            throws IOException {
        guard.check();
        BasicFileAttributes attributes = ops.readBasicAttributes(path);
        guard.check();
        return attributes;
    }

    private static long guardedUnixNumber(
            Path path, String name, QualityEvidenceReadGuard guard,
            AccessOps ops) throws IOException {
        guard.check();
        long number = ops.unixAttribute(path, name);
        guard.check();
        if (number < 0L) throw invalid("evidence Unix " + name + " is invalid");
        return number;
    }

    private static int guardedUnixInt(
            Path path, String name, QualityEvidenceReadGuard guard,
            AccessOps ops) throws IOException {
        long value = guardedUnixNumber(path, name, guard, ops);
        if (value > Integer.MAX_VALUE) {
            throw invalid("evidence Unix " + name + " exceeds integer bound");
        }
        return (int) value;
    }

    private static AclFileAttributeView guardedAclView(
            Path path, QualityEvidenceReadGuard guard, AccessOps ops) {
        guard.check();
        AclFileAttributeView view = ops.aclView(path);
        guard.check();
        return view;
    }

    private static List<AclEntry> guardedAclEntries(
            AclFileAttributeView view, QualityEvidenceReadGuard guard)
            throws IOException {
        guard.check();
        List<AclEntry> entries = view.getAcl();
        guard.check();
        return entries;
    }

    private static int guardedProcessUid(
            QualityEvidenceReadGuard guard, AccessOps ops) {
        guard.check();
        int uid = ops.processUid();
        guard.check();
        if (uid < 0) throw invalid("current OS UID is invalid");
        return uid;
    }

    private static long guardedChannelSize(
            SeekableByteChannel channel, QualityEvidenceReadGuard guard)
            throws IOException {
        guard.check();
        long size = channel.size();
        guard.check();
        return size;
    }

    private static void closeRegistered(
            SeekableByteChannel channel, QualityEvidenceReadGuard guard,
            Throwable failure) {
        if (channel == null) return;
        if (channel instanceof InterruptibleChannel) {
            guard.unregister((InterruptibleChannel) channel);
        }
        closePreserving(channel, failure);
    }

    private static void closePreserving(
            AutoCloseable resource, Throwable failure) {
        if (resource == null) return;
        try {
            resource.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    interface AccessOps {
        Path toRealPath(Path path) throws IOException;
        DirectoryStream<Path> newDirectoryStream(Path path) throws IOException;
        FileStore fileStore(Path path) throws IOException;
        BasicFileAttributes readBasicAttributes(Path path) throws IOException;
        long unixAttribute(Path path, String name) throws IOException;
        AclFileAttributeView aclView(Path path);
        SeekableByteChannel openByteChannel(
                Path path, Set<OpenOption> options) throws IOException;
        int processUid();
    }

    private static final class SystemAccessOps implements AccessOps {
        @Override
        public Path toRealPath(Path path) throws IOException {
            return path.toRealPath();
        }

        @Override
        public DirectoryStream<Path> newDirectoryStream(Path path)
                throws IOException {
            return Files.newDirectoryStream(path);
        }

        @Override
        public FileStore fileStore(Path path) throws IOException {
            return Files.getFileStore(path);
        }

        @Override
        public BasicFileAttributes readBasicAttributes(Path path)
                throws IOException {
            return Files.readAttributes(path, BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS);
        }

        @Override
        public long unixAttribute(Path path, String name) throws IOException {
            Object value = Files.getAttribute(
                    path, "unix:" + name, LinkOption.NOFOLLOW_LINKS);
            if (!(value instanceof Number)) {
                throw invalid("evidence path lacks stable Unix " + name);
            }
            return ((Number) value).longValue();
        }

        @Override
        public AclFileAttributeView aclView(Path path) {
            return Files.getFileAttributeView(path, AclFileAttributeView.class,
                    LinkOption.NOFOLLOW_LINKS);
        }

        @Override
        public SeekableByteChannel openByteChannel(
                Path path, Set<OpenOption> options) throws IOException {
            return java.nio.channels.FileChannel.open(path, options);
        }

        @Override
        public int processUid() {
            return systemProcessUid();
        }
    }

    private static Set<OpenOption> readNoFollow() {
        Set<OpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.READ);
        options.add(LinkOption.NOFOLLOW_LINKS);
        return Collections.unmodifiableSet(options);
    }

    private static IllegalStateException invalid(String detail) {
        return new IllegalStateException("QUALITY_EVIDENCE_INVALID: " + detail);
    }

    private static IllegalStateException invalid(String detail, Throwable cause) {
        return new IllegalStateException("QUALITY_EVIDENCE_INVALID: " + detail, cause);
    }

    static final class ComponentProof {
        private final long dev;
        private final long ino;
        private final int uid;
        private final int mode;
        private final byte kind;

        private ComponentProof(long dev, long ino, int uid, int mode, byte kind) {
            this.dev = dev;
            this.ino = ino;
            this.uid = uid;
            this.mode = mode;
            this.kind = kind;
        }

        private boolean same(ComponentProof other) {
            return other != null && dev == other.dev && ino == other.ino
                    && uid == other.uid && mode == other.mode && kind == other.kind;
        }
    }

    static final class OpenedMember implements AutoCloseable {
        private final SeekableByteChannel channel;
        private final InterruptibleChannel interruptible;
        private final QualityEvidenceReadGuard guard;
        private final Path member;
        private final Path domain;
        private final int processUid;
        private final ComponentProof[] before;
        private final AccessOps ops;

        private OpenedMember(
                SeekableByteChannel channel, InterruptibleChannel interruptible,
                QualityEvidenceReadGuard guard, Path member, Path domain,
                int processUid, ComponentProof[] before, AccessOps ops) {
            this.channel = channel;
            this.interruptible = interruptible;
            this.guard = guard;
            this.member = member;
            this.domain = domain;
            this.processUid = processUid;
            this.before = before;
            this.ops = ops;
        }

        SeekableByteChannel channel() {
            return channel;
        }

        void verifyUnchanged() throws IOException {
            guard.check();
            ComponentProof[] after = snapshot(
                    member, domain, processUid, guard, ops);
            if (before.length != after.length) {
                throw invalid("evidence path proof length changed");
            }
            for (int i = 0; i < before.length; i++) {
                if (!before[i].same(after[i])) {
                    throw invalid("evidence path identity changed during read");
                }
            }
            guard.check();
        }

        @Override
        public void close() throws IOException {
            guard.unregister(interruptible);
            channel.close();
        }
    }
}
