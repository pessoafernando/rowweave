package io.rowweave;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.InterruptibleChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** explicit secure or trusted-local path capability. */
class QualityEvidenceSecurePathTest {

    @Test
    void realLocalProviderSelectsAnExplicitAdmittedMode(@TempDir Path temp)
            throws Exception {
        Path domain = temp.resolve("domain");
        Files.createDirectories(domain);

        QualityEvidencePathAccess.Mode mode =
                QualityEvidencePathAccess.selectMode(domain, guard());

        assertTrue(mode == QualityEvidencePathAccess.Mode.SDS_V1
                || mode == QualityEvidencePathAccess.Mode.VERIFIED_FILE_V1);
    }

    @Test
    void trustedLocalFileStoreTypesAreAClosedSameBuildSet() {
        assertTrue(QualityEvidencePathAccess.isTrustedLocalFileStoreType("apfs"));
        assertTrue(QualityEvidencePathAccess.isTrustedLocalFileStoreType("ext4"));
        assertTrue(QualityEvidencePathAccess.isTrustedLocalFileStoreType("overlay"));
        assertFalse(QualityEvidencePathAccess.isTrustedLocalFileStoreType("nfs"));
        assertFalse(QualityEvidencePathAccess.isTrustedLocalFileStoreType("smbfs"));
        assertFalse(QualityEvidencePathAccess.isTrustedLocalFileStoreType("unknown"));
    }

    @Test
    void componentAndProofShapeHaveExactHardBounds() {
        assertDoesNotThrow(() -> QualityEvidencePathAccess.requireComponentCount(256));
        assertThrows(IllegalStateException.class,
                () -> QualityEvidencePathAccess.requireComponentCount(257));

        Field[] fields = java.util.Arrays.stream(
                        QualityEvidencePathAccess.ComponentProof.class
                                .getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toArray(Field[]::new);
        assertEquals(5, fields.length);
        for (Field field : fields) {
            assertTrue(field.getType().isPrimitive(), field::toString);
        }
    }

    @Test
    void actualOversizedPathFailsBeforeAnyMetadataOrOpen() throws Exception {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < 253; i++) path.append('/').append('x');
        TrackingPlatform platform = new TrackingPlatform(false);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> open(Paths.get(path.toString()), guard(), platform));

        assertTrue(failure.getMessage().contains("component count"));
        assertEquals(0, platform.operations.get());
    }

    @Test
    void actualMaximumPathOpensWithBoundedProof() throws Exception {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < 252; i++) path.append('/').append('x');
        TrackingPlatform platform = new TrackingPlatform(false);

        AutoCloseable opened = open(Paths.get(path.toString()), guard(), platform);

        opened.close();
        assertEquals(0, platform.openDirectories.get());
        assertEquals(0, platform.openChannels.get());
    }

    @Test
    void verifiedModeRejectsNestedDeviceAndVisibleAcl() throws Exception {
        TrackingPlatform nested = new TrackingPlatform(false);
        nested.nestedDevice = true;
        assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), nested));
        assertEquals(0, nested.openFileCalls.get());

        TrackingPlatform acl = new TrackingPlatform(false);
        acl.visibleAcl = true;
        assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), acl));
        assertEquals(0, acl.openFileCalls.get());

        TrackingPlatform remote = new TrackingPlatform(false);
        remote.fileStoreType = "nfs";
        assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), remote));
        assertEquals(0, remote.openFileCalls.get());
    }

    @Test
    void verifiedModeDetectsPostOpenIdentityDriftAndChecksCapabilityDeadline()
            throws Exception {
        TrackingPlatform drift = new TrackingPlatform(false);
        drift.driftInodeAfterOpen = true;
        AutoCloseable opened = open(Paths.get("/trusted/domain"), guard(), drift);
        Method verify = opened.getClass().getDeclaredMethod("verifyUnchanged");
        verify.setAccessible(true);
        InvocationTargetException changed = assertThrows(
                InvocationTargetException.class, () -> verify.invoke(opened));
        assertTrue(changed.getCause() instanceof IllegalStateException);
        opened.close();

        TrackingPlatform deadline = new TrackingPlatform(false);
        AtomicInteger fileStoreChecks = new AtomicInteger();
        QualityEvidenceReadGuard cancelAfterFileStore =
                QualityEvidenceReadGuard.until(
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(30L), () ->
                                stackContains("QualityEvidencePathAccess",
                                        "guardedGetFileStore")
                                        && fileStoreChecks.incrementAndGet() == 2);
        QualityEvidenceReadGuard.Terminated termination = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> open(Paths.get("/trusted/domain"),
                        cancelAfterFileStore, deadline));
        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                termination.termination());
        assertEquals(0, deadline.openDirectories.get());
        assertEquals(0, deadline.openChannels.get());
    }

    @Test
    void secureModeNeverFallsBackAndClosesFailedTraversal() throws Exception {
        TrackingPlatform platform = new TrackingPlatform(true);
        platform.nonSecureRootAt = 2;

        assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), platform));

        assertEquals(0, platform.openDirectories.get());
        assertEquals(0, platform.openChannels.get());
        assertEquals(0, platform.openFileCalls.get(),
                "SDS failure must not switch to verified lexical open");
    }

    @Test
    void secureModeBoundsDescriptorsAndClosesPostOpenCancellation()
            throws Exception {
        TrackingPlatform success = new TrackingPlatform(true);
        AutoCloseable opened = open(Paths.get("/trusted/domain"), guard(), success);
        assertTrue(success.maxOpenDirectories.get() <= 2);
        assertTrue(success.maxOpenChannels.get() <= 1);
        opened.close();
        assertEquals(0, success.openDirectories.get());
        assertEquals(0, success.openChannels.get());

        TrackingPlatform cancelled = new TrackingPlatform(true);
        cancelled.closeFailureDepths.add(4);
        AtomicInteger registrationChecks = new AtomicInteger();
        QualityEvidenceReadGuard cancelAtRegistration =
                QualityEvidenceReadGuard.until(
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(30L), () ->
                                stackContains("QualityEvidencePathAccess",
                                        "registerOpened")
                                        && registrationChecks.incrementAndGet() == 2);
        QualityEvidenceReadGuard.Terminated termination = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> open(Paths.get("/trusted/domain"),
                        cancelAtRegistration, cancelled));
        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                termination.termination());
        assertEquals(1, termination.getSuppressed().length,
                "directory close failure must be suppressed by cancellation");
        assertEquals(0, cancelled.openDirectories.get());
        assertEquals(0, cancelled.openChannels.get());
    }

    @Test
    void secureAndMemberFailureMatrixClosesEveryResource() throws Exception {
        TrackingPlatform rootOpen = new TrackingPlatform(true);
        rootOpen.throwRootOpenAt = 1;
        assertThrows(IOException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), rootOpen));
        assertClosed(rootOpen);

        TrackingPlatform rootClose = new TrackingPlatform(true);
        rootClose.closeFailureDepths.add(0);
        assertThrows(IOException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), rootClose));
        assertClosed(rootClose);

        TrackingPlatform childOpen = new TrackingPlatform(true);
        childOpen.throwChildOpenAt = 2;
        assertThrows(IOException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), childOpen));
        assertClosed(childOpen);

        TrackingPlatform parentAndChildClose = new TrackingPlatform(true);
        parentAndChildClose.closeFailureRootCall = 2;
        parentAndChildClose.closeFailureDepths.add(0);
        parentAndChildClose.closeFailureDepths.add(1);
        IOException nestedClose = assertThrows(IOException.class,
                () -> open(Paths.get("/trusted/domain"), guard(),
                        parentAndChildClose));
        assertEquals(1, nestedClose.getSuppressed().length);
        assertClosed(parentAndChildClose);

        TrackingPlatform memberOpen = new TrackingPlatform(true);
        memberOpen.throwMemberOpen = true;
        assertThrows(IOException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), memberOpen));
        assertClosed(memberOpen);

        TrackingPlatform leafClose = new TrackingPlatform(true);
        leafClose.closeFailureDepths.add(4);
        assertThrows(IOException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), leafClose));
        assertClosed(leafClose);

        TrackingPlatform nonInterruptible = new TrackingPlatform(false);
        nonInterruptible.nonInterruptibleChannel = true;
        assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(),
                        nonInterruptible));
        assertClosed(nonInterruptible);

        TrackingPlatform sizeMismatch = new TrackingPlatform(false);
        sizeMismatch.channelSize = 1L;
        assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), sizeMismatch));
        assertClosed(sizeMismatch);

        TrackingPlatform channelClose = new TrackingPlatform(false);
        channelClose.channelSize = 1L;
        channelClose.failChannelClose = true;
        IllegalStateException mismatch = assertThrows(IllegalStateException.class,
                () -> open(Paths.get("/trusted/domain"), guard(), channelClose));
        assertEquals(1, mismatch.getSuppressed().length);
        assertClosed(channelClose);
    }

    @Test
    void everyMetadataAndOpenWrapperHasPostOperationCancellationCleanup()
            throws Exception {
        assertCheckpoint("guardedRealPath", false, false);
        assertCheckpoint("guardedProcessUid", false, false);
        assertCheckpoint("guardedBasicAttributes", false, false);
        assertCheckpoint("guardedUnixNumber", false, false);
        assertCheckpoint("guardedAclView", false, false);
        assertCheckpoint("guardedAclEntries", false, true);
        assertCheckpoint("guardedNewDirectoryStream", false, false);
        assertCheckpoint("guardedGetFileStore", false, false);
        assertCheckpoint("registerOpened", false, false);
        assertCheckpoint("guardedChannelSize", false, false);
        assertCheckpoint("guardedChildDirectoryStream", true, false);
    }

    private static void assertCheckpoint(
            String method, boolean secure, boolean visibleAcl) {
        TrackingPlatform platform = new TrackingPlatform(secure);
        platform.visibleAcl = visibleAcl;
        AtomicInteger checks = new AtomicInteger();
        QualityEvidenceReadGuard guard = QualityEvidenceReadGuard.until(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(30L), () ->
                        stackContains("QualityEvidencePathAccess", method)
                                && checks.incrementAndGet() == 2);
        QualityEvidenceReadGuard.Terminated terminated = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> open(Paths.get("/trusted/domain"), guard, platform), method);
        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                terminated.termination(), method);
        assertClosed(platform);
    }

    private static void assertClosed(TrackingPlatform platform) {
        assertEquals(0, platform.openDirectories.get(), "directory leak");
        assertEquals(0, platform.openChannels.get(), "channel leak");
    }

    private static QualityEvidenceReadGuard guard() {
        return QualityEvidenceReadGuard.until(System.nanoTime()
                + TimeUnit.SECONDS.toNanos(30L), () -> false);
    }

    private static boolean stackContains(String classSuffix, String method) {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (frame.getClassName().endsWith(classSuffix)
                    && method.equals(frame.getMethodName())) return true;
        }
        return false;
    }

    private static AutoCloseable open(
            Path domain, QualityEvidenceReadGuard guard,
            TrackingPlatform platform) throws Exception {
        Class<?> accessOps = Class.forName(
                "io.rowweave.QualityEvidencePathAccess$AccessOps");
        Object proxy = Proxy.newProxyInstance(accessOps.getClassLoader(),
                new Class<?>[]{accessOps}, platform::invoke);
        Method open = QualityEvidencePathAccess.class.getDeclaredMethod(
                "open", Path.class, long.class, String.class, long.class,
                QualityEvidenceReadGuard.class, accessOps);
        open.setAccessible(true);
        try {
            return (AutoCloseable) open.invoke(null, domain, 1L,
                    "rejected", 0L, guard, proxy);
        } catch (InvocationTargetException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception) throw (Exception) cause;
            throw (Error) cause;
        }
    }

    private static final class TrackingPlatform {
        private final boolean secure;
        private final AtomicInteger operations = new AtomicInteger();
        private final AtomicInteger openDirectories = new AtomicInteger();
        private final AtomicInteger maxOpenDirectories = new AtomicInteger();
        private final AtomicInteger openChannels = new AtomicInteger();
        private final AtomicInteger maxOpenChannels = new AtomicInteger();
        private final AtomicInteger openFileCalls = new AtomicInteger();
        private final AtomicInteger rootCalls = new AtomicInteger();
        private final AtomicInteger childCalls = new AtomicInteger();
        private boolean nestedDevice;
        private boolean visibleAcl;
        private boolean driftInodeAfterOpen;
        private String fileStoreType = "ext4";
        private int nonSecureRootAt = -1;
        private final Set<Integer> closeFailureDepths = new HashSet<>();
        private int closeFailureRootCall = -1;
        private int throwRootOpenAt = -1;
        private int throwChildOpenAt = -1;
        private boolean throwMemberOpen;
        private boolean nonInterruptibleChannel;
        private boolean failChannelClose;
        private long channelSize;

        private TrackingPlatform(boolean secure) {
            this.secure = secure;
        }

        private Object invoke(Object ignored, Method method, Object[] args)
                throws Exception {
            operations.incrementAndGet();
            switch (method.getName()) {
                case "toRealPath":
                    return args[0];
                case "newDirectoryStream":
                    int rootCall = rootCalls.incrementAndGet();
                    if (rootCall == throwRootOpenAt) {
                        throw new IOException("injected root open failure");
                    }
                    return secure && rootCall != nonSecureRootAt
                            ? new TrackingSecureStream(this, rootCall, 0)
                            : new TrackingPlainStream(this);
                case "fileStore":
                    return new FixedFileStore(fileStoreType);
                case "readBasicAttributes":
                    return new FixedBasicAttributes(
                            ((Path) args[0]).toString().endsWith(".data"));
                case "unixAttribute": {
                    Path path = (Path) args[0];
                    String name = (String) args[1];
                    if ("dev".equals(name)) {
                        return nestedDevice && path.toString().contains("/data/")
                                ? 2L : 1L;
                    }
                    if ("ino".equals(name)) {
                        long ino = (long) path.toString().hashCode() & 0xffffffffL;
                        return driftInodeAfterOpen && openChannels.get() > 0
                                ? ino + 1L : ino;
                    }
                    if ("uid".equals(name)) return 501L;
                    if ("mode".equals(name)) return 0755L;
                    throw new AssertionError(name);
                }
                case "aclView":
                    return visibleAcl ? aclView() : null;
                case "openByteChannel":
                    openFileCalls.incrementAndGet();
                    if (throwMemberOpen) {
                        throw new IOException("injected member open failure");
                    }
                    return nonInterruptibleChannel
                            ? new NonInterruptibleChannel(this)
                            : new TrackingChannel(this);
                case "processUid":
                    return 501;
                default:
                    throw new AssertionError(method.getName());
            }
        }

        private void openedDirectory() {
            int current = openDirectories.incrementAndGet();
            maxOpenDirectories.accumulateAndGet(current, Math::max);
        }

        private void openedChannel() {
            int current = openChannels.incrementAndGet();
            maxOpenChannels.accumulateAndGet(current, Math::max);
        }
    }

    private static class TrackingPlainStream implements DirectoryStream<Path> {
        final TrackingPlatform platform;
        boolean closed;

        private TrackingPlainStream(TrackingPlatform platform) {
            this.platform = platform;
            platform.openedDirectory();
        }

        @Override public Iterator<Path> iterator() {
            return Collections.<Path>emptyList().iterator();
        }

        @Override public void close() throws IOException {
            if (!closed) {
                closed = true;
                platform.openDirectories.decrementAndGet();
            }
        }
    }

    private static final class TrackingSecureStream extends TrackingPlainStream
            implements SecureDirectoryStream<Path> {
        private final int depth;
        private final int rootCall;

        private TrackingSecureStream(
                TrackingPlatform platform, int rootCall, int depth) {
            super(platform);
            this.rootCall = rootCall;
            this.depth = depth;
        }

        @Override public SecureDirectoryStream<Path> newDirectoryStream(
                Path path, java.nio.file.LinkOption... options) throws IOException {
            int call = platform.childCalls.incrementAndGet();
            if (call == platform.throwChildOpenAt) {
                throw new IOException("injected child open failure");
            }
            return new TrackingSecureStream(platform, rootCall, depth + 1);
        }

        @Override public SeekableByteChannel newByteChannel(
                Path path, Set<? extends OpenOption> options,
                java.nio.file.attribute.FileAttribute<?>... attrs) throws IOException {
            if (platform.throwMemberOpen) {
                throw new IOException("injected member open failure");
            }
            return new TrackingChannel(platform);
        }

        @Override public void deleteFile(Path path) { throw new UnsupportedOperationException(); }
        @Override public void deleteDirectory(Path path) { throw new UnsupportedOperationException(); }
        @Override public void move(Path srcpath, SecureDirectoryStream<Path> targetdir,
                                   Path targetpath) { throw new UnsupportedOperationException(); }
        @Override public <V extends FileAttributeView> V getFileAttributeView(
                Class<V> type) { return null; }
        @Override public <V extends FileAttributeView> V getFileAttributeView(
                Path path, Class<V> type, java.nio.file.LinkOption... options) {
            return null;
        }

        @Override public void close() throws IOException {
            boolean first = !closed;
            super.close();
            if (first && platform.closeFailureDepths.contains(depth)
                    && (platform.closeFailureRootCall < 0
                    || platform.closeFailureRootCall == rootCall)) {
                throw new IOException("injected directory close failure");
            }
        }
    }

    private static final class TrackingChannel
            implements SeekableByteChannel, InterruptibleChannel {
        private final TrackingPlatform platform;
        private boolean open = true;

        private TrackingChannel(TrackingPlatform platform) {
            this.platform = platform;
            platform.openedChannel();
        }

        @Override public int read(ByteBuffer dst) { return -1; }
        @Override public int write(ByteBuffer src) { throw new UnsupportedOperationException(); }
        @Override public long position() { return 0L; }
        @Override public SeekableByteChannel position(long newPosition) { return this; }
        @Override public long size() { return platform.channelSize; }
        @Override public SeekableByteChannel truncate(long size) { return this; }
        @Override public boolean isOpen() { return open; }
        @Override public void close() throws IOException {
            if (open) {
                open = false;
                platform.openChannels.decrementAndGet();
                if (platform.failChannelClose) {
                    throw new IOException("injected channel close failure");
                }
            }
        }
    }

    private static final class NonInterruptibleChannel
            implements SeekableByteChannel {
        private final TrackingChannel delegate;
        private NonInterruptibleChannel(TrackingPlatform platform) {
            delegate = new TrackingChannel(platform);
        }
        @Override public int read(ByteBuffer dst) { return -1; }
        @Override public int write(ByteBuffer src) { throw new UnsupportedOperationException(); }
        @Override public long position() { return 0L; }
        @Override public SeekableByteChannel position(long newPosition) { return this; }
        @Override public long size() { return delegate.platform.channelSize; }
        @Override public SeekableByteChannel truncate(long size) { return this; }
        @Override public boolean isOpen() { return delegate.isOpen(); }
        @Override public void close() throws IOException { delegate.close(); }
    }

    private static final class FixedBasicAttributes implements BasicFileAttributes {
        private final boolean file;
        private FixedBasicAttributes(boolean file) { this.file = file; }
        @Override public java.nio.file.attribute.FileTime lastModifiedTime() { return java.nio.file.attribute.FileTime.fromMillis(1L); }
        @Override public java.nio.file.attribute.FileTime lastAccessTime() { return lastModifiedTime(); }
        @Override public java.nio.file.attribute.FileTime creationTime() { return lastModifiedTime(); }
        @Override public boolean isRegularFile() { return file; }
        @Override public boolean isDirectory() { return !file; }
        @Override public boolean isSymbolicLink() { return false; }
        @Override public boolean isOther() { return false; }
        @Override public long size() { return 0L; }
        @Override public Object fileKey() { return null; }
    }

    private static final class FixedFileStore extends FileStore {
        private final String type;
        private FixedFileStore(String type) { this.type = type; }
        @Override public String name() { return "fixed"; }
        @Override public String type() { return type; }
        @Override public boolean isReadOnly() { return true; }
        @Override public long getTotalSpace() { return 0L; }
        @Override public long getUsableSpace() { return 0L; }
        @Override public long getUnallocatedSpace() { return 0L; }
        @Override public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) { return true; }
        @Override public boolean supportsFileAttributeView(String name) { return true; }
        @Override public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) { return null; }
        @Override public Object getAttribute(String attribute) { throw new UnsupportedOperationException(); }
    }

    private static AclFileAttributeView aclView() {
        UserPrincipal principal = () -> "intruder";
        AclEntry entry = AclEntry.newBuilder().setType(AclEntryType.ALLOW)
                .setPrincipal(principal).setPermissions(AclEntryPermission.WRITE_DATA)
                .build();
        return new AclFileAttributeView() {
            @Override public String name() { return "acl"; }
            @Override public java.util.List<AclEntry> getAcl() {
                return Collections.singletonList(entry);
            }
            @Override public void setAcl(java.util.List<AclEntry> acl) {
                throw new UnsupportedOperationException();
            }
            @Override public UserPrincipal getOwner() { return principal; }
            @Override public void setOwner(UserPrincipal owner) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
