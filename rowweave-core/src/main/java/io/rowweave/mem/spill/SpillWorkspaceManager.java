package io.rowweave.mem.spill;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Process-scoped spill owner, quota authority and bounded orphan recovery coordinator.
 */
public final class SpillWorkspaceManager implements AutoCloseable {

    static final String BASE_MARKER = ".rowweave-spill-base-v1";
    static final String OWNER_MARKER = ".rowweave-spill-owner-v1";
    static final String JOB_MARKER = ".rowweave-spill-job-v1";
    static final String LEASE_FILE = "lease.lock";

    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE);
    private static final Set<Path> ACTIVE_PROCESS_LEASES =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    private final SpillWorkspaceConfig config;
    private final LongSupplier clock;
    private final Supplier<String> identities;
    private final SpillUsableSpaceProbe usableSpaceProbe;
    private final String instanceId;
    private final Path baseDirectory;
    private final Path instancesDirectory;
    private final Path instanceDirectory;
    private final Path jobsDirectory;
    private final FileChannel leaseChannel;
    private final FileLock lease;
    private final Set<SpillJobWorkspace> jobs = new LinkedHashSet<>();
    private DirectoryStream<Path> scanStream;
    private Iterator<Path> scanIterator;
    private SpillScavengeReport lastScavengeReport = SpillScavengeReport.empty();
    private long lastMaintenanceMillis = Long.MIN_VALUE;
    private long reservedBytes;
    private int activeRuns;
    private boolean closed;

    private SpillWorkspaceManager(SpillWorkspaceConfig config, LongSupplier clock,
                                  Supplier<String> identities,
                                  SpillUsableSpaceProbe usableSpaceProbe,
                                  String instanceId, Path baseDirectory,
                                  Path instancesDirectory, Path instanceDirectory,
                                  Path jobsDirectory, FileChannel leaseChannel, FileLock lease) {
        this.config = config;
        this.clock = clock;
        this.identities = identities;
        this.usableSpaceProbe = usableSpaceProbe;
        this.instanceId = instanceId;
        this.baseDirectory = baseDirectory;
        this.instancesDirectory = instancesDirectory;
        this.instanceDirectory = instanceDirectory;
        this.jobsDirectory = jobsDirectory;
        this.leaseChannel = leaseChannel;
        this.lease = lease;
    }

    public static SpillWorkspaceManager open(SpillWorkspaceConfig config) {
        // Resolving the mount for every frame walks the OS mount table on macOS. The workspace
        // stays on one file store for its lease, while getUsableSpace() must remain fresh for
        // every reservation so the min-free guard is never served from a stale capacity sample.
        java.util.concurrent.atomic.AtomicReference<FileStore> store =
                new java.util.concurrent.atomic.AtomicReference<>();
        return openForTests(config, System::currentTimeMillis,
                () -> UUID.randomUUID().toString().replace("-", ""),
                path -> {
                    FileStore current = store.get();
                    if (current == null) {
                        FileStore resolved = Files.getFileStore(path);
                        store.compareAndSet(null, resolved);
                        current = store.get();
                    }
                    return current.getUsableSpace();
                });
    }

    static SpillWorkspaceManager openForTests(SpillWorkspaceConfig config,
                                              LongSupplier clock,
                                              Supplier<String> identities) {
        return openForTests(config, clock, identities,
                path -> Files.getFileStore(path).getUsableSpace());
    }

    static SpillWorkspaceManager openForTests(SpillWorkspaceConfig config,
                                              LongSupplier clock,
                                              Supplier<String> identities,
                                              SpillUsableSpaceProbe usableSpaceProbe) {
        if (config == null || clock == null || identities == null || usableSpaceProbe == null) {
            throw SpillDiskException.workspaceUnavailable(
                    new NullPointerException("spill workspace dependency"));
        }
        Path base = config.baseDirectory().toAbsolutePath().normalize();
        Path instances = base.resolve("instances");
        Path instance = null;
        Path jobs = null;
        Path registeredLease = null;
        FileChannel channel = null;
        FileLock lock = null;
        try {
            prepareBase(base);
            verifyOrPublishMarker(base.resolve(BASE_MARKER), SpillWorkspaceMarker.base(), true);
            createPrivateDirectory(instances);
            requireContainedDirectory(base, instances);

            String instanceId = identities.get();
            SpillWorkspaceMarker owner = SpillWorkspaceMarker.instance(instanceId, clock.getAsLong());
            instance = instances.resolve("instance-" + instanceId);
            createPrivateDirectoryExactly(instance);
            requireContainedDirectory(instances, instance);

            Path leasePath = instance.resolve(LEASE_FILE);
            channel = FileChannel.open(leasePath,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            setPrivateFilePermissions(leasePath);
            lock = channel.tryLock();
            if (lock == null) {
                throw new IOException("exclusive spill lease unavailable");
            }
            registeredLease = leasePath.toAbsolutePath().normalize();
            if (!ACTIVE_PROCESS_LEASES.add(registeredLease)) {
                throw new IOException("spill lease identity already active");
            }
            verifyOrPublishMarker(instance.resolve(OWNER_MARKER), owner, false);
            jobs = instance.resolve("jobs");
            createPrivateDirectoryExactly(jobs);
            requireContainedDirectory(instance, jobs);
            SpillWorkspaceManager manager = new SpillWorkspaceManager(
                    config, clock, identities, usableSpaceProbe, instanceId,
                    base, instances, instance, jobs, channel, lock);
            manager.runScavengePass();
            return manager;
        } catch (Throwable failure) {
            closeQuietly(lock);
            closeQuietly(channel);
            if (registeredLease != null) ACTIVE_PROCESS_LEASES.remove(registeredLease);
            cleanupIncompleteInstance(instance, jobs);
            if (failure instanceof SpillDiskException) {
                throw (SpillDiskException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw SpillDiskException.workspaceUnavailable(failure);
        }
    }

    public Path baseDirectory() {
        return baseDirectory;
    }

    public Path instanceDirectory() {
        return instanceDirectory;
    }

    public synchronized long reservedBytes() {
        return reservedBytes;
    }

    synchronized SpillWorkspaceUsageSnapshot usageSnapshot() {
        return new SpillWorkspaceUsageSnapshot(jobs.size(), activeRuns, reservedBytes);
    }

    public synchronized SpillJobWorkspace openJob() {
        if (closed) {
            throw new IllegalStateException("spill workspace manager is closed");
        }
        try {
            maintainIfDue();
            String jobId = identities.get();
            SpillWorkspaceMarker marker = SpillWorkspaceMarker.job(jobId, instanceId);
            Path directory = jobsDirectory.resolve("job-" + jobId);
            createPrivateDirectoryExactly(directory);
            requireContainedDirectory(jobsDirectory, directory);
            try {
                verifyOrPublishMarker(directory.resolve(JOB_MARKER), marker, false);
            } catch (Throwable failure) {
                deleteIfExists(directory.resolve(JOB_MARKER));
                deleteIfExists(directory);
                throw failure;
            }
            SpillJobWorkspace job = new SpillJobWorkspace(this, jobId, directory);
            jobs.add(job);
            return job;
        } catch (SpillDiskException e) {
            throw e;
        } catch (Throwable failure) {
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw SpillDiskException.workspaceUnavailable(failure);
        }
    }

    synchronized DiskSpillStore openSpillStore(SpillJobWorkspace job,
                                                SpillRunOutputFactory outputFactory) {
        requireActiveJob(job);
        Path directory = job.directory().resolve("op-" + job.nextStoreSequence());
        try {
            createPrivateDirectoryExactly(directory);
            requireContainedDirectory(job.directory(), directory);
            DiskSpillStore store = outputFactory == null
                    ? new DiskSpillStore(directory.toFile(), job)
                    : new DiskSpillStore(directory.toFile(), job, outputFactory);
            job.registerStore(store, directory);
            return store;
        } catch (SpillDiskException e) {
            deleteIfExists(directory);
            throw e;
        } catch (Throwable failure) {
            deleteIfExists(directory);
            if (failure instanceof Error) throw (Error) failure;
            throw SpillDiskException.workspaceUnavailable(failure);
        }
    }

    void reserve(SpillJobWorkspace job, long requestedBytes) {
        if (requestedBytes <= 0) {
            throw new IllegalArgumentException("spill reservation must be positive");
        }
        synchronized (this) {
            validateReservationQuota(job, requestedBytes);
        }
        // FileStore/statfs may block; keep it outside the process-wide quota monitor. The final
        // quota recheck and accounting below remain linearized under the same monitor.
        final long usable;
        try {
            usable = usableSpaceProbe.usableBytes(baseDirectory);
        } catch (IOException probeFailure) {
            throw SpillDiskException.workspaceUnavailable(probeFailure);
        }
        synchronized (this) {
            validateReservationQuota(job, requestedBytes);
            long minFree = config.minFreeBytes();
            if (usable < 0 || usable < minFree || requestedBytes > usable - minFree) {
                throw SpillDiskException.resourceLimit(
                        SpillFailureCode.SPILL_MIN_FREE_SPACE,
                        requestedBytes, Math.max(0, minFree - Math.max(usable, 0)),
                        Math.max(0, usable - minFree), digestPrefix(job), null);
            }
            job.addReservedBytes(requestedBytes);
            reservedBytes += requestedBytes;
        }
    }

    /** Caller holds this manager's monitor. */
    private void validateReservationQuota(SpillJobWorkspace job, long requestedBytes) {
        requireActiveJob(job);
        long jobUsed = job.rawReservedBytes();
        if (jobUsed > config.jobMaxBytes() - requestedBytes) {
            throw SpillDiskException.resourceLimit(
                    SpillFailureCode.SPILL_JOB_QUOTA_EXCEEDED,
                    requestedBytes, jobUsed, config.jobMaxBytes(), digestPrefix(job), null);
        }
        if (reservedBytes > config.workerMaxBytes() - requestedBytes) {
            throw SpillDiskException.resourceLimit(
                    SpillFailureCode.SPILL_WORKER_QUOTA_EXCEEDED,
                    requestedBytes, reservedBytes, config.workerMaxBytes(), digestPrefix(job), null);
        }
    }

    synchronized void release(SpillJobWorkspace job, long releasedBytes) {
        if (job == null || releasedBytes <= 0
                || releasedBytes > job.rawReservedBytes()
                || releasedBytes > reservedBytes) {
            throw new IllegalStateException("invalid spill reservation release");
        }
        job.subtractReservedBytes(releasedBytes);
        reservedBytes -= releasedBytes;
    }

    synchronized long reservedBytes(SpillJobWorkspace job) {
        if (job == null) throw new IllegalArgumentException("job must not be null");
        return job.rawReservedBytes();
    }

    synchronized void runOpened(SpillJobWorkspace job) {
        requireActiveJob(job);
        if (activeRuns == Integer.MAX_VALUE) {
            throw new IllegalStateException("active spill run count overflow");
        }
        activeRuns++;
    }

    synchronized void runClosed(SpillJobWorkspace job) {
        if (job == null || activeRuns <= 0) {
            throw new IllegalStateException("invalid active spill run close");
        }
        activeRuns--;
    }

    void closeJob(SpillJobWorkspace job) {
        synchronized (this) {
            if (job == null || !job.markClosed()) return;
        }
        Throwable failure = null;
        for (DiskSpillStore store : job.storesSnapshot()) {
            try {
                store.close();
            } catch (Throwable cleanup) {
                failure = addFailure(failure, cleanup);
            }
        }
        for (Path operatorDirectory : job.operatorDirectoriesSnapshot()) {
            try {
                requireOnlyEntries(operatorDirectory);
                deleteRequired(operatorDirectory);
            } catch (Throwable cleanup) {
                failure = addFailure(failure, cleanup);
            }
        }
        try {
            requireOnlyEntries(job.directory(), JOB_MARKER);
            deleteRequired(job.directory().resolve(JOB_MARKER));
            deleteRequired(job.directory());
        } catch (Throwable cleanup) {
            failure = cleanup;
        } finally {
            synchronized (this) {
                jobs.remove(job);
                notifyAll();
            }
        }
        if (job.rawReservedBytes() != 0) {
            failure = addFailure(failure,
                    new IOException("spill job closed with unreleased reservations"));
        }
        if (failure != null) {
            if (failure instanceof Error) throw (Error) failure;
            throw failure instanceof SpillDiskException
                    ? (SpillDiskException) failure
                    : SpillDiskException.cleanupFailed(failure);
        }
    }

    @Override
    public void close() {
        final java.util.List<SpillJobWorkspace> closing;
        synchronized (this) {
            if (closed) return;
            closed = true;
            closing = new ArrayList<>(jobs);
        }
        Throwable failure = null;
        try {
            closeScanCursor();
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        for (SpillJobWorkspace job : closing) {
            try {
                closeJob(job);
            } catch (Throwable cleanup) {
                failure = addFailure(failure, cleanup);
            }
        }
        boolean interrupted = false;
        synchronized (this) {
            while (!jobs.isEmpty()) {
                try {
                    wait();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        try {
            requireOnlyEntries(jobsDirectory);
            deleteRequired(jobsDirectory);
            requireOnlyEntries(instanceDirectory, OWNER_MARKER, LEASE_FILE);
            // Remove the authority marker while the lease is still held. A scavenger must preserve
            // an unmarked directory even after the channel is released.
            deleteRequired(instanceDirectory.resolve(OWNER_MARKER));
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        try {
            lease.release();
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        try {
            leaseChannel.close();
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        if (!lease.isValid() || !leaseChannel.isOpen()) {
            ACTIVE_PROCESS_LEASES.remove(instanceDirectory.resolve(LEASE_FILE)
                    .toAbsolutePath().normalize());
        }
        try {
            deleteRequired(instanceDirectory.resolve(LEASE_FILE));
            deleteRequired(instanceDirectory);
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        if (failure != null) {
            if (failure instanceof Error) throw (Error) failure;
            throw failure instanceof SpillDiskException
                    ? (SpillDiskException) failure
                    : SpillDiskException.cleanupFailed(failure);
        }
    }

    synchronized SpillScavengeReport lastScavengeReport() {
        return lastScavengeReport;
    }

    synchronized SpillScavengeReport scavengeNowForTests() {
        if (closed) throw new IllegalStateException("spill workspace manager is closed");
        return runScavengePass();
    }

    /** Simulates abrupt process death without granting normal cleanup authority to the test. */
    synchronized void abandonForTests() {
        if (closed) return;
        closed = true;
        Throwable failure = null;
        try {
            closeScanCursor();
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        try {
            lease.release();
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        try {
            leaseChannel.close();
        } catch (Throwable cleanup) {
            failure = addFailure(failure, cleanup);
        }
        if (!lease.isValid() || !leaseChannel.isOpen()) {
            ACTIVE_PROCESS_LEASES.remove(instanceDirectory.resolve(LEASE_FILE)
                    .toAbsolutePath().normalize());
        }
        if (failure != null) {
            throw SpillDiskException.cleanupFailed(failure);
        }
    }

    private void maintainIfDue() {
        long now = clock.getAsLong();
        long interval = config.maintenanceIntervalMillis();
        if (lastMaintenanceMillis == Long.MIN_VALUE
                || now < lastMaintenanceMillis
                || now - lastMaintenanceMillis >= interval) {
            runScavengePass();
        }
    }

    private SpillScavengeReport runScavengePass() {
        SpillScavengeReport.Builder report = new SpillScavengeReport.Builder(
                config.scanMaxEntries(), config.deleteMaxEntries());
        try {
            while (report.canInspect()) {
                Path candidate = nextScanEntry();
                if (candidate == null) break;
                report.inspect();
                String name = candidate.getFileName().toString();
                if (!name.matches("instance-[0-9a-f]{32}")) {
                    report.suspicious(null, "name");
                    continue;
                }
                String candidateId = name.substring("instance-".length());
                cleanCandidate(candidate, candidateId, report);
            }
        } catch (IOException | DirectoryIteratorException failure) {
            report.failure(null);
            closeScanCursorQuietly();
        }
        lastMaintenanceMillis = clock.getAsLong();
        lastScavengeReport = report.build();
        return lastScavengeReport;
    }

    private Path nextScanEntry() throws IOException {
        if (scanIterator == null) {
            requireDirectory(instancesDirectory);
            scanStream = Files.newDirectoryStream(instancesDirectory);
            scanIterator = scanStream.iterator();
        }
        try {
            if (scanIterator.hasNext()) return scanIterator.next();
            closeScanCursor();
            return null;
        } catch (DirectoryIteratorException failure) {
            closeScanCursorQuietly();
            throw failure;
        }
    }

    private void cleanCandidate(Path candidate, String candidateId,
                                SpillScavengeReport.Builder report) {
        try {
            if (Files.isSymbolicLink(candidate)
                    || !Files.isDirectory(candidate, LinkOption.NOFOLLOW_LINKS)) {
                report.suspicious(candidateId, "type");
                return;
            }
            if (!report.inspect()) return;
            SpillWorkspaceMarker owner = readMarker(candidate.resolve(OWNER_MARKER));
            if (owner.kind() != SpillWorkspaceMarker.Kind.INSTANCE
                    || !candidateId.equals(owner.instanceId())) {
                report.suspicious(candidateId, "owner");
                return;
            }
            long now = clock.getAsLong();
            long created = owner.createdAtMillis();
            if (now < 0 || created < 0 || now < created) {
                report.suspicious(candidateId, "time");
                return;
            }
            if (now - created < config.orphanGraceMillis()) {
                report.tooYoung(candidateId);
                return;
            }

            if (!report.inspect()) return;
            Path leasePath = candidate.resolve(LEASE_FILE);
            if (Files.isSymbolicLink(leasePath)
                    || !Files.isRegularFile(leasePath, LinkOption.NOFOLLOW_LINKS)) {
                report.suspicious(candidateId, "lease");
                return;
            }
            if (ACTIVE_PROCESS_LEASES.contains(leasePath.toAbsolutePath().normalize())) {
                report.live(candidateId);
                return;
            }
            try (FileChannel candidateChannel = FileChannel.open(
                    leasePath, StandardOpenOption.WRITE)) {
                FileLock candidateLock;
                try {
                    candidateLock = candidateChannel.tryLock();
                } catch (OverlappingFileLockException live) {
                    report.live(candidateId);
                    return;
                }
                if (candidateLock == null) {
                    report.live(candidateId);
                    return;
                }
                try {
                    CleanupResult result = cleanJobsTree(candidate, candidateId, report);
                    if (result != CleanupResult.CLEAN || !report.canDelete(3)) return;
                    deleteAuthorized(candidate.resolve(OWNER_MARKER), report);
                    candidateLock.release();
                    deleteAuthorized(leasePath, report);
                    deleteAuthorized(candidate, report);
                } finally {
                    if (candidateLock.isValid()) candidateLock.release();
                }
            }
        } catch (IOException | RuntimeException failure) {
            report.failure(candidateId);
            report.suspicious(candidateId, "failure");
        }
    }

    private CleanupResult cleanJobsTree(Path candidate, String candidateId,
                                        SpillScavengeReport.Builder report) throws IOException {
        if (!report.inspect()) return CleanupResult.INCOMPLETE;
        Path jobsPath = candidate.resolve("jobs");
        if (!Files.exists(jobsPath, LinkOption.NOFOLLOW_LINKS)) {
            return CleanupResult.CLEAN;
        }
        if (Files.isSymbolicLink(jobsPath)
                || !Files.isDirectory(jobsPath, LinkOption.NOFOLLOW_LINKS)) {
            report.suspicious(candidateId, "jobs");
            return CleanupResult.SUSPICIOUS;
        }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(jobsPath)) {
            for (Path jobPath : children) {
                if (!report.inspect()) return CleanupResult.INCOMPLETE;
                String name = jobPath.getFileName().toString();
                if (!name.matches("job-[0-9a-f]{32}")) {
                    report.suspicious(candidateId, "job-name");
                    return CleanupResult.SUSPICIOUS;
                }
                CleanupResult result = cleanJobTree(
                        jobPath, name.substring("job-".length()), candidateId, report);
                if (result != CleanupResult.CLEAN) return result;
            }
        }
        if (!report.canDelete(1)) return CleanupResult.INCOMPLETE;
        deleteAuthorized(jobsPath, report);
        return CleanupResult.CLEAN;
    }

    private CleanupResult cleanJobTree(Path jobPath, String jobId, String candidateId,
                                       SpillScavengeReport.Builder report) throws IOException {
        if (Files.isSymbolicLink(jobPath)
                || !Files.isDirectory(jobPath, LinkOption.NOFOLLOW_LINKS)) {
            report.suspicious(candidateId, "job-type");
            return CleanupResult.SUSPICIOUS;
        }
        if (!report.inspect()) return CleanupResult.INCOMPLETE;
        SpillWorkspaceMarker marker;
        try {
            marker = readMarker(jobPath.resolve(JOB_MARKER));
        } catch (RuntimeException | IOException invalid) {
            report.suspicious(candidateId, "job-owner");
            return CleanupResult.SUSPICIOUS;
        }
        if (marker.kind() != SpillWorkspaceMarker.Kind.JOB
                || !candidateId.equals(marker.instanceId())
                || !jobId.equals(marker.jobId())) {
            report.suspicious(candidateId, "job-owner");
            return CleanupResult.SUSPICIOUS;
        }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(jobPath)) {
            for (Path child : children) {
                if (!report.inspect()) return CleanupResult.INCOMPLETE;
                String name = child.getFileName().toString();
                if (JOB_MARKER.equals(name)) continue;
                if (!name.matches("op-[1-9][0-9]*")) {
                    report.suspicious(candidateId, "op-name");
                    return CleanupResult.SUSPICIOUS;
                }
                CleanupResult result = cleanOperatorTree(child, candidateId, report);
                if (result != CleanupResult.CLEAN) return result;
            }
        }
        if (!report.canDelete(2)) return CleanupResult.INCOMPLETE;
        deleteAuthorized(jobPath.resolve(JOB_MARKER), report);
        deleteAuthorized(jobPath, report);
        return CleanupResult.CLEAN;
    }

    private CleanupResult cleanOperatorTree(Path operatorPath, String candidateId,
                                            SpillScavengeReport.Builder report) throws IOException {
        if (Files.isSymbolicLink(operatorPath)
                || !Files.isDirectory(operatorPath, LinkOption.NOFOLLOW_LINKS)) {
            report.suspicious(candidateId, "op-type");
            return CleanupResult.SUSPICIOUS;
        }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(operatorPath)) {
            for (Path runPath : children) {
                if (!report.inspect()) return CleanupResult.INCOMPLETE;
                String name = runPath.getFileName().toString();
                if (!name.matches("run-(0|[1-9][0-9]*)--?[0-9]+\\.spill")
                        || Files.isSymbolicLink(runPath)
                        || !Files.isRegularFile(runPath, LinkOption.NOFOLLOW_LINKS)) {
                    report.suspicious(candidateId, "run");
                    return CleanupResult.SUSPICIOUS;
                }
                if (!report.canDelete(1)) return CleanupResult.INCOMPLETE;
                deleteAuthorized(runPath, report);
            }
        }
        if (!report.canDelete(1)) return CleanupResult.INCOMPLETE;
        deleteAuthorized(operatorPath, report);
        return CleanupResult.CLEAN;
    }

    private static void deleteAuthorized(Path path, SpillScavengeReport.Builder report)
            throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("refusing spill symlink deletion");
        }
        long bytes = Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                ? Files.size(path) : 0;
        if (!Files.deleteIfExists(path)) {
            throw new IOException("authorized spill entry is absent");
        }
        report.deleted(bytes);
    }

    private void closeScanCursor() throws IOException {
        DirectoryStream<Path> current = scanStream;
        scanStream = null;
        scanIterator = null;
        if (current != null) current.close();
    }

    private void closeScanCursorQuietly() {
        try {
            closeScanCursor();
        } catch (IOException ignored) {
            // The bounded report already records the maintenance failure.
        }
    }

    private enum CleanupResult {
        CLEAN,
        INCOMPLETE,
        SUSPICIOUS
    }

    private static void prepareBase(Path base) throws IOException {
        rejectSymlinkChain(base);
        createPrivateDirectory(base);
        rejectSymlinkChain(base);
        requireDirectory(base);
    }

    private static void createPrivateDirectory(Path directory) throws IOException {
        Files.createDirectories(directory);
        requireDirectory(directory);
        setPrivateDirectoryPermissions(directory);
    }

    private static void createPrivateDirectoryExactly(Path directory) throws IOException {
        Files.createDirectory(directory);
        requireDirectory(directory);
        setPrivateDirectoryPermissions(directory);
    }

    private static void requireContainedDirectory(Path parent, Path child) throws IOException {
        Path normalizedParent = parent.toAbsolutePath().normalize();
        Path normalizedChild = child.toAbsolutePath().normalize();
        if (!normalizedChild.startsWith(normalizedParent) || normalizedChild.equals(normalizedParent)) {
            throw new IOException("spill directory escapes owner");
        }
        requireDirectory(normalizedChild);
    }

    private static void requireDirectory(Path path) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("spill path is not an owned directory");
        }
    }

    private static void rejectSymlinkChain(Path path) throws IOException {
        Path absolute = path.toAbsolutePath().normalize();
        Path current = absolute.getRoot();
        for (Path name : absolute) {
            current = current.resolve(name);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(current)) {
                    throw new IOException("spill path contains symbolic link");
                }
                if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)
                        && !current.equals(absolute)) {
                    throw new IOException("spill path parent is not a directory");
                }
            }
        }
    }

    private static void verifyOrPublishMarker(Path path, SpillWorkspaceMarker expected,
                                              boolean acceptExisting) throws IOException {
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            if (!acceptExisting) throw new FileAlreadyExistsException("spill marker exists");
            verifyMarker(path, expected);
            return;
        }
        byte[] encoded = SpillWorkspaceMarkerCodec.encode(expected);
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp-"
                + UUID.randomUUID().toString().replace("-", ""));
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.wrap(encoded);
                while (bytes.hasRemaining()) channel.write(bytes);
                channel.force(true);
            }
            setPrivateFilePermissions(temporary);
            try {
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
            } catch (FileAlreadyExistsException raced) {
                if (!acceptExisting) throw raced;
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("atomic spill marker publication unavailable", unsupported);
            }
            setPrivateFilePermissions(path);
            verifyMarker(path, expected);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void verifyMarker(Path path, SpillWorkspaceMarker expected) throws IOException {
        SpillWorkspaceMarker actual = readMarker(path);
        if (!expected.equals(actual)) {
            throw new IOException("spill marker ownership conflict");
        }
    }

    private static SpillWorkspaceMarker readMarker(Path path) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("spill marker is not a regular file");
        }
        long size = Files.size(path);
        if (size < 1 || size > SpillWorkspaceMarkerCodec.MAX_ENCODED_BYTES) {
            throw new IOException("spill marker size is invalid");
        }
        return SpillWorkspaceMarkerCodec.decode(Files.readAllBytes(path));
    }

    private static void requireOnlyEntries(Path directory, String... allowed) throws IOException {
        Set<String> names = new java.util.HashSet<>();
        java.util.Collections.addAll(names, allowed);
        try (Stream<Path> children = Files.list(directory)) {
            if (children.anyMatch(child -> !names.contains(child.getFileName().toString()))) {
                throw new IOException("spill directory contains an unknown entry");
            }
        }
    }

    private static void deleteRequired(Path path) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("refusing to delete spill symbolic link");
        }
        if (!Files.deleteIfExists(path)) {
            throw new IOException("required spill entry is absent");
        }
    }

    private static void deleteIfExists(Path path) {
        if (path == null || Files.isSymbolicLink(path)) return;
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Construction failure remains primary; unmarked residue is never scavenger-authorized.
        }
    }

    private static void cleanupIncompleteInstance(Path instance, Path jobs) {
        deleteIfExists(jobs);
        if (instance == null) return;
        deleteIfExists(instance.resolve(OWNER_MARKER));
        deleteIfExists(instance.resolve(LEASE_FILE));
        deleteIfExists(instance);
    }

    private static void setPrivateDirectoryPermissions(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, DIRECTORY_PERMISSIONS);
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX filesystem: containment and NOFOLLOW checks remain mandatory.
        }
    }

    private static void setPrivateFilePermissions(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, FILE_PERMISSIONS);
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX filesystem.
        }
    }

    private static Throwable addFailure(Throwable primary, Throwable added) {
        if (primary == null) return added;
        primary.addSuppressed(added);
        return primary;
    }

    private void requireActiveJob(SpillJobWorkspace job) {
        if (closed || job == null || job.isClosed() || !jobs.contains(job)) {
            throw new IllegalStateException("spill job workspace is not active");
        }
    }

    private static String digestPrefix(SpillJobWorkspace job) {
        return job.jobId().substring(0, 16);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Construction failure remains primary.
        }
    }
}
