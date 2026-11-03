package io.rowweave.artifact;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * A crash-durable, content-addressed Artifact repository on one local file system.
 * Cross-host HA needs a caller-supplied shared directory with consistent locking and atomic-move semantics.
 */
public final class FileArtifactRepository implements ArtifactLifecycleRepository {

    enum PublicationPoint { AFTER_TEMP_FORCE, AFTER_DIRECTORY_FORCE }

    enum InventoryPoint { AFTER_PUBLISHING_FORCE, AFTER_LIVE_FORCE }

    enum GcPoint {
        AFTER_DELETING_FORCE,
        AFTER_UNLINK_DIRECTORY_FORCE,
        AFTER_FINALIZE_FORCE
    }

    @FunctionalInterface
    interface PublicationHook {
        void at(PublicationPoint point);
    }

    @FunctionalInterface
    interface InventoryHook {
        void at(InventoryPoint point);
    }

    @FunctionalInterface
    interface GcHook {
        void at(GcPoint point);
    }

    private static final int STRIPE_COUNT = 256;
    private static final ReentrantLock[] JVM_LOCKS = new ReentrantLock[STRIPE_COUNT];

    static {
        for (int i = 0; i < JVM_LOCKS.length; i++) {
            JVM_LOCKS[i] = new ReentrantLock();
        }
    }

    private final Path root;
    private final Path lockRoot;
    private final Path inventoryRoot;
    private final PublicationHook publicationHook;
    private final ArtifactRetentionPolicy retentionPolicy;
    private final LongSupplier authorityClock;
    private final InventoryHook inventoryHook;
    private final GcHook gcHook;

    public FileArtifactRepository(Path baseDirectory) {
        this(baseDirectory, point -> { },
                ArtifactRetentionPolicy.fromProperties(System.getProperties()),
                System::currentTimeMillis, point -> { }, point -> { });
    }

    /** Injection point for the same-package crash probe only; not part of the published production API. */
    FileArtifactRepository(Path baseDirectory, PublicationHook publicationHook) {
        this(baseDirectory, publicationHook,
                ArtifactRetentionPolicy.fromProperties(System.getProperties()),
                System::currentTimeMillis, point -> { }, point -> { });
    }

    FileArtifactRepository(Path baseDirectory, PublicationHook publicationHook,
                           ArtifactRetentionPolicy retentionPolicy,
                           LongSupplier authorityClock,
                           InventoryHook inventoryHook) {
        this(baseDirectory, publicationHook, retentionPolicy, authorityClock,
                inventoryHook, point -> { });
    }

    FileArtifactRepository(Path baseDirectory, PublicationHook publicationHook,
                           ArtifactRetentionPolicy retentionPolicy,
                           LongSupplier authorityClock,
                           InventoryHook inventoryHook, GcHook gcHook) {
        if (baseDirectory == null) {
            throw new IllegalArgumentException("baseDirectory is required");
        }
        if (publicationHook == null) {
            throw new IllegalArgumentException("publicationHook is required");
        }
        if (retentionPolicy == null) {
            throw new IllegalArgumentException("retentionPolicy is required");
        }
        if (authorityClock == null) {
            throw new IllegalArgumentException("authorityClock is required");
        }
        if (inventoryHook == null) {
            throw new IllegalArgumentException("inventoryHook is required");
        }
        if (gcHook == null) {
            throw new IllegalArgumentException("gcHook is required");
        }
        this.root = baseDirectory.toAbsolutePath().normalize().resolve("artifacts-v1");
        this.lockRoot = root.resolve(".locks-v2");
        this.inventoryRoot = root.resolve(".inventory-v2");
        this.publicationHook = publicationHook;
        this.retentionPolicy = retentionPolicy;
        this.authorityClock = authorityClock;
        this.inventoryHook = inventoryHook;
        this.gcHook = gcHook;
        initializeRoot();
    }

    @Override
    public String put(byte[] encodedArtifact) {
        return put(encodedArtifact, value -> { });
    }

    @Override
    public String put(byte[] encodedArtifact,
                      java.util.function.LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        ArtifactRepositorySupport.StoredArtifact candidate =
                ArtifactRepositorySupport.validateEncoded(encodedArtifact, reservation);
        return withPublicationGuard(candidate, reservation,
                artifactId -> artifactId);
    }

    @Override
    public <T> T publishReferenced(byte[] encodedArtifact,
            java.util.function.LongConsumer reservation,
            ArtifactReferencePublisher<T> publisher) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        if (publisher == null) throw new IllegalArgumentException("publisher is required");
        ArtifactRepositorySupport.StoredArtifact candidate =
                ArtifactRepositorySupport.validateEncoded(encodedArtifact, reservation);
        return withPublicationGuard(candidate, reservation, publisher);
    }

    private <T> T withPublicationGuard(
            ArtifactRepositorySupport.StoredArtifact candidate,
            java.util.function.LongConsumer reservation,
            ArtifactReferencePublisher<T> publisher) {
        Path finalPath = artifactPath(candidate.artifactId);
        ReentrantLock jvm = jvmLock(candidate.artifactId);
        jvm.lock();
        try {
            Path parent = finalPath.getParent();
            ensureDirectory(parent);
            ensureDirectory(lockRoot);
            Path lockPath = stripeLockPath(candidate.artifactId);
            ensureLockFile(lockPath);
            try (FileChannel lockChannel = FileChannel.open(
                    lockPath, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                 FileLock ignored = lockChannel.lock()) {
                ArtifactInventorySnapshot inventory = loadOrMigrateInventory(
                        ArtifactLifecycleSupport.shard(candidate.artifactId));
                ArtifactInventoryEntry inventoryEntry = findEntry(
                        inventory, candidate.artifactId);
                if (existsNoFollow(finalPath)) {
                    byte[] existing = readValidated(
                            finalPath, candidate.artifactId, reservation);
                    if (!Arrays.equals(existing, candidate.bytes)) {
                        throw new IllegalStateException(
                                "artifact identity collision: " + candidate.artifactId);
                    }
                    if (inventoryEntry == null) {
                        throw new IllegalStateException(
                                "stored Artifact is absent from bounded inventory");
                    }
                    requireInventoryIdentity(inventoryEntry, candidate);
                    if (inventoryEntry.state() == ArtifactInventoryState.PUBLISHING) {
                        inventory = transitionLive(inventory, inventoryEntry);
                        inventoryEntry = findEntry(inventory, candidate.artifactId);
                    }
                    if (inventoryEntry.state() != ArtifactInventoryState.LIVE) {
                        throw new IllegalStateException(
                                "stored Artifact inventory is not LIVE");
                    }
                    return publisher.publish(candidate.artifactId);
                }
                if (inventoryEntry == null) {
                    requireCapacity(inventory, candidate.bytes.length);
                    long now = authorityClock.getAsLong();
                    if (now <= 0L) {
                        throw new IllegalStateException(
                                "Artifact inventory authority clock must be > 0");
                    }
                    long intentGeneration = nextGeneration(inventory.generation());
                    inventoryEntry = ArtifactInventoryEntry.publishing(
                            candidate.artifactId, candidate.bytes.length,
                            now, intentGeneration);
                    inventory = withEntry(inventory, inventoryEntry, intentGeneration);
                    persistInventory(inventory);
                    inventoryHook.at(InventoryPoint.AFTER_PUBLISHING_FORCE);
                } else {
                    requireInventoryIdentity(inventoryEntry, candidate);
                    if (inventoryEntry.state() != ArtifactInventoryState.PUBLISHING) {
                        throw new IllegalStateException(
                                "LIVE/DELETING inventory has no Artifact final");
                    }
                }
                publish(finalPath, tempPath(candidate.artifactId), candidate.bytes);
                transitionLive(inventory, inventoryEntry);
                return publisher.publish(candidate.artifactId);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "artifact repository write failed: " + candidate.artifactId, failure);
        } finally {
            jvm.unlock();
        }
    }

    @Override
    public byte[] get(String artifactId) {
        return get(artifactId, value -> { });
    }

    @Override
    public byte[] get(String artifactId, java.util.function.LongConsumer reservation) {
        String id = ArtifactRepositorySupport.validateId(artifactId);
        Path finalPath = artifactPath(id);
        if (!existsNoFollow(finalPath)) {
            return null;
        }
        return readValidated(finalPath, id, reservation);
    }

    Path artifactPathForTest(String artifactId) {
        return artifactPath(ArtifactRepositorySupport.validateId(artifactId));
    }

    Path tempPathForTest(String artifactId) {
        return tempPath(ArtifactRepositorySupport.validateId(artifactId));
    }

    Path stripeLockPathForTest(String artifactId) {
        return stripeLockPath(ArtifactRepositorySupport.validateId(artifactId));
    }

    Path inventoryPathForTest(String artifactId) {
        return inventoryPath(ArtifactLifecycleSupport.shard(
                ArtifactRepositorySupport.validateId(artifactId)));
    }

    ArtifactInventorySnapshot inventoryForTest(String artifactId) {
        String id = ArtifactRepositorySupport.validateId(artifactId);
        int shard = ArtifactLifecycleSupport.shard(id);
        ReentrantLock jvm = JVM_LOCKS[shard];
        jvm.lock();
        try {
            ensureDirectory(lockRoot);
            Path lockPath = stripeLockPathForShard(shard);
            ensureLockFile(lockPath);
            try (FileChannel channel = FileChannel.open(
                    lockPath, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                 FileLock ignored = channel.lock()) {
                return loadExistingInventory(shard);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("read Artifact inventory for test", failure);
        } finally {
            jvm.unlock();
        }
    }

    @Override
    public ArtifactBackfillStatus inventoryStatus() {
        int complete = 0;
        for (int shard = 0; shard < STRIPE_COUNT; shard++) {
            if (!existsNoFollow(inventoryPath(shard))) {
                return ArtifactBackfillStatus.backfilling(
                        ArtifactLifecycleCursor.at(
                                ArtifactLifecycleCursor.Kind.INVENTORY,
                                0L, shard, null).encode(), complete);
            }
            complete++;
        }
        return ArtifactBackfillStatus.ready();
    }

    @Override
    public ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries) {
        if (maxEntries < 1 || maxEntries > ArtifactCandidatePage.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("maintenance limit must be in [1,1000]");
        }
        ArtifactBackfillStatus status = inventoryStatus();
        if (status.code() == ArtifactBackfillStatus.Code.READY) return status;
        ArtifactLifecycleCursor cursor = ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.INVENTORY, status.cursor());
        migrateShardUnderGuard(cursor.shard());
        return inventoryStatus();
    }

    @Override
    public int inventoryUsageShardCount() {
        return STRIPE_COUNT;
    }

    @Override
    public ArtifactInventoryUsage inventoryUsage(int shard) {
        if (shard < 0 || shard >= STRIPE_COUNT) {
            throw new IllegalArgumentException("inventory usage shard must be in [0,255]");
        }
        ArtifactInventorySnapshot inventory = readInventoryUnderGuard(shard);
        return ArtifactInventoryUsage.shard(
                shard, inventory.entries().size(), inventory.storedBytes(),
                retentionPolicy.maxEntriesPerShard(),
                retentionPolicy.maxBytesPerShard());
    }

    @Override
    public ArtifactCandidatePage scanCandidates(
            String cursor, int limit, long cutoffMillis) {
        if (limit < 1 || limit > ArtifactCandidatePage.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("candidate limit must be in [1,1000]");
        }
        if (cutoffMillis < 0L) {
            throw new IllegalArgumentException("cutoffMillis must be >= 0");
        }
        if (inventoryStatus().code() != ArtifactBackfillStatus.Code.READY) {
            throw new IllegalStateException("Artifact inventory migration is not READY");
        }
        ArtifactLifecycleCursor position = cursor == null
                ? ArtifactLifecycleCursor.start(ArtifactLifecycleCursor.Kind.INVENTORY)
                : ArtifactLifecycleCursor.decode(
                        ArtifactLifecycleCursor.Kind.INVENTORY, cursor);
        ArtifactInventorySnapshot inventory = readInventoryUnderGuard(position.shard());
        List<ArtifactCandidate> candidates = new ArrayList<>();
        String last = null;
        boolean more = false;
        for (ArtifactInventoryEntry entry : inventory.entries()) {
            if (position.afterDigest() != null
                    && entry.artifactId().compareTo(position.afterDigest()) <= 0) continue;
            if (entry.state() != ArtifactInventoryState.LIVE
                    || entry.createdAtMillis() > cutoffMillis) continue;
            if (candidates.size() == limit) {
                more = true;
                break;
            }
            candidates.add(ArtifactCandidate.from(entry));
            last = entry.artifactId();
        }
        ArtifactLifecycleCursor next;
        boolean complete;
        if (more) {
            next = ArtifactLifecycleCursor.at(ArtifactLifecycleCursor.Kind.INVENTORY,
                    position.cycle(), position.shard(), last);
            complete = false;
        } else {
            complete = position.shard() == 255;
            next = position.nextShard();
        }
        return new ArtifactCandidatePage(candidates, next.encode(), complete);
    }

    @Override
    public ArtifactRecoveryPage scanRecovering(String cursor, int limit) {
        if (limit < 1 || limit > ArtifactCandidatePage.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "recovery limit must be in [1,1000]");
        }
        if (inventoryStatus().code() != ArtifactBackfillStatus.Code.READY) {
            throw new IllegalStateException(
                    "Artifact inventory migration is not READY");
        }
        ArtifactLifecycleCursor position = cursor == null
                ? ArtifactLifecycleCursor.start(
                ArtifactLifecycleCursor.Kind.INVENTORY)
                : ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.INVENTORY, cursor);
        ArtifactInventorySnapshot inventory =
                readInventoryUnderGuard(position.shard());
        List<ArtifactRecoveryCandidate> candidates = new ArrayList<>();
        String last = null;
        boolean more = false;
        for (ArtifactInventoryEntry entry : inventory.entries()) {
            if (position.afterDigest() != null
                    && entry.artifactId().compareTo(
                    position.afterDigest()) <= 0) continue;
            if (entry.state() != ArtifactInventoryState.DELETING) continue;
            if (candidates.size() == limit) {
                more = true;
                break;
            }
            candidates.add(ArtifactRecoveryCandidate.from(entry));
            last = entry.artifactId();
        }
        ArtifactLifecycleCursor next = more
                ? ArtifactLifecycleCursor.at(
                ArtifactLifecycleCursor.Kind.INVENTORY,
                position.cycle(), position.shard(), last)
                : position.nextShard();
        return new ArtifactRecoveryPage(candidates, next.encode(),
                !more && position.shard() == 255);
    }

    @Override
    public ArtifactCollectResult collect(
            ArtifactCandidate candidate, ArtifactGcToken token,
            ArtifactReferenceAuthority references) {
        requireCollectArguments(candidate, token, references);
        if (inventoryStatus().code() != ArtifactBackfillStatus.Code.READY) {
            throw new IllegalStateException(
                    "Artifact inventory migration is not READY");
        }
        int shard = ArtifactLifecycleSupport.shard(candidate.artifactId());
        return withStripeGuard(shard, () -> collectUnderGuard(
                candidate, token, references, shard));
    }

    private ArtifactCollectResult collectUnderGuard(
            ArtifactCandidate candidate, ArtifactGcToken token,
            ArtifactReferenceAuthority references, int shard) throws IOException {
        Path finalPath = artifactPath(candidate.artifactId());
        ArtifactInventorySnapshot inventory = loadExistingInventory(shard);
        ArtifactInventoryEntry current = findEntry(inventory, candidate.artifactId());
        if (current == null) {
            if (existsNoFollow(finalPath)) {
                throw new IllegalStateException(
                        "stored Artifact is absent from bounded inventory");
            }
            return ArtifactCollectResult.of(
                    ArtifactCollectResult.Code.ALREADY_DELETED);
        }
        if (!matchesCandidate(current, candidate)) {
            return ArtifactCollectResult.of(
                    ArtifactCollectResult.Code.STALE_CANDIDATE);
        }
        if (current.state() == ArtifactInventoryState.PUBLISHING) {
            return ArtifactCollectResult.of(
                    ArtifactCollectResult.Code.STALE_CANDIDATE);
        }
        if (current.state() == ArtifactInventoryState.LIVE) {
            requireCollectableFinal(finalPath, candidate);
            ArtifactReferenceView first = inspectReady(
                    references, candidate.artifactId());
            if (first.code() == ArtifactReferenceView.Code.READY_REFERENCED) {
                return ArtifactCollectResult.of(
                        ArtifactCollectResult.Code.RETAINED_REFERENCED);
            }
            current = ArtifactInventoryEntry.deleting(
                    current.artifactId(), current.encodedBytes(),
                    current.createdAtMillis(), current.generation(), token);
            inventory = withEntry(inventory, current,
                    nextGeneration(inventory.generation()));
            persistInventory(inventory);
            gcHook.at(GcPoint.AFTER_DELETING_FORCE);
        } else if (!current.gcToken().equals(token)) {
            if (!ArtifactGcToken.isExactSuccessor(current.gcToken(), token)) {
                return ArtifactCollectResult.of(
                        ArtifactCollectResult.Code.STALE_OWNER);
            }
            current = ArtifactInventoryEntry.deleting(
                    current.artifactId(), current.encodedBytes(),
                    current.createdAtMillis(), current.generation(), token);
            inventory = withEntry(inventory, current,
                    nextGeneration(inventory.generation()));
            persistInventory(inventory);
            gcHook.at(GcPoint.AFTER_DELETING_FORCE);
        }

        ArtifactReferenceView second = inspectReady(
                references, candidate.artifactId());
        if (second.code() == ArtifactReferenceView.Code.READY_REFERENCED) {
            if (!existsNoFollow(finalPath)) {
                throw new IllegalStateException(
                        "referenced Artifact final is missing during GC recovery");
            }
            requireCollectableFinal(finalPath, candidate);
            long liveGeneration = nextGeneration(inventory.generation());
            ArtifactInventoryEntry live = ArtifactInventoryEntry.live(
                    current.artifactId(), current.encodedBytes(),
                    current.createdAtMillis(), liveGeneration);
            persistInventory(withEntry(inventory, live, liveGeneration));
            return ArtifactCollectResult.of(
                    ArtifactCollectResult.Code.RETAINED_REFERENCED);
        }

        if (existsNoFollow(finalPath)) {
            requireCollectableFinal(finalPath, candidate);
            Files.delete(finalPath);
            forceDirectory(finalPath.getParent());
            gcHook.at(GcPoint.AFTER_UNLINK_DIRECTORY_FORCE);
        }
        persistInventory(withoutEntry(inventory, candidate.artifactId(),
                nextGeneration(inventory.generation())));
        gcHook.at(GcPoint.AFTER_FINALIZE_FORCE);
        return ArtifactCollectResult.of(ArtifactCollectResult.Code.DELETED);
    }

    private void initializeRoot() {
        try {
            boolean existed = existsNoFollow(root);
            Files.createDirectories(root);
            requireDirectory(root);
            if (!existed) {
                forceDirectory(root.getParent());
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to initialize artifact repository", failure);
        }
    }

    private void publish(Path finalPath, Path tempPath, byte[] bytes) throws IOException {
        if (existsNoFollow(tempPath)
                && !Files.isRegularFile(tempPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("artifact temp path is not a regular file: " + tempPath);
        }
        boolean moved = false;
        try {
            try (FileChannel channel = FileChannel.open(tempPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
                ByteBuffer source = ByteBuffer.wrap(bytes);
                while (source.hasRemaining()) {
                    channel.write(source);
                }
                channel.force(true);
            }
            publicationHook.at(PublicationPoint.AFTER_TEMP_FORCE);
            try {
                Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IllegalStateException(
                        "artifact repository requires atomic move: " + finalPath, unsupported);
            }
            moved = true;
            forceDirectory(finalPath.getParent());
            publicationHook.at(PublicationPoint.AFTER_DIRECTORY_FORCE);
        } finally {
            if (!moved) {
                Files.deleteIfExists(tempPath);
            }
        }
    }

    private byte[] readValidated(Path finalPath, String expectedArtifactId) {
        return readValidated(finalPath, expectedArtifactId, value -> { });
    }

    private byte[] readValidated(Path finalPath, String expectedArtifactId,
                                 java.util.function.LongConsumer reservation) {
        if (!Files.isRegularFile(finalPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException(
                    "stored artifact is not a regular file: " + finalPath);
        }
        try (FileChannel channel = FileChannel.open(
                finalPath, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            long size = channel.size();
            if (size < 0L || size > PlanArtifactCodec.MAX_FRAME_BYTES) {
                throw new IllegalStateException(
                        "stored artifact exceeds frame limit: " + size);
            }
            reservation.accept(size);
            byte[] bytes = new byte[(int) size];
            ByteBuffer target = ByteBuffer.wrap(bytes);
            while (target.hasRemaining()) {
                if (channel.read(target) < 0) {
                    throw new IllegalStateException("stored artifact truncated while reading");
                }
            }
            if (channel.size() != size) {
                throw new IllegalStateException("stored artifact changed while reading");
            }
            try {
                byte[] validated = ArtifactRepositorySupport.validateStored(
                        expectedArtifactId, bytes, reservation);
                reservation.accept(validated.length);
                return validated.clone();
            } catch (RuntimeException corruption) {
                throw new IllegalStateException(
                        "stored artifact failed validation: " + expectedArtifactId, corruption);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "artifact repository read failed: " + expectedArtifactId, failure);
        }
    }

    private ArtifactInventorySnapshot transitionLive(
            ArtifactInventorySnapshot inventory, ArtifactInventoryEntry current) {
        long generation = nextGeneration(inventory.generation());
        ArtifactInventoryEntry live = ArtifactInventoryEntry.live(
                current.artifactId(), current.encodedBytes(),
                current.createdAtMillis(), generation);
        ArtifactInventorySnapshot next = withEntry(inventory, live, generation);
        persistInventory(next);
        inventoryHook.at(InventoryPoint.AFTER_LIVE_FORCE);
        return next;
    }

    private void requireCapacity(ArtifactInventorySnapshot inventory, int bytes) {
        if (inventory.entries().size() >= retentionPolicy.maxEntriesPerShard()) {
            throw new IllegalStateException(
                    "Artifact inventory entry capacity exhausted: shard="
                            + inventory.shard() + " limit="
                            + retentionPolicy.maxEntriesPerShard());
        }
        if (bytes > retentionPolicy.maxBytesPerShard() - inventory.storedBytes()) {
            throw new IllegalStateException(
                    "Artifact inventory byte capacity exhausted: shard="
                            + inventory.shard() + " limit="
                            + retentionPolicy.maxBytesPerShard());
        }
    }

    private static void requireInventoryIdentity(
            ArtifactInventoryEntry entry,
            ArtifactRepositorySupport.StoredArtifact candidate) {
        if (!entry.artifactId().equals(candidate.artifactId)
                || entry.encodedBytes() != candidate.bytes.length) {
            throw new IllegalStateException("Artifact inventory identity/size mismatch");
        }
    }

    private static ArtifactInventoryEntry findEntry(
            ArtifactInventorySnapshot inventory, String artifactId) {
        for (ArtifactInventoryEntry entry : inventory.entries()) {
            if (entry.artifactId().equals(artifactId)) return entry;
        }
        return null;
    }

    private static ArtifactInventorySnapshot withEntry(
            ArtifactInventorySnapshot inventory, ArtifactInventoryEntry replacement,
            long generation) {
        List<ArtifactInventoryEntry> entries = new ArrayList<>(inventory.entries().size() + 1);
        boolean replaced = false;
        for (ArtifactInventoryEntry entry : inventory.entries()) {
            if (entry.artifactId().equals(replacement.artifactId())) {
                entries.add(replacement);
                replaced = true;
            } else {
                entries.add(entry);
            }
        }
        if (!replaced) entries.add(replacement);
        return ArtifactInventorySnapshot.of(inventory.shard(), generation, entries);
    }

    private static ArtifactInventorySnapshot withoutEntry(
            ArtifactInventorySnapshot inventory, String artifactId,
            long generation) {
        List<ArtifactInventoryEntry> entries =
                new ArrayList<>(inventory.entries().size());
        for (ArtifactInventoryEntry entry : inventory.entries()) {
            if (!entry.artifactId().equals(artifactId)) entries.add(entry);
        }
        return ArtifactInventorySnapshot.of(inventory.shard(), generation, entries);
    }

    private void requireCollectableFinal(
            Path finalPath, ArtifactCandidate candidate) {
        byte[] encoded = readValidated(finalPath, candidate.artifactId());
        if (encoded.length != candidate.encodedBytes()) {
            throw new IllegalStateException(
                    "stored Artifact size changed after candidate selection");
        }
    }

    private static boolean matchesCandidate(
            ArtifactInventoryEntry entry, ArtifactCandidate candidate) {
        return entry.artifactId().equals(candidate.artifactId())
                && entry.encodedBytes() == candidate.encodedBytes()
                && entry.createdAtMillis() == candidate.createdAtMillis()
                && entry.generation() == candidate.inventoryGeneration();
    }

    private static void requireCollectArguments(
            ArtifactCandidate candidate, ArtifactGcToken token,
            ArtifactReferenceAuthority references) {
        if (candidate == null || token == null || references == null) {
            throw new IllegalArgumentException(
                    "candidate, token, and references are required");
        }
        if (!candidate.artifactId().equals(token.artifactId())
                || candidate.inventoryGeneration()
                != token.inventoryGeneration()) {
            throw new IllegalArgumentException(
                    "Artifact GC token does not match candidate");
        }
    }

    private static ArtifactReferenceView inspectReady(
            ArtifactReferenceAuthority references, String artifactId) {
        ArtifactBackfillStatus status = references.backfillStatus();
        if (status == null
                || status.code() != ArtifactBackfillStatus.Code.READY) {
            throw new IllegalStateException(
                    "Artifact reference authority is not READY");
        }
        ArtifactReferenceView view = references.inspect(artifactId);
        if (view == null) {
            throw new IllegalStateException(
                    "Artifact reference authority returned no decision");
        }
        return view;
    }

    private ArtifactInventorySnapshot loadOrMigrateInventory(int shard)
            throws IOException {
        Path path = inventoryPath(shard);
        return existsNoFollow(path)
                ? loadExistingInventory(shard) : migrateLegacyShard(shard);
    }

    private ArtifactInventorySnapshot loadExistingInventory(int shard)
            throws IOException {
        Path path = inventoryPath(shard);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException(
                    "Artifact inventory is not a regular file: " + path);
        }
        long size = Files.size(path);
        if (size < 0L || size > ArtifactLifecycleCodec.MAX_INVENTORY_BYTES) {
            throw new IllegalStateException("Artifact inventory exceeds codec bound: " + size);
        }
        byte[] encoded = Files.readAllBytes(path);
        if (encoded.length != size || Files.size(path) != size) {
            throw new IllegalStateException("Artifact inventory changed while reading");
        }
        ArtifactInventorySnapshot inventory =
                ArtifactLifecycleCodec.decodeInventory(encoded);
        if (inventory.shard() != shard) {
            throw new IllegalStateException("Artifact inventory shard mismatch");
        }
        requireInventoryPolicy(inventory);
        return inventory;
    }

    private ArtifactInventorySnapshot migrateLegacyShard(int shard)
            throws IOException {
        Path prefix = root.resolve(String.format(java.util.Locale.ROOT, "%02x", shard));
        List<ArtifactInventoryEntry> entries = new ArrayList<>();
        long totalBytes = 0L;
        if (existsNoFollow(prefix)) {
            requireDirectory(prefix);
            try (DirectoryStream<Path> paths = Files.newDirectoryStream(prefix)) {
                for (Path path : paths) {
                    String name = path.getFileName().toString();
                    if (name.endsWith(".artifact")) {
                        if (entries.size() >= retentionPolicy.maxEntriesPerShard()) {
                            throw new IllegalStateException(
                                    "legacy Artifact shard exceeds entry quota: " + shard);
                        }
                        String artifactId = name.substring(
                                0, name.length() - ".artifact".length());
                        ArtifactRepositorySupport.validateId(artifactId);
                        if (ArtifactLifecycleSupport.shard(artifactId) != shard
                                || !path.equals(artifactPath(artifactId))) {
                            throw new IllegalStateException(
                                    "legacy Artifact path identity mismatch: " + path);
                        }
                        byte[] validated = readValidated(path, artifactId);
                        if (validated.length > retentionPolicy.maxBytesPerShard() - totalBytes) {
                            throw new IllegalStateException(
                                    "legacy Artifact shard exceeds byte quota: " + shard);
                        }
                        totalBytes += validated.length;
                        FileTime modified = Files.getLastModifiedTime(
                                path, LinkOption.NOFOLLOW_LINKS);
                        long createdAt = modified.toMillis();
                        if (createdAt <= 0L) {
                            throw new IllegalStateException(
                                    "legacy Artifact has invalid authority time");
                        }
                        entries.add(ArtifactInventoryEntry.live(
                                artifactId, validated.length, createdAt, 1L));
                    } else if (name.endsWith(".tmp") || name.endsWith(".lock")) {
                        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                            throw new IllegalStateException(
                                    "legacy Artifact sidecar is not regular: " + path);
                        }
                        int suffix = name.endsWith(".tmp") ? ".tmp".length() : ".lock".length();
                        String artifactId = name.substring(0, name.length() - suffix);
                        ArtifactRepositorySupport.validateId(artifactId);
                        if (ArtifactLifecycleSupport.shard(artifactId) != shard) {
                            throw new IllegalStateException(
                                    "legacy Artifact sidecar is in wrong shard");
                        }
                    } else {
                        throw new IllegalStateException(
                                "unexpected legacy Artifact shard entry: " + path);
                    }
                }
            }
        }
        ArtifactInventorySnapshot inventory =
                ArtifactInventorySnapshot.of(shard, 1L, entries);
        requireInventoryPolicy(inventory);
        persistInventory(inventory);
        return inventory;
    }

    private void requireInventoryPolicy(ArtifactInventorySnapshot inventory) {
        if (inventory.entries().size() > retentionPolicy.maxEntriesPerShard()) {
            throw new IllegalStateException("Artifact inventory exceeds entry quota");
        }
        if (inventory.storedBytes() > retentionPolicy.maxBytesPerShard()) {
            throw new IllegalStateException("Artifact inventory exceeds byte quota");
        }
    }

    private void persistInventory(ArtifactInventorySnapshot inventory) {
        Path path = inventoryPath(inventory.shard());
        Path temp = inventoryTempPath(inventory.shard());
        byte[] encoded = ArtifactLifecycleCodec.encodeInventory(inventory);
        boolean moved = false;
        try {
            ensureDirectory(inventoryRoot);
            if (existsNoFollow(temp)
                    && !Files.isRegularFile(temp, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException(
                        "Artifact inventory temp is not regular: " + temp);
            }
            try (FileChannel channel = FileChannel.open(temp,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
                ByteBuffer source = ByteBuffer.wrap(encoded);
                while (source.hasRemaining()) channel.write(source);
                channel.force(true);
            }
            try {
                Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IllegalStateException(
                        "Artifact inventory requires atomic replace: " + path, unsupported);
            }
            moved = true;
            forceDirectory(inventoryRoot);
        } catch (IOException failure) {
            throw new UncheckedIOException("persist Artifact inventory", failure);
        } finally {
            if (!moved) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException cleanup) {
                    // Preserve the primary failure; fixed temp is safely taken over next time.
                }
            }
        }
    }

    private void migrateShardUnderGuard(int shard) {
        withStripeGuard(shard, () -> {
            loadOrMigrateInventory(shard);
            return null;
        });
    }

    private ArtifactInventorySnapshot readInventoryUnderGuard(int shard) {
        return withStripeGuard(shard, () -> loadExistingInventory(shard));
    }

    private <T> T withStripeGuard(int shard, IoSupplier<T> operation) {
        ReentrantLock jvm = JVM_LOCKS[shard];
        jvm.lock();
        try {
            ensureDirectory(lockRoot);
            Path lockPath = stripeLockPathForShard(shard);
            ensureLockFile(lockPath);
            try (FileChannel channel = FileChannel.open(
                    lockPath, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                 FileLock ignored = channel.lock()) {
                return operation.get();
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("Artifact stripe operation failed", failure);
        } finally {
            jvm.unlock();
        }
    }

    private static long nextGeneration(long current) {
        if (current == Long.MAX_VALUE) {
            throw new IllegalStateException("Artifact inventory generation exhausted");
        }
        return current + 1L;
    }

    private void ensureDirectory(Path directory) throws IOException {
        boolean existed = existsNoFollow(directory);
        Files.createDirectories(directory);
        requireDirectory(directory);
        if (!existed) {
            forceDirectory(root);
        }
    }

    private static void requireDirectory(Path directory) {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException(
                    "artifact repository path is not a directory: " + directory);
        }
    }

    private static void ensureLockFile(Path lockPath) throws IOException {
        if (existsNoFollow(lockPath)) {
            if (!Files.isRegularFile(lockPath, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException(
                        "artifact lock path is not a regular file: " + lockPath);
            }
            return;
        }
        try {
            Files.createFile(lockPath);
            forceDirectory(lockPath.getParent());
        } catch (java.nio.file.FileAlreadyExistsException raced) {
            if (!Files.isRegularFile(lockPath, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException(
                        "artifact lock path is not a regular file: " + lockPath, raced);
            }
        }
    }

    private Path artifactPath(String artifactId) {
        return prefixDirectory(artifactId).resolve(artifactId + ".artifact");
    }

    private Path stripeLockPath(String artifactId) {
        return stripeLockPathForShard(ArtifactLifecycleSupport.shard(artifactId));
    }

    private Path stripeLockPathForShard(int shard) {
        return lockRoot.resolve(String.format(
                java.util.Locale.ROOT, "%02x.lock", shard));
    }

    private Path inventoryPath(int shard) {
        return inventoryRoot.resolve(String.format(
                java.util.Locale.ROOT, "%02x.inventory", shard));
    }

    private Path inventoryTempPath(int shard) {
        return inventoryRoot.resolve(String.format(
                java.util.Locale.ROOT, "%02x.tmp", shard));
    }

    private Path tempPath(String artifactId) {
        return prefixDirectory(artifactId).resolve(artifactId + ".tmp");
    }

    private Path prefixDirectory(String artifactId) {
        return root.resolve(artifactId.substring(0, 2));
    }

    private static ReentrantLock jvmLock(String artifactId) {
        return JVM_LOCKS[ArtifactLifecycleSupport.shard(artifactId)];
    }

    private static boolean existsNoFollow(Path path) {
        return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
    }

    private static void forceDirectory(Path directory) throws IOException {
        if (directory == null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }
}
