package io.rowweave.artifact;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/** Reference backend; a capacity rejection never evicts an Artifact that a Run may still reference. */
public final class InMemoryArtifactRepository
        implements ArtifactLifecycleRepository, AutoCloseable {

    private static final int STRIPE_COUNT = 256;

    private final int maxEntries;
    private final long maxTotalBytes;
    private final ControlResidentBudget residentBudget;
    private final Map<String, ResidentArtifact> artifacts = new HashMap<>();
    private final ReentrantLock[] publicationLocks = new ReentrantLock[STRIPE_COUNT];
    private long storedBytes;
    private long inventoryGeneration;
    private boolean closed;

    public InMemoryArtifactRepository(int maxEntries, long maxTotalBytes) {
        this(maxEntries, maxTotalBytes, ControlResidentBudget.processWide());
    }

    /** Explicit-budget entry point for tests that exercise several repository lifecycles in one JVM, and for embedded deployment. */
    public InMemoryArtifactRepository(int maxEntries, long maxTotalBytes,
                                      ControlResidentBudget residentBudget) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0");
        }
        if (maxTotalBytes <= 0L) {
            throw new IllegalArgumentException("maxTotalBytes must be > 0");
        }
        if (residentBudget == null) {
            throw new IllegalArgumentException("residentBudget is required");
        }
        this.maxEntries = maxEntries;
        this.maxTotalBytes = maxTotalBytes;
        this.residentBudget = residentBudget;
        for (int i = 0; i < publicationLocks.length; i++) {
            publicationLocks[i] = new ReentrantLock();
        }
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
        ReentrantLock guard = publicationLock(candidate.artifactId);
        guard.lock();
        try {
            return store(candidate);
        } finally {
            guard.unlock();
        }
    }

    @Override
    public <T> T publishReferenced(byte[] encodedArtifact,
            java.util.function.LongConsumer reservation,
            ArtifactReferencePublisher<T> publisher) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        if (publisher == null) throw new IllegalArgumentException("publisher is required");
        ArtifactRepositorySupport.StoredArtifact candidate =
                ArtifactRepositorySupport.validateEncoded(encodedArtifact, reservation);
        ReentrantLock guard = publicationLock(candidate.artifactId);
        guard.lock();
        try {
            String artifactId = store(candidate);
            return publisher.publish(artifactId);
        } finally {
            guard.unlock();
        }
    }

    private synchronized String store(
            ArtifactRepositorySupport.StoredArtifact candidate) {
        requireOpen();
        ResidentArtifact existing = artifacts.get(candidate.artifactId);
        if (existing != null) {
            if (!Arrays.equals(existing.bytes, candidate.bytes)) {
                throw new IllegalStateException(
                        "artifact identity collision: " + candidate.artifactId);
            }
            if (existing.state != ArtifactInventoryState.LIVE) {
                throw new IllegalStateException(
                        "stored Artifact inventory is not LIVE");
            }
            return candidate.artifactId;
        }
        if (artifacts.size() >= maxEntries) {
            throw new IllegalStateException(
                    "artifact repository entry capacity exhausted: limit=" + maxEntries);
        }
        if (candidate.bytes.length > maxTotalBytes - storedBytes) {
            throw new IllegalStateException(
                    "artifact repository byte capacity exhausted: limit=" + maxTotalBytes);
        }
        ControlResidentLease resident = residentBudget.openLease();
        ControlResidentLease.OwnerToken owner = resident.ownerToken();
        try {
            resident.reserve(owner, candidate.bytes.length);
            byte[] stored = candidate.bytes.clone();
            long generation = nextInventoryGeneration();
            long createdAt = System.currentTimeMillis();
            if (createdAt <= 0L) {
                throw new IllegalStateException(
                        "Artifact inventory authority clock must be > 0");
            }
            artifacts.put(candidate.artifactId,
                    new ResidentArtifact(stored, resident, owner,
                            createdAt, generation));
        } catch (RuntimeException | Error failure) {
            resident.release(owner);
            throw failure;
        }
        storedBytes += candidate.bytes.length;
        return candidate.artifactId;
    }

    @Override
    public ArtifactBackfillStatus inventoryStatus() {
        return ArtifactBackfillStatus.ready();
    }

    @Override
    public ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries) {
        validateMaintenanceLimit(maxEntries);
        return ArtifactBackfillStatus.ready();
    }

    @Override
    public int inventoryUsageShardCount() {
        return 1;
    }

    @Override
    public synchronized ArtifactInventoryUsage inventoryUsage(int shard) {
        requireOpen();
        if (shard != 0) {
            throw new IllegalArgumentException("in-memory inventory shard must be 0");
        }
        return ArtifactInventoryUsage.global(
                artifacts.size(), storedBytes, maxEntries, maxTotalBytes);
    }

    @Override
    public synchronized ArtifactCandidatePage scanCandidates(
            String cursor, int limit, long cutoffMillis) {
        validateMaintenanceLimit(limit);
        if (cutoffMillis < 0L) {
            throw new IllegalArgumentException("cutoffMillis must be >= 0");
        }
        ArtifactLifecycleCursor position = cursor == null
                ? ArtifactLifecycleCursor.start(ArtifactLifecycleCursor.Kind.INVENTORY)
                : ArtifactLifecycleCursor.decode(
                        ArtifactLifecycleCursor.Kind.INVENTORY, cursor);
        List<ArtifactCandidate> matches = new ArrayList<>();
        for (Map.Entry<String, ResidentArtifact> stored : artifacts.entrySet()) {
            String artifactId = stored.getKey();
            ResidentArtifact artifact = stored.getValue();
            if (ArtifactLifecycleSupport.shard(artifactId) != position.shard()
                    || artifact.state != ArtifactInventoryState.LIVE
                    || artifact.createdAtMillis > cutoffMillis
                    || (position.afterDigest() != null
                    && artifactId.compareTo(position.afterDigest()) <= 0)) {
                continue;
            }
            matches.add(ArtifactCandidate.from(artifact.inventoryEntry(artifactId)));
        }
        matches.sort(Comparator.comparing(ArtifactCandidate::artifactId));
        boolean more = matches.size() > limit;
        if (more) matches = new ArrayList<>(matches.subList(0, limit));
        String last = matches.isEmpty()
                ? position.afterDigest()
                : matches.get(matches.size() - 1).artifactId();
        ArtifactLifecycleCursor next = more
                ? ArtifactLifecycleCursor.at(ArtifactLifecycleCursor.Kind.INVENTORY,
                position.cycle(), position.shard(), last)
                : position.nextShard();
        return new ArtifactCandidatePage(matches, next.encode(),
                !more && position.shard() == 255);
    }

    @Override
    public synchronized ArtifactRecoveryPage scanRecovering(
            String cursor, int limit) {
        validateMaintenanceLimit(limit);
        requireOpen();
        ArtifactLifecycleCursor position = cursor == null
                ? ArtifactLifecycleCursor.start(
                ArtifactLifecycleCursor.Kind.INVENTORY)
                : ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.INVENTORY, cursor);
        List<ArtifactRecoveryCandidate> matches = new ArrayList<>();
        for (Map.Entry<String, ResidentArtifact> stored : artifacts.entrySet()) {
            String artifactId = stored.getKey();
            ResidentArtifact artifact = stored.getValue();
            if (ArtifactLifecycleSupport.shard(artifactId) != position.shard()
                    || artifact.state != ArtifactInventoryState.DELETING
                    || (position.afterDigest() != null
                    && artifactId.compareTo(position.afterDigest()) <= 0)) {
                continue;
            }
            matches.add(ArtifactRecoveryCandidate.from(
                    artifact.inventoryEntry(artifactId)));
        }
        matches.sort(Comparator.comparing(
                value -> value.candidate().artifactId()));
        boolean more = matches.size() > limit;
        if (more) matches = new ArrayList<>(matches.subList(0, limit));
        String last = matches.isEmpty()
                ? position.afterDigest()
                : matches.get(matches.size() - 1).candidate().artifactId();
        ArtifactLifecycleCursor next = more
                ? ArtifactLifecycleCursor.at(
                ArtifactLifecycleCursor.Kind.INVENTORY,
                position.cycle(), position.shard(), last)
                : position.nextShard();
        return new ArtifactRecoveryPage(matches, next.encode(),
                !more && position.shard() == 255);
    }

    @Override
    public ArtifactCollectResult collect(
            ArtifactCandidate candidate, ArtifactGcToken token,
            ArtifactReferenceAuthority references) {
        requireCollectArguments(candidate, token, references);
        ReentrantLock guard = publicationLock(candidate.artifactId());
        guard.lock();
        try {
            ResidentArtifact current;
            synchronized (this) {
                requireOpen();
                current = artifacts.get(candidate.artifactId());
                if (current == null) {
                    return ArtifactCollectResult.of(
                            ArtifactCollectResult.Code.ALREADY_DELETED);
                }
                if (!current.matches(candidate)) {
                    return ArtifactCollectResult.of(
                            ArtifactCollectResult.Code.STALE_CANDIDATE);
                }
                if (current.state == ArtifactInventoryState.DELETING) {
                    if (!current.gcToken.equals(token)) {
                        if (!ArtifactGcToken.isExactSuccessor(
                                current.gcToken, token)) {
                            return ArtifactCollectResult.of(
                                    ArtifactCollectResult.Code.STALE_OWNER);
                        }
                        current.gcToken = token;
                    }
                } else {
                    ArtifactReferenceView first = inspectReady(
                            references, candidate.artifactId());
                    if (first.code()
                            == ArtifactReferenceView.Code.READY_REFERENCED) {
                        return ArtifactCollectResult.of(
                                ArtifactCollectResult.Code.RETAINED_REFERENCED);
                    }
                    current.state = ArtifactInventoryState.DELETING;
                    current.gcToken = token;
                }
            }

            ArtifactReferenceView second = inspectReady(
                    references, candidate.artifactId());
            synchronized (this) {
                current = artifacts.get(candidate.artifactId());
                if (current == null) {
                    return ArtifactCollectResult.of(
                            ArtifactCollectResult.Code.ALREADY_DELETED);
                }
                if (!current.matches(candidate) || current.gcToken == null
                        || !current.gcToken.equals(token)) {
                    return ArtifactCollectResult.of(
                            ArtifactCollectResult.Code.STALE_OWNER);
                }
                if (second.code()
                        == ArtifactReferenceView.Code.READY_REFERENCED) {
                    current.state = ArtifactInventoryState.LIVE;
                    current.gcToken = null;
                    current.generation = nextInventoryGeneration();
                    return ArtifactCollectResult.of(
                            ArtifactCollectResult.Code.RETAINED_REFERENCED);
                }
                artifacts.remove(candidate.artifactId());
                storedBytes -= current.bytes.length;
                current.lease.release(current.owner);
                return ArtifactCollectResult.of(ArtifactCollectResult.Code.DELETED);
            }
        } finally {
            guard.unlock();
        }
    }

    @Override
    public synchronized byte[] get(String artifactId) {
        return get(artifactId, value -> { });
    }

    @Override
    public synchronized byte[] get(String artifactId,
                                   java.util.function.LongConsumer reservation) {
        String id = ArtifactRepositorySupport.validateId(artifactId);
        requireOpen();
        ResidentArtifact entry = artifacts.get(id);
        byte[] stored = entry == null ? null : entry.bytes;
        if (stored == null) {
            return null;
        }
        byte[] validated = ArtifactRepositorySupport.validateStored(id, stored, reservation);
        reservation.accept(validated.length);
        return validated.clone();
    }

    public synchronized int size() {
        return artifacts.size();
    }

    public synchronized long storedBytes() {
        return storedBytes;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        for (ResidentArtifact artifact : artifacts.values()) {
            artifact.lease.release(artifact.owner);
        }
        artifacts.clear();
        storedBytes = 0L;
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("artifact repository is closed");
    }

    private ReentrantLock publicationLock(String artifactId) {
        return publicationLocks[ArtifactLifecycleSupport.shard(artifactId)];
    }

    private synchronized long nextInventoryGeneration() {
        if (inventoryGeneration == Long.MAX_VALUE) {
            throw new IllegalStateException("Artifact inventory generation exhausted");
        }
        return ++inventoryGeneration;
    }

    private static void requireCollectArguments(
            ArtifactCandidate candidate, ArtifactGcToken token,
            ArtifactReferenceAuthority references) {
        if (candidate == null || token == null || references == null) {
            throw new IllegalArgumentException(
                    "candidate, token, and references are required");
        }
        if (!candidate.artifactId().equals(token.artifactId())
                || candidate.inventoryGeneration() != token.inventoryGeneration()) {
            throw new IllegalArgumentException(
                    "Artifact GC token does not match candidate");
        }
    }

    private static ArtifactReferenceView inspectReady(
            ArtifactReferenceAuthority references, String artifactId) {
        ArtifactBackfillStatus status = references.backfillStatus();
        if (status.code() != ArtifactBackfillStatus.Code.READY) {
            throw new IllegalStateException(
                    "Artifact reference authority is not READY: " + status.code());
        }
        return references.inspect(artifactId);
    }

    private static void validateMaintenanceLimit(int limit) {
        if (limit < 1 || limit > ArtifactCandidatePage.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("maintenance limit must be in [1,1000]");
        }
    }

    private static final class ResidentArtifact {
        private final byte[] bytes;
        private final ControlResidentLease lease;
        private final ControlResidentLease.OwnerToken owner;
        private final long createdAtMillis;
        private long generation;
        private ArtifactInventoryState state = ArtifactInventoryState.LIVE;
        private ArtifactGcToken gcToken;

        private ResidentArtifact(byte[] bytes, ControlResidentLease lease,
                                 ControlResidentLease.OwnerToken owner,
                                 long createdAtMillis, long generation) {
            this.bytes = bytes;
            this.lease = lease;
            this.owner = owner;
            this.createdAtMillis = createdAtMillis;
            this.generation = generation;
        }

        private ArtifactInventoryEntry inventoryEntry(String artifactId) {
            return state == ArtifactInventoryState.LIVE
                    ? ArtifactInventoryEntry.live(artifactId, bytes.length,
                    createdAtMillis, generation)
                    : ArtifactInventoryEntry.deleting(artifactId, bytes.length,
                    createdAtMillis, generation, gcToken);
        }

        private boolean matches(ArtifactCandidate candidate) {
            return bytes.length == candidate.encodedBytes()
                    && createdAtMillis == candidate.createdAtMillis()
                    && generation == candidate.inventoryGeneration();
        }
    }
}
