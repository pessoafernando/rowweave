package io.rowweave.cluster;

import io.rowweave.Sink;
import io.rowweave.ManifestFileSink;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.RunAttemptFence;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Registry of prepared sinks on a worker, kept apart from the assignment ledger. Active entries are
 * hard-capped; a terminal entry leaves only a light tombstone to absorb duplicate or late control
 * messages, and that tombstone is dropped once its time to live passes.
 */
final class PreparedOutputRegistry implements AutoCloseable {

    private enum State {
        PREPARED, DATA_INSTALLED, DATA_SEALED, PUBLISHED,
        GROUP_PUBLISHED, ROLLED_BACK, FINALIZED
    }

    private static final class Entry {
        final String groupId;
        final RunAttemptFence runAttemptFence;
        Sink output;
        String manifestAttemptId;
        long manifestGeneration;
        byte[] installedMemberBytes;
        State state;
        long touchedAt;

        Entry(String groupId, RunAttemptFence runAttemptFence,
              Sink output, State state, long touchedAt) {
            this.groupId = groupId;
            this.runAttemptFence = runAttemptFence;
            this.output = output;
            this.state = state;
            this.touchedAt = touchedAt;
        }
    }

    private final int maxEntries;
    private final long retentionMs;
    private final Map<Long, Entry> entries = new HashMap<>();
    private boolean closed;

    PreparedOutputRegistry(int maxEntries, long retentionMs) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("prepared output limit must be > 0");
        }
        if (retentionMs <= 0) {
            throw new IllegalArgumentException("prepared output retention must be > 0");
        }
        this.maxEntries = maxEntries;
        this.retentionMs = retentionMs;
    }

    synchronized boolean register(
            String groupId, long stageId, Sink output, RunAttemptFence runAttemptFence) {
        requireGroup(groupId);
        requireFence(runAttemptFence);
        if (closed) {
            output.rollbackCommit();
            throw new IllegalStateException("prepared output registry is closed");
        }
        if (output == null || !output.supportsCoordinatedCommit()) {
            throw new IllegalArgumentException("prepared Sink must support coordinated commit");
        }
        long now = System.currentTimeMillis();
        purgeExpiredLocked(now);
        Entry existing = entries.get(stageId);
        if (existing != null) {
            if (!existing.runAttemptFence.equals(runAttemptFence)) {
                if (existing.output == null) {
                    entries.remove(stageId);
                    existing = null;
                } else {
                    throw fenceMismatch(stageId);
                }
            }
        }
        if (existing != null) {
            requireSameGroup(stageId, groupId, existing);
            existing.touchedAt = now;
            if (existing.state == State.ROLLED_BACK || existing.state == State.FINALIZED) {
                output.rollbackCommit();
                return false;
            }
            throw new IllegalStateException("prepared output already registered: stageId=" + stageId);
        }
        makeRoomForEntry();
        if (entries.size() >= maxEntries) {
            output.rollbackCommit();
            throw new IllegalStateException("prepared output registry capacity exhausted: limit=" + maxEntries);
        }
        entries.put(stageId, new Entry(groupId, runAttemptFence, output, State.PREPARED, now));
        return true;
    }

    /** Only the owning live job may extend an uncommitted prepared Sink's cleanup lease. */
    synchronized void renew(String groupId, long stageId, RunAttemptFence runAttemptFence) {
        long now = System.currentTimeMillis();
        purgeExpiredLocked(now);
        Entry entry = required(groupId, stageId, runAttemptFence);
        if (closed || entry.state != State.PREPARED || entry.output == null) {
            throw new IllegalStateException("prepared output is not renewable: stageId=" + stageId);
        }
        entry.touchedAt = now;
    }

    synchronized void commit(String groupId, long stageId, RunAttemptFence runAttemptFence) {
        Entry entry = required(groupId, stageId, runAttemptFence);
        if (entry.state == State.PUBLISHED || entry.state == State.FINALIZED) {
            return;
        }
        if (entry.state == State.ROLLED_BACK) {
            return;  // a late commit must never resurrect a result already rolled back
        }
        entry.output.commitPrepared();
        entry.state = State.PUBLISHED;
        entry.touchedAt = System.currentTimeMillis();
    }

    synchronized byte[] installManifestMember(
            String groupId, long stageId, String attemptId, long generation,
            RunAttemptFence runAttemptFence) {
        Entry entry = required(groupId, stageId, runAttemptFence);
        if (entry.state == State.DATA_SEALED) {
            if (!attemptId.equals(entry.manifestAttemptId)
                    || generation != entry.manifestGeneration
                    || entry.installedMemberBytes == null) {
                throw new IllegalArgumentException(
                        "sealed manifest member identity mismatch: stageId=" + stageId);
            }
            entry.touchedAt = System.currentTimeMillis();
            return entry.installedMemberBytes.clone();
        }
        if (!(entry.output instanceof ManifestFileSink)) {
            throw new IllegalArgumentException("prepared Sink is not a ManifestFileSink");
        }
        ManifestFileSink output = (ManifestFileSink) entry.output;
        output.requireManifestContext(attemptId, generation);
        if (entry.state == State.ROLLED_BACK || entry.state == State.FINALIZED) {
            throw new IllegalStateException("manifest member is already terminal: stageId=" + stageId);
        }
        if (entry.state == State.PREPARED) {
            output.commitPrepared();
            entry.state = State.DATA_INSTALLED;
        } else if (entry.state != State.DATA_INSTALLED && entry.state != State.GROUP_PUBLISHED) {
            throw new IllegalStateException("invalid manifest member state: " + entry.state);
        }
        entry.touchedAt = System.currentTimeMillis();
        byte[] encoded = ManifestCommitWire.encodeMember(output.installedMember());
        entry.manifestAttemptId = attemptId;
        entry.manifestGeneration = generation;
        entry.installedMemberBytes = encoded.clone();
        return encoded;
    }

    synchronized void sealManifestMember(
            String groupId, long stageId, String attemptId, long generation,
            RunAttemptFence runAttemptFence) {
        Entry entry = required(groupId, stageId, runAttemptFence);
        if (entry.state == State.DATA_SEALED || entry.state == State.FINALIZED) return;
        if (!(entry.output instanceof ManifestFileSink)) {
            throw new IllegalArgumentException("prepared Sink is not a ManifestFileSink");
        }
        ManifestFileSink output = (ManifestFileSink) entry.output;
        output.requireManifestContext(attemptId, generation);
        if (entry.state != State.DATA_INSTALLED) {
            throw new IllegalStateException(
                    "manifest member is not installed: stageId=" + stageId);
        }
        output.sealInstalledMember();
        entry.output = null;
        entry.state = State.DATA_SEALED;
        entry.touchedAt = System.currentTimeMillis();
    }

    synchronized void publishManifestGroup(
            String groupId, long stageId, String attemptId, long generation,
            byte[] encodedManifest, RunAttemptFence runAttemptFence) {
        Entry coordinator = required(groupId, stageId, runAttemptFence);
        if (!(coordinator.output instanceof ManifestFileSink)) {
            throw new IllegalArgumentException("manifest coordinator Sink is not a ManifestFileSink");
        }
        if (coordinator.state != State.DATA_INSTALLED && coordinator.state != State.GROUP_PUBLISHED) {
            throw new IllegalStateException("manifest coordinator member is not installed: stageId=" + stageId);
        }
        CommitManifest manifest = CommitManifest.decode(encodedManifest);
        if (!attemptId.equals(manifest.attemptId()) || generation != manifest.generation()) {
            throw new IllegalArgumentException("manifest publish command identity mismatch");
        }
        ((ManifestFileSink) coordinator.output).publishManifestGroup(manifest);
        long now = System.currentTimeMillis();
        for (Entry entry : entries.values()) {
            if (entry.groupId.equals(groupId)
                    && entry.runAttemptFence.equals(runAttemptFence)
                    && entry.output instanceof ManifestFileSink
                    && entry.state == State.DATA_INSTALLED) {
                entry.state = State.GROUP_PUBLISHED;
                entry.touchedAt = now;
            }
        }
    }

    synchronized void rollback(
            String groupId, long stageId, RunAttemptFence runAttemptFence) {
        requireGroup(groupId);
        requireFence(runAttemptFence);
        long now = System.currentTimeMillis();
        Entry entry = entries.get(stageId);
        if (entry == null) {
            purgeExpiredLocked(now);
            makeRoomForEntry();
            if (entries.size() >= maxEntries) {
                throw new IllegalStateException("prepared output registry capacity exhausted: limit=" + maxEntries);
            }
            // A rollback that arrives before the computing stage finishes leaves a tombstone, so the prepared sink cannot be registered later and leak.
            entries.put(stageId, new Entry(
                    groupId, runAttemptFence, null, State.ROLLED_BACK, now));
            return;
        }
        if (!entry.runAttemptFence.equals(runAttemptFence)) {
            throw fenceMismatch(stageId);
        }
        requireSameGroup(stageId, groupId, entry);
        if (entry.state == State.ROLLED_BACK || entry.state == State.FINALIZED
                || entry.state == State.DATA_SEALED) {
            return;
        }
        entry.output.rollbackCommit();
        entry.output = null;
        entry.state = State.ROLLED_BACK;
        entry.touchedAt = now;
    }

    synchronized void rollbackStage(long stageId, RunAttemptFence runAttemptFence) {
        requireFence(runAttemptFence);
        Entry entry = entries.get(stageId);
        if (entry != null) {
            rollback(entry.groupId, stageId, runAttemptFence);
        }
    }

    synchronized void finalizeCommit(
            String groupId, long stageId, RunAttemptFence runAttemptFence) {
        Entry entry = required(groupId, stageId, runAttemptFence);
        if (entry.state == State.FINALIZED || entry.state == State.ROLLED_BACK) {
            return;
        }
        if (entry.state == State.DATA_SEALED) {
            entry.state = State.FINALIZED;
            entry.touchedAt = System.currentTimeMillis();
            return;
        }
        if (entry.state != State.PUBLISHED && entry.state != State.DATA_INSTALLED
                && entry.state != State.GROUP_PUBLISHED) {
            throw new IllegalStateException("prepared output is not published: stageId=" + stageId);
        }
        entry.output.finalizeCommit();
        entry.output = null;
        entry.state = State.FINALIZED;
        entry.touchedAt = System.currentTimeMillis();
    }

    synchronized void purgeExpired() {
        purgeExpiredLocked(System.currentTimeMillis());
    }

    private void purgeExpiredLocked(long now) {
        Iterator<Map.Entry<Long, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Entry> mapped = iterator.next();
            Entry entry = mapped.getValue();
            if (now - entry.touchedAt < retentionMs) {
                continue;
            }
            if (entry.output != null) {
                try {
                    entry.output.rollbackCommit();
                } catch (RuntimeException | Error cleanupFailure) {
                    entry.touchedAt = now;
                    continue;  // kept with a strong reference and retried by a later maintenance cycle, never reported as cleaned
                }
            }
            iterator.remove();
        }
    }

    /** A terminal tombstone serves only short-lived idempotence and must not evict a newer active prepared sink. */
    private void makeRoomForEntry() {
        while (entries.size() >= maxEntries) {
            Long oldestId = null;
            long oldestTouched = Long.MAX_VALUE;
            for (Map.Entry<Long, Entry> mapped : entries.entrySet()) {
                Entry entry = mapped.getValue();
                if (entry.output == null && entry.touchedAt < oldestTouched) {
                    oldestId = mapped.getKey();
                    oldestTouched = entry.touchedAt;
                }
            }
            if (oldestId == null) {
                return;
            }
            entries.remove(oldestId);
        }
    }

    synchronized int activeCount() {
        int count = 0;
        for (Entry entry : entries.values()) {
            if (entry.output != null) {
                count++;
            }
        }
        return count;
    }

    synchronized int entryCount() {
        return entries.size();
    }

    synchronized void rollbackAll() {
        Throwable failure = null;
        long now = System.currentTimeMillis();
        for (Entry entry : entries.values()) {
            if (entry.output == null) {
                continue;
            }
            try {
                entry.output.rollbackCommit();
                entry.output = null;
                entry.state = State.ROLLED_BACK;
                entry.touchedAt = now;
            } catch (Throwable rollbackFailure) {
                if (failure == null) {
                    failure = rollbackFailure;
                } else {
                    failure.addSuppressed(rollbackFailure);
                }
            }
        }
        if (failure != null) {
            throwUnchecked(failure);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        rollbackAll();
        entries.clear();
    }

    private Entry required(String groupId, long stageId, RunAttemptFence runAttemptFence) {
        requireGroup(groupId);
        requireFence(runAttemptFence);
        Entry entry = entries.get(stageId);
        if (entry == null) {
            throw new IllegalStateException("prepared output not found: stageId=" + stageId);
        }
        requireSameGroup(stageId, groupId, entry);
        if (!entry.runAttemptFence.equals(runAttemptFence)) {
            throw fenceMismatch(stageId);
        }
        return entry;
    }

    private static IllegalArgumentException fenceMismatch(long stageId) {
        return new IllegalArgumentException("run attempt fence mismatch: stageId=" + stageId);
    }

    private static void requireSameGroup(long stageId, String groupId, Entry entry) {
        if (!entry.groupId.equals(groupId)) {
            throw new IllegalArgumentException("prepared output group mismatch: stageId=" + stageId);
        }
    }

    private static void requireGroup(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("commitGroupId must not be empty");
        }
    }

    private static void requireFence(RunAttemptFence runAttemptFence) {
        if (runAttemptFence == null) {
            throw new IllegalArgumentException("run attempt fence is required");
        }
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }
}
