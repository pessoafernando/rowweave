package io.rowweave.sched.net;

import io.rowweave.DataSchema;
import io.rowweave.sched.MultiProducerChannel;

import java.util.UUID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The <b>per-worker</b> data-plane demux registry. A worker exposes one data port, so when the server
 * receives a frame it looks up the {@code targetId} and finds the inbound {@link MultiProducerChannel} of
 * one inbound edge of the matching downstream stage instance, together with the {@link DataSchema} of that
 * edge, which is what deserialization needs.
 *
 * <p><b>Keyed by targetId</b>: the master generates each targetId as a random UUID, <b>unique across
 * jobs</b>, and the frame header carries only the targetId with no jobId, which keeps the wire cost at
 * nothing extra, so demux can only route by targetId. The registry is therefore keyed by targetId, which is
 * globally unique and cannot collide. The {@code jobId} serves only {@link #unregisterJob} for grouped
 * reclamation, since this is a resident worker table and a job reaching a terminal state has all of its
 * targets removed.
 *
 * <p><b>Each target carries its DataSchema</b>: one port serves several stages whose targets have different
 * row schemas, so a downstream registers with its own inbound meta and the demux takes the serializer for
 * that targetId. One global meta would not do.
 *
 * <p>It reproduces the register and await rendezvous semantics of {@link
 * io.rowweave.sched.LocalShuffleRegistry}: a frame may arrive before the downstream registers, so await
 * blocks until the registration arrives or it times out, which neither fails nor hangs. During execution it
 * only holds references to make the rendezvous; when the owning server closes, every wait is cancelled and
 * every registered channel is poisoned, so a handler cannot block teardown. A stage reaching a normal
 * terminal state deletes its target exactly; a cancel or timeout keeps only a bounded TTL tombstone without
 * the large Entry reference, and the worker heartbeat clears it.
 */
public final class DataInboundRegistry implements AutoCloseable {

    /** The registration: the downstream inbound channel plus the row meta of that inbound edge. */
    public static final class Entry {
        private final String jobId;
        public final MultiProducerChannel channel;
        public final DataSchema dataSchema;
        /** The total number of remote logical producers for this target, used to hand out a conservative initial credit per connection. */
        public final int remoteProducerCount;
        /** The wire byte budget of this target's merged channel. */
        public final long windowBytes;

        Entry(String jobId, MultiProducerChannel channel, DataSchema dataSchema,
              int remoteProducerCount, long windowBytes) {
            this.jobId = jobId;
            this.channel = channel;
            this.dataSchema = dataSchema;
            this.remoteProducerCount = remoteProducerCount;
            this.windowBytes = windowBytes;
        }
    }

    private static final class Slot {
        final ReentrantLock lock = new ReentrantLock();
        final Condition registered = lock.newCondition();
        volatile Entry entry;  // Set by the downstream registration, volatile so the fast path of await reads it without the lock
        volatile RuntimeException cancelled;
        volatile long terminalAt;
    }

    private static final class JobTombstone {
        final IllegalStateException cause;
        final long terminalAt;

        JobTombstone(String jobId, long terminalAt) {
            this.cause = new IllegalStateException("data inbound job " + jobId + " is terminal");
            this.terminalAt = terminalAt;
        }
    }

    private final Map<UUID, Slot> slots = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    // jobId to every targetId registered by that job, used by unregisterJob, held in a concurrency-safe set.
    private final Map<String, Set<UUID>> jobTargets = new ConcurrentHashMap<>();
    // Protected by lifecycleLock; a job in a terminal state rejects a late target within its retention window, and the LinkedHashMap keeps the eviction order bounded.
    private final Map<String, JobTombstone> terminalJobs = new LinkedHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final RuntimeException shutdownCause = new IllegalStateException("data inbound registry closed");
    private final long retentionMs;
    private final int maxEntries;
    private final LongSupplier clock;

    public DataInboundRegistry() {
        this(Long.getLong("rowweave.worker.rendezvousRetentionMs",
                        Long.getLong("rowweave.worker.assignmentRetentionMs", 300_000L)),
                Integer.getInteger("rowweave.worker.rendezvousMaxEntries", 100_000),
                System::currentTimeMillis);
    }

    DataInboundRegistry(long retentionMs, int maxEntries, LongSupplier clock) {
        if (retentionMs < 1 || maxEntries < 1 || clock == null) {
            throw new IllegalArgumentException("invalid data inbound registry bounds");
        }
        this.retentionMs = retentionMs;
        this.maxEntries = maxEntries;
        this.clock = clock;
    }

    boolean isClosed() {
        return closed.get();
    }

    private Slot slotOf(UUID targetId) {
        ensureOpen();
        Slot existing = slots.get(targetId);
        if (existing != null) {
            return existing;
        }
        synchronized (lifecycleLock) {
            ensureOpen();
            existing = slots.get(targetId);
            if (existing != null) {
                return existing;
            }
            purgeExpiredLocked(clock.getAsLong());
            if (slots.size() >= maxEntries) {
                throw new IllegalStateException("data inbound registry is full: entries="
                        + slots.size() + " maxEntries=" + maxEntries);
            }
            Slot created = new Slot();
            slots.put(targetId, created);
            return created;
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw shutdownCause;
        }
    }

    /**
     * Registered by a downstream stage after it has built the inbound channel, keyed globally uniquely by
     * targetId, with the jobId kept for reclamation. The first complete contract for a target wins; a
     * registration identical to it is idempotent, and a conflicting one must neither replace the Entry already
     * published nor pollute the ownership of the job.
     */
    public void register(UUID targetId, String jobId, MultiProducerChannel channel, DataSchema dataSchema) {
        register(targetId, jobId, channel, dataSchema, 1, 0L);
    }

    /** A mux target also declares its remote producer count and its consumption window when it registers. */
    public void register(UUID targetId, String jobId, MultiProducerChannel channel, DataSchema dataSchema,
                         int remoteProducerCount, long windowBytes) {
        if (targetId == null || jobId == null || channel == null || dataSchema == null) {
            throw new IllegalArgumentException("targetId, jobId, channel and dataSchema must not be null");
        }
        if (remoteProducerCount <= 0) {
            throw new IllegalArgumentException("remoteProducerCount must be > 0");
        }
        if (windowBytes < 0) {
            throw new IllegalArgumentException("windowBytes must be >= 0");
        }
        if (closed.get()) {
            channel.fail(shutdownCause);
            throw new IllegalStateException("cannot register target on closed data inbound registry");
        }
        Entry candidate = new Entry(jobId, channel, dataSchema, remoteProducerCount, windowBytes);
        Slot slot;
        try {
            slot = slotOf(targetId);
        } catch (RuntimeException full) {
            channel.fail(full);
            throw full;
        }
        RuntimeException rejection = null;
        boolean poisonNewcomer = false;
        slot.lock.lock();
        try {
            if (closed.get()) {
                slot.cancelled = shutdownCause;
                slot.terminalAt = clock.getAsLong();
                slot.registered.signalAll();
                rejection = shutdownCause;
                poisonNewcomer = true;
            } else if (slot.cancelled != null) {
                rejection = slot.cancelled;
                poisonNewcomer = true;
            } else {
                Entry existing = slot.entry;
                synchronized (lifecycleLock) {
                    if (closed.get()) {
                        slot.cancelled = shutdownCause;
                        slot.terminalAt = clock.getAsLong();
                        slot.registered.signalAll();
                        rejection = shutdownCause;
                        poisonNewcomer = true;
                    } else {
                        JobTombstone terminalJob = terminalJobs.get(jobId);
                        if (terminalJob != null) {
                            rejection = terminalJob.cause;
                            poisonNewcomer = existing == null || existing.channel != channel;
                            if (existing == null) {
                                slot.cancelled = terminalJob.cause;
                                slot.terminalAt = terminalJob.terminalAt;
                                slot.registered.signalAll();
                            }
                        } else if (existing == null) {
                            jobTargets.computeIfAbsent(jobId, j -> ConcurrentHashMap.newKeySet()).add(targetId);
                            slot.entry = candidate;
                            slot.registered.signalAll();
                            return;
                        } else if (sameRegistration(existing, candidate)) {
                            slot.registered.signalAll();
                            return;
                        } else {
                            rejection = new IllegalStateException(
                                    "data inbound target " + targetId + " has a conflicting registration");
                            poisonNewcomer = existing.channel != channel;
                        }
                    }
                }
            }
        } finally {
            slot.lock.unlock();
        }
        // The terminal callback of a newcomer is external code and must not be called while holding the registry slot lock; a conflict on the same channel must not poison the winner.
        if (poisonNewcomer) {
            channel.fail(rejection);
        }
        throw rejection;
    }

    private static boolean sameRegistration(Entry existing, Entry candidate) {
        return existing.channel == candidate.channel
                && existing.jobId.equals(candidate.jobId)
                && existing.remoteProducerCount == candidate.remoteProducerCount
                && existing.windowBytes == candidate.windowBytes
                && sameSchema(existing.dataSchema, candidate.dataSchema);
    }

    private static boolean sameSchema(DataSchema left, DataSchema right) {
        if (left == right) {
            return true;
        }
        if (left.getFieldCount() != right.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < left.getFieldCount(); i++) {
            io.rowweave.Field a = left.getField(i);
            io.rowweave.Field b = right.getField(i);
            if (!Objects.equals(a.getName(), b.getName())
                    || !Objects.equals(a.getAlias(), b.getAlias())
                    || a.getDataType() != b.getDataType()) {
                return false;
            }
        }
        return true;
    }

    /**
     * The server demux looks up the downstream registration by targetId, and when nothing is registered it
     * blocks until the registration arrives or the timeout is reached. A timeout throws {@link
     * IllegalStateException} rather than hanging, the same as the socket accept guard and
     * LocalShuffleRegistry.
     */
    public Entry await(UUID targetId, long timeoutMillis) {
        if (closed.get()) {
            throw shutdownCause;
        }
        Slot slot = slotOf(targetId);
        RuntimeException cancelled = slot.cancelled;
        if (cancelled != null) {
            throw cancelled;
        }
        Entry fast = slot.entry;
        if (fast != null) {
            return fast;
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        slot.lock.lock();
        try {
            while (slot.entry == null) {
                if (slot.cancelled != null) {
                    throw slot.cancelled;
                }
                if (closed.get()) {
                    throw shutdownCause;
                }
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    IllegalStateException timeout = new IllegalStateException("data inbound target " + targetId
                            + " not registered within " + timeoutMillis + "ms (not hang)");
                    slot.cancelled = timeout;
                    slot.terminalAt = clock.getAsLong();
                    slot.registered.signalAll();
                    throw timeout;
                }
                try {
                    slot.registered.await(remain, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted awaiting data inbound target", ie);
                }
            }
            if (slot.cancelled != null) {
                throw slot.cancelled;
            }
            return slot.entry;
        } finally {
            slot.lock.unlock();
        }
    }

    /** Cancels one target directly; both the current and a future await, and a late register, observe the same terminal state. */
    public void cancel(UUID targetId, RuntimeException cause) {
        if (cause == null) {
            throw new IllegalArgumentException("cause must not be null");
        }
        if (closed.get()) {
            return;
        }
        Slot slot;
        try {
            slot = slotOf(targetId);
        } catch (IllegalStateException closing) {
            if (closed.get()) {
                return;
            }
            throw closing;
        }
        MultiProducerChannel channel = null;
        slot.lock.lock();
        try {
            if (slot.cancelled == null) {
                slot.cancelled = cause;
            }
            if (slot.entry != null) {
                channel = slot.entry.channel;
                slot.entry = null;  // A tombstone must not hold on to a channel or the DataRows inside a queue
            }
            slot.terminalAt = clock.getAsLong();
            slot.registered.signalAll();
        } finally {
            slot.lock.unlock();
        }
        if (channel != null) {
            channel.fail(slot.cancelled);
        }
    }

    /** Job terminal state: remove every target the job registered, which keeps the resident table from leaking and stops a stale target from being reused across jobs. Idempotent. */
    public void unregisterJob(String jobId) {
        if (jobId == null) {
            throw new IllegalArgumentException("jobId must not be null");
        }
        Set<UUID> targets;
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return;
            }
            long now = clock.getAsLong();
            purgeExpiredLocked(now);
            if (!terminalJobs.containsKey(jobId)) {
                terminalJobs.put(jobId, new JobTombstone(jobId, now));
                trimTerminalJobsLocked();
            }
            targets = jobTargets.remove(jobId);
        }
        if (targets != null) {
            for (UUID t : targets) {
                unregister(t);
            }
        }
    }

    private void unregister(UUID targetId) {
        Slot slot = slots.get(targetId);
        if (slot == null) {
            return;
        }
        slot.lock.lock();
        try {
            if (slot.cancelled != null) {
                return;
            }
            slot.entry = null;
            synchronized (lifecycleLock) {
                slots.remove(targetId, slot);
            }
        } finally {
            slot.lock.unlock();
        }
    }

    public void purgeExpired() {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return;
            }
            purgeExpiredLocked(clock.getAsLong());
        }
    }

    private void purgeExpiredLocked(long now) {
        long cutoff = now - retentionMs;
        for (Map.Entry<UUID, Slot> entry : slots.entrySet()) {
            Slot slot = entry.getValue();
            if (slot.terminalAt > 0 && slot.terminalAt <= cutoff && slot.entry == null) {
                slots.remove(entry.getKey(), slot);
            }
        }
        Iterator<Map.Entry<String, JobTombstone>> terminal = terminalJobs.entrySet().iterator();
        while (terminal.hasNext()) {
            if (terminal.next().getValue().terminalAt <= cutoff) {
                terminal.remove();
            }
        }
    }

    private void trimTerminalJobsLocked() {
        Iterator<String> oldest = terminalJobs.keySet().iterator();
        while (terminalJobs.size() > maxEntries && oldest.hasNext()) {
            oldest.next();
            oldest.remove();
        }
    }

    public int entryCount() {
        return slots.size();
    }

    int activeEntryCount() {
        int count = 0;
        for (Slot slot : slots.values()) {
            if (slot.entry != null) count++;
        }
        return count;
    }

    int jobCount() {
        return jobTargets.size();
    }

    /**
     * Teardown of the worker's data server: wake the awaits of unregistered targets and poison every
     * registered channel, which releases a handler blocked on send. Idempotent. The registry lives as long as
     * the WorkerDataServer it belongs to, so registration is not allowed after closing, and both the main table
     * and its indexes are left empty.
     */
    @Override
    public void close() {
        Map<UUID, Slot> snapshot;
        synchronized (lifecycleLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            snapshot = new HashMap<>(slots);
            slots.clear();
            jobTargets.clear();
            terminalJobs.clear();
        }
        Set<MultiProducerChannel> channels = ConcurrentHashMap.newKeySet();
        for (Slot slot : snapshot.values()) {
            slot.lock.lock();
            try {
                if (slot.entry != null) {
                    channels.add(slot.entry.channel);
                    slot.entry = null;
                }
                if (slot.cancelled == null) {
                    slot.cancelled = shutdownCause;
                }
                slot.terminalAt = clock.getAsLong();
                slot.registered.signalAll();
            } finally {
                slot.lock.unlock();
            }
        }
        for (MultiProducerChannel channel : channels) {
            channel.fail(shutdownCause);
        }
    }
}
