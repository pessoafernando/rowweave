package io.rowweave.sched;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The in-process shuffle rendezvous <b>owned by one WorkerServer</b>. When two adjacent stages in the same
 * worker talk through a LocalChannel, the downstream stage builds its inbound {@link Channel} in advance and
 * registers it, and the upstream stage's FanoutSink looks it up by key through {@link #await}.
 *
 * <p>Comparable to the socket-side {@link io.rowweave.sched.net.ShuffleRegistry}, which registers endpoints,
 * except that what is registered here is <b>an in-process channel</b>, so the DataRow reference travels within
 * one JVM with no serialization. The key is {@code
 * jobId#downStageIndex#downInstanceIndex#edge}, which pins down one inbound edge of one instance of one
 * downstream stage.
 *
 * <p><b>Why it cannot hang</b>: the order of assignments is not the order in which threads run, so an
 * upstream thread may reach the rendezvous before the downstream thread does. {@link #await} therefore blocks
 * for the registration and throws on timeout, as {@code ShuffleRegistry.awaitPartitions} and the StageTask
 * ACCEPT_TIMEOUT do. The stagePool of WorkerServer is unbounded, being a cachedThreadPool, so the upstream and
 * downstream stages of the same worker each take a thread and run concurrently, and await cannot deadlock for
 * want of a thread.
 *
 * <p>A normal inbound terminal state keeps a success tombstone with a bounded TTL, which absorbs a late
 * producer that has not finished openTarget; a cancel or timeout clears the channel at once and keeps a
 * failure tombstone. The worker heartbeat clears both kinds once they expire. The registry is still not
 * responsible for closing channels in the normal case, only for the rendezvous and for poisoning on failure.
 * A worker reaching a terminal state is the exception, where every active channel must be poisoned and its
 * strong reference dropped.
 */
public final class LocalShuffleRegistry implements AutoCloseable {

    private static final class Slot {
        final ReentrantLock lock = new ReentrantLock();
        final Condition registered = lock.newCondition();
        volatile Channel channel;  // Set by the downstream registration, volatile so the fast path of await reads it without the lock
        volatile RuntimeException cancelled;
        volatile boolean completed;
        volatile long terminalAt;
    }

    private final Map<String, Slot> slots = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private final long retentionMs;
    private final int maxEntries;
    private final LongSupplier clock;
    private volatile boolean closed;
    private final RuntimeException shutdownCause =
            new IllegalStateException("local shuffle registry closed");

    public LocalShuffleRegistry() {
        this(Long.getLong("rowweave.worker.rendezvousRetentionMs",
                        Long.getLong("rowweave.worker.assignmentRetentionMs", 300_000L)),
                Integer.getInteger("rowweave.worker.rendezvousMaxEntries", 100_000),
                System::currentTimeMillis);
    }

    LocalShuffleRegistry(long retentionMs, int maxEntries, LongSupplier clock) {
        if (retentionMs < 1 || maxEntries < 1 || clock == null) {
            throw new IllegalArgumentException("invalid local rendezvous registry bounds");
        }
        this.retentionMs = retentionMs;
        this.maxEntries = maxEntries;
        this.clock = clock;
    }

    private Slot slotOf(String key) {
        ensureOpen();
        Slot existing = slots.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (lifecycleLock) {
            ensureOpen();
            existing = slots.get(key);
            if (existing != null) {
                return existing;
            }
            purgeExpiredLocked(clock.getAsLong());
            if (slots.size() >= maxEntries) {
                throw new IllegalStateException("local rendezvous registry is full: entries="
                        + slots.size() + " maxEntries=" + maxEntries);
            }
            Slot created = new Slot();
            slots.put(key, created);
            return created;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw shutdownCause;
        }
    }

    /**
     * Registered by a downstream stage after it has built the inbound channel, which wakes an upstream await
     * that may already be parked. An active key is bound to the first channel identity: repeated registration
     * from the same instance is idempotent, a different instance fails fast, and the reference already handed
     * to the upstream is never replaced.
     */
    public void register(String key, Channel channel) {
        if (channel == null) {
            throw new IllegalArgumentException("channel must not be null");
        }
        Slot slot;
        try {
            slot = slotOf(key);
        } catch (RuntimeException full) {
            channel.fail(full);
            throw full;
        }
        IllegalStateException duplicateFailure = null;
        slot.lock.lock();
        try {
            if (closed) {
                channel.fail(shutdownCause);
                throw shutdownCause;
            }
            if (slot.cancelled != null) {
                channel.fail(slot.cancelled);
                throw slot.cancelled;
            }
            if (slot.completed) {
                IllegalStateException late = new IllegalStateException(
                        "local shuffle channel '" + key + "' already completed");
                channel.fail(late);
                throw late;
            }
            if (slot.channel == null) {
                slot.channel = channel;
                slot.registered.signalAll();
                return;
            }
            if (slot.channel == channel) {
                slot.registered.signalAll();
                return;
            }
            duplicateFailure = new IllegalStateException(
                    "local shuffle channel '" + key + "' already registered with a different channel");
        } finally {
            slot.lock.unlock();
        }
        // The channel terminal callback is external code and must not be called while holding the registry slot lock.
        channel.fail(duplicateFailure);
        throw duplicateFailure;
    }

    /**
     * The upstream FanoutSink looks up the inbound channel the downstream registered, and when nothing is
     * registered it blocks until the registration arrives or the timeout is reached. A timeout throws {@link
     * IllegalStateException} rather than hanging, the same as the socket accept guard.
     */
    public Channel await(String key, long timeoutMillis) {
        ensureOpen();
        Slot slot = slotOf(key);
        RuntimeException cancelled = slot.cancelled;
        if (cancelled != null) {
            throw cancelled;
        }
        Channel fast = slot.channel;
        if (fast != null) {
            return fast;
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        slot.lock.lock();
        try {
            while (slot.channel == null) {
                if (closed) {
                    throw shutdownCause;
                }
                if (slot.cancelled != null) {
                    throw slot.cancelled;
                }
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    IllegalStateException timeout = new IllegalStateException("local shuffle channel '" + key
                            + "' not registered within " + timeoutMillis + "ms (not hang)");
                    slot.cancelled = timeout;
                    slot.terminalAt = clock.getAsLong();
                    slot.registered.signalAll();
                    throw timeout;
                }
                try {
                    slot.registered.await(remain, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted awaiting local shuffle channel", ie);
                }
            }
            if (slot.cancelled != null) {
                throw slot.cancelled;
            }
            return slot.channel;
        } finally {
            slot.lock.unlock();
        }
    }

    /** Cancels one rendezvous key directly; the cancelled terminal state cannot be overwritten by a late register. */
    public void cancel(String key, RuntimeException cause) {
        if (cause == null) {
            throw new IllegalArgumentException("cause must not be null");
        }
        if (closed) {
            return;
        }
        Slot slot;
        try {
            slot = slotOf(key);
        } catch (IllegalStateException closing) {
            if (closed) {
                return;
            }
            throw closing;
        }
        Channel channel = null;
        slot.lock.lock();
        try {
            if (slot.cancelled == null) {
                slot.cancelled = cause;
            }
            channel = slot.channel;
            slot.channel = null;  // A tombstone must not hold on to a channel or the DataRows inside a queue
            slot.terminalAt = clock.getAsLong();
            slot.registered.signalAll();
        } finally {
            slot.lock.unlock();
        }
        if (channel != null) {
            channel.fail(slot.cancelled);
        }
    }

    /**
     * The normal inbound terminal state: keep the already closed channel until its retention expires, so a
     * late producer that has not finished openTarget takes the same terminal reference and fails loudly,
     * instead of having a new empty slot created after removal and waiting until timeout. The tombstones of a
     * cancel or timeout are untouched by this call.
     */
    public void unregister(String key) {
        if (key == null || closed) {
            return;
        }
        Slot slot = slots.get(key);
        if (slot == null) {
            return;
        }
        slot.lock.lock();
        try {
            if (slot.cancelled != null) {
                return;
            }
            if (!slot.completed) {
                slot.completed = true;
                slot.terminalAt = clock.getAsLong();
                slot.registered.signalAll();
            }
        } finally {
            slot.lock.unlock();
        }
    }

    /** Called by the worker heartbeat and before a new slot is created; clears the success and failure tombstones whose retention has expired. */
    public void purgeExpired() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            purgeExpiredLocked(clock.getAsLong());
        }
    }

    private void purgeExpiredLocked(long now) {
        long cutoff = now - retentionMs;
        for (Map.Entry<String, Slot> entry : slots.entrySet()) {
            Slot slot = entry.getValue();
            if (slot.terminalAt > 0 && slot.terminalAt <= cutoff) {
                slots.remove(entry.getKey(), slot);
            }
        }
    }

    /** Visible to tests and diagnostics, and exposes no channel. */
    public int entryCount() {
        return slots.size();
    }

    int activeChannelCount() {
        int count = 0;
        for (Slot slot : slots.values()) {
            if (slot.channel != null) count++;
        }
        return count;
    }

    /** Worker terminal state: linearize the closure, drop every strong channel reference and wake every rendezvous waiter. */
    @Override
    public void close() {
        Map<String, Slot> snapshot;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = new HashMap<>(slots);
            slots.clear();
        }
        Set<Channel> channels = ConcurrentHashMap.newKeySet();
        for (Slot slot : snapshot.values()) {
            slot.lock.lock();
            try {
                if (slot.channel != null) {
                    channels.add(slot.channel);
                    slot.channel = null;
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
        for (Channel channel : channels) {
            channel.fail(shutdownCause);
        }
    }
}
