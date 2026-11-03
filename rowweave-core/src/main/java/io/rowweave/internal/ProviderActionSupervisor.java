package io.rowweave.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-wide fixed execution boundary for every call into provider code.
 * A timed-out action permanently poisons this JVM; no replacement lanes exist.
 */
public final class ProviderActionSupervisor {

    public static final int MAX_CAPACITY = 64;
    private static final Object OWNER_LOCK = new Object();
    private static volatile ProviderActionSupervisor owner;

    private final int capacity;
    private final Semaphore permits;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean poisoned = new AtomicBoolean();

    private ProviderActionSupervisor(int requestedCapacity) {
        if (requestedCapacity <= 0) {
            throw new IllegalArgumentException(
                    "provider action supervisor capacity must be positive");
        }
        this.capacity = Math.min(requestedCapacity, MAX_CAPACITY);
        this.permits = new Semaphore(capacity, true);
        AtomicInteger sequence = new AtomicInteger();
        ThreadFactory threads = task -> {
            Thread thread = new Thread(task,
                    "rowweave-provider-action-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.executor = new ThreadPoolExecutor(
                capacity, capacity, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity), threads,
                new ThreadPoolExecutor.AbortPolicy());
        this.executor.prestartAllCoreThreads();
    }

    public static ProviderActionSupervisor processWide(int requestedCapacity) {
        ProviderActionSupervisor current = owner;
        if (current != null) return current;
        synchronized (OWNER_LOCK) {
            if (owner == null) owner = new ProviderActionSupervisor(requestedCapacity);
            return owner;
        }
    }

    public int capacity() { return capacity; }

    public boolean isPoisoned() { return poisoned.get(); }

    public <T> T call(String actionId, long deadlineNanos, Callable<T> action) {
        requireAction(actionId, action);
        acquirePermit(deadlineNanos);
        try {
            return execute(actionId, deadlineNanos, action);
        } finally {
            permits.release();
        }
    }

    public AttemptPermit openAttempt(String attemptId, long deadlineNanos) {
        requireText(attemptId, "attemptId");
        acquirePermit(deadlineNanos);
        return new AttemptPermit(this, attemptId, deadlineNanos);
    }

    private <T> T execute(String actionId, long deadlineNanos,
                          Callable<T> action) {
        requireAvailable();
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L) throw timeout(actionId);
        final Future<T> future;
        try {
            future = executor.submit(action);
        } catch (java.util.concurrent.RejectedExecutionException unavailable) {
            requireAvailable();
            throw new IllegalStateException(
                    "PROVIDER_ACTION_SUPERVISOR_EXHAUSTED", unavailable);
        }
        try {
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeout) {
            future.cancel(true);
            throw timeout(actionId);
        } catch (InterruptedException interrupted) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "PROVIDER_ACTION_INTERRUPTED", interrupted);
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw new IllegalStateException("PROVIDER_ACTION_FAILED", cause);
        }
    }

    private void acquirePermit(long deadlineNanos) {
        requireAvailable();
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L) throw timeout("permit");
        try {
            if (!permits.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
                throw timeout("permit");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "PROVIDER_ACTION_INTERRUPTED", interrupted);
        }
        if (poisoned.get()) {
            permits.release();
            throw poisoned();
        }
    }

    private IllegalStateException timeout(String actionId) {
        if (poisoned.compareAndSet(false, true)) {
            executor.shutdownNow();
        }
        return new IllegalStateException(
                "PROVIDER_ACTION_TIMEOUT: " + actionId);
    }

    private void requireAvailable() {
        if (poisoned.get()) throw poisoned();
    }

    private static IllegalStateException poisoned() {
        return new IllegalStateException(
                "PROVIDER_ACTION_SUPERVISOR_POISONED");
    }

    private static void requireAction(String actionId, Callable<?> action) {
        requireText(actionId, "actionId");
        if (action == null) {
            throw new IllegalArgumentException("provider action is required");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isEmpty() || value.length() > 1024) {
            throw new IllegalArgumentException(field + " is invalid");
        }
    }

    public static final class AttemptPermit implements AutoCloseable {
        private final ProviderActionSupervisor supervisor;
        private final String attemptId;
        private final long deadlineNanos;
        private final AtomicBoolean closed = new AtomicBoolean();

        private AttemptPermit(ProviderActionSupervisor supervisor,
                              String attemptId, long deadlineNanos) {
            this.supervisor = supervisor;
            this.attemptId = attemptId;
            this.deadlineNanos = deadlineNanos;
        }

        public <T> T call(String actionId, Callable<T> action) {
            if (closed.get()) {
                throw new IllegalStateException(
                        "provider attempt permit is closed");
            }
            requireAction(actionId, action);
            return supervisor.execute(attemptId + ':' + actionId,
                    deadlineNanos, action);
        }

        public long deadlineNanos() { return deadlineNanos; }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                supervisor.permits.release();
            }
        }
    }
}
