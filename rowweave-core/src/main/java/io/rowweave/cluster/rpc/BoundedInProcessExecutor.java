package io.rowweave.cluster.rpc;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** In-process RPC handler executor with no task queue and a stable admission rejection. */
final class BoundedInProcessExecutor {

    private static final long KEEP_ALIVE_SECONDS = 60L;

    private final ExecutorService delegate;
    private final int maxHandlers;
    private final AtomicInteger createdThreads;

    private BoundedInProcessExecutor(
            ExecutorService delegate, int maxHandlers, AtomicInteger createdThreads) {
        this.delegate = delegate;
        this.maxHandlers = maxHandlers;
        this.createdThreads = createdThreads;
    }

    static BoundedInProcessExecutor create(String threadPrefix, int maxHandlers) {
        if (threadPrefix == null || threadPrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("threadPrefix must not be blank");
        }
        requireMaxHandlers(maxHandlers);
        AtomicInteger sequence = new AtomicInteger();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                0,
                maxHandlers,
                KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable,
                            threadPrefix + sequence.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
        return new BoundedInProcessExecutor(executor, maxHandlers, sequence);
    }

    /** Injection constructor kept for compatibility; it accepts only a bounded pool provably using direct handoff with AbortPolicy. */
    static BoundedInProcessExecutor fromInjected(ExecutorService executor) {
        if (!(executor instanceof ThreadPoolExecutor)) {
            throw new IllegalArgumentException(
                    "in-process RPC pool must be a bounded ThreadPoolExecutor");
        }
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
        int maximum = threadPool.getMaximumPoolSize();
        requireMaxHandlers(maximum);
        if (!(threadPool.getQueue() instanceof SynchronousQueue)) {
            throw new IllegalArgumentException(
                    "in-process RPC pool must use SynchronousQueue direct handoff");
        }
        if (!(threadPool.getRejectedExecutionHandler()
                instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException(
                    "in-process RPC pool must use AbortPolicy rejection");
        }
        return new BoundedInProcessExecutor(executor, maximum, new AtomicInteger());
    }

    <T> Future<T> submit(Callable<T> task) {
        try {
            return delegate.submit(task);
        } catch (RejectedExecutionException rejected) {
            throw admissionFailure(rejected);
        }
    }

    Future<?> submit(Runnable task) {
        try {
            return delegate.submit(task);
        } catch (RejectedExecutionException rejected) {
            throw admissionFailure(rejected);
        }
    }

    void shutdownNow() {
        delegate.shutdownNow();
    }

    int maxHandlers() {
        return maxHandlers;
    }

    int createdThreadCount() {
        return createdThreads.get();
    }

    private IllegalStateException admissionFailure(RejectedExecutionException rejected) {
        if (delegate.isShutdown()) {
            return new IllegalStateException("in-process RPC endpoint is closed", rejected);
        }
        return new IllegalStateException(
                "in-process RPC admission exhausted: maxHandlers=" + maxHandlers, rejected);
    }

    private static void requireMaxHandlers(int maxHandlers) {
        if (maxHandlers < 1 || maxHandlers > 4096) {
            throw new IllegalArgumentException(
                    "maxHandlers must be in 1..4096, got " + maxHandlers);
        }
    }
}
