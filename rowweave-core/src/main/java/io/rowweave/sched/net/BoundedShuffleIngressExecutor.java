package io.rowweave.sched.net;

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Target ingress executor with no task queue, which rejects instead of running in the caller. */
final class BoundedShuffleIngressExecutor implements Executor {

    private final ThreadPoolExecutor delegate;

    BoundedShuffleIngressExecutor(int maxHandlers) {
        AtomicInteger sequence = new AtomicInteger();
        delegate = new ThreadPoolExecutor(0, maxHandlers, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), runnable -> {
                    Thread thread = new Thread(runnable,
                            "rowweave-wds-ingress-" + sequence.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(command);
    }

    int activeHandlers() { return delegate.getActiveCount(); }
    int peakHandlers() { return delegate.getLargestPoolSize(); }
    void shutdownNow() { delegate.shutdownNow(); }
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }
}
