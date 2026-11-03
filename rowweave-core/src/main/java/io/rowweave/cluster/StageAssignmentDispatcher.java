package io.rowweave.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bounded executor dedicated to stage assignment, which joins structurally and reports errors in a deterministic order. */
final class StageAssignmentDispatcher implements AutoCloseable {
    static final class DispatchException extends RuntimeException {
        DispatchException(Throwable primary) {
            super("stage assignment dispatch failed: " + primary.getMessage(), primary);
        }
    }

    private static final class Handle<T> {
        final CountDownLatch done = new CountDownLatch(1);
        volatile T result;
        volatile Throwable failure;
        volatile Future<?> future;
    }

    private final ExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();

    StageAssignmentDispatcher(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("assignment parallelism must be >= 1");
        }
        executor = Executors.newFixedThreadPool(parallelism, r -> {
            Thread thread = new Thread(r, "rowweave-stage-assign");
            thread.setDaemon(true);
            return thread;
        });
    }

    <T> List<T> invokeAll(List<? extends Callable<T>> calls) {
        if (calls == null || calls.isEmpty()) {
            return Collections.emptyList();
        }
        if (closed.get()) {
            throw new IllegalStateException("stage assignment dispatcher is closed");
        }
        List<Handle<T>> handles = new ArrayList<>(calls.size());
        for (int i = 0; i < calls.size(); i++) {
            handles.add(new Handle<>());
        }

        RejectedExecutionException rejection = null;
        for (int i = 0; i < calls.size(); i++) {
            Handle<T> handle = handles.get(i);
            Callable<T> call = calls.get(i);
            try {
                FutureTask<Void> task = new FutureTask<Void>(() -> {
                    try {
                        handle.result = call.call();
                    } catch (Throwable failure) {
                        handle.failure = failure;
                    }
                    return null;
                }) {
                    @Override
                    protected void done() {
                        handle.done.countDown();
                    }
                };
                handle.future = task;
                executor.execute(task);
            } catch (RejectedExecutionException rejected) {
                rejection = rejected;
                handle.failure = rejected;
                handle.done.countDown();
                for (int j = i + 1; j < handles.size(); j++) {
                    handles.get(j).failure = rejected;
                    handles.get(j).done.countDown();
                }
                cancelSubmitted(handles);
                break;
            }
        }

        boolean interrupted = false;
        for (Handle<T> handle : handles) {
            for (;;) {
                try {
                    handle.done.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                    cancelSubmitted(handles);
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        Throwable primary = rejection;
        if (primary == null && interrupted) {
            primary = new InterruptedException("interrupted awaiting stage assignment dispatch");
        }
        for (Handle<T> handle : handles) {
            if (handle.failure != null && primary == null) {
                primary = handle.failure;
            }
        }
        if (primary != null) {
            DispatchException dispatch = new DispatchException(primary);
            for (Handle<T> handle : handles) {
                if (handle.failure != null && handle.failure != primary
                        && !containsSuppressed(dispatch, handle.failure)) {
                    dispatch.addSuppressed(handle.failure);
                }
            }
            throw dispatch;
        }

        List<T> results = new ArrayList<>(handles.size());
        for (Handle<T> handle : handles) {
            results.add(handle.result);
        }
        return results;
    }

    private static boolean containsSuppressed(Throwable owner, Throwable candidate) {
        for (Throwable suppressed : owner.getSuppressed()) {
            if (suppressed == candidate) {
                return true;
            }
        }
        return false;
    }

    private static void cancelSubmitted(List<? extends Handle<?>> handles) {
        for (Handle<?> handle : handles) {
            Future<?> future = handle.future;
            if (future != null && !future.isDone()) {
                future.cancel(true);
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.shutdownNow();
        boolean interrupted = false;
        try {
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
