package io.rowweave.cluster;

import io.rowweave.run.StageAssignmentManifest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** bounded, cross-Worker cancellation under one caller-owned deadline. */
final class StageCancellationCoordinator implements AutoCloseable {
    private static final int MAX_ASSIGNMENTS = StageAssignmentManifest.MAX_ASSIGNMENTS;
    private static final AtomicInteger THREAD_SEQUENCE = new AtomicInteger();

    @FunctionalInterface
    interface CancelCall {
        void cancel() throws Exception;
    }

    static final class WorkerBatch {
        private final String workerKey;
        private final List<CancelCall> calls;
        private final Consumer<Throwable> incompleteHandler;

        private WorkerBatch(String workerKey, List<CancelCall> calls,
                            Consumer<Throwable> incompleteHandler) {
            if (workerKey == null || workerKey.isEmpty()) {
                throw new IllegalArgumentException("worker key is required");
            }
            if (calls == null || calls.isEmpty()) {
                throw new IllegalArgumentException("cancellation calls are required");
            }
            if (incompleteHandler == null) {
                throw new IllegalArgumentException("incomplete handler is required");
            }
            List<CancelCall> copy = new ArrayList<>(calls.size());
            for (CancelCall call : calls) {
                if (call == null) {
                    throw new IllegalArgumentException("cancellation call is required");
                }
                copy.add(call);
            }
            this.workerKey = workerKey;
            this.calls = Collections.unmodifiableList(copy);
            this.incompleteHandler = incompleteHandler;
        }

        static WorkerBatch of(String workerKey, List<CancelCall> calls,
                              Consumer<Throwable> incompleteHandler) {
            return new WorkerBatch(workerKey, calls, incompleteHandler);
        }
    }

    static final class Result {
        private final int requestedAssignments;
        private final int acknowledgedAssignments;
        private final int incompleteWorkers;
        private final boolean deadlineExceeded;

        private Result(int requestedAssignments, int acknowledgedAssignments,
                       int incompleteWorkers, boolean deadlineExceeded) {
            this.requestedAssignments = requestedAssignments;
            this.acknowledgedAssignments = acknowledgedAssignments;
            this.incompleteWorkers = incompleteWorkers;
            this.deadlineExceeded = deadlineExceeded;
        }

        boolean complete() {
            return !deadlineExceeded && incompleteWorkers == 0
                    && acknowledgedAssignments == requestedAssignments;
        }

        int requestedAssignments() { return requestedAssignments; }
        int acknowledgedAssignments() { return acknowledgedAssignments; }
        int incompleteWorkers() { return incompleteWorkers; }
        boolean deadlineExceeded() { return deadlineExceeded; }

        static Result incompleteBeforeDispatch(int requestedAssignments,
                                               int incompleteWorkers) {
            return new Result(requestedAssignments, 0, incompleteWorkers, true);
        }

        @Override
        public String toString() {
            return "StageCancellationResult{requested=" + requestedAssignments
                    + ", acknowledged=" + acknowledgedAssignments
                    + ", incompleteWorkers=" + incompleteWorkers
                    + ", deadlineExceeded=" + deadlineExceeded + '}';
        }
    }

    private static final class Outcome {
        final WorkerBatch batch;
        final int acknowledged;
        final Throwable failure;

        Outcome(WorkerBatch batch, int acknowledged, Throwable failure) {
            this.batch = batch;
            this.acknowledged = acknowledged;
            this.failure = failure;
        }
    }

    private static final class Submitted {
        final WorkerBatch batch;
        final Future<Outcome> future;
        boolean collected;

        Submitted(WorkerBatch batch, Future<Outcome> future) {
            this.batch = batch;
            this.future = future;
        }
    }

    private final ThreadPoolExecutor executor;
    private final AtomicBoolean closed = new AtomicBoolean();

    StageCancellationCoordinator(int parallelism) {
        if (parallelism < 1 || parallelism > MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException(
                    "cancellation parallelism must be in [1," + MAX_ASSIGNMENTS + "]");
        }
        executor = new ThreadPoolExecutor(
                parallelism, parallelism, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_ASSIGNMENTS), runnable -> {
                    Thread thread = new Thread(runnable,
                            "rowweave-stage-cancel-" + THREAD_SEQUENCE.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    /** Maintenance shares the already budgeted pool, but never the caller's RPC monitor. */
    boolean runControlUntil(CancelCall call, Runnable abortIo, long deadlineNanos) {
        if (call == null || abortIo == null) {
            throw new IllegalArgumentException("control call and I/O abort are required");
        }
        java.util.concurrent.FutureTask<Void> task = new java.util.concurrent.FutureTask<>(() -> {
            if (deadlineNanos - System.nanoTime() <= 0L) {
                throw new java.util.concurrent.TimeoutException("control deadline exceeded");
            }
            call.cancel();
            return null;
        });
        try {
            if (closed.get() || deadlineNanos - System.nanoTime() <= 0L) return false;
            executor.execute(task);
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L) return false;
            task.get(remaining, TimeUnit.NANOSECONDS);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | java.util.concurrent.TimeoutException
                 | java.util.concurrent.RejectedExecutionException failed) {
            return false;
        } finally {
            task.cancel(true);
            executor.remove(task); // A saturated pool must not retain canceled maintenance work.
            abortIo.run(); // Interrupt alone cannot release a blocked socket write.
        }
    }

    Result cancel(List<WorkerBatch> batches, long deadlineNanos) {
        if (batches == null) {
            throw new IllegalArgumentException("cancellation batches are required");
        }
        if (closed.get()) {
            throw new IllegalStateException("stage cancellation coordinator is closed");
        }
        int requested = validateAndCount(batches);
        if (requested == 0) {
            return new Result(0, 0, 0, false);
        }

        ExecutorCompletionService<Outcome> completions =
                new ExecutorCompletionService<>(executor,
                        new ArrayBlockingQueue<>(batches.size()));
        List<Submitted> submitted = new ArrayList<>(batches.size());
        java.util.Map<Future<Outcome>, Submitted> submittedByFuture =
                new IdentityHashMap<>();
        int incompleteWorkers = 0;
        for (WorkerBatch batch : batches) {
            try {
                Submitted handle = new Submitted(batch,
                        completions.submit(() -> runBatch(batch, deadlineNanos)));
                submitted.add(handle);
                submittedByFuture.put(handle.future, handle);
            } catch (RuntimeException rejected) {
                notifyIncomplete(batch, rejected);
                incompleteWorkers++;
            }
        }

        int acknowledged = 0;
        int remaining = submitted.size();
        boolean deadlineExceeded = false;
        while (remaining > 0) {
            long waitNanos = deadlineNanos - System.nanoTime();
            if (waitNanos <= 0L) {
                deadlineExceeded = true;
                break;
            }
            Future<Outcome> completed;
            try {
                completed = completions.poll(waitNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                deadlineExceeded = true;
                break;
            }
            if (completed == null) {
                deadlineExceeded = true;
                break;
            }
            Submitted handle = submittedByFuture.get(completed);
            if (handle != null) handle.collected = true;
            Outcome outcome = outcome(completed, handle == null ? null : handle.batch);
            remaining--;
            acknowledged += outcome.acknowledged;
            if (outcome.failure != null) {
                notifyIncomplete(outcome.batch, outcome.failure);
                incompleteWorkers++;
            }
        }

        if (remaining > 0) {
            for (Submitted handle : submitted) {
                if (handle.collected) continue;
                handle.collected = true;
                handle.future.cancel(true);
                notifyIncomplete(handle.batch,
                        new IllegalStateException("cancellation deadline exceeded"));
                incompleteWorkers++;
            }
        }
        return new Result(requested, acknowledged, incompleteWorkers, deadlineExceeded);
    }

    private static int validateAndCount(List<WorkerBatch> batches) {
        int requested = 0;
        Set<String> workerKeys = new HashSet<>();
        for (WorkerBatch batch : batches) {
            if (batch == null) {
                throw new IllegalArgumentException("cancellation batch is required");
            }
            if (!workerKeys.add(batch.workerKey)) {
                throw new IllegalArgumentException(
                        "duplicate cancellation worker batch");
            }
            requested = Math.addExact(requested, batch.calls.size());
            if (requested > MAX_ASSIGNMENTS) {
                throw new IllegalArgumentException(
                        "cancellation assignments exceed limit " + MAX_ASSIGNMENTS);
            }
        }
        return requested;
    }

    private static Outcome runBatch(WorkerBatch batch, long deadlineNanos) {
        int acknowledged = 0;
        try {
            for (CancelCall call : batch.calls) {
                if (deadlineNanos - System.nanoTime() <= 0L) {
                    throw new IllegalStateException("cancellation deadline exceeded");
                }
                call.cancel();
                acknowledged++;
            }
            return new Outcome(batch, acknowledged, null);
        } catch (Throwable failure) {
            return new Outcome(batch, acknowledged, failure);
        }
    }

    private static Outcome outcome(Future<Outcome> future, WorkerBatch batch) {
        try {
            return future.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new Outcome(batch, 0, interrupted);
        } catch (ExecutionException | RuntimeException failure) {
            return new Outcome(batch, 0, failure);
        }
    }

    private static void notifyIncomplete(WorkerBatch batch, Throwable failure) {
        if (batch == null) return;
        try {
            batch.incompleteHandler.accept(failure);
        } catch (Throwable ignored) {
            // Cleanup proof is already incomplete; callback failure cannot restore it.
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            executor.shutdownNow();
        }
    }
}
