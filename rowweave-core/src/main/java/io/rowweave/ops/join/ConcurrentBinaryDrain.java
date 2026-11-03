package io.rowweave.ops.join;

import io.rowweave.mem.SortBuffer;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** The two-input drain shared by SortMergeJoin and CoGroup; each side fills only its own SortBuffer. */
final class ConcurrentBinaryDrain {

    private static final AtomicLong threadIds = new AtomicLong();
    private static final ThreadPoolExecutor helpers = new ThreadPoolExecutor(
            0, BinaryDrainLimiter.executorCapacity(), 100L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(), runnable -> {
                Thread thread = new Thread(runnable,
                        "rowweave-binary-drain-" + threadIds.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });

    private ConcurrentBinaryDrain() {
    }

    static StreamEvent drain(RowStream left, SortBuffer leftBuffer,
                             RowStream right, SortBuffer rightBuffer) {
        return drain(left, leftBuffer, right, rightBuffer, helpers);
    }

    /** The package-private executor seam exists only to make a partially submitted failure deterministic in tests; production always uses the unqueued helper pool. */
    static StreamEvent drain(RowStream left, SortBuffer leftBuffer,
                             RowStream right, SortBuffer rightBuffer,
                             Executor executor) {
        boolean enabled = Boolean.parseBoolean(
                System.getProperty("rowweave.binary.concurrentDrain", "true"));
        if (!enabled || !left.supportsCancellation() || !right.supportsCancellation()
                || !BinaryDrainLimiter.tryAcquirePair()) {
            StreamEvent terminal = drainOne(left, leftBuffer);
            return terminal != null ? terminal : drainOne(right, rightBuffer);
        }

        AtomicReference<Terminal> first = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(2);
        DrainTask[] tasks = new DrainTask[2];
        tasks[0] = new DrainTask(
                () -> drainSide(left, leftBuffer, right, tasks, 1, first), completed);
        tasks[1] = new DrainTask(
                () -> drainSide(right, rightBuffer, left, tasks, 0, first), completed);

        boolean callerInterrupted = false;
        try {
            executor.execute(tasks[0]);
            executor.execute(tasks[1]);
            while (completed.getCount() > 0) {
                try {
                    completed.await();
                } catch (InterruptedException interrupted) {
                    callerInterrupted = true;
                    CancellationException cancellation = callerCancellation(interrupted);
                    if (first.compareAndSet(null, Terminal.failure(cancellation))) {
                        safeCancel(left, cancellation, first);
                        safeCancel(right, cancellation, first);
                    }
                    cancelTasks(tasks);
                }
            }
        } catch (Throwable submissionFailure) {
            if (first.compareAndSet(null, Terminal.failure(submissionFailure))) {
                safeCancel(left, submissionFailure, first);
                safeCancel(right, submissionFailure, first);
            } else {
                recordSecondary(first, submissionFailure);
            }
            // Failing to submit zero tasks is still loud; once any task has been submitted, falling back to the serial path that has already consumed input is forbidden outright.
            cancelTasks(tasks);
        } finally {
            callerInterrupted |= awaitCompleted(completed);
            BinaryDrainLimiter.releasePair();
            if (callerInterrupted) {
                Thread.currentThread().interrupt();
            }
        }

        Terminal terminal = first.get();
        if (terminal == null) {
            return null;
        }
        if (terminal.failure != null) {
            rethrow(terminal.failure);
        }
        return terminal.event;
    }

    private static void drainSide(RowStream input, SortBuffer buffer, RowStream sibling,
                                  DrainTask[] tasks, int siblingIndex,
                                  AtomicReference<Terminal> first) {
        try {
            StreamEvent terminal = drainOne(input, buffer);
            if (terminal != null) {
                Throwable cause = terminal.isError() ? terminal.cause()
                        : new CancellationException("upstream cancelled binary drain");
                if (first.compareAndSet(null, Terminal.event(terminal))) {
                    safeCancel(sibling, cause, first);
                    tasks[siblingIndex].cancelExecution();
                } else {
                    recordSecondary(first, cause);
                }
            }
        } catch (Throwable failure) {
            if (first.compareAndSet(null, Terminal.failure(failure))) {
                safeCancel(sibling, failure, first);
                tasks[siblingIndex].cancelExecution();
            } else {
                recordSecondary(first, failure);
            }
        }
    }

    private static StreamEvent drainOne(RowStream input, SortBuffer buffer) {
        StreamEvent event;
        while ((event = input.next()) != null) {
            if (event.isError() || event.isCancel()) {
                return event;
            }
            if (event.isRecord()) {
                buffer.putRow(event.row());
            }
        }
        return null;
    }

    private static CancellationException callerCancellation(InterruptedException interrupted) {
        CancellationException cancellation = new CancellationException("binary drain caller interrupted");
        cancellation.initCause(interrupted);
        return cancellation;
    }

    private static void cancelTasks(DrainTask[] tasks) {
        for (DrainTask task : tasks) {
            task.cancelExecution();
        }
    }

    private static boolean awaitCompleted(CountDownLatch completed) {
        boolean interrupted = false;
        while (completed.getCount() > 0) {
            try {
                completed.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        return interrupted;
    }

    private static void safeCancel(RowStream stream, Throwable cause, AtomicReference<Terminal> first) {
        try {
            stream.cancel(cause);
        } catch (Throwable cancelFailure) {
            recordSecondary(first, cancelFailure);
        }
    }

    private static void recordSecondary(AtomicReference<Terminal> first, Throwable secondary) {
        Terminal terminal = first.get();
        if (terminal == null || secondary == null) {
            return;
        }
        Throwable primary = terminal.failure;
        if (primary == null && terminal.event != null && terminal.event.isError()) {
            primary = terminal.event.cause();
        }
        if (primary != null && primary != secondary) {
            try {
                primary.addSuppressed(secondary);
            } catch (IllegalArgumentException ignored) {
                // self-suppression race; the primary remains intact
            }
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    private static final class Terminal {
        final StreamEvent event;
        final Throwable failure;

        private Terminal(StreamEvent event, Throwable failure) {
            this.event = event;
            this.failure = failure;
        }

        static Terminal event(StreamEvent event) {
            return new Terminal(event, null);
        }

        static Terminal failure(Throwable failure) {
            return new Terminal(null, failure);
        }
    }

    /** Cancel-before-run and cancel-during-run are tracked apart, and the latch counts down once only when the task has really exited. */
    static final class DrainTask implements Runnable {
        private static final int NEW = 0;
        private static final int RUNNING = 1;
        private static final int EXITED = 2;

        private final Runnable body;
        private final CountDownLatch completed;
        private int state = NEW;
        private Thread runner;

        DrainTask(Runnable body, CountDownLatch completed) {
            this.body = body;
            this.completed = completed;
        }

        @Override
        public void run() {
            synchronized (this) {
                if (state != NEW) {
                    return;  // cancel-before-run already handles EXITED and countDown
                }
                state = RUNNING;
                runner = Thread.currentThread();
            }
            try {
                body.run();
            } finally {
                synchronized (this) {
                    runner = null;
                    state = EXITED;
                    completed.countDown();
                }
            }
        }

        synchronized void cancelExecution() {
            if (state == NEW) {
                state = EXITED;
                completed.countDown();
                return;
            }
            if (state == RUNNING && runner != null) {
                // Shares the lifecycle lock with the finally block: when interrupt is called, the worker still belongs to the current task.
                runner.interrupt();
            }
        }
    }

}
