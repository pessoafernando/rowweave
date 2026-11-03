package io.rowweave.sched;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * A thread and its sub-graph: one push sub-graph gets one dedicated thread. The body pulls the upstream
 * RowStream in order and pushes the downstream RowSink, each adapted from its channel. When a channel is
 * full, send blocks, which is the backpressure.
 *
 * <p>{@code onFinish(failure)} always runs after the body ends, whether normally, cancelled or failed. A
 * middle stage uses it to close out its downstream channel, so the downstream observes the end of the stream
 * and never hangs forever. <b>When the body throws, failure is non-null</b>, and onFinish posts an error event
 * downstream rather than a bare EOS, so a downstream breaker or binary terminates on the error instead of
 * producing from incomplete data. The trailing Sink stage has no downstream channel, so its onFinish is empty.
 */
public final class Task {

    private static final ThreadLocal<Task> current = new ThreadLocal<>();

    private final Thread thread;
    private final Consumer<Throwable> onCancel;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final Map<Thread, Integer> delegatedWorkers = new ConcurrentHashMap<>();
    private volatile boolean waitingOnExternalBackpressure;
    private volatile boolean completed;
    private volatile Throwable failure;  // The exception thrown by the body, kept rather than swallowed, so the fail-fast can pass it through

    public Task(String name, Runnable body, Consumer<Throwable> onFinish) {
        this(name, body, onFinish, cause -> { });
    }

    public Task(String name, Runnable body, Consumer<Throwable> onFinish,
                Consumer<Throwable> onCancel) {
        if (onCancel == null) {
            throw new IllegalArgumentException("task cancellation hook must not be null");
        }
        this.onCancel = onCancel;
        this.thread = new Thread(() -> runBody(body, onFinish), "rowweave-task-" + name);
    }

    private void runBody(Runnable body, Consumer<Throwable> onFinish) {
        Throwable captured = null;
        current.set(this);
        try {
            body.run();
        } catch (Throwable t) {
            captured = t;
            failure = t; // capture so the joining thread can fail-fast instead of silently dropping it
        } finally {
            // always run: success closes out normally, posting EOS; failure posts an error event downstream, and the downstream terminates on seeing it.
            try {
                onFinish.accept(captured);
            } catch (Throwable finishFailure) {
                if (captured == null) {
                    failure = finishFailure;
                } else if (captured != finishFailure
                        && !containsThrowable(captured, finishFailure)
                        && !containsThrowable(finishFailure, captured)) {
                    captured.addSuppressed(finishFailure);
                }
            } finally {
                completed = true;
                current.remove();
            }
        }
    }

    /** The pipeline task whose body is constructing a delegated sort buffer, if any. */
    public static Task currentPipelineTask() {
        return current.get();
    }

    /** A helper executing this task's buffer work participates in deadlock observation. */
    public void beginDelegatedWork() {
        if (Thread.currentThread() != thread) {
            delegatedWorkers.merge(Thread.currentThread(), 1, Integer::sum);
        }
    }

    public void endDelegatedWork() {
        if (Thread.currentThread() != thread) {
            delegatedWorkers.computeIfPresent(Thread.currentThread(),
                    (worker, depth) -> depth == 1 ? null : depth - 1);
        }
    }

    /** Lazy buffer factories retain their constructing Task even when invoked by a helper. */
    public <T> T withPipelineContext(java.util.function.Supplier<T> body) {
        Task previous = current.get();
        beginDelegatedWork();
        current.set(this);
        try {
            return body.get();
        } finally {
            if (previous == null) current.remove();
            else current.set(previous);
            endDelegatedWork();
        }
    }

    /** The caller is waiting for a downstream stage outside this pipeline's watchdog scope. */
    public void beginExternalBackpressure() {
        waitingOnExternalBackpressure = true;
    }

    public void endExternalBackpressure() {
        waitingOnExternalBackpressure = false;
    }

    /** The exception thrown while the body ran, or null when there was none; read after the join to pass the fail-fast through. */
    public Throwable failure() {
        return failure;
    }

    public void start() {
        thread.start();
    }

    public boolean isCompleted() {
        return completed;
    }

    public void awaitCompletion() throws InterruptedException {
        thread.join();
    }

    /** Join with a timeout, used by the fail-fast polling; zero or below waits indefinitely. */
    public void awaitCompletion(long millis) throws InterruptedException {
        thread.join(millis);
    }

    /**
     * One input to the deadlock watchdog: whether the thread is <b>not making progress</b>, meaning parked or
     * blocked. WAITING, TIMED_WAITING and BLOCKED all count as no progress, which covers a channel poll or
     * send, a lock, a latch or a sleep. RUNNABLE is progress, including a busy CPU in a large sort and spill
     * disk I/O in native code, so a thread doing real work is <b>not</b> read as parked, which is what keeps
     * the watchdog from killing a healthy job. A completed thread takes no part in the judgement and is not
     * treated as parked.
     */
    public boolean isParked() {
        if (completed) {
            return false;
        }
        // A pipeline-local watchdog cannot prove a DAG deadlock while its terminal is waiting
        // for an independently scheduled (possibly remote) stage to consume bounded output.
        if (waitingOnExternalBackpressure) {
            return false;
        }
        Thread.State s = thread.getState();
        if (s != Thread.State.WAITING && s != Thread.State.TIMED_WAITING
                && s != Thread.State.BLOCKED) {
            return false;
        }
        for (Thread worker : delegatedWorkers.keySet()) {
            if (worker.getState() == Thread.State.RUNNABLE) {
                return false;
            }
        }
        return true;
    }

    /**
     * Breaking a deadlock on the watchdog's order: interrupt this task's thread, which releases a body parked
     * at a blocking point that is <b>not a channel</b>, such as a latch, sleep or lock, since channel blocks are
     * released by poisoning instead. After the interrupt the body throws InterruptedException and exits through
     * the fail-fast path.
     */
    public void interrupt() {
        thread.interrupt();
    }

    /**
     * Actively release a task-owned blocking resource before interrupting its thread. The hook is
     * invoked at most once because failure detection, owner interruption and watchdog teardown can
     * race. Source stages use this to close an active SourceExecutor: some connectors intentionally
     * ignore thread interruption and unblock only when closed.
     */
    public void cancel(Throwable cause) {
        if (cancellationRequested.compareAndSet(false, true)) {
            onCancel.accept(cause);
        }
    }

    private static boolean containsThrowable(Throwable root, Throwable target) {
        if (root == null || target == null) return false;
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());
        java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
        pending.push(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.pop();
            if (current == target) return true;
            if (!seen.add(current)) continue;
            if (current.getCause() != null) pending.push(current.getCause());
            for (Throwable suppressed : current.getSuppressed()) pending.push(suppressed);
        }
        return false;
    }
}
