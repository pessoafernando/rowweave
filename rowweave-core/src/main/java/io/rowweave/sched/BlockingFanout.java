package io.rowweave.sched;

import io.rowweave.ops.Partitioner;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * BLOCKING fan-out, the same idea as a Flink BLOCKING ResultPartition. It breaks the deadlock cycle of a
 * fan-out that re-joins into an order-consuming operator: SortMergeJoin, CoGroup, HashJoin or Union.
 *
 * <p><b>Mechanism</b>: the fork output is first <b>materialized once</b> into a
 * {@link MaterializedRowBuffer}, where the fill writes rows and exits under no downstream backpressure at
 * all, which cuts the cycle of a live upstream pinned by one side draining downstream. Once the fill is
 * done, <b>each subpartition group is replayed concurrently on its own thread</b>, each over its own
 * {@link MaterializedRowBuffer#replay()} cursor and pushing through its own partitioner, and each group
 * closes independently, that is posts its own EOS, when it finishes.
 *
 * <p><b>Why there is no deadlock, independent of the order the consumers drain</b>: a group that parks
 * because its consumer has not drained, for example with socket credit at zero, parks <b>only its own
 * replay thread</b> and blocks no other group. Every consumer eventually drains to its own subpartition, so
 * that group leaves the park and finishes. A replay thread waits only for the credit of its own consumer, a
 * consumer waits only for data, and the data source is a finite materialized buffer, so <b>there is no
 * circular wait and therefore no deadlock</b>. This is better than replaying twice in a fixed order, where
 * breaking the cycle would depend on the left consumer happening to drain first and would couple to the
 * internal drain order.
 *
 * <p><b>One subpartition group is one inbound edge of one downstream stage</b>: the sinks are the channels
 * of every instance of that downstream, and the partitioner decides which instance a row lands on, with null
 * meaning a single downstream instance receiving all of them. A self-join fork makes two groups, edge0 and
 * edge1, into the same join.
 */
public final class BlockingFanout {

    /** One subpartition group: the channels of every downstream instance plus this group's partitioner, where null means a single instance. */
    public static final class Group {
        private final List<Channel> sinks;
        private final Partitioner partitioner;
        private final Runnable closeAction;
        private final Runnable preTerminalCloseAction;
        private final Consumer<Throwable> failAction;

        public Group(List<Channel> sinks, Partitioner partitioner) {
            this(sinks, partitioner, null, null);
        }

        /** The owner-aware terminal action a StageTask can inject; null keeps the standalone scheduling tests and the earlier call semantics. */
        public Group(List<Channel> sinks, Partitioner partitioner,
                     Runnable closeAction, Consumer<Throwable> failAction) {
            this(sinks, partitioner, closeAction, closeAction, failAction);
        }

        /** The independent permit action for a group that is not the last; the last group takes the ordinary close action after the global terminal is claimed. */
        public Group(List<Channel> sinks, Partitioner partitioner,
                     Runnable closeAction, Runnable preTerminalCloseAction,
                     Consumer<Throwable> failAction) {
            this.sinks = sinks;
            this.partitioner = partitioner;
            this.closeAction = closeAction;
            this.preTerminalCloseAction = preTerminalCloseAction;
            this.failAction = failAction;
        }

        private void send(StreamEvent e) {
            int part = partitioner == null ? 0 : partitioner.partitionOf(e.row());
            sinks.get(part).send(e);  // Within a group the target partition is exclusive, so there is no cross-talk and no copy
        }

        private void closeAll(boolean globalTerminalClaimed) {
            Runnable action = globalTerminalClaimed ? closeAction : preTerminalCloseAction;
            if (action != null) {
                action.run();
                return;
            }
            Throwable failure = null;
            for (Channel s : sinks) {
                try {
                    s.close();
                } catch (Throwable closeFailure) {
                    if (failure == null) failure = closeFailure;
                    else if (failure != closeFailure) failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
            if (failure != null) {
                throw new IllegalStateException("unexpected checked channel close failure", failure);
            }
        }

        private void failAll(Throwable t) {
            if (failAction != null) {
                failAction.accept(t);
                return;
            }
            for (Channel s : sinks) {
                try {
                    s.fail(t);
                } catch (Throwable cleanupFailure) {
                    if (t != cleanupFailure) t.addSuppressed(cleanupFailure);
                }
            }
        }
    }

    private BlockingFanout() {
    }

    /**
     * Materialize, then replay concurrently. {@code upstream} is poured into {@code buffer} once, without
     * downstream backpressure, and each group is then replayed on its own thread. The number of materialized
     * records is returned, which is the output count. A failure in any group poisons that group and throws,
     * the first error winning.
     *
     * @param upstream the fork output stream, drained to the end before the fill exits
     * @param buffer the materialization backend, with spill as backstop, so being disk-only it never runs out of memory
     * @param groups the subpartition groups, each one an inbound edge of the downstream
     * @param label the thread name and error-message label
     */
    public static long materializeAndReplay(RowStream upstream, MaterializedRowBuffer buffer,
                                            List<Group> groups, String label) {
        return materializeAndReplay(upstream, buffer, groups, label, () -> true);
    }

    /** The terminal gate for execution: the last group to finish replaying its data linearizes the success of the whole stage ahead of its own EOS. */
    public static long materializeAndReplay(RowStream upstream, MaterializedRowBuffer buffer,
                                            List<Group> groups, String label,
                                            BooleanSupplier terminalGate) {
        // Materialize first: drain the whole upstream into the buffer, pushing nothing downstream so backpressure cannot reach it, and pass an error event through by throwing.
        final long[] cnt = {0};
        buffer.fill(() -> {
            StreamEvent e = upstream.next();
            if (e != null && e.isRecord()) {
                cnt[0]++;
            }
            if (e != null && e.isError()) {
                throw new RuntimeException(label + " upstream error", e.cause());
            }
            return e;
        });
        return replayFilled(buffer, groups, label, cnt[0], terminalGate);
    }

    /**
     * The per-group independent concurrent replay for a BLOCKING partition whose writes have
     * finished. A breaker that produces rows directly calls this: it has no upstream {@link
     * RowStream} to hand to {@link #materializeAndReplay}, but it still must write everything out
     * before sending records and EOS.
     */
    public static long replayFilled(MaterializedRowBuffer buffer, List<Group> groups, String label, long count) {
        return replayFilled(buffer, groups, label, count, () -> true);
    }

    public static long replayFilled(MaterializedRowBuffer buffer, List<Group> groups, String label,
                                    long count, BooleanSupplier terminalGate) {
        if (terminalGate == null) {
            throw new IllegalArgumentException("terminal gate is required");
        }
        // One thread per group, replaying concurrently, decoupled from the drain order.
        List<Thread> threads = new ArrayList<>(groups.size());
        final AtomicReference<Throwable> firstErr = new AtomicReference<>();
        final AtomicInteger groupsStillReplaying = new AtomicInteger(groups.size());
        final CountDownLatch nonFinalGroupsClosed =
                new CountDownLatch(Math.max(0, groups.size() - 1));
        for (int gi = 0; gi < groups.size(); gi++) {
            final Group g = groups.get(gi);
            Thread t = new Thread(() -> {
                try {
                    RowStream rs = buffer.replay();  // An independent cursor
                    StreamEvent e;
                    while ((e = rs.next()) != null) {
                        if (e.isRecord()) {
                            g.send(e);
                        }
                    }
                    boolean finalGroup = groupsStillReplaying.decrementAndGet() == 0;
                    if (finalGroup) {
                        // The fast groups post their own EOS first, and while their close may still block, the stage stays cancellable.
                        // Only after the last group has seen the other closes converge does it freeze the success of the whole stage ahead of its own EOS.
                        nonFinalGroupsClosed.await();
                        Throwable priorFailure = firstErr.get();
                        if (priorFailure != null) throwUnchecked(priorFailure);
                        if (!terminalGate.getAsBoolean()) {
                            throw new TerminalRejectedException();
                        }
                        g.closeAll(true);
                    } else {
                        try {
                            g.closeAll(false);  // This group's own EOS, with the owner permit and cancel linearized under the same lock
                        } catch (Throwable closeFailure) {
                            recordFailure(firstErr, closeFailure, groups);
                            throw closeFailure;
                        } finally {
                            nonFinalGroupsClosed.countDown();
                        }
                    }
                } catch (Throwable ex) {
                    recordFailure(firstErr, ex, groups);
                }
            }, "rowweave-blocking-replay-" + label + "-" + gi);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("blocking replay interrupted", ie);
            }
        }
        Throwable err = firstErr.get();
        if (err != null) {
            throw new RuntimeException(label + " blocking replay failed", err);
        }
        return count;
    }

    private static void recordFailure(
            AtomicReference<Throwable> firstErr, Throwable failure, List<Group> groups) {
        boolean first = firstErr.compareAndSet(null, failure);
        // A gate of false means the caller's cancellation owner already handles poisoning, so teardown must not be repeated here.
        if (first && !(failure instanceof TerminalRejectedException)) {
            // Any genuine replay failure must wake every group still parked; the owner fence absorbs the repeated cleanup from the outer layer.
            for (int i = groups.size() - 1; i >= 0; i--) {
                try {
                    groups.get(i).failAll(failure);
                } catch (Throwable cleanupFailure) {
                    if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
                }
            }
        }
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw new IllegalStateException("unexpected checked blocking fanout failure", failure);
    }

    private static final class TerminalRejectedException extends RuntimeException {
        private TerminalRejectedException() {
            super("blocking fanout terminal rejected by cancellation");
        }
    }
}
