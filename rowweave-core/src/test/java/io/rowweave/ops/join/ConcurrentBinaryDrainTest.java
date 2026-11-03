package io.rowweave.ops.join;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConcurrentBinaryDrainTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema OUT = new DataSchema(new Field("n", DataType.IntegerType));

    @AfterEach
    void clearProperties() {
        System.clearProperty("rowweave.binary.concurrentDrain");
        System.clearProperty("rowweave.binary.drain.maxThreads");
    }

    @Test
    @Timeout(3)
    void sortMergeJoinOverlapsBothCancellableInputs() {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        CountDownLatch entered = new CountDownLatch(2);
        MemoryManager memory = new MemoryManager(128 * 1024);
        HeapPool pool = new HeapPool();
        SortMergeJoin smj = SortMergeJoin.of(
                        LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                .withSpill(memory, new MemorySpillStore(), pool);
        CollectingRowSink sink = new CollectingRowSink();

        smj.join(new CoordinatedStream(row(1, "L"), entered),
                new CoordinatedStream(row(1, "R"), entered), sink);

        assertEquals(1, sink.rows().size());
        assertTrue(sink.sawEndOfStream());
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, BinaryDrainLimiter.currentThreads());
    }

    @Test
    @Timeout(3)
    void coGroupOverlapsBothCancellableInputs() {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        CountDownLatch entered = new CountDownLatch(2);
        MemoryManager memory = new MemoryManager(128 * 1024);
        HeapPool pool = new HeapPool();
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return OUT; }
            @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector out) {
                DataRow result = new DataRow(1);
                result.set(0, 1);
                out.collect(result);
            }
        };
        CoGroup cg = CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, fn)
                .withSpill(memory, new MemorySpillStore(), pool);
        CollectingRowSink sink = new CollectingRowSink();

        cg.coGroup(new CoordinatedStream(row(1, "L"), entered),
                new CoordinatedStream(row(1, "R"), entered), sink);

        assertEquals(1, sink.rows().size());
        assertTrue(sink.sawEndOfStream());
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, BinaryDrainLimiter.currentThreads());
    }

    @Test
    @Timeout(3)
    void firstErrorCancelsBlockedSiblingAndEmitsExactlyOnce() {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        RuntimeException boom = new RuntimeException("boom");
        AtomicBoolean siblingCancelled = new AtomicBoolean();
        CountDownLatch releaseSibling = new CountDownLatch(1);
        RowStream error = terminalStream(StreamEvent.error(boom));
        RowStream blocked = blockingStream(siblingCancelled, releaseSibling);
        MemoryManager memory = new MemoryManager(4096);
        HeapPool pool = new HeapPool();
        SortMergeJoin join = SortMergeJoin.of(
                        LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                .withSpill(memory, new MemorySpillStore(), pool);
        AtomicInteger emitted = new AtomicInteger();
        final StreamEvent[] seen = new StreamEvent[1];

        join.join(error, blocked, event -> {
            emitted.incrementAndGet();
            seen[0] = event;
        });

        assertEquals(1, emitted.get());
        assertTrue(seen[0].isError());
        assertSame(boom, seen[0].cause());
        assertTrue(siblingCancelled.get());
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, BinaryDrainLimiter.currentThreads());
    }

    @Test
    void sinkFailureRemainsOuterAndRetainsTerminalCause() {
        RuntimeException terminal = new RuntimeException("terminal");
        RuntimeException sinkFailure = new RuntimeException("sink");
        SortMergeJoin join = SortMergeJoin.of(
                LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> join.join(terminalStream(StreamEvent.error(terminal)),
                        RowStream.fromIterable(java.util.Collections.emptyList()),
                        event -> { throw sinkFailure; }));

        assertSame(sinkFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(terminal, thrown.getSuppressed()[0]);
    }

    @Test
    @Timeout(3)
    void secondSubmissionFailureCancelsStartedTaskAndReleasesPermits() throws Exception {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        AtomicBoolean leftCancelled = new AtomicBoolean();
        AtomicBoolean rightCancelled = new AtomicBoolean();
        CountDownLatch leftStarted = new CountDownLatch(1);
        CountDownLatch leftRelease = new CountDownLatch(1);
        RowStream left = blockingStream(leftCancelled, leftStarted, leftRelease);
        RowStream right = blockingStream(rightCancelled, new CountDownLatch(1));
        AtomicInteger submissions = new AtomicInteger();
        Executor rejectSecond = task -> {
            if (submissions.incrementAndGet() == 2) {
                try {
                    if (!leftStarted.await(1, TimeUnit.SECONDS)) {
                        throw new AssertionError("first helper did not start");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("submission thread interrupted", interrupted);
                }
                throw new RejectedExecutionException("second submission rejected");
            }
            Thread thread = new Thread(task, "binary-drain-partial-submit-test");
            thread.start();
        };

        RejectedExecutionException thrown = assertThrows(RejectedExecutionException.class,
                () -> ConcurrentBinaryDrain.drain(left, discardBuffer(), right, discardBuffer(), rejectSecond));

        assertEquals("second submission rejected", thrown.getMessage());
        assertTrue(leftStarted.await(1, TimeUnit.SECONDS));
        assertTrue(leftCancelled.get());
        assertTrue(rightCancelled.get());
        assertEquals(0, BinaryDrainLimiter.currentThreads());
    }

    @Test
    @Timeout(3)
    void secondSubmissionFailureCancelsFirstTaskBeforeItRuns() {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        AtomicReference<Runnable> stashed = new AtomicReference<>();
        AtomicInteger submissions = new AtomicInteger();
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        Executor rejectSecond = task -> {
            if (submissions.incrementAndGet() == 1) {
                stashed.set(task);
                return;
            }
            throw new RejectedExecutionException("second submission rejected before first run");
        };
        RowStream neverStarted = countingStream(nextCalls, cancellations);

        RejectedExecutionException thrown = assertThrows(RejectedExecutionException.class,
                () -> ConcurrentBinaryDrain.drain(
                        neverStarted, discardBuffer(), neverStarted, discardBuffer(), rejectSecond));

        assertEquals("second submission rejected before first run", thrown.getMessage());
        assertEquals(2, cancellations.get());
        assertEquals(0, nextCalls.get());
        assertEquals(0, BinaryDrainLimiter.currentThreads());
        stashed.get().run();
        assertEquals(0, nextCalls.get(), "cancel-before-run task must never enter its body");
        assertEquals(0, BinaryDrainLimiter.currentThreads());
    }

    @Test
    @Timeout(3)
    void cancellingRunningTaskCannotLateInterruptReusedWorker() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseBody = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        CountDownLatch interruptEntered = new CountDownLatch(1);
        CountDownLatch allowInterrupt = new CountDownLatch(1);
        CountDownLatch sentinelStarted = new CountDownLatch(1);
        AtomicBoolean sentinelInterrupted = new AtomicBoolean();
        AtomicBoolean interceptOnce = new AtomicBoolean(true);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), runnable ->
                new Thread(runnable, "binary-drain-reuse-test") {
                    @Override public void interrupt() {
                        if (interceptOnce.compareAndSet(true, false)) {
                            interruptEntered.countDown();
                            try {
                                allowInterrupt.await();
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        super.interrupt();
                    }
                });
        ConcurrentBinaryDrain.DrainTask task = new ConcurrentBinaryDrain.DrainTask(() -> {
            taskStarted.countDown();
            try {
                releaseBody.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, completed);
        try {
            executor.execute(task);
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            Thread canceller = new Thread(task::cancelExecution, "binary-drain-canceller-test");
            canceller.start();
            assertTrue(interruptEntered.await(1, TimeUnit.SECONDS));

            releaseBody.countDown();
            executor.execute(() -> {
                sentinelInterrupted.set(Thread.currentThread().isInterrupted());
                sentinelStarted.countDown();
            });
            assertTrue(!sentinelStarted.await(100, TimeUnit.MILLISECONDS),
                    "worker must remain owned by old task while cancellation holds lifecycle lock");
            allowInterrupt.countDown();

            canceller.join(1000);
            assertTrue(completed.await(1, TimeUnit.SECONDS));
            assertTrue(sentinelStarted.await(1, TimeUnit.SECONDS));
            assertTrue(!sentinelInterrupted.get(), "reused worker must not inherit a late interrupt");
        } finally {
            releaseBody.countDown();
            allowInterrupt.countDown();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @Timeout(3)
    void callerInterruptCancelsBothTasksWaitsForExitAndRestoresInterruptFlag() throws Exception {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        AtomicBoolean leftCancelled = new AtomicBoolean();
        AtomicBoolean rightCancelled = new AtomicBoolean();
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                ConcurrentBinaryDrain.drain(
                        blockingStream(leftCancelled, bothStarted, release), discardBuffer(),
                        blockingStream(rightCancelled, bothStarted, release), discardBuffer());
            } catch (Throwable thrown) {
                failure.set(thrown);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            }
        }, "binary-drain-interrupted-caller-test");
        caller.start();
        assertTrue(bothStarted.await(1, TimeUnit.SECONDS));

        caller.interrupt();
        caller.join(1000);

        assertTrue(!caller.isAlive());
        assertTrue(failure.get() instanceof java.util.concurrent.CancellationException);
        assertTrue(interruptRestored.get());
        assertTrue(leftCancelled.get());
        assertTrue(rightCancelled.get());
        assertEquals(0, BinaryDrainLimiter.currentThreads());
    }

    private static DataRow row(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static final class CoordinatedStream implements RowStream {
        private final DataRow row;
        private final CountDownLatch entered;
        private boolean emitted;

        private CoordinatedStream(DataRow row, CountDownLatch entered) {
            this.row = row;
            this.entered = entered;
        }

        @Override
        public StreamEvent next() {
            if (emitted) {
                return null;
            }
            emitted = true;
            entered.countDown();
            try {
                if (!entered.await(1, TimeUnit.SECONDS)) {
                    throw new AssertionError("binary inputs were drained serially");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return StreamEvent.cancel();
            }
            return StreamEvent.record(row);
        }

        @Override
        public boolean supportsCancellation() {
            return true;
        }

        @Override
        public boolean cancel(Throwable cause) {
            while (entered.getCount() > 0) {
                entered.countDown();
            }
            return true;
        }
    }

    private static RowStream terminalStream(StreamEvent terminal) {
        return new RowStream() {
            private boolean emitted;
            @Override public StreamEvent next() {
                if (emitted) return null;
                emitted = true;
                return terminal;
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) { return true; }
        };
    }

    private static RowStream blockingStream(AtomicBoolean cancelled, CountDownLatch release) {
        return blockingStream(cancelled, null, release);
    }

    private static RowStream blockingStream(AtomicBoolean cancelled, CountDownLatch started,
                                            CountDownLatch release) {
        return new RowStream() {
            @Override public StreamEvent next() {
                if (started != null) {
                    started.countDown();
                }
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return StreamEvent.cancel();
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) {
                cancelled.set(true);
                release.countDown();
                return true;
            }
        };
    }

    private static RowStream countingStream(AtomicInteger nextCalls, AtomicInteger cancellations) {
        return new RowStream() {
            @Override public StreamEvent next() {
                nextCalls.incrementAndGet();
                return null;
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) {
                cancellations.incrementAndGet();
                return true;
            }
        };
    }

    private static SortBuffer discardBuffer() {
        return new SortBuffer() {
            @Override public void putRow(DataRow row) { }
            @Override public void sort() { }
            @Override public long size() { return 0; }
            @Override public java.util.Iterator<DataRow> iterator() {
                return java.util.Collections.emptyIterator();
            }
        };
    }
}
