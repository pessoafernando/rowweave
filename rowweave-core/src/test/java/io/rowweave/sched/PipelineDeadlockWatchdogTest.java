package io.rowweave.sched;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.PointerSortBuffer;
import io.rowweave.mem.PipelinedExternalSortBuffer;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.TwoTierSortBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.SortField;
import io.rowweave.ops.RowComparator;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The deadlock watchdog — when a pipeline is genuinely deadlocked (every task thread parked, zero channel
 * throughput and no task finishing, for longer than the threshold) it **fails fast with an error** instead
 * of hanging forever, which is the third class of production incident after wrong results and running out
 * of memory: a silent stall.
 *
 * <p>The watchdog is the **fallback** for the fan-out materialization decision — if that decision misses a
 * case, the run ends with a clear error rather than a silent hang — and it is generic against any pipeline
 * deadlock bug. The detector must not kill healthy CPU-bound work (a big sort thread in RUNNABLE and spill
 * disk I/O in native RUNNABLE are both not parked).
 */
class PipelineDeadlockWatchdogTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

/**
 * The signature of a real deadlock: the source stage parks forever on a latch that is never
 * released, so it neither emits nor closes, and every downstream stage parks on an empty poll.
 * With all threads parked, no throughput and no completion, the watchdog must fail fast within
 * its timeout. Without the watchdog the run would hang forever, which the preemptive timeout
 * assertion exposes.
 */
    @Test
    void deadlockedPipelineFailsFastInsteadOfHanging() {
        AtomicBoolean released = new AtomicBoolean(false);
        CountDownLatch never = new CountDownLatch(1);  // never counted down
        assertTimeoutPreemptively(ofSeconds(15), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor()
                    .withDeadlockTimeoutMillis(1_000);  // a short timeout so the test runs quickly
/** The source parks forever, standing in for a shared producer stuck on backpressure, and emits nothing and never returns, so everything downstream parks. */
            exec.addStage("stuck-source", (in, sink) -> {
                try {
                    never.await();  // blocks forever
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            exec.addStage("passthrough", PipelineExecutor.passThroughStream((in, sink) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        sink.emitRow(row((Integer) e.row().get(0)));
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(out));

            RuntimeException ex = assertThrows(RuntimeException.class, exec::run,
                    "a deadlocked pipeline fails fast with an exception instead of hanging forever");
            String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
            assertTrue(msg.contains("deadlock") || msg.contains("no progress") || msg.contains("deadlocked"),
                    "the exception names the deadlock or the lack of progress, observed: " + ex.getMessage());
            released.set(true);
        });
        assertTrue(released.get());
    }

/**
 * The counter-case where nothing is killed in error: a healthy pipeline keeps flowing, so
 * throughput keeps growing and the watchdog must never fire, and the run completes normally.
 * A short timeout plus a source that is slow but steady, emitting one row after each sleep,
 * keeps throughput rising throughout.
 */
    @Test
    void slowButProgressingPipelineIsNotKilled() {
        assertTimeoutPreemptively(ofSeconds(20), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor()
                    .withDeadlockTimeoutMillis(500);  // longer than one row interval but far shorter than the whole run
            exec.addStage("slow-source", (in, sink) -> {
                for (int i = 0; i < 20; i++) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);  // slow, yet every row makes progress, so throughput grows
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    sink.emitRow(row(i));
                }
            });
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();  // must complete normally and not be killed by the watchdog
            assertEquals(20, out.getRows().size(), "a slow pipeline that keeps making progress is not killed by the watchdog");
        });
    }

    @Test
    void delegatedExternalSortSpillKeepsPipelineAliveWhileStageWaits() {
        delegatedExternalSortSpill(false, false);
    }

    @Test
    void lazilyCreatedExternalSortSpillKeepsPipelineAliveWhileStageWaits() {
        delegatedExternalSortSpill(true, false);
    }

    @Test
    void pipelinedSortWorkersKeepPipelineAliveWhileStageWaits() {
        delegatedExternalSortSpill(false, true);
    }

    @Test
    void lazilyCreatedPipelinedSortWorkersKeepPipelineAliveWhileStageWaits() {
        delegatedExternalSortSpill(true, true);
    }

    private void delegatedExternalSortSpill(boolean lazy, boolean pipelined) {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor().withDeadlockTimeoutMillis(150);
            exec.addStage("source", (in, sink) -> sink.emitRow(row(1)));
            exec.addStage("delegated-sort", (in, sink) -> {
                assertNotNull(in.next());
                SortField key = new SortField(0, SortField.Direction.ASC,
                        SortField.NullOrder.DEFAULT);
                java.util.function.Supplier<SortBuffer> factory = () -> pipelined
                        ? new PipelinedExternalSortBuffer(META, new RowComparator(META, key),
                                new SortField[]{key}, new MemoryManager(64 * 1024), 1, 3,
                                new BusySpillStore(), HeapPool::new)
                        : new PointerSortBuffer(META,
                                new RowComparator(META, key), new SortField[]{key},
                                new HeapPool(), new MemoryManager(64 * 1024), 1,
                                new BusySpillStore());
                // SMJ constructs the wrapper on the Task, then creates its secondary on a helper.
                SortBuffer buffer = lazy
                        ? new TwoTierSortBuffer(META, new RowComparator(META, key),
                                new MemoryManager(1), factory)
                        : factory.get();
                AtomicReference<Throwable> helperFailure = new AtomicReference<>();
                Thread helper = new Thread(() -> {
                    try {
                        buffer.putRow(row(1));
                    } catch (Throwable failure) {
                        helperFailure.set(failure);
                    }
                }, "test-binary-sort-helper");
                helper.start();
                try {
                    helper.join();
                    if (helperFailure.get() != null) {
                        throw new AssertionError(helperFailure.get());
                    }
                    buffer.sort();
                    sink.emitRow(buffer.iterator().next());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interrupted);
                } finally {
                    buffer.close();
                }
            });
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();
            assertEquals(1, out.getRows().size());
        });
    }

    @Test
    void downstreamStageBackpressureIsNotAnInternalPipelineDeadlock() {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            MultiProducerChannel downstream = new MultiProducerChannel(1, 1);
            AtomicReference<Throwable> receiverFailure = new AtomicReference<>();
            java.util.concurrent.atomic.AtomicInteger received = new java.util.concurrent.atomic.AtomicInteger();
            Thread receiver = new Thread(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(750);
                    while (downstream.poll() != null) {
                        received.incrementAndGet();
                    }
                } catch (Throwable failure) {
                    receiverFailure.set(failure);
                }
            }, "slow-downstream-stage");
            receiver.start();
            PipelineExecutor exec = new PipelineExecutor().withDeadlockTimeoutMillis(150);
            exec.addStage("source", (in, sink) -> {
                for (int i = 0; i < 3; i++) sink.emitRow(row(i));
            });
            exec.addStage("external-terminal", (in, ignored) -> {
                try {
                    StreamEvent event;
                    while ((event = in.next()) != null) downstream.send(event, 0);
                } finally {
                    downstream.producerDone();
                }
            });
            exec.run();
            receiver.join(2_000);
            assertFalse(receiver.isAlive());
            assertNull(receiverFailure.get());
            assertEquals(3, received.get());
        });
    }

    @Test
    void upstreamStageWaitIsNotAnInternalPipelineDeadlock() {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            MultiProducerChannel upstream = new MultiProducerChannel(1, 1);
            Thread producer = new Thread(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(750);
                    upstream.send(StreamEvent.record(row(7)), 0);
                    upstream.producerDone();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }, "slow-upstream-stage");
            producer.start();
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor().withDeadlockTimeoutMillis(150);
            exec.addStage("external-source", (ignored, sink) -> {
                try {
                    StreamEvent event;
                    while ((event = upstream.poll()) != null) sink.emit(event);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interrupted);
                }
            });
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();
            producer.join(2_000);
            assertFalse(producer.isAlive());
            assertEquals(1, out.getRows().size());
        });
    }

    private static final class BusySpillStore implements SpillStore {
        private final MemorySpillStore delegate = new MemorySpillStore();

        @Override
        public SpillRun createRun() {
            SpillRun run = delegate.createRun();
            return new SpillRun() {
                @Override public void append(byte[] frame) {
                    long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(750);
                    while (System.nanoTime() < until) {
                        Thread.yield();
                    }
                    run.append(frame);
                }
                @Override public void finishWriting() { run.finishWriting(); }
                @Override public java.util.Iterator<byte[]> read() { return run.read(); }
                @Override public void abort() { run.abort(); }
                @Override public void dispose() { run.dispose(); }
            };
        }

        @Override public void close() { delegate.close(); }
    }
}
