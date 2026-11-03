package io.rowweave.sched;

import io.rowweave.DataRow;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** First-event activation, replay, and the first-cause lease lifecycle of a managed breaker. */
class ManagedBreakerActivationTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    @Test
    void waitsForFirstEventBeforeAcquiringAndReplaysIt() throws Exception {
        System.setProperty("rowweave.worker.operator.maxMemory", "4096");
        System.setProperty("rowweave.worker.operator.minMemory", "1024");
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean delivered = new AtomicBoolean();
        DataRow row = new DataRow(1);
        row.set(0, 7);
        StreamEvent first = StreamEvent.record(row);
        RowStream gated = () -> {
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            return delivered.compareAndSet(false, true) ? first : null;
        };
        List<StreamEvent> emitted = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        PipelineExecutor.Stage stage = PipelineExecutor.managedStreamingBreaker(memory, false,
                mm -> (in, out) -> out.emit(in.next()));

        Thread worker = new Thread(() -> {
            try {
                stage.run(gated, emitted::add);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "managed-breaker-activation");
        worker.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertEquals(3072, pool.availableBytes(), "before the first event only the guarantee is registered and nothing is reserved");
        assertEquals(0, pool.snapshot().reservedBytes());

        release.countDown();
        worker.join(5_000);
        assertFalse(worker.isAlive());
        assertNull(failure.get());
        assertEquals(1, emitted.size());
        assertSame(first, emitted.get(0), "the first event replays as the same object in the same order");
        assertEquals(4096, pool.availableBytes(), "once the body finishes the lease is returned");
        memory.close();
    }

    @Test
    void emptyStreamCanSkipOrActivateExplicitly() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4096");
        System.setProperty("rowweave.worker.operator.minMemory", "1024");
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 2);
        AtomicInteger factories = new AtomicInteger();
        AtomicInteger bodies = new AtomicInteger();

        PipelineExecutor.managedStreamingBreaker(memory, false, mm -> {
            factories.incrementAndGet();
            return (in, out) -> bodies.incrementAndGet();
        }).run(() -> null, event -> fail("an empty stream emits nothing"));
        assertEquals(0, factories.get());
        assertEquals(0, bodies.get());
        assertEquals(2048, pool.availableBytes(), "both unclaimed guarantees stay protected until the execution unit closes");

        PipelineExecutor.managedStreamingBreaker(memory, true, mm -> {
            factories.incrementAndGet();
            return (in, out) -> {
                assertNull(in.next());
                bodies.incrementAndGet();
            };
        }).run(() -> null, event -> fail("the test body emits nothing"));
        assertEquals(1, factories.get());
        assertEquals(1, bodies.get());
        assertEquals(3072, pool.availableBytes(), "the executed owner returns its share and the other unclaimed guarantee remains");
        memory.close();
    }

    @Test
    void replayPreservesControlOrderAndCancellationCapability() {
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 1);
        StreamEvent barrier = StreamEvent.barrier(19);
        StreamEvent record = StreamEvent.record(new DataRow(0));
        AtomicInteger index = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        RowStream upstream = new RowStream() {
            @Override public StreamEvent next() {
                int i = index.getAndIncrement();
                return i == 0 ? barrier : i == 1 ? record : null;
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) {
                cancelled.set(true);
                return true;
            }
        };

        PipelineExecutor.managedStreamingBreaker(memory, false, mm -> (in, out) -> {
            assertTrue(in.supportsCancellation());
            assertSame(barrier, in.next());
            assertSame(record, in.next());
            assertTrue(in.cancel(new RuntimeException("stop")));
        }).run(upstream, event -> fail("the test body emits nothing"));

        assertTrue(cancelled.get());
        assertEquals(4096, pool.availableBytes());
        memory.close();
    }

    @Test
    void factoryAndBodyFailuresReleaseLeaseAndPreserveRoot() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4096");
        System.setProperty("rowweave.worker.operator.minMemory", "1024");
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 2);
        StreamEvent first = StreamEvent.record(new DataRow(0));
        RuntimeException factoryBoom = new RuntimeException("factory boom");
        RuntimeException seenFactory = assertThrows(RuntimeException.class, () ->
                PipelineExecutor.managedStreamingBreaker(memory, false, mm -> {
                    throw factoryBoom;
                }).run(single(first), event -> { }));
        assertSame(factoryBoom, seenFactory);
        assertEquals(3072, pool.availableBytes(), "the second pre-registered owner still holds its minimum guarantee");

        RuntimeException bodyBoom = new RuntimeException("body boom");
        RuntimeException seenBody = assertThrows(RuntimeException.class, () ->
                PipelineExecutor.managedStreamingBreaker(memory, false,
                        mm -> (in, out) -> { throw bodyBoom; })
                        .run(single(first), event -> { }));
        assertSame(bodyBoom, seenBody);
        assertEquals(4096, pool.availableBytes());
        memory.close();
    }

    private static RowStream single(StreamEvent event) {
        AtomicBoolean first = new AtomicBoolean(true);
        return () -> first.compareAndSet(true, false) ? event : null;
    }
}
