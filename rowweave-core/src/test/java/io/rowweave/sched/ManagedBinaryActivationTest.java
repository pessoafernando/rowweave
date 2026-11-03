package io.rowweave.sched;

import io.rowweave.DataRow;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.mem.MemoryOwnerRequirement;
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

/** Dependency activation, replay on both sides, and the first-cause lease lifecycle of a managed binary operator. */
class ManagedBinaryActivationTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    @Test
    void waitsForLeftFirstEventBeforeAcquiringAndReplaysBothSides() throws Exception {
        configure();
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = binaryMemory(pool, 1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean leftDelivered = new AtomicBoolean();
        StreamEvent leftEvent = StreamEvent.record(row(1));
        StreamEvent rightEvent = StreamEvent.record(row(2));
        RowStream gatedLeft = () -> {
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            return leftDelivered.compareAndSet(false, true) ? leftEvent : null;
        };
        List<StreamEvent> emitted = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        PipelineExecutor.BinaryStage stage = PipelineExecutor.managedBinaryBreaker(memory,
                mm -> (left, right, out) -> {
                    out.emit(left.next());
                    out.emit(right.next());
                });

        Thread worker = new Thread(() -> {
            try {
                stage.run(gatedLeft, single(rightEvent), emitted::add);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "managed-binary-activation");
        worker.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertEquals(3072, pool.availableBytes(), "before the first left event only the guarantee is registered and nothing is reserved");
        assertEquals(0, pool.snapshot().reservedBytes());

        release.countDown();
        worker.join(5_000);
        assertFalse(worker.isAlive());
        assertNull(failure.get());
        assertEquals(2, emitted.size());
        assertSame(leftEvent, emitted.get(0));
        assertSame(rightEvent, emitted.get(1));
        assertEquals(4096, pool.availableBytes(), "once the body finishes the owner guarantee is returned");
        memory.close();
    }

    @Test
    void bothEmptySkipsLeaseButRightOnlyActivatesAndReplays() {
        configure();
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = binaryMemory(pool, 2);
        AtomicInteger factories = new AtomicInteger();
        PipelineExecutor.BinaryStage stage = PipelineExecutor.managedBinaryBreaker(memory, mm -> {
            factories.incrementAndGet();
            return (left, right, out) -> {
                assertNull(left.next());
                StreamEvent event = right.next();
                if (event != null) out.emit(event);
            };
        });

        stage.run(() -> null, () -> null, event -> fail("two empty inputs emit nothing"));
        assertEquals(0, factories.get());
        assertEquals(2048, pool.availableBytes(), "both unclaimed guarantees stay protected until the execution unit closes");

        StreamEvent right = StreamEvent.record(row(9));
        List<StreamEvent> out = new ArrayList<>();
        stage.run(() -> null, single(right), out::add);
        assertEquals(1, factories.get());
        assertEquals(1, out.size());
        assertSame(right, out.get(0));
        assertEquals(3072, pool.availableBytes(), "the executed owner returns its share and the other unclaimed guarantee remains");
        memory.close();
    }

    @Test
    void replayPreservesCancellationCapabilityOnBothSides() {
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = binaryMemory(pool, 1);
        AtomicBoolean leftCancelled = new AtomicBoolean();
        AtomicBoolean rightCancelled = new AtomicBoolean();
        RowStream left = cancellable(StreamEvent.barrier(1), leftCancelled);
        RowStream right = cancellable(StreamEvent.barrier(2), rightCancelled);

        PipelineExecutor.managedBinaryBreaker(memory, mm -> (l, r, out) -> {
            assertTrue(l.supportsCancellation());
            assertTrue(r.supportsCancellation());
            assertEquals(1, l.next().batchId());
            assertEquals(2, r.next().batchId());
            assertTrue(l.cancel(new RuntimeException("left stop")));
            assertTrue(r.cancel(new RuntimeException("right stop")));
        }).run(left, right, event -> fail("the test body emits nothing"));

        assertTrue(leftCancelled.get());
        assertTrue(rightCancelled.get());
        assertEquals(4096, pool.availableBytes());
        memory.close();
    }

    @Test
    void factoryAndBodyFailuresReleaseLeaseAndPreserveRoot() {
        configure();
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        ExecutionMemory memory = binaryMemory(pool, 2);
        RuntimeException factoryBoom = new RuntimeException("binary factory boom");
        RuntimeException seenFactory = assertThrows(RuntimeException.class, () ->
                PipelineExecutor.managedBinaryBreaker(memory, mm -> {
                    throw factoryBoom;
                }).run(single(StreamEvent.record(row(1))), () -> null, event -> { }));
        assertSame(factoryBoom, seenFactory);
        assertEquals(3072, pool.availableBytes(), "the second pre-registered owner still holds its minimum guarantee");

        RuntimeException bodyBoom = new RuntimeException("binary body boom");
        RuntimeException seenBody = assertThrows(RuntimeException.class, () ->
                PipelineExecutor.managedBinaryBreaker(memory,
                        mm -> (left, right, out) -> { throw bodyBoom; })
                        .run(single(StreamEvent.record(row(1))), () -> null, event -> { }));
        assertSame(bodyBoom, seenBody);
        assertEquals(4096, pool.availableBytes());
        memory.close();
    }

    private static void configure() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4096");
        System.setProperty("rowweave.worker.operator.minMemory", "1024");
    }

    private static ExecutionMemory binaryMemory(ManagedMemory pool, int ownerCount) {
        MemoryOwnerRequirement[] requirements = new MemoryOwnerRequirement[ownerCount];
        for (int i = 0; i < ownerCount; i++) {
            requirements[i] = MemoryOwnerRequirement.partitioned(2);
        }
        return ExecutionMemory.forJob(pool, true, requirements);
    }

    private static DataRow row(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static RowStream single(StreamEvent event) {
        AtomicBoolean first = new AtomicBoolean(true);
        return () -> first.compareAndSet(true, false) ? event : null;
    }

    private static RowStream cancellable(StreamEvent event, AtomicBoolean cancelled) {
        AtomicBoolean first = new AtomicBoolean(true);
        return new RowStream() {
            @Override public StreamEvent next() {
                return first.compareAndSet(true, false) ? event : null;
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) {
                cancelled.set(true);
                return true;
            }
        };
    }
}
