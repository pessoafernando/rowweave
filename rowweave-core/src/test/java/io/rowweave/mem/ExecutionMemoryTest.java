package io.rowweave.mem;

import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.SpillStore;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ExecutionMemory is the engine's always-bounded internal context with a per-operator
 * sub-budget: each breaker gets its own MemoryManager, carved as total divided by
 * breakerCount from the shared ManagedMemory pool. A buffer registers only with its own
 * operator manager, so cross-spilling never crosses threads. The OOM ceiling is the sum of
 * acquired, which the pool gate keeps at or below total.
 */
class ExecutionMemoryTest {

    @org.junit.jupiter.api.AfterEach
    void clearOpProps() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
        ExecutionMemory.setAcquireListenerForTest(null);
    }

    /** The owner cap is fixed, the guarantee is registered when the execution unit is built, and a claim never stands in for an actual reservation. */
    @Test
    void newOperatorMemoryAcquiresUpToMaxFromPool() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4000");  // a cap of 4000, to make the assertions easy
        System.setProperty("rowweave.worker.operator.minMemory", "1000");
        ManagedMemory pool = ManagedMemory.ofBytes(12_000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 3);
        try {
            MemoryManager a = mem.newOperatorMemory();
            MemoryManager b = mem.newOperatorMemory();
            assertNotSame(a, b, "each operator has its own memory manager");
            assertEquals(4000, a.budget());
            assertEquals(4000, b.budget());
            assertEquals(9000, pool.availableBytes(), "the three floors are registered once at construction");
            assertEquals(0, pool.snapshot().reservedBytes());
            mem.releaseOperatorMemory(a);
            mem.releaseOperatorMemory(b);
        } finally {
            mem.close();
        }
        assertEquals(8000, mem.telemetrySnapshot().acquiredBytes(),
                "never-claimed guarantee must not become historical acquired cap");
    }

    /** The old positive argument is now an exact owner count, so the claim after the last guaranteed owner fails steadily and creates no reservation outside the pool. */
    @Test
    void exactOwnerCountRejectsUnregisteredFallbackOwner() {
        System.setProperty("rowweave.worker.operator.maxMemory", "3000");
        System.setProperty("rowweave.worker.operator.minMemory", "512");
        ManagedMemory pool = ManagedMemory.ofBytes(9000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 3);
        try {
            MemoryManager a = mem.newOperatorMemory();
            MemoryManager b = mem.newOperatorMemory();
            MemoryManager c = mem.newOperatorMemory();
            assertThrows(IllegalStateException.class, mem::newOperatorMemory);
            assertEquals(3 * 512, pool.snapshot().guaranteedBytes());
            mem.releaseOperatorMemory(a);
            mem.releaseOperatorMemory(b);
            mem.releaseOperatorMemory(c);
        } finally {
            mem.close();
        }
    }

    /** Lazy: breakerCount=0 for a pure pass-through graph claims no pool budget, and newOperatorMemory fails loudly. */
    @Test
    void zeroBreakerCountDoesNotAcquireAndFailsLoudOnRequest() {
        ManagedMemory pool = ManagedMemory.ofBytes(10_000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 0);
        try {
            assertEquals(10_000, pool.availableBytes(), "no breaker means the pool is untouched");
            assertThrows(IllegalStateException.class, mem::newOperatorMemory,
                    "requesting memory outside a breaker context means the breaker count was wrong and fails loudly");
            assertNull(mem.spillRootDir(), "no breaker means no temp directory");
            ExecutionMemorySnapshot snapshot = mem.telemetrySnapshot();
            assertTrue(snapshot.complete());
            assertEquals(0L, snapshot.peakReservedBytes());
            assertEquals(0L, snapshot.acquiredBytes());
        } finally {
            mem.close();
        }
    }

    @Test
    void telemetrySnapshotCombinesLiveAndReleasedOwnersAndSurvivesClose() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4000");
        System.setProperty("rowweave.worker.operator.minMemory", "1000");
        ManagedMemory pool = ManagedMemory.ofBytes(12_000);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 2);
        MemoryManager released = memory.newOperatorMemory();
        MemoryManager live = memory.newOperatorMemory();
        released.reserve(100);
        live.reserve(200);
        released.releaseExact(100);
        memory.releaseOperatorMemory(released);

        ExecutionMemorySnapshot beforeClose = memory.telemetrySnapshot();
        assertTrue(beforeClose.complete());
        assertEquals(8_000L, beforeClose.acquiredBytes());
        assertEquals(300L, beforeClose.peakReservedBytes());

        live.releaseExact(200);
        memory.releaseOperatorMemory(live);
        memory.close();
        ExecutionMemorySnapshot afterClose = memory.telemetrySnapshot();
        assertNotSame(beforeClose, afterClose, "OPEN snapshot must not freeze terminal telemetry");
        assertSame(afterClose, memory.telemetrySnapshot(), "terminal telemetry must freeze exactly once");
        assertEquals(beforeClose.acquiredBytes(), afterClose.acquiredBytes());
        assertEquals(beforeClose.peakReservedBytes(), afterClose.peakReservedBytes());
        assertEquals(pool.totalBytes(), pool.availableBytes());
    }

    @Test
    void diagnosticListenerFailureCannotUndoSuccessfulAcquire() {
        ManagedMemory pool = ManagedMemory.ofBytes(10_000);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 1);
        ExecutionMemory.setAcquireListenerForTest((level, granted, waited, rounds) -> {
            throw new AssertionError("diagnostic failed");
        });
        MemoryManager owner = assertDoesNotThrow(memory::newOperatorMemory);
        memory.releaseOperatorMemory(owner);
        memory.close();
        assertEquals(pool.totalBytes(), pool.availableBytes());
        assertTrue(memory.telemetrySnapshot().complete());
    }

    @Test
    void telemetryAggregationOverflowDoesNotEscapeOrWrap() {
        System.setProperty("rowweave.worker.operator.maxMemory", Long.toString(Long.MAX_VALUE));
        System.setProperty("rowweave.worker.operator.minMemory", "1");
        ManagedMemory pool = ManagedMemory.ofBytes(Long.MAX_VALUE);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 2);
        MemoryManager first = memory.newOperatorMemory();
        MemoryManager second = memory.newOperatorMemory();

        ExecutionMemorySnapshot snapshot = assertDoesNotThrow(memory::telemetrySnapshot);
        assertFalse(snapshot.complete());
        assertEquals(0L, snapshot.acquiredBytes(), "overflow must not wrap into a plausible total");
        memory.releaseOperatorMemory(first);
        memory.releaseOperatorMemory(second);
        assertDoesNotThrow(memory::close);
        ExecutionMemorySnapshot terminal = memory.telemetrySnapshot();
        assertFalse(terminal.complete());
        assertSame(terminal, memory.telemetrySnapshot());
        assertEquals(pool.totalBytes(), pool.availableBytes());
    }

    @Test
    void pressureCounterOverflowOnlyInvalidatesTelemetry() throws Exception {
        ManagedMemory pool = ManagedMemory.ofBytes(10_000);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 1);
        MemoryManager manager = memory.newOperatorMemory();
        java.lang.reflect.Field counterField = MemoryManager.class.getDeclaredField("spillCount");
        counterField.setAccessible(true);
        java.util.concurrent.atomic.AtomicLong counter =
                (java.util.concurrent.atomic.AtomicLong) counterField.get(manager);
        counter.set(Long.MAX_VALUE);

        assertDoesNotThrow(manager::recordSelfSpill);
        assertEquals(Long.MAX_VALUE, manager.spillCount(), "counter must not wrap");
        memory.releaseOperatorMemory(manager);
        ExecutionMemorySnapshot snapshot = memory.telemetrySnapshot();
        assertFalse(snapshot.complete());
        assertDoesNotThrow(memory::close);
        assertEquals(pool.totalBytes(), pool.availableBytes());
    }

    /** close returns every unclaimed owner guarantee and recursively deletes the temp root directory. */
    @Test
    void closeReleasesAllOpMemoryAndDeletesTempDir() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4000");
        System.setProperty("rowweave.worker.operator.minMemory", "1000");
        ManagedMemory pool = ManagedMemory.ofBytes(12_000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 3);
        File spillRoot = mem.spillRootDir();
        assertNotNull(spillRoot);
        assertTrue(spillRoot.isDirectory(), "the temp root directory exists");
        mem.close();
        assertEquals(12_000, pool.availableBytes(), "close returns every operator allowance to the pool");
        assertFalse(spillRoot.exists(), "close deletes the temp root directory recursively");
    }

    /** The segmentPool and newSpillStore are independent per operator. */
    @Test
    void segmentPoolAndSpillStorePerOp() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4000");
        System.setProperty("rowweave.worker.operator.minMemory", "1000");
        ManagedMemory pool = ManagedMemory.ofBytes(10_000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 2);
        try {
            MemorySegmentPool p1 = mem.segmentPool();
            MemorySegmentPool p2 = mem.segmentPool();
            assertNotSame(p1, p2, "the segment pool is private to the operator and not shared");
            SpillStore s1 = mem.newSpillStore();
            SpillStore s2 = mem.newSpillStore();
            assertNotSame(s1, s2, "each operator gets its own spill store directory");
        } finally {
            mem.close();
        }
    }

    /** acquiredBytes and peakReservedBytes aggregate every operator manager, which the ExecutionReport no-OOM assertions depend on. */
    @Test
    void aggregatesAcquiredAndPeakAcrossOps() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4000");
        System.setProperty("rowweave.worker.operator.minMemory", "1000");
        ManagedMemory pool = ManagedMemory.ofBytes(12_000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 3);
        try {
            MemoryManager a = mem.newOperatorMemory();
            MemoryManager b = mem.newOperatorMemory();
            assertEquals(8000, mem.acquiredBytes(), "the aggregate is the sum of the operator budgets (2 x 4000)");
            a.reserve(1000);
            b.reserve(500);
            assertEquals(1500, mem.peakReservedBytes(), "the aggregate peak is the sum of the operator peaks");
            a.releaseExact(1000);
            b.releaseExact(500);
            mem.releaseOperatorMemory(a);
            mem.releaseOperatorMemory(b);
        } finally {
            mem.close();
        }
    }

    /**
     * The defensive no-OOM tripwire: assertWithinBudget does not throw under normal operation,
     * where peak stays at or below acquired by construction. The cluster Master.run* paths call
     * it before reporting success, since a cluster run has no ExecutionReport exit and this is
     * the unified no-OOM exit.
     */
    @Test
    void assertWithinBudgetPassesUnderNormalUse() {
        System.setProperty("rowweave.worker.operator.maxMemory", "4000");
        System.setProperty("rowweave.worker.operator.minMemory", "1000");
        ManagedMemory pool = ManagedMemory.ofBytes(12_000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 3);
        try {
            MemoryManager a = mem.newOperatorMemory();
            a.reserve(2000);  // within the 4000 sub-budget
            mem.assertWithinBudget();  // does not throw
            a.releaseExact(2000);
            mem.releaseOperatorMemory(a);
        } finally {
            mem.close();
        }
    }

    /** A tiny pool that cannot hold the full minimum guarantee set refuses steadily, before any workspace or owner side effect. */
    @Test
    void tinyPoolRejectsUnaccountedFallbackOwners() {
        System.setProperty("rowweave.worker.operator.minMemory", "512");
        ManagedMemory pool = ManagedMemory.ofBytes(2);
        assertThrows(MemoryBudgetExceededException.class, () -> ExecutionMemory.forJob(pool, 5));
        assertEquals(2, pool.availableBytes());
    }

    /**
     * Guard: a SortBuffer is never passively cross-spilled. PointerSortBuffer,
     * SpillingSegmentSortBuffer and SpillingSortBuffer must not implement MemorySpillable,
     * because under a parallel OrderBy where N threads share a manager, a cross-spill from
     * reserve would spill the sort buffer another thread is writing, which races. Each of
     * them spills only inside its own thread's active addEntry.
     */
    @Test
    void sortBuffersAreNotPassiveSpillables() {
        assertFalse(MemorySpillable.class.isAssignableFrom(PointerSortBuffer.class),
                "PointerSortBuffer must not implement MemorySpillable because parallel OrderBy shares one memory manager across N threads and the spills would race");
        assertFalse(MemorySpillable.class.isAssignableFrom(SpillingSegmentSortBuffer.class),
                "SpillingSegmentSortBuffer must not implement MemorySpillable");
        assertFalse(MemorySpillable.class.isAssignableFrom(SpillingSortBuffer.class),
                "SpillingSortBuffer must not implement MemorySpillable");
    }
}
