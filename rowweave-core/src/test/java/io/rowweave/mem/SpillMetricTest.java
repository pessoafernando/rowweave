package io.rowweave.mem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where the spill metrics come from. MemoryManager.reserve() counts a spill when it calls
 * spillable.spill(), and ExecutionMemory aggregates the spill counts of the per-operator
 * managers, the same way it aggregates peakReservedBytes. The counters are read-only and
 * change no semantics.
 */
class SpillMetricTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    /** A controlled spillable: spill() hands back a fixed amount and records how often it was called. */
    private static final class FakeSpillable implements MemorySpillable {
        private final MemoryManager mm;
        private long held;
        int spillCalls;

        FakeSpillable(MemoryManager mm) {
            this.mm = mm;
        }

        void reserve(long bytes) {
            mm.reserve(bytes);
            held += bytes;
        }

        @Override
        public long spill() {
            spillCalls++;
            long freed = held;
            held = 0;
            mm.release(freed);  // hand back everything, so a later tryReserve can succeed
            return freed;
        }
    }

    /** A reserve that falls short triggers a spill, which MemoryManager.spillCount() counts. */
    @Test
    void reserveTriggeringSpillIsCounted() {
        MemoryManager mm = new MemoryManager(100);
        FakeSpillable s = new FakeSpillable(mm);
        mm.registerSpillable(s);

        s.reserve(80);                 // reserved=80
        assertEquals(0, mm.spillCount(), "no spill was triggered");

        mm.reserve(50);  // 80+50 is over 100, so spill s hands back 80 and the retry of 50 succeeds
        assertEquals(1, mm.spillCount(), "one spill");
        assertEquals(1, s.spillCalls);
    }

    /** Enough room means a plain reserve with no spill, so spillCount stays 0. */
    @Test
    void reserveWithoutSpillCountsZero() {
        MemoryManager mm = new MemoryManager(100);
        mm.registerSpillable(new FakeSpillable(mm));
        mm.reserve(50);
        assertEquals(0, mm.spillCount());
    }

    /** ExecutionMemory aggregates the spillCount of each per-operator manager, the same way as peakReservedBytes. */
    @Test
    void executionMemoryAggregatesSpillCount() {
        System.setProperty("rowweave.worker.operator.maxMemory", "500");
        System.setProperty("rowweave.worker.operator.minMemory", "100");
        ManagedMemory pool = ManagedMemory.ofBytes(1000);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 2);  // two breaker sub-budgets
        try {
            MemoryManager mm1 = mem.newOperatorMemory();
            MemoryManager mm2 = mem.newOperatorMemory();
            FakeSpillable s1 = new FakeSpillable(mm1);
            FakeSpillable s2 = new FakeSpillable(mm2);
            mm1.registerSpillable(s1);
            mm2.registerSpillable(s2);

            long perOp = mm1.budget();  // each operator sub-budget is 1000/2 = 500
            s1.reserve(perOp - 10);
            mm1.reserve(50);  // triggers a spill in mm1
            s2.reserve(perOp - 10);
            mm2.reserve(50);  // triggers a spill in mm2

            assertEquals(2, mem.spillCount(), "both aggregation operators spill once each");
            mm1.releaseExact(mm1.reservedBytes());
            mm2.releaseExact(mm2.reservedBytes());
            mm1.unregisterSpillable(s1);
            mm2.unregisterSpillable(s2);
            mem.releaseOperatorMemory(mm1);
            mem.releaseOperatorMemory(mm2);
        } finally {
            mem.close();
        }
    }
}
