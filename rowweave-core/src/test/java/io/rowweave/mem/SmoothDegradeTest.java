package io.rowweave.mem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Smooth degradation: the single-breaker ceiling maxMem is clamp(total/slots, 2M, 16M), which
 * matches the pool capacity. The pool holds about slots breakers at full size, and an early
 * arrival takes at most maxMem, which is the pool divided by slots, so it never monopolizes the
 * whole pool. This replaces the cliff a fixed 16M used to give, where a 96M pool served only six
 * breakers at full size and every seventh onward fell to the floor guarantee.
 */
class SmoothDegradeTest {

    @AfterEach
    void clear() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
        System.clearProperty("rowweave.worker.slots");
        System.clearProperty("rowweave.worker.memory");
    }

    /** max derived from the pool divided by slots: 96M with slots=32 is 3M each, so the pool holds 32 full-size breakers, evenly and without a cliff. */
    @Test
    void maxMemoryDerivedFromPoolOverSlotsSpreadsEvenly() {
        System.setProperty("rowweave.worker.memory", String.valueOf(96L * 1024 * 1024));
        System.setProperty("rowweave.worker.slots", "32");
        // max = clamp(96M/32=3M, 2M, 16M) = 3M
        assertEquals(3L * 1024 * 1024, WorkerConfig.operatorMaxMemory(), "max = 96M/32 = 3M (inside the [2M, 16M] clamp)");

        ManagedMemory pool = ManagedMemory.ofBytes(96L * 1024 * 1024);
        ExecutionMemory mem = ExecutionMemory.forJob(pool, 32);
        List<MemoryManager> owners = new ArrayList<>();
        try {
            List<Long> budgets = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                MemoryManager owner = mem.newOperatorMemory();
                owners.add(owner);
                budgets.add(owner.budget());
            }
            // 32 breakers take 3M each (96M/32), all at full size and evenly, with no earlier ones at 16M and the rest at the floor.
            for (long b : budgets) {
                assertEquals(3L * 1024 * 1024, b, "every share is an even 3M with no cliff: " + budgets);
            }
        } finally {
            for (MemoryManager owner : owners) mem.releaseOperatorMemory(owner);
            mem.close();
        }
    }

    /** max is clamped to [2M,16M]: very large memory makes pool/slots large, which the 16M ceiling caps; very small memory is lifted to the 2M floor. */
    @Test
    void maxMemoryClampedBetween2MAnd16M() {
        // large memory: 32G/32 = 1G, so capped at 16M
        System.setProperty("rowweave.worker.memory", String.valueOf(32L * 1024 * 1024 * 1024));
        System.setProperty("rowweave.worker.slots", "32");
        assertEquals(16L * 1024 * 1024, WorkerConfig.operatorMaxMemory(), "however large the memory, one breaker tops out at the 16M ceiling");

        // small memory: 32M/32 = 1M, so lifted to the 2M floor
        System.setProperty("rowweave.worker.memory", String.valueOf(32L * 1024 * 1024));
        assertEquals(2L * 1024 * 1024, WorkerConfig.operatorMaxMemory(), "a small memory pool still gets the 2M floor");
    }

    /** When the full minimum-guarantee set exceeds the pool total, reject atomically instead of creating a reservation that is not accounted for. */
    @Test
    void ownerGuaranteesBeyondPoolRejectAtomically() {
        System.setProperty("rowweave.worker.memory", String.valueOf(8L * 1024 * 1024));
        System.setProperty("rowweave.worker.slots", "2"); // max=clamp(8M/2=4M,2M,16M)=4M
        System.setProperty("rowweave.worker.operator.minMemory", "524288"); // 512K
        ManagedMemory pool = ManagedMemory.ofBytes(8L * 1024 * 1024);
        assertThrows(MemoryBudgetExceededException.class,
                () -> ExecutionMemory.forJob(pool, 17));
        assertEquals(pool.totalBytes(), pool.availableBytes());
    }
}
