package io.rowweave.mem;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ManagedMemoryTest {

    @Test
    void acquireReturnsScopedManagerAndDrawsFromPool() {
        ManagedMemory pool = ManagedMemory.ofBytes(1000);
        MemoryManager mgr = pool.acquire(600);
        assertEquals(600, mgr.budget());
        assertEquals(400, pool.availableBytes());
    }

    @Test
    void acquireOverPoolThrows() {
        ManagedMemory pool = ManagedMemory.ofBytes(1000);
        pool.acquire(800);
        assertThrows(MemoryBudgetExceededException.class, () -> pool.acquire(300));
    }

    @Test
    void releaseReturnsQuotaToPool() {
        ManagedMemory pool = ManagedMemory.ofBytes(1000);
        MemoryManager mgr = pool.acquire(800);
        pool.release(mgr);
        assertEquals(1000, pool.availableBytes());
        assertNotNull(pool.acquire(900)); // freed, so this now fits
    }

    @Test
    void budgetPolicyExplicitBytesWins() {
        assertEquals(2048, MemoryBudgetPolicy.ofBytes(2048).resolveBudget(999));
    }

    @Test
    void budgetPolicyDisabledFallsBack() {
        assertEquals(777, MemoryBudgetPolicy.auto(0.0).resolveBudget(777));
    }

    @Test
    void globalPoolConfigureForTestIsHonored() {
        try {
            GlobalMemoryPool.configureForTest(5000);
            assertEquals(5000, GlobalMemoryPool.instance().totalBytes());
        } finally {
            GlobalMemoryPool.resetForTest();
        }
    }
}
