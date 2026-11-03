package io.rowweave.mem;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MemoryManagerTest {

    @Test
    void reserveWithinBudgetSucceeds() {
        MemoryManager mm = new MemoryManager(100);
        assertTrue(mm.reserve(60));
        assertEquals(60, mm.reservedBytes());
        assertEquals(40, mm.availableBytes());
    }

    @Test
    void tryReserveOverBudgetFailsWithoutChangingState() {
        MemoryManager mm = new MemoryManager(100);
        assertTrue(mm.tryReserve(80));
        assertFalse(mm.tryReserve(30)); // 80+30 > 100
        assertEquals(80, mm.reservedBytes());
    }

    @Test
    void releaseGivesBudgetBack() {
        MemoryManager mm = new MemoryManager(100);
        mm.reserve(80);
        mm.release(50);
        assertEquals(30, mm.reservedBytes());
        assertTrue(mm.reserve(70)); // 30+70 = 100 ok
    }

    @Test
    void releaseNeverGoesNegative() {
        MemoryManager mm = new MemoryManager(100);
        mm.reserve(10);
        mm.release(50);
        assertEquals(0, mm.reservedBytes());
    }

    @Test
    void reserveTriggersSpillThenSucceeds() {
        MemoryManager mm = new MemoryManager(100);
        mm.reserve(90); // near full
        AtomicInteger spillCalls = new AtomicInteger();
        mm.registerSpillable(() -> {
            spillCalls.incrementAndGet();
            mm.release(90); // spillable lets go of its memory
            return 90;
        });
        assertTrue(mm.reserve(50)); // 90+50 too big -> spill frees 90 -> retry ok
        assertEquals(1, spillCalls.get());
        assertEquals(50, mm.reservedBytes());
    }

    @Test
    void reserveThrowsWhenSpillInsufficient() {
        MemoryManager mm = new MemoryManager(100);
        mm.reserve(90);
        mm.registerSpillable(() -> 0); // cannot free anything
        assertThrows(MemoryBudgetExceededException.class, () -> mm.reserve(50));
    }

    @Test
    void budgetMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new MemoryManager(0));
        assertThrows(IllegalArgumentException.class, () -> new MemoryManager(-1));
    }

    @Test
    void childPartitionsShareOneAtomicRootLedger() {
        MemoryManager root = new MemoryManager(101);
        MemoryManager[] sides = root.partitionEvenly(2);

        assertEquals(51, sides[0].budget());
        assertEquals(50, sides[1].budget());
        assertTrue(sides[0].tryReserve(51));
        assertTrue(sides[1].tryReserve(50));
        assertEquals(101, root.reservedBytes());
        assertEquals(51, sides[0].reservedBytes());
        assertEquals(50, sides[1].reservedBytes());
        assertFalse(sides[0].tryReserve(1));

        sides[0].releaseExact(51);
        sides[1].releaseExact(50);
        assertEquals(0, root.reservedBytes());
        assertEquals(0, sides[0].reservedBytes());
        assertEquals(0, sides[1].reservedBytes());
    }

    @Test
    void exactReleaseAndInvalidPartitionFailLoud() {
        MemoryManager root = new MemoryManager(8);
        MemoryManager child = root.partitionEvenly(2)[0];
        child.reserve(3);
        assertThrows(IllegalStateException.class, () -> child.releaseExact(4));
        assertEquals(3, child.reservedBytes());
        assertEquals(3, root.reservedBytes(), "underflow validation must not half-commit root ledger");

        assertThrows(IllegalArgumentException.class, () -> new MemoryManager(1).partitionEvenly(2));
    }

    @Test
    void handoffRejectsRegisteredOwnerEvenWhenLedgerIsZero() {
        MemoryManager root = new MemoryManager(1024);
        MemorySpillable owner = () -> 0;
        root.registerSpillable(owner);

        assertThrows(IllegalStateException.class, () -> root.partitionEvenly(2));

        root.unregisterSpillable(owner);
        assertEquals(2, root.partitionEvenly(2).length);
    }

    // ---- peak reservation is observable, which upgrades the no-OOM invariant from inference to assertion ----

    /** The peak is the historical high water mark: release does not lower it, and a later lower occupancy does not overwrite it. */
    @Test
    void peakIsHighWaterMarkNotCurrent() {
        MemoryManager mm = new MemoryManager(200);
        assertEquals(0, mm.peakReservedBytes());
        mm.reserve(100);
        assertEquals(100, mm.peakReservedBytes());
        mm.release(100);
        assertEquals(0, mm.reservedBytes());
        assertEquals(100, mm.peakReservedBytes(), "release does not lower the peak (the high watermark only rises)");
        mm.reserve(50);
        assertEquals(100, mm.peakReservedBytes(), "a lower occupancy does not overwrite the recorded peak");
        mm.reserve(120);  // 50 plus 120 is 170, a new high
        assertEquals(170, mm.peakReservedBytes());
    }

    /** A successful tryReserve also raises the peak; a failed one leaves it unchanged. */
    @Test
    void tryReserveUpdatesPeakOnSuccessOnly() {
        MemoryManager mm = new MemoryManager(100);
        assertTrue(mm.tryReserve(80));
        assertEquals(80, mm.peakReservedBytes());
        assertFalse(mm.tryReserve(30));  // fails past the budget
        assertEquals(80, mm.peakReservedBytes(), "a failed tryReserve leaves the peak unchanged");
    }

    /** A retry after a spill succeeds: the peak reflects the true high water mark reached during the attempt, where 90 before the spill was the earlier peak. */
    @Test
    void peakReflectsPreSpillHighWater() {
        MemoryManager mm = new MemoryManager(100);
        mm.reserve(90);
        assertEquals(90, mm.peakReservedBytes());
        mm.registerSpillable(() -> {
            mm.release(90);
            return 90;
        });
        assertTrue(mm.reserve(50));  // the spill hands back 90, so 50 is now held
        assertEquals(50, mm.reservedBytes());
        assertEquals(90, mm.peakReservedBytes(), "the peak is the 90 held before the spill rather than the current 50");
    }
}
