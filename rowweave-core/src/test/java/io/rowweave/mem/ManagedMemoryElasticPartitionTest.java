package io.rowweave.mem;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** slice 1: process ledger, elastic borrowing and pooled-lease terminal state. */
class ManagedMemoryElasticPartitionTest {

    @Test
    void ownerSetAdmissionIsAtomicAndOverflowClosed() {
        ManagedMemory pool = ManagedMemory.ofBytes(10);

        assertThrows(MemoryBudgetExceededException.class, () -> pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(6, 8),
                ManagedMemory.OwnerSpec.elastic(5, 8)));
        assertPool(pool.snapshot(), 10, 0, 0, 0, 0, 0, 0);

        ManagedMemory huge = ManagedMemory.ofBytes(Long.MAX_VALUE);
        MemoryManager fixed = huge.acquire(Long.MAX_VALUE);
        assertThrows(MemoryBudgetExceededException.class, () -> huge.acquire(1));
        assertEquals(0, huge.availableBytes());
        huge.release(fixed);
        assertEquals(Long.MAX_VALUE, huge.availableBytes());

        assertThrows(MemoryBudgetExceededException.class, () -> huge.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(Long.MAX_VALUE / 2 + 1, Long.MAX_VALUE),
                ManagedMemory.OwnerSpec.elastic(Long.MAX_VALUE / 2 + 1, Long.MAX_VALUE)));
        assertPool(huge.snapshot(), Long.MAX_VALUE, 0, 0, 0, 0, 0, 0);
    }

    @Test
    void ownersBorrowOnlyUncommittedBytesAndReleasedCapacityIsImmediatelyReusable() {
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        MemoryManager[] owners = pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(20, 80),
                ManagedMemory.OwnerSpec.elastic(20, 80));
        MemoryManager first = owners[0];
        MemoryManager second = owners[1];

        assertPool(pool.snapshot(), 100, 40, 40, 0, 0, 2, 2);
        assertEquals(80, first.budget());
        assertEquals(20, first.guaranteedBytes());
        assertTrue(first.tryReserve(70));
        assertEquals(10, pool.availableBytes());
        assertTrue(second.tryReserve(30));
        assertFalse(second.tryReserve(1));
        assertPool(pool.snapshot(), 100, 40, 100, 100, 100, 2, 2);

        first.releaseExact(60);
        assertEquals(50, pool.availableBytes());
        assertTrue(second.tryReserve(50));
        assertPool(pool.snapshot(), 100, 40, 100, 90, 100, 2, 2);

        second.releaseExact(80);
        first.releaseExact(10);
        pool.release(first);
        pool.release(second);
        assertPool(pool.snapshot(), 100, 0, 0, 0, 100, 0, 0);
    }

    @Test
    void partitionedOwnerSplitsGuaranteeAndCapWithoutEscapingProcessLedger() {
        ManagedMemory pool = ManagedMemory.ofBytes(20);
        MemoryManager root = pool.acquireOwners(
                ManagedMemory.OwnerSpec.partitioned(5, 9, 2))[0];
        MemoryManager[] leaves = root.partitionEvenly(2);

        assertEquals(3, leaves[0].guaranteedBytes());
        assertEquals(2, leaves[1].guaranteedBytes());
        assertEquals(5, leaves[0].budget());
        assertEquals(4, leaves[1].budget());
        assertTrue(leaves[0].tryReserve(5));
        assertTrue(leaves[1].tryReserve(4));
        assertPool(pool.snapshot(), 20, 5, 9, 9, 9, 1, 2);
        assertThrows(IllegalStateException.class, () -> root.tryReserve(1));
        assertThrows(IllegalArgumentException.class, () -> pool.release(leaves[0]));

        leaves[0].releaseExact(5);
        leaves[1].releaseExact(4);
        pool.release(root);
        assertPool(pool.snapshot(), 20, 0, 0, 0, 9, 0, 0);
    }

    @Test
    void childArityIsRejectedBeforeAnyOwnerIsRegistered() {
        ManagedMemory pool = ManagedMemory.ofBytes(8);

        assertThrows(MemoryBudgetExceededException.class, () -> pool.acquireOwners(
                ManagedMemory.OwnerSpec.partitioned(1, 8, 2)));
        assertPool(pool.snapshot(), 8, 0, 0, 0, 0, 0, 0);
    }

    @Test
    void dirtyReleaseClosesForNewWorkAndFinalizesAfterExistingOwnerDrains() {
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        MemoryManager owner = pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(20, 100))[0];
        AtomicInteger spills = new AtomicInteger();
        MemorySpillable spillable = () -> {
            spills.incrementAndGet();
            return 0;
        };
        owner.registerSpillable(spillable);
        assertTrue(owner.tryReserve(60));

        pool.release(owner);
        assertEquals(40, pool.availableBytes(), "closing lease must retain its commitment");
        assertThrows(IllegalStateException.class, () -> owner.tryReserve(1));
        assertThrows(IllegalStateException.class, () -> owner.registerSpillable(() -> 0));
        assertThrows(IllegalStateException.class, () -> owner.partitionEvenly(2));

        owner.releaseExact(60);
        assertEquals(80, pool.availableBytes(), "guarantee remains until spillable owner exits");
        owner.unregisterSpillable(spillable);
        assertEquals(100, pool.availableBytes(), "last cleanup action finalizes the lease");
        assertThrows(IllegalStateException.class, () -> owner.tryReserve(1));
        pool.release(owner); // repeated/unknown release is compatible no-op
        assertEquals(0, spills.get(), "closing must not invoke another owner's spill callback");
    }

    @Test
    void availableBytesIsOnlyASnapshotAndReserveRechecksSharedCapacity() {
        ManagedMemory pool = ManagedMemory.ofBytes(12);
        MemoryManager[] owners = pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(2, 10),
                ManagedMemory.OwnerSpec.elastic(2, 10));

        long observedBySecond = owners[1].availableBytes();
        assertEquals(10, observedBySecond);
        assertTrue(owners[0].tryReserve(10));
        assertFalse(owners[1].tryReserve(observedBySecond));
        assertEquals(2, owners[1].availableBytes());
    }

    @Test
    void boundedReservationUsesCurrentSharedCapacityInOneLedgerCommit() {
        ManagedMemory pool = ManagedMemory.ofBytes(20);
        MemoryManager[] owners = pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(2, 18),
                ManagedMemory.OwnerSpec.elastic(2, 18));
        assertTrue(owners[0].tryReserve(10));
        long stalePreferred = owners[1].availableBytes();
        assertEquals(10, stalePreferred);
        assertTrue(owners[0].tryReserve(8));

        assertEquals(2, owners[1].reserveAvailableUpTo(2, stalePreferred));
        assertPool(pool.snapshot(), 20, 4, 20, 20, 20, 2, 2);
        owners[1].releaseExact(2);
        assertEquals(0, owners[1].reserveAvailableUpTo(3, stalePreferred));
        assertPool(pool.snapshot(), 20, 4, 20, 18, 20, 2, 2);

        owners[0].releaseExact(18);
        pool.release(owners[0]);
        pool.release(owners[1]);
        assertEquals(20, pool.availableBytes());
    }

    @Test
    void concurrentBorrowersCannotBothConsumeTheSameSharedRemainder() throws Exception {
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        MemoryManager[] owners = pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(20, 80),
                ManagedMemory.OwnerSpec.elastic(20, 80));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        Thread[] threads = new Thread[2];
        for (int i = 0; i < threads.length; i++) {
            MemoryManager owner = owners[i];
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    if (owner.tryReserve(80)) successes.incrementAndGet();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }, "elastic-borrower-" + i);
            threads[i].start();
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        for (Thread thread : threads) thread.join(5_000);

        assertEquals(1, successes.get());
        ManagedMemorySnapshot snapshot = pool.snapshot();
        assertEquals(100, snapshot.committedBytes());
        assertEquals(80, snapshot.reservedBytes());
        assertTrue(snapshot.reservedBytes() <= snapshot.committedBytes());
        assertTrue(snapshot.committedBytes() <= snapshot.totalBytes());

        for (MemoryManager owner : owners) {
            owner.release(owner.reservedBytes());
            pool.release(owner);
        }
        assertEquals(100, pool.availableBytes());
    }

    private static void assertPool(ManagedMemorySnapshot snapshot, long total, long guaranteed,
                                   long committed, long reserved, long peak,
                                   int roots, int leaves) {
        assertEquals(total, snapshot.totalBytes());
        assertEquals(guaranteed, snapshot.guaranteedBytes());
        assertEquals(committed, snapshot.committedBytes());
        assertEquals(reserved, snapshot.reservedBytes());
        assertEquals(peak, snapshot.peakReservedBytes());
        assertEquals(roots, snapshot.activeOwnerCount());
        assertEquals(leaves, snapshot.activeLeafCount());
        assertTrue(snapshot.committedBytes() <= snapshot.totalBytes());
        assertTrue(snapshot.reservedBytes() <= snapshot.committedBytes());
    }
}
