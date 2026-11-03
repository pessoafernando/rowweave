package io.rowweave.mem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** slice 2: exact execution-owner admission, workspace separation and deferred finalization. */
class ExecutionMemoryPartitionAdmissionTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    @Test
    void constructionAtomicallyRegistersGuaranteesButLeavesActualReservationAtZero() {
        configure(80, 20);
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, true,
                MemoryOwnerRequirement.ordinary(), MemoryOwnerRequirement.ordinary());
        try {
            ManagedMemorySnapshot admitted = pool.snapshot();
            assertEquals(40, admitted.guaranteedBytes());
            assertEquals(40, admitted.committedBytes());
            assertEquals(0, admitted.reservedBytes());
            assertEquals(2, admitted.activeOwnerCount());

            MemoryManager first = memory.newOperatorMemory();
            MemoryManager second = memory.newOperatorMemory();
            assertEquals(80, first.budget());
            assertEquals(20, first.guaranteedBytes());
            assertEquals(40, pool.snapshot().committedBytes(),
                    "claiming a pre-admitted token is not an actual reservation");
            assertThrows(IllegalStateException.class, memory::newOperatorMemory,
                    "the legacy count is now an exact owner count");
            memory.releaseOperatorMemory(first);
            memory.releaseOperatorMemory(second);
        } finally {
            memory.close();
        }
        assertEquals(100, pool.availableBytes());
    }

    @Test
    void admissionFailureHasNoWorkspaceOrPartialOwnerSideEffects() {
        configure(80, 20);
        ManagedMemory pool = ManagedMemory.ofBytes(30);
        AtomicInteger workspaceCalls = new AtomicInteger();

        assertThrows(MemoryBudgetExceededException.class, () -> ExecutionMemory.forJob(
                pool, true, () -> {
                    workspaceCalls.incrementAndGet();
                    return null;
                }, MemoryOwnerRequirement.ordinary(), MemoryOwnerRequirement.ordinary()));
        assertEquals(0, workspaceCalls.get(), "capacity must reject before workspace creation");
        assertEquals(30, pool.availableBytes());
        assertEquals(0, pool.snapshot().activeOwnerCount());
    }

    @Test
    void diskOnlyWorkspaceDoesNotRegisterAFakeMemoryOwner() {
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, true);
        File root = memory.spillRootDir();
        assertNotNull(root);
        assertTrue(root.isDirectory());
        assertEquals(0, pool.snapshot().activeOwnerCount());
        assertThrows(IllegalStateException.class, memory::newOperatorMemory);

        memory.close();
        assertFalse(root.exists());
        assertEquals(100, pool.availableBytes());
    }

    @Test
    void partitionArityIsValidatedDuringAdmission() {
        configure(8, 1);
        ManagedMemory pool = ManagedMemory.ofBytes(8);
        assertThrows(MemoryBudgetExceededException.class, () -> ExecutionMemory.forJob(
                pool, true, MemoryOwnerRequirement.partitioned(2)));
        assertEquals(8, pool.availableBytes());
    }

    @Test
    void heterogeneousOwnersAreClaimedByContractInsteadOfRegistrationOrder() {
        configure(80, 20);
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, true,
                MemoryOwnerRequirement.ordinary(),
                MemoryOwnerRequirement.partitioned(2));

        MemoryManager partitioned = memory.newOperatorMemoryFor(
                MemoryOwnerRequirement.partitioned(2));
        MemoryManager ordinary = memory.newOperatorMemoryFor(
                MemoryOwnerRequirement.ordinary());
        assertEquals(2, partitioned.partitionEvenly(2).length);
        assertThrows(IllegalStateException.class,
                () -> memory.newOperatorMemoryFor(
                        MemoryOwnerRequirement.partitioned(2)));

        memory.releaseOperatorMemory(partitioned);
        memory.releaseOperatorMemory(ordinary);
        memory.close();
        assertEquals(100, pool.availableBytes());
        assertEquals(0, pool.snapshot().activeOwnerCount());
    }

    @Test
    void cancellationCloseDefersPoolAndWorkspaceReleaseUntilActiveOwnerExits() {
        configure(100, 20);
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        ExecutionMemory memory = ExecutionMemory.forJob(
                pool, true, MemoryOwnerRequirement.ordinary());
        MemoryManager owner = memory.newOperatorMemory();
        MemorySpillable spillable = () -> 0;
        owner.registerSpillable(spillable);
        owner.reserve(60);
        File root = memory.spillRootDir();

        assertThrows(IllegalStateException.class, memory::close);
        assertTrue(root.exists(), "dirty close cannot destroy an active owner's workspace");
        assertEquals(40, pool.availableBytes());
        assertThrows(IllegalStateException.class, () -> owner.tryReserve(1));

        owner.releaseExact(60);
        owner.unregisterSpillable(spillable);
        assertEquals(80, pool.availableBytes(), "guarantee remains until owner-exited signal");
        memory.releaseOperatorMemory(owner);

        assertEquals(100, pool.availableBytes());
        assertFalse(root.exists(), "last owner finalizer closes workspace exactly once");
        assertDoesNotThrow(memory::close);
        ExecutionMemorySnapshot terminal = memory.telemetrySnapshot();
        assertSame(terminal, memory.telemetrySnapshot());
    }

    @Test
    void legacyHistoricalOwnerPeaksCanExceedTotalWhilePoolSimultaneousPeakCannot() {
        configure(80, 20);
        ManagedMemory pool = ManagedMemory.ofBytes(100);
        ExecutionMemory memory = ExecutionMemory.forJob(pool, 2);
        MemoryManager first = memory.newOperatorMemory();
        MemoryManager second = memory.newOperatorMemory();

        first.reserve(80);
        first.releaseExact(80);
        memory.releaseOperatorMemory(first);
        second.reserve(80);
        second.releaseExact(80);
        memory.releaseOperatorMemory(second);

        ExecutionMemorySnapshot live = memory.telemetrySnapshot();
        assertEquals(160, live.acquiredBytes());
        assertEquals(160, live.peakReservedBytes());
        assertEquals(80, pool.snapshot().peakReservedBytes());
        assertDoesNotThrow(memory::assertWithinBudget);

        memory.close();
        ExecutionMemorySnapshot terminal = memory.telemetrySnapshot();
        assertEquals(160, terminal.peakReservedBytes());
        assertSame(terminal, memory.telemetrySnapshot());
    }

    private static void configure(long max, long min) {
        System.setProperty("rowweave.worker.operator.maxMemory", Long.toString(max));
        System.setProperty("rowweave.worker.operator.minMemory", Long.toString(min));
    }
}
