package io.rowweave.internal;

import io.rowweave.incremental.IncrementalSourceActionBudget;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** source-control work has independent fixed normal/large lanes. */
class IncrementalSourceControlLedgerTest {

    @Test
    void exactNormalAndLargeCapacityRejectOneMoreAndReleaseToZero() {
        IncrementalSourceControlLedger ledger =
                IncrementalSourceControlLedger.forTesting();
        List<IncrementalSourceControlLedger.Lease> normal = new ArrayList<>();
        List<IncrementalSourceControlLedger.Lease> large = new ArrayList<>();
        long deadline = System.nanoTime() + 5_000_000_000L;

        for (int i = 0; i < 64; i++) {
            normal.add(ledger.acquireNormal(
                    IncrementalSourceActionBudget.MAX_NORMAL_RESIDENT_BYTES,
                    deadline));
        }
        for (int i = 0; i < 8; i++) {
            large.add(ledger.acquireLarge(
                    IncrementalSourceActionBudget.MAX_LARGE_RESIDENT_BYTES,
                    deadline));
        }
        assertEquals(64, ledger.normalActionsInUse());
        assertEquals(8, ledger.largeActionsInUse());
        assertEquals(64L * 1024L * 1024L,
                ledger.normalResidentBytesInUse());
        assertEquals(192L * 1024L * 1024L,
                ledger.largeResidentBytesInUse());

        IllegalStateException normalFull = assertThrows(
                IllegalStateException.class,
                () -> ledger.acquireNormal(1L, System.nanoTime()));
        assertTrue(normalFull.getMessage().contains(
                "INCREMENTAL_SOURCE_NORMAL_LANE_EXHAUSTED"));
        IllegalStateException largeFull = assertThrows(
                IllegalStateException.class,
                () -> ledger.acquireLarge(1L, System.nanoTime()));
        assertTrue(largeFull.getMessage().contains(
                "INCREMENTAL_SOURCE_LARGE_LANE_EXHAUSTED"));

        for (IncrementalSourceControlLedger.Lease lease : normal) lease.close();
        for (IncrementalSourceControlLedger.Lease lease : large) lease.close();
        assertEquals(0, ledger.normalActionsInUse());
        assertEquals(0, ledger.largeActionsInUse());
        assertEquals(0L, ledger.normalResidentBytesInUse());
        assertEquals(0L, ledger.largeResidentBytesInUse());
    }

    @Test
    void perActionCeilingsAndLeaseCloseAreFailClosedAndIdempotent() {
        IncrementalSourceControlLedger ledger =
                IncrementalSourceControlLedger.forTesting();
        long deadline = System.nanoTime() + 5_000_000_000L;

        assertThrows(IllegalArgumentException.class,
                () -> ledger.acquireNormal(0L, deadline));
        assertThrows(IllegalArgumentException.class,
                () -> ledger.acquireNormal(
                        IncrementalSourceActionBudget.MAX_NORMAL_RESIDENT_BYTES + 1L,
                        deadline));
        assertThrows(IllegalArgumentException.class,
                () -> ledger.acquireLarge(
                        IncrementalSourceActionBudget.MAX_LARGE_RESIDENT_BYTES + 1L,
                        deadline));

        IncrementalSourceControlLedger.Lease lease =
                ledger.acquireLarge(1024L, deadline);
        lease.close();
        lease.close();
        assertEquals(0, ledger.largeActionsInUse());
        assertEquals(0L, ledger.largeResidentBytesInUse());
    }
}
