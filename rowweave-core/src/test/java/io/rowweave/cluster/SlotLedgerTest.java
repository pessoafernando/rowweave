package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@link SlotLedger} cluster slot ledger — the master is authoritative, taking slots on submit and
 * releasing them at a terminal state.
 * acquire(jobId, perWorkerDemand, timeout): under the lock, check that every worker has enough free
 * slots, then take them and return true; if not, await until the timeout and return false.
 * release(jobId) returns what that job holds; releaseWorker(workerId) clears everything a worker held
 * (the worker died).
 * Ledger invariants: concurrent acquire never oversells, and after every release the count returns to
 * zero (no leaks).
 */
class SlotLedgerTest {

    private static Map<String, Integer> demand(Object... kv) {
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], (Integer) kv[i + 1]);
        }
        return m;
    }

    @Test
    void acquireWithinCapacityThenReject() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 8);
        ledger.setWorkerCapacity("w1", 8);

        assertTrue(ledger.acquire("j1", demand("w0", 4, "w1", 4), 100), "taking 8 of 16 capacity is granted");
        assertTrue(ledger.acquire("j2", demand("w0", 4), 100), "w0 takes 4 more for a total of 8 and is granted");
        assertFalse(ledger.acquire("j3", demand("w0", 1), 200), "w0 is full so the request times out");
    }

    @Test
    void releaseFreesSlots() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 4);
        assertTrue(ledger.acquire("j1", demand("w0", 4), 100), "taken");
        assertFalse(ledger.acquire("j2", demand("w0", 1), 100), "full and refused");
        ledger.release("j1");
        assertTrue(ledger.acquire("j2", demand("w0", 1), 100), "granted after the release");
    }

    @Test
    void releaseWorkerClearsAll() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 8);
        ledger.acquire("j1", demand("w0", 5), 100);
        ledger.acquire("j2", demand("w0", 3), 100);
        assertFalse(ledger.acquire("j3", demand("w0", 1), 100), "full");
        ledger.releaseWorker("w0");
        ledger.setWorkerCapacity("w0", 8);
        assertTrue(ledger.acquire("j3", demand("w0", 8), 100), "the full capacity is available again after the worker re-registers");
    }

    @Test
    void demandExceedsCapacityFailsFast() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 4);
        long t0 = System.currentTimeMillis();
        assertFalse(ledger.acquire("j1", demand("w0", 5), 5000), "demand 5 over capacity 4 fails fast");
        assertTrue(System.currentTimeMillis() - t0 < 1000, "the fail-fast path does not wait the full 5s");
    }

    @Test
    void waiterWokenByRelease() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 4);
        assertTrue(ledger.acquire("j1", demand("w0", 4), 100), "taken");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                got.set(ledger.acquire("j2", demand("w0", 4), 5000));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        started.await();
        Thread.sleep(200);
        ledger.release("j1");
        waiter.join(3000);
        assertTrue(got.get(), "the waiter is woken by the release and acquires");
    }

    @Test
    void waiterTimesOutWhenNeverReleased() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 4);
        assertTrue(ledger.acquire("j1", demand("w0", 4), 100), "taken");
        long t0 = System.currentTimeMillis();
        assertFalse(ledger.acquire("j2", demand("w0", 4), 300), "held without release leaves the waiter to time out");
        long dt = System.currentTimeMillis() - t0;
        assertTrue(dt >= 250, "the wait really runs up to the timeout instead of failing fast, waited " + dt + "ms");
    }

    @Test
    void snapshotReportsFreeAndCapacity() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 8);
        ledger.setWorkerCapacity("w1", 4);
        ledger.acquire("j1", demand("w0", 3), 100);

        java.util.Map<String, int[]> snap = ledger.snapshot();
        assertArrayEquals(new int[]{5, 8}, snap.get("w0"), "w0 holds 3 so free=5 cap=8");
        assertArrayEquals(new int[]{4, 4}, snap.get("w1"), "w1 is unused so free=4 cap=4");
        // snapshot is read-only: the ledger is still the authoritative state.
        assertEquals(5, ledger.freeSlots("w0"), "a snapshot leaves the state unchanged");
    }

    @Test
    void bestFitPicksHighestFeasibleCandidate() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 4);
        java.util.List<Map<String, Integer>> cands = java.util.Arrays.asList(
                demand("w0", 8), demand("w0", 4), demand("w0", 2), demand("w0", 1));
        int idx = ledger.acquireBestFit("j1", cands, 100);
        assertEquals(1, idx, "capacity 4 selects the parallelism 4 candidate at index 1");
        assertEquals(0, ledger.freeSlots("w0"), "all 4 taken");
    }

    @Test
    void bestFitReturnsMinusOneWhenNoneFit() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 0);
        java.util.List<Map<String, Integer>> cands = java.util.Arrays.asList(
                demand("w0", 4), demand("w0", 1));
        assertEquals(-1, ledger.acquireBestFit("j1", cands, 100), "zero capacity fits not even the lowest candidate and returns -1");
    }

    @Test
    void bestFitWaitsThenPicksAfterRelease() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 4);
        assertTrue(ledger.acquire("occupy", demand("w0", 4), 100), "taken first");
        java.util.List<Map<String, Integer>> cands = java.util.Arrays.asList(
                demand("w0", 4), demand("w0", 2));
        AtomicBoolean done = new AtomicBoolean(false);
        int[] gotIdx = {-99};
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                gotIdx[0] = ledger.acquireBestFit("j1", cands, 5000);
                done.set(true);
            } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        });
        waiter.start();
        started.await();
        Thread.sleep(200);
        ledger.release("occupy");
        waiter.join(3000);
        assertTrue(done.get(), "best-fit is woken by the release and succeeds");
        assertEquals(0, gotIdx[0], "after the release the full capacity selects the highest candidate at index 0");
    }

    /** reconcile reports drift without changing state; the ledger stays authoritative, and an unknown worker (-1) is skipped. */
    @Test
    void reconcileReportsDriftWithoutMutating() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 8);
        ledger.acquire("j1", demand("w0", 3), 100); // the ledger computes 5 slots free

        assertEquals(0, ledger.reconcile("w0", 5), "the worker reports exactly what the ledger holds, so drift is 0");
        assertEquals(2, ledger.reconcile("w0", 3), "the worker reports 2 fewer than the ledger, so drift is 2");
        assertEquals(-1, ledger.reconcile("w0", 6), "the worker reports 1 more than the ledger, so drift is -1");
        assertEquals(0, ledger.reconcile("w0", -1), "an unknown value of -1 is skipped without a warning");

        // reconcile does not change any state: the ledger still holds 3 taken and 5 free.
        assertEquals(5, ledger.freeSlots("w0"), "reconcile leaves the ledger unchanged");
        assertFalse(ledger.acquire("j2", demand("w0", 6), 100), "the ledger is still authoritative: only 5 are free, so 6 is refused");
    }

    @Test
    void concurrentAcquireNoOversell() throws Exception {
        SlotLedger ledger = new SlotLedger();
        ledger.setWorkerCapacity("w0", 8);
        int n = 16;
        CountDownLatch go = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger ok = new java.util.concurrent.atomic.AtomicInteger();
        Thread[] ts = new Thread[n];
        for (int i = 0; i < n; i++) {
            final String jid = "j" + i;
            ts[i] = new Thread(() -> {
                try {
                    go.await();
                    if (ledger.acquire(jid, demand("w0", 1), 300)) {
                        ok.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
            ts[i].start();
        }
        go.countDown();
        for (Thread t : ts) t.join(5000);
        assertEquals(8, ok.get(), "capacity 8 with 16 concurrent requests of 1 grants exactly 8 and never oversells");
    }
}

