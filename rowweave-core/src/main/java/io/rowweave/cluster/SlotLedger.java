package io.rowweave.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The cluster slot ledger, master-authoritative. Before a job is submitted the master {@link #acquire}s
 * the slots each worker needs for it; a job reaching a terminal state (complete, fail or cancel)
 * {@link #release}s them, and a dead worker has all of its occupancy dropped by {@link #releaseWorker}.
 *
 * <p>Consistency: one {@link ReentrantLock} makes check-then-occupy atomic, so concurrent submissions
 * cannot oversell. When slots are short the caller waits on a {@link Condition} and release signals all
 * waiters to retry; the wait is bounded, and a timeout returns false instead of hanging. If the demand
 * exceeds a worker's total capacity, fail fast by returning false immediately rather than entering a wait
 * for a demand that can never be satisfied.
 *
 * <p><b>Gang admission for the whole job</b>: acquire takes the slots for every stage instance of the job
 * at once, distributed across workers — either all of it or the caller waits. This avoids partial
 * occupancy deadlock, where pass-through stages inside a pipelined region hold slots while waiting for a
 * breaker that cannot get any.
 */
public final class SlotLedger {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition released = lock.newCondition();

    private final Map<String, Integer> capacity = new HashMap<>();  // workerId to total slots
    private final Map<String, Integer> used = new HashMap<>();  // workerId to slots in use
    private final Map<String, Map<String, Integer>> jobHoldings = new HashMap<>();  // jobId to (workerId to slots held)

    /** Records or refreshes a worker's total capacity, which is its slot count, as of registration. */
    public void setWorkerCapacity(String workerId, int slots) {
        lock.lock();
        try {
            capacity.put(workerId, slots);
            used.putIfAbsent(workerId, 0);
            released.signalAll();  // new capacity may satisfy a waiter
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes, for one job, the slots it needs on each worker, as a gang across the whole job. Returns
     * true once the occupancy is atomic. If the slots are short the caller waits until the timeout
     * and gets false. When any worker is asked for more than its total capacity, fail fast with false
     * rather than waiting for a demand that can never be met.
     */
    public boolean acquire(String jobId, Map<String, Integer> perWorkerDemand, long timeoutMs)
            throws InterruptedException {
        lock.lock();
        try {
            // Fail fast: demand above total capacity can never be satisfied.
            for (Map.Entry<String, Integer> e : perWorkerDemand.entrySet()) {
                int cap = capacity.getOrDefault(e.getKey(), 0);
                if (e.getValue() > cap) {
                    return false;
                }
            }
            long deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
            while (!fits(perWorkerDemand)) {
                long remain = deadlineNanos - System.nanoTime();
                if (remain <= 0) {
                    return false;
                }
                released.awaitNanos(remain);
            }
            occupy(jobId, perWorkerDemand);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomic best-fit acquisition, used when a job will run at lower parallelism. Each entry of
     * {@code candidatesHighToLow} is the per-worker demand for one fallback tier, ordered from the
     * desired P down to 1. While holding the lock, the highest tier that currently fits on every
     * worker is occupied atomically and its index returned, where 0 is the highest tier. If no tier
     * fits, the caller waits and re-evaluates every tier on waking, instead of waiting for one fixed
     * demand; that is what removes the gap between choosing a P and acquiring for it. A timeout
     * returns -1, and so does fail-fast without waiting when even the lowest tier exceeds some
     * worker's capacity.
     *
     * <p>One deadline covers the whole attempt: the caller passes the budget left as {@code
     * timeoutMs}, so retries never stack. Re-picking the highest feasible tier on every waking is
     * itself the loop that drops one tier and retries, with no window where the choice and the
     * acquisition disagree.
     */
    public int acquireBestFit(String jobId, java.util.List<Map<String, Integer>> candidatesHighToLow, long timeoutMs)
            throws InterruptedException {
        lock.lock();
        try {
            // Fail fast: if even the lowest tier, the smallest demand in the list, exceeds some
            // worker's capacity, no tier can ever fit, so there is nothing to wait for.
            Map<String, Integer> lowest = candidatesHighToLow.get(candidatesHighToLow.size() - 1);
            for (Map.Entry<String, Integer> e : lowest.entrySet()) {
                if (e.getValue() > capacity.getOrDefault(e.getKey(), 0)) {
                    return -1;
                }
            }
            long deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
            while (true) {
                // Under the lock, take the highest tier that fits now; a lower index is a higher P.
                for (int i = 0; i < candidatesHighToLow.size(); i++) {
                    Map<String, Integer> demand = candidatesHighToLow.get(i);
                    if (fits(demand)) {
                        occupy(jobId, demand);
                        return i;
                    }
                }
                long remain = deadlineNanos - System.nanoTime();
                if (remain <= 0) {
                    return -1;
                }
                released.awaitNanos(remain);
            }
        } finally {
            lock.unlock();
        }
    }

    // The caller must hold the lock: occupy demand and record it in jobHoldings as one step.
    private void occupy(String jobId, Map<String, Integer> demand) {
        Map<String, Integer> holding = new HashMap<>();
        for (Map.Entry<String, Integer> e : demand.entrySet()) {
            used.merge(e.getKey(), e.getValue(), Integer::sum);
            holding.merge(e.getKey(), e.getValue(), Integer::sum);
        }
        jobHoldings.put(jobId, holding);
    }

    /** Returns everything a job holds once it reaches a terminal state. Idempotent if it held none. */
    public void release(String jobId) {
        lock.lock();
        try {
            Map<String, Integer> holding = jobHoldings.remove(jobId);
            if (holding == null) {
                return;
            }
            for (Map.Entry<String, Integer> e : holding.entrySet()) {
                used.merge(e.getKey(), -e.getValue(), Integer::sum);
            }
            released.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * A worker died: drop its capacity and its usage, and remove its share from the holdings of any
     * job that had slots here. That job is released separately by the master after failing fast.
     */
    public void releaseWorker(String workerId) {
        lock.lock();
        try {
            capacity.remove(workerId);
            used.remove(workerId);
            for (Map<String, Integer> holding : jobHoldings.values()) {
                holding.remove(workerId);
            }
            released.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reconciliation: the difference between the free slots a worker reports in its heartbeat and the free
     * slots derived here (ledger minus report). The ledger is authoritative — **no state is modified**, and
     * only the drift is returned, so the master can log a leak or drift signal. A reportedFree below 0
     * (unknown, from an idle heartbeat or an old frame) returns 0 and raises nothing.
     */
    public int reconcile(String workerId, long reportedFree) {
        if (reportedFree < 0) {
            return 0; // unknown, skip reconciliation
        }
        return freeSlots(workerId) - (int) reportedFree;
    }

    /** Observation: free slots on a worker, that is capacity minus what is currently used. */
    public int freeSlots(String workerId) {
        lock.lock();
        try {
            return capacity.getOrDefault(workerId, 0) - used.getOrDefault(workerId, 0);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Diagnostic snapshot taken when admission refuses a job: the free slots and total capacity of
     * each worker, so the master can report demand against free against capacity and a reader can
     * tell a genuinely short cluster apart from a demand computed wrong. Read-only; it changes no
     * state.
     */
    public java.util.Map<String, int[]> snapshot() {
        lock.lock();
        try {
            java.util.Map<String, int[]> snap = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Integer> e : capacity.entrySet()) {
                int cap = e.getValue();
                int free = cap - used.getOrDefault(e.getKey(), 0);
                snap.put(e.getKey(), new int[]{free, cap});
            }
            return snap;
        } finally {
            lock.unlock();
        }
    }

    // The caller must hold the lock: does every worker have enough free slots for its demand.
    private boolean fits(Map<String, Integer> perWorkerDemand) {
        for (Map.Entry<String, Integer> e : perWorkerDemand.entrySet()) {
            int free = capacity.getOrDefault(e.getKey(), 0) - used.getOrDefault(e.getKey(), 0);
            if (free < e.getValue()) {
                return false;
            }
        }
        return true;
    }
}
