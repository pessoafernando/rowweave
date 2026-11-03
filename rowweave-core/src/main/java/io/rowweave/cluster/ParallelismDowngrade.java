package io.rowweave.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parallelism downgrade solver (pure logic, stateless). When slots are short it steps down from the
 * desired P, trial-assigns each candidate P to get its **per-worker** slot demand, and takes the first P
 * that fits every worker's capacity as the effective P.
 *
 * <p><b>Why this is safe</b>: lowering P leaves the resulting row multiset independent of P, because the
 * cluster sink tail is a single canonicalizing instance. A downgrade therefore cannot change results, only
 * throughput.
 *
 * <p><b>Per-worker fit</b>: acquire checks worker by worker, since a round-robin distribution can have
 * enough slots in total while one specific worker falls short, so the solver must judge worker by worker
 * rather than only totals. {@link StageAssignment#assign} is a pure function, which makes trial-assign cheap.
 */
public final class ParallelismDowngrade {

    private ParallelismDowngrade() {
    }

    /**
     * Searches downward from the desired parallelism for the highest value that fits, where a value fits
     * when every worker's demand stays within that worker's capacity.
     * @return the highest fitting parallelism from 1 to desired, or 0 when even one does not fit, which
     *         the caller must treat as a real failure rather than downgrading forever.
     */
    public static int highestFeasible(ExecutionGraph eg, int desiredP, List<String> workerIds,
                                      Map<String, Integer> capacity) {
        String target = workerIds.isEmpty() ? null : workerIds.get(0);
        for (int p = desiredP; p >= 1; p--) {
            Map<String, Integer> demand = perWorkerDemand(eg, p, workerIds, target);
            if (fits(demand, capacity)) {
                return p;
            }
        }
        return 0;
    }

    /** Slot demand per worker for a candidate parallelism, that is the stage instances landing on each worker, where a whole-graph plan claims one. */
    public static Map<String, Integer> perWorkerDemand(ExecutionGraph eg, int p, List<String> workerIds,
                                                       String target) {
        PhysicalPlan plan = StageAssignment.assign(eg, p, workerIds);
        Map<String, Integer> demand = new HashMap<>();
        if (plan.isWholeGraph()) {
            demand.merge(target, 1, Integer::sum);
            return demand;
        }
        for (PhysicalPlan.Instance inst : plan.instances()) {
            demand.merge(inst.workerId(), 1, Integer::sum);
        }
        return demand;
    }

    /** Total slot demand for a candidate parallelism, summed over workers; used by observation and tests. */
    public static int totalDemand(ExecutionGraph eg, int p, List<String> workerIds, String target) {
        int total = 0;
        for (int v : perWorkerDemand(eg, p, workerIds, target).values()) {
            total += v;
        }
        return total;
    }

    /** Whether every worker's demand fits its capacity; a worker with no capacity record counts as zero. */
    private static boolean fits(Map<String, Integer> demand, Map<String, Integer> capacity) {
        for (Map.Entry<String, Integer> e : demand.entrySet()) {
            if (e.getValue() > capacity.getOrDefault(e.getKey(), 0)) {
                return false;
            }
        }
        return true;
    }
}
