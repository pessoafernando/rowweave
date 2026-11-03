package io.rowweave.cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * A pure function from a logical {@link ExecutionGraph}, a parallelism and a worker list to a
 * {@link PhysicalPlan}. It executes nothing, opens no network, and is deterministic: the same
 * input gives the same output, with round-robin placement rather than a greedy search. Being
 * free of RPC it is developed against its own tests.<p>Expansion rules:<ul><li>A logical stage
 * with upstream stages is where a shuffle converges, so it takes P instances, P being the
 * parallelism, and is marked listening.</li><li>A SINK stage takes one instance that converges
 * the input, and is marked listening because an upstream keyed operator connects into
 * it.</li><li>A stage that is purely a source, with no upstream and not a sink, takes P
 * instances each reading one source split, and is not listening because it connects
 * downstream.</li><li>A graph with no shuffle edge is one stage, so wholeGraph is set and the
 * single-worker fast path runs the whole graph.</li></ul><p>Instances are placed on workers
 * round-robin, which is deterministic. Non-sink instances keep one global rotation; sinks
 * rotate separately, so that where the sinks land is not fixed by the parity of the number of
 * stages placed before them.
 */
public final class StageAssignment {

    private StageAssignment() {
    }

    public static PhysicalPlan assign(ExecutionGraph eg, int parallelism, List<String> workerIds) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0");
        }
        if (workerIds.isEmpty()) {
            throw new IllegalArgumentException("no workers to assign");
        }

        List<int[]> shuffleEdges = new ArrayList<>();
        for (Stage s : eg.stages()) {
            for (int up : s.inputStages()) {
                shuffleEdges.add(new int[]{up, s.index()}); // producer up → consumer s
            }
        }

        // No shuffle edge means one linear stage, so take the WHOLE_GRAPH fast path.
        if (shuffleEdges.isEmpty()) {
            List<PhysicalPlan.Instance> single = new ArrayList<>();
            single.add(new PhysicalPlan.Instance(eg.rootStage().index(), 0, workerIds.get(0), false));
            return new PhysicalPlan(parallelism, single, shuffleEdges, true);
        }

        // Instance counts for every stage come out of one forward pass in topological order, upstream
        // first, so that a pass-through inherits its upstream's parallelism rather than collapsing to one,
        // and an upstream count is settled before anything reading it is expanded.
        int[] stageCount = new int[eg.stages().size()];
        for (Stage s : eg.stages()) {
            stageCount[s.index()] = instanceCountOf(s, parallelism, stageCount);
        }

        List<PhysicalPlan.Instance> instances = new ArrayList<>();
        int rr = 0;
        int sinkRr = -1;
        for (Stage s : eg.stages()) {
            boolean isSink = s.isSink();
            boolean hasUpstream = !s.inputStages().isEmpty();
            boolean listening = isSink || hasUpstream;  // an upstream connects into it, so it is listened to
            int count = stageCount[s.index()];
            for (int i = 0; i < count; i++) {
                if (isSink && sinkRr < 0) {
                    sinkRr = rr;
                }
                int workerOrdinal = isSink ? sinkRr++ : rr;
                instances.add(new PhysicalPlan.Instance(s.index(), i,
                        workerIds.get(workerOrdinal % workerIds.size()), listening));
                rr++;
            }
        }
        return new PhysicalPlan(parallelism, instances, shuffleEdges, false);
    }

    /**
     * How many instances one stage takes.<ul><li>A sink takes one, since it
     * converges.</li><li>A keyed shuffle head, that is the group by family, distinct, join,
     * co-group or partition by hash, takes P.</li><li>A pass-through unary that is not keyed,
     * such as map, flatMap, filter or select, inherits the instance count U of its only
     * upstream when U is greater than one, which removes the collapse to a single instance.
     * This is correct because the upstream fans out to the P instances of this stage under its
     * own partitioning rule, this stage holds no state and processes rows one at a time, and
     * the breaker at the end of the chain re-shuffles by key anyway.</li><li>Anything else
     * takes one: a source-headed stage with no upstream, a breaker over a total order that is
     * not keyed such as order by, top or whole-table aggregate, or a stage with several
     * upstreams.</li></ul>
     */
    static int instanceCountOf(Stage s, int parallelism, int[] stageCount) {
        if (s.isSink()) {
            return 1;
        }
        if (needsKeyedShuffle(s.headTypeId())) {
            return parallelism;
        }
        // A source-headed stage, with no upstream, runs as many instances as its source splits allow:
        // R is the minimum of parallelism and split count. One split gives one instance, so a plan reading a
        // single file behaves exactly as before. A source holds no state and only reads, so splitting the
        // reads leaves the resulting set unchanged.
        if (s.inputStages().isEmpty() && s.headSourceInputCount() > 0) {
            return io.rowweave.plan.InputSharding.shardCount(s.headSourceInputCount(), parallelism);
        }
        // Only a **stateless per-row pass-through** (a whitelist) inherits the upstream parallelism.
        // **Never** inherit just "because the op is not keyed": orderBy and top produce a total order, and
        // reduceGroup, combineGroup and combineReduceGroup consume the whole input, so these global breakers
        // would each compute the same result once per instance and duplicate rows — a top(150) run in
        // parallel twice yields 300.
        if (isCoPartitionable(s.headTypeId()) && s.inputStages().size() == 1) {
            int upCount = stageCount[s.inputStages().get(0)];
            if (upCount > 1) {
                return upCount;  // inherits the parallelism of its only upstream, which the keyed fan-out path already carries from M to N by re-shuffling
            }
        }
        return 1;
    }

    /**
     * Whether the head of this stage's chain is a stateless pass-through, which may inherit the
     * upstream parallelism and run per partition, since that changes nothing about the
     * resulting set. Only the map, flatMap, filter, select and addFields families qualify: row
     * at a time, no row sees another, no ordering, no aggregation. Excluded on purpose are
     * order by, top and topN over a total order, reduce group, combine group and combine reduce
     * group over the whole input, and every breaker, because parallelizing those computes the
     * wrong answer.
     */
    static boolean isCoPartitionable(String headTypeId) {
        if (headTypeId == null) {
            return false;
        }
        switch (headTypeId) {
            case "map": case "flatMap": case "mapPartition":
            case "filter": case "filterFn": case "filterParams":
            case "select": case "selectByIndex": case "addFields":
                return true;
            default:
                return false;
        }
    }

    /**
     * Whether the head operator of this stage's chain is a keyed shuffle, where equal keys land
     * in the same partition, so P instances may run in parallel: the group by family, distinct,
     * join and co-group qualify, each instance handling one partition on its own. Order by over
     * a total order, a global top N and a whole-table aggregate do not, since hashing would
     * scatter the input and there is no range partitioner to preserve order, so those are
     * forced to a single instance. A source, filter or select headed stage does not either, and
     * runs one source instance; parallel source splits come later.
     */
    static boolean needsKeyedShuffle(String headTypeId) {
        if (headTypeId == null) {
            return false;
        }
        switch (headTypeId) {
            case "groupBySum": case "groupByCount": case "groupByMax": case "groupByMin":
            case "groupBy": case "distinct": case "join": case "coGroup":
            case "partitionByHash":  // re-partitions by key, so P instances with a keyed shuffle; it is an identity pass-through rather than a breaker, and reuses the union mechanism for a non-breaker KEYED_OP
                return true;
            default:
                return false;
        }
    }
}
