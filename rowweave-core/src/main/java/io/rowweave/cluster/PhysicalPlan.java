package io.rowweave.cluster;

import java.util.Collections;
import java.util.List;

/**
 * The physical execution plan, which expands a logical {@link ExecutionGraph}, a stage DAG
 * without parallelism, partitions or endpoints, into stage instances, instance to worker
 * assignments and the endpoints of shuffle edges. It is a pure value object that neither
 * executes nor opens a network, is produced by the pure function {@link StageAssignment}, and
 * is what a MasterServer uses to orchestrate RPCs layer by layer with barriers.<p>One logical
 * shuffle stage becomes P {@link Instance} objects, where P is the parallelism, while a sink or
 * linear stage becomes one. Along a shuffle edge every instance of the upstream stage, the
 * router, connects by partition to all P instances of the downstream stage, the consumers.
 */
public final class PhysicalPlan {

    /** One stage instance: the logical stage index, the instance index from 0 to P-1, and the assigned worker id. */
    public static final class Instance {
        private final int logicalStage;
        private final int instanceIndex;
        private final String workerId;
        private final boolean listening;  // whether this instance is listened to: an upstream connects into it, so it binds and reports a port

        public Instance(int logicalStage, int instanceIndex, String workerId, boolean listening) {
            this.logicalStage = logicalStage;
            this.instanceIndex = instanceIndex;
            this.workerId = workerId;
            this.listening = listening;
        }

        public int logicalStage() { return logicalStage; }
        public int instanceIndex() { return instanceIndex; }
        public String workerId() { return workerId; }
        public boolean listening() { return listening; }

        @Override
        public String toString() {
            return "Inst[stage=" + logicalStage + ",i=" + instanceIndex + ",w=" + workerId
                    + (listening ? ",listen" : "") + "]";
        }
    }

    private final int parallelism;
    private final List<Instance> instances;  // topological order, upstream first and the sink last
    private final List<int[]> shuffleEdges;  // pairs of {producer logical stage, consumer logical stage}
    private final boolean wholeGraph;  // no shuffle edges means the WHOLE_GRAPH fast path on a single worker

    PhysicalPlan(int parallelism, List<Instance> instances, List<int[]> shuffleEdges, boolean wholeGraph) {
        this.parallelism = parallelism;
        this.instances = Collections.unmodifiableList(instances);
        this.shuffleEdges = Collections.unmodifiableList(shuffleEdges);
        this.wholeGraph = wholeGraph;
    }

    public int parallelism() { return parallelism; }
    public List<Instance> instances() { return instances; }
    public List<int[]> shuffleEdges() { return shuffleEdges; }

    /** With no shuffle edge the graph is linear, so the WHOLE_GRAPH fast path runs it on a single worker. */
    public boolean isWholeGraph() { return wholeGraph; }

    /** All instances of one logical stage, ordered by instance index. */
    public List<Instance> instancesOf(int logicalStage) {
        List<Instance> out = new java.util.ArrayList<>();
        for (Instance ins : instances) {
            if (ins.logicalStage() == logicalStage) {
                out.add(ins);
            }
        }
        return out;
    }
}
