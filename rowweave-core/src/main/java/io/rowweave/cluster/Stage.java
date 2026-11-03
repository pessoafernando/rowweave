package io.rowweave.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One stage of the execution graph: a linear pure-push sub-chain of operators, bounded where a
 * breaker head, a BINARY node or a SINK forces a cut.<ul><li>{@link #members()} holds the
 * original plan graph node indices in this stage, ordered as they execute, source side first
 * and sink side last.</li><li>{@link #inputStages()} holds the upstream stage indices, one per
 * placeholder source of this stage, and is empty when the stage is headed by its own
 * source.</li><li>{@link #isSink()} says the chain ends in a SINK, which is a sink written by
 * the worker.</li></ul><p>A stage only describes: it holds no rows and executes nothing, since
 * a worker rebuilds the operator chain from the members and runs it. Immutable. This is a
 * linear one-to-one slice without parallelism or partitioning, as a keyed shuffle from N to M
 * is assembled by routing.
 */
public final class Stage {

    private final int index;
    private final List<Integer> members;
    private final List<Integer> inputStages;
    private final boolean isSink;
    private final String headTypeId;  // the chain head, that is members.get(0), decides whether this stage is a keyed shuffle
    // How many splits the source of a source-headed stage holds, and zero otherwise. StageAssignment
    // derives the number of source instances from it, the minimum of parallelism and splits, so that
    // assignment stays a pure function and is never handed the plan graph itself.
    private final int headSourceInputCount;
    // Marks this stage as the shared fork of a fan-out re-join, whose output edges are BLOCKING: the
    // worker materializes the output of this stage and replays each subpartition independently and
    // concurrently, which breaks the deadlock ring of a fan-out feeding an operator that consumes in
    // order, while the shared subgraph is still computed once.
    private final boolean blockingFanout;

    public Stage(int index, List<Integer> members, List<Integer> inputStages, boolean isSink) {
        this(index, members, inputStages, isSink, null);
    }

    public Stage(int index, List<Integer> members, List<Integer> inputStages, boolean isSink, String headTypeId) {
        this(index, members, inputStages, isSink, headTypeId, 0);
    }

    public Stage(int index, List<Integer> members, List<Integer> inputStages, boolean isSink,
                 String headTypeId, int headSourceInputCount) {
        this(index, members, inputStages, isSink, headTypeId, headSourceInputCount, false);
    }

    public Stage(int index, List<Integer> members, List<Integer> inputStages, boolean isSink,
                 String headTypeId, int headSourceInputCount, boolean blockingFanout) {
        this.index = index;
        this.members = Collections.unmodifiableList(new ArrayList<>(members));
        this.inputStages = Collections.unmodifiableList(new ArrayList<>(inputStages));
        this.isSink = isSink;
        this.headTypeId = headTypeId;
        this.headSourceInputCount = headSourceInputCount;
        this.blockingFanout = blockingFanout;
    }

    /** Whether this stage is a BLOCKING fan-out fork, whose edges materialize and replay concurrently to break the deadlock ring. */
    public boolean blockingFanout() {
        return blockingFanout;
    }

    /** Splits held by the source of a source-headed stage, and zero otherwise; sizes the source instances. */
    public int headSourceInputCount() {
        return headSourceInputCount;
    }

    /** Type id of the operator at the source-side head of the chain, which decides the instance count: a keyed breaker takes P, anything else one. */
    public String headTypeId() {
        return headTypeId;
    }

    public int index() {
        return index;
    }

    /** Original plan graph node indices in this stage, ordered as they execute, source side first. */
    public List<Integer> members() {
        return members;
    }

    /** Upstream stage indices, one per placeholder source; empty means the stage is headed by its own source. */
    public List<Integer> inputStages() {
        return inputStages;
    }

    public boolean isSink() {
        return isSink;
    }

    @Override
    public String toString() {
        return "Stage#" + index + members + (isSink ? " SINK" : "") + " in=" + inputStages;
    }
}
