package io.rowweave.cluster;

import io.rowweave.plan.PlanGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A plan graph extracted for one stage, executable on its own, together with the bindings of
 * its boundaries.<p>{@link #plan()} rebuilds the members of the stage as a graph in its own
 * right, replacing a child reference that crosses the stage boundary with a placeholder source,
 * an empty source; at execution the upstream stage's output is fed in through a shuffle
 * channel, as named by {@link #boundaries()}. When {@link #isSink()} is true the root of the
 * plan is a SINK holding a sink that execution writes, so no rows are returned. Immutable.
 */
public final class StagePlan {

    private final int stageIndex;
    private final PlanGraph plan;
    private final List<StageBoundary> boundaries;
    private final boolean isSink;

    public StagePlan(int stageIndex, PlanGraph plan, List<StageBoundary> boundaries, boolean isSink) {
        this.stageIndex = stageIndex;
        this.plan = plan;
        this.boundaries = Collections.unmodifiableList(new ArrayList<>(boundaries));
        this.isSink = isSink;
    }

    public int stageIndex() {
        return stageIndex;
    }

    public PlanGraph plan() {
        return plan;
    }

    /** Boundary bindings, each pairing one placeholder source with one upstream stage; empty means the stage is headed by its own source. */
    public List<StageBoundary> boundaries() {
        return boundaries;
    }

    public boolean isSink() {
        return isSink;
    }

    @Override
    public String toString() {
        return "StagePlan#" + stageIndex + (isSink ? "[SINK]" : "") + " boundaries=" + boundaries;
    }
}
