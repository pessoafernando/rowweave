package io.rowweave.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A topologically ordered set of {@link Stage} objects, upstream first with the sink stage
 * last, cut from an {@link io.rowweave.plan.PlanGraph} along shuffle boundaries by {@link
 * ExecutionGraphBuilder}. The master deploys workers and wires the shuffle channels from it.
 * Immutable.
 */
public final class ExecutionGraph {

    private final List<Stage> stages;

    public ExecutionGraph(List<Stage> stages) {
        if (stages == null || stages.isEmpty()) {
            throw new IllegalArgumentException("execution graph must have at least one stage");
        }
        this.stages = Collections.unmodifiableList(new ArrayList<>(stages));
    }

    public List<Stage> stages() {
        return stages;
    }

    public int stageCount() {
        return stages.size();
    }

    public Stage stage(int index) {
        return stages.get(index);
    }

    /** The root stage is the last one, since topological order puts the sink stage at the end. */
    public Stage rootStage() {
        return stages.get(stages.size() - 1);
    }
}
