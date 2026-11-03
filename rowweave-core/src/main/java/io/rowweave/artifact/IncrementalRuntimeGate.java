package io.rowweave.artifact;

import io.rowweave.plan.PlanGraph;

/** Keeps bounded-incremental plans out of every generic batch execution path. */
public final class IncrementalRuntimeGate {
    private IncrementalRuntimeGate() {
    }

    public static void rejectGenericExecution(PlanGraph graph) {
        if (graph == null) return;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).hasIncrementalPlanDescriptor()) {
                throw ArtifactText.failure(
                        "INCREMENTAL_RUNTIME_LOCKED", "payload.incremental",
                        "bounded incremental plans require the frontier-aware submission path");
            }
        }
    }
}
