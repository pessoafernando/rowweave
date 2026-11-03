package io.rowweave.cluster;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.OutputDestinationValidator;
import io.rowweave.plan.PlanGraph;

/** Side-effect-free cluster capability validation used before a local runtime is started. */
public final class ClusterSubmissionPreflight {

    private ClusterSubmissionPreflight() {
    }

    public static void validate(PlanArtifact artifact) {
        PlanGraph graph = LegacyPlanArtifactAdapter.unwrapValidated(artifact);
        OutputDestinationValidator.validate(graph);
        ClusterOutputValidator.validate(graph);
        OutputCommitValidator.validate(graph);
        ClusterPlanCapabilityValidator.validate(graph);
    }
}
