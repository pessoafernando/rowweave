package io.rowweave.stream.contract;

import io.rowweave.artifact.PlanArtifact;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Exact projection between PortableStreamJob v1 and PlanArtifact. */
public final class PortableStreamJobArtifact {
    private PortableStreamJobArtifact() { }

    public static PlanArtifact wrap(String definitionId, PortableStreamJob job) {
        if (job == null) throw new IllegalArgumentException("job is required");
        PortableStreamPlan plan = job.plan();
        List<String> features = new ArrayList<>(
                PortableStreamPlanArtifact.requiredFeatures(plan));
        features.remove(PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1);
        features.add(PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1);
        PlanArtifact artifact = new PlanArtifact(
                definitionId, PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.UNBOUNDED,
                PortableStreamPlanArtifact.changelog(plan),
                PortableStreamPlanArtifact.timeMode(plan),
                features, Collections.<String>emptyList(),
                PortableStreamPlanArtifact.descriptors(plan),
                PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                PortableStreamJobCodec.encode(job));
        decodeValidated(artifact);
        return artifact;
    }

    public static PortableStreamJob decodeValidated(PlanArtifact artifact) {
        if (artifact == null
                || artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1
                || artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.STREAM
                || artifact.boundedness() != PlanArtifact.Boundedness.UNBOUNDED) {
            throw StreamContractSupport.failure("STREAM_SEMANTIC_MISMATCH",
                    "artifact.semantics", "artifact is not a Stream product job");
        }
        PortableStreamJob job = PortableStreamJobCodec.decode(artifact.payload());
        PortableStreamPlan plan = job.plan();
        if (artifact.changelogMode() != PortableStreamPlanArtifact.changelog(plan)
                || artifact.timeMode() != PortableStreamPlanArtifact.timeMode(plan)) {
            throw StreamContractSupport.failure("STREAM_SEMANTIC_MISMATCH",
                    "artifact.semantics", "outer Stream semantics drift from job");
        }
        if (!artifact.operators().equals(
                PortableStreamPlanArtifact.descriptors(plan))) {
            throw StreamContractSupport.failure("STREAM_DESCRIPTOR_MISMATCH",
                    "artifact.operators", "outer descriptors drift from Stream job");
        }
        List<String> expected = new ArrayList<>(
                PortableStreamPlanArtifact.requiredFeatures(plan));
        expected.remove(PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1);
        expected.add(PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1);
        PortableStreamPlanArtifact.requireFeatureProjection(
                artifact, expected);
        return job;
    }
}
