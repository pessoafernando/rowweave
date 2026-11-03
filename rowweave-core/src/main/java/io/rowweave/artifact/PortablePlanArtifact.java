package io.rowweave.artifact;

import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableBatchPlanCodec;
import io.rowweave.portable.PortableBindingVerifier;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortableRuntimeResolver;
import io.rowweave.plan.PlanGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongConsumer;

/** Bridges the public portable IR and the existing checksummed Artifact envelope. */
public final class PortablePlanArtifact {
    private static final String TYPE_PREFIX = "rowweave.portable.opcode.";

    private PortablePlanArtifact() { }

    public static PlanArtifact wrap(String definitionId, PortableBatchPlan plan) {
        return wrap(definitionId, plan, ignored -> { });
    }

    public static PlanArtifact wrap(
            String definitionId, PortableBatchPlan plan,
            LongConsumer reservation) {
        if (plan == null || reservation == null) {
            throw new IllegalArgumentException("portable plan and reservation are required");
        }
        List<String> features = new ArrayList<>();
        features.add(PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1);
        features.add(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS);
        Set<String> deploymentSets = new HashSet<>();
        for (PortablePlanNode node : plan.nodes()) {
            if (node.binding() instanceof FunctionDeploymentReference) {
                deploymentSets.add(((FunctionDeploymentReference) node.binding())
                        .deploymentSetId());
            }
        }
        if (deploymentSets.size() > 1) {
            throw ArtifactText.failure("MULTIPLE_DEPLOYMENT_SETS",
                    "payload.functions",
                    "one portable plan must bind one exact deployment set");
        }
        if (!deploymentSets.isEmpty()) {
            features.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                    + deploymentSets.iterator().next());
        }
        return new PlanArtifact(
                definitionId,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                features,
                Collections.emptyList(),
                descriptors(plan),
                PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1,
                PortableBatchPlanCodec.encode(plan, reservation));
    }

    public static PortableBatchPlan decodeValidated(PlanArtifact artifact) {
        return decodeValidated(artifact, ignored -> { });
    }

    public static PortableBatchPlan decodeValidated(
            PlanArtifact artifact, LongConsumer reservation) {
        PlanArtifactValidator.validateReadableMetadata(artifact);
        return decodeAndValidateProjection(artifact, reservation);
    }

    /** Public-v1 Client/Master preflight; no runtime binding is resolved. */
    public static PortableBatchPlan preflightValidated(
            PlanArtifact artifact, PortableBindingVerifier verifier,
            LongConsumer reservation) {
        PlanArtifactValidator.validateForPublicV1Execution(artifact);
        requireDeploymentMatch(artifact, verifier);
        PortableBatchPlan plan = decodeAndValidateProjection(
                artifact, reservation);
        PortablePlanCompiler.preflight(plan, verifier);
        return plan;
    }

    /** Durable public-v1 preflight with exact replay and Sink profile checks. */
    public static PortableBatchPlan preflightDurableValidated(
            PlanArtifact artifact, PortableBindingVerifier verifier,
            LongConsumer reservation) {
        PlanArtifactValidator.validateForPublicV1Execution(artifact);
        requireDeploymentMatch(artifact, verifier);
        PortableBatchPlan plan = decodeAndValidateProjection(
                artifact, reservation);
        PortablePlanCompiler.preflightDurable(plan, verifier);
        return plan;
    }

    /** Public-v1 Worker materialization path; legacy Java payloads never enter it. */
    public static PlanGraph compileValidated(
            PlanArtifact artifact, PortableBindingVerifier verifier,
            PortableRuntimeResolver resolver, LongConsumer reservation) {
        PlanArtifactValidator.validateForPublicV1Execution(artifact);
        requireDeploymentMatch(artifact, verifier);
        PortableBatchPlan plan = decodeAndValidateProjection(
                artifact, reservation);
        return PortablePlanCompiler.compile(plan, verifier, resolver);
    }

    private static void requireDeploymentMatch(
            PlanArtifact artifact, PortableBindingVerifier verifier) {
        String deploymentSetId = verifier == null
                ? null : verifier.deploymentSetId();
        if (deploymentSetId == null) {
            throw ArtifactText.failure("DEPLOYMENT_RUNTIME_REQUIRED",
                    "runtime.deploymentSetId",
                    "public portable materialization requires an exact deployment set");
        }
        PlanArtifactDeploymentBinding.requireMatches(
                artifact, deploymentSetId, true);
    }

    static PortableBatchPlan decodeAndValidateProjection(
            PlanArtifact artifact, LongConsumer reservation) {
        if (artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            throw ArtifactText.failure("PORTABLE_PAYLOAD_REQUIRED", "payload.format",
                    "artifact does not contain a portable batch plan");
        }
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        if (artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.BATCH
                || artifact.boundedness() != PlanArtifact.Boundedness.BOUNDED
                || artifact.changelogMode() != PlanArtifact.ChangelogMode.APPEND
                || artifact.timeMode() != PlanArtifact.TimeMode.NONE) {
            throw ArtifactText.failure("PORTABLE_SEMANTIC_MISMATCH",
                    "plan.semantics",
                    "portable batch payload requires bounded append-only batch semantics");
        }
        reservation.accept(artifact.payloadSize());
        PortableBatchPlan plan = PortableBatchPlanCodec.decode(
                artifact.payload(), reservation);
        if (!descriptors(plan).equals(artifact.operators())) {
            throw ArtifactText.failure("PORTABLE_DESCRIPTOR_MISMATCH",
                    "plan.operators",
                    "portable payload differs from its Artifact projection");
        }
        return plan;
    }

    static List<OperatorDescriptor> descriptors(PortableBatchPlan plan) {
        List<OperatorDescriptor> descriptors = new ArrayList<>(plan.nodes().size());
        for (PortablePlanNode node : plan.nodes()) {
            descriptors.add(new OperatorDescriptor(
                    node.operatorId(), kind(node.opcode()), typeId(node.opcode()),
                    node.outputSchema().fingerprint(), node.inputOperatorIds()));
        }
        return descriptors;
    }

    private static OperatorDescriptor.Kind kind(PortableOperatorCode opcode) {
        if (opcode == PortableOperatorCode.SOURCE) {
            return OperatorDescriptor.Kind.SOURCE;
        }
        if (opcode == PortableOperatorCode.SINK) {
            return OperatorDescriptor.Kind.SINK;
        }
        return opcode.inputCount() == 2
                ? OperatorDescriptor.Kind.BINARY
                : OperatorDescriptor.Kind.UNARY;
    }

    private static String typeId(PortableOperatorCode opcode) {
        return TYPE_PREFIX + opcode.wireId();
    }
}
