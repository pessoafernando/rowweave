package io.rowweave.stream.contract;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Exact projection between PortableStreamPlan v1 and the frozen PlanArtifact envelope. */
public final class PortableStreamPlanArtifact {
    private PortableStreamPlanArtifact() { }

    public static PlanArtifact wrap(String definitionId, PortableStreamPlan plan) {
        if (plan == null) throw new IllegalArgumentException("plan is required");
        List<String> requiredFeatures = requiredFeatures(plan);
        PlanArtifact artifact = new PlanArtifact(
                definitionId, PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.UNBOUNDED,
                changelog(plan),
                plan.timeMode() == PortableStreamPlan.TimeMode.EVENT_TIME
                        ? PlanArtifact.TimeMode.EVENT_TIME : PlanArtifact.TimeMode.PROCESSING_TIME,
                requiredFeatures,
                Collections.emptyList(), descriptors(plan),
                PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1,
                PortableStreamPlanCodec.encode(plan));
        decodeValidated(artifact);
        return artifact;
    }

    public static PortableStreamPlan decodeValidated(PlanArtifact artifact) {
        if (artifact == null
                || artifact.payloadFormat() != PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1
                || artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.STREAM
                || artifact.boundedness() != PlanArtifact.Boundedness.UNBOUNDED) {
            throw StreamContractSupport.failure("STREAM_SEMANTIC_MISMATCH",
                    "artifact.semantics", "artifact is not a Stream carrier");
        }
        PortableStreamPlan plan = PortableStreamPlanCodec.decode(artifact.payload());
        PlanArtifact.ChangelogMode expectedChangelog = changelog(plan);
        PlanArtifact.TimeMode expectedTime =
                plan.timeMode() == PortableStreamPlan.TimeMode.EVENT_TIME
                        ? PlanArtifact.TimeMode.EVENT_TIME : PlanArtifact.TimeMode.PROCESSING_TIME;
        if (artifact.changelogMode() != expectedChangelog
                || artifact.timeMode() != expectedTime) {
            throw StreamContractSupport.failure("STREAM_SEMANTIC_MISMATCH",
                    "artifact.semantics", "outer Stream semantics drift from payload");
        }
        if (!artifact.operators().equals(descriptors(plan))) {
            throw StreamContractSupport.failure("STREAM_DESCRIPTOR_MISMATCH",
                    "artifact.operators", "outer descriptors drift from Stream plan");
        }
        requireFeatureProjection(artifact, requiredFeatures(plan));
        return plan;
    }

    static void requireFeatureProjection(
            PlanArtifact artifact, List<String> expected) {
        java.util.Set<String> actual = new java.util.HashSet<>(
                artifact.requiredFeatures());
        java.util.Set<String> projected = new java.util.HashSet<>(expected);
        if (actual.equals(projected)) return;
        boolean planBindsSet = false;
        for (String feature : expected) {
            if (feature.startsWith(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX)) {
                planBindsSet = true;
                break;
            }
        }
        if (!planBindsSet) {
            String setId = PlanArtifactDeploymentBinding.deploymentSetId(
                    artifact);
            if (setId != null) {
                projected.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                        + setId);
                if (actual.equals(projected)) return;
            }
        }
        if (actual.contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)) {
            if (PlanArtifactDeploymentBinding.deploymentSetId(
                    artifact) != null) {
                projected.add(
                        PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
            }
        }
        if (actual.equals(projected)) return;
        throw StreamContractSupport.failure("STREAM_CAPABILITY_MISMATCH",
                "artifact.requiredFeatures",
                "outer required features drift from Stream plan bindings");
    }

    static List<String> requiredFeatures(PortableStreamPlan plan) {
        List<String> required = new ArrayList<>(Arrays.asList(
                PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1,
                PlanArtifact.FEATURE_RUNTIME_STREAM_V1));
        String deploymentSetId = null;
        for (PortableStreamNode node : plan.nodes()) {
            String current = functionDeploymentSetId(node);
            if (current != null) {
                if (deploymentSetId != null && !deploymentSetId.equals(current)) {
                    throw StreamContractSupport.failure("MULTIPLE_DEPLOYMENT_SETS",
                            "plan.nodes.function",
                            "Stream requires one deployment set");
                }
                deploymentSetId = current;
            }
        }
        if (deploymentSetId != null) {
            required.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX + deploymentSetId);
        }
        if (plan.requiredCapabilities().contains(
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY)) {
            required.add(PlanArtifact.FEATURE_STREAM_WINDOW_FIELD_BINDING_V1);
        }
        if (plan.requiredCapabilities().contains(
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY)) {
            required.add(PlanArtifact.FEATURE_STREAM_SOURCE_TIME_POLICY_V1);
        }
        if (plan.requiredCapabilities().contains(
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY)) {
            required.add(PlanArtifact.FEATURE_STREAM_SOURCE_END_OF_INPUT_V1);
        }
        if (plan.requiredCapabilities().contains(
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY)) {
            required.add(PlanArtifact.FEATURE_STREAM_RESIDENT_CDC_V1);
        }
        if (plan.requiredCapabilities().contains(
                PortableStreamPlan.RESIDENT_CDC_V2_CAPABILITY)) {
            required.add(PlanArtifact.FEATURE_STREAM_RESIDENT_CDC_V2);
        }
        projectCapability(plan, required, PortableStreamPlan.CALC_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_CALC_V1);
        projectCapability(plan, required, PortableStreamPlan.FLAT_MAP_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_FLAT_MAP_V1);
        projectCapability(plan, required, PortableStreamPlan.UNION_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_UNION_V1);
        projectCapability(plan, required,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_EVENT_TIME_ASSIGNMENT_V1);
        projectCapability(plan, required,
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_KEYED_PROCESS_V1);
        projectCapability(plan, required,
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_KEYED_CO_PROCESS_V1);
        projectCapability(plan, required,
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_BROADCAST_STATE_V1);
        projectCapability(plan, required,
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_GENERIC_WINDOW_V1);
        projectCapability(plan, required,
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_TAGGED_OUTPUT_V1);
        projectCapability(plan, required,
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_ASYNC_WAIT_V1);
        projectCapability(plan, required,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_NODE_EXECUTION_CONTRACT_V1);
        projectCapability(plan, required,
                PortableStreamPlan.DYNAMIC_SPLIT_DISCOVERY_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_DYNAMIC_SPLIT_DISCOVERY_V2);
        projectCapability(plan, required,
                PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_RETRACT_CHANGELOG_V2);
        projectCapability(plan, required,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY,
                PlanArtifact.FEATURE_STREAM_UNALIGNED_CHECKPOINT_V2);
        return required;
    }

    private static String functionDeploymentSetId(PortableStreamNode node) {
        if (node.binding() instanceof StreamFunctionBinding) {
            return ((StreamFunctionBinding) node.binding())
                    .deploymentReference().deploymentSetId();
        }
        if (node.binding() instanceof StreamProcessBinding) {
            return ((StreamProcessBinding) node.binding())
                    .deploymentReference().deploymentSetId();
        }
        if (node.binding() instanceof StreamWindowBinding) {
            return ((StreamWindowBinding) node.binding())
                    .deploymentReference().deploymentSetId();
        }
        if (node.binding() instanceof StreamAsyncBinding) {
            return ((StreamAsyncBinding) node.binding())
                    .deploymentReference().deploymentSetId();
        }
        return null;
    }

    private static void projectCapability(PortableStreamPlan plan,
            List<String> required, String nested, String outer) {
        if (plan.requiredCapabilities().contains(nested)) required.add(outer);
    }

    static List<OperatorDescriptor> descriptors(PortableStreamPlan plan) {
        List<OperatorDescriptor> result = new ArrayList<>(plan.nodes().size());
        for (PortableStreamNode node : plan.nodes()) {
            OperatorDescriptor.Kind kind;
            if (node.opcode() == StreamOperatorCode.SOURCE) kind = OperatorDescriptor.Kind.SOURCE;
            else if (node.opcode() == StreamOperatorCode.SINK) kind = OperatorDescriptor.Kind.SINK;
            else if (node.opcode().inputCount() == 2) kind = OperatorDescriptor.Kind.BINARY;
            else kind = OperatorDescriptor.Kind.UNARY;
            result.add(new OperatorDescriptor(node.operatorUid(), kind,
                    "rowweave.stream.opcode." + node.opcode().wireId(),
                    node.outputSchema().fingerprint(), node.inputOperatorUids()));
        }
        return result;
    }

    static PlanArtifact.ChangelogMode changelog(PortableStreamPlan plan) {
        switch (plan.changelogMode()) {
            case APPEND: return PlanArtifact.ChangelogMode.APPEND;
            case UPSERT: return PlanArtifact.ChangelogMode.UPSERT;
            case RETRACT: return PlanArtifact.ChangelogMode.RETRACT;
            default: throw new IllegalStateException(
                    "unknown Stream changelog mode");
        }
    }

    static PlanArtifact.TimeMode timeMode(PortableStreamPlan plan) {
        return plan.timeMode() == PortableStreamPlan.TimeMode.EVENT_TIME
                ? PlanArtifact.TimeMode.EVENT_TIME : PlanArtifact.TimeMode.PROCESSING_TIME;
    }
}
