package io.rowweave.artifact;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Layered validation of version and feature readability against the current Batch Runtime capabilities. */
public final class PlanArtifactValidator {

    private static final Set<String> KNOWN_REQUIRED = new HashSet<>(Arrays.asList(
            PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
            PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS,
            PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
            PlanArtifact.FEATURE_LOCAL_ATTACHED_PLAN_GRAPH,
            PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1,
            PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1,
            PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1,
            PlanArtifact.FEATURE_STREAM_DYNAMIC_SPLIT_DISCOVERY_V2,
            PlanArtifact.FEATURE_STREAM_RETRACT_CHANGELOG_V2,
            PlanArtifact.FEATURE_STREAM_UNALIGNED_CHECKPOINT_V2,
            PlanArtifact.FEATURE_RUNTIME_STREAM_V1,
            PlanArtifact.FEATURE_STREAM_WINDOW_FIELD_BINDING_V1,
            PlanArtifact.FEATURE_STREAM_SOURCE_TIME_POLICY_V1,
            PlanArtifact.FEATURE_STREAM_SOURCE_END_OF_INPUT_V1,
            PlanArtifact.FEATURE_STREAM_RESIDENT_CDC_V1,
            PlanArtifact.FEATURE_STREAM_RESIDENT_CDC_V2,
            PlanArtifact.FEATURE_STREAM_CALC_V1,
            PlanArtifact.FEATURE_STREAM_FLAT_MAP_V1,
            PlanArtifact.FEATURE_STREAM_UNION_V1,
            PlanArtifact.FEATURE_STREAM_EVENT_TIME_ASSIGNMENT_V1,
            PlanArtifact.FEATURE_STREAM_KEYED_PROCESS_V1,
            PlanArtifact.FEATURE_STREAM_KEYED_CO_PROCESS_V1,
            PlanArtifact.FEATURE_STREAM_BROADCAST_STATE_V1,
            PlanArtifact.FEATURE_STREAM_GENERIC_WINDOW_V1,
            PlanArtifact.FEATURE_STREAM_TAGGED_OUTPUT_V1,
            PlanArtifact.FEATURE_STREAM_ASYNC_WAIT_V1,
            PlanArtifact.FEATURE_STREAM_NODE_EXECUTION_CONTRACT_V1,
            PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1,
            PlanArtifact.FEATURE_SINK_PROVIDER_CONFIG_ENVELOPE,
            PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER));

    private PlanArtifactValidator() {
    }

    public static void validateReadable(PlanArtifact artifact) {
        validateReadableMetadata(artifact);
        if (artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            PortablePlanArtifact.decodeAndValidateProjection(
                    artifact, ignored -> { });
        } else if (artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1) {
            io.rowweave.stream.contract.PortableStreamPlanArtifact.decodeValidated(artifact);
        } else if (artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1) {
            io.rowweave.stream.contract.PortableStreamJobArtifact.decodeValidated(artifact);
        }
    }

    /**
     * Validates the bounded public envelope metadata without decoding the plan payload.
     * Intended for composition/catalog inspection; it does not establish runtime executability.
     */
    public static void validateReadableMetadata(PlanArtifact artifact) {
        if (artifact == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "plan", "artifact is required");
        }
        if (artifact.planSchemaVersion() != PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION) {
            throw ArtifactText.failure("UNSUPPORTED_PLAN_SCHEMA", "plan.planSchemaVersion",
                    "unsupported plan schema version: " + artifact.planSchemaVersion());
        }
        if (artifact.minimumReaderVersion() > PlanArtifact.CURRENT_READER_VERSION) {
            throw ArtifactText.failure("READER_TOO_OLD", "plan.minimumReaderVersion",
                    "artifact requires reader " + artifact.minimumReaderVersion());
        }
        int deploymentFeatures = 0;
        for (int i = 0; i < artifact.requiredFeatures().size(); i++) {
            String feature = artifact.requiredFeatures().get(i);
            if (feature.startsWith(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX)) {
                deploymentFeatures++;
                String setId = feature.substring(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX.length());
                if (!PlanArtifactDeploymentBinding.isSetId(setId) || deploymentFeatures > 1) {
                    throw ArtifactText.failure("INVALID_DEPLOYMENT_FEATURE",
                            "plan.requiredFeatures[" + i + "]",
                            "deployment feature must contain exactly one canonical set id");
                }
            } else if (!KNOWN_REQUIRED.contains(feature)) {
                throw ArtifactText.failure("UNSUPPORTED_REQUIRED_FEATURE",
                        "plan.requiredFeatures[" + i + "]", "unsupported required feature: " + feature);
            }
        }
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)
                && (artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.STREAM
                || deploymentFeatures != 1)) {
            throw ArtifactText.failure("INVALID_DEPLOYMENT_FEATURE",
                    "plan.requiredFeatures",
                    "Stream connector deployment requires one bound set");
        }
        boolean hasIdentityFeature = artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS)
                || artifact.requiredFeatures().contains(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS);
        int payloadFeatureCount = 0;
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH)) {
            payloadFeatureCount++;
        }
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_LOCAL_ATTACHED_PLAN_GRAPH)) {
            payloadFeatureCount++;
        }
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1)) {
            payloadFeatureCount++;
        }
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1)) {
            payloadFeatureCount++;
        }
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1)) {
            payloadFeatureCount++;
        }
        boolean payloadFeatureMatches;
        switch (artifact.payloadFormat()) {
            case TRUSTED_LEGACY_PLAN_GRAPH:
                payloadFeatureMatches = artifact.requiredFeatures().contains(
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH);
                break;
            case LOCAL_ATTACHED_PLAN_GRAPH:
                payloadFeatureMatches = artifact.requiredFeatures().contains(
                        PlanArtifact.FEATURE_LOCAL_ATTACHED_PLAN_GRAPH);
                break;
            case PORTABLE_BATCH_PLAN_V1:
                payloadFeatureMatches = artifact.requiredFeatures().contains(
                        PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1);
                break;
            case PORTABLE_STREAM_PLAN_V1:
                payloadFeatureMatches = artifact.requiredFeatures().contains(
                        PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1);
                break;
            case PORTABLE_STREAM_JOB_V1:
                payloadFeatureMatches = artifact.requiredFeatures().contains(
                        PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1);
                break;
            default:
                payloadFeatureMatches = false;
        }
        if (payloadFeatureCount != 1 || !payloadFeatureMatches || !hasIdentityFeature) {
            throw ArtifactText.failure("PAYLOAD_FEATURE_MISMATCH", "payload.format",
                    "payload format requires its payload feature and an operator identity feature");
        }
        if (artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1
                || artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1) {
            validateStreamCarrierSemantics(artifact);
        }
    }

    /** Carrier-only closure; executable Stream plan validation is owned by 's Stream adapter. */
    private static void validateStreamCarrierSemantics(PlanArtifact artifact) {
        if (artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.STREAM
                || artifact.boundedness() != PlanArtifact.Boundedness.UNBOUNDED
                || artifact.timeMode() == PlanArtifact.TimeMode.NONE) {
            throw ArtifactText.failure("STREAM_SEMANTIC_MISMATCH", "plan.semantics",
                    "portable Stream carrier requires STREAM/UNBOUNDED, a supported changelog, and declared time semantics");
        }
        if (artifact.changelogMode() == PlanArtifact.ChangelogMode.RETRACT
                && !artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_RETRACT_CHANGELOG_V2)) {
            throw ArtifactText.failure("MISSING_STREAM_CAPABILITY",
                    "plan.requiredFeatures",
                    "RETRACT Stream carrier requires its runtime capability");
        }
        if (!artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_RUNTIME_STREAM_V1)) {
            throw ArtifactText.failure("MISSING_STREAM_CAPABILITY",
                    "plan.requiredFeatures",
                    "portable Stream plan requires the Stream runtime capability");
        }
    }

    public static void validateForCurrentRuntime(PlanArtifact artifact) {
        validateReadable(artifact);
        validateBatchSemantics(artifact);
        if (artifact.payloadFormat() != PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH) {
            throw ArtifactText.failure("UNSUPPORTED_RUNTIME", "payload.format",
                    "cluster runtime requires a wire-executable trusted legacy payload");
        }
    }

    /** Public production-v1 admission: Java object-graph payloads are never executable. */
    public static void validateForPublicV1Execution(PlanArtifact artifact) {
        validateReadable(artifact);
        validateBatchSemantics(artifact);
        if (artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            throw ArtifactText.failure("LEGACY_NOT_PUBLIC_V1", "payload.format",
                    "public v1 execution requires a portable batch plan");
        }
    }

    /** Metadata-only public Batch carrier admission; the portable payload is not decoded. */
    public static void validateForPublicV1Metadata(PlanArtifact artifact) {
        validateReadableMetadata(artifact);
        validateBatchSemantics(artifact);
        if (artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            throw ArtifactText.failure("LEGACY_NOT_PUBLIC_V1", "payload.format",
                    "public v1 composition requires a portable batch plan");
        }
    }

    /** product admission; legacy Stream plan carriers are never executable here. */
    public static void validateForStreamProductExecution(PlanArtifact artifact) {
        validateReadable(artifact);
        if (artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1) {
            throw ArtifactText.failure("STREAM_PRODUCT_JOB_REQUIRED",
                    "payload.format",
                    "Stream product execution requires a portable Stream job");
        }
    }

    /** Metadata-only Stream product carrier admission; the portable payload is not decoded. */
    public static void validateForStreamProductMetadata(PlanArtifact artifact) {
        validateReadableMetadata(artifact);
        if (artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1) {
            throw ArtifactText.failure("STREAM_PRODUCT_JOB_REQUIRED",
                    "payload.format",
                    "Stream product composition requires a portable Stream job");
        }
    }

    static void validateForLocalRuntime(PlanArtifact artifact) {
        validateReadable(artifact);
        validateBatchSemantics(artifact);
    }

    private static void validateBatchSemantics(PlanArtifact artifact) {
        if (artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.BATCH) {
            throw ArtifactText.failure("UNSUPPORTED_RUNTIME", "plan.runtimeTarget",
                    "current runtime only supports BATCH");
        }
        if (artifact.boundedness() != PlanArtifact.Boundedness.BOUNDED) {
            throw ArtifactText.failure("UNSUPPORTED_RUNTIME", "plan.boundedness",
                    "current runtime only supports BOUNDED input");
        }
        boolean incremental = artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER);
        if (incremental && artifact.changelogMode() != PlanArtifact.ChangelogMode.UPSERT) {
            throw ArtifactText.failure("INVALID_INCREMENTAL_PLAN", "plan.changelogMode",
                    "incremental feature requires UPSERT changelog");
        }
        if (!incremental && artifact.changelogMode() != PlanArtifact.ChangelogMode.APPEND) {
            throw ArtifactText.failure("INVALID_INCREMENTAL_PLAN", "plan.changelogMode",
                    "UPSERT requires the bounded incremental feature");
        }
        if (artifact.timeMode() != PlanArtifact.TimeMode.NONE) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "plan.timeMode",
                    "current runtime does not execute time semantics");
        }
    }
}
