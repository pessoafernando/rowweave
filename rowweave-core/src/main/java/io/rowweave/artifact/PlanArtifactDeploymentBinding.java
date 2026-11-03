package io.rowweave.artifact;

import java.util.ArrayList;
import java.util.List;

/** Exact deployment-set feature binding used before any Artifact object materialization. */
public final class PlanArtifactDeploymentBinding {
    private PlanArtifactDeploymentBinding() { }

    public static PlanArtifact bind(PlanArtifact artifact, String deploymentSetId) {
        PlanArtifactValidator.validateReadable(artifact);
        requireSetId(deploymentSetId);
        String existing = deploymentSetId(artifact);
        if (existing != null) {
            if (!existing.equals(deploymentSetId)) mismatch(existing, deploymentSetId);
            return artifact;
        }
        List<String> features = new ArrayList<>(artifact.requiredFeatures());
        features.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX + deploymentSetId);
        return new PlanArtifact(
                artifact.definitionId(), artifact.planSchemaVersion(), artifact.minimumReaderVersion(),
                artifact.runtimeTarget(), artifact.boundedness(), artifact.changelogMode(),
                artifact.timeMode(), features, artifact.optionalFeatures(), artifact.operators(),
                artifact.payloadFormat(), artifact.payload());
    }

    /** Marks a Stream graph for signed connector admission and placement. */
    public static PlanArtifact bindStreamConnector(
            PlanArtifact artifact, String deploymentSetId) {
        if (artifact == null
                || artifact.runtimeTarget() != PlanArtifact.RuntimeTarget.STREAM
                || (artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1
                && artifact.payloadFormat()
                != PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1)) {
            throw new IllegalArgumentException(
                    "portable Stream artifact is required");
        }
        PlanArtifact bound = bind(artifact, deploymentSetId);
        if (bound.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)) {
            return bound;
        }
        List<String> features = new ArrayList<>(bound.requiredFeatures());
        features.add(PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
        PlanArtifact connectorBound = new PlanArtifact(
                bound.definitionId(), bound.planSchemaVersion(),
                bound.minimumReaderVersion(), bound.runtimeTarget(),
                bound.boundedness(), bound.changelogMode(),
                bound.timeMode(), features, bound.optionalFeatures(),
                bound.operators(), bound.payloadFormat(), bound.payload());
        PlanArtifactValidator.validateReadable(connectorBound);
        return connectorBound;
    }

    /** MTLS sets required=true; LOOPBACK permits absence but never ignores an explicit mismatch. */
    public static void requireMatches(
            PlanArtifact artifact, String deploymentSetId, boolean required) {
        PlanArtifactValidator.validateReadable(artifact);
        requireSetId(deploymentSetId);
        String actual = deploymentSetId(artifact);
        if (actual == null) {
            if (required) {
                throw ArtifactText.failure("DEPLOYMENT_FEATURE_REQUIRED",
                        "plan.requiredFeatures", "secure cluster artifact requires deployment identity");
            }
            return;
        }
        if (!actual.equals(deploymentSetId)) mismatch(actual, deploymentSetId);
    }

    public static String deploymentSetId(PlanArtifact artifact) {
        String found = null;
        for (String feature : artifact.requiredFeatures()) {
            if (!feature.startsWith(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX)) continue;
            String candidate = feature.substring(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX.length());
            if (!isSetId(candidate) || found != null) {
                throw ArtifactText.failure("INVALID_DEPLOYMENT_FEATURE",
                        "plan.requiredFeatures", "deployment feature is not canonical and unique");
            }
            found = candidate;
        }
        return found;
    }

    static boolean isSetId(String value) {
        if (value == null || value.length() != 64) return false;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (!((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    private static void requireSetId(String value) {
        if (!isSetId(value)) {
            throw new IllegalArgumentException("deploymentSetId must be exactly 64 lowercase hex");
        }
    }

    private static void mismatch(String actual, String expected) {
        throw ArtifactText.failure("DEPLOYMENT_SET_MISMATCH", "plan.requiredFeatures",
                "artifact deployment set " + actual + " does not match runtime " + expected);
    }
}
