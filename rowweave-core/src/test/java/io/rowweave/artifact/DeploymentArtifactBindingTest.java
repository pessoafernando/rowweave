package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.rowweave.artifact.PlanArtifactModelTest.artifact;
import static io.rowweave.artifact.PlanArtifactModelTest.requiredFeatures;
import static io.rowweave.artifact.PlanArtifactModelTest.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** deployment identity is checksum-bound Artifact metadata. */
class DeploymentArtifactBindingTest {
    private static final String SET_A = repeat('a', 64);
    private static final String SET_B = repeat('b', 64);

    @Test
    void bindingAddsCanonicalRequiredFeatureAndChangesArtifactIdentity() {
        PlanArtifact original = fixture();

        PlanArtifact bound = PlanArtifactDeploymentBinding.bind(original, SET_A);
        PlanArtifact decoded = PlanArtifactCodec.decode(PlanArtifactCodec.encode(bound));

        assertTrue(decoded.requiredFeatures().contains(
                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX + SET_A));
        assertNotEquals(original.artifactId(), decoded.artifactId());
        assertEquals(bound.artifactId(), decoded.artifactId());
        PlanArtifactDeploymentBinding.requireMatches(decoded, SET_A, true);
    }

    @Test
    void secureRequiresExactSetWhileLoopbackAllowsOnlyAbsentOrMatchingFeature() {
        PlanArtifact unbound = fixture();
        assertCode("DEPLOYMENT_FEATURE_REQUIRED",
                () -> PlanArtifactDeploymentBinding.requireMatches(unbound, SET_A, true));
        PlanArtifactDeploymentBinding.requireMatches(unbound, SET_A, false);

        PlanArtifact bound = PlanArtifactDeploymentBinding.bind(unbound, SET_A);
        assertCode("DEPLOYMENT_SET_MISMATCH",
                () -> PlanArtifactDeploymentBinding.requireMatches(bound, SET_B, true));
        assertCode("DEPLOYMENT_SET_MISMATCH",
                () -> PlanArtifactDeploymentBinding.requireMatches(bound, SET_B, false));
        assertCode("DEPLOYMENT_SET_MISMATCH",
                () -> PlanArtifactDeploymentBinding.bind(bound, SET_B));
    }

    @Test
    void malformedDeploymentFeatureIsNeverTreatedAsUnknownOptionalSyntax() {
        PlanArtifact malformed = artifact("definition-1",
                java.util.Arrays.asList(
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
                        PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS,
                        PlanArtifact.DEPLOYMENT_FEATURE_PREFIX + "ABC"),
                Collections.emptyList(), Collections.singletonList(source("source-1")), new byte[0]);

        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateForCurrentRuntime(malformed));
        assertEquals("INVALID_DEPLOYMENT_FEATURE", failure.code());
    }

    private static PlanArtifact fixture() {
        return artifact("definition-1", requiredFeatures(), Collections.emptyList(),
                Collections.singletonList(source("source-1")), new byte[]{1, 2, 3});
    }

    private static void assertCode(String code, org.junit.jupiter.api.function.Executable action) {
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class, action);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
