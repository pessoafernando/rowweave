package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlanArtifactModelTest {

    @Test
    void modelIsImmutableAndCanonicalizesFeatureOrder() {
        byte[] payload = new byte[]{1, 2, 3};
        List<String> required = new ArrayList<>(Arrays.asList("z.feature", "a.feature"));
        List<OperatorDescriptor> operators = new ArrayList<>(Collections.singletonList(source("source-1")));

        PlanArtifact artifact = artifact("definition-1", required, Collections.singletonList("x.optional"),
                operators, payload);
        payload[0] = 9;
        required.clear();
        operators.clear();

        assertArrayEquals(new byte[]{1, 2, 3}, artifact.payload());
        byte[] returned = artifact.payload();
        returned[1] = 8;
        assertArrayEquals(new byte[]{1, 2, 3}, artifact.payload());
        assertEquals(Arrays.asList("a.feature", "z.feature"), artifact.requiredFeatures());
        assertEquals(1, artifact.operators().size());
        assertThrows(UnsupportedOperationException.class,
                () -> artifact.requiredFeatures().add("mutate"));
        assertThrows(UnsupportedOperationException.class,
                () -> artifact.operators().add(source("source-2")));
    }

    @Test
    void rejectsDuplicateOrAmbiguousIdentityAndFeatureText() {
        PlanArtifactValidationException duplicateOperator = assertThrows(
                PlanArtifactValidationException.class,
                () -> artifact("definition-1", requiredFeatures(), Collections.emptyList(),
                        Arrays.asList(source("same"), source("same")), new byte[0]));
        assertEquals("DUPLICATE_OPERATOR_ID", duplicateOperator.code());
        assertEquals("operators[1].operatorId", duplicateOperator.path());

        PlanArtifactValidationException duplicateFeature = assertThrows(
                PlanArtifactValidationException.class,
                () -> artifact("definition-1", Arrays.asList("same", "same"), Collections.emptyList(),
                        Collections.singletonList(source("source-1")), new byte[0]));
        assertEquals("DUPLICATE_FEATURE", duplicateFeature.code());

        PlanArtifactValidationException overlap = assertThrows(
                PlanArtifactValidationException.class,
                () -> artifact("definition-1", Collections.singletonList("same"),
                        Collections.singletonList("same"),
                        Collections.singletonList(source("source-1")), new byte[0]));
        assertEquals("DUPLICATE_FEATURE", overlap.code());

        String decomposed = Normalizer.normalize("é", Normalizer.Form.NFD);
        PlanArtifactValidationException nonCanonical = assertThrows(
                PlanArtifactValidationException.class,
                () -> artifact(decomposed, requiredFeatures(), Collections.emptyList(),
                        Collections.singletonList(source("source-1")), new byte[0]));
        assertEquals("NON_CANONICAL_TEXT", nonCanonical.code());
        assertEquals("plan.definitionId", nonCanonical.path());

        PlanArtifactValidationException nul = assertThrows(
                PlanArtifactValidationException.class,
                () -> artifact("bad\0id", requiredFeatures(), Collections.emptyList(),
                        Collections.singletonList(source("source-1")), new byte[0]));
        assertEquals("NON_CANONICAL_TEXT", nul.code());

        char[] oversized = new char[PlanArtifactLimits.MAX_TEXT_BYTES + 1];
        Arrays.fill(oversized, 'a');
        PlanArtifactValidationException limit = assertThrows(
                PlanArtifactValidationException.class,
                () -> artifact(new String(oversized), requiredFeatures(), Collections.emptyList(),
                        Collections.singletonList(source("source-1")), new byte[0]));
        assertEquals("LIMIT_EXCEEDED", limit.code());
    }

    @Test
    void rejectsUnknownOrNonTopologicalOperatorInputs() {
        OperatorDescriptor unknown = new OperatorDescriptor(
                "filter-1", OperatorDescriptor.Kind.UNARY, "filter", fingerprint(),
                Collections.singletonList("missing"));
        PlanArtifactValidationException missing = assertThrows(PlanArtifactValidationException.class,
                () -> artifact("definition-1", requiredFeatures(), Collections.emptyList(),
                        Arrays.asList(source("source-1"), unknown), new byte[0]));
        assertEquals("UNKNOWN_OPERATOR_INPUT", missing.code());

        OperatorDescriptor forward = new OperatorDescriptor(
                "filter-1", OperatorDescriptor.Kind.UNARY, "filter", fingerprint(),
                Collections.singletonList("source-1"));
        PlanArtifactValidationException topology = assertThrows(PlanArtifactValidationException.class,
                () -> artifact("definition-1", requiredFeatures(), Collections.emptyList(),
                        Arrays.asList(forward, source("source-1")), new byte[0]));
        assertEquals("NON_TOPOLOGICAL_OPERATOR_INPUT", topology.code());
    }

    static PlanArtifact artifact(
            String definitionId, List<String> required, List<String> optional,
            List<OperatorDescriptor> operators, byte[] payload) {
        return new PlanArtifact(
                definitionId,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                required,
                optional,
                operators,
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                payload);
    }

    static List<String> requiredFeatures() {
        return Arrays.asList(
                PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
                PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS);
    }

    static OperatorDescriptor source(String id) {
        return new OperatorDescriptor(id, OperatorDescriptor.Kind.SOURCE, "source", fingerprint(),
                Collections.emptyList());
    }

    static String fingerprint() {
        return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    }
}
