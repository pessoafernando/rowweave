package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlanArtifact;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Production connector graph may bind one signed set without changing plan bytes. */
class StreamConnectorDeploymentArtifactTest {
    @Test
    void productAndLegacyStreamCarriersAcceptExactDeploymentBinding() {
        StreamSession session = session();
        PlanArtifact product = session.toPlanArtifact();
        PlanArtifact legacy = PortableStreamPlanArtifact.wrap(
                "connector-deployment-legacy",
                PortableStreamJobArtifact.decodeValidated(product).plan());
        String setId = sha256("signed-connector-set");

        for (PlanArtifact original : new PlanArtifact[] {product, legacy}) {
            PlanArtifact bound = PlanArtifactDeploymentBinding.bind(
                    original, setId);
            PlanArtifactValidator.validateReadable(bound);
            assertEquals(setId,
                    PlanArtifactDeploymentBinding.deploymentSetId(bound));
            assertNotEquals(original.artifactId(), bound.artifactId());
            assertArrayEquals(original.payload(), bound.payload());
        }
    }

    @Test
    void publicSessionEmitsBoundArtifactForNormalSubmission() {
        StreamSession session = session();
        String setId = sha256("signed-connector-set");
        session.setDeploymentSetId(setId);

        PlanArtifact artifact = session.toPlanArtifact();

        PlanArtifactValidator.validateReadable(artifact);
        assertEquals(setId,
                PlanArtifactDeploymentBinding.deploymentSetId(artifact));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1));
    }

    @Test
    void deploymentSetIdentityIsCanonicalAtThePublicBuilderBoundary() {
        StreamSession session = session();
        String setId = sha256("signed-connector-set");
        assertThrows(IllegalArgumentException.class,
                () -> session.setDeploymentSetId(null));
        assertThrows(IllegalArgumentException.class,
                () -> session.setDeploymentSetId(setId.toUpperCase()));
        assertThrows(IllegalArgumentException.class,
                () -> session.setDeploymentSetId(setId.substring(1)));
    }

    private static StreamSession session() {
        StreamSession session = RowWeave.createStreamSession(
                "connector-deployment");
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType,
                        false)));
        session.fromSource(StreamSourceSpec.exactOrdered(schema,
                "source-v1", sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "position-v1", new byte[] {2}))))
                .uid("source").sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3}));
        return session;
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
