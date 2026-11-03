package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins every refusal code that a public entry point of this module returns, so
 * the code name itself cannot be renamed, reordered or dropped without failing
 * a test. Each case is the one measured through the public API; the pair with
 * the published contract page is the table in
 * {@code docs/product/reference/pipeline-boundary.md} section 3.1.
 */
class PipelineRefusalContractTest {
    private static final String SCHEMA = repeat('c', 64);
    private static final String DIGEST = repeat('e', 64);
    private static final long EPOCH_MILLIS = 1_700_000_000_000L;

    @TempDir
    Path temporary;

    @Test
    void definitionPartsRefuseAMissingOrMalformedComponent() {
        assertFailure("INVALID_ARTIFACT",
                new Runnable() {
                    @Override public void run() { new PipelineStep("step", null); }
                });
        assertFailure("INVALID_STEP", new Runnable() {
            @Override public void run() { PipelineDefinition.builder("p", "r").addStep(null); }
        });
        assertFailure("INVALID_EDGE", new Runnable() {
            @Override public void run() { PipelineDefinition.builder("p", "r").addEdge(null); }
        });
        assertFailure("INVALID_OUTPUT", new Runnable() {
            @Override public void run() {
                PipelineDefinition.builder("p", "r").addOutput((PipelineOutput) null);
            }
        });
        assertFailure("INVALID_BOUNDARY", new Runnable() {
            @Override public void run() {
                new PipelineEdge("edge", "producer", "producer-sink", "consumer",
                        "consumer-source", "dataset", SCHEMA, null);
            }
        });
        assertFailure("INVALID_SCHEMA_FINGERPRINT", new Runnable() {
            @Override public void run() {
                new PipelineEdge("edge", "producer", "producer-sink", "consumer",
                        "consumer-source", "dataset", "not-a-fingerprint",
                        PipelineEdge.BoundaryKind.VERSIONED_DATASET);
            }
        });
    }

    @Test
    void compilerAndExplainerEntriesRefuseAMissingPlanOrDefinition() {
        assertFailure("INVALID_DEFINITION", new Runnable() {
            @Override public void run() {
                PipelineCompiler.compile(null, Collections.singletonList("out"));
            }
        });
        assertFailure("INVALID_PLAN", new Runnable() {
            @Override public void run() { PipelineExplainer.explain(null); }
        });
        assertFailure("INVALID_PIPELINE_PLAN", new Runnable() {
            @Override public void run() {
                VersionedDatasetHandoffSdk.handoffKey(null, null, "edge", "run");
            }
        });
        assertFailure("INVALID_BINDING_INPUT", new Runnable() {
            @Override public void run() {
                VersionedDatasetHandoffSdk.consumerRequest(
                        null, null, null, null, "consumer-run", "consumer-attempt");
            }
        });
    }

    @Test
    void publicDefinitionConstructorRequiresTheCompositionFeature() {
        final PipelineStep producer = step("a", PlanArtifact.RuntimeTarget.BATCH);
        assertFailure("MISSING_FEATURE", new Runnable() {
            @Override public void run() {
                new PipelineDefinition(1, 1, "p", "r",
                        Collections.<String>emptyList(), Collections.<String>emptyList(),
                        Collections.singletonList(producer),
                        Collections.<PipelineEdge>emptyList(),
                        Collections.<PipelineOutput>emptyList());
            }
        });
    }

    @Test
    void requestedOutputMustBindASinkOperator() {
        assertFailure("INVALID_OUTPUT_ENDPOINT_KIND", new Runnable() {
            @Override public void run() {
                PipelineDefinition.builder("p", "r")
                        .addBatchStep("a", step("a", PlanArtifact.RuntimeTarget.BATCH).artifact())
                        .addOutput("on-a", "a", "a-source")
                        .build();
            }
        });
    }

    @Test
    void handOffValuesRefuseAMissingOrOutOfRangePart() {
        assertFailure("INVALID_HANDOFF_KEY", new Runnable() {
            @Override public void run() {
                new VersionedDatasetProducerAttempt(null, "run", "attempt", 1L, 1L);
            }
        });
        assertFailure("INVALID_ATTEMPT", new Runnable() {
            @Override public void run() {
                new VersionedDatasetCommitEvidence(null, "file", 1, "v1", "raw", 1,
                        new byte[] {1, 2}, DIGEST, 1L, 2L, EPOCH_MILLIS);
            }
        });
        assertFailure("INVALID_EVIDENCE", new Runnable() {
            @Override public void run() {
                new VersionedDatasetVisibleWinner(null, EPOCH_MILLIS);
            }
        });
        assertFailure("INVALID_TIMESTAMP", new Runnable() {
            @Override public void run() {
                new VersionedDatasetVisibleWinner(evidence(), 0L);
            }
        });
        assertFailure("INVALID_VERSION", new Runnable() {
            @Override public void run() {
                new VersionedDatasetCommitEvidence(attempt(), "file", 0, "v1", "raw", 1,
                        new byte[] {1, 2}, DIGEST, 1L, 2L, EPOCH_MILLIS);
            }
        });
        assertFailure("INVALID_BYTES", new Runnable() {
            @Override public void run() {
                new VersionedDatasetCommitEvidence(attempt(), "file", 1, "v1", "raw", 1,
                        null, DIGEST, 1L, 2L, EPOCH_MILLIS);
            }
        });
        assertFailure("INVALID_BYTES", new Runnable() {
            @Override public void run() { new VersionedDatasetAuthorityRecord(null); }
        });
        assertFailure("INVALID_BINDING", new Runnable() {
            @Override public void run() {
                new VersionedDatasetConsumerBinding(null, null, EPOCH_MILLIS);
            }
        });
    }

    @Test
    void consumerBindingMustIdentifyTheWinnerItBinds() {
        final VersionedDatasetVisibleWinner winner =
                new VersionedDatasetVisibleWinner(evidence(), EPOCH_MILLIS);
        final VersionedDatasetConsumerRequest otherKey = new VersionedDatasetConsumerRequest(
                key("edge-two", "dataset-two"), winner.winnerDigest(),
                "b", "b-source", "artifact-b", "consumer-run", "consumer-attempt");
        assertFailure("BINDING_MISMATCH", binding(otherKey, winner));

        final VersionedDatasetConsumerRequest otherDigest =
                new VersionedDatasetConsumerRequest(
                        key("edge-one", "dataset-one"), repeat('f', 64),
                        "b", "b-source", "artifact-b", "consumer-run", "consumer-attempt");
        assertFailure("BINDING_MISMATCH", binding(otherDigest, winner));

        VersionedDatasetConsumerRequest matching = new VersionedDatasetConsumerRequest(
                key("edge-one", "dataset-one"), winner.winnerDigest(),
                "b", "b-source", "artifact-b", "consumer-run", "consumer-attempt");
        VersionedDatasetConsumerBinding binding =
                new VersionedDatasetConsumerBinding(matching, winner, EPOCH_MILLIS);
        assertNotNull(binding.bindingId());
        assertEquals(64, binding.bindingId().length());
    }

    @Test
    void durableAuthorityStorageReportsItsOwnFailureCode() throws IOException {
        final Path regularFile = temporary.resolve("not-a-directory");
        Files.write(regularFile, new byte[] {0});
        VersionedDatasetHandoffException failure = assertThrows(
                VersionedDatasetHandoffException.class, () ->
                        new FileVersionedDatasetAuthorityRecordStore(regularFile));
        assertEquals("AUTHORITY_IO_FAILURE", failure.code());
        assertTrue(failure.getMessage().contains("AUTHORITY_IO_FAILURE"));
    }

    private static Runnable binding(
            final VersionedDatasetConsumerRequest request,
            final VersionedDatasetVisibleWinner winner) {
        return new Runnable() {
            @Override public void run() {
                new VersionedDatasetConsumerBinding(request, winner, EPOCH_MILLIS);
            }
        };
    }

    private static VersionedDatasetHandoffKey key(String edgeId, String materializationId) {
        return new VersionedDatasetHandoffKey("p", "r", DIGEST, edgeId, materializationId,
                SCHEMA, "a", "a-sink", "artifact-a", "producer-run");
    }

    private static VersionedDatasetProducerAttempt attempt() {
        return new VersionedDatasetProducerAttempt(
                key("edge-one", "dataset-one"), "producer-run", "producer-attempt", 1L, 7L);
    }

    private static VersionedDatasetCommitEvidence evidence() {
        return new VersionedDatasetCommitEvidence(attempt(), "file", 1, "v1", "raw", 1,
                new byte[] {1, 2, 3}, DIGEST, 10L, 1024L, EPOCH_MILLIS);
    }

    private static PipelineStep step(String id, PlanArtifact.RuntimeTarget runtime) {
        List<OperatorDescriptor> operators = Arrays.asList(
                new OperatorDescriptor(id + "-source", OperatorDescriptor.Kind.SOURCE,
                        "test.source", SCHEMA, Collections.<String>emptyList()),
                new OperatorDescriptor(id + "-sink", OperatorDescriptor.Kind.SINK,
                        "test.sink", SCHEMA, Collections.singletonList(id + "-source")));
        return new PipelineStep(id, new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION, PlanArtifact.CURRENT_READER_VERSION,
                runtime,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.Boundedness.BOUNDED : PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.TimeMode.NONE : PlanArtifact.TimeMode.PROCESSING_TIME,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1)
                        : Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1,
                                PlanArtifact.FEATURE_RUNTIME_STREAM_V1),
                Collections.<String>emptyList(), operators,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1
                        : PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                new byte[] {1}));
    }

    private static void assertFailure(String code, Runnable action) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, action::run);
        assertEquals(code, failure.code());
        assertTrue(failure.getMessage().contains(code));
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
