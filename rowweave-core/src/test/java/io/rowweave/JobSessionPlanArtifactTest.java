package io.rowweave;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidationException;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DataSet uid compilation, definition identity and the Artifact submit boundary. */
class JobSessionPlanArtifactTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void compilesExplicitSourceUnaryBinaryAndSinkIds() {
        BatchSession session = new BatchSession("ledger", "Ledger Close");
        session.setDefinitionId("domain.ledger-close.v1");
        DataSet left = session.fromSource(new RowsInput(1, 2)).uid("left-source")
                .filter("id > 0").uid("positive-filter");
        DataSet right = session.fromSource(new RowsInput(3)).uid("right-source");
        DataSet union = left.union(right).uid("all-rows");
        union.sinkTo("audit-output", new WorkerCollectingOutput(META));
        union.sinkTo("posting-output", new WorkerCollectingOutput(META));

        PlanArtifact artifact = session.toPlanArtifact();

        assertEquals("domain.ledger-close.v1", artifact.definitionId());
        assertEquals(Arrays.asList("left-source", "positive-filter", "right-source", "all-rows",
                        "audit-output", "posting-output"),
                Arrays.asList(
                        artifact.operators().get(0).operatorId(), artifact.operators().get(1).operatorId(),
                        artifact.operators().get(2).operatorId(), artifact.operators().get(3).operatorId(),
                        artifact.operators().get(4).operatorId(), artifact.operators().get(5).operatorId()));
        assertTrue(artifact.requiredFeatures().contains(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS));
        assertEquals(false, artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS));
    }

    @Test
    void mixedIdsAndLegacyDefinitionIdAreDeterministic() {
        PlanArtifact first = legacyArtifact("same-name", "same-title");
        PlanArtifact second = legacyArtifact("same-name", "same-title");
        PlanArtifact changed = legacyArtifact("same-name", "changed-title");

        assertEquals(first.definitionId(), second.definitionId());
        assertEquals(first.artifactId(), second.artifactId());
        assertTrue(first.definitionId().startsWith("legacy-definition-"));
        assertNotEquals(first.definitionId(), changed.definitionId());
        assertTrue(first.requiredFeatures().contains(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS));
        assertTrue(first.requiredFeatures().contains(PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS));
    }

    @Test
    void duplicateUidFailsBeforeClientOrOutputSideEffects() {
        RecordingArtifactClient client = new RecordingArtifactClient();
        BatchSession session = new BatchSession("duplicate", "duplicate", client);
        DataSet source = session.fromSource(new RowsInput(1)).uid("same");
        source.filter("id > 0").uid("same")
                .sinkTo("output", new WorkerCollectingOutput(META));

        PlanArtifactValidationException failure = assertThrows(PlanArtifactValidationException.class,
                session::execute);
        assertEquals("DUPLICATE_OPERATOR_ID", failure.code());
        assertEquals(0, client.artifactSubmits);
        assertEquals(0, client.graphSubmits);
    }

    @Test
    void commitUsesArtifactOverloadAndLegacyClientsUseDefaultAdapter() {
        RecordingArtifactClient recording = new RecordingArtifactClient();
        BatchSession session = new BatchSession("artifact-submit", "artifact-submit", recording);
        session.fromSource(new RowsInput(1)).uid("source")
                .sinkTo("output", new WorkerCollectingOutput(META));

        session.execute();

        assertEquals(1, recording.artifactSubmits);
        assertEquals(0, recording.graphSubmits);
        assertEquals("source", recording.artifact.operators().get(0).operatorId());

        LegacyOnlyClient legacy = new LegacyOnlyClient();
        legacy.submit(recording.artifact);
        assertEquals(1, legacy.graphSubmits);
        assertEquals("source", legacy.graph.node(0).param("__operator_uid__"));
    }

    @Test
    void localAttachedFallbackPreservesV1ClosuresButCannotCrossCodecOrClusterBoundary() {
        BatchSession session = new BatchSession("local-closure", "local-closure");
        WorkerCollectingOutput output = new WorkerCollectingOutput(META);
        session.fromSource(new NonPortableInput(7)).sinkTo(output);

        assertThrows(UncheckedIOException.class, session::toPlanArtifact,
                "public artifact compilation must not hide a non-portable payload");
        session.execute();
        assertEquals(1, output.getRows().size());
        assertEquals(7, output.getRows().get(0).get(0));

        PlanArtifact local = LegacyPlanArtifactAdapter.wrapLocal(
                "local-only", session.toPlanGraph());
        PlanArtifact decoded = PlanArtifactCodec.decode(PlanArtifactCodec.encode(local));
        PlanArtifactValidationException missing = assertThrows(PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.unwrapValidated(decoded));
        assertEquals("LOCAL_PAYLOAD_REFERENCE_MISSING", missing.code());

        AtomicInteger lookups = new AtomicInteger();
        PlanArtifactValidationException rejected = assertThrows(PlanArtifactValidationException.class,
                () -> new ClusterJobClient(() -> {
                    lookups.incrementAndGet();
                    throw new AssertionError("local-only artifact must fail before Master lookup");
                }).submit(local));
        assertEquals("UNSUPPORTED_RUNTIME", rejected.code());
        assertEquals(0, lookups.get());
    }

    private static PlanArtifact legacyArtifact(String name, String title) {
        BatchSession session = new BatchSession(name, title);
        DataSet source = session.fromSource(new RowsInput(1)).uid("source");
        source.filter("id > 0").sinkTo(new DeterministicWorkerOutput());
        return session.toPlanArtifact();
    }

    private static final class DeterministicWorkerOutput implements Sink {
        private static final long serialVersionUID = 1L;

        @Override public DataSchema getSchema() { return META; }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public void collect(DataRow row) { }
    }

    private static final class RecordingArtifactClient implements JobClient {
        private int artifactSubmits;
        private int graphSubmits;
        private PlanArtifact artifact;

        @Override
        public long submitToSink(PlanGraph sinkGraph) {
            graphSubmits++;
            return 0;
        }

        @Override
        public ExecutionReport submit(PlanArtifact submitted) {
            artifactSubmits++;
            artifact = submitted;
            return new ExecutionReport(0, 0, 0, 0, null);
        }
    }

    private static final class LegacyOnlyClient implements JobClient {
        private int graphSubmits;
        private PlanGraph graph;

        @Override
        public long submitToSink(PlanGraph sinkGraph) {
            graphSubmits++;
            graph = sinkGraph;
            return 0;
        }
    }

    private static final class RowsInput implements Source {
        private static final long serialVersionUID = 1L;
        private final List<Integer> values;

        private RowsInput(Integer... values) {
            this.values = Arrays.asList(values);
        }

        @Override
        public DataSchema getSchema() {
            return META;
        }

        @Override
        public Iterator<DataRow> iterator() {
            return values.stream().map(value -> {
                DataRow row = new DataRow(1);
                row.set(0, value);
                return row;
            }).iterator();
        }
    }

    private static final class NonPortableInput implements Source {
        private static final long serialVersionUID = 1L;
        private final Object deliberatelyNonSerializable = new Object();
        private final int value;

        private NonPortableInput(int value) {
            this.value = value;
        }

        @Override public DataSchema getSchema() { return META; }

        @Override
        public Iterator<DataRow> iterator() {
            DataRow row = new DataRow(1);
            row.set(0, value);
            return java.util.Collections.singletonList(row).iterator();
        }
    }
}
