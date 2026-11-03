package io.rowweave;

import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public compound target and canonical three-sink plan skeleton. */
class TypedQualityManifestModelTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void exposesFixedSummarySchema() {
        DataSchema summary = TypedQualityManifest.summarySchema();

        assertEquals(6, summary.getFieldCount());
        assertField(summary, 0, "quality_metric_kind", DataType.StringType);
        assertField(summary, 1, "quality_error_code", DataType.StringType);
        assertField(summary, 2, "quality_rule_id", DataType.StringType);
        assertField(summary, 3, "quality_field_index", DataType.IntegerType);
        assertField(summary, 4, "quality_detail", DataType.StringType);
        assertField(summary, 5, "row_count", DataType.LongType);
    }

    @Test
    void validatesDomainAndThreeDistinctCanonicalMemberNamesWithoutCreatingDirectory(
            @TempDir Path tmp) {
        Path domain = tmp.resolve("not-created").resolve("quality");
        TypedQualityManifest.of(domain.toString(), "valid", "rejected", "summary");
        assertFalse(Files.exists(domain), "model construction must not create the commit domain");

        assertInvalid(() -> TypedQualityManifest.of(null, "v", "r", "s"));
        assertInvalid(() -> TypedQualityManifest.of("", "v", "r", "s"));
        assertInvalid(() -> TypedQualityManifest.of(domain.toString(), "v", "v", "s"));
        assertInvalid(() -> TypedQualityManifest.of(domain.toString(), "v", "r", "r"));
        assertInvalid(() -> TypedQualityManifest.of(domain.toString(), "../v", "r", "s"));
        assertInvalid(() -> TypedQualityManifest.of(domain.toString(),
                Normalizer.normalize("e\u0301", Normalizer.Form.NFD), "r", "s"));
    }

    @Test
    void oneCallBuildsExactlyThreeSameDomainManifestSinksAndCanonicalSummaryDag(
            @TempDir Path tmp) {
        Path domain = tmp.resolve("quality-domain");
        BatchSession session = new BatchSession("quality-publication", "quality publication");
        DataRowClassification classification = classification(session);

        classification.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid-data", "rejected-data", "quality-summary"));
        PlanGraph graph = session.toPlanGraph();

        List<PlanNode> sinks = nodesOfKind(graph, PlanNode.Kind.SINK);
        assertEquals(3, sinks.size());
        Set<String> members = new HashSet<>();
        Set<String> domains = new HashSet<>();
        for (PlanNode sink : sinks) {
            assertEquals(ManifestFileSink.class, sink.output().getClass());
            ManifestFileSink output = (ManifestFileSink) sink.output();
            members.add(output.memberName());
            domains.add(output.domainId());
        }
        assertEquals(new HashSet<>(Arrays.asList(
                "valid-data", "rejected-data", "quality-summary")), members);
        assertEquals(1, domains.size());
        assertEquals(1, countType(graph, "typedQualityAcceptedSummary"));
        assertEquals(1, countType(graph, "typedQualityRejectedSummary"));
        assertEquals(1, countType(graph, "union"));
        assertEquals(1, countType(graph, "orderBy"));

        assertEquals(1, sinkMetas(graph, "valid-data", META).size());
        assertEquals(1, sinkMetas(graph, "rejected-data",
                classification.rejectedRows().getSchema()).size());
        assertEquals(1, sinkMetas(graph, "quality-summary",
                TypedQualityManifest.summarySchema()).size());
        assertFalse(Files.exists(domain), "plan construction must not open outputs");
    }

    @Test
    void invalidCompoundTargetLeavesSessionWithoutPartiallyRegisteredSinks(
            @TempDir Path tmp) {
        BatchSession session = new BatchSession("quality-atomic-build", "quality atomic build");
        DataRowClassification classification = classification(session);

        assertInvalid(() -> classification.writeToManifest(null));
        assertThrows(IllegalStateException.class, session::toPlanGraph,
                "a failed compound call must not leave valid/rejected sinks behind");
        assertFalse(Files.exists(tmp.resolve("unused")));
    }

    @Test
    void planConstructionRemainsSideEffectFreeAfterSummaryRuntimeAdmission(
            @TempDir Path tmp) {
        Path domain = tmp.resolve("runtime-admitted");
        BatchSession session = new BatchSession("quality-plan-only", "quality plan only");
        classification(session).writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));

        session.toPlanGraph();
        assertFalse(Files.exists(domain), "Plan compilation must not open the commit domain");
    }

    private static DataRowClassification classification(BatchSession session) {
        RowValidation validation = RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
        return session.fromSource(new CollectionSource(
                        META, "quality-split", Collections.emptyList()))
                .uid("quality.source")
                .classifyRowsWithSourcePosition(validation);
    }

    private static List<PlanNode> nodesOfKind(PlanGraph graph, PlanNode.Kind kind) {
        List<PlanNode> result = new ArrayList<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == kind) result.add(graph.node(i));
        }
        return result;
    }

    private static int countType(PlanGraph graph, String typeId) {
        int count = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (typeId.equals(graph.node(i).typeId())) count++;
        }
        return count;
    }

    private static List<DataSchema> sinkMetas(
            PlanGraph graph, String member, DataSchema expected) {
        List<DataSchema> result = new ArrayList<>();
        for (PlanNode sink : nodesOfKind(graph, PlanNode.Kind.SINK)) {
            ManifestFileSink output = (ManifestFileSink) sink.output();
            if (member.equals(output.memberName())) {
                assertEquals(expected, sink.outMeta());
                result.add(sink.outMeta());
            }
        }
        return result;
    }

    private static void assertField(DataSchema meta, int index, String name, DataType type) {
        assertEquals(name, meta.getField(index).getName());
        assertEquals(type, meta.getField(index).getDataType());
    }

    private static void assertInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:"),
                failure::getMessage);
    }

}
