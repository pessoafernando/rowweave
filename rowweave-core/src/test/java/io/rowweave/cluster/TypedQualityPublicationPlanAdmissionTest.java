package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.QualityGatePolicy;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.TypedQualityManifest;
import io.rowweave.DataRow;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.TypedQualityPublicationPlanValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** full publication proof and fragment-local admission. */
class TypedQualityPublicationPlanAdmissionTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    @Test
    void canonicalApiGraphRoundTripsAndExposesBothSummaryTypesToMonitoring(@TempDir Path tmp) {
        Path domain = tmp.resolve("round-trip");
        PlanGraph graph = PlanGraph.fromBytes(canonical(domain).toBytes());

        assertDoesNotThrow(() -> TypedQualityPublicationPlanValidator.validateFullGraph(graph));
        assertDoesNotThrow(() -> OutputCommitValidator.validate(graph));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(PlanOps.supportsUnaryType("typedQualityAcceptedSummary"));
        assertTrue(PlanOps.supportsUnaryType("typedQualityRejectedSummary"));
        assertTrue(graph.toJson().contains("typedQualityAcceptedSummary"));
        assertTrue(graph.toJson().contains("typedQualityRejectedSummary"));
        assertTrue(graph.toDot().contains("typedQualityAcceptedSummary"));
        assertFalse(Files.exists(domain));
    }

    @Test
    void canonicalPublicationExecutesThroughRealMasterWorkerFragments(@TempDir Path tmp) {
        Path domain = tmp.resolve("cluster-publication");
        Path worker = tmp.resolve("worker");
        ExecutionEnvironment session;
        try (InProcessCluster cluster = new InProcessCluster(2, worker.toFile())) {
            session = new ExecutionEnvironment("quality-cluster", "quality cluster", cluster);
            session.setParallelism(2);
            DataRowClassification classified = session.fromSource(new CollectionSource(
                            META, "quality-cluster-split", Arrays.asList(
                            row("A", 1), row(null, 2), row("B", 13))))
                    .uid("ledger.source")
                    .classifyRowsWithSourcePosition(validation());
            classified.writeToManifest(TypedQualityManifest.of(
                    domain.toString(), "valid", "rejected", "summary"));
            session.execute();
        }

        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertEquals(3, manifest.members().size());
        assertEquals(1, read(new ManifestFileSource(
                META, domain.toString(), "valid", manifest.generation())).size());
        assertEquals(2, read(new ManifestFileSource(
                REJECTED_META, domain.toString(), "rejected", manifest.generation())).size());
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||1",
                "REJECTED_REASON|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL|1",
                "REJECTED_REASON|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED|1",
                "REJECTED_TOTAL|||-1||2"), summaryLines(read(new ManifestFileSource(
                TypedQualityManifest.summarySchema(), domain.toString(), "summary",
                manifest.generation()))));
    }

    @Test
    void localP1P2P4EnforceQualityPolicyBeforeManifestVisibility(@TempDir Path tmp)
            throws Exception {
        for (int parallelism : Arrays.asList(1, 2, 4)) {
            Path domain = tmp.resolve("local-gate-p" + parallelism);
            runLocalQuality(domain, parallelism,
                    Collections.singletonList(row("baseline", 1)),
                    QualityGatePolicy.allowAll());
            ManifestFileStore store = new ManifestFileStore(domain);
            CommitManifest baseline = store.latest();

            IllegalStateException rejected = assertThrows(
                    IllegalStateException.class,
                    () -> runLocalQuality(domain, parallelism,
                            Collections.singletonList(row(null, 1)),
                            QualityGatePolicy.builder()
                                    .maxRejectedRows(0L).build()));
            assertTrue(allMessages(rejected).contains("QUALITY_GATE_REJECTED"),
                    allMessages(rejected));
            assertEquals(baseline, store.latest());
            long rejectedGeneration = baseline.generation() + 1L;
            assertEquals(null, store.exact(rejectedGeneration));
            for (String member : Arrays.asList("valid", "rejected", "summary")) {
                assertFalse(Files.exists(domain.resolve("data")
                        .resolve(Long.toString(rejectedGeneration))
                        .resolve(member + ".data")));
            }
            Path staging = domain.resolve("staging");
            if (Files.exists(staging)) {
                try (java.util.stream.Stream<Path> paths = Files.walk(staging)) {
                    assertFalse(paths.anyMatch(Files::isRegularFile));
                }
            }

            runLocalQuality(domain, parallelism,
                    Collections.singletonList(row(null, 1)),
                    QualityGatePolicy.builder().maxRejectedRows(1L).build());
            assertTrue(store.latest().generation() > baseline.generation());
        }
    }

    @Test
    void rejectsMissingExtraRepeatedAndMixedPublications(@TempDir Path tmp) {
        PlanGraph canonical = canonical(tmp.resolve("canonical"));
        List<PlanNode> missing = new ArrayList<>(canonical.nodes());
        missing.remove(summarySink(canonical));
        assertInvalid(new PlanGraph(missing, canonical.parallelism()));

        Path extraDomain = tmp.resolve("extra");
        ExecutionEnvironment extraSession = new ExecutionEnvironment("quality-extra", "quality extra");
        DataRowClassification extra = classification(extraSession);
        extra.writeToManifest(TypedQualityManifest.of(
                extraDomain.toString(), "valid", "rejected", "summary"));
        extra.validRows().sinkTo(new ManifestFileSink(META,
                extraDomain.toString(), "fourth"));
        assertInvalid(extraSession.toPlanGraph());

        Path repeatedDomain = tmp.resolve("repeated");
        ExecutionEnvironment repeatedSession = new ExecutionEnvironment("quality-repeated", "quality repeated");
        DataRowClassification repeated = classification(repeatedSession);
        repeated.writeToManifest(TypedQualityManifest.of(
                repeatedDomain.toString(), "valid-a", "rejected-a", "summary-a"));
        repeated.writeToManifest(TypedQualityManifest.of(
                repeatedDomain.toString(), "valid-b", "rejected-b", "summary-b"));
        assertInvalid(repeatedSession.toPlanGraph());

        Path mixedDomain = tmp.resolve("mixed");
        ExecutionEnvironment mixedSession = new ExecutionEnvironment("quality-mixed", "quality mixed");
        DataRowClassification first = classification(mixedSession);
        first.writeToManifest(TypedQualityManifest.of(
                mixedDomain.toString(), "valid", "rejected", "summary"));
        mixedSession.fromSource(new CollectionSource(
                        META, "second-split", Collections.emptyList()))
                .uid("second.source")
                .classifyRowsWithSourcePosition(validation())
                .validRows()
                .sinkTo(new ManifestFileSink(META, mixedDomain.toString(), "other"));
        assertInvalid(mixedSession.toPlanGraph());
    }

    @Test
    void rejectsSummaryVersionBoundMetaTopologySortRoleAndOutputTamper(@TempDir Path tmp) {
        PlanGraph graph = canonical(tmp.resolve("tamper"));
        int accepted = type(graph, "typedQualityAcceptedSummary");
        int rejected = type(graph, "typedQualityRejectedSummary");
        int union = type(graph, "union");
        int order = type(graph, "orderBy");

        assertInvalid(replace(graph, accepted,
                PlanNode.unary("typedQualityAcceptedSummary",
                                TypedQualityManifest.summarySchema(),
                                graph.node(accepted).child(0))
                        .param("summaryVersion", "2").build()));
        assertInvalid(replace(graph, rejected,
                PlanNode.unary("typedQualityRejectedSummary",
                                TypedQualityManifest.summarySchema(),
                                graph.node(rejected).child(0))
                        .param("summaryVersion", "1")
                        .param("maxReasonBuckets", "999").build()));
        assertInvalid(replace(graph, accepted,
                PlanNode.unary("typedQualityAcceptedSummary", META,
                                graph.node(accepted).child(0))
                        .param("summaryVersion", "1").build()));
        assertInvalid(replace(graph, union,
                PlanNode.binary("union", TypedQualityManifest.summarySchema(),
                        rejected, accepted).build()));
        assertInvalid(replace(graph, order,
                PlanNode.unary("orderBy", TypedQualityManifest.summarySchema(), union)
                        .listParam("sort", Collections.singletonList("5:ASC:DEFAULT"))
                        .build()));

        int validSink = sinkByRole(graph, "typed-quality.valid");
        assertInvalid(replace(graph, validSink,
                graph.node(validSink).withParam(
                        PlanNode.OPERATOR_UID_PARAM, "typed-quality.summary")));

        int summarySink = summarySink(graph);
        PlanNode wrongOutput = PlanNode.sink(
                        TypedQualityManifest.summarySchema(), order,
                        new DiscardSink(TypedQualityManifest.summarySchema()))
                .param(PlanNode.OPERATOR_UID_PARAM, "typed-quality.summary").build();
        assertInvalid(replace(graph, summarySink, wrongOutput));

        ManifestFileSink otherDomain = new ManifestFileSink(
                REJECTED_META, tmp.resolve("other-domain").toString(), "rejected");
        int rejectedSink = sinkByRole(graph, "typed-quality.rejected");
        PlanNode wrongDomain = PlanNode.sink(REJECTED_META,
                        graph.node(rejectedSink).child(0), otherDomain)
                .param(PlanNode.OPERATOR_UID_PARAM, "typed-quality.rejected").build();
        assertInvalid(replace(graph, rejectedSink, wrongDomain));
    }

    @Test
    void masterRejectsInvalidPublicationBeforeAssignmentOrDomainCreation(@TempDir Path tmp) {
        Path domain = tmp.resolve("master-reject");
        PlanGraph graph = canonical(domain);
        int accepted = type(graph, "typedQualityAcceptedSummary");
        PlanGraph invalid = replace(graph, accepted,
                graph.node(accepted).withParam("summaryVersion", "future"));
        AtomicInteger assignments = new AtomicInteger();
        String host = "quality-publication-worker";
        int port = 61_210;

        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.register(
                    host, port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "quality-worker", host, port, 8, -1, "inc-quality"));
                RpcMessage result = master.handler().apply(
                        RpcMessage.submitJob(invalid.toBytes()));
                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("QUALITY_PUBLICATION_INVALID"),
                        result.errorMessage());
                assertEquals(0, assignments.get());
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
                assertFalse(Files.exists(domain));
            } finally {
                registration.close();
            }
        }
    }

    @Test
    void workerRejectsSummaryAndSinkFragmentTamperBeforeOpeningRuntime(@TempDir Path tmp) {
        PlanNode placeholder = PlanNode.placeholderSource(REJECTED_META);
        PlanNode future = PlanNode.unary(
                        "typedQualityRejectedSummary",
                        TypedQualityManifest.summarySchema(), 0)
                .param("summaryVersion", "future")
                .param("maxReasonBuckets", "5").build();
        PlanGraph invalidSummary = new PlanGraph(Arrays.asList(placeholder, future));
        StageTask.StageResult result = new StageTask(
                StageCommand.keyedOp(REJECTED_META, 8, 0, invalidSummary, null),
                StageTask.PortListener.NONE).run();
        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertTrue(allMessages(result.failure()).contains("QUALITY_PUBLICATION_INVALID"),
                allMessages(result.failure()));

        PlanNode sinkPlaceholder = PlanNode.placeholderSource(
                        TypedQualityManifest.summarySchema())
                .withParam(TypedQualityPublicationPlanValidator.WORKER_SINK_ROLE_PARAM,
                        "typed-quality.summary");
        PlanNode wrongOrder = PlanNode.unary(
                        "orderBy", TypedQualityManifest.summarySchema(), 0)
                .listParam("sort", Collections.singletonList("5:ASC:DEFAULT")).build();
        PlanGraph sinkFragment = new PlanGraph(Arrays.asList(sinkPlaceholder, wrongOrder));
        ManifestFileSink output = new ManifestFileSink(
                TypedQualityManifest.summarySchema(), tmp.resolve("worker").toString(), "summary");
        assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateWorkerSinkFragment(
                        sinkFragment, output));
        assertFalse(Files.exists(tmp.resolve("worker")));
    }

    @Test
    void workerRejectsSameRoleOutputWithDifferentPolicyIdentity(@TempDir Path tmp) {
        PlanGraph allowAll = canonical(tmp.resolve("same-domain"));
        ExecutionEnvironment strictSession = new ExecutionEnvironment("strict-quality", "strict quality");
        classification(strictSession).writeToManifest(TypedQualityManifest.of(
                        tmp.resolve("same-domain").toString(), "valid", "rejected", "summary")
                .withQualityGate(QualityGatePolicy.builder().maxRejectedRows(0).build()));
        PlanGraph strict = strictSession.toPlanGraph();
        PlanNode allowAllSink = allowAll.node(sinkByRole(
                allowAll, "typed-quality.summary"));
        ManifestFileSink substituted = (ManifestFileSink) strict.node(sinkByRole(
                strict, "typed-quality.summary")).output();

        PlanNode placeholder = PlanNode.placeholderSource(
                        TypedQualityManifest.summarySchema())
                .withParam(TypedQualityPublicationPlanValidator.WORKER_SINK_ROLE_PARAM,
                        "typed-quality.summary")
                .withParam(TypedQualityPublicationPlanValidator.WORKER_SINK_BINDING_PARAM,
                        allowAllSink.batchSinkDescriptor().bindingIdentity());
        PlanNode order = PlanNode.unary(
                        "orderBy", TypedQualityManifest.summarySchema(), 0)
                .listParam("sort", Arrays.asList(
                        "0:ASC:DEFAULT", "1:ASC:DEFAULT", "2:ASC:DEFAULT",
                        "3:ASC:DEFAULT", "4:ASC:DEFAULT", "5:ASC:DEFAULT"))
                .build();
        PlanGraph fragment = new PlanGraph(Arrays.asList(placeholder, order));

        assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateWorkerSinkFragment(
                        fragment, substituted));

        PlanGraph markerless = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(TypedQualityManifest.summarySchema()), order));
        ManifestFileSink gated = (ManifestFileSink) allowAllSink.output();
        assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateWorkerSinkFragment(
                        markerless, gated));
        assertFalse(Files.exists(tmp.resolve("same-domain")));
    }

    private static PlanGraph canonical(Path domain) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-plan", "quality plan");
        classification(session).writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));
        return session.toPlanGraph();
    }

    private static void runLocalQuality(
            Path domain, int parallelism, List<DataRow> rows,
            QualityGatePolicy policy) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-local-gate",
                "quality local gate");
        session.setParallelism(parallelism);
        session.fromSource(new CollectionSource(META,
                        "quality-local-split", rows))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(validation())
                .writeToManifest(TypedQualityManifest.of(
                                domain.toString(), "valid", "rejected", "summary")
                        .withQualityGate(policy));
        session.execute();
    }

    private static DataRowClassification classification(ExecutionEnvironment session) {
        DataSet source = session.fromSource(new CollectionSource(
                META, "quality-split", Collections.emptyList())).uid("ledger.source");
        return source.classifyRowsWithSourcePosition(validation());
    }

    private static RowValidation validation() {
        return RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
    }

    private static PlanGraph replace(PlanGraph graph, int index, PlanNode replacement) {
        List<PlanNode> nodes = new ArrayList<>(graph.nodes());
        nodes.set(index, replacement);
        return new PlanGraph(nodes, graph.parallelism());
    }

    private static int type(PlanGraph graph, String typeId) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (typeId.equals(graph.node(i).typeId())) return i;
        }
        throw new AssertionError("missing type " + typeId);
    }

    private static int sinkByRole(PlanGraph graph, String role) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && role.equals(node.param(PlanNode.OPERATOR_UID_PARAM))) return i;
        }
        throw new AssertionError("missing sink role " + role);
    }

    private static int summarySink(PlanGraph graph) {
        return sinkByRole(graph, "typed-quality.summary");
    }

    private static void assertInvalid(PlanGraph graph) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateFullGraph(graph));
        assertTrue(failure.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:"),
                failure::getMessage);
    }

    private static String allMessages(Throwable failure) {
        StringBuilder result = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null) result.append(current.getMessage()).append('\n');
        }
        return result.toString();
    }

    private static List<DataRow> read(io.rowweave.Source input) {
        List<DataRow> rows = new ArrayList<>();
        for (DataRow row : input) rows.add(row);
        return rows;
    }

    private static List<String> summaryLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getString(0) + "|" + row.getString(1) + "|"
                    + row.getString(2) + "|" + row.getInteger(3) + "|"
                    + row.getString(4) + "|" + row.getLong(5));
        }
        return result;
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
