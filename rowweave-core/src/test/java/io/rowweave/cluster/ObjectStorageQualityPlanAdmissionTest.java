package io.rowweave.cluster;

import io.rowweave.AssignedSourceBatch;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.ManifestFileSink;
import io.rowweave.QualityGatePolicy;
import io.rowweave.QualityGatePolicyCodec;
import io.rowweave.DataSchema;
import io.rowweave.TypedQualityManifest;
import io.rowweave.plan.ObjectStorageQualityPlanValidator;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical Object quality graph admission in every role. */
class ObjectStorageQualityPlanAdmissionTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));

    @AfterEach
    void resetProvider() {
        ProbeProvider.opens.set(0);
    }

    @Test
    void canonicalTenNodeGraphRoundTripsAndIsExecutableWithoutSideEffects(
            @TempDir Path temp) {
        Path domain = temp.resolve("quality");
        PlanGraph graph = PlanGraph.fromBytes(canonical(domain).toBytes());

        assertDoesNotThrow(() -> ObjectStorageQualityPlanValidator.validateFullGraph(graph));
        assertDoesNotThrow(() -> OutputCommitValidator.validate(graph));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(PlanOps.supportsUnaryType("objectQualityRoute"));
        assertTrue(graph.toJson().contains("objectQualityRoute"));
        assertTrue(graph.toDot().contains("objectQualityRoute"));
        assertEquals(10, graph.nodeCount());
        assertEquals(0, ProbeProvider.opens.get());
        assertFalse(Files.exists(domain));
    }

    @Test
    void strictPolicySurvivesObjectPlanRoundTripOnAllThreeSinks(@TempDir Path temp) {
        QualityGatePolicy policy = QualityGatePolicy.builder()
                .maxRejectedRows(0)
                .maxRejectedPartsPerMillion(0)
                .exemplarLimit(4)
                .build();
        PlanGraph graph = PlanGraph.fromBytes(
                canonical(temp.resolve("strict"), policy).toBytes());

        ObjectStorageQualityPlanValidator.validateFullGraph(graph);
        int sinks = 0;
        for (PlanNode node : graph.nodes()) {
            if (node.kind() != PlanNode.Kind.SINK) continue;
            sinks++;
            ManifestFileSink output = (ManifestFileSink) node.output();
            assertEquals(policy, output.qualityGatePolicy());
            assertArrayEquals(QualityGatePolicyCodec.digest(policy),
                    output.qualityGatePolicyDigest());
            assertEquals(node.param(PlanNode.OPERATOR_UID_PARAM),
                    output.qualityGateRole());
        }
        assertEquals(3, sinks);
        assertEquals(0, ProbeProvider.opens.get());
        assertFalse(Files.exists(temp.resolve("strict")));
    }

    @Test
    void mtlsBuiltInTrustAdmitsWrapperAndStillRejectsApplicationProvider(
            @TempDir Path temp) {
        PlanGraph graph = canonical(temp.resolve("trust"));

        byte[] locallyEncoded = graph.toBytes();
        UncheckedIOException rejected = assertThrows(UncheckedIOException.class,
                () -> PlanGraph.fromBytes(locallyEncoded,
                        ObjectTrustContext.mtlsBuiltIns(), ignored -> { }));
        String messages = allMessages(rejected);
        assertTrue(messages.contains(ProbeProvider.class.getName()), messages);
        assertFalse(messages.contains(
                "rejected serialized class: io.rowweave.ObjectStorageQualitySource"), messages);
        assertEquals(0, ProbeProvider.opens.get());
        assertFalse(Files.exists(temp.resolve("trust")));
    }

    @Test
    void rejectsRouteSourceSummaryAndTopologyTamperBeforeSideEffects(
            @TempDir Path temp) throws Exception {
        PlanGraph graph = canonical(temp.resolve("tamper"));
        int validRoute = route(graph, "VALID");
        int rejectedRoute = route(graph, "REJECTED");
        int rejectedSummary = type(graph, "typedQualityRejectedSummary");

        assertInvalid(replace(graph, validRoute,
                graph.node(validRoute).withParam("carrierVersion", "future")));
        assertInvalid(replace(graph, rejectedRoute,
                PlanNode.unary("objectQualityRoute",
                                graph.node(rejectedRoute).outMeta(), validRoute)
                        .param("carrierVersion", "1")
                        .param("route", "REJECTED").build()));
        assertInvalid(replace(graph, rejectedSummary,
                graph.node(rejectedSummary).withParam("maxReasonBuckets", "5")));

        List<PlanNode> missing = new ArrayList<>(graph.nodes());
        missing.remove(missing.size() - 1);
        assertInvalid(new PlanGraph(missing, graph.parallelism()));

        PlanGraph uidTamper = canonical(temp.resolve("uid-tamper"));
        Object wrapper = uidTamper.node(uidTamper.singleSourceIndex()).inputs().get(0);
        java.lang.reflect.Field uid = wrapper.getClass().getDeclaredField("sourceOperatorUid");
        uid.setAccessible(true);
        uid.set(wrapper, "different.source");
        assertInvalid(uidTamper);

        assertEquals(0, ProbeProvider.opens.get());
        assertFalse(Files.exists(temp.resolve("tamper")));
        assertFalse(Files.exists(temp.resolve("uid-tamper")));
    }

    @Test
    void masterRejectsTamperBeforeAssignmentProviderOrDomain(@TempDir Path temp) {
        Path domain = temp.resolve("master-reject");
        PlanGraph graph = canonical(domain);
        PlanGraph invalid = replace(graph, route(graph, "VALID"),
                graph.node(route(graph, "VALID")).withParam("route", "UNKNOWN"));
        AtomicInteger assignments = new AtomicInteger();
        String host = "object-quality-worker";
        int port = 61_211;

        try (MasterServer master = new MasterServer(
                60_000L, 0, new io.rowweave.cluster.registry.InProcessRegistry())) {
            io.rowweave.cluster.rpc.InProcessRpcRegistry.Registration registration =
                    io.rowweave.cluster.rpc.InProcessRpcRegistry.register(
                            host, port, request -> {
                                if (request.type()
                                        == io.rowweave.cluster.rpc.RpcMessage.Type.ASSIGN_STAGE) {
                                    assignments.incrementAndGet();
                                }
                                return io.rowweave.cluster.rpc.RpcMessage.ack();
                            });
            try {
                master.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                        "object-quality-worker", host, port, 8, -1, "inc-object-quality"));
                io.rowweave.cluster.rpc.RpcMessage result = master.handler().apply(
                        io.rowweave.cluster.rpc.RpcMessage.submitJob(invalid.toBytes()));
                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("QUALITY_PUBLICATION_INVALID"),
                        result.errorMessage());
                assertEquals(0, assignments.get());
                assertEquals(0, ProbeProvider.opens.get());
                assertFalse(Files.exists(domain));
            } finally {
                registration.close();
            }
        }
    }

    @Test
    void workerRejectsRouteTamperBeforeOpeningAssignedInput() {
        PlanGraph full = canonical(java.nio.file.Paths.get("target", "unused-quality"));
        PlanNode source = full.node(full.singleSourceIndex());
        DataSchema carrierMeta = source.outMeta();
        PlanNode placeholder = PlanNode.placeholderSource(carrierMeta)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger.object.source");
        PlanNode invalidRoute = PlanNode.unary(
                        "objectQualityRoute", META, 0)
                .param("carrierVersion", "future")
                .param("route", "VALID").build();
        PlanGraph fragment = new PlanGraph(java.util.Arrays.asList(
                placeholder, invalidRoute));
        AssignedSourceBatch assigned =
                new AssignedSourceBatch(source.sourceSplitBindings());

        StageTask.StageResult result = new StageTask(
                StageCommand.sourceStage(
                        carrierMeta, META, 8, assigned, fragment,
                        new ShuffleEndpoint("127.0.0.1", 61_212)),
                StageTask.PortListener.NONE).run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertTrue(allMessages(result.failure()).contains("QUALITY_PUBLICATION_INVALID"),
                allMessages(result.failure()));
        assertEquals(0, ProbeProvider.opens.get());
    }

    private static PlanGraph canonical(Path domain) {
        return canonical(domain, TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));
    }

    private static PlanGraph canonical(Path domain, QualityGatePolicy policy) {
        return canonical(domain, TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy));
    }

    private static PlanGraph canonical(Path domain, TypedQualityManifest target) {
        byte[] empty = new byte[0];
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                META, new ProbeProvider(), "endpoint-test", "credential-test", "domain",
                "ledger.tsv", "version-1", 0L, sha256(empty),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        ExecutionEnvironment session = new ExecutionEnvironment("object-quality-plan", "object quality plan");
        session.classifyObjectStorageRows("ledger.object.source", input)
                .writeToManifest(target);
        return session.toPlanGraph();
    }

    private static PlanGraph replace(PlanGraph graph, int index, PlanNode replacement) {
        List<PlanNode> nodes = new ArrayList<>(graph.nodes());
        nodes.set(index, replacement);
        return new PlanGraph(nodes, graph.parallelism());
    }

    private static int route(PlanGraph graph, String route) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if ("objectQualityRoute".equals(node.typeId())
                    && route.equals(node.param("route"))) return i;
        }
        throw new AssertionError("missing route " + route);
    }

    private static int type(PlanGraph graph, String typeId) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (typeId.equals(graph.node(i).typeId())) return i;
        }
        throw new AssertionError("missing type " + typeId);
    }

    private static void assertInvalid(PlanGraph graph) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> ObjectStorageQualityPlanValidator.validateFullGraph(graph));
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

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte item : digest) {
                result.append(Character.forDigit((item >>> 4) & 0xf, 16));
                result.append(Character.forDigit(item & 0xf, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static final class ProbeProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static final AtomicInteger opens = new AtomicInteger();
        public ProbeProvider() { }
        @Override public String providerType() { return "object-quality-probe"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            opens.incrementAndGet();
            return null;
        }
    }
}
