package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.SchemaConversion;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** describable execution and pre-side-effect tamper closure. */
class SchemaConversionPlanAdmissionTest {

    private static final DataSchema INPUT = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("cents", DataType.LongType),
            new Field("memo", DataType.StringType));
    private static final SchemaConversion CONVERSION = SchemaConversion.builder(INPUT)
            .required("id", new Field("adaptor_id", DataType.LongType),
                    SchemaConversion.Cast.INTEGER_TO_LONG)
            .required("cents", new Field("amount", DataType.BigDecimalType),
                    SchemaConversion.Cast.LONG_TO_DECIMAL)
            .nullable("memo", new Field("description", DataType.StringType),
                    SchemaConversion.Cast.IDENTITY)
            .defaulted(new Field("district", DataType.StringType), "AAA")
            .build();
    private static final DataSchema OUTPUT = CONVERSION.getOutputSchema();

    @Test
    void planRoundTripKeepsBuiltInDescriptionAndExecutesLocally(@TempDir Path temp) {
        Path output = temp.resolve("local.tsv");
        BatchSession session = new BatchSession("schema-plan", "schema plan");
        session.fromSource(new CollectionSource(INPUT, inputRows()))
                .convertSchema(CONVERSION)
                .sinkTo(new FileSink(OUTPUT, output.toString()));

        PlanGraph graph = PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        PlanNode node = graph.node(1);
        assertEquals("schemaConvert", node.typeId());
        assertEquals(CONVERSION.sourceIndices(), node.listParam("sourceIndices"));
        assertEquals(CONVERSION.casts(), node.listParam("casts"));
        assertEquals(CONVERSION.nullPolicies(), node.listParam("nullPolicies"));
        assertEquals(CONVERSION.defaultKinds(), node.listParam("defaultKinds"));
        assertEquals(CONVERSION.defaultValues(), node.listParam("defaultValues"));

        assertEquals(2L, new LocalJobClient(2).submitToSink(graph));
        assertEquals(Arrays.asList("1|10|first|AAA", "2|-5|null|AAA"), read(output));
    }

    @Test
    void malformedDescriptionsFailCapabilityPreflightBeforeInputOpen() {
        AtomicInteger iteratorCalls = new AtomicInteger();
        Source source = new Source() {
            @Override public DataSchema getSchema() { return INPUT; }
            @Override public java.util.Iterator<DataRow> iterator() {
                iteratorCalls.incrementAndGet();
                return Collections.<DataRow>emptyList().iterator();
            }
        };

        assertInvalid(graph(source, lists(
                Arrays.asList("0"),
                Arrays.asList("INTEGER_TO_LONG", "LONG_TO_DECIMAL", "IDENTITY", "IDENTITY"),
                CONVERSION.nullPolicies(), CONVERSION.defaultKinds(), CONVERSION.defaultValues())));
        assertInvalid(graph(source, lists(
                CONVERSION.sourceIndices(),
                Arrays.asList("FUTURE_CAST", "LONG_TO_DECIMAL", "IDENTITY", "IDENTITY"),
                CONVERSION.nullPolicies(), CONVERSION.defaultKinds(), CONVERSION.defaultValues())));
        assertInvalid(graph(source, lists(
                Arrays.asList("99", "1", "2", "-1"), CONVERSION.casts(),
                CONVERSION.nullPolicies(), CONVERSION.defaultKinds(), CONVERSION.defaultValues())));
        assertInvalid(graph(source, lists(
                CONVERSION.sourceIndices(), CONVERSION.casts(), CONVERSION.nullPolicies(),
                Arrays.asList("NONE", "NONE", "NONE", "NONE"), CONVERSION.defaultValues())));

        assertEquals(0, iteratorCalls.get(), "capability preflight must not open the source");
    }

    @Test
    void masterRejectsTamperedDescriptionBeforeWorkerAssignment() {
        AtomicInteger assignments = new AtomicInteger();
        String host = "schema-conversion-worker";
        int port = 61_097;
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "schema-worker", host, port, 8, -1, "inc-schema-conversion"));
                PlanGraph tampered = graph(null, lists(
                        CONVERSION.sourceIndices(),
                        Arrays.asList("FUTURE_CAST", "LONG_TO_DECIMAL", "IDENTITY", "IDENTITY"),
                        CONVERSION.nullPolicies(), CONVERSION.defaultKinds(), CONVERSION.defaultValues()));

                RpcMessage result = master.handler().apply(RpcMessage.submitJob(tampered.toBytes()));

                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("SCHEMA_MAPPING_INVALID"), result.errorMessage());
                assertEquals(0, assignments.get(), "tampered mapping must fail before stage dispatch");
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            } finally {
                registration.close();
            }
        }
    }

    @Test
    void localAndInProcessP1P2P4MatchIndependentOracle(@TempDir Path temp) throws Exception {
        assertEquals(oracle(), run(null, 1, temp.resolve("local-p1.tsv")));
        assertEquals(oracle(), run(null, 2, temp.resolve("local-p2.tsv")));
        assertEquals(oracle(), run(null, 4, temp.resolve("local-p4.tsv")));

        try (InProcessCluster cluster = new InProcessCluster(1, temp.resolve("cluster-p1").toFile())) {
            assertEquals(oracle(), run(cluster, 1, temp.resolve("cluster-p1.tsv")));
        }
        try (InProcessCluster cluster = new InProcessCluster(2, temp.resolve("cluster-p2").toFile())) {
            assertEquals(oracle(), run(cluster, 2, temp.resolve("cluster-p2.tsv")));
        }
        try (InProcessCluster cluster = new InProcessCluster(4, temp.resolve("cluster-p4").toFile())) {
            assertEquals(oracle(), run(cluster, 4, temp.resolve("cluster-p4.tsv")));
        }
    }

    @Test
    void conversionRemainsAVisibleFusibleLogicalNode() {
        BatchSession session = new BatchSession("schema-visible", "schema visible");
        DataSet converted = session.fromSource(new CollectionSource(INPUT, inputRows()))
                .convertSchema(CONVERSION)
                .filter("adaptor_id > 0");
        converted.sinkTo(new FileSink(converted.getSchema(),
                new java.io.File(System.getProperty("java.io.tmpdir"),
                        "schema-visible-" + System.nanoTime() + ".tsv").getAbsolutePath()));
        PlanGraph graph = session.toPlanGraph();

        assertEquals("schemaConvert", graph.node(1).typeId());
        assertEquals("filter", graph.node(2).typeId());
        assertTrue(io.rowweave.plan.PlanOps.isPassThrough("schemaConvert"));
        assertTrue(graph.toJson().contains("schemaConvert"));
        assertFalse(graph.toJson().contains("SCHEMA_CONVERSION_RUNTIME_NOT_IMPLEMENTED"));
    }

    private static List<List<String>> lists(List<String> sourceIndices, List<String> casts,
                                             List<String> nullPolicies, List<String> defaultKinds,
                                             List<String> defaultValues) {
        return Arrays.asList(sourceIndices, casts, nullPolicies, defaultKinds, defaultValues);
    }

    private static PlanGraph graph(Source input, List<List<String>> params) {
        PlanNode.Builder conversion = PlanNode.unary("schemaConvert", OUTPUT, 0)
                .listParam("sourceIndices", params.get(0))
                .listParam("casts", params.get(1))
                .listParam("nullPolicies", params.get(2))
                .listParam("defaultKinds", params.get(3))
                .listParam("defaultValues", params.get(4));
        return new PlanGraph(Arrays.asList(
                input == null
                        ? PlanNode.source(INPUT, Collections.emptyList())
                        : PlanNode.sourceFromInputs(INPUT, Collections.singletonList(input)),
                conversion.build(),
                PlanNode.sink(OUTPUT, 1, new io.rowweave.DiscardSink(OUTPUT)).build()));
    }

    private static void assertInvalid(PlanGraph graph) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(failure.getMessage().contains("SCHEMA_MAPPING_INVALID"), failure::getMessage);
    }

    private static List<String> run(JobClient client, int parallelism, Path output) {
        BatchSession session = client == null
                ? new BatchSession("schema-runtime", "schema runtime")
                : new BatchSession("schema-runtime", "schema runtime", client);
        session.setParallelism(parallelism);
        session.fromSource(new CollectionSource(INPUT, inputRows()))
                .convertSchema(CONVERSION)
                .sinkTo(new FileSink(OUTPUT, output.toString()));
        session.execute();
        return read(output);
    }

    private static List<String> oracle() {
        return Arrays.asList("1|10|first|AAA", "2|-5|null|AAA");
    }

    private static List<String> read(Path output) {
        List<String> result = new ArrayList<>();
        for (DataRow row : FileSink.read(OUTPUT, output.toString())) {
            result.add(row.getLong(0) + "|" + row.getBigDecimal(1).toPlainString()
                    + "|" + row.get(2) + "|" + row.getString(3));
        }
        Collections.sort(result);
        return result;
    }

    private static List<DataRow> inputRows() {
        return Arrays.asList(row(1, 10L, "first"), row(2, -5L, null));
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
