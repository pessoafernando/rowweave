package io.rowweave.plan;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plan IR: serialization round-trip and DAG analysis. The IR is a serializable description with
 * no lambda and no List of rows carried between nodes, and the wire format is frozen so a graph
 * round-trips byte for byte.
 */
class PlanGraphTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("name", DataType.StringType));

    private static DataRow row(Integer id, BigDecimal amt, String name) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, amt);
        r.set(2, name);
        return r;
    }

    /** PlanGraph.toBytes -> fromBytes preserves structure: nodeCount, kind, typeId, outMeta, children and params. */
    @Test
    void serializationRoundTripStructure() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter));

        PlanGraph back = PlanGraph.fromBytes(g.toBytes());

        assertEquals(2, back.nodeCount());
        assertEquals(PlanNode.Kind.SOURCE, back.node(0).kind());
        assertEquals(PlanNode.Kind.UNARY, back.node(1).kind());
        assertEquals("filter", back.node(1).typeId());
        assertEquals("id > 5", back.node(1).param("expr"));
        assertArrayEquals(new int[]{0}, back.node(1).childIndices());
        // outMeta is equivalent
        DataSchema m = back.node(1).outMeta();
        assertEquals(3, m.getFieldCount());
        assertEquals("amt", m.getField(1).getName());
        assertEquals(DataType.BigDecimalType, m.getField(1).getDataType());
    }

    @Test
    void writerReservesUtf8ScratchAndObjectStreamBaselineBeforeAllocations() {
        String large = String.join("", Collections.nCopies(2_000, "x"));
        PlanNode source = PlanNode.source(META, Collections.emptyList())
                .withParam("large", large);
        PlanGraph graph = new PlanGraph(Collections.singletonList(source));
        List<Long> reservations = new ArrayList<>();

        graph.toBytes(reservations::add);

        assertTrue(reservations.contains(3L * large.length()),
                "UTF-8 scratch must be reserved before String.getBytes");
        assertTrue(reservations.contains(
                        io.rowweave.memory.ResidentObjectOutputStreams.BASELINE_BYTES),
                "embedded ObjectOutputStream internals need an up-front baseline");
        assertTrue(Collections.frequency(reservations, 128L) > 1,
                "ObjectOutputStream handle growth must charge each encountered reference");
    }

    /** SOURCE rows travel through a Java serialization round-trip on their Source, restoring nulls and BigDecimal scale. */
    @Test
    void sourceRowsRoundTrip() {
        List<DataRow> rows = Arrays.asList(
                row(1, new BigDecimal("12.3400000000"), "a"),
                row(2, null, null),
                row(null, new BigDecimal("0.0000000001"), "z"));
        PlanNode src = PlanNode.source(META, rows);
        PlanGraph g = new PlanGraph(Arrays.asList(src,
                PlanNode.unary("select", META, 0).build()));

        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        List<DataRow> got = new ArrayList<>();
        for (io.rowweave.Source input : back.node(0).inputs()) {
            for (DataRow value : input) {
                got.add(value);
            }
        }

        assertEquals(3, got.size());
        assertEquals(1, got.get(0).get(0));
        assertEquals(new BigDecimal("12.3400000000"), got.get(0).get(1));
        assertEquals("a", got.get(0).get(2));
        assertNull(got.get(1).get(1));
        assertNull(got.get(1).get(2));
        assertNull(got.get(2).get(0));
        assertEquals(new BigDecimal("0.0000000001"), got.get(2).get(1));
    }

    /** A placeholder SOURCE on a boundary is the wire's native zero Source, not an empty CollectionSource posing as one. */
    @Test
    void placeholderSourceHasZeroInputPayloadAndRoundTrips() {
        PlanNode placeholder = PlanNode.placeholderSource(META);
        assertNotNull(placeholder.inputs());
        assertTrue(placeholder.inputs().isEmpty(), "a placeholder source carries no empty CollectionSource");

        byte[] placeholderWire = new PlanGraph(Arrays.asList(placeholder)).toBytes();
        byte[] emptyCollectionWire = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()))).toBytes();
        assertTrue(placeholderWire.length < emptyCollectionWire.length,
                "the wire for a placeholder with zero sources carries no empty CollectionSource blob");

        PlanGraph back = PlanGraph.fromBytes(placeholderWire);
        assertNotNull(back.node(0).inputs());
        assertTrue(back.node(0).inputs().isEmpty(), "a wire with inputCount=0 round-trips");

        List<DataRow> emitted = new ArrayList<>();
        new PushDagExecutor().execute(back, emitted::add);
        assertTrue(emitted.isEmpty(), "an unbound placeholder source ends as an empty stream");
    }

    /** A SINK instance round-trips through Java serialization on its Sink. */
    @Test
    void sinkOutputRoundTrip() {
        Sink out = new CollectingSink(META);
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode sink = PlanNode.sink(META, 0, out).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, sink));

        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        PlanNode backSink = back.node(1);

        assertEquals(PlanNode.Kind.SINK, backSink.kind());
        assertNotNull(backSink.output());
        assertTrue(backSink.output() instanceof CollectingSink);
        assertArrayEquals(new int[]{0}, backSink.childIndices());
    }

    /** UNARY, BINARY and SINK nodes are built with the right child indices, and params and listParams round-trip. */
    @Test
    void binaryAndListParamsRoundTrip() {
        PlanNode left = PlanNode.source(META, new ArrayList<>());
        PlanNode right = PlanNode.source(META, new ArrayList<>());
        PlanNode join = PlanNode.binary("join", META, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("id"))
                .listParam("rightKeys", Arrays.asList("id"))
                .build();
        PlanGraph g = new PlanGraph(Arrays.asList(left, right, join));

        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        PlanNode bj = back.node(2);

        assertEquals(PlanNode.Kind.BINARY, bj.kind());
        assertEquals("join", bj.typeId());
        assertEquals("INNER", bj.param("joinType"));
        assertArrayEquals(new int[]{0, 1}, bj.childIndices());
        assertEquals(Arrays.asList("id"), bj.listParam("leftKeys"));
        assertEquals(Arrays.asList("id"), bj.listParam("rightKeys"));
    }

    /** DAG analysis: isPurePipeline, isLinearSegment, singleSourceIndex, subGraphRootedAt and withNodeRemoved. */
    @Test
    void dagAnalysis() {
        // source -> filter -> select: a pure pipeline
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).build();
        PlanNode select = PlanNode.unary("select", META, 1).build();
        PlanGraph pure = new PlanGraph(Arrays.asList(src, filter, select));
        assertTrue(pure.isPurePipeline());
        assertTrue(pure.isLinearSegment());
        assertEquals(0, pure.singleSourceIndex());

        // with a breaker: a linear segment, but not a pure pipeline
        PlanNode src2 = PlanNode.source(META, new ArrayList<>());
        PlanNode orderBy = PlanNode.unary("orderBy", META, 0).build();
        PlanGraph linear = new PlanGraph(Arrays.asList(src2, orderBy));
        assertFalse(linear.isPurePipeline());
        assertTrue(linear.isLinearSegment());

        // withNodeRemoved: drop the middle UNARY filter and select reconnects to the source
        PlanGraph removed = pure.withNodeRemoved(1);
        assertEquals(2, removed.nodeCount());
        assertEquals(PlanNode.Kind.SOURCE, removed.node(0).kind());
        assertEquals("select", removed.node(1).typeId());
        assertArrayEquals(new int[]{0}, removed.node(1).childIndices());

        // subGraphRootedAt: the subgraph rooted at filter (index 1) is {source, filter}
        PlanGraph sub = pure.subGraphRootedAt(1);
        assertEquals(2, sub.nodeCount());
        assertEquals("filter", sub.root().typeId());
    }

    // ===== job parallelism P is a PlanGraph property: it travels with the graph and is decoupled from worker slots =====

    /** P defaults to 1 in the older constructor, and the overload carrying P stores it. */
    @Test
    void parallelismDefaultAndOverload() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        assertEquals(1, new PlanGraph(Arrays.asList(src)).parallelism(), "the constructor without parallelism defaults to 1");
        assertEquals(4, new PlanGraph(Arrays.asList(src), 4).parallelism(), "the overload with parallelism keeps it");
    }

    /** P round-trips at the end of the frame through toBytes/fromBytes; a frame without the P field falls back to the default of 1, which keeps older plans readable. */
    @Test
    void parallelismWireRoundTripAndBackCompat() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter), 8);
        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        assertEquals(8, back.parallelism(), "the parallelism round-trips at the end of the frame");
        // a graph whose P is the default 1 round-trips as 1, without being misread
        assertEquals(1, PlanGraph.fromBytes(new PlanGraph(Arrays.asList(src, filter)).toBytes()).parallelism());
    }

    /** Every method that derives a PlanGraph has to carry P through: miss one and that path collapses to parallelism 1. */
    @Test
    void parallelismPropagatesThroughAllDerivations() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).build();
        PlanNode select = PlanNode.unary("select", META, 1).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter, select), 6);

        assertEquals(6, g.subGraphRootedAt(2).parallelism(), "subGraphRootedAt carries the parallelism through");
        assertEquals(6, g.withNodeRemoved(1).parallelism(), "withNodeRemoved carries the parallelism through");
        assertEquals(6, g.withNodeReplaced(1,
                PlanNode.unary("select", META, 0).build()).parallelism(), "withNodeReplaced carries the parallelism through");
        assertEquals(6, g.replaceSourceInputs(0, Collections.emptyList()).parallelism(),
                "replaceSourceInputs carries the parallelism through");
    }

    /** toJson and toDot emit the right structure: nodes plus edges. */
    @Test
    void toJsonAndToDot() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter));

        String json = g.toJson();
        assertTrue(json.contains("\"nodes\""));
        assertTrue(json.contains("\"edges\""));
        assertTrue(json.contains("\"typeId\":\"filter\""));
        assertTrue(json.contains("\"from\":0,\"to\":1"));

        String dot = g.toDot();
        assertTrue(dot.startsWith("digraph plan {"));
        assertTrue(dot.contains("n0 -> n1;"));
    }

    /** An empty graph is rejected, and a SINK written as a legacy type string still round-trips through the fallback. */
    @Test
    void emptyGraphRejectedAndSinkTypeFallback() {
        assertThrows(IllegalArgumentException.class, () -> new PlanGraph(new ArrayList<>()));

        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode sink = PlanNode.sink(META, 0, PlanNode.OUTPUT_COLLECTING).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, sink));
        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        assertEquals(PlanNode.Kind.SINK, back.node(1).kind());
        assertEquals(PlanNode.OUTPUT_COLLECTING, back.node(1).param(PlanNode.OUTPUT_TYPE_PARAM));
    }

    /**
     * A UNARY userFunction (-3 trailer) between a SOURCE (a non-negative trailer) and a SINK (-2)
     * round-trips, which shows the stream stays aligned: the reader's -3 branch consumes the blob,
     * so later nodes do not desynchronize.
     */
    @Test
    void userFunctionTrailerInterleavedRoundTrip() {
        io.rowweave.FilterFunction fn = (java.io.Serializable & io.rowweave.FilterFunction)
                r -> ((Integer) r.get(0)) > 5;
        PlanNode src = PlanNode.source(META, new ArrayList<>());          // trailer ≥0(inputCount)
        PlanNode filterFn = PlanNode.unaryFn("filterFn", META, 0, fn);    // trailer -3(userFunction)
        PlanNode sink = PlanNode.sink(META, 1, new CollectingSink(META)).build(); // trailer -2(Sink)
        PlanGraph g = new PlanGraph(Arrays.asList(src, filterFn, sink));

        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        assertEquals(3, back.nodeCount(), "the stream does not desync and all 3 nodes are restored");
        assertEquals(PlanNode.Kind.SOURCE, back.node(0).kind());
        assertEquals("filterFn", back.node(1).typeId());
        assertNotNull(back.node(1).userFunction(), "the user function survives serialization");
        assertArrayEquals(new int[]{0}, back.node(1).childIndices());
        assertEquals(PlanNode.Kind.SINK, back.node(2).kind());
        assertArrayEquals(new int[]{1}, back.node(2).childIndices(), "the SINK child index is not shifted by the -3 blob");
    }
}
