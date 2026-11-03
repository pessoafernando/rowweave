package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterPushTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));
    private static final DataSchema ID_META = new DataSchema(new Field("id", DataType.IntegerType));

    private static DataRow row(Integer id, String grp, BigDecimal amt) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, grp);
        r.set(2, amt);
        return r;
    }

    @Test
    void executeToOutput() {
        List<DataRow> rows = Arrays.asList(
                row(10, "a", new BigDecimal("100")), row(1, "b", new BigDecimal("50")));
        PlanNode src = PlanNode.source(META, rows);
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter));

        CollectingSink out = new CollectingSink(META);
        new PushDagExecutor().executeToOutput(g, out);
        assertEquals(1, out.getRows().size());
        assertEquals(10, out.getRows().get(0).get(0));
    }

    @Test
    void stageBoundaryRebuild() {
        io.rowweave.ops.ParallelGroupByAgg agg =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").sum("amt");
        DataSchema gbMeta = agg.getOutputSchema();
        PlanNode src = PlanNode.source(META, new ArrayList<>())
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withParam("connector.mode", "snapshot");
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, new CollectingSink(gbMeta)).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, sink));

        ExecutionGraph eg = ExecutionGraphBuilder.build(g);
        assertEquals(2, eg.stageCount());
        Stage sinkStage = eg.rootStage();
        assertTrue(sinkStage.isSink());

        StagePlan sinkPlan = StageBoundaryRebuilder.rebuild(g, eg, sinkStage);
        assertTrue(sinkPlan.isSink());
        assertEquals(1, sinkPlan.boundaries().size());
        assertEquals(0, sinkPlan.boundaries().get(0).upstreamStage());

        StagePlan sourcePlan = StageBoundaryRebuilder.rebuild(g, eg, eg.stage(0));
        assertEquals("ledger-source", sourcePlan.plan().node(0).param(PlanNode.OPERATOR_UID_PARAM));
        assertEquals("snapshot", sourcePlan.plan().node(0).param("connector.mode"));
    }

    @Test
    void executionGraphBuilderLinear() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 0").build();
        PlanNode select = PlanNode.unary("select", ID_META, 1)
                .listParam("exprs", Arrays.asList("id")).build();
        PlanNode sink = PlanNode.sink(ID_META, 2, new CollectingSink(ID_META)).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter, select, sink));

        ExecutionGraph eg = ExecutionGraphBuilder.build(g);
        assertEquals(1, eg.stageCount());
        assertTrue(eg.rootStage().isSink());
        assertEquals(4, eg.rootStage().members().size());
    }

    @Test
    void localWorkerBoundSource() throws Exception {
        // consumer plan: placeholder-source(idx0) -> filter -> root
        PlanNode ph = PlanNode.source(META, Collections.emptyList());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        PlanGraph consumerGraph = new PlanGraph(Arrays.asList(ph, filter));
        StagePlan consumerPlan = new StagePlan(1, consumerGraph,
                Collections.singletonList(new StageBoundary(0, 0)), false);

        io.rowweave.sched.LocalChannel inbound = new io.rowweave.sched.LocalChannel(64);
        io.rowweave.sched.LocalChannel outbound = new io.rowweave.sched.LocalChannel(64);

        Thread producer = new Thread(() -> {
            inbound.send(io.rowweave.sched.stream.StreamEvent.record(row(10, "a", null)));
            inbound.send(io.rowweave.sched.stream.StreamEvent.record(row(1, "b", null)));
            inbound.close();
        });
        Thread consumer = new Thread(() -> {
            java.util.Map<Integer, io.rowweave.sched.Channel> in = new java.util.HashMap<>();
            in.put(0, inbound);
            io.rowweave.mem.ExecutionMemory mem = io.rowweave.mem.ExecutionMemory.forJob(
                    io.rowweave.mem.ManagedMemory.ofBytes(1024 * 1024), 0); // pass-through,0 breaker
            try {
                new LocalWorker(1, mem).runIntermediate(consumerPlan, in, outbound);
            } finally {
                mem.close();
            }
            outbound.close();
        });
        producer.start();
        consumer.start();

        List<DataRow> got = new ArrayList<>();
        io.rowweave.sched.stream.StreamEvent e;
        while ((e = outbound.poll()) != null) {
            if (e.isRecord()) got.add(e.row());
        }
        producer.join();
        consumer.join();
        assertEquals(1, got.size()); // id>5: only 10
        assertEquals(10, got.get(0).get(0));
    }

    @Test
    void e2eLinearSubmitToSink() {
        List<DataRow> rows = Arrays.asList(
                row(10, "a", new BigDecimal("100")),
                row(1, "b", new BigDecimal("50")),
                row(8, "a", new BigDecimal("80")));
        CollectingSink out = new CollectingSink(ID_META);
        PlanNode src = PlanNode.source(META, rows);
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        PlanNode select = PlanNode.unary("select", ID_META, 1)
                .listParam("exprs", Arrays.asList("id")).build();
        PlanNode sink = PlanNode.sink(ID_META, 2, out).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter, select, sink));

        long n = new LocalCluster().submitToSink(g);
        assertEquals(2, n); // 10>5, 8>5
        assertEquals(2, out.getRows().size());
        assertEquals(1, out.getRows().get(0).size(), "select(id) must carry a runtime row width matching the single-column DataSchema");

        PlanGraph p82 = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("filter", META, 0).param("expr", "id > 5").build(),
                PlanNode.unary("select", ID_META, 1).listParam("exprs", Arrays.asList("id")).build()));
        List<DataRow> single = new ArrayList<>();
        new PushDagExecutor().execute(p82, single::add);
        assertEquals(single.size(), out.getRows().size());
    }

    @Test
    void e2eCrossStageGroupBy() {
        io.rowweave.ops.ParallelGroupByAgg agg =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").sum("amt");
        DataSchema gbMeta = agg.getOutputSchema();
        List<DataRow> rows = Arrays.asList(
                row(1, "a", new BigDecimal("10.5")),
                row(2, "b", new BigDecimal("20")),
                row(3, "a", new BigDecimal("4.5")));
        CollectingSink out = new CollectingSink(gbMeta);
        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, out).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, sink));

        long n = new LocalCluster().submitToSink(g);
        assertEquals(2, n);
        BigDecimal sumA = null;
        for (DataRow r : out.getRows()) {
            if ("a".equals(r.get(0))) sumA = (BigDecimal) r.get(1);
        }
        assertEquals(0, new BigDecimal("15.0000000000").compareTo(sumA));
    }

    @Test
    void e2eSinkStageMultiMemberDownstreamOfBoundary() {
        io.rowweave.ops.ParallelGroupByAgg agg =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").sum("amt");
        DataSchema gbMeta = agg.getOutputSchema(); // [grp, sum0]
        io.rowweave.ops.Select sel = io.rowweave.ops.Select.byIndex(gbMeta, 0);
        DataSchema selMeta = sel.getOutputSchema();
        List<DataRow> rows = Arrays.asList(
                row(1, "a", new BigDecimal("10")),
                row(2, "b", new BigDecimal("20")),
                row(3, "a", new BigDecimal("5")));
        CollectingSink out = new CollectingSink(selMeta);
        PlanNode src = PlanNode.source(META, rows);                                  // idx 0
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)                        // idx 1
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode select = PlanNode.unary("select", selMeta, 1)                       // idx 2
                .listParam("exprs", Arrays.asList("grp")).build();
        PlanNode sink = PlanNode.sink(selMeta, 2, out).build();                      // idx 3
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, select, sink));

        long n = new LocalCluster().submitToSink(g);
        assertEquals(2, n); // 2 groups: a, b
        assertEquals(1, out.getRows().get(0).size()); // only grp column
    }

    @Test
    void parallelismMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new LocalCluster(0));
        assertThrows(IllegalArgumentException.class, () -> new Master(-1));
    }

    @Test
    void failFastNoDeadlock() {
        Source boom = new Source() {
            @Override
            public DataSchema getSchema() {
                return META;
            }

            @Override
            public java.util.Iterator<DataRow> iterator() {
                return new java.util.Iterator<DataRow>() {
                    private int n = 0;
                    @Override public boolean hasNext() { return n++ < 1; }
                    @Override public DataRow next() { throw new RuntimeException("source boom"); }
                };
            }
        };
        io.rowweave.ops.ParallelGroupByAgg agg =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").sum("amt");
        DataSchema gbMeta = agg.getOutputSchema();
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(boom));
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, new CollectingSink(gbMeta)).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, sink));

        assertThrows(RuntimeException.class, () -> new LocalCluster().submitToSink(g));
    }

    @Test
    void ironRuleNoListRowDataCarrier() throws Exception {
        java.nio.file.Path dir = java.nio.file.Paths.get("src/main/java/io/rowweave/cluster");
        List<String> offenders = new ArrayList<>();
        java.nio.file.Files.walk(dir)
                .filter(p -> p.toString().endsWith(".java"))
                .forEach(p -> {
                    try {
                        for (String line : java.nio.file.Files.readAllLines(p)) {
                            String t = line.trim();
                            if (t.startsWith("//") || t.startsWith("*")) continue;
                            if (line.contains("List<DataRow>") || line.contains("ArrayList<DataRow>")) {
                                offenders.add(p.getFileName() + ": " + t);
                            }
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
        assertTrue(offenders.isEmpty(), "cluster src/main must not carry List<DataRow>: " + offenders);
    }
}

