package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KeyedShuffleTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(Integer id, String grp, BigDecimal amt) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, grp);
        r.set(2, amt);
        return r;
    }

    private PlanGraph groupBySumPlan(List<DataRow> rows, CollectingSink out, DataSchema gbMeta) {
        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, out).build();
        return new PlanGraph(Arrays.asList(src, gb, sink));
    }

    private static java.util.Map<String, BigDecimal> sumsByGrp(List<DataRow> rows) {
        java.util.Map<String, BigDecimal> m = new java.util.TreeMap<>();
        for (DataRow r : rows) {
            m.put((String) r.get(0), (BigDecimal) r.get(1));
        }
        return m;
    }

    @Test
    void keyedGroupByDeterminismAcrossParallelism() {
        List<DataRow> rows = Arrays.asList(
                row(1, "a", new BigDecimal("10.5")),
                row(2, "b", new BigDecimal("20")),
                row(3, "a", new BigDecimal("4.5")),
                row(4, "c", new BigDecimal("7")),
                row(5, "b", new BigDecimal("3")));
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();

        java.util.Map<String, BigDecimal> baseline = null;
        for (int par : new int[]{1, 2, 3}) {
            CollectingSink out = new CollectingSink(gbMeta);
            long n = new LocalCluster(par).submitKeyed(groupBySumPlan(rows, out, gbMeta));
            assertEquals(3, n, "par=" + par); // 3 groups a/b/c
            java.util.Map<String, BigDecimal> got = sumsByGrp(out.getRows());
            assertEquals(0, new BigDecimal("15.0000000000").compareTo(got.get("a")), "a par=" + par);
            assertEquals(0, new BigDecimal("23.0000000000").compareTo(got.get("b")), "b par=" + par);
            assertEquals(0, new BigDecimal("7.0000000000").compareTo(got.get("c")), "c par=" + par);
            if (baseline == null) {
                baseline = got;
            } else {
                assertEquals(baseline.keySet(), got.keySet(), "key set par=" + par);
            }
        }
    }

    @Test
    void keyedDistinctParallel() {
        List<DataRow> rows = Arrays.asList(
                row(1, "a", null), row(1, "a", null), row(2, "b", null),
                row(3, "c", null), row(2, "b", null));
        CollectingSink out = new CollectingSink(META);
        PlanNode src = PlanNode.source(META, rows);
        PlanNode d = PlanNode.unary("distinct", META, 0)
                .listParam("keys", Arrays.asList("grp")).build();
        PlanNode sink = PlanNode.sink(META, 1, out).build();
        long n = new LocalCluster(2).submitKeyed(new PlanGraph(Arrays.asList(src, d, sink)));
        assertEquals(3, n); // distinct grp: a/b/c
    }

    @Test
    void skewAllOneKeyNoDeadlock() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            rows.add(row(i, "same", new BigDecimal("1")));
        }
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        CollectingSink out = new CollectingSink(gbMeta);
        long n = new LocalCluster(3).submitKeyed(groupBySumPlan(rows, out, gbMeta));
        assertEquals(1, n);
        assertEquals(0, new BigDecimal("500.0000000000").compareTo((BigDecimal) out.getRows().get(0).get(1)));
    }

    @Test
    void failFastSourceErrorNoDeadlock() {
        Source boom = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public java.util.Iterator<DataRow> iterator() {
                return new java.util.Iterator<DataRow>() {
                    private int n = 0;
                    @Override public boolean hasNext() { return n++ < 1; }
                    @Override public DataRow next() { throw new RuntimeException("source boom"); }
                };
            }
        };
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(boom));
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, new CollectingSink(gbMeta)).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, sink));
        assertThrows(RuntimeException.class, () -> new LocalCluster(3).submitKeyed(g));
    }

    @Test
    void bigDecimalKeyPartitionConsistency() {
        DataSchema meta = new DataSchema(
                new Field("k", DataType.BigDecimalType),
                new Field("amt", DataType.BigDecimalType));
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            DataRow r = new DataRow(2);
            r.set(0, new BigDecimal("5.00"));
            r.set(1, new BigDecimal("1"));
            rows.add(r);
        }
        DataSchema gbMeta = ParallelGroupByAgg.of(meta, "k").sum("amt").getOutputSchema();
        CollectingSink out = new CollectingSink(gbMeta);
        PlanNode src = PlanNode.source(meta, rows);
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, out).build();
        long n = new LocalCluster(4).submitKeyed(new PlanGraph(Arrays.asList(src, gb, sink)));
        assertEquals(1, n, "keys with the same scale and value must form one group (the partition key and the grouping key come from the same source)");
        assertEquals(0, new BigDecimal("6.0000000000").compareTo((BigDecimal) out.getRows().get(0).get(1)));
    }

    @Test
    void failFastSinkErrorNoDeadlock() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(row(i, "g" + (i % 50), new BigDecimal("1")));
        }
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        Sink boomSink = new Sink() {
            @Override public DataSchema getSchema() { return gbMeta; }
            @Override public void collect(DataRow row) { throw new RuntimeException("sink boom"); }
        };
        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", gbMeta, 0)
                .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gbMeta, 1, boomSink).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, sink));
        assertThrows(RuntimeException.class, () -> new LocalCluster(3).submitKeyed(g));
    }

    @Test
    void sameKeyLandsSamePartition() {
        List<DataRow> rows = new ArrayList<>();
        for (int g = 0; g < 10; g++) {
            for (int k = 0; k < 3; k++) {
                rows.add(row(g * 3 + k, "g" + g, new BigDecimal("2")));
            }
        }
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        CollectingSink out = new CollectingSink(gbMeta);
        long n = new LocalCluster(4).submitKeyed(groupBySumPlan(rows, out, gbMeta));
        assertEquals(10, n);
        for (DataRow r : out.getRows()) {
            assertEquals(0, new BigDecimal("6.0000000000").compareTo((BigDecimal) r.get(1)));
        }
    }
}

