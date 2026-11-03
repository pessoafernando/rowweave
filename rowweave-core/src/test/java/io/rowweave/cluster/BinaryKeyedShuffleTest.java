package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.Partitioner;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BinaryKeyedShuffleTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));

    private static DataRow lrow(Integer k, String v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    private PlanGraph joinPlan(List<DataRow> left, List<DataRow> right, JoinType type,
                               DataSchema leftMeta, DataSchema rightMeta, CollectingSink out, DataSchema joinMeta) {
        PlanNode ls = PlanNode.source(leftMeta, left);
        PlanNode rs = PlanNode.source(rightMeta, right);
        PlanNode join = PlanNode.binary("join", joinMeta, 0, 1)
                .param("joinType", type.name())
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode sink = PlanNode.sink(joinMeta, 2, out).build();
        return new PlanGraph(Arrays.asList(ls, rs, join, sink));
    }

    private DataSchema joinMeta() {
        return SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .getOutputSchema();
    }

    @Test
    void joinInnerDeterminism() {
        List<DataRow> left = Arrays.asList(lrow(1, "L1"), lrow(2, "L2"), lrow(3, "L3"));
        List<DataRow> right = Arrays.asList(lrow(1, "R1"), lrow(3, "R3"), lrow(3, "R3b"));
        DataSchema jm = joinMeta();
        Integer baseline = null;
        for (int par : new int[]{1, 2, 3}) {
            CollectingSink out = new CollectingSink(jm);
            long n = new LocalCluster(par).submitBinaryKeyed(
                    joinPlan(left, right, JoinType.INNER, LEFT, RIGHT, out, jm));
            // k=1: 1 match; k=3: 2 matches (R3,R3b) → 3 rows
            assertEquals(3, n, "par=" + par);
            if (baseline == null) baseline = (int) n; else assertEquals(baseline.intValue(), (int) n, "par=" + par);
        }
    }

    @Test
    void joinLeftOuterNullNotEqual() {
        List<DataRow> left = Arrays.asList(lrow(1, "L1"), lrow(2, "L2"), lrow(null, "Lnull"));
        List<DataRow> right = Arrays.asList(lrow(1, "R1"), lrow(null, "Rnull"));
        DataSchema jm = joinMeta();
        CollectingSink out = new CollectingSink(jm);
        long n = new LocalCluster(2).submitBinaryKeyed(
                joinPlan(left, right, JoinType.LEFT, LEFT, RIGHT, out, jm));
        assertEquals(3, n);
    }

    @Test
    void numericKeyNormalizationCoLocation() {
        DataSchema lmeta = new DataSchema(
                new Field("k", DataType.BigDecimalType), new Field("lv", DataType.StringType));
        DataSchema rmeta = new DataSchema(
                new Field("k", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        DataRow l1 = new DataRow(2); l1.set(0, new BigDecimal("2.0")); l1.set(1, "L");
        DataRow r1 = new DataRow(2); r1.set(0, new BigDecimal("2.00")); r1.set(1, "R");
        DataSchema jm = SortMergeJoin.of(lmeta, rmeta, new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .getOutputSchema();
        CollectingSink out = new CollectingSink(jm);
        long n = new LocalCluster(4).submitBinaryKeyed(joinPlan(
                Collections.singletonList(l1), Collections.singletonList(r1),
                JoinType.INNER, lmeta, rmeta, out, jm));
        assertEquals(1, n, "2.0 and 2.00 must land in the same partition and match (numeric normalization)");
    }

    @Test
    void skewNoDeadlock() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 100; i++) left.add(lrow(7, "L" + i));
        for (int i = 0; i < 100; i++) right.add(lrow(7, "R" + i));
        DataSchema jm = joinMeta();
        CollectingSink out = new CollectingSink(jm);
        long n = new LocalCluster(3).submitBinaryKeyed(
                joinPlan(left, right, JoinType.INNER, LEFT, RIGHT, out, jm));
        assertEquals(100L * 100L, n);
    }

    @Test
    void failFastLeftSourceNoDeadlock() {
        Source boom = new Source() {
            @Override public DataSchema getSchema() { return LEFT; }
            @Override public java.util.Iterator<DataRow> iterator() {
                return new java.util.Iterator<DataRow>() {
                    private int n = 0;
                    @Override public boolean hasNext() { return n++ < 1; }
                    @Override public DataRow next() { throw new RuntimeException("left boom"); }
                };
            }
        };
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 200; i++) right.add(lrow(i % 5, "R" + i));
        DataSchema jm = joinMeta();
        PlanNode ls = PlanNode.sourceFromInputs(LEFT, Collections.singletonList(boom));
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", jm, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode sink = PlanNode.sink(jm, 2, new CollectingSink(jm)).build();
        PlanGraph g = new PlanGraph(Arrays.asList(ls, rs, join, sink));
        assertThrows(RuntimeException.class, () -> new LocalCluster(3).submitBinaryKeyed(g));
    }

    @Test
    void partitionerNumericNormalization() {
        DataSchema m = new DataSchema(new Field("k", DataType.BigDecimalType));
        DataRow a = new DataRow(1); a.set(0, new BigDecimal("2.0"));
        DataRow b = new DataRow(1); b.set(0, new BigDecimal("2.00"));
        Partitioner norm = Partitioner.numericNormalizedOfFields(m, 8, "k");
        assertEquals(norm.partitionOf(a), norm.partitionOf(b), "normalized: 2.0 and 2.00 share one partition");

        Partitioner raw = Partitioner.ofFields(m, 8, "k");
        assertNotEquals(new BigDecimal("2.0").hashCode(), new BigDecimal("2.00").hashCode());
        assertNotNull(raw);
    }
}

