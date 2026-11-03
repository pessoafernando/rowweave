package io.rowweave.ops.join;

import io.rowweave.WorkerCollectingOutput;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An explicit join hint stays faithful from the public API through PlanGraph to the executor.
 */
class HashJoinHintTest {

    private static final DataSchema L = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema R = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema OUT = SortMergeJoin.of(
            L, R, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER).getOutputSchema();

    private static Source input(DataSchema meta, List<DataRow> rows) {
        return new Source() {
            public DataSchema getSchema() { return meta; }
            public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    private static PlanNode src(DataSchema meta, List<DataRow> rows) {
        return PlanNode.sourceFromInputs(meta, java.util.Collections.singletonList(input(meta, rows)));
    }

    private static Map<String, Integer> runJoin(List<DataRow> left, List<DataRow> right, String hint) {
        return runJoin(left, right, hint, 1);
    }

    private static Map<String, Integer> runJoin(
            List<DataRow> left, List<DataRow> right, String hint, int parallelism) {
        PlanNode.Builder jb = PlanNode.binary("join", OUT, 0, 1).param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("lk"))
                .listParam("rightKeys", Arrays.asList("rk"));
        if (hint != null) jb = jb.param("joinHint", hint);
        WorkerCollectingOutput out = new WorkerCollectingOutput(OUT);
        PlanGraph g = new PlanGraph(Arrays.asList(
                src(L, left), src(R, right), jb.build(), PlanNode.sink(OUT, 2, out).build()));
        new PushDagExecutor(parallelism).runSinkGraph(g);
        Map<String, Integer> m = new HashMap<>();
        for (DataRow r : out.getRows()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < r.size(); i++) sb.append(r.get(i)).append('|');
            m.merge(sb.toString(), 1, Integer::sum);
        }
        return m;
    }

    private static Map<String, Integer> runMasterJoin(
            List<DataRow> left, List<DataRow> right, String hint, int parallelism) {
        PlanNode.Builder builder = PlanNode.binary("join", OUT, 0, 1).param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("lk"))
                .listParam("rightKeys", Arrays.asList("rk"));
        if (hint != null) builder.param("joinHint", hint);
        WorkerCollectingOutput output = new WorkerCollectingOutput(OUT);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                src(L, left), src(R, right), builder.build(), PlanNode.sink(OUT, 2, output).build()));
        new io.rowweave.cluster.Master(parallelism).runBinaryKeyed(graph);
        return multiset(output.getRows());
    }

    private static Map<String, Integer> multiset(List<DataRow> rows) {
        Map<String, Integer> result = new HashMap<>();
        for (DataRow row : rows) {
            StringBuilder fingerprint = new StringBuilder();
            for (int i = 0; i < row.size(); i++) fingerprint.append(row.get(i)).append('|');
            result.merge(fingerprint.toString(), 1, Integer::sum);
        }
        return result;
    }

    @Test
    void explicitSortMergeHintSurvivesPublicApiPlanCompilation() {
        SortMergeJoin op = SortMergeJoin.of(
                L, R, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        io.rowweave.BatchSession session = io.rowweave.RowWeave.createBatchSession("explicit-smj-plan");
        io.rowweave.DataSet joined = session.fromSource(input(L, new ArrayList<>()))
                .join(session.fromSource(input(R, new ArrayList<>())), op, JoinHint.SORT_MERGE);
        joined.sinkTo(new io.rowweave.WorkerCollectingOutput(op.getOutputSchema()));

        PlanGraph graph = PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        PlanNode join = graph.nodes().stream()
                .filter(node -> "join".equals(node.typeId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("compiled graph must contain join node"));

        assertEquals("SORT_MERGE", join.param("joinHint"),
                "explicit SORT_MERGE must not degrade into local AUTO during plan compilation");
    }

    @Test
    void dataSetJoinHintApiMatchesSmj() {
        // DataSet.join(right, op, HASH) local-compatibility API, committed through BatchSession and multiset-equivalent to the unhinted path.
        java.util.List<DataRow> ld = new ArrayList<>(), rd = new ArrayList<>();
        for (int i = 0; i < 100; i++) { DataRow r = new DataRow(2); r.set(0, i % 20); r.set(1, "l" + i); ld.add(r); }
        for (int i = 0; i < 25; i++) { DataRow r = new DataRow(2); r.set(0, i % 20); r.set(1, "r" + i); rd.add(r); }
        SortMergeJoin op = SortMergeJoin.of(L, R, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);

        // no hint: local AUTO
        io.rowweave.BatchSession s1 = io.rowweave.RowWeave.createBatchSession("smj");
        io.rowweave.WorkerCollectingOutput o1 = new io.rowweave.WorkerCollectingOutput(op.getOutputSchema());
        s1.fromSource(input(L, new ArrayList<>(ld))).join(s1.fromSource(input(R, new ArrayList<>(rd))), op).sinkTo(o1);
        s1.execute();
        // HASH hint
        io.rowweave.BatchSession s2 = io.rowweave.RowWeave.createBatchSession("hash");
        io.rowweave.WorkerCollectingOutput o2 = new io.rowweave.WorkerCollectingOutput(op.getOutputSchema());
        s2.fromSource(input(L, new ArrayList<>(ld)))
                .join(s2.fromSource(input(R, new ArrayList<>(rd))), op, JoinHint.HASH).sinkTo(o2);
        s2.execute();

        Map<String, Integer> m1 = new HashMap<>(), m2 = new HashMap<>();
        for (DataRow r : o1.getRows()) { StringBuilder sb = new StringBuilder(); for (int i = 0; i < r.size(); i++) sb.append(r.get(i)).append('|'); m1.merge(sb.toString(), 1, Integer::sum); }
        for (DataRow r : o2.getRows()) { StringBuilder sb = new StringBuilder(); for (int i = 0; i < r.size(); i++) sb.append(r.get(i)).append('|'); m2.merge(sb.toString(), 1, Integer::sum); }
        assertEquals(m1, m2, "join with the HASH hint gives the same multiset as the local AUTO choice with no hint");
    }

    @Test
    void hashHintRoutesToHashJoinAndMatchesSmj() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 200; i++) { DataRow r = new DataRow(2); r.set(0, i % 30); r.set(1, "l" + i); left.add(r); }
        for (int i = 0; i < 40; i++) { DataRow r = new DataRow(2); r.set(0, i % 30); r.set(1, "r" + i); right.add(r); }
        Map<String, Integer> smj = runJoin(left, right, "SORT_MERGE");
        Map<String, Integer> hash = runJoin(left, right, "HASH"); // HASH hint
        assertEquals(smj, hash, "the HASH hint routes through binaryStageFor to the hash join and the result matches the sort-merge join");
    }

    @Test
    void p1P2P4PreserveHashSortMergeAndAutoSemantics() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            DataRow row = new DataRow(2); row.set(0, i % 37); row.set(1, "l" + i); left.add(row);
        }
        for (int i = 0; i < 80; i++) {
            DataRow row = new DataRow(2); row.set(0, i % 37); row.set(1, "r" + i); right.add(row);
        }
        Map<String, Integer> expected = runJoin(left, right, "SORT_MERGE", 1);

        for (int parallelism : new int[]{1, 2, 4}) {
            assertEquals(expected, runJoin(left, right, "HASH", parallelism),
                    "HASH P=" + parallelism);
            assertEquals(expected, runJoin(left, right, "SORT_MERGE", parallelism),
                    "SORT_MERGE P=" + parallelism);
            assertEquals(expected, runJoin(left, right, null, parallelism),
                    "AUTO P=" + parallelism);
        }
    }

    @Test
    void compatibilityMasterUsesTheSameThreeStrategies() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            DataRow row = new DataRow(2); row.set(0, i % 19); row.set(1, "l" + i); left.add(row);
        }
        for (int i = 0; i < 35; i++) {
            DataRow row = new DataRow(2); row.set(0, i % 19); row.set(1, "r" + i); right.add(row);
        }
        Map<String, Integer> expected = runJoin(left, right, "SORT_MERGE", 1);

        assertEquals(expected, runMasterJoin(left, right, "HASH", 2));
        assertEquals(expected, runMasterJoin(left, right, "SORT_MERGE", 2));
        assertEquals(expected, runMasterJoin(left, right, null, 2));
    }
}
