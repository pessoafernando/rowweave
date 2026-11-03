package io.rowweave;

import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.ops.Union;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DataSetBinaryTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    private static DataRow l(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    private static DataRow r(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    private static List<DataRow> rows(DataRow... rs) {
        return new ArrayList<>(Arrays.asList(rs));
    }

    private ExecutionEnvironment session;

    private static Source input(DataSchema meta, List<DataRow> rs) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public java.util.Iterator<DataRow> iterator() { return rs.iterator(); }
        };
    }

    /** The left source is bound to a session: resolveSession walks the left side, and a binary needs only its left input to carry the session. */
    private DataSet lsrc(DataSchema meta, List<DataRow> rs) {
        session = RowWeave.createEnvironment("t");
        return session.fromSource(input(meta, rs));
    }

    /** The right source is bound to the same session, since toPlanGraph walks left and right and both SOURCE nodes have to be rebuildable. */
    private DataSet rsrc(DataSchema meta, List<DataRow> rs) {
        return session.fromSource(input(meta, rs));
    }

    private List<DataRow> run(DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return out.getRows();
    }

    private static Set<String> render4(List<DataRow> rows) {
        Set<String> s = new HashSet<>();
        for (DataRow r : rows) {
            s.add(cell(r.get(0)) + ":" + cell(r.get(1)) + "|" + cell(r.get(2)) + ":" + cell(r.get(3)));
        }
        return s;
    }

    private static String cell(Object o) {
        return o == null ? "N" : o.toString();
    }

    @Test
    void joinInnerEndToEnd() {
        DataSet left = lsrc(LEFT, rows(l(1, "a"), l(2, "b"), l(3, "c")));
        DataSet right = rsrc(RIGHT, rows(r(2, "x"), r(3, "y"), r(4, "z")));
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        Set<String> out = render4(run(left.join(right, smj)));
        assertEquals(new HashSet<>(Arrays.asList("2:b|2:x", "3:c|3:y")), out);
    }

    @Test
    void joinLeftOuterNullFilled() {
        DataSet left = lsrc(LEFT, rows(l(1, "a"), l(2, "b")));
        DataSet right = rsrc(RIGHT, rows(r(2, "x")));
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.LEFT);
        Set<String> out = render4(run(left.join(right, smj)));
        assertEquals(new HashSet<>(Arrays.asList("1:a|N:N", "2:b|2:x")), out);
    }

    @Test
    void joinNullKeyNeverMatchesEndToEnd() {
        DataSet left = lsrc(LEFT, rows(l(null, "a"), l(1, "b")));
        DataSet right = rsrc(RIGHT, rows(r(null, "x"), r(1, "y")));
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        Set<String> out = render4(run(left.join(right, smj)));
        assertEquals(new HashSet<>(Arrays.asList("1:b|1:y")), out); // null != null
    }

    @Test
    void coGroupEndToEnd() {
        DataSchema outMeta = new DataSchema(new Field("k", DataType.IntegerType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroup cg = CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"},
                new CountingCoGroupFunction(outMeta));
        DataSet left = lsrc(LEFT, rows(l(1, "a"), l(1, "b"), l(2, "c")));
        DataSet right = rsrc(RIGHT, rows(r(1, "x")));
        List<DataRow> out = run(left.coGroup(right, cg));
        // key1: lc2 rc1 ; key2: lc1 rc0
        assertEquals(2, out.size());
    }

    /** The Function API requires serializability, so a static fixture keeps an anonymous inner class from capturing the test instance. */
    private static final class CountingCoGroupFunction implements CoGroupFunction {
        private static final long serialVersionUID = 1L;
        private final DataSchema outMeta;

        private CountingCoGroupFunction(DataSchema outMeta) {
            this.outMeta = outMeta;
        }

        @Override public DataSchema getResultSchema() { return outMeta; }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
            int lc = 0;
            Object k = null;
            for (DataRow row : a) { lc++; k = row.get(0); }
            int rc = 0;
            for (DataRow row : b) { rc++; if (k == null) k = row.get(0); }
            DataRow output = new DataRow(3);
            output.set(0, k);
            output.set(1, lc);
            output.set(2, rc);
            c.collect(output);
        }
    }

    @Test
    void unionEndToEnd() {
        DataSet a = lsrc(LEFT, rows(l(1, "a"), l(2, "b")));
        DataSet b = rsrc(LEFT, rows(l(3, "c")));  // the same session, as rsrc reuses the session of a
        Union u = Union.of(LEFT);
        List<DataRow> out = run(a.union(b, u));
        assertEquals(3, out.size()); // concat
    }

    @Test
    void subChainsWithPassThrough() {
        // (source -> filter) join (source -> nothing)
        DataSet left = lsrc(LEFT, rows(l(1, "a"), l(2, "b"), l(3, "c")))
                .filter(row -> (Integer) row.get(0) >= 2); // {2,3}
        DataSet right = rsrc(RIGHT, rows(r(1, "x"), r(2, "y"), r(3, "z")));
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        assertEquals(2, run(left.join(right, smj)).size()); // 2,3 match
    }

    @Test
    void downstreamAfterBinary() {
        DataSet left = lsrc(LEFT, rows(l(1, "a"), l(2, "b")));
        DataSet right = rsrc(RIGHT, rows(r(1, "x"), r(2, "y")));
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        DataSet joined = left.join(right, smj)
                .filter(row -> Integer.valueOf(1).equals(row.get(0))); // keep lk==1
        assertEquals(1, run(joined).size());
    }

    /** A recursive binary dag: a nested binary, where the left subtree of a join is itself a join, is supported and produces the right rows. */
    @Test
    void nestedBinarySupported() {
        DataSet a = lsrc(LEFT, rows(l(1, "a")));
        DataSet b = rsrc(RIGHT, rows(r(1, "x")));
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        DataSet joined = a.join(b, smj);  // BINARY: emits [lk,lv,rk,rv], one row, since lk=1 matches rk=1
        // joining once more nests a binary, since the left subtree already holds a BINARY. The left output key is "lk" and equals 1, and the right rk is 1, so they match.
        DataSchema jm = joined.getSchema();
        SortMergeJoin smj2 = SortMergeJoin.of(jm, RIGHT,
                new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        DataSet nested = joined.join(rsrc(RIGHT, rows(r(1, "z"))), smj2);
        assertEquals(1, run(nested).size(), "nested binary (join->join) is supported and emits exactly 1 row");
    }
}
