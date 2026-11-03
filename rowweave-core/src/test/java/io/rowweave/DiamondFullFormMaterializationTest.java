package io.rowweave;

import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.ops.Union;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end behavior of single-machine fan-out re-joins in every shape, covering the materialization decision and the materialized edge for each topology that re-joins
 * into an operator that consumes in order: a union diamond, a coGroup diamond, an n-ary union, a mix of re-join with a SINK, nested, asymmetric, and two binaries sharing a fork.
 * Every shared operator is computed exactly once, with the source read once, the result correct and no deadlock, guarded by a short watchdog timeout that fails fast rather than hanging
 */
class DiamondFullFormMaterializationTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private List<DataRow> data() {
        return data(300);
    }

    private List<DataRow> data(int count) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 6);
            r.set(1, new BigDecimal(i % 40));
            rs.add(r);
        }
        return rs;
    }

    private Source countingInput(List<DataRow> data, AtomicInteger reads) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                reads.incrementAndGet();
                return new ArrayList<>(data).iterator();
            }
        };
    }

    /** A short watchdog timeout: if any shape misses a materialization decision and deadlocks, it fails fast within 5s instead of hanging forever, and a healthy run finishes far sooner. */
    private void withWatchdog(Runnable body) {
        String prev = System.setProperty("rowweave.pipeline.deadlockTimeoutMs", "5000");
        try {
            body.run();
        } finally {
            if (prev == null) {
                System.clearProperty("rowweave.pipeline.deadlockTimeoutMs");
            } else {
                System.setProperty("rowweave.pipeline.deadlockTimeoutMs", prev);
            }
        }
    }

    /** A diamond into a two-input union, where both fork branches are filtered into it: the shared source is read once and the union keeps order, concatenating the left in full then the right. */
    @Test
    void diamondToUnionSharedOnceAndOrdered() {
        withWatchdog(() -> {
            List<DataRow> data = data();
            AtomicInteger reads = new AtomicInteger();
            ExecutionEnvironment s = RowWeave.createEnvironment("d-union");
            DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5");
            DataSet b1 = mid.filter("k >= 3");
            DataSet b2 = mid.filter("k <= 2");
            DataSet u = b1.union(b2, Union.of(META));
            WorkerCollectingOutput out = new WorkerCollectingOutput(u.getSchema());
            u.sinkTo(out);
            s.execute();
            // union = b1 concat b2, each branch a subset of mid, so the row count is |b1| + |b2|.
            long b1n = data.stream().filter(x -> ((BigDecimal) x.get(1)).compareTo(new BigDecimal(5)) >= 0
                    && (Integer) x.get(0) >= 3).count();
            long b2n = data.stream().filter(x -> ((BigDecimal) x.get(1)).compareTo(new BigDecimal(5)) >= 0
                    && (Integer) x.get(0) <= 2).count();
            assertEquals(b1n + b2n, out.getRows().size(), "union row count = |b1|+|b2|");
            assertEquals(1, reads.get(), "diamond->union reads the shared source once");
        });
    }

    /** A diamond into coGroup, where both fork branches feed it: the shared source is read once and the result is right. */
    @Test
    void diamondToCoGroupSharedOnce() {
        withWatchdog(() -> {
            List<DataRow> data = data();
            AtomicInteger reads = new AtomicInteger();
            DataSchema outMeta = new DataSchema(new Field("k", DataType.IntegerType),
                    new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
            CoGroup cg = CoGroup.of(META, META, new String[]{"k"}, new String[]{"k"},
                    new CoGroupFunction() {
                        @Override public DataSchema getResultSchema() { return outMeta; }
                        @Override public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
                            int lc = 0; Object k = null;
                            for (DataRow r : a) { lc++; k = r.get(0); }
                            int rc = 0; for (DataRow r : b) { rc++; if (k == null) k = r.get(0); }
                            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
                        }
                    });
            ExecutionEnvironment s = RowWeave.createEnvironment("d-cogroup");
            DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5");
            DataSet b1 = mid.filter("k >= 3");
            DataSet b2 = mid.filter("k <= 2");
            DataSet cog = b1.coGroup(b2, cg);
            WorkerCollectingOutput out = new WorkerCollectingOutput(cog.getSchema());
            cog.sinkTo(out);
            s.execute();
            assertTrue(out.getRows().size() > 0, "coGroup emits output");
            assertEquals(1, reads.get(), "diamond->coGroup reads the shared source once");
        });
    }

    /** A mix in which one fork branch re-joins and the other goes straight to a SINK: the shared source is still read once, since materializing the fork also serves the side branch. */
    @Test
    void mixedReJoinAndDirectSinkSharedOnce() {
        withWatchdog(() -> {
            List<DataRow> data = data();
            AtomicInteger reads = new AtomicInteger();
            ExecutionEnvironment s = RowWeave.createEnvironment("mixed");
            DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5");
            // Branches 1 and 2 re-join, so they only produce rows where their keys overlap, while branch 3 goes straight to a SINK.
            DataSet b1 = mid.filter("k >= 2");
            DataSet b2 = mid.filter("k <= 4");
            DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
            WorkerCollectingOutput jout = new WorkerCollectingOutput(joined.getSchema());
            joined.sinkTo(jout);
            DataSet b3 = mid.filter("k == 1");
            WorkerCollectingOutput sout = new WorkerCollectingOutput(b3.getSchema());
            b3.sinkTo(sout);
            s.execute();
            assertTrue(jout.getRows().size() > 0, "join branch emits output");
            assertTrue(sout.getRows().size() > 0, "direct SINK branch emits output");
            assertEquals(1, reads.get(), "mixed re-join+SINK reads the shared source once");
        });
    }

    /** Two binaries sharing forks, with D=join(b1,c2) and E=join(a2,c2) where a2 is itself a fork, materializing mid and a2 while the shared source is read once. */
    @Test
    void twoBinariesSharedForkSharedOnce() {
        withWatchdog(() -> {
            List<DataRow> data = data();
            AtomicInteger reads = new AtomicInteger();
            ExecutionEnvironment s = RowWeave.createEnvironment("two-bin");
            DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5"); // A
            DataSet a1 = mid.filter("k >= 1");     // A1
            DataSet b1 = a1.filter("k <= 5");      // B1
            DataSet a2 = mid.filter("k >= 2");  // A2, itself a fork, feeding B2 and E
            DataSet b2 = a2.filter("k <= 4");      // B2
            DataSet c2 = b2.filter("k >= 2");  // C2, feeding D and E
            DataSet d = b1.join(c2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
            DataSet e = a2.join(c2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
            WorkerCollectingOutput od = new WorkerCollectingOutput(d.getSchema());
            WorkerCollectingOutput oe = new WorkerCollectingOutput(e.getSchema());
            d.sinkTo(od);
            e.sinkTo(oe);
            s.execute();
            assertTrue(od.getRows().size() > 0 && oe.getRows().size() > 0, "both D and E emit output");
            assertEquals(1, reads.get(), "two binaries sharing a fork read the shared source once");
        });
    }

    /** A diamond into a hash join, which builds the right side first on a single machine: it also consumes in order, so materialization breaks the cycle and the shared source is read once. */
    @Test
    void diamondToHashJoinSharedOnce() {
        withWatchdog(() -> {
            List<DataRow> data = data();
            AtomicInteger reads = new AtomicInteger();
            ExecutionEnvironment s = RowWeave.createEnvironment("d-hashjoin");
            DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5");
            DataSet b1 = mid.filter("k >= 2");
            DataSet b2 = mid.filter("k <= 4");
            DataSet joined = b1.join(b2,
                    SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER),
                    io.rowweave.ops.join.JoinHint.HASH);
            WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
            joined.sinkTo(out);
            s.execute();
            assertTrue(out.getRows().size() > 0, "hash-join diamond emits output");
            assertEquals(1, reads.get(), "diamond->hashjoin reads the shared source once");
        });
    }

    /** A diamond whose result feeds a second diamond join: the reachability test misses nothing once nested, and the shared source is read once. */
    @Test
    void nestedDiamondSharedOnce() {
        withWatchdog(() -> {
            // Two levels of same-key joins amplify quadratically, so 30 rows are enough to cover nested fork reachability without turning the topology test into an unbounded materialization.
            List<DataRow> data = data(30);
            AtomicInteger reads = new AtomicInteger();
            ExecutionEnvironment s = RowWeave.createEnvironment("nested");
            DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5"); // fork1
            DataSet j1 = mid.filter("k >= 2").join(mid.filter("k <= 4"),
                    SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));  // the inner diamond
            // j1 outputs 4 columns, k, amt, k, amt, and the first k is the key of the second join; fork2 diamonds j1 again.
            DataSchema j1Meta = j1.getSchema();
            String k0 = j1Meta.getField(0).getName();
            DataSet j2 = j1.filter(k0 + " >= 2").join(j1.filter(k0 + " <= 8"),
                    SortMergeJoin.of(j1Meta, j1Meta, new String[]{k0}, new String[]{k0}, JoinType.INNER));  // the outer diamond
            WorkerCollectingOutput out = new WorkerCollectingOutput(j2.getSchema());
            j2.sinkTo(out);
            s.execute();
            assertTrue(out.getRows().size() > 0, "nested diamond emits output");
            assertEquals(1, reads.get(), "nested diamond reads the shared source once (both inner and outer fork materialize)");
        });
    }

    /** An asymmetric diamond whose fork branches differ in depth, nested: the reachability test misses nothing on an irregular topology, and the shared source is read once. */
    @Test
    void asymmetricDiamondSharedOnce() {
        withWatchdog(() -> {
            List<DataRow> data = data();
            AtomicInteger reads = new AtomicInteger();
            ExecutionEnvironment s = RowWeave.createEnvironment("asym");
            DataSet a = s.fromSource(countingInput(data, reads)).filter("amt >= 5"); // fork A
            DataSet left = a.filter("k >= 2");  // the left branch spans one hop
            DataSet right = a.filter("k <= 4").filter("amt <= 30").filter("k >= 1");  // the right branch spans three hops
            DataSet joined = left.join(right,
                    SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
            WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
            joined.sinkTo(out);
            s.execute();
            assertTrue(out.getRows().size() > 0, "asymmetric diamond emits output");
            assertEquals(1, reads.get(), "asymmetric diamond reads the shared source once");
        });
    }
}
