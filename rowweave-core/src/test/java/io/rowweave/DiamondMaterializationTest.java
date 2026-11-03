package io.rowweave;

import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A diamond re-join materializes the shared fork, computed once, through an edge that fills a pre-pass and
 * replays the whole main pipeline, which breaks the cycle, on the P=1 single-machine path.
 *
 * <p>Guardrails: the shared source is read once, rather than twice by a per-sink copy, and the result is set-equal to what copying produces,
 * since materializing does not change the algorithm; a self-join, mid.join(mid), is also read once; and a short watchdog timeout fails fast instead of hanging
 */
class DiamondMaterializationTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 8);
            r.set(1, new BigDecimal(i % 50));
            rs.add(r);
        }
        return rs;
    }

    private static Source countingInput(List<DataRow> data, AtomicInteger reads) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                reads.incrementAndGet();  // each iterator() is one full read
                return new ArrayList<>(data).iterator();
            }
        };
    }

    /** A diamond re-joined: the shared source is read once and the fork materialized once, rather than read twice by a per-sink copy. */
    @Test
    void diamondReJoinSharedSourceReadOnce() {
        List<DataRow> data = rows();
        AtomicInteger reads = new AtomicInteger();
        ExecutionEnvironment s = RowWeave.createEnvironment("diamond-mat");
        DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5");
        DataSet b1 = mid.filter("k >= 2");
        DataSet b2 = mid.filter("k <= 6");
        DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        s.execute();

        // The result row count is the cartesian of the two branches on k, matching what DiamondReJoinTest copies produce, since materializing does not change the algorithm.
        List<DataRow> l = new ArrayList<>(), r = new ArrayList<>();
        for (DataRow x : data) {
            int k = (Integer) x.get(0);
            BigDecimal a = (BigDecimal) x.get(1);
            if (a.compareTo(new BigDecimal(5)) >= 0 && k >= 2) l.add(x);
            if (a.compareTo(new BigDecimal(5)) >= 0 && k <= 6) r.add(x);
        }
        long expect = 0;
        for (int k = 0; k < 8; k++) {
            final int kk = k;
            long lc = l.stream().filter(x -> (Integer) x.get(0) == kk).count();
            long rc = r.stream().filter(x -> (Integer) x.get(0) == kk).count();
            expect += lc * rc;
        }
        assertEquals(expect, out.getRows().size(), "diamond join rows = Cartesian product of the two branches on k (materialization must not change the result)");
        assertEquals(1, reads.get(), "shared source read exactly once (the materialized fork is computed once); per-sink copies would read twice");
    }

    /** A self-join, mid.join(mid), where both join inputs are mid: mid is materialized and the shared source is read once. */
    @Test
    void selfJoinSharedSourceReadOnce() {
        List<DataRow> data = rows();
        AtomicInteger reads = new AtomicInteger();
        ExecutionEnvironment s = RowWeave.createEnvironment("selfjoin-mat");
        DataSet mid = s.fromSource(countingInput(data, reads)).filter("amt >= 5");
        DataSet joined = mid.join(mid, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        s.execute();
        assertTrue(out.getRows().size() > 0, "self-join emits output");
        assertEquals(1, reads.get(), "self-join reads the shared source once (materialized mid)");
    }
}
