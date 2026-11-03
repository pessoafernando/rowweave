package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class InProcessClusterTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(ORDER, "k").sum("amt").getOutputSchema();

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < n; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    private static Map<Integer, Long> oracle(List<DataRow> rows) {
        Map<Integer, Long> m = new HashMap<>();
        for (DataRow r : rows) {
            if (((BigDecimal) r.get(1)).longValue() >= 0) {
                m.merge((Integer) r.get(0), ((BigDecimal) r.get(1)).longValue(), Long::sum);
            }
        }
        return m;
    }

    private Map<Integer, Long> runInProcess(List<DataRow> rows, int parallelism, Path tmp) throws Exception {
        Path outPath = tmp.resolve("inproc-p" + parallelism + ".tsv");
        try (InProcessCluster cluster = new InProcessCluster(parallelism, tmp.toFile())) {
            BatchSession s = new BatchSession("inproc", "inproc", cluster);
            s.setParallelism(parallelism);
            s.fromSource(new CollectionSource(ORDER, rows))
                    .filter("amt >= 0")
                    .groupBy(ParallelGroupByAgg.of(ORDER, "k").sum("amt"))
                    .sinkTo(new FileSink(GB, outPath.toString()));
            s.execute();
        }
        Map<Integer, Long> got = new HashMap<>();
        for (DataRow r : FileSink.read(GB, outPath.toString())) {
            got.put((Integer) r.get(0), ((BigDecimal) r.get(1)).longValue());
        }
        return got;
    }

    @Test
    void inProcessP1MatchesOracle(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = rows(1600);
        assertEquals(oracle(rows), runInProcess(rows, 1, tmp), "InProcessCluster P=1 == oracle");
    }

    /** InProcessCluster at P=4: several slots running in parallel inside one worker process, and the result still equals the oracle. */
    @Test
    void inProcessP4MatchesOracle(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = rows(1600);
        assertEquals(oracle(rows), runInProcess(rows, 4, tmp), "InProcessCluster P=4 == oracle");
    }
}

