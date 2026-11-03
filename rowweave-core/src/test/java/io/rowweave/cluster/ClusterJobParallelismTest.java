package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterJobParallelismTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 20);
            r.set(1, new BigDecimal(i % 100));
            rs.add(r);
        }
        return rs;
    }

    private long runGroupBy(int par, MasterServer m, String name, Path out) {
        ExecutionEnvironment s = new ExecutionEnvironment(name, name, new RealClusterJobClient("127.0.0.1", m.port()));
        s.setParallelism(par);
        s.fromSource(new CollectionSource(META, rows(4000)))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                .orderBy("k")
                .sinkTo(new FileSink(GB, out.toString()));
        s.execute();
        long n = 0;
        for (DataRow ignored : FileSink.read(GB, out.toString())) n++;
        return n;
    }

    private int keyedStageInstances(MasterServer m) {
        PhysicalPlan plan = m.lastPhysicalPlan();
        int max = 1;
        for (int st = 0; st < 8; st++) {
            int c = plan.instancesOf(st).size();
            if (c > max) max = c;
        }
        return max;
    }

    @Test
    void setParallelismReachesClusterPlan(@TempDir Path tmp) {
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 64, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 64, "127.0.0.1", m.port(), w1)) {
            assertEquals(20, runGroupBy(3, m, "p3", tmp.resolve("p3.tsv")), "P=3 groupBy yields 20 groups");
            assertEquals(3, keyedStageInstances(m), "setParallelism(3) fans the keyed stage out to 3 instances (effective on the cluster)");
        }
    }

    @Test
    void sameMasterRunsDifferentParallelism(@TempDir Path tmp) {
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 64, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 64, "127.0.0.1", m.port(), w1)) {
            assertEquals(20, runGroupBy(2, m, "j2", tmp.resolve("j2.tsv")), "job P=2");
            assertEquals(2, keyedStageInstances(m), "first job on the same master at P=2");
            assertEquals(20, runGroupBy(4, m, "j4", tmp.resolve("j4.tsv")), "job P=4");
            assertEquals(4, keyedStageInstances(m), "second job on the same master at P=4 (parallelism follows the job, not the master)");
        }
    }

    @Test
    void slotsDoNotDetermineParallelism(@TempDir Path tmp) {
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 128, "127.0.0.1", m.port(), w0)) {
            assertEquals(20, runGroupBy(2, m, "dec", tmp.resolve("dec.tsv")), "P=2 groupBy yields 20 groups");
            assertEquals(2, keyedStageInstances(m), "slots=128 with P=2 still fans out to 2 (slots and parallelism are independent)");
        }
    }
}

