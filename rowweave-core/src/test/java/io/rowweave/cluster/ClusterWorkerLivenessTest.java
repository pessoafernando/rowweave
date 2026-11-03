package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("multiprocess")
class ClusterWorkerLivenessTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void lostWorkerIsExcludedWhileHealthyWorkerStillExecutes(@TempDir Path tmp) throws Exception {
        System.setProperty("rowweave.worker.heartbeatMs", "25");
        File d0 = tmp.resolve("w0").toFile(); d0.mkdirs();
        File d1 = tmp.resolve("w1").toFile(); d1.mkdirs();
        MasterServer master = new MasterServer(200L);
        WorkerServer w0 = WorkerServer.newUnguarded("w0", 8, "127.0.0.1", master.port(), d0);
        WorkerServer w1 = WorkerServer.newUnguarded("w1", 8, "127.0.0.1", master.port(), d1);
        try {
            w0.close();
            awaitQuarantined(master, "w0", 3_000L);

            List<DataRow> rows = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                DataRow row = new DataRow(1); row.set(0, i); rows.add(row);
            }
            Path output = tmp.resolve("healthy.tsv");
            ExecutionEnvironment session = new ExecutionEnvironment(
                    "healthy-after-loss", "healthy-after-loss",
                    new RealClusterJobClient("127.0.0.1", master.port()));
            session.setParallelism(1);
            session.fromSource(new CollectionSource(META, rows))
                    .sinkTo(new FileSink(META, output.toString()));
            session.execute();

            assertEquals(100, FileSink.read(META, output.toString()).size());
            assertTrue(!master.workerQuarantined("w1"), "a healthy Worker must not be quarantined as collateral damage");
        } finally {
            try { w0.close(); } catch (RuntimeException ignored) { }
            w1.close();
            master.close();
            System.clearProperty("rowweave.worker.heartbeatMs");
        }
    }

    private static void awaitQuarantined(MasterServer master, String workerId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!master.workerQuarantined(workerId) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(master.workerQuarantined(workerId), "a lost Worker should be quarantined within a bounded time");
    }
}

