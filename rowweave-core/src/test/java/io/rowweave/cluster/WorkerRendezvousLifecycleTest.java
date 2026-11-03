package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("multiprocess")
class WorkerRendezvousLifecycleTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema OUT = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    @Test
    void repeatedJobsReleaseWorkerRendezvousEntries(@TempDir Path tmp) throws Exception {
        System.setProperty("rowweave.worker.rendezvousRetentionMs", "40");
        File d0 = tmp.resolve("w0").toFile(); d0.mkdirs();
        File d1 = tmp.resolve("w1").toFile(); d1.mkdirs();
        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), d0);
             WorkerServer w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), d1)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());
            for (int job = 0; job < 8; job++) {
                BatchSession session = new BatchSession("rz-" + job, "rz-" + job, client);
                session.setParallelism(2);
                session.fromSource(new CollectionSource(META, rows(2_000)))
                        .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                        .sinkTo(new FileSink(OUT, tmp.resolve("out-" + job + ".tsv").toString()));
                session.execute();
            }
            Thread.sleep(50L);
            w0.purgeExpiredRendezvous();
            w1.purgeExpiredRendezvous();
            assertEquals(0, w0.rendezvousEntryCount() + w1.rendezvousEntryCount(),
                    "a completed rendezvous converges out of the long-lived Worker once the grace window expires");
        } finally {
            System.clearProperty("rowweave.worker.rendezvousRetentionMs");
        }
    }

    @Test
    void cancelledInboundTombstoneIsPurgedByWorkerMaintenance(@TempDir Path tmp) throws Exception {
        System.setProperty("rowweave.worker.rendezvousRetentionMs", "40");
        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "w", 4, "127.0.0.1", master.port(), tmp.toFile())) {
            PlanGraph passthrough = new PlanGraph(Collections.singletonList(
                    PlanNode.source(META, Collections.emptyList())));
            StageCommand command = StageCommand.sink(
                    META, 64, 1, passthrough, new DiscardSink(META))
                    .withLocalInbound(1, "cancelled-local-key");
            long stageId = 81_001L;
            assertEquals(RpcMessage.Type.ACK,
                    worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                            "w", stageId, command, worker.incarnationId())).type());

            awaitEntries(worker, 1, 3_000L);
            assertEquals(RpcMessage.Type.ACK,
                    worker.handler().apply(RpcMessage.cancelStage(
                            "w", stageId, worker.incarnationId(),
                            RunAttemptFenceTestSupport.FENCE)).type());
            Thread.sleep(50L);
            worker.purgeExpiredRendezvous();
            assertEquals(0, worker.rendezvousEntryCount());
        } finally {
            System.clearProperty("rowweave.worker.rendezvousRetentionMs");
        }
    }

    private static List<DataRow> rows(int count) {
        List<DataRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(2);
            row.set(0, i % 20);
            row.set(1, BigDecimal.valueOf(i % 100));
            rows.add(row);
        }
        return rows;
    }

    private static void awaitEntries(WorkerServer worker, int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (worker.rendezvousEntryCount() != expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, worker.rendezvousEntryCount());
    }
}

