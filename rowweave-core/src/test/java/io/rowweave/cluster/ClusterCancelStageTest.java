package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcTransport;
import io.rowweave.cluster.rpc.RpcTransportCache;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterCancelStageTest {

    @Test
    void cancelUnknownStageCreatesNegativeTombstone(@TempDir Path tmp) throws Exception {
        File wtmp = tmp.resolve("w").toFile(); wtmp.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer w = WorkerServer.newUnguarded("w", 32, "127.0.0.1", master.port(), wtmp);
             RpcClient toWorker = new RpcClient("127.0.0.1", w.port())) {
            RpcMessage resp = toWorker.call(RpcMessage.cancelStage(
                    "w", 9999L, w.incarnationId(), RunAttemptFenceTestSupport.FENCE));
            assertEquals(RpcMessage.Type.ACK, resp.type(), "a CANCEL for an unknown stageId is answered with a silent ACK");
            assertEquals(0, w.runningStageCount(), "no stage is running");

            io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                    new io.rowweave.Field("k", io.rowweave.DataType.IntegerType));
            byte[] delayed = master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                    meta, 64, new io.rowweave.CollectingSink(meta))));
            RpcMessage rejected = toWorker.call(RunAttemptFenceTestSupport.socket(
                    "w", 9999L, delayed, w.incarnationId()));
            assertEquals(RpcMessage.Type.JOB_RESULT, rejected.type());
            assertFalse(rejected.success(), "once CANCEL arrives first, a late ASSIGN must be rejected");
            assertEquals(0, w.runningStageCount(), "a late ASSIGN must not enter the body");
        }
    }

    @Test
    void duplicateAssignIsIdempotentAndConflictingAssignFails(@TempDir Path tmp) throws Exception {
        File wtmp = tmp.resolve("w").toFile(); wtmp.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer w = WorkerServer.newUnguarded("w", 4, "127.0.0.1", master.port(), wtmp);
             RpcClient toWorker = new RpcClient("127.0.0.1", w.port())) {
            io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                    new io.rowweave.Field("k", io.rowweave.DataType.IntegerType));
            byte[] blob = master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                    meta, 64, new io.rowweave.CollectingSink(meta))));
            long stageId = 10001L;

            assertEquals(RpcMessage.Type.ACK,
                    toWorker.call(RunAttemptFenceTestSupport.socket(
                            "w", stageId, blob, w.incarnationId())).type());
            assertEquals(RpcMessage.Type.ACK,
                    toWorker.call(RunAttemptFenceTestSupport.socket(
                            "w", stageId, blob, w.incarnationId())).type(),
                    "a retry with the same stageId and fingerprint only ACKs, it does not resubmit");

            long deadline = System.currentTimeMillis() + 3000L;
            while (w.runningStageCount() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, w.runningStageCount(), "a duplicate ASSIGN can have only one active body");
            assertEquals(3L, w.freeSlots(), "the active ledger must contain the stage already registered");

            byte[] conflicting = master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                    meta, 128, new io.rowweave.CollectingSink(meta))));
            RpcMessage conflict = toWorker.call(RunAttemptFenceTestSupport.socket(
                    "w", stageId, conflicting, w.incarnationId()));
            assertEquals(RpcMessage.Type.JOB_RESULT, conflict.type());
            assertFalse(conflict.success(), "the same id carrying a different command must fail loudly");

            assertEquals(RpcMessage.Type.ACK,
                    toWorker.call(RpcMessage.cancelStage(
                            "w", stageId, w.incarnationId(),
                            RunAttemptFenceTestSupport.FENCE)).type());
            assertEquals(0, w.runningStageCount(), "when the CANCEL ACK returns the executing thread must already have exited");
            assertEquals(4L, w.freeSlots(), "the slot is restored only after a real exit");
        }
    }

    @Test
    void lostAssignAckRetryExecutesBodyOnce(@TempDir Path tmp) throws Exception {
        File wtmp = tmp.resolve("w").toFile(); wtmp.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "w", 4, "127.0.0.1", master.port(), wtmp)) {
            java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
            RpcTransportCache cache = new RpcTransportCache((host, port) -> new RpcTransport() {
                @Override public RpcMessage call(RpcMessage request) {
                    RpcMessage response = worker.handler().apply(request);
                    if (calls.incrementAndGet() == 1) {
                        throw new RuntimeException("simulated lost ACK after worker accepted");
                    }
                    return response;
                }
                @Override public void send(RpcMessage request) { call(request); }
                @Override public void close() { }
            }, (h, p) -> true);
            try {
                io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                        new io.rowweave.Field("k", io.rowweave.DataType.IntegerType));
                byte[] blob = master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                        meta, 64, new io.rowweave.CollectingSink(meta))));
                long stageId = 10002L;
                assertEquals(RpcMessage.Type.ACK, cache.call("worker", 1,
                        RunAttemptFenceTestSupport.socket(
                                "w", stageId, blob, worker.incarnationId())).type());
                long deadline = System.currentTimeMillis() + 3000L;
                while (worker.runningStageCount() == 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10L);
                }
                assertEquals(2, calls.get(), "after the first lost ACK exactly one retry should follow");
                assertEquals(1, worker.runningStageCount(), "a retry can correspond to only one active body");
                assertEquals(RpcMessage.Type.ACK,
                        worker.handler().apply(RpcMessage.cancelStage(
                                "w", stageId, worker.incarnationId(),
                                RunAttemptFenceTestSupport.FENCE)).type());
                assertEquals(0, worker.runningStageCount());
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void jobFailureMasterCancelsAndReclaims(@TempDir Path tmp) throws Exception {
        io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                new io.rowweave.Field("k", io.rowweave.DataType.StringType),
                new io.rowweave.Field("amt", io.rowweave.DataType.BigDecimalType));
        io.rowweave.DataSchema gb = io.rowweave.ops.ParallelGroupByAgg.of(meta, "k").sum("amt").getOutputSchema();

        io.rowweave.Source boom = new io.rowweave.Source() {
            @Override public io.rowweave.DataSchema getSchema() { return meta; }
            @Override public java.util.Iterator<io.rowweave.DataRow> iterator() {
                return new java.util.Iterator<io.rowweave.DataRow>() {
                    private int n = 0;
                    @Override public boolean hasNext() { return n++ < 1; }
                    @Override public io.rowweave.DataRow next() { throw new RuntimeException("source boom"); }
                };
            }
        };
        File outFile = tmp.resolve("ff-gb.tsv").toFile();
        io.rowweave.plan.PlanNode src = io.rowweave.plan.PlanNode.sourceFromInputs(
                meta, java.util.Collections.singletonList(boom));
        io.rowweave.plan.PlanNode gbNode = io.rowweave.plan.PlanNode.unary("groupBySum", gb, 0)
                .listParam("keys", java.util.Arrays.asList("k")).param("measure", "amt").build();
        io.rowweave.plan.PlanNode sink = io.rowweave.plan.PlanNode.sink(
                gb, 1, new io.rowweave.FileSink(gb, outFile.getAbsolutePath())).build();
        io.rowweave.plan.PlanGraph graph = new io.rowweave.plan.PlanGraph(
                java.util.Arrays.asList(src, gbNode, sink));

        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();
        File w1tmp = tmp.resolve("w1").toFile(); w1tmp.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0tmp);
             WorkerServer w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1tmp)) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
            assertThrows(RuntimeException.class, () -> client.submitToSink(graph), "a throwing source makes the submit fail fast");
            long deadline = System.currentTimeMillis() + 8000;
            while ((w0.runningStageCount() + w1.runningStageCount()) > 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(0, w0.runningStageCount() + w1.runningStageCount(),
                    "a failed Job leaves no leaked worker process (Master CANCEL plus self-exit)");
        }
    }

    @Test
    void cancelReclaimsRunningChild(@TempDir Path tmp) throws Exception {
        File wtmp = tmp.resolve("w").toFile(); wtmp.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer w = WorkerServer.newUnguarded("w", 32, "127.0.0.1", master.port(), wtmp);
             RpcClient toWorker = new RpcClient("127.0.0.1", w.port())) {

            io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                    new io.rowweave.Field("k", io.rowweave.DataType.IntegerType));
            io.rowweave.CollectingSink out = new io.rowweave.CollectingSink(meta);
            byte[] blob = master.serialize(RunAttemptFenceTestSupport.bind(
                    StageCommand.consumer(meta, 64, out)));
            long stageId = 1L;
            toWorker.call(RunAttemptFenceTestSupport.socket(
                    "w", stageId, blob, w.incarnationId()));

            long deadline = System.currentTimeMillis() + 5000;
            while (w.runningStageCount() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, w.runningStageCount(), "a long-running listener worker should be registered as running");

            toWorker.call(RpcMessage.cancelStage(
                    "w", stageId, w.incarnationId(), RunAttemptFenceTestSupport.FENCE));
            assertEquals(0, w.runningStageCount(), "when the CANCEL ACK returns the worker is reclaimed and running is empty");
        }
    }
}

