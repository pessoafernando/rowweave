package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("multiprocess")
class WorkerSlotCapacityTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));

    @Test
    void fullWorkerRejectsNewStageButKeepsDuplicateIdempotentAndRecovers(@TempDir Path tmp) throws Exception {
        File workerDir = tmp.resolve("worker").toFile();
        workerDir.mkdirs();
        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "w", 1, "127.0.0.1", master.port(), workerDir);
             RpcClient toWorker = new RpcClient("127.0.0.1", worker.port())) {
            byte[] command = blockingConsumer(master, 64);
            byte[] conflicting = blockingConsumer(master, 128);
            long firstId = 71_001L;
            long secondId = 71_002L;
            try {
                assertEquals(RpcMessage.Type.ACK,
                        toWorker.call(RunAttemptFenceTestSupport.socket(
                                "w", firstId, command, worker.incarnationId())).type());
                awaitActive(worker, 1);

                assertEquals(RpcMessage.Type.ACK,
                        toWorker.call(RunAttemptFenceTestSupport.socket(
                                "w", firstId, command, worker.incarnationId())).type(),
                        "a retry with the same stageId and fingerprint still acks idempotently when the worker is full");
                RpcMessage conflict = toWorker.call(RunAttemptFenceTestSupport.socket(
                        "w", firstId, conflicting, worker.incarnationId()));
                assertEquals(RpcMessage.Type.JOB_RESULT, conflict.type());
                assertTrue(conflict.errorMessage().contains("conflicting"),
                        "the same id with a different command is decided by the conflict semantics and is not hidden by capacity");
                assertFalse(conflict.errorMessage().contains("capacity"));

                RpcMessage rejected = toWorker.call(RunAttemptFenceTestSupport.socket(
                        "w", secondId, command, worker.incarnationId()));
                assertEquals(RpcMessage.Type.JOB_RESULT, rejected.type());
                assertFalse(rejected.success());
                assertTrue(rejected.errorMessage().contains("capacity"));
                assertTrue(rejected.errorMessage().contains("active=1"));
                assertTrue(rejected.errorMessage().contains("slots=1"));
                assertEquals(1, worker.runningStageCount(), "a refusal neither registers nor starts a second stage");
                assertEquals(0L, worker.freeSlots());

                assertEquals(RpcMessage.Type.ACK,
                        toWorker.call(RpcMessage.cancelStage(
                                "w", firstId, worker.incarnationId(),
                                RunAttemptFenceTestSupport.FENCE)).type());
                assertEquals(0, worker.runningStageCount());
                assertEquals(1L, worker.freeSlots());

                assertEquals(RpcMessage.Type.ACK,
                        toWorker.call(RunAttemptFenceTestSupport.socket(
                                "w", secondId, command, worker.incarnationId())).type(),
                        "once capacity is released the stage that was not accepted can be submitted again");
                awaitActive(worker, 1);
            } finally {
                toWorker.call(RpcMessage.cancelStage(
                        "w", firstId, worker.incarnationId(), RunAttemptFenceTestSupport.FENCE));
                toWorker.call(RpcMessage.cancelStage(
                        "w", secondId, worker.incarnationId(), RunAttemptFenceTestSupport.FENCE));
            }
        }
    }

    @Test
    void concurrentAssignmentsNeverExceedWorkerSlots(@TempDir Path tmp) throws Exception {
        File workerDir = tmp.resolve("worker-concurrent").toFile();
        workerDir.mkdirs();
        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "w", 4, "127.0.0.1", master.port(), workerDir)) {
            byte[] command = blockingConsumer(master, 64);
            int attempts = 16;
            ExecutorService callers = Executors.newFixedThreadPool(attempts);
            List<Future<RpcMessage>> futures = new ArrayList<>();
            for (int i = 0; i < attempts; i++) {
                final long stageId = 72_000L + i;
                futures.add(callers.submit(() -> {
                    try (RpcClient client = new RpcClient("127.0.0.1", worker.port())) {
                        return client.call(RunAttemptFenceTestSupport.socket(
                                "w", stageId, command, worker.incarnationId()));
                    }
                }));
            }

            List<Long> accepted = new ArrayList<>();
            List<RpcMessage> rejected = new ArrayList<>();
            try {
                for (int i = 0; i < attempts; i++) {
                    RpcMessage response = futures.get(i).get();
                    if (response.type() == RpcMessage.Type.ACK) {
                        accepted.add(72_000L + i);
                    } else {
                        rejected.add(response);
                    }
                }

                assertEquals(4, accepted.size(), "the concurrent check and increment is atomic and exactly as many requests as there are slots succeed");
                assertEquals(12, rejected.size());
                for (RpcMessage response : rejected) {
                    assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
                    assertFalse(response.success());
                    assertTrue(response.errorMessage().contains("capacity"));
                }
                awaitActive(worker, 4);
                assertEquals(4, worker.runningStageCount());
                assertEquals(0L, worker.freeSlots());
            } finally {
                callers.shutdownNow();
                try (RpcClient client = new RpcClient("127.0.0.1", worker.port())) {
                    for (long stageId : accepted) {
                        client.call(RpcMessage.cancelStage(
                                "w", stageId, worker.incarnationId(),
                                RunAttemptFenceTestSupport.FENCE));
                    }
                }
            }
            assertEquals(0, worker.runningStageCount());
            assertEquals(4L, worker.freeSlots());
        }
    }

    @Test
    void nonPositiveSlotsFailBeforeWorkerRegistration(@TempDir Path tmp) {
        try (MasterServer master = new MasterServer(5_000L)) {
            for (int invalidSlots : new int[]{0, -1}) {
                IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                        () -> new WorkerServer(
                                "invalid", invalidSlots, "127.0.0.1", master.port(), tmp.toFile()));
                assertTrue(error.getMessage().contains("slots"));
            }
            assertTrue(master.workers().isEmpty(), "an illegal slots value registers nothing and creates no half-initialized Worker");

            try (WorkerServer valid = new WorkerServer(
                    "valid", 1, "127.0.0.1", master.port(), tmp.toFile())) {
                assertTrue(master.workers().containsKey("valid"),
                        "an illegal guarded construction claims no process singleton slot and does not block a later valid Worker");
            }
        }
    }

    private static byte[] blockingConsumer(MasterServer master, int channelCapacity) {
        return master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                META, channelCapacity, new DiscardSink(META))));
    }

    private static void awaitActive(WorkerServer worker, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000L;
        while (worker.runningStageCount() != expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, worker.runningStageCount());
    }
}

