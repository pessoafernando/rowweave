package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcTransportCache;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.sched.LocalShuffleRegistry;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.net.DataInboundRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerGracefulCloseTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));
    private static volatile CountDownLatch currentBodyStarted;
    private static volatile CountDownLatch batchBodiesStarted;
    private static volatile CountDownLatch batchRelease;
    private static volatile AtomicInteger batchStopsObserved;
    private static volatile CountDownLatch zeroBodyStarted;
    private static volatile CountDownLatch zeroRelease;

    private static final class SlowMap implements MapFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public DataRow map(DataRow input) {
            currentBodyStarted.countDown();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return input;
        }
    }

    private static final class BatchStopMap implements MapFunction {
        private static final long serialVersionUID = 1L;
        private boolean stopObserved;

        @Override
        public DataRow map(DataRow input) {
            batchBodiesStarted.countDown();
            while (batchStopsObserved.get() < 2 && batchRelease.getCount() > 0) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException interrupted) {
                    if (!stopObserved) {
                        stopObserved = true;
                        batchStopsObserved.incrementAndGet();
                    }
                }
            }
            return input;
        }
    }

    private static final class UncooperativeMap implements MapFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public DataRow map(DataRow input) {
            zeroBodyStarted.countDown();
            while (zeroRelease.getCount() > 0) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException ignored) {
                }
            }
            return input;
        }
    }

    @Test
    void closeDeliversCancellationReportBeforeStoppingReportTransport(@TempDir Path temp) throws Exception {
        String masterHost = "worker-close-master";
        int masterPort = 45130;
        AtomicInteger stageReports = new AtomicInteger();
        AtomicReference<StageExecutionTelemetry> terminalTelemetry = new AtomicReference<>();
        CountDownLatch bodyStarted = new CountDownLatch(1);
        currentBodyStarted = bodyStarted;
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message -> {
            if (message.type() == RpcMessage.Type.HEARTBEAT
                    || message.type() == RpcMessage.Type.PROGRESS_REPORT) {
                return null;
            }
            if (message.type() == RpcMessage.Type.STAGE_REPORT) {
                stageReports.incrementAndGet();
                terminalTelemetry.set(message.stageExecutionTelemetry());
            }
            return RpcMessage.ack();
        });

        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded(
                    "closing-worker", 1, masterHost, masterPort, temp.toFile());
            RpcMessage accepted = worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "closing-worker", 13001L, StageCommand.wholeGraph(slowGraph()), worker.incarnationId()));
            assertEquals(RpcMessage.Type.ACK, accepted.type());
            assertTrue(bodyStarted.await(5, TimeUnit.SECONDS), "stage body did not start");

            worker.close();

            assertEquals(1, stageReports.get(),
                    "a graceful close delivers the cancelled terminal state once before the report transport closes");
            assertEquals(TelemetryCoverage.COMPLETE, terminalTelemetry.get().coverage(),
                    "a started stage keeps its real execution telemetry and a close must not rewrite it as NOT_EXECUTED");
            assertEquals(0, worker.runningStageCount());
        } finally {
            if (worker != null) {
                worker.close();
            }
            masterRegistration.close();
        }
    }

    @Test
    void closeStopsEveryActiveStageBodyBeforeReturning(@TempDir Path temp) throws Exception {
        String masterHost = "worker-batch-close-master";
        int masterPort = 45131;
        batchBodiesStarted = new CountDownLatch(2);
        batchRelease = new CountDownLatch(1);
        batchStopsObserved = new AtomicInteger();
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message ->
                message.type() == RpcMessage.Type.HEARTBEAT
                        || message.type() == RpcMessage.Type.PROGRESS_REPORT
                        ? null : RpcMessage.ack());

        WorkerServer worker = null;
        Thread closeThread = null;
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        try {
            worker = WorkerServer.newUnguarded(
                    "batch-closing-worker", 2, masterHost, masterPort, temp.toFile());
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "batch-closing-worker", 13011L, StageCommand.wholeGraph(batchStopGraph()),
                    worker.incarnationId())).type());
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "batch-closing-worker", 13012L, StageCommand.wholeGraph(batchStopGraph()),
                    worker.incarnationId())).type());
            assertTrue(batchBodiesStarted.await(5, TimeUnit.SECONDS), "both stage bodies did not start");

            WorkerServer target = worker;
            closeThread = new Thread(() -> {
                try {
                    target.close();
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                }
            }, "worker-batch-close");
            closeThread.start();

            closeThread.join(2_000L);
            assertTrue(!closeThread.isAlive(),
                    "close returns within the global deadline");
            assertEquals(2, batchStopsObserved.get(),
                    "every user function has seen stop before close returns, and the outer owner must not release the ledger early");
            assertTrue(closeFailure.get() == null, "close failed: " + closeFailure.get());
        } finally {
            batchRelease.countDown();
            if (closeThread != null && closeThread.isAlive() && worker != null) {
                worker.handler().apply(RpcMessage.cancelStage(
                        "batch-closing-worker", 13012L, worker.incarnationId(),
                        RunAttemptFenceTestSupport.FENCE));
                closeThread.join(5_000L);
            }
            if (worker != null) {
                worker.close();
            }
            masterRegistration.close();
        }
    }

    @Test
    void closedWorkerRejectsLateAssignment(@TempDir Path temp) {
        String masterHost = "closed-worker-master";
        int masterPort = 45132;
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message ->
                message.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack());
        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded(
                    "closed-worker", 1, masterHost, masterPort, temp.toFile());
            worker.close();

            RpcMessage rejected = worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "closed-worker", 13021L, StageCommand.wholeGraph(slowGraph()), worker.incarnationId()));
            assertEquals(RpcMessage.Type.JOB_RESULT, rejected.type());
            assertFalse(rejected.success());
            assertTrue(rejected.errorMessage().contains("closing or closed"));
            assertEquals(0, worker.runningStageCount());
        } finally {
            if (worker != null) {
                worker.close();
            }
            masterRegistration.close();
        }
    }

    @Test
    void closedWorkerRejectsResourceEntrypointsAndPermanentlyShutsTransports(@TempDir Path temp)
            throws Exception {
        String masterHost = "closed-worker-resource-master";
        int masterPort = 45136;
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message ->
                message.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack());
        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded(
                    "closed-resource-worker", 1, masterHost, masterPort, temp.toFile());
            RpcTransportCache reportCache = transportCache(worker, "reportCache");
            RpcTransportCache heartbeatCache = transportCache(worker, "heartbeatCache");

            worker.close();

            WorkerServer closedWorker = worker;
            assertThrows(IllegalStateException.class, closedWorker::heartbeat);
            assertThrows(IllegalStateException.class, () -> closedWorker.startHeartbeat(1L));
            assertThrows(IllegalStateException.class, closedWorker::dataPort);
            assertThrows(IllegalStateException.class,
                    () -> reportCache.call(masterHost, masterPort, RpcMessage.ack()));
            assertThrows(IllegalStateException.class,
                    () -> heartbeatCache.call(masterHost, masterPort, RpcMessage.ack()));
        } finally {
            if (worker != null) {
                worker.close();
            }
            masterRegistration.close();
        }
    }

    @Test
    void closedWorkerDrainsAndFencesBothRendezvousRegistries(@TempDir Path temp) throws Exception {
        String masterHost = "closed-worker-rendezvous-master";
        int masterPort = 45137;
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message ->
                message.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack());
        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded(
                    "closed-rendezvous-worker", 1, masterHost, masterPort, temp.toFile());
            LocalShuffleRegistry local = field(worker, "localShuffle", LocalShuffleRegistry.class);
            DataInboundRegistry data = field(worker, "dataInbound", DataInboundRegistry.class);
            MultiProducerChannel localChannel = new MultiProducerChannel(16, 1);
            local.register("late-local", localChannel);
            if (data != null) {
                data.register(new java.util.UUID(132L, 1L), "late-data",
                        new MultiProducerChannel(16, 1), META);
            }

            worker.close();

            assertEquals(0, worker.rendezvousEntryCount());
            MultiProducerChannel rejected = new MultiProducerChannel(16, 1);
            assertThrows(IllegalStateException.class,
                    () -> local.register("after-worker-close", rejected));
            assertTrue(rejected.poll().isError());
            local.cancel("after-worker-close", new IllegalStateException("late cleanup"));
            if (data != null) {
                data.cancel(new java.util.UUID(132L, 2L),
                        new IllegalStateException("late cleanup"));
            }
            assertEquals(0, worker.rendezvousEntryCount(),
                    "a late stage cleanup after Worker close must not resurrect a rendezvous entry");
        } finally {
            if (worker != null) {
                worker.close();
            }
            masterRegistration.close();
        }
    }

    @Test
    void rejectsNegativeCloseDrainBeforeOpeningResources(@TempDir Path temp) {
        String key = "rowweave.worker.closeDrainMs";
        String previous = System.setProperty(key, "-1");
        try {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> WorkerServer.newUnguarded(
                            "invalid-close-worker", 1, "unused-master", 45133, temp.toFile()));
            assertTrue(failure.getMessage().contains(key));
        } finally {
            restoreProperty(key, previous);
        }
    }

    @Test
    void zeroDrainBoundsUncooperativeStageAndPreservesInterrupt(@TempDir Path temp) throws Exception {
        String key = "rowweave.worker.closeDrainMs";
        String previous = System.setProperty(key, "0");
        String masterHost = "zero-worker-close-master";
        int masterPort = 45134;
        zeroBodyStarted = new CountDownLatch(1);
        zeroRelease = new CountDownLatch(1);
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message ->
                message.type() == RpcMessage.Type.HEARTBEAT
                        || message.type() == RpcMessage.Type.PROGRESS_REPORT
                        ? null : RpcMessage.ack());
        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded(
                    "zero-close-worker", 1, masterHost, masterPort, temp.toFile());
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "zero-close-worker", 13031L, StageCommand.wholeGraph(uncooperativeGraph()),
                    worker.incarnationId())).type());
            assertTrue(zeroBodyStarted.await(5, TimeUnit.SECONDS));

            Thread.currentThread().interrupt();
            long started = System.nanoTime();
            worker.close();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMs < 2_000L, "timeout=0 close took " + elapsedMs + "ms");
            assertTrue(Thread.currentThread().isInterrupted(), "close restores the caller interrupt flag");
        } finally {
            Thread.interrupted();
            zeroRelease.countDown();
            if (worker != null) {
                WorkerServer targetWorker = worker;
                await(() -> targetWorker.runningStageCount() == 0, 5_000L);
                assertEquals(0, assignments(worker).frozenTelemetryCount(),
                        "late task completion after close timeout must discard undeliverable telemetry");
                worker.close();
            }
            masterRegistration.close();
            restoreProperty(key, previous);
        }
    }

    @Test
    void closeProactivelyWithdrawsDataEndpointBeforeItsPortCanBeReused(@TempDir Path temp)
            throws Exception {
        try (MasterServer master = new MasterServer(60_000L)) {
            WorkerServer worker = WorkerServer.newUnguarded(
                    "departing-worker", 1, "127.0.0.1", master.port(), temp.toFile());
            try {
                assertTrue(worker.dataPort() > 0);
                assertTrue(master.worker("departing-worker") != null);

                worker.close();

                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500L);
                while (!master.workerQuarantined("departing-worker") && System.nanoTime() < deadline) {
                    Thread.sleep(5L);
                }
                assertTrue(master.workerQuarantined("departing-worker"),
                        "a cooperative close withdraws the old RPC and data endpoint instead of waiting for the 60s heartbeat stale window");
                assertTrue(master.worker("departing-worker") == null,
                        "the old worker identity stops taking part in scheduling once close returns");
            } finally {
                worker.close();
            }
        }
    }

    @Test
    void concurrentCloseHasOneOwnerAndGuardedReplacementWaitsForFullShutdown(@TempDir Path temp) throws Exception {
        String masterHost = "guarded-worker-close-master";
        int masterPort = 45135;
        CountDownLatch reportEntered = new CountDownLatch(1);
        CountDownLatch releaseReport = new CountDownLatch(1);
        AtomicInteger stageReports = new AtomicInteger();
        currentBodyStarted = new CountDownLatch(1);
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message -> {
            if (message.type() == RpcMessage.Type.HEARTBEAT
                    || message.type() == RpcMessage.Type.PROGRESS_REPORT) {
                return null;
            }
            if (message.type() == RpcMessage.Type.STAGE_REPORT) {
                stageReports.incrementAndGet();
                reportEntered.countDown();
                try {
                    releaseReport.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return RpcMessage.ack();
        });

        WorkerServer worker = null;
        WorkerServer replacement = null;
        Thread firstClose = null;
        Thread secondClose = null;
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        try {
            File firstDir = temp.resolve("guarded-first").toFile();
            worker = new WorkerServer("guarded-first", 1, masterHost, masterPort, firstDir);
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "guarded-first", 13041L, StageCommand.wholeGraph(slowGraph()),
                    worker.incarnationId())).type());
            assertTrue(currentBodyStarted.await(5, TimeUnit.SECONDS));

            WorkerServer target = worker;
            firstClose = new Thread(() -> closeCapturing(target, firstFailure), "guarded-close-owner");
            firstClose.start();
            assertTrue(reportEntered.await(5, TimeUnit.SECONDS), "close owner did not enter report drain");

            secondClose = new Thread(() -> closeCapturing(target, secondFailure), "guarded-close-waiter");
            secondClose.start();
            Thread.sleep(50L);
            assertTrue(secondClose.isAlive(), "a concurrent close waits for the single owner to finish");
            assertThrows(IllegalStateException.class, () -> new WorkerServer(
                    "guarded-too-early", 1, masterHost, masterPort,
                    temp.resolve("guarded-too-early").toFile()));

            releaseReport.countDown();
            firstClose.join(5_000L);
            secondClose.join(5_000L);
            assertFalse(firstClose.isAlive());
            assertFalse(secondClose.isAlive());
            assertTrue(firstFailure.get() == null, "owner close failed: " + firstFailure.get());
            assertTrue(secondFailure.get() == null, "waiter close failed: " + secondFailure.get());
            assertEquals(1, stageReports.get(), "only one owner reports the terminal state under concurrent close");

            replacement = new WorkerServer("guarded-replacement", 1, masterHost, masterPort,
                    temp.resolve("guarded-replacement").toFile());
        } finally {
            releaseReport.countDown();
            if (firstClose != null) {
                firstClose.join(5_000L);
            }
            if (secondClose != null) {
                secondClose.join(5_000L);
            }
            if (replacement != null) {
                replacement.close();
            }
            if (worker != null) {
                worker.close();
            }
            masterRegistration.close();
        }
    }

    private static PlanGraph slowGraph() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.unaryFn("map", META, 0, new SlowMap()),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static PlanGraph batchStopGraph() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.unaryFn("map", META, 0, new BatchStopMap()),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static PlanGraph uncooperativeGraph() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.unaryFn("map", META, 0, new UncooperativeMap()),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static void restoreProperty(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    private static WorkerAssignmentLedger assignments(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("assignments");
        field.setAccessible(true);
        return (WorkerAssignmentLedger) field.get(worker);
    }

    private static void closeCapturing(WorkerServer worker, AtomicReference<Throwable> failure) {
        try {
            worker.close();
        } catch (Throwable closeFailure) {
            failure.set(closeFailure);
        }
    }

    private static RpcTransportCache transportCache(WorkerServer worker, String fieldName)
            throws ReflectiveOperationException {
        return field(worker, fieldName, RpcTransportCache.class);
    }

    private static <T> T field(WorkerServer worker, String fieldName, Class<T> type)
            throws ReflectiveOperationException {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return type.cast(field.get(worker));
    }

    private static void await(Check check, long timeoutMs) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!check.ok() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(check.ok());
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }
}

