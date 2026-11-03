package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcTransportCache;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterGracefulCloseTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));
    private static final MapFunction SLOW_MAP = row -> {
        try {
            Thread.sleep(2L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        return row;
    };

    @Test
    void closeCancelsInflightStageAndDrainsMasterOwnership(@TempDir Path temp) throws Exception {
        MasterServer master = new MasterServer(60_000L);
        File workerDir = temp.resolve("worker").toFile();
        assertTrue(workerDir.mkdirs());
        WorkerServer worker = WorkerServer.newUnguarded(
                "close-worker", 4, "127.0.0.1", master.port(), workerDir);
        AtomicReference<Throwable> submitFailure = new AtomicReference<>();
        Thread submit = new Thread(() -> {
            try {
                new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(slowWholeGraph());
            } catch (Throwable failure) {
                submitFailure.set(failure);
            }
        }, "master-close-submit");
        try {
            submit.start();
            await(() -> worker.runningStageCount() > 0, 5_000L, "worker stage did not start");

            master.close();

            assertEquals(0, worker.runningStageCount(), "the Worker stage terminal state must be confirmed before close returns");
            assertEquals(0, master.activeStageTrackerCount(), "the tracker must be empty before close returns");
            assertEquals(0, master.activeStageAssignmentCount(), "the stageWorker count must be zero before close returns");
            submit.join(5_000L);
            assertFalse(submit.isAlive(), "the submit owner must be woken by close and exit");
            assertTrue(submitFailure.get() != null, "a job being closed must report failure to the client");
        } finally {
            worker.close();
            master.close();
            submit.join(5_000L);
        }
    }

    @Test
    void closedMasterRejectsNewStateButAcknowledgesLateTerminalReports() {
        MasterServer master = new MasterServer(5_000L);
        master.close();

        RpcMessage submit = master.handler().apply(RpcMessage.submitJob(new byte[]{1}));
        assertEquals(RpcMessage.Type.JOB_RESULT, submit.type());
        assertFalse(submit.success());
        assertTrue(submit.errorMessage().contains("master is closing or closed"));

        RpcMessage register = master.handler().apply(
                RpcMessage.registerWorker("late", "127.0.0.1", 12345, 1));
        assertEquals(RpcMessage.Type.JOB_RESULT, register.type());
        assertFalse(register.success());
        assertTrue(register.errorMessage().contains("master is closing or closed"));
        assertNull(master.worker("late"));

        assertNull(master.handler().apply(RpcMessage.heartbeat("late")));
        assertNull(master.handler().apply(RpcMessage.progressReport("late", 1L, 0L, 0L, true)));
        assertEquals(RpcMessage.Type.ACK,
                master.handler().apply(RpcMessage.stageReport("late", 1L, true, 0L, 0L, null)).type());
        assertEquals(RpcMessage.Type.ACK,
                master.handler().apply(RpcMessage.portReport("late", 1L, 12346)).type());

        master.close();
    }

    @Test
    void closedMasterPermanentlyShutsWorkerTransport() throws Exception {
        String workerHost = "closed-master-worker";
        int workerPort = 45129;
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(
                workerHost, workerPort, message -> RpcMessage.ack());
        MasterServer master = new MasterServer(5_000L);
        try {
            RpcTransportCache cache = transportCache(master);

            master.close();

            assertThrows(IllegalStateException.class,
                    () -> cache.call(workerHost, workerPort, RpcMessage.ack()),
                    "after the Master reaches a terminal state a late owner must not rebuild the worker transport");
        } finally {
            master.close();
            endpointRegistration.close();
        }
    }

    @Test
    void rejectsNegativeCloseDrainBeforeOpeningServerResources() {
        String key = "rowweave.master.closeDrainMs";
        String previous = System.setProperty(key, "-1");
        try {
            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class, () -> new MasterServer(5_000L));
            assertTrue(failure.getMessage().contains(key));
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    void closePreservesCallerInterruptFlag() {
        MasterServer master = new MasterServer(5_000L);
        try {
            Thread.currentThread().interrupt();
            master.close();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            master.close();
        }
    }

    @Test
    void zeroDrainTimeoutStillBoundsCloseWithInflightSubmit(@TempDir Path temp) throws Exception {
        String key = "rowweave.master.closeDrainMs";
        String previous = System.setProperty(key, "0");
        MasterServer master = null;
        WorkerServer worker = null;
        Thread submit = null;
        try {
            master = new MasterServer(60_000L);
            File workerDir = temp.resolve("zero-drain-worker").toFile();
            assertTrue(workerDir.mkdirs());
            worker = WorkerServer.newUnguarded(
                    "zero-drain-worker", 4, "127.0.0.1", master.port(), workerDir);
            MasterServer targetMaster = master;
            submit = new Thread(() -> {
                try {
                    new RealClusterJobClient("127.0.0.1", targetMaster.port()).submitToSink(slowWholeGraph());
                } catch (RuntimeException expectedDuringClose) {
                }
            }, "zero-drain-submit");
            submit.start();
            WorkerServer targetWorker = worker;
            await(() -> targetWorker.runningStageCount() > 0, 5_000L, "worker stage did not start");

            long started = System.nanoTime();
            master.close();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMs < 2_000L, "a zero-drain close must be bounded, actual " + elapsedMs + "ms");
        } finally {
            if (worker != null) {
                worker.close();
            }
            if (master != null) {
                master.close();
            }
            if (submit != null) {
                submit.join(5_000L);
            }
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    private static PlanGraph slowWholeGraph() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            DataRow row = new DataRow(1);
            row.set(0, i);
            rows.add(row);
        }
        PlanNode source = PlanNode.source(META, rows);
        PlanNode map = PlanNode.unaryFn("map", META, 0, SLOW_MAP);
        PlanNode sink = PlanNode.sink(META, 1, new DiscardSink(META)).build();
        return new PlanGraph(Arrays.asList(source, map, sink));
    }

    private static RpcTransportCache transportCache(MasterServer master)
            throws ReflectiveOperationException {
        java.lang.reflect.Field field = MasterServer.class.getDeclaredField("rpcCache");
        field.setAccessible(true);
        return (RpcTransportCache) field.get(master);
    }

    private static void await(Check condition, long timeoutMs, String failure) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.ok() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(condition.ok(), failure);
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }
}

