package io.rowweave.cluster;

import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerCriticalReportDeliveryTest {

    @TempDir
    Path temp;

    @Test
    void criticalReportsRetryButProgressRemainsBestEffort() throws Exception {
        String host = "critical-report-master";
        int port = 45127;
        AtomicInteger stageCalls = new AtomicInteger();
        AtomicInteger portCalls = new AtomicInteger();
        AtomicInteger progressCalls = new AtomicInteger();
        CountDownLatch progressSeen = new CountDownLatch(1);

        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, message -> {
            switch (message.type()) {
                case REGISTER_WORKER:
                    return RpcMessage.ack();
                case HEARTBEAT:
                    return null;
                case STAGE_REPORT:
                    if (stageCalls.incrementAndGet() <= 4) {
                        throw new RuntimeException("drop stage report");
                    }
                    return RpcMessage.ack();
                case PORT_REPORT:
                    if (portCalls.incrementAndGet() <= 2) {
                        throw new RuntimeException("drop port report");
                    }
                    return RpcMessage.ack();
                case PROGRESS_REPORT:
                    progressCalls.incrementAndGet();
                    progressSeen.countDown();
                    throw new RuntimeException("drop progress report");
                default:
                    return RpcMessage.ack();
            }
        });

        try (WorkerServer worker = WorkerServer.newUnguarded(
                "critical-worker", 1, host, port, temp.toFile())) {
            worker.deliverReport(RpcMessage.stageReport(
                    "critical-worker", 101L, true, 7L, 5L, null));
            worker.deliverReport(RpcMessage.portReport(
                    "critical-worker", 102L, 23001, 0));
            worker.deliverReport(RpcMessage.progressReport(
                    "critical-worker", 103L, 11L, 9L, true));

            assertEquals(5, stageCalls.get(), "STAGE_REPORT retries until ACKed");
            assertEquals(3, portCalls.get(), "PORT_REPORT retries until ACKed");
            assertTrue(progressSeen.await(1, TimeUnit.SECONDS));
            assertEquals(1, progressCalls.get(), "PROGRESS_REPORT stays a single best-effort send");
        } finally {
            endpointRegistration.close();
        }
    }
}

