package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MasterServerTest {

    @Test
    void workerRegisters() {
        try (MasterServer master = new MasterServer(1000)) {
            try (RpcClient c = new RpcClient("127.0.0.1", master.port())) {
                RpcMessage ack = c.call(RpcMessage.registerWorker("w1", "127.0.0.1", 6001, 8));
                assertEquals(RpcMessage.Type.ACK, ack.type());
            }
            assertNotNull(master.worker("w1"));
            assertEquals(8, master.worker("w1").slots());
        }
    }

    @Test
    void loopbackWorkerCapabilityIsAcceptedBeforeRegistrationBecomesVisible() {
        try (MasterServer master = new MasterServer(1000);
             RpcClient client = new RpcClient("127.0.0.1", master.port())) {
            RpcMessage response = client.call(RpcMessage.registerWorker(
                    "v0-worker", "127.0.0.1", 6001, 8, 6002, "inc-v0",
                    DataPlaneRegistration.loopbackPlainV0()));

            assertEquals(RpcMessage.Type.ACK, response.type());
            assertNotNull(master.worker("v0-worker"));
        }
    }

    @Test
    void incompatibleDataCapabilityIsRejectedWithoutPublishingWorker() {
        byte[] fingerprint = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        try (MasterServer master = new MasterServer(1000);
             RpcClient client = new RpcClient("127.0.0.1", master.port())) {
            RpcMessage response = client.call(RpcMessage.registerWorker(
                    "wrong-protocol", "127.0.0.1", 6101, 8, 6102, "inc-wrong",
                    DataPlaneRegistration.shuffleMtlsV1(fingerprint)));
            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertNull(master.worker("wrong-protocol"));
        }
    }

    @Test
    void heartbeatKeepsWorkerAlive() throws Exception {
        try (MasterServer master = new MasterServer(300)) { // staleness 300ms
            try (RpcClient c = new RpcClient("127.0.0.1", master.port())) {
                c.call(RpcMessage.registerWorker("w1", "127.0.0.1", 6001, 4));
                for (int i = 0; i < 6; i++) {
                    c.send(RpcMessage.heartbeat("w1"));
                    Thread.sleep(100);
                }
                assertTrue(master.worker("w1").heartbeat.isAlive(), "a steady heartbeat should be judged alive");
            }
        }
    }

    @Test
    void stoppedHeartbeatTriggersLostStanching() throws Exception {
        List<String> lost = new CopyOnWriteArrayList<>();
        try (MasterServer master = new MasterServer(200)) { // staleness 200ms
            try (RpcClient c = new RpcClient("127.0.0.1", master.port())) {
                c.call(RpcMessage.registerWorker("w1", "127.0.0.1", 6001, 4));
            }
            master.setOnWorkerLost(lost::add);
            master.startHeartbeatMonitor(50);
            long deadline = System.currentTimeMillis() + 3000;
            while (lost.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(lost.contains("w1"), "once heartbeats stop, the dead-worker callback should fire (no hang)");
        }
    }

    /** A late Master sweep must give the Worker heartbeat lane one bounded recovery window. */
    @Test
    void delayedMasterSweepDoesNotImmediatelyLoseWorkerAfterSchedulingPause() throws Exception {
        try (MasterServer master = new MasterServer(300L)) {
            // Suppress sweeps while both roles would be unscheduled during a process-wide pause.
            master.startHeartbeatMonitor(10_000L);
            master.handler().apply(RpcMessage.registerWorker("paused", "127.0.0.1", 6001, 4));
            Thread.sleep(450L);

            master.startHeartbeatMonitor(10L);
            Thread.sleep(80L);
            assertNotNull(master.worker("paused"),
                    "one late Master sweep must not kill a Worker before its heartbeat can resume");
            assertFalse(master.workerQuarantined("paused"));

            // No resumed beat: the grace must expire and preserve dead-worker fail-fast.
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
            while (!master.workerQuarantined("paused") && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(master.workerQuarantined("paused"),
                    "a truly stopped Worker must still be quarantined after bounded grace");
        }
    }
}

