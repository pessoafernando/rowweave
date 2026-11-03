package io.rowweave.cluster;

import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** A delayed control-plane registration must not consume the worker's liveness lane. */
class WorkerHeartbeatIsolationTest {

    @Test
    void periodicHeartbeatsContinueWhileReregistrationWaitsForMaster(@TempDir Path temp)
            throws Exception {
        String property = "rowweave.worker.heartbeatMs";
        String previous = System.getProperty(property);
        System.setProperty(property, "20");
        CountDownLatch registrationBlocked = new CountDownLatch(1);
        CountDownLatch releaseRegistration = new CountDownLatch(1);
        CountDownLatch beatsAfterBlock = new CountDownLatch(3);
        AtomicInteger registrations = new AtomicInteger();
        InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "heartbeat-isolation-master", 45401, message -> {
                    if (message.type() == RpcMessage.Type.REGISTER_WORKER
                            && registrations.incrementAndGet() > 1) {
                        registrationBlocked.countDown();
                        try {
                            releaseRegistration.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (message.type() == RpcMessage.Type.HEARTBEAT
                            && registrationBlocked.getCount() == 0) {
                        beatsAfterBlock.countDown();
                    }
                    return message.type() == RpcMessage.Type.HEARTBEAT
                            ? null : RpcMessage.ack();
                });
        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded("heartbeat-isolation-worker", 1,
                    "heartbeat-isolation-master", 45401, temp.toFile());
            assertTrue(registrationBlocked.await(2, TimeUnit.SECONDS),
                    "periodic re-registration did not enter the delayed master call");
            assertTrue(beatsAfterBlock.await(2, TimeUnit.SECONDS),
                    "blocked re-registration must not stop worker liveness heartbeats");
        } finally {
            releaseRegistration.countDown();
            if (worker != null) worker.close();
            master.close();
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }
}
