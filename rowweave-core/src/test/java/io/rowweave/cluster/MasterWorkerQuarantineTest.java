package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MasterWorkerQuarantineTest {

    @Test
    void onlyFullIdleHeartbeatOrNewEndpointRestoresWorker() {
        try (MasterServer master = new MasterServer(5000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 9100, 2));
            master.quarantineWorker("w");
            assertTrue(master.workerQuarantined("w"));

            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 9100, 2));
            assertTrue(master.workerQuarantined("w"), "a periodic REGISTER from the same endpoint cannot lift the quarantine");
            master.handler().apply(RpcMessage.heartbeat("w", 1));
            assertTrue(master.workerQuarantined("w"), "a heartbeat that is not fully idle cannot prove the old stage exited");
            master.handler().apply(RpcMessage.heartbeat("w", 2));
            assertFalse(master.workerQuarantined("w"), "a fully idle heartbeat can restore capacity");

            master.quarantineWorker("w");
            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 9101, 2));
            assertFalse(master.workerQuarantined("w"), "a new endpoint or incarnation can lift the quarantine of the old process");
        }
    }
}

