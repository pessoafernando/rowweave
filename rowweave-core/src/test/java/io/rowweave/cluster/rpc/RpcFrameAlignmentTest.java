package io.rowweave.cluster.rpc;

import io.rowweave.cluster.MasterServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RpcFrameAlignmentTest {

    private static RpcMessage contractHandler(RpcMessage req) {
        switch (req.type()) {
            case HEARTBEAT:
            case PROGRESS_REPORT:
                return null;
            case STAGE_REPORT:
                return RpcMessage.jobResult(true, req.stageId(), null);
            default:
                return RpcMessage.ack();
        }
    }

    @Test
    void sendThenCallDoesNotMisalignFrames() {
        try (RpcServer server = new RpcServer(RpcFrameAlignmentTest::contractHandler)) {
            try (RpcClient client = new RpcClient("127.0.0.1", server.port())) {
                for (long i = 1; i <= 5; i++) {
                    client.send(RpcMessage.heartbeat("w"));
                    client.send(RpcMessage.progressReport("w", i, 0, 0, true));
                    RpcMessage resp = client.call(RpcMessage.stageReport("w", i, true, 0, i, null));
                    assertEquals(RpcMessage.Type.JOB_RESULT, resp.type(), "call " + i + " returned a response of the wrong type (frames out of alignment)");
                    assertEquals(i, resp.rowCount(), "call " + i + " read a response belonging to a different message (off-by-one frame alignment)");
                }
            }
        }
    }

    @Test
    void masterHandleReturnsNullForOneWayTypes() {
        MasterServer master = new MasterServer(5_000L);
        try {
            java.util.function.Function<RpcMessage, RpcMessage> h = master.handler();
            assertNull(h.apply(RpcMessage.heartbeat("w")), "HEARTBEAT must return null (one-way, no reply frame)");
            assertNull(h.apply(RpcMessage.progressReport("w", 1L, 0, 0, true)),
                    "PROGRESS_REPORT must return null (one-way, no reply frame)");
            assertNull(h.apply(RpcMessage.deregisterWorker("w", "127.0.0.1", 1, 4, -1)),
                    "DEREGISTER_WORKER must return null: the close path cannot wait for an ACK and must not poison a reused connection");
            assertNotNull(h.apply(RpcMessage.registerWorker("w", "127.0.0.1", 1, 4)), "REGISTER still replies with ACK");
        } finally {
            master.close();
        }
    }
}

