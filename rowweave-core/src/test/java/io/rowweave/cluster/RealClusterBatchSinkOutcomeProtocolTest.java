package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** malformed Sink outcome generations must fail closed at the client. */
class RealClusterBatchSinkOutcomeProtocolTest {

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void negativeGenerationIsAProtocolErrorInsteadOfAbsentEvidence() {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<RpcMessage> observed = new AtomicReference<>();
        byte[] identity = new byte[32];
        identity[0] = 17;
        try (RpcServer server = new RpcServer(request -> {
            requests.incrementAndGet();
            observed.set(request);
            return RpcMessage.batchSinkOutcome(
                    request.runId(), "COMMITTED", -1L, identity,
                    "MANIFEST_VISIBLE_VERIFIED");
        });
             RealClusterJobClient client = new RealClusterJobClient(
                     "127.0.0.1", server.port())) {

            assertThrows(RuntimeException.class,
                    () -> client.queryBatchSinkOutcome("negative-generation"));
            assertEquals(1, requests.get());
            assertNotNull(observed.get());
            assertEquals(RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME,
                    observed.get().type());
            assertEquals("negative-generation", observed.get().runId());
        }
    }
}
