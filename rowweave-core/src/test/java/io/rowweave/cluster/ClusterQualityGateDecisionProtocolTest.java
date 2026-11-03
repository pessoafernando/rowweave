package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** query absence is distinct from authority and protocol failure. */
class ClusterQualityGateDecisionProtocolTest {

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void malformedFailureAndIdentityMismatchNeverBecomeNull() {
        AtomicInteger requests = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            requests.incrementAndGet();
            assertEquals(RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
                    request.type());
            switch (request.runId()) {
                case "missing":
                    return RpcMessage.qualityGateDecisionNotFound(request.runId());
                case "unavailable":
                    return RpcMessage.qualityGateDecisionFailure(
                            request.runId(), "AUTHORITY_UNAVAILABLE");
                case "mismatched":
                    return RpcMessage.qualityGateDecisionNotFound("another-run");
                default:
                    return RpcMessage.qualityGateDecision(
                            request.runId(), new byte[] {1, 2, 3});
            }
        });
             ClusterJobClient client = new ClusterJobClient(
                     "127.0.0.1", server.port())) {
            assertNull(client.queryQualityGateDecision("missing"));
            assertThrows(RuntimeException.class,
                    () -> client.queryQualityGateDecision("unavailable"));
            assertThrows(RuntimeException.class,
                    () -> client.queryQualityGateDecision("mismatched"));
            assertThrows(RuntimeException.class,
                    () -> client.queryQualityGateDecision("malformed"));
            assertEquals(4, requests.get());
        }
    }
}
