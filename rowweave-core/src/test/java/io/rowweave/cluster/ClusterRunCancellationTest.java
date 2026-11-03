package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterRunCancellationTest {

    @Test
    void eachCallReresolvesLeaderAndReturnsValidatedAuthorityResult() {
        AtomicInteger calls = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            assertEquals(RpcMessage.Type.CANCEL_RUN, request.type());
            assertEquals("request", request.cancelRequestId());
            int call = calls.incrementAndGet();
            return RpcMessage.runCancellationResult(new RunCancellationResult(
                    request.runId(), call == 1
                    ? RunCancellationResult.Disposition.ACCEPTED
                    : RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    RunStatus.CANCELING, 5L));
        })) {
            AtomicInteger lookups = new AtomicInteger();
            ClusterJobClient client = new ClusterJobClient(() -> {
                lookups.incrementAndGet();
                return new Endpoint("127.0.0.1", server.port());
            });
            assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                    client.cancelRun("run", "request").disposition());
            assertEquals(RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    client.cancelRun("run", "request").disposition());
            assertEquals(2, lookups.get());
        }
    }

    @Test
    void malformedArgumentsFailBeforeLeaderLookup() {
        AtomicInteger lookups = new AtomicInteger();
        ClusterJobClient client = new ClusterJobClient(() -> {
            lookups.incrementAndGet();
            throw new AssertionError("invalid cancellation must not locate leader");
        });
        assertThrows(IllegalArgumentException.class,
                () -> client.cancelRun("", "request"));
        assertThrows(IllegalArgumentException.class,
                () -> client.cancelRun("run", ""));
        assertThrows(IllegalArgumentException.class,
                () -> client.cancelRun("run", repeat('x', 129)));
        assertEquals(0, lookups.get());
    }

    @Test
    void failuresUnexpectedTypeAndIdentityMismatchNeverBecomeBusinessResults() {
        assertClientFailure(request -> RpcMessage.runCancellationFailure(
                request.runId(), "authority unavailable"));
        assertClientFailure(request -> RpcMessage.ack());
        assertClientFailure(request -> RpcMessage.runCancellationResult(
                new RunCancellationResult("different-run",
                        RunCancellationResult.Disposition.NOT_FOUND, null, 0L)));
    }

    private static void assertClientFailure(
            java.util.function.Function<RpcMessage, RpcMessage> response) {
        try (RpcServer server = new RpcServer(response)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", server.port());
            assertThrows(RuntimeException.class,
                    () -> client.cancelRun("run", "request"));
        }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}

