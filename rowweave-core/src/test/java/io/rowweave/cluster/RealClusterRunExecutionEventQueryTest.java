package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunExecutionEvent;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RealClusterRunExecutionEventQueryTest {

    @Test
    void eachPageReresolvesLeaderAndReturnsTheExactValidatedPage() {
        AtomicInteger calls = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            assertEquals(RpcMessage.Type.QUERY_RUN_EVENTS, request.type());
            int call = calls.incrementAndGet();
            assertEquals(call == 1 ? 0L : 1L, request.eventAfterSequence());
            assertEquals(1, request.eventLimit());
            long sequence = call;
            return RpcMessage.runEventsPage(request.runId(), RunExecutionEventPageCodec.encode(
                    RunExecutionEventPage.found(sequence - 1L,
                            Collections.singletonList(event(sequence)), call == 1)));
        })) {
            AtomicInteger lookups = new AtomicInteger();
            RealClusterJobClient client = new RealClusterJobClient(() -> {
                lookups.incrementAndGet();
                return new Endpoint("127.0.0.1", server.port());
            });

            RunExecutionEventPage first = client.queryRunEvents("run-client-events", 0L, 1);
            RunExecutionEventPage second = client.queryRunEvents(
                    "run-client-events", first.nextAfterSequence(), 1);
            assertEquals(1L, first.events().get(0).sequence());
            assertEquals(2L, second.events().get(0).sequence());
            assertEquals(2, lookups.get());
            assertEquals(2, calls.get());
        }
    }

    @Test
    void invalidArgumentsFailBeforeLeaderLookup() {
        AtomicInteger lookups = new AtomicInteger();
        RealClusterJobClient client = new RealClusterJobClient(() -> {
            lookups.incrementAndGet();
            throw new AssertionError("invalid query must not locate a leader");
        });
        assertThrows(IllegalArgumentException.class,
                () -> client.queryRunEvents("run", -1L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> client.queryRunEvents("run", 0L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> client.queryRunEvents("run", 0L, 257));
        assertEquals(0, lookups.get());
    }

    @Test
    void failureUnexpectedTypeIdentityCursorAndCorruptPageNeverBecomeEmptyOrNotFound() {
        assertClientFailure(request -> RpcMessage.runEventsPageFailure(
                request.runId(), "authority unavailable"));
        assertClientFailure(request -> RpcMessage.ack());
        assertClientFailure(request -> RpcMessage.runEventsPage(
                "different-run", validPage(request.eventAfterSequence())));
        assertClientFailure(request -> RpcMessage.runEventsPage(
                request.runId(), RunExecutionEventPageCodec.encode(
                        RunExecutionEventPage.found(
                                request.eventAfterSequence() + 1L,
                                Collections.emptyList(), false))));
        assertClientFailure(request -> RpcMessage.runEventsPage(
                request.runId(), new byte[] {1, 2, 3}));
    }

    private static void assertClientFailure(
            java.util.function.Function<RpcMessage, RpcMessage> response) {
        try (RpcServer server = new RpcServer(response)) {
            RealClusterJobClient client = new RealClusterJobClient(
                    "127.0.0.1", server.port());
            assertThrows(RuntimeException.class,
                    () -> client.queryRunEvents("run", 0L, 1));
        }
    }

    private static byte[] validPage(long afterSequence) {
        return RunExecutionEventPageCodec.encode(
                RunExecutionEventPage.found(afterSequence,
                        Collections.singletonList(event(afterSequence + 1L)), false));
    }

    private static RunExecutionEvent event(long sequence) {
        return RunExecutionEvent.of(sequence, RunExecutionEvent.Type.ATTEMPT_ADVANCED,
                RunStatus.RUNNING, RunStatus.RUNNING,
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                1000L + sequence, RunExecutionEvent.DiagnosticCategory.NONE,
                "0001020304050607");
    }
}

