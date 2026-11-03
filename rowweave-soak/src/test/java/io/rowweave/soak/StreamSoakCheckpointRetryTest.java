package io.rowweave.soak;

import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamOperationStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSoakCheckpointRetryTest {
    @Test void retriesUncertainTransportWithTheSameCommandId()
            throws Exception {
        StreamCommandId command = StreamCommandId.of("checkpoint-1");
        AtomicInteger attempts = new AtomicInteger();
        List<StreamCommandId> observed = new ArrayList<>();
        StreamJobHandle job = job((StreamCommandId requested) -> {
            observed.add(requested);
            if (attempts.getAndIncrement() == 0) {
                throw new StreamControlException(
                        StreamControlException.Code.TRANSPORT_FAILURE,
                        "response was lost");
            }
            return checkpoint(requested, StreamOperationStatus.SUCCEEDED);
        });

        StreamSoakMain.triggerCheckpoint(job, command, 1L);

        assertEquals(2, attempts.get());
        assertEquals(java.util.Arrays.asList(command, command), observed);
    }

    @Test void doesNotRetryDefinitiveControlFailure() {
        StreamCommandId command = StreamCommandId.of("checkpoint-2");
        AtomicInteger attempts = new AtomicInteger();
        StreamJobHandle job = job((StreamCommandId requested) -> {
            attempts.incrementAndGet();
            throw new StreamControlException(
                    StreamControlException.Code.COMMAND_CONFLICT,
                    "different command payload");
        });

        assertThrows(StreamControlException.class,
                () -> StreamSoakMain.triggerCheckpoint(job, command, 1L));
        assertEquals(1, attempts.get());
    }

    @Test void rejectsDefinitivelyFailedCheckpoint() {
        StreamCommandId command = StreamCommandId.of("checkpoint-3");
        StreamJobHandle job = job((StreamCommandId requested) ->
                checkpoint(requested, StreamOperationStatus.FAILED));

        assertThrows(IllegalStateException.class,
                () -> StreamSoakMain.triggerCheckpoint(job, command, 1L));
    }

    private static StreamJobHandle job(CheckpointCall call) {
        return (StreamJobHandle) Proxy.newProxyInstance(
                StreamSoakCheckpointRetryTest.class.getClassLoader(),
                new Class<?>[] { StreamJobHandle.class },
                (proxy, method, arguments) -> {
                    if ("triggerCheckpoint".equals(method.getName())) {
                        return call.call((StreamCommandId) arguments[0]);
                    }
                    throw new AssertionError(
                            "unexpected job method: " + method.getName());
                });
    }

    private static StreamCheckpointHandle checkpoint(StreamCommandId command,
            StreamOperationStatus status) {
        return (StreamCheckpointHandle) Proxy.newProxyInstance(
                StreamSoakCheckpointRetryTest.class.getClassLoader(),
                new Class<?>[] { StreamCheckpointHandle.class },
                (proxy, method, arguments) -> {
                    if ("id".equals(method.getName())) return command;
                    if ("status".equals(method.getName())
                            || "awaitCompletion".equals(method.getName())) {
                        return status;
                    }
                    if ("checkpointId".equals(method.getName())) return "cp";
                    throw new AssertionError("unexpected checkpoint method: "
                            + method.getName());
                });
    }

    private interface CheckpointCall {
        StreamCheckpointHandle call(StreamCommandId command);
    }
}
