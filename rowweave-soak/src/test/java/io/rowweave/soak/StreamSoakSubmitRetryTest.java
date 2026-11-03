package io.rowweave.soak;

import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamSubmissionId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSoakSubmitRetryTest {
    @Test void retriesUncertainSubmissionWithTheSameIdentityAndArtifact()
            throws Exception {
        StreamSubmissionId id = StreamSubmissionId.of("s3-retry");
        PlanArtifact artifact = artifact();
        StreamJobHandle expected = handle();
        AtomicInteger attempts = new AtomicInteger();
        List<StreamSubmissionId> ids = new ArrayList<>();
        List<PlanArtifact> artifacts = new ArrayList<>();
        StreamJobClient client = client((requestedId, requestedArtifact) -> {
            ids.add(requestedId);
            artifacts.add(requestedArtifact);
            if (attempts.getAndIncrement() == 0) {
                throw new StreamControlException(
                        StreamControlException.Code.TRANSPORT_FAILURE,
                        "the first Master committed but its reply was lost");
            }
            return expected;
        });

        assertSame(expected, StreamSoakMain.submit(client, id, artifact,
                120_000L));
        assertEquals(2, attempts.get());
        assertEquals(java.util.Arrays.asList(id, id), ids);
        assertEquals(java.util.Arrays.asList(artifact, artifact), artifacts);
    }

    @Test void doesNotRetryDefinitiveSubmissionConflict() {
        AtomicInteger attempts = new AtomicInteger();
        StreamJobClient client = client((id, artifact) -> {
            attempts.incrementAndGet();
            throw new StreamControlException(
                    StreamControlException.Code.SUBMISSION_CONFLICT,
                    "same identity has different content");
        });

        assertThrows(StreamControlException.class,
                () -> StreamSoakMain.submit(client,
                        StreamSubmissionId.of("s3-conflict"), artifact(),
                        120_000L));
        assertEquals(1, attempts.get());
    }

    @Test void stopsRetryingWhenTheRecoveryBudgetHasElapsed() {
        AtomicInteger attempts = new AtomicInteger();
        StreamJobClient client = client((id, artifact) -> {
            attempts.incrementAndGet();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            throw new StreamControlException(
                    StreamControlException.Code.TRANSPORT_FAILURE,
                    "the Master did not answer");
        });

        assertThrows(IllegalStateException.class,
                () -> StreamSoakMain.submit(client,
                        StreamSubmissionId.of("s3-budget"), artifact(), 1L));
        assertEquals(1, attempts.get());
    }

    private static StreamJobClient client(SubmitCall call) {
        return (StreamJobClient) Proxy.newProxyInstance(
                StreamSoakSubmitRetryTest.class.getClassLoader(),
                new Class<?>[] { StreamJobClient.class },
                (proxy, method, arguments) -> {
                    if ("submit".equals(method.getName())) {
                        return call.submit((StreamSubmissionId) arguments[0],
                                (PlanArtifact) arguments[1]);
                    }
                    throw new AssertionError("unexpected client method: "
                            + method.getName());
                });
    }

    private static StreamJobHandle handle() {
        return (StreamJobHandle) Proxy.newProxyInstance(
                StreamSoakSubmitRetryTest.class.getClassLoader(),
                new Class<?>[] { StreamJobHandle.class },
                (proxy, method, arguments) -> {
                    throw new AssertionError("unexpected handle method: "
                            + method.getName());
                });
    }

    private static PlanArtifact artifact() {
        return new PlanArtifact("stream-soak-submit-retry", 1, 1,
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                Collections.emptyList(), Collections.emptyList(),
                Collections.singletonList(new OperatorDescriptor(
                        "source", OperatorDescriptor.Kind.SOURCE, "test-source",
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        Collections.emptyList())),
                PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                new byte[] {1});
    }

    private interface SubmitCall {
        StreamJobHandle submit(StreamSubmissionId id, PlanArtifact artifact);
    }
}
