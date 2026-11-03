package io.rowweave.soak;

import io.rowweave.StreamJobHandle;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSoakBoundedJobTimeoutTest {
    @Test void timeoutReportsPublicStatusAndTaskEvidence() {
        StreamJobEvidence unavailable = StreamJobEvidence.unavailable(
                StreamJobId.of("s3-timeout"), StreamJobProgress.empty(),
                System.currentTimeMillis());
        StreamJobHandle job = job(StreamJobStatus.STARTING, unavailable,
                true);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> StreamSoakMain.awaitS3Finished(job, 1L));
        assertTrue(failure.getMessage().contains("status=STARTING"));
        assertTrue(failure.getMessage().contains("complete=false"));
        assertTrue(failure.getMessage().contains("reported=0/0"));
        assertTrue(failure.getCause() instanceof StreamControlException);
    }

    @Test void finishedJobNeedsNoDiagnosticQueries() {
        StreamJobHandle job = job(StreamJobStatus.FINISHED, null, false);
        assertDoesNotThrow(() -> StreamSoakMain.awaitS3Finished(job, 1L));
    }

    private static StreamJobHandle job(StreamJobStatus status,
            StreamJobEvidence evidence, boolean timeout) {
        return (StreamJobHandle) Proxy.newProxyInstance(
                StreamSoakBoundedJobTimeoutTest.class.getClassLoader(),
                new Class<?>[] { StreamJobHandle.class },
                (proxy, method, arguments) -> {
                    if ("awaitTermination".equals(method.getName())) {
                        if (timeout) {
                            throw new StreamControlException(
                                    StreamControlException.Code.WAIT_TIMEOUT,
                                    "bounded job did not finish");
                        }
                        return status;
                    }
                    if ("status".equals(method.getName())) return status;
                    if ("evidence".equals(method.getName())) return evidence;
                    throw new AssertionError("unexpected job method: "
                            + method.getName());
                });
    }
}
