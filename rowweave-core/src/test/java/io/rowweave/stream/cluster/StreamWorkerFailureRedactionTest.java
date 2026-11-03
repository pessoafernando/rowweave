package io.rowweave.stream.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Connector exceptions must not echo credential or row values on Worker RPC. */
class StreamWorkerFailureRedactionTest {
    @Test
    void commandFailureReturnsOnlyStableCode() {
        IllegalArgumentException providerFailure =
                new IllegalArgumentException("password=secret-value",
                        new IllegalStateException("row=private-value"));

        StreamRemoteWorkerWire.Response response =
                StreamRemoteWorkerServer.failure(providerFailure);

        assertEquals(StreamRemoteWorkerWire.ErrorCode.ADMISSION_REJECTED,
                response.errorCode);
        assertEquals("Stream Worker rejected the fenced command: "
                + "ADMISSION_REJECTED", response.errorMessage);
        assertFalse(response.errorMessage.contains("secret-value"));
        assertFalse(response.errorMessage.contains("private-value"));
    }

    @Test
    void taskStatusNeverFormatsProviderExceptionChain() {
        Throwable providerFailure = new IllegalStateException(
                "token=secret-value", new IllegalArgumentException(
                "row=private-value"));

        String summary = StreamRemoteWorkerServer.failureDetail(
                providerFailure);

        assertEquals("TASK_FAILURE", summary);
        assertFalse(summary.contains("secret-value"));
        assertFalse(summary.contains("private-value"));
    }

    @Test
    void taskDiagnosticsExposeOnlyStableFailureCodes() {
        Throwable coded = new IllegalArgumentException(
                "POSTGRES_CDC_PROTOCOL_INVALID");
        Throwable unsafe = new IllegalStateException(
                "POSTGRES_CDC_PROTOCOL_INVALID token=secret-value");

        String codedSummary =
                StreamRemoteWorkerServer.safeFailureLocations(coded);
        String unsafeSummary =
                StreamRemoteWorkerServer.safeFailureLocations(unsafe);

        assertEquals(true, codedSummary.contains(
                "[POSTGRES_CDC_PROTOCOL_INVALID]"));
        assertFalse(unsafeSummary.contains("POSTGRES_CDC_PROTOCOL_INVALID"));
        assertFalse(unsafeSummary.contains("secret-value"));
    }
}
