package io.rowweave;

import java.util.Arrays;
import java.util.Objects;

/** Immutable identity and canonical frame for one supervised async attempt. */
public final class StreamAsyncRequest {
    private final String operationIdentity;
    private final int attempt;
    private final byte[] requestFrame;

    private StreamAsyncRequest(String operationIdentity, int attempt,
            byte[] requestFrame) {
        if (operationIdentity == null
                || !operationIdentity.matches(
                "[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
                || attempt <= 0 || requestFrame == null
                || requestFrame.length == 0) {
            throw new IllegalArgumentException(
                    "async request identity, attempt and frame are required");
        }
        this.operationIdentity = operationIdentity;
        this.attempt = attempt;
        this.requestFrame = requestFrame.clone();
    }

    public static StreamAsyncRequest of(String operationIdentity, int attempt,
            byte[] requestFrame) {
        return new StreamAsyncRequest(
                operationIdentity, attempt, requestFrame);
    }

    public String operationIdentity() { return operationIdentity; }
    public int attempt() { return attempt; }
    public byte[] requestFrame() { return requestFrame.clone(); }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamAsyncRequest)) return false;
        StreamAsyncRequest that = (StreamAsyncRequest) other;
        return attempt == that.attempt
                && operationIdentity.equals(that.operationIdentity)
                && Arrays.equals(requestFrame, that.requestFrame);
    }

    @Override public int hashCode() {
        return 31 * Objects.hash(operationIdentity, attempt)
                + Arrays.hashCode(requestFrame);
    }
}
