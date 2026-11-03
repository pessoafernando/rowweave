package io.rowweave.stream.api;

import java.util.Objects;
import java.util.UUID;

/** Caller-owned idempotency identity allocated before the first submit RPC. */
public final class StreamSubmissionId {
    private final String value;
    private StreamSubmissionId(String value) {
        this.value = StreamIdentifierSupport.validate(value, "submission ID");
    }
    public static StreamSubmissionId of(String value) { return new StreamSubmissionId(value); }
    public static StreamSubmissionId random() {
        return new StreamSubmissionId(UUID.randomUUID().toString());
    }
    public String value() { return value; }
    @Override public boolean equals(Object other) {
        return other instanceof StreamSubmissionId
                && value.equals(((StreamSubmissionId) other).value);
    }
    @Override public int hashCode() { return Objects.hash(value); }
    @Override public String toString() { return value; }
}
