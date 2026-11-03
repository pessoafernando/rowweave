package io.rowweave.stream.api;

import java.util.Objects;
import java.util.UUID;

/** Caller-owned idempotency identity for a durable lifecycle command. */
public final class StreamCommandId {
    private final String value;
    private StreamCommandId(String value) {
        this.value = StreamIdentifierSupport.validate(value, "command ID");
    }
    public static StreamCommandId of(String value) { return new StreamCommandId(value); }
    public static StreamCommandId random() {
        return new StreamCommandId(UUID.randomUUID().toString());
    }
    public String value() { return value; }
    @Override public boolean equals(Object other) {
        return other instanceof StreamCommandId
                && value.equals(((StreamCommandId) other).value);
    }
    @Override public int hashCode() { return Objects.hash(value); }
    @Override public String toString() { return value; }
}
