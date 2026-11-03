package io.rowweave.stream.api;

import java.util.Objects;

/** Durable Stream job identity used to reattach after client restart. */
public final class StreamJobId {
    private final String value;
    private StreamJobId(String value) {
        this.value = StreamIdentifierSupport.validate(value, "job ID");
    }
    public static StreamJobId of(String value) { return new StreamJobId(value); }
    public String value() { return value; }
    @Override public boolean equals(Object other) {
        return other instanceof StreamJobId
                && value.equals(((StreamJobId) other).value);
    }
    @Override public int hashCode() { return Objects.hash(value); }
    @Override public String toString() { return value; }
}
