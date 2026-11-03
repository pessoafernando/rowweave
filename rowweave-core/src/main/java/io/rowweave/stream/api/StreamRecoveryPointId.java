package io.rowweave.stream.api;

import java.util.Objects;

/** Typed stable identity of a completed checkpoint or savepoint. */
public final class StreamRecoveryPointId {
    public enum Kind { CHECKPOINT, SAVEPOINT }
    private final Kind kind;
    private final String value;

    private StreamRecoveryPointId(Kind kind, String value) {
        if (kind == null) throw new IllegalArgumentException("recovery point kind is required");
        this.kind = kind;
        this.value = StreamIdentifierSupport.validate(value, "recovery point ID");
    }

    public static StreamRecoveryPointId checkpoint(String value) {
        return new StreamRecoveryPointId(Kind.CHECKPOINT, value);
    }
    public static StreamRecoveryPointId savepoint(String value) {
        return new StreamRecoveryPointId(Kind.SAVEPOINT, value);
    }
    public Kind kind() { return kind; }
    public String value() { return value; }
    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamRecoveryPointId)) return false;
        StreamRecoveryPointId that = (StreamRecoveryPointId) other;
        return kind == that.kind && value.equals(that.value);
    }
    @Override public int hashCode() { return Objects.hash(kind, value); }
    @Override public String toString() { return kind + ":" + value; }
}
