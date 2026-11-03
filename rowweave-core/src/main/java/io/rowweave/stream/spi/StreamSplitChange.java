package io.rowweave.stream.spi;

import io.rowweave.stream.api.StreamSplitSpec;
import java.util.Objects;

/** Canonical split addition/removal with a generation that fences ABA reuse. */
public final class StreamSplitChange implements Comparable<StreamSplitChange> {
    public enum Kind { ADD, REMOVE }

    private final Kind kind;
    private final String splitId;
    private final long generation;
    private final StreamSplitSpec split;

    private StreamSplitChange(Kind kind, String splitId, long generation,
            StreamSplitSpec split) {
        if (kind == null || splitId == null || splitId.isEmpty()
                || generation < 0L
                || (kind == Kind.ADD) != (split != null)
                || (split != null && !splitId.equals(split.splitId()))) {
            throw new IllegalArgumentException("split change is invalid");
        }
        this.kind = kind;
        this.splitId = splitId;
        this.generation = generation;
        this.split = split;
    }

    public static StreamSplitChange add(StreamSplitSpec split,
            long generation) {
        if (split == null) {
            throw new IllegalArgumentException("added split is required");
        }
        return new StreamSplitChange(Kind.ADD, split.splitId(), generation,
                split);
    }

    public static StreamSplitChange remove(String splitId, long generation) {
        return new StreamSplitChange(Kind.REMOVE, splitId, generation, null);
    }

    public Kind kind() { return kind; }
    public String splitId() { return splitId; }
    public long generation() { return generation; }
    public StreamSplitSpec split() {
        if (split == null) {
            throw new IllegalStateException("removed split has no descriptor");
        }
        return split;
    }

    @Override public int compareTo(StreamSplitChange other) {
        int byId = splitId.compareTo(other.splitId);
        if (byId != 0) return byId;
        int byGeneration = Long.compare(generation, other.generation);
        if (byGeneration != 0) return byGeneration;
        return kind.compareTo(other.kind);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSplitChange)) return false;
        StreamSplitChange that = (StreamSplitChange) other;
        return generation == that.generation && kind == that.kind
                && splitId.equals(that.splitId)
                && Objects.equals(split, that.split);
    }

    @Override public int hashCode() {
        return Objects.hash(kind, splitId, generation, split);
    }
}
