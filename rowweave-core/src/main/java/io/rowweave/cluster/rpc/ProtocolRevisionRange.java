package io.rowweave.cluster.rpc;

import java.util.Objects;

/** Inclusive protocol revision range advertised by one peer. */
public final class ProtocolRevisionRange {
    private final int minimum;
    private final int maximum;

    public ProtocolRevisionRange(int minimum, int maximum) {
        if (minimum < 0 || maximum < minimum) {
            throw new IllegalArgumentException("invalid protocol revision range");
        }
        this.minimum = minimum;
        this.maximum = maximum;
    }

    public int minimum() { return minimum; }
    public int maximum() { return maximum; }

    ProtocolRevisionRange intersect(ProtocolRevisionRange other) {
        int lower = Math.max(minimum, other.minimum);
        int upper = Math.min(maximum, other.maximum);
        return lower <= upper ? new ProtocolRevisionRange(lower, upper) : null;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ProtocolRevisionRange)) return false;
        ProtocolRevisionRange that = (ProtocolRevisionRange) other;
        return minimum == that.minimum && maximum == that.maximum;
    }

    @Override
    public int hashCode() { return Objects.hash(minimum, maximum); }
}
