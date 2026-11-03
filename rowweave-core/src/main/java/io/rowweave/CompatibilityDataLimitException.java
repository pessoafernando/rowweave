package io.rowweave;

/** A local compatibility materialization exceeded its explicit hard bound. */
public final class CompatibilityDataLimitException
        extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public enum Kind { ROWS, BYTES }

    private final Kind kind;
    private final long limit;
    private final long observed;
    private final String owner;

    public CompatibilityDataLimitException(
            Kind kind, long limit, long observed, String owner) {
        super(message(kind, limit, observed, owner));
        if (kind == null) throw new IllegalArgumentException("kind is required");
        if (limit < 1L || observed < 0L) {
            throw new IllegalArgumentException("limit observation is invalid");
        }
        CompatibilityDataLimits.requireOwner(owner);
        this.kind = kind;
        this.limit = limit;
        this.observed = observed;
        this.owner = owner;
    }

    private static String message(
            Kind kind, long limit, long observed, String owner) {
        if (kind == null) throw new IllegalArgumentException("kind is required");
        CompatibilityDataLimits.requireOwner(owner);
        return owner + " compatibility " + kind.name().toLowerCase()
                + " limit exceeded: limit=" + limit + " observed=" + observed;
    }

    public Kind kind() { return kind; }
    public long limit() { return limit; }
    public long observed() { return observed; }
    public String owner() { return owner; }
}
