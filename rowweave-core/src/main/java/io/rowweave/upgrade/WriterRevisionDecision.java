package io.rowweave.upgrade;

/** Immutable cluster-wide writer revision intersection for one active store header. */
public final class WriterRevisionDecision {
    private final int activeRevision;
    private final int minimumCommonRevision;
    private final int maximumCommonRevision;

    WriterRevisionDecision(
            int activeRevision, int minimumCommonRevision,
            int maximumCommonRevision) {
        this.activeRevision = activeRevision;
        this.minimumCommonRevision = minimumCommonRevision;
        this.maximumCommonRevision = maximumCommonRevision;
    }

    public int activeRevision() { return activeRevision; }
    public int minimumCommonRevision() { return minimumCommonRevision; }
    public int maximumCommonRevision() { return maximumCommonRevision; }

    public int requireCurrent() {
        if (activeRevision < minimumCommonRevision
                || activeRevision > maximumCommonRevision) {
            throw rejected("ACTIVE_WRITER_REVISION_UNAVAILABLE",
                    "active writer revision is unsupported by a writable node");
        }
        return activeRevision;
    }

    public int requirePromotion(int targetRevision) {
        requireCurrent();
        if (targetRevision <= activeRevision) {
            throw rejected("WRITER_REVISION_NOT_PROMOTION",
                    "target writer revision must advance the active revision");
        }
        if (targetRevision < minimumCommonRevision
                || targetRevision > maximumCommonRevision) {
            throw rejected("WRITER_REVISION_UNAVAILABLE",
                    "target writer revision is unsupported by a writable node");
        }
        return targetRevision;
    }

    private static WriterRevisionRejectedException rejected(
            String code, String message) {
        return new WriterRevisionRejectedException(code, message);
    }
}
