package io.rowweave.upgrade;

import java.util.Objects;

/** Fenced owner identity for one upgrade generation. */
public final class UpgradeOwner {
    private final long term;
    private final String masterId;
    private final String incarnationId;
    private final long fence;

    public UpgradeOwner(long term, String masterId, String incarnationId,
                        long fence) {
        if (term <= 0L) throw new IllegalArgumentException("owner term must be positive");
        UpgradeCodecSupport.requireText(masterId, "master id");
        UpgradeCodecSupport.requireText(incarnationId, "incarnation id");
        if (fence <= 0L) throw new IllegalArgumentException("owner fence must be positive");
        this.term = term;
        this.masterId = masterId;
        this.incarnationId = incarnationId;
        this.fence = fence;
    }

    public long term() { return term; }
    public String masterId() { return masterId; }
    public String incarnationId() { return incarnationId; }
    public long fence() { return fence; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof UpgradeOwner)) return false;
        UpgradeOwner that = (UpgradeOwner) other;
        return term == that.term && fence == that.fence
                && masterId.equals(that.masterId)
                && incarnationId.equals(that.incarnationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, masterId, incarnationId, fence);
    }
}
