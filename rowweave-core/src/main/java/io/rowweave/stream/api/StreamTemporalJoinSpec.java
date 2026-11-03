package io.rowweave.stream.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Finite event-time interval and stable key mapping for a temporal join. */
public final class StreamTemporalJoinSpec {
    private final String leftKeyFieldId;
    private final String rightKeyFieldId;
    private final long lowerBoundMillis;
    private final long upperBoundMillis;
    private final List<String> outputStableKeyFieldIds;

    public StreamTemporalJoinSpec(String leftKeyFieldId, String rightKeyFieldId,
            long lowerBoundMillis, long upperBoundMillis,
            List<String> outputStableKeyFieldIds) {
        if (leftKeyFieldId == null || leftKeyFieldId.isEmpty()
                || rightKeyFieldId == null || rightKeyFieldId.isEmpty()
                || lowerBoundMillis > upperBoundMillis
                || lowerBoundMillis == Long.MIN_VALUE
                || outputStableKeyFieldIds == null
                || outputStableKeyFieldIds.isEmpty()) {
            throw new IllegalArgumentException("invalid finite temporal join specification");
        }
        Set<String> unique = new HashSet<>();
        List<String> copied = new ArrayList<>(outputStableKeyFieldIds.size());
        for (String key : outputStableKeyFieldIds) {
            if (key == null || key.isEmpty() || !unique.add(key)) {
                throw new IllegalArgumentException(
                        "temporal join output keys must be non-empty and unique");
            }
            copied.add(key);
        }
        this.leftKeyFieldId = leftKeyFieldId;
        this.rightKeyFieldId = rightKeyFieldId;
        this.lowerBoundMillis = lowerBoundMillis;
        this.upperBoundMillis = upperBoundMillis;
        this.outputStableKeyFieldIds = Collections.unmodifiableList(copied);
    }

    public String leftKeyFieldId() { return leftKeyFieldId; }
    public String rightKeyFieldId() { return rightKeyFieldId; }
    public long lowerBoundMillis() { return lowerBoundMillis; }
    public long upperBoundMillis() { return upperBoundMillis; }
    public List<String> outputStableKeyFieldIds() { return outputStableKeyFieldIds; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamTemporalJoinSpec)) return false;
        StreamTemporalJoinSpec that = (StreamTemporalJoinSpec) other;
        return lowerBoundMillis == that.lowerBoundMillis
                && upperBoundMillis == that.upperBoundMillis
                && leftKeyFieldId.equals(that.leftKeyFieldId)
                && rightKeyFieldId.equals(that.rightKeyFieldId)
                && outputStableKeyFieldIds.equals(that.outputStableKeyFieldIds);
    }

    @Override public int hashCode() {
        return Objects.hash(leftKeyFieldId, rightKeyFieldId, lowerBoundMillis,
                upperBoundMillis, outputStableKeyFieldIds);
    }
}
