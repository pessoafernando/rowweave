package io.rowweave.stream.contract;

import io.rowweave.stream.spi.StreamDiscoveryCursor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Canonical checkpoint state for one Source coordinator. */
public final class StreamSourceCoordinatorState {
    private final String jobId;
    private final String attemptId;
    private final String sourceOperatorUid;
    private final long discoveryEpoch;
    private final StreamDiscoveryCursor cursor;
    private final List<StreamSourceCoordinatorSplit> splits;

    public StreamSourceCoordinatorState(String jobId, String attemptId,
            String sourceOperatorUid, long discoveryEpoch,
            StreamDiscoveryCursor cursor,
            List<StreamSourceCoordinatorSplit> splits) {
        this.jobId = StreamContractSupport.text(
                jobId, "source.coordinator.jobId", 256);
        this.attemptId = StreamContractSupport.text(
                attemptId, "source.coordinator.attemptId", 256);
        this.sourceOperatorUid = StreamContractSupport.text(
                sourceOperatorUid, "source.coordinator.operatorUid", 256);
        if (discoveryEpoch < -1L || cursor == null || splits == null
                || splits.size() > 1_000_000) {
            throw new IllegalArgumentException(
                    "source coordinator state is invalid");
        }
        List<StreamSourceCoordinatorSplit> copied =
                new ArrayList<>(splits.size());
        Set<String> identities = new HashSet<>();
        for (StreamSourceCoordinatorSplit split : splits) {
            if (split == null || !identities.add(split.splitId())) {
                throw new IllegalArgumentException(
                        "source coordinator split identities must be unique");
            }
            copied.add(split);
        }
        Collections.sort(copied);
        this.discoveryEpoch = discoveryEpoch;
        this.cursor = cursor;
        this.splits = Collections.unmodifiableList(copied);
    }

    public String jobId() { return jobId; }
    public String attemptId() { return attemptId; }
    public String sourceOperatorUid() { return sourceOperatorUid; }
    public long discoveryEpoch() { return discoveryEpoch; }
    public StreamDiscoveryCursor cursor() { return cursor; }
    public List<StreamSourceCoordinatorSplit> splits() { return splits; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSourceCoordinatorState)) return false;
        StreamSourceCoordinatorState that =
                (StreamSourceCoordinatorState) other;
        return discoveryEpoch == that.discoveryEpoch
                && jobId.equals(that.jobId)
                && attemptId.equals(that.attemptId)
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && cursor.equals(that.cursor)
                && splits.equals(that.splits);
    }

    @Override public int hashCode() {
        return Objects.hash(jobId, attemptId, sourceOperatorUid,
                discoveryEpoch, cursor, splits);
    }
}
