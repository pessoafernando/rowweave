package io.rowweave.stream.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** One immutable, canonical and resource-bounded enumerator response. */
public final class StreamSplitDiscovery {
    private static final int HARD_MAX_CHANGES = 4_096;
    private final long discoveryEpoch;
    private final StreamDiscoveryCursor nextCursor;
    private final List<StreamSplitChange> changes;

    public StreamSplitDiscovery(long discoveryEpoch,
            StreamDiscoveryCursor nextCursor,
            List<StreamSplitChange> changes) {
        if (discoveryEpoch < 0L || nextCursor == null || changes == null
                || changes.size() > HARD_MAX_CHANGES) {
            throw new IllegalArgumentException("split discovery is invalid");
        }
        List<StreamSplitChange> copied = new ArrayList<>(changes.size());
        Set<StreamSplitChange> unique = new HashSet<>();
        for (StreamSplitChange change : changes) {
            if (change == null || !unique.add(change)) {
                throw new IllegalArgumentException(
                        "split discovery changes must be unique");
            }
            copied.add(change);
        }
        Collections.sort(copied);
        this.discoveryEpoch = discoveryEpoch;
        this.nextCursor = nextCursor;
        this.changes = Collections.unmodifiableList(copied);
    }

    public long discoveryEpoch() { return discoveryEpoch; }
    public StreamDiscoveryCursor nextCursor() { return nextCursor; }
    public List<StreamSplitChange> changes() { return changes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSplitDiscovery)) return false;
        StreamSplitDiscovery that = (StreamSplitDiscovery) other;
        return discoveryEpoch == that.discoveryEpoch
                && nextCursor.equals(that.nextCursor)
                && changes.equals(that.changes);
    }

    @Override public int hashCode() {
        return Objects.hash(discoveryEpoch, nextCursor, changes);
    }
}
