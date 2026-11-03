package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamManagedStateSpec;

import java.util.Objects;

/** Portable resource envelope around one managed keyed-state descriptor. */
public final class StreamManagedStateDescriptor {
    private final StreamStateDescriptor state;
    private final int maximumEntriesPerKey;
    private final long maximumTotalEntries;
    private final long maximumTotalBytes;
    private final long maximumEntryBytes;

    public StreamManagedStateDescriptor(StreamStateDescriptor state,
            int maximumEntriesPerKey, long maximumTotalEntries,
            long maximumTotalBytes, long maximumEntryBytes) {
        if (state == null
                || state.kind() == StreamStateDescriptor.Kind.OPERATOR_LIST
                || state.ttlMode() == StreamStateDescriptor.TtlMode.NONE
                || maximumEntriesPerKey <= 0
                || maximumEntriesPerKey
                > StreamManagedStateSpec.MAX_ENTRIES_PER_KEY
                || maximumTotalEntries < maximumEntriesPerKey
                || maximumTotalEntries
                > StreamManagedStateSpec.MAX_TOTAL_ENTRIES
                || maximumTotalBytes <= 0L
                || maximumTotalBytes > StreamManagedStateSpec.MAX_TOTAL_BYTES
                || maximumEntryBytes <= 0L
                || maximumEntryBytes > StreamManagedStateSpec.MAX_ENTRY_BYTES
                || maximumEntryBytes > maximumTotalBytes) {
            throw StreamContractSupport.failure("INVALID_STATE_RESOURCE_LIMIT",
                    "nodes.process.states",
                    "managed state resource limits are invalid");
        }
        this.state = state;
        this.maximumEntriesPerKey = maximumEntriesPerKey;
        this.maximumTotalEntries = maximumTotalEntries;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumEntryBytes = maximumEntryBytes;
    }

    public StreamStateDescriptor state() { return state; }
    public int maximumEntriesPerKey() { return maximumEntriesPerKey; }
    public long maximumTotalEntries() { return maximumTotalEntries; }
    public long maximumTotalBytes() { return maximumTotalBytes; }
    public long maximumEntryBytes() { return maximumEntryBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamManagedStateDescriptor)) return false;
        StreamManagedStateDescriptor that =
                (StreamManagedStateDescriptor) other;
        return maximumEntriesPerKey == that.maximumEntriesPerKey
                && maximumTotalEntries == that.maximumTotalEntries
                && maximumTotalBytes == that.maximumTotalBytes
                && maximumEntryBytes == that.maximumEntryBytes
                && state.equals(that.state);
    }

    @Override public int hashCode() {
        return Objects.hash(state, maximumEntriesPerKey, maximumTotalEntries,
                maximumTotalBytes, maximumEntryBytes);
    }
}
