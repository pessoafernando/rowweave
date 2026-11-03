package io.rowweave.stream.api;

import java.util.Objects;

/** Managed keyed-state declaration with explicit entry and byte ceilings. */
public final class StreamManagedStateSpec {
    public static final int MAX_ENTRIES_PER_KEY = 1_000_000;
    public static final long MAX_TOTAL_ENTRIES = 1_000_000_000L;
    public static final long MAX_TOTAL_BYTES = 1L << 40;
    public static final long MAX_ENTRY_BYTES = 16L * 1024L * 1024L;

    private final StreamStateSpec state;
    private final int maximumEntriesPerKey;
    private final long maximumTotalEntries;
    private final long maximumTotalBytes;
    private final long maximumEntryBytes;

    private StreamManagedStateSpec(StreamStateSpec state,
            int maximumEntriesPerKey, long maximumTotalEntries,
            long maximumTotalBytes, long maximumEntryBytes) {
        if (state == null || state.kind() == StreamStateSpec.Kind.OPERATOR_LIST
                || state.ttlMode() == StreamStateSpec.TtlMode.NONE
                || maximumEntriesPerKey <= 0
                || maximumEntriesPerKey > MAX_ENTRIES_PER_KEY
                || maximumTotalEntries < maximumEntriesPerKey
                || maximumTotalEntries > MAX_TOTAL_ENTRIES
                || maximumTotalBytes <= 0L
                || maximumTotalBytes > MAX_TOTAL_BYTES
                || maximumEntryBytes <= 0L
                || maximumEntryBytes > MAX_ENTRY_BYTES
                || maximumEntryBytes > maximumTotalBytes) {
            throw new IllegalArgumentException(
                    "managed keyed-state bounds are invalid");
        }
        this.state = state;
        this.maximumEntriesPerKey = maximumEntriesPerKey;
        this.maximumTotalEntries = maximumTotalEntries;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumEntryBytes = maximumEntryBytes;
    }

    public static StreamManagedStateSpec bounded(StreamStateSpec state,
            int maximumEntriesPerKey, long maximumTotalEntries,
            long maximumTotalBytes, long maximumEntryBytes) {
        return new StreamManagedStateSpec(state, maximumEntriesPerKey,
                maximumTotalEntries, maximumTotalBytes, maximumEntryBytes);
    }

    public StreamStateSpec state() { return state; }
    public int maximumEntriesPerKey() { return maximumEntriesPerKey; }
    public long maximumTotalEntries() { return maximumTotalEntries; }
    public long maximumTotalBytes() { return maximumTotalBytes; }
    public long maximumEntryBytes() { return maximumEntryBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamManagedStateSpec)) return false;
        StreamManagedStateSpec that = (StreamManagedStateSpec) other;
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
