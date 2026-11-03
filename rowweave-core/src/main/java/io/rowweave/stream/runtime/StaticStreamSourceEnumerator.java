package io.rowweave.stream.runtime;

import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Built-in fixed-split adapter for the common Source enumerator protocol. */
public final class StaticStreamSourceEnumerator
        implements StreamSourceEnumerator {
    private final List<StreamSplitSpec> splits;

    public StaticStreamSourceEnumerator(List<StreamSplitSpec> splits) {
        if (splits == null || splits.isEmpty()
                || splits.size() > 4_096) {
            throw new IllegalArgumentException("fixed splits are invalid");
        }
        List<StreamSplitSpec> copied = new ArrayList<>(splits.size());
        for (StreamSplitSpec split : splits) {
            if (split == null) {
                throw new IllegalArgumentException("fixed split is required");
            }
            copied.add(split);
        }
        copied.sort(Comparator.comparing(StreamSplitSpec::splitId));
        this.splits = Collections.unmodifiableList(copied);
    }

    @Override
    public StreamSplitDiscovery discover(StreamDiscoveryCursor cursor,
            int limit) {
        if (cursor == null || limit < 1 || limit > 4_096) {
            throw new IllegalArgumentException("discovery request is invalid");
        }
        int offset = decodeOffset(cursor);
        int end = Math.min(splits.size(), Math.addExact(offset, limit));
        List<StreamSplitChange> changes = new ArrayList<>(end - offset);
        for (int index = offset; index < end; index++) {
            changes.add(StreamSplitChange.add(splits.get(index), 0L));
        }
        return new StreamSplitDiscovery(0L, cursor(end), changes);
    }

    @Override public void close() { }

    private int decodeOffset(StreamDiscoveryCursor cursor) {
        byte[] frame = cursor.frame();
        if (frame.length == 0) return 0;
        if (frame.length != 4) {
            throw new IllegalArgumentException("fixed cursor is invalid");
        }
        int offset = ByteBuffer.wrap(frame).getInt();
        if (offset < 0 || offset > splits.size()) {
            throw new IllegalArgumentException("fixed cursor is invalid");
        }
        return offset;
    }

    private static StreamDiscoveryCursor cursor(int offset) {
        return StreamDiscoveryCursor.of(
                ByteBuffer.allocate(4).putInt(offset).array());
    }
}
