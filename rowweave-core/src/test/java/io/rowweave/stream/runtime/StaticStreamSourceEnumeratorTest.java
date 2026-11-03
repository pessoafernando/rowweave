package io.rowweave.stream.runtime;

import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Fixed splits must enter the same cursor-based enumerator protocol as dynamic splits. */
class StaticStreamSourceEnumeratorTest {

    @Test
    void fixedSplitsAreCanonicalPagedAndIdempotent() throws Exception {
        StaticStreamSourceEnumerator enumerator =
                new StaticStreamSourceEnumerator(Arrays.asList(
                        split('b', 2), split('a', 1)));

        StreamSplitDiscovery first = enumerator.discover(
                StreamDiscoveryCursor.initial(), 1);
        StreamSplitDiscovery replay = enumerator.discover(
                StreamDiscoveryCursor.initial(), 1);
        StreamSplitDiscovery second = enumerator.discover(
                first.nextCursor(), 1);
        StreamSplitDiscovery exhausted = enumerator.discover(
                second.nextCursor(), 1);

        assertEquals(first, replay);
        assertEquals(repeat('a'), first.changes().get(0).splitId());
        assertEquals(StreamSplitChange.Kind.ADD,
                first.changes().get(0).kind());
        assertEquals(0L, first.changes().get(0).generation());
        assertEquals(repeat('b'), second.changes().get(0).splitId());
        assertEquals(0, exhausted.changes().size());
        assertEquals(second.nextCursor(), exhausted.nextCursor());
    }

    private static StreamSplitSpec split(char id, int descriptor) {
        return new StreamSplitSpec(repeat(id), "position-v1",
                new byte[] {(byte) descriptor});
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }
}
