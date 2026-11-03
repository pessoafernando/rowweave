package io.rowweave.stream.api;

import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSourceEnumeratorContext;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** public dynamic Source values are inert, canonical and bounded. */
class StreamDynamicSourceApiTest {

    @Test
    void discoveryValuesAreCanonicalImmutableAndGenerationAware() {
        byte[] cursorFrame = new byte[] {1, 2};
        byte[] descriptor = new byte[] {3, 4};
        StreamDiscoveryCursor cursor = StreamDiscoveryCursor.of(cursorFrame);
        StreamSplitSpec split = new StreamSplitSpec(repeat('b'), "position-v1",
                descriptor);
        List<StreamSplitChange> changes = new ArrayList<>(Arrays.asList(
                StreamSplitChange.remove(repeat('c'), 8L),
                StreamSplitChange.add(split, 7L)));

        StreamSplitDiscovery discovery = new StreamSplitDiscovery(
                11L, cursor, changes);
        cursorFrame[0] = 99;
        descriptor[0] = 99;
        changes.clear();

        assertEquals(11L, discovery.discoveryEpoch());
        assertArrayEquals(new byte[] {1, 2},
                discovery.nextCursor().frame());
        assertEquals(repeat('b'), discovery.changes().get(0).splitId());
        assertEquals(StreamSplitChange.Kind.ADD,
                discovery.changes().get(0).kind());
        assertEquals(7L, discovery.changes().get(0).generation());
        assertArrayEquals(new byte[] {3, 4}, discovery.changes().get(0)
                .split().descriptorFrame());
        assertThrows(UnsupportedOperationException.class,
                () -> discovery.changes().clear());
        assertThrows(IllegalArgumentException.class,
                () -> new StreamSplitDiscovery(0L,
                        StreamDiscoveryCursor.initial(), Arrays.asList(
                        StreamSplitChange.remove(repeat('a'), 1L),
                        StreamSplitChange.remove(repeat('a'), 1L))));
    }

    @Test
    void discoveryResourcesAndEnumeratorContextRejectUnboundedInputs() {
        StreamSourceDiscoverySpec limits = new StreamSourceDiscoverySpec(
                100L, 1_000L, 16, 8, 4, 32, 64);
        byte[] configuration = new byte[] {5, 6};
        StreamSourceEnumeratorContext context =
                new StreamSourceEnumeratorContext(
                        "dynamic-source", configuration, limits);
        configuration[0] = 99;

        assertEquals(16, limits.maximumKnownSplits());
        assertEquals(8, limits.maximumPendingSplits());
        assertEquals(4, limits.maximumChangesPerDiscovery());
        assertEquals(32, limits.maximumCursorBytes());
        assertEquals(64, limits.maximumDescriptorBytes());
        assertArrayEquals(new byte[] {5, 6}, context.configurationFrame());
        assertEquals(limits, context.discoverySpec());

        assertThrows(IllegalArgumentException.class,
                () -> new StreamSourceDiscoverySpec(
                        0L, 1_000L, 16, 8, 4, 32, 64));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamSourceDiscoverySpec(
                        100L, 1_000L, 16, 17, 4, 32, 64));
        assertThrows(IllegalArgumentException.class,
                () -> StreamDiscoveryCursor.of(new byte[33], 32));
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }
}
