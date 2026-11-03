package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArtifactLifecycleCursorTest {

    @Test
    void cursorRoundTripsAndSeparatesInventoryFromReferenceBackfill() {
        String digest = repeat('a', 64);
        ArtifactLifecycleCursor cursor = ArtifactLifecycleCursor.at(
                ArtifactLifecycleCursor.Kind.INVENTORY, 3L, 0xaa, digest);
        String encoded = cursor.encode();
        ArtifactLifecycleCursor decoded = ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.INVENTORY, encoded);
        assertEquals(cursor.kind(), decoded.kind());
        assertEquals(3L, decoded.cycle());
        assertEquals(0xaa, decoded.shard());
        assertEquals(digest, decoded.afterDigest());
        assertEquals(encoded, decoded.encode());
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCursor.decode(
                        ArtifactLifecycleCursor.Kind.REFERENCE_BACKFILL, encoded));

        ArtifactLifecycleCursor start = ArtifactLifecycleCursor.start(
                ArtifactLifecycleCursor.Kind.REFERENCE_BACKFILL);
        assertEquals(0L, start.cycle());
        assertEquals(0, start.shard());
        assertNull(start.afterDigest());
    }

    @Test
    void cursorRejectsCorruptionAliasesWrongShardAndOverflow() {
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCursor.decode(
                        ArtifactLifecycleCursor.Kind.INVENTORY, ""));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCursor.at(
                        ArtifactLifecycleCursor.Kind.INVENTORY, -1L, 0, null));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCursor.at(
                        ArtifactLifecycleCursor.Kind.INVENTORY, 0L, 0xbb,
                        repeat('a', 64)));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCursor.at(
                        ArtifactLifecycleCursor.Kind.INVENTORY, 0L, 0xaa,
                        repeat('A', 64)));

        ArtifactLifecycleCursor last = ArtifactLifecycleCursor.at(
                ArtifactLifecycleCursor.Kind.INVENTORY,
                Long.MAX_VALUE, 255, null);
        assertThrows(IllegalStateException.class, last::nextShard);

        ArtifactLifecycleCursor valid = ArtifactLifecycleCursor.at(
                ArtifactLifecycleCursor.Kind.INVENTORY, 1L, 0xaa, null);
        char[] token = valid.encode().toCharArray();
        token[token.length / 2] = token[token.length / 2] == 'A' ? 'B' : 'A';
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCursor.decode(
                        ArtifactLifecycleCursor.Kind.INVENTORY, new String(token)));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
