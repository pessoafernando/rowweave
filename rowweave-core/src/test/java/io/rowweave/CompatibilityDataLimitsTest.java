package io.rowweave;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The rows-plus-bytes guardrails around compatible materialization cannot be turned off. */
class CompatibilityDataLimitsTest {

    @Test
    void defaultsAndExplicitBoundsAreFinite() {
        CompatibilityDataLimits defaults = CompatibilityDataLimits.defaults();
        assertEquals(100_000, defaults.maxRows());
        assertEquals(64L * 1024L * 1024L, defaults.maxBytes());

        CompatibilityDataLimits hard = CompatibilityDataLimits.of(
                1_000_000, 512L * 1024L * 1024L);
        assertEquals(1_000_000, hard.maxRows());
        assertEquals(512L * 1024L * 1024L, hard.maxBytes());

        assertThrows(IllegalArgumentException.class,
                () -> CompatibilityDataLimits.of(0, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> CompatibilityDataLimits.of(1, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> CompatibilityDataLimits.of(1_000_001, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> CompatibilityDataLimits.of(
                        1, 512L * 1024L * 1024L + 1L));
    }

    @Test
    void admissionRejectsCurrentRowBeforeCounterMutation() {
        CompatibilityDataLimits limits = CompatibilityDataLimits.of(2, 100L);
        assertEquals(100L, limits.requireAdditional(
                1, 60L, 40L, "CollectingSink"));

        CompatibilityDataLimitException rows = assertThrows(
                CompatibilityDataLimitException.class,
                () -> limits.requireAdditional(2, 60L, 1L, "CollectingSink"));
        assertEquals(CompatibilityDataLimitException.Kind.ROWS, rows.kind());
        assertEquals(2L, rows.limit());
        assertEquals(3L, rows.observed());
        assertEquals("CollectingSink", rows.owner());

        CompatibilityDataLimitException bytes = assertThrows(
                CompatibilityDataLimitException.class,
                () -> limits.requireAdditional(1, 60L, 41L, "CollectionSource"));
        assertEquals(CompatibilityDataLimitException.Kind.BYTES, bytes.kind());
        assertEquals(100L, bytes.limit());
        assertEquals(101L, bytes.observed());
        assertEquals("CollectionSource", bytes.owner());
    }

    @Test
    void invalidUsageAndOverflowFailClosed() {
        CompatibilityDataLimits limits = CompatibilityDataLimits.of(2, 100L);
        assertThrows(IllegalArgumentException.class,
                () -> limits.requireAdditional(-1, 0L, 1L, "owner"));
        assertThrows(IllegalArgumentException.class,
                () -> limits.requireAdditional(0, -1L, 1L, "owner"));
        assertThrows(IllegalArgumentException.class,
                () -> limits.requireAdditional(0, 0L, -1L, "owner"));
        assertThrows(IllegalArgumentException.class,
                () -> limits.requireAdditional(0, 0L, 1L, ""));

        CompatibilityDataLimitException overflow = assertThrows(
                CompatibilityDataLimitException.class,
                () -> limits.requireAdditional(
                        0, Long.MAX_VALUE, 1L, "FileSink.read"));
        assertEquals(CompatibilityDataLimitException.Kind.BYTES,
                overflow.kind());
        assertEquals(Long.MAX_VALUE, overflow.observed());
    }

    @Test
    void limitsAndExceptionAreSerializationStable() throws Exception {
        CompatibilityDataLimits limits = roundTrip(
                CompatibilityDataLimits.of(7, 1024L));
        assertEquals(7, limits.maxRows());
        assertEquals(1024L, limits.maxBytes());

        CompatibilityDataLimitException exception = roundTrip(
                new CompatibilityDataLimitException(
                        CompatibilityDataLimitException.Kind.BYTES,
                        1024L, 1025L, "FileSink.read"));
        assertEquals(CompatibilityDataLimitException.Kind.BYTES,
                exception.kind());
        assertEquals(1024L, exception.limit());
        assertEquals(1025L, exception.observed());
        assertEquals("FileSink.read", exception.owner());
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }
}
