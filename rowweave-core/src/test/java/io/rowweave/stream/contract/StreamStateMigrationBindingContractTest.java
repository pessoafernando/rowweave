package io.rowweave.stream.contract;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamStateMigrationBindingContractTest {
    @Test
    void canonicalBindingRoundTripsAndDetectsCorruption() {
        StreamStateMigrationBinding binding = binding();

        byte[] frame = StreamStateMigrationBindingCodec.encode(binding);

        assertEquals(binding,
                StreamStateMigrationBindingCodec.decode(frame));
        assertArrayEquals(frame, StreamStateMigrationBindingCodec.encode(
                StreamStateMigrationBindingCodec.decode(frame)));
        byte[] corrupt = frame.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertCode("CHECKSUM_MISMATCH",
                () -> StreamStateMigrationBindingCodec.decode(corrupt));
    }

    @Test
    void stateIdentityAndSemanticsCannotDriftAcrossMigration() {
        StreamStateDescriptor source = state(
                "readout", "decimal-v1", 1);

        assertThrows(IllegalArgumentException.class,
                () -> new StreamStateMigrationBinding(
                        "ledger", source,
                        state("renamed", "decimal-v2", 2), digest('a'),
                        "state-migration", digest('b'),
                        "app.DecimalMigration", digest('c'), 4096L));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamStateMigrationBinding(
                        "ledger", source, source, digest('a'),
                        "state-migration", digest('b'),
                        "app.DecimalMigration", digest('c'), 4096L));
    }

    private static StreamStateMigrationBinding binding() {
        return new StreamStateMigrationBinding(
                "ledger", state("readout", "decimal-v1", 1),
                state("readout", "decimal-v2", 2), digest('a'),
                "state-migration", digest('b'),
                "app.DecimalMigration", digest('c'), 4096L);
    }

    private static StreamStateDescriptor state(
            String name, String serializer, int schemaVersion) {
        return new StreamStateDescriptor(
                name, StreamStateDescriptor.Kind.KEYED_VALUE,
                serializer, schemaVersion,
                StreamStateDescriptor.TtlMode.EVENT_TIME, 60_000L);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void assertCode(String code, Runnable action) {
        StreamContractException failure = assertThrows(
                StreamContractException.class, action::run);
        assertEquals(code, failure.code());
    }
}
