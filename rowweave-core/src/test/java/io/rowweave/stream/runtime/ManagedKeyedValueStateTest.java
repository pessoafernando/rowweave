package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedKeyedValueStateTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void keyGroupHashHasAStableGoldenMapping() {
        assertEquals(10,
                ManagedKeyedValueState.keyGroup(bytes("adaptor-7"), 128));
        assertEquals(125,
                ManagedKeyedValueState.keyGroup(bytes("key-0"), 128));
    }

    @Test
    void canonicalKeyGroupsOwnershipAndMailboxThreadAreExact() throws Exception {
        byte[] ownedKey = keyInRange(16, 4, 7, true);
        byte[] foreignKey = keyInRange(16, 4, 7, false);
        MemoryManager memory = new MemoryManager(512);

        try (ManagedKeyedValueState state = state(
                descriptor(StreamStateDescriptor.TtlMode.NONE, 0L),
                16, 4, 7, memory, 64 * 1024L, new AtomicLong())) {
            assertTrue(ManagedKeyedValueState.keyGroup(ownedKey, 16) >= 4);
            assertTrue(ManagedKeyedValueState.keyGroup(ownedKey, 16) <= 7);

            StreamStateException foreign = assertThrows(
                    StreamStateException.class,
                    () -> state.put(foreignKey, bytes("window-a"),
                            bytes("not-written"), 0L));
            assertEquals(StreamStateException.Code.FOREIGN_KEY_GROUP,
                    foreign.code());
            assertEquals(0L, state.spillBytes());

            byte[] input = bytes("first");
            state.put(ownedKey, bytes("window-a"), input, 0L);
            input[0] = 'X';
            assertArrayEquals(bytes("first"),
                    state.get(ownedKey, bytes("window-a")));
            assertNull(state.get(ownedKey, bytes("window-b")));

            AtomicReference<Throwable> crossThread = new AtomicReference<>();
            Thread intruder = new Thread(() -> {
                try {
                    state.get(ownedKey, bytes("window-a"));
                } catch (Throwable failure) {
                    crossThread.set(failure);
                }
            });
            intruder.start();
            intruder.join(3_000L);
            assertFalse(intruder.isAlive());
            assertEquals(StreamStateException.Code.THREAD_OWNERSHIP,
                    ((StreamStateException) crossThread.get()).code());
        }
        assertEquals(0L, memory.reservedBytes());
    }

    @Test
    void boundedResidentCacheSpillsAndReplaysWithoutLosingUpdates() throws Exception {
        MemoryManager memory = new MemoryManager(384);
        ManagedKeyedValueState state = state(
                descriptor(StreamStateDescriptor.TtlMode.NONE, 0L),
                1, 0, 0, memory, 256 * 1024L, new AtomicLong());
        try {
            for (int i = 0; i < 24; i++) {
                state.put(bytes("key-" + i), bytes("ns"),
                        bytes("value-" + i + "-payload"), 0L);
            }
            state.put(bytes("key-3"), bytes("ns"), bytes("updated"), 0L);
            state.remove(bytes("key-4"), bytes("ns"));

            assertTrue(state.spillBytes() > 0L);
            assertTrue(state.residentBytes() <= memory.budget());
            assertTrue(memory.peakReservedBytes() <= memory.budget());
            state.spill();
            assertEquals(0L, state.residentBytes());
            assertArrayEquals(bytes("updated"),
                    state.get(bytes("key-3"), bytes("ns")));
            assertNull(state.get(bytes("key-4"), bytes("ns")));
            assertArrayEquals(bytes("value-23-payload"),
                    state.get(bytes("key-23"), bytes("ns")));
        } finally {
            state.close();
        }
        assertEquals(0L, memory.reservedBytes());
        try (Stream<Path> children = Files.list(temporaryDirectory)) {
            assertEquals(0L, children.count());
        }

        try (ManagedKeyedValueState limited = state(
                descriptor(StreamStateDescriptor.TtlMode.NONE, 0L),
                1, 0, 0, new MemoryManager(256), 8L,
                new AtomicLong())) {
            StreamStateException exhausted = assertThrows(
                    StreamStateException.class,
                    () -> limited.put(bytes("k"), bytes("n"),
                            bytes("v"), 0L));
            assertEquals(StreamStateException.Code.SPILL_LIMIT,
                    exhausted.code());
            assertEquals(0L, limited.spillBytes());
            assertNull(limited.get(bytes("k"), bytes("n")));
        }

        try (ManagedKeyedValueState churn = state(
                descriptor(StreamStateDescriptor.TtlMode.NONE, 0L),
                1, 0, 0, new MemoryManager(256), 128L,
                new AtomicLong())) {
            for (int i = 0; i < 100; i++) {
                churn.put(bytes("same-key"), bytes("same-ns"),
                        bytes("v" + i), 0L);
                assertTrue(churn.spillBytes() <= 128L,
                        "update churn must retain only current logical state");
            }
            assertArrayEquals(bytes("v99"),
                    churn.get(bytes("same-key"), bytes("same-ns")));
        }
    }

    @Test
    void eventAndProcessingTimeTtlExpireDeterministicallyWithoutRevival()
            throws Exception {
        byte[] key = bytes("adaptor-7");
        byte[] namespace = bytes("readout");
        try (ManagedKeyedValueState eventState = state(
                descriptor(StreamStateDescriptor.TtlMode.EVENT_TIME, 100L),
                1, 0, 0, new MemoryManager(512), 64 * 1024L,
                new AtomicLong())) {
            eventState.put(key, namespace, bytes("12.3400000000"), 1_000L);
            eventState.advanceWatermark(1_099L);
            assertArrayEquals(bytes("12.3400000000"),
                    eventState.get(key, namespace));
            eventState.advanceWatermark(1_100L);
            assertNull(eventState.get(key, namespace));

            byte[] overflowKey = bytes("far-future-adaptor");
            eventState.put(overflowKey, namespace, bytes("future"),
                    Long.MAX_VALUE - 10L);
            eventState.advanceWatermark(Long.MAX_VALUE);
            assertArrayEquals(bytes("future"),
                    eventState.get(overflowKey, namespace),
                    "TTL beyond the representable clock must not expire early");

            StreamStateException regression = assertThrows(
                    StreamStateException.class,
                    () -> eventState.advanceWatermark(1_099L));
            assertEquals(StreamStateException.Code.WATERMARK_REGRESSION,
                    regression.code());
        }

        AtomicLong clock = new AtomicLong(5_000L);
        try (ManagedKeyedValueState processingState = state(
                descriptor(StreamStateDescriptor.TtlMode.PROCESSING_TIME, 50L),
                1, 0, 0, new MemoryManager(512), 64 * 1024L, clock)) {
            processingState.put(key, namespace, bytes("visible"), 123L);
            clock.set(5_049L);
            assertArrayEquals(bytes("visible"),
                    processingState.get(key, namespace));
            clock.set(5_050L);
            assertNull(processingState.get(key, namespace));
            clock.set(1L);
            assertNull(processingState.get(key, namespace),
                    "processing-time rollback must not resurrect expired state");
        }
    }

    @Test
    void corruptWorkingStateCannotBeMaskedByAnOverwrite() throws Exception {
        try (ManagedKeyedValueState state = state(
                descriptor(StreamStateDescriptor.TtlMode.NONE, 0L),
                1, 0, 0, new MemoryManager(256), 64 * 1024L,
                new AtomicLong())) {
            state.put(bytes("key"), bytes("ns"), bytes("original"), 0L);
            state.spill();
            Path stateFile;
            try (Stream<Path> files = Files.walk(temporaryDirectory)) {
                stateFile = files.filter(path -> path.toString().endsWith(".state"))
                        .findFirst().orElseThrow(AssertionError::new);
            }
            Files.write(stateFile, new byte[] {0},
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);

            StreamStateException corrupt = assertThrows(
                    StreamStateException.class,
                    () -> state.put(bytes("key"), bytes("ns"),
                            bytes("replacement"), 0L));
            assertEquals(StreamStateException.Code.CORRUPT_SPILL,
                    corrupt.code());
        }
    }

    private ManagedKeyedValueState state(
            StreamStateDescriptor descriptor,
            int maxParallelism, int keyGroupStart, int keyGroupEnd,
            MemoryManager memory, long maximumSpillBytes,
            AtomicLong processingClock) {
        return new ManagedKeyedValueState(
                "artifact-1", "aggregate-by-adaptor", descriptor,
                maxParallelism, keyGroupStart, keyGroupEnd,
                memory, temporaryDirectory, maximumSpillBytes,
                128, 1024, processingClock::get);
    }

    private static StreamStateDescriptor descriptor(
            StreamStateDescriptor.TtlMode ttlMode, long ttlMillis) {
        return new StreamStateDescriptor(
                "readout", StreamStateDescriptor.Kind.KEYED_VALUE,
                "canonical-decimal-v1", 1, ttlMode, ttlMillis);
    }

    private static byte[] keyInRange(
            int maxParallelism, int start, int end, boolean inside) {
        for (int i = 0; i < 10_000; i++) {
            byte[] key = bytes("key-" + i);
            int group = ManagedKeyedValueState.keyGroup(key, maxParallelism);
            if ((group >= start && group <= end) == inside) return key;
        }
        throw new AssertionError("unable to find test key");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
