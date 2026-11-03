package io.rowweave.soak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSoakAcceleratedPurgeTest {
    @Test
    void countsOnlyRealDeletionWithBoundedRetainedAuthority(
            @TempDir Path root) throws Exception {
        Path version = Files.createDirectories(root.resolve("version-2"));
        for (int index = 3; index < 8; index++) {
            Files.write(version.resolve("snapshot." + index),
                    new byte[] {(byte) index});
        }
        for (int index : new int[] {1, 2, 4, 6, 7, 8}) {
            Files.write(version.resolve("log." + index),
                    new byte[] {(byte) index});
        }
        AtomicBoolean first = new AtomicBoolean(true);
        try (StreamSoakAcceleratedPurge purge = new StreamSoakAcceleratedPurge(
                root, (data, retain) -> {
                    if (first.getAndSet(false)) {
                        Files.delete(version.resolve("snapshot.3"));
                        Files.delete(version.resolve("snapshot.4"));
                        Files.delete(version.resolve("log.1"));
                        Files.delete(version.resolve("log.2"));
                    }
                })) {
            purge.runOnce();
            assertEquals(1L, purge.completed());
            String evidence = new String(Files.readAllBytes(
                    root.resolve("accelerated-purges.log")),
                    StandardCharsets.UTF_8);
            assertTrue(evidence.contains("snapshot.3"));
            assertTrue(evidence.contains("log.1"));
            assertThrows(IllegalStateException.class, purge::runOnce);
            assertEquals(1L, purge.completed());
        }
    }

    @Test
    void retriesRealPurgeWhenAuthorityRollsDuringFirstSweep(
            @TempDir Path root) throws Exception {
        Path version = Files.createDirectories(root.resolve("version-2"));
        for (int index = 3; index < 8; index++) {
            Files.write(version.resolve("snapshot." + index),
                    new byte[] {(byte) index});
        }
        for (int index : new int[] {1, 2, 4, 6, 7, 8}) {
            Files.write(version.resolve("log." + index),
                    new byte[] {(byte) index});
        }
        AtomicInteger calls = new AtomicInteger();
        try (StreamSoakAcceleratedPurge purge = new StreamSoakAcceleratedPurge(
                root, (data, retain) -> {
                    if (calls.incrementAndGet() == 1) {
                        Files.delete(version.resolve("snapshot.3"));
                        Files.delete(version.resolve("snapshot.4"));
                        Files.delete(version.resolve("log.1"));
                        Files.delete(version.resolve("log.2"));
                        Files.write(version.resolve("snapshot.8"), new byte[1]);
                        Files.write(version.resolve("log.9"), new byte[1]);
                    } else {
                        Files.delete(version.resolve("snapshot.8"));
                        Files.delete(version.resolve("log.9"));
                    }
                })) {
            purge.runOnce();
            assertEquals(2, calls.get());
            assertEquals(1L, purge.completed());
            assertTrue(Files.readAllLines(
                    root.resolve("accelerated-purges.log")).get(0)
                    .contains("snapshot.3,snapshot.4,snapshot.8"));
        }
    }

    @Test
    void waitsForInFlightSnapshotBeforeRetryingPurge(
            @TempDir Path root) throws Exception {
        Path version = Files.createDirectories(root.resolve("version-2"));
        for (int index = 3; index < 8; index++) {
            Files.write(version.resolve("snapshot." + index),
                    new byte[] {(byte) index});
        }
        for (int index : new int[] {1, 2, 4, 6, 7, 8}) {
            Files.write(version.resolve("log." + index),
                    new byte[] {(byte) index});
        }
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger waits = new AtomicInteger();
        AtomicBoolean snapshotFinished = new AtomicBoolean();
        try (StreamSoakAcceleratedPurge purge = new StreamSoakAcceleratedPurge(
                root, (data, retain) -> {
                    if (calls.incrementAndGet() == 1) {
                        Files.delete(version.resolve("snapshot.3"));
                        Files.delete(version.resolve("snapshot.4"));
                        Files.delete(version.resolve("log.1"));
                        Files.delete(version.resolve("log.2"));
                        Files.write(version.resolve("snapshot.8"), new byte[1]);
                        Files.write(version.resolve("log.9"), new byte[1]);
                    } else if (snapshotFinished.get()) {
                        Files.delete(version.resolve("snapshot.8"));
                        Files.delete(version.resolve("log.9"));
                    }
                }, () -> {
                    waits.incrementAndGet();
                    snapshotFinished.set(true);
                })) {
            purge.runOnce();
            assertEquals(2, calls.get());
            assertEquals(1, waits.get());
            assertEquals(1L, purge.completed());
        }
    }

    @Test
    void failsAfterBoundedWaitsWhenSnapshotNeverFinishes(
            @TempDir Path root) throws Exception {
        Path version = Files.createDirectories(root.resolve("version-2"));
        for (int index = 3; index < 8; index++) {
            Files.write(version.resolve("snapshot." + index),
                    new byte[] {(byte) index});
        }
        for (int index : new int[] {1, 2, 4, 6, 7, 8}) {
            Files.write(version.resolve("log." + index),
                    new byte[] {(byte) index});
        }
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger waits = new AtomicInteger();
        try (StreamSoakAcceleratedPurge purge = new StreamSoakAcceleratedPurge(
                root, (data, retain) -> {
                    if (calls.incrementAndGet() == 1) {
                        Files.delete(version.resolve("snapshot.3"));
                        Files.delete(version.resolve("snapshot.4"));
                        Files.delete(version.resolve("log.1"));
                        Files.delete(version.resolve("log.2"));
                        Files.write(version.resolve("snapshot.8"), new byte[1]);
                        Files.write(version.resolve("log.9"), new byte[1]);
                    }
                }, waits::incrementAndGet)) {
            assertThrows(IllegalStateException.class, purge::runOnce);
            assertEquals(3, calls.get());
            assertEquals(2, waits.get());
            assertEquals(0L, purge.completed());
            assertTrue(Files.notExists(
                    root.resolve("accelerated-purges.log")));
        }
    }

    @Test
    void frozenScheduleLeavesTailAfterTwentyFourthPurge() {
        long last = StreamSoakAcceleratedPurge.lastPlannedOffsetMillis(
                300_000L, 600_000L, 24);
        assertEquals(14_100_000L, last);
        assertTrue(last < 14_400_000L);
    }
}
