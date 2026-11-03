package io.rowweave.soak;

import org.apache.zookeeper.server.PurgeTxnLog;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Real ZooKeeper purge on an accelerated schedule with direct file inventories. */
final class StreamSoakAcceleratedPurge implements AutoCloseable {
    private static final long RETRY_DELAY_MILLIS = 1_000L;

    interface PurgeAction {
        void purge(File data, int retainCount) throws Exception;
    }

    interface RetryPause {
        void await() throws InterruptedException;
    }

    private final Path data;
    private final Path log;
    private final PurgeAction action;
    private final RetryPause retryPause;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "stream-soak-accelerated-purge");
                thread.setDaemon(true);
                return thread;
            });
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private long completed;
    private boolean started;

    StreamSoakAcceleratedPurge(Path data) {
        this(data, (directory, retain) ->
                PurgeTxnLog.purge(directory, directory, retain));
    }

    StreamSoakAcceleratedPurge(Path data, PurgeAction action) {
        this(data, action, () -> Thread.sleep(RETRY_DELAY_MILLIS));
    }

    StreamSoakAcceleratedPurge(Path data, PurgeAction action,
            RetryPause retryPause) {
        this.data = data;
        this.log = data.resolve("accelerated-purges.log");
        this.action = action;
        this.retryPause = retryPause;
    }

    synchronized void start(long firstOffsetMillis,
            long intervalMillis) {
        if (started || firstOffsetMillis <= 0L || intervalMillis <= 0L) {
            throw new IllegalArgumentException("invalid accelerated purge schedule");
        }
        started = true;
        scheduler.scheduleAtFixedRate(() -> {
            try {
                runOnce();
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
                scheduler.shutdown();
            }
        }, firstOffsetMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    synchronized void runOnce() throws Exception {
        requireHealthy();
        long startedAt = System.currentTimeMillis();
        // A live ZooKeeper may roll a new snapshot while the sweep runs.
        // Keep every pre-sweep inventory so the evidence includes files
        // created and then deleted by a retry in this same scheduled event.
        SortedSet<String> before = new TreeSet<>();
        SortedSet<String> after = inventory();
        boolean retained = false;
        for (int attempt = 0; attempt < 3; attempt++) {
            before.addAll(after);
            action.purge(data.toFile(), 3);
            after = inventory();
            if (boundedRetained(after)) {
                retained = true;
                break;
            }
            if (attempt < 2) retryPause.await();
        }
        SortedSet<String> removed = new TreeSet<>(before);
        removed.removeAll(after);
        if (count(removed, "snapshot.") < 1
                || count(removed, "log.") < 1
                || !retained) {
            throw new IllegalStateException(
                    "STREAM_SOAK_ACCELERATED_PURGE_RETENTION_INVALID");
        }
        long finishedAt = System.currentTimeMillis();
        String payload = (completed + 1L) + "|" + startedAt + "|"
                + finishedAt + "|" + join(before) + "|" + join(after)
                + "|" + join(removed);
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        String frame = payload + "#" + sha256(bytes) + "\n";
        Files.write(log, frame.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND,
                StandardOpenOption.WRITE);
        completed++;
    }

    synchronized long completed() { return completed; }

    void requireHealthy() {
        Throwable error = failure.get();
        if (error != null) {
            throw new IllegalStateException(
                    "STREAM_SOAK_ACCELERATED_PURGE_FAILED", error);
        }
    }

    static long lastPlannedOffsetMillis(long firstOffsetMillis,
            long intervalMillis, int requiredEvents) {
        if (firstOffsetMillis <= 0L || intervalMillis <= 0L
                || requiredEvents < 1) {
            throw new IllegalArgumentException("invalid purge plan");
        }
        return Math.addExact(firstOffsetMillis,
                Math.multiplyExact(intervalMillis, requiredEvents - 1L));
    }

    private SortedSet<String> inventory() throws IOException {
        SortedSet<String> names = new TreeSet<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                data.resolve("version-2"))) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (Files.isRegularFile(file)
                        && (name.matches("snapshot\\.[0-9a-f]+")
                        || name.matches("log\\.[0-9a-f]+"))) {
                    names.add(name);
                }
            }
        }
        return names;
    }

    private static int count(SortedSet<String> names, String prefix) {
        int result = 0;
        for (String name : names) if (name.startsWith(prefix)) result++;
        return result;
    }

    private static boolean boundedRetained(SortedSet<String> names) {
        return count(names, "snapshot.") == 3
                && count(names, "log.") == 4
                && predecessorBound(names);
    }

    private static boolean predecessorBound(SortedSet<String> names) {
        List<Long> snapshots = new ArrayList<>();
        List<Long> logs = new ArrayList<>();
        for (String name : names) {
            long sequence = Long.parseLong(name.substring(
                    name.indexOf('.') + 1), 16);
            (name.startsWith("snapshot.") ? snapshots : logs)
                    .add(sequence);
        }
        java.util.Collections.sort(snapshots);
        java.util.Collections.sort(logs);
        return snapshots.size() == 3 && logs.size() == 4
                && logs.get(0) < snapshots.get(0)
                && snapshots.get(0) < logs.get(1);
    }

    private static String join(SortedSet<String> names) {
        return String.join(",", names);
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(bytes);
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest) {
                result.append(Character.forDigit((value >>> 4) & 0xf, 16));
                result.append(Character.forDigit(value & 0xf, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    @Override public void close() throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(5L, TimeUnit.SECONDS);
        requireHealthy();
    }
}
