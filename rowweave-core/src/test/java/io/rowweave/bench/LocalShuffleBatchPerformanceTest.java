package io.rowweave.bench;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Same JVM, same data, alternating row-at-a-time send with bounded batch send, verified against the oracle on every run. */
@Tag("profile")
class LocalShuffleBatchPerformanceTest {

    private static final String BATCH_PROPERTY = "rowweave.local.shuffle.batch";
    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema RESULT =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void compareBatchOffAndOnForOneAndFourSplits() throws Exception {
        long orders = Long.getLong("rowweave.bench.orders", 2_000_000L);
        int iterations = Integer.getInteger("rowweave.profile.iters", 3);
        String previous = System.getProperty(BATCH_PROPERTY);
        Path work = Files.createTempDirectory("rowweave-local-batch-perf-");
        try {
            compare(work.resolve("s1"), orders, 1, iterations);
            compare(work.resolve("s4"), orders, 4, iterations);
        } finally {
            if (previous == null) {
                System.clearProperty(BATCH_PROPERTY);
            } else {
                System.setProperty(BATCH_PROPERTY, previous);
            }
            deleteRecursively(work.toFile());
        }
    }

    private static void compare(Path dir, long orders, int shards, int iterations) throws Exception {
        Files.createDirectories(dir);
        List<Path> parts = CsvDataGen.ordersSharded(dir, orders, 200, 50, shards);
        List<String> oracle = oracle(parts);

        assertEquals(oracle, runOnce(dir, parts, false, "warm-off").rows);
        assertEquals(oracle, runOnce(dir, parts, true, "warm-on").rows);

        long offBest = Long.MAX_VALUE;
        long onBest = Long.MAX_VALUE;
        for (int i = 0; i < iterations; i++) {
            boolean onFirst = (i & 1) != 0;
            RunResult first = runOnce(dir, parts, onFirst, "iter-" + i + "-first");
            RunResult second = runOnce(dir, parts, !onFirst, "iter-" + i + "-second");
            assertEquals(oracle, first.rows, "shards=" + shards + " iteration=" + i + " first");
            assertEquals(oracle, second.rows, "shards=" + shards + " iteration=" + i + " second");
            if (onFirst) {
                onBest = Math.min(onBest, first.elapsedMs);
                offBest = Math.min(offBest, second.elapsedMs);
            } else {
                offBest = Math.min(offBest, first.elapsedMs);
                onBest = Math.min(onBest, second.elapsedMs);
            }
        }

        System.out.printf("[] orders=%d shards=%d best-of=%d batchOff=%dms batchOn=%dms speedup=%.2fx%n",
                orders, shards, iterations, offBest, onBest, (double) offBest / onBest);
    }

    private static RunResult runOnce(Path dir, List<Path> parts, boolean batch,
                                     String label) {
        System.setProperty(BATCH_PROPERTY, Boolean.toString(batch));
        Path output = dir.resolve(label + "-" + batch + "-" + System.nanoTime() + ".tsv");
        Source[] inputs = new Source[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            inputs[i] = new FileSource(ORDER, parts.get(i).toString());
        }
        BatchSession session = RowWeave.createBatchSession("batch-" + label);
        session.setParallelism(4);
        session.fromSource(inputs)
                .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount"))
                .sinkTo(new FileSink(RESULT, output.toString()));
        long started = System.nanoTime();
        session.execute();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        List<String> rows = fingerprint(output);
        try {
            Files.deleteIfExists(output);
        } catch (Exception ignored) {
            // the finally block still cleans the temporary tree.
        }
        return new RunResult(elapsedMs, rows);
    }

    private static List<String> oracle(List<Path> parts) {
        Map<Integer, BigDecimal> sums = new LinkedHashMap<>();
        for (Path part : parts) {
            try (FileSource.Reader reader = new FileSource(ORDER, part.toString()).openReader()) {
                while (reader.hasNext()) {
                    DataRow row = reader.next();
                    sums.merge(row.getInteger(2), row.getBigDecimal(3), BigDecimal::add);
                }
            }
        }
        List<String> rows = new ArrayList<>(sums.size());
        for (Map.Entry<Integer, BigDecimal> entry : sums.entrySet()) {
            rows.add(entry.getKey() + "="
                    + entry.getValue().setScale(10, RoundingMode.HALF_UP).toPlainString());
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> fingerprint(Path output) {
        List<String> rows = new ArrayList<>();
        try (FileSource.Reader reader = new FileSource(RESULT, output.toString()).openReader()) {
            while (reader.hasNext()) {
                DataRow row = reader.next();
                rows.add(row.getInteger(0) + "=" + row.getBigDecimal(1).toPlainString());
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    private static final class RunResult {
        final long elapsedMs;
        final List<String> rows;

        RunResult(long elapsedMs, List<String> rows) {
            this.elapsedMs = elapsedMs;
            this.rows = rows;
        }
    }
}
