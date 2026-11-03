package io.rowweave.bench;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
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

/** Same JVM, same data, alternating legacy keyRow encoding with direct projected encoding, verified against the oracle on every run. */
@Tag("profile")
class ProjectedKeySerializationPerformanceTest {

    private static final String PROJECTED_PROPERTY = "rowweave.key.serialization.projected";
    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema RESULT =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void compareLegacyAndProjectedKeysEndToEnd() throws Exception {
        long orders = Long.getLong("rowweave.bench.orders", 2_000_000L);
        int iterations = Integer.getInteger("rowweave.profile.iters", 3);
        int shards = Integer.getInteger("rowweave.profile.shards", 1);
        String previous = System.getProperty(PROJECTED_PROPERTY);
        Path work = Files.createTempDirectory("rowweave-projected-key-perf-");
        try {
            List<Path> parts = CsvDataGen.ordersSharded(work, orders, 200, 50, shards);
            List<String> oracle = oracle(parts);
            assertEquals(oracle, runOnce(work, parts, false, "warm-legacy").rows);
            assertEquals(oracle, runOnce(work, parts, true, "warm-projected").rows);

            long legacyBest = Long.MAX_VALUE;
            long projectedBest = Long.MAX_VALUE;
            for (int i = 0; i < iterations; i++) {
                boolean projectedFirst = (i & 1) != 0;
                RunResult first = runOnce(work, parts, projectedFirst, "iter-" + i + "-first");
                RunResult second = runOnce(work, parts, !projectedFirst, "iter-" + i + "-second");
                assertEquals(oracle, first.rows, "iteration=" + i + " first");
                assertEquals(oracle, second.rows, "iteration=" + i + " second");
                if (projectedFirst) {
                    projectedBest = Math.min(projectedBest, first.elapsedMs);
                    legacyBest = Math.min(legacyBest, second.elapsedMs);
                } else {
                    legacyBest = Math.min(legacyBest, first.elapsedMs);
                    projectedBest = Math.min(projectedBest, second.elapsedMs);
                }
            }

            System.out.printf("[] orders=%d shards=%d best-of=%d legacy=%dms projected=%dms speedup=%.2fx%n",
                    orders, shards, iterations, legacyBest, projectedBest,
                    (double) legacyBest / projectedBest);
        } finally {
            if (previous == null) {
                System.clearProperty(PROJECTED_PROPERTY);
            } else {
                System.setProperty(PROJECTED_PROPERTY, previous);
            }
            deleteRecursively(work.toFile());
        }
    }

    private static RunResult runOnce(Path dir, List<Path> parts, boolean projected, String label) {
        System.setProperty(PROJECTED_PROPERTY, Boolean.toString(projected));
        Path output = dir.resolve(label + "-" + projected + "-" + System.nanoTime() + ".tsv");
        Source[] inputs = new Source[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            inputs[i] = new FileSource(ORDER, parts.get(i).toString());
        }
        ExecutionEnvironment session = RowWeave.createEnvironment("projected-key-" + label);
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
