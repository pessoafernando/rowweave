package io.rowweave.bench;

import io.rowweave.CollectionSource;
import io.rowweave.CompatibilityDataLimits;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Median of three join profile runs in one JVM, on the same data and the same budget, using the production DiskSpillStore. */
@Tag("profile")
class JoinProfileTest {

    private static final DataSchema SIDE = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("payload", DataType.StringType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            SIDE, SIDE, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
    private static final long SMALL_BUILD_BUDGET = 64L * 1024 * 1024;
    private static final long FALLBACK_BUDGET = 512L * 1024;

    @Test
    void productionDiskMedianProfile() {
        int probeRows = Integer.getInteger("rowweave.join.profile.probeRows", 200_000);
        int buildRows = Integer.getInteger("rowweave.join.profile.buildRows", 256);
        List<DataRow> probe = repeatingRows(probeRows, buildRows, "probe-", 0);
        List<DataRow> build = uniqueRows(buildRows, "build-", 0);

        run("HASH", probe, build, SMALL_BUILD_BUDGET, probeRows);
        run("SORT_MERGE", probe, build, SMALL_BUILD_BUDGET, probeRows);
        run(null, probe, build, SMALL_BUILD_BUDGET, probeRows);

        long[] hash = new long[3];
        long[] smj = new long[3];
        long[] auto = new long[3];
        for (int round = 0; round < 3; round++) {
            if (round == 0) {
                hash[round] = run("HASH", probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
                smj[round] = run("SORT_MERGE", probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
                auto[round] = run(null, probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
            } else if (round == 1) {
                auto[round] = run(null, probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
                hash[round] = run("HASH", probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
                smj[round] = run("SORT_MERGE", probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
            } else {
                smj[round] = run("SORT_MERGE", probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
                auto[round] = run(null, probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
                hash[round] = run("HASH", probe, build, SMALL_BUILD_BUDGET, probeRows).nanos;
            }
        }

        long hashMedian = median(hash);
        long smjMedian = median(smj);
        long autoMedian = median(auto);
        double hashSpeedup = (double) smjMedian / hashMedian;
        double autoOverhead = (double) autoMedian / hashMedian;

        int fallbackRows = Integer.getInteger("rowweave.join.profile.fallbackRows", 5_000);
        List<DataRow> fallbackProbe = uniqueRows(fallbackRows, "left-", 0);
        List<DataRow> wideBuild = uniqueRows(fallbackRows, "right-", 512);
        run("HASH", fallbackProbe, wideBuild, FALLBACK_BUDGET, fallbackRows);
        run("SORT_MERGE", fallbackProbe, wideBuild, FALLBACK_BUDGET, fallbackRows);
        long[] fallback = new long[3];
        long[] fallbackSmj = new long[3];
        long fallbackSpills = 0;
        for (int round = 0; round < 3; round++) {
            Sample hashFallback = run("HASH", fallbackProbe, wideBuild, FALLBACK_BUDGET, fallbackRows);
            Sample directSmj = run("SORT_MERGE", fallbackProbe, wideBuild, FALLBACK_BUDGET, fallbackRows);
            fallback[round] = hashFallback.nanos;
            fallbackSmj[round] = directSmj.nanos;
            fallbackSpills += hashFallback.spillCount;
        }
        long fallbackMedian = median(fallback);
        long fallbackSmjMedian = median(fallbackSmj);
        double fallbackRatio = (double) fallbackMedian / fallbackSmjMedian;

        System.out.printf("%n===  Join profile (3-round median, production DiskSpillStore) ===%n");
        System.out.printf("small build: probe=%d build=%d budget=%dMiB%n",
                probeRows, buildRows, SMALL_BUILD_BUDGET / 1024 / 1024);
        System.out.printf("HASH=%dms SMJ=%dms AUTO=%dms HASH speedup=%.2fx AUTO/HASH=%.2fx%n",
                millis(hashMedian), millis(smjMedian), millis(autoMedian), hashSpeedup, autoOverhead);
        System.out.printf("fallback: rows/side=%d budget=%dKiB HASH->SMJ=%dms direct SMJ=%dms ratio=%.2fx spills=%d%n",
                fallbackRows, FALLBACK_BUDGET / 1024, millis(fallbackMedian),
                millis(fallbackSmjMedian), fallbackRatio, fallbackSpills);
        System.out.println("===============================================================================");

        assertTrue(hashSpeedup >= 1.5,
                "small-build HASH median must be at least 1.5x faster than SMJ; got " + hashSpeedup);
        assertTrue(fallbackSpills > 0, "tiny-budget HASH fixture must execute real disk fallback");
    }

    private static Sample run(String hint, List<DataRow> left, List<DataRow> right,
                              long budget, long expectedRows) {
        ManagedMemory memory = ManagedMemory.ofBytes(budget);
        AtomicLong outputRows = new AtomicLong();
        PlanNode.Builder join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k"));
        if (hint != null) join.param("joinHint", hint);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                profileSource(left), profileSource(right), join.build()));

        long started = System.nanoTime();
        ExecutionReport report = new PushDagExecutor(1, memory)
                .execute(graph, ignored -> outputRows.incrementAndGet());
        long elapsed = System.nanoTime() - started;

        assertEquals(expectedRows, outputRows.get());
        assertTrue(report.withinBudget(), report.summary());
        assertEquals(memory.totalBytes(), memory.availableBytes(), "operator budget must be returned");
        return new Sample(elapsed, report.spillCount());
    }

    private static PlanNode profileSource(List<DataRow> rows) {
        CompatibilityDataLimits limits = CompatibilityDataLimits.of(
                CompatibilityDataLimits.HARD_MAX_ROWS,
                CompatibilityDataLimits.HARD_MAX_BYTES);
        return PlanNode.sourceFromInputs(SIDE, Collections.singletonList(
                new CollectionSource(SIDE, rows, limits)));
    }

    private static List<DataRow> repeatingRows(int count, int keys, String prefix, int padding) {
        List<DataRow> rows = new ArrayList<>(count);
        String suffix = repeated('x', padding);
        for (int i = 0; i < count; i++) rows.add(row(i % keys, prefix + i + suffix));
        return rows;
    }

    private static List<DataRow> uniqueRows(int count, String prefix, int padding) {
        List<DataRow> rows = new ArrayList<>(count);
        String suffix = repeated('x', padding);
        for (int i = 0; i < count; i++) rows.add(row(i, prefix + i + suffix));
        return rows;
    }

    private static DataRow row(int key, String payload) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, payload);
        return row;
    }

    private static String repeated(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    private static long millis(long nanos) {
        return nanos / 1_000_000;
    }

    private static final class Sample {
        final long nanos;
        final long spillCount;
        Sample(long nanos, long spillCount) {
            this.nanos = nanos;
            this.spillCount = spillCount;
        }
    }
}
