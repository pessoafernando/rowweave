package io.rowweave.bench;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Re-measures performance on the parallel path, comparing canonicalization on, which appends a
 * full ordering over every field, against off, which stops at multiset equivalence, and also the
 * speedup of parallelism 4 over serial parallelism 1. Both go through
 * {@link ExecutionEnvironment#commit()}, because a test uses the public entry point and never
 * constructs an internal execution object.
 *
 * <p>Tagged bench, so the default suite skips it. To run it by hand:
 * {@code mvn test -Dtest=CanonicalizeRetireBenchmarkTest -Dgroups=bench
 * -Drowweave.bench.orders=2000000 -Drowweave.bench.partners=2000000}.
 */
@Tag("bench")
class CanonicalizeRetireBenchmarkTest {

    private static final long ORDERS = Long.getLong("rowweave.bench.orders", 200_000L);
    private static final long PARTNERS = Long.getLong("rowweave.bench.partners", ORDERS);
    private static final int DEPTS = 200;
    private static final int REGIONS = 50;
    private static final int P = Integer.getInteger("rowweave.bench.p", 4);

    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema BONUS = new DataSchema(
            new Field("dept", DataType.IntegerType),
            new Field("bonus", DataType.IntegerType));
    private static final DataSchema PARTNER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("pamount", DataType.IntegerType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            ORDER, BONUS, new String[]{"dept"}, new String[]{"dept"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema BIG_JOIN = SortMergeJoin.of(
            ORDER, PARTNER, new String[]{"id"}, new String[]{"id"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema GB_REGION =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    private static Path dataDir;
    private static String ordersCsv, bonusesCsv, partnersCsv;

    private static Source orders()   { return new CsvInput(ordersCsv, ORDER); }
    private static Source bonuses()  { return new CsvInput(bonusesCsv, BONUS); }
    private static Source partners() { return new CsvInput(partnersCsv, PARTNER); }

    /** A scenario builds the graph with the DataSet API on the given ExecutionEnvironment and outputs, which commit executes. */
    @FunctionalInterface
    private interface Scene { void build(ExecutionEnvironment s, FileSink out); }

    private void ensureData() {
        if (ordersCsv != null) return;
        dataDir = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), "rowweave-bench-csv", "n" + ORDERS);
        try { Files.createDirectories(dataDir); } catch (Exception e) { throw new RuntimeException(e); }
        ordersCsv = CsvDataGen.orders(dataDir, ORDERS, DEPTS, REGIONS).toString();
        bonusesCsv = CsvDataGen.bonuses(dataDir, DEPTS).toString();
        partnersCsv = CsvDataGen.partners(dataDir, PARTNERS).toString();
    }

    private static long fileRows(Path p) {
        long n = 0;
        try (BufferedReader r = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            while (r.readLine() != null) n++;
        } catch (Exception e) { throw new RuntimeException(e); }
        return n;
    }

    /** Runs once through ExecutionEnvironment.execute(), writes to a unique temporary file, and returns the row count and the net milliseconds. A null canon uses the default of false. */
    private long[] runOnce(String tag, int parallelism, Boolean canon, DataSchema outMeta, Scene scene) {
        Path f;
        try { f = Files.createTempFile("bench-" + tag + "-", ".tsv"); } catch (Exception e) { throw new RuntimeException(e); }
        ExecutionEnvironment s = RowWeave.createEnvironment(tag);
        s.setParallelism(parallelism);
        if (canon != null) s.setCanonicalizeOutput(canon);
        long t = System.nanoTime();
        scene.build(s, new FileSink(outMeta, f.toString()));
        s.execute();
        long ms = (System.nanoTime() - t) / 1_000_000;
        long rows = fileRows(f);
        try { Files.deleteIfExists(f); } catch (Exception ignored) { }
        return new long[]{rows, ms};
    }

    /** Canonicalization on against off, both at parallelism 4, to isolate the cost of the trailing sort. */
    private void compareCanon(String name, DataSchema outMeta, Scene scene) {
        runOnce("warm", P, false, outMeta, scene);  // warm up
        runOnce("warm", P, true, outMeta, scene);
        long[] on = runOnce("canon-on", P, true, outMeta, scene);
        long[] off = runOnce("canon-off", P, false, outMeta, scene);
        double sp = off[1] == 0 ? 0 : (double) on[1] / off[1];
        System.out.printf("%-28s | out=%-10d | P%d canon ON=%7dms | OFF=%7dms | gain %.2fx%n",
                name, on[0], P, on[1], off[1], sp);
    }

    /** Parallelism 1 against 4, both with canonicalization off, to ask whether parallelism itself pays. */
    private void compareParallelism(String name, DataSchema outMeta, Scene scene) {
        runOnce("warm", 1, false, outMeta, scene);
        runOnce("warm", P, false, outMeta, scene);
        long[] p1 = runOnce("p1", 1, false, outMeta, scene);
        long[] p4 = runOnce("p" + P, P, false, outMeta, scene);
        double sp = p4[1] == 0 ? 0 : (double) p1[1] / p4[1];
        System.out.printf("%-28s | out=%-10d | P1=%7dms | P%d=%7dms | speedup %.2fx%n",
                name, p1[0], p1[1], P, p4[1], sp);
    }

    // ---- scenarios built with the public DataSet API ----
    private Scene sceneGroupBy() {
        return (s, out) -> s.fromSource(orders())
                .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount")).sinkTo(out);
    }
    private Scene sceneDistinct() {
        return (s, out) -> s.fromSource(orders())
                .distinct(io.rowweave.ops.Distinct.byColumns(ORDER, "dept", "region")).sinkTo(out);
    }
    private Scene sceneJoin() {
        return (s, out) -> s.fromSource(orders())
                .join(s.fromSource(bonuses()),
                        SortMergeJoin.of(ORDER, BONUS, new String[]{"dept"}, new String[]{"dept"}, JoinType.INNER))
                .sinkTo(out);
    }
    private Scene sceneBigJoin() {
        return (s, out) -> s.fromSource(orders())
                .join(s.fromSource(partners()),
                        SortMergeJoin.of(ORDER, PARTNER, new String[]{"id"}, new String[]{"id"}, JoinType.INNER))
                .sinkTo(out);
    }

    @Test
    void canonOnVsOff() {
        ensureData();
        System.out.println("=== canonicalize ON/OFF comparison (orders=" + ORDERS + ", P=" + P + ", ExecutionEnvironment.commit)===");
        compareCanon("join(orders x bonuses) large x small", JOIN, sceneJoin());
        compareCanon("bigJoin(orders x partners) large x large", BIG_JOIN, sceneBigJoin());
        compareCanon("groupBySum(region) small output", GB_REGION, sceneGroupBy());
    }

    @Test
    void parallelismP1vsP4() {
        ensureData();
        System.out.println("=== P1 vs P" + P + " speedup (orders=" + ORDERS + ", canonicalize=false, ExecutionEnvironment.commit)===");
        compareParallelism("groupBySum(region)", GB_REGION, sceneGroupBy());
        compareParallelism("distinct(dept,region)", ORDER, sceneDistinct());
        compareParallelism("join(orders x bonuses) large x small", JOIN, sceneJoin());
        compareParallelism("bigJoin(orders x partners) large x large", BIG_JOIN, sceneBigJoin());
    }
}
