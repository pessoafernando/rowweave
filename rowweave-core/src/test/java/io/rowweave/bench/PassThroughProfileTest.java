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
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.JobClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** TE6: public API, split-aware, output-inclusive profile for a pure pass-through chain. */
@Tag("profile")
class PassThroughProfileTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void inputArrayFilterSelectProfile() throws Exception {
        long rows = Long.getLong("rowweave.bench.rows", 200_000L);
        int shards = Integer.getInteger("rowweave.bench.shards", 4);
        Path work = Files.createTempDirectory("rowweave-pass-through-profile-");
        try {
            List<Path> parts = CsvDataGen.ordersSharded(work, rows, 200, 50, shards);
            Fingerprint expected = expected(rows);

            run(work, parts, 1, null, "warm-local");
            Sample localP1 = run(work, parts, 1, null, "local-p1");
            Sample localP4 = run(work, parts, 4, null, "local-p4");

            Sample clusterP1;
            try (InProcessCluster cluster = new InProcessCluster(
                    1, work.resolve("cluster-p1-worker").toFile())) {
                clusterP1 = run(work, parts, 1, cluster, "cluster-p1");
            }
            Sample clusterP4;
            try (InProcessCluster cluster = new InProcessCluster(
                    4, work.resolve("cluster-p4-worker").toFile())) {
                clusterP4 = run(work, parts, 4, cluster, "cluster-p4");
            }

            assertEquals(expected, localP1.fingerprint);
            assertEquals(expected, localP4.fingerprint);
            assertEquals(expected, clusterP1.fingerprint);
            assertEquals(expected, clusterP4.fingerprint);

            System.out.printf("[TE6-PASS-THROUGH] rows=%d shards=%d "
                            + "localP1=%dms localP4=%dms clusterP1=%dms clusterP4=%dms "
                            + "clusterP4Throughput=%.1f rows/s%n",
                    rows, shards, localP1.elapsedMs, localP4.elapsedMs,
                    clusterP1.elapsedMs, clusterP4.elapsedMs,
                    rows * 1000.0d / Math.max(1L, clusterP4.elapsedMs));
        } finally {
            deleteRecursively(work.toFile());
        }
    }

    private static Sample run(Path work, List<Path> parts, int parallelism,
                              JobClient client, String label) {
        Source[] inputs = new Source[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            inputs[i] = new FileSource(ORDER, parts.get(i).toString());
        }
        Path output = work.resolve(label + "-" + System.nanoTime() + ".tsv");
        BatchSession session = client == null
                ? RowWeave.createBatchSession(label)
                : new BatchSession(label, label, client);
        session.setParallelism(parallelism);
        session.fromSource(inputs)
                .filter("amount >= 0")
                .select("id", "dept", "region", "amount")
                .sinkTo(new FileSink(ORDER, output.toString()));

        long started = System.nanoTime();
        session.execute();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        return new Sample(elapsedMs, fingerprint(output));
    }

    private static Fingerprint expected(long rows) {
        long idSum = Math.multiplyExact(rows, rows + 1L) / 2L;
        BigDecimal amountSum = BigDecimal.ZERO;
        for (long id = 1L; id <= rows; id++) {
            amountSum = amountSum.add(BigDecimal.valueOf(id % 997L));
        }
        return new Fingerprint(rows, idSum, amountSum);
    }

    private static Fingerprint fingerprint(Path output) {
        long count = 0L;
        long idSum = 0L;
        BigDecimal amountSum = BigDecimal.ZERO;
        try (FileSource.Reader reader = new FileSource(ORDER, output.toString()).openReader()) {
            while (reader.hasNext()) {
                DataRow row = reader.next();
                count++;
                idSum = Math.addExact(idSum, row.getLong(0));
                amountSum = amountSum.add(row.getBigDecimal(3));
            }
        }
        return new Fingerprint(count, idSum, amountSum);
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

    private static final class Sample {
        private final long elapsedMs;
        private final Fingerprint fingerprint;

        private Sample(long elapsedMs, Fingerprint fingerprint) {
            this.elapsedMs = elapsedMs;
            this.fingerprint = fingerprint;
        }
    }

    private static final class Fingerprint {
        private final long rows;
        private final long idSum;
        private final BigDecimal amountSum;

        private Fingerprint(long rows, long idSum, BigDecimal amountSum) {
            this.rows = rows;
            this.idSum = idSum;
            this.amountSum = amountSum;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Fingerprint)) {
                return false;
            }
            Fingerprint that = (Fingerprint) other;
            return rows == that.rows && idSum == that.idSum
                    && amountSum.compareTo(that.amountSum) == 0;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(rows);
            result = 31 * result + Long.hashCode(idSum);
            result = 31 * result + amountSum.stripTrailingZeros().hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Fingerprint{rows=" + rows + ", idSum=" + idSum
                    + ", amountSum=" + amountSum.toPlainString() + '}';
        }
    }
}
