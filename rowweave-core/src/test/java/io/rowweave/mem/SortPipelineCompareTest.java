package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wall-clock comparison between the three-thread PipelinedExternalSortBuffer and the
 * single-threaded PointerSortBuffer, forcing spilling on a large data set through the same
 * DiskSpillStore used in production, and measuring the three-thread gain. Tagged profile and
 * run by hand:
 * mvn test -Dtest=SortPipelineCompareTest -Dgroups=profile -Drowweave.test.excludedGroups=
 * [-Drowweave.sort.rows=5000000] [-Drowweave.sort.cardinality=1]
 * [-Drowweave.sort.keyType=long|string|decimal|timestamp]
 * [-Drowweave.sort.decimalPrecision=19, decimal only, while 0 keeps the compact long path]
 * [-Drowweave.sort.countFullComparisons=true, which wraps the comparator and deliberately turns
 * off the built-in encoded fast path to obtain a baseline]
 */
@Tag("profile")
class SortPipelineCompareTest {

    private static final SortField[] SORT = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};

    private static DataSchema meta(KeyType keyType) {
        return new DataSchema(
                new Field("k", keyType.dataType),
                new Field("amt", DataType.BigDecimalType),
                new Field("pad", DataType.StringType));
    }

    private static DataRow gen(long i, long cardinality, KeyType keyType, BigInteger decimalBase) {
        DataRow r = new DataRow(3);
        long key = (i * 2654435761L) % cardinality;
        switch (keyType) {
            case LONG:
                r.set(0, key);
                break;
            case STRING:
                r.set(0, "adaptor-key-" + key);
                break;
            case DECIMAL:
                r.set(0, decimalBase == null
                        ? BigDecimal.valueOf(key, 2)
                        : new BigDecimal(decimalBase.add(BigInteger.valueOf(key)), 2));
                break;
            case TIMESTAMP:
                Timestamp timestamp = new Timestamp(1_700_000_000_000L);
                timestamp.setNanos((int) (key % 1_000_000L));
                r.set(0, timestamp);
                break;
            default:
                throw new IllegalStateException("unknown key type " + keyType);
        }
        r.set(1, new BigDecimal(i % 10000).movePointLeft(2));
        r.set(2, "padding-col-to-inflate-row-" + (i % 97));  // widen the rows, to amplify spill I/O
        return r;
    }

    @Test
    void compareThreeThreadVsSingleThread(@TempDir Path tempDir) {
        long rows = Long.getLong("rowweave.sort.rows", 2_000_000L);
        long budget = Long.getLong("rowweave.sort.budget", 16L * 1024 * 1024);  // 16M, far below the data size, so spilling is forced heavily
        int runThreshold = 4096;
        int rounds = Integer.getInteger("rowweave.sort.rounds", 3);
        long cardinality = Long.getLong("rowweave.sort.cardinality", 1_000_000L);
        boolean countFullComparisons = Boolean.getBoolean("rowweave.sort.countFullComparisons");
        KeyType keyType = KeyType.parse(System.getProperty("rowweave.sort.keyType", "long"));
        int decimalPrecision = Integer.getInteger("rowweave.sort.decimalPrecision", 0);
        BigInteger decimalBase = decimalBase(keyType, decimalPrecision);
        DataSchema meta = meta(keyType);
        assertTrue(rounds >= 1, "profile rounds must be positive");
        assertTrue(cardinality >= 1, "profile cardinality must be positive");

        long warmRows = Math.min(50_000L, Math.max(1L, rows / 10));
        runSingle(meta, keyType, decimalBase, warmRows, cardinality, budget, runThreshold,
                countFullComparisons,
                tempDir.resolve("warm-single"));
        runPipelined(meta, keyType, decimalBase, warmRows, cardinality, budget, runThreshold, 3,
                countFullComparisons,
                tempDir.resolve("warm-pipelined"));

        List<ProfileSample> singles = new ArrayList<>();
        List<ProfileSample> pipelined = new ArrayList<>();
        for (int round = 0; round < rounds; round++) {
            if ((round & 1) == 0) {
                singles.add(runSingle(meta, keyType, decimalBase, rows, cardinality, budget, runThreshold,
                        countFullComparisons,
                        tempDir.resolve("single-" + round)));
                pipelined.add(runPipelined(meta, keyType, decimalBase, rows, cardinality, budget,
                        runThreshold, 3,
                        countFullComparisons,
                        tempDir.resolve("pipelined-" + round)));
            } else {
                pipelined.add(runPipelined(meta, keyType, decimalBase, rows, cardinality, budget,
                        runThreshold, 3,
                        countFullComparisons,
                        tempDir.resolve("pipelined-" + round)));
                singles.add(runSingle(meta, keyType, decimalBase, rows, cardinality, budget, runThreshold,
                        countFullComparisons,
                        tempDir.resolve("single-" + round)));
            }
        }
        ProfileSample single = median(singles);
        ProfileSample triple = median(pipelined);

        System.out.println("\n=== external sort phases (rows=" + rows + ", budget="
                + (budget / 1024 / 1024) + "M, cardinality=" + cardinality
                + ", keyType=" + keyType.name().toLowerCase()
                + ", decimalPrecision=" + decimalPrecision
                + ", rounds=" + rounds + ", Disk spill) ===");
        System.out.println("                         ingest     sort+merge       drain       total      runs       peak   full-compare");
        System.out.println("Pointer             " + single.format());
        System.out.println("Pipelined(3buf)     " + triple.format());
        System.out.printf("single/pipelined total = %.2fx%n",
                single.totalMillis() / (double) Math.max(1, triple.totalMillis()));
        System.out.println("===============================================================\n");

        assertEquals(rows, single.rows);
        assertEquals(rows, triple.rows);
        assertEquals(single.checksum, triple.checksum, "sorted output checksum");
        assertTrue(single.runs > 0 && triple.runs > 0, "both paths must really spill to disk");
        assertTrue(single.peakReserved <= budget && triple.peakReserved <= budget,
                "managed peak must stay within budget");
        assertTrue(triple.totalMillis() <= Math.max(1_000L, single.totalMillis() * 2L),
                "three-thread production spill path regressed by more than 2x: single="
                        + single.totalMillis() + "ms triple=" + triple.totalMillis() + "ms");
    }

    private static BigInteger decimalBase(KeyType keyType, int decimalPrecision) {
        assertTrue(decimalPrecision >= 0, "decimal precision must not be negative");
        if (keyType != KeyType.DECIMAL || decimalPrecision == 0) return null;
        assertTrue(decimalPrecision > 18,
                "configured decimal precision must exercise the arbitrary-precision encoding");
        return BigInteger.TEN.pow(decimalPrecision - 1);
    }

    private ProfileSample runSingle(DataSchema meta, KeyType keyType, BigInteger decimalBase, long rows,
                                    long cardinality, long budget,
                                    int runThreshold, boolean countFullComparisons, Path spillDir) {
        Comparator<DataRow> cmp = comparator(meta, countFullComparisons);
        DiskSpillStore spillStore = new DiskSpillStore(spillDir.toFile());
        MemoryManager memory = new MemoryManager(budget);
        PointerSortBuffer buf = new PointerSortBuffer(meta, cmp, SORT, new HeapPool(),
                memory, runThreshold, spillStore);
        try {
            long started = System.nanoTime();
            for (long i = 0; i < rows; i++) {
                buf.putRow(gen(i, cardinality, keyType, decimalBase));
            }
            long ingested = System.nanoTime();
            buf.sort();
            long sorted = System.nanoTime();
            long n = 0;
            long checksum = 1;
            for (DataRow row : buf) {
                n++;
                checksum = checksum * 31 + row.get(0).hashCode();
                checksum = checksum * 31 + row.get(1).hashCode();
                checksum = checksum * 31 + row.get(2).hashCode();
            }
            long drained = System.nanoTime();
            return new ProfileSample(ingested - started, sorted - ingested, drained - sorted,
                    n, checksum, spillStore.runsCreated(), memory.peakReservedBytes(), count(cmp));
        } finally {
            buf.close();
            spillStore.close();
            assertEquals(0, memory.reservedBytes());
            assertSpillDirEmpty(spillDir);
        }
    }

    private ProfileSample runPipelined(DataSchema meta, KeyType keyType, BigInteger decimalBase,
                                       long rows,
                                       long cardinality, long budget,
                                       int runThreshold, int nBuf, boolean countFullComparisons,
                                       Path spillDir) {
        Comparator<DataRow> cmp = comparator(meta, countFullComparisons);
        DiskSpillStore spillStore = new DiskSpillStore(spillDir.toFile());
        MemoryManager memory = new MemoryManager(budget);
        PipelinedExternalSortBuffer buf = new PipelinedExternalSortBuffer(meta, cmp, SORT,
                memory, runThreshold, nBuf, spillStore, HeapPool::new);
        try {
            long started = System.nanoTime();
            for (long i = 0; i < rows; i++) {
                buf.putRow(gen(i, cardinality, keyType, decimalBase));
            }
            long ingested = System.nanoTime();
            buf.sort();
            long sorted = System.nanoTime();
            long n = 0;
            long checksum = 1;
            for (DataRow row : buf) {
                n++;
                checksum = checksum * 31 + row.get(0).hashCode();
                checksum = checksum * 31 + row.get(1).hashCode();
                checksum = checksum * 31 + row.get(2).hashCode();
            }
            long drained = System.nanoTime();
            return new ProfileSample(ingested - started, sorted - ingested, drained - sorted,
                    n, checksum, spillStore.runsCreated(), memory.peakReservedBytes(), count(cmp));
        } finally {
            buf.close();
            assertEquals(0, memory.reservedBytes());
            assertSpillDirEmpty(spillDir);
        }
    }

    private static void assertSpillDirEmpty(Path spillDir) {
        java.io.File[] files = spillDir.toFile().listFiles();
        assertEquals(0, files == null ? 0 : files.length, "spill payload must be deleted");
    }

    private static Comparator<DataRow> comparator(DataSchema meta, boolean countFullComparisons) {
        RowComparator comparator = new RowComparator(meta, SORT);
        return countFullComparisons ? new CountingComparator(comparator) : comparator;
    }

    private static long count(Comparator<DataRow> comparator) {
        return comparator instanceof CountingComparator
                ? ((CountingComparator) comparator).count() : 0L;
    }

    private static ProfileSample median(List<ProfileSample> samples) {
        List<ProfileSample> sorted = new ArrayList<>(samples);
        sorted.sort(Comparator.comparingLong(ProfileSample::totalNanos));
        return sorted.get(sorted.size() / 2);
    }

    private static final class ProfileSample {
        final long ingestNanos;
        final long sortNanos;
        final long drainNanos;
        final long rows;
        final long checksum;
        final int runs;
        final long peakReserved;
        final long fullComparisons;

        private ProfileSample(long ingestNanos, long sortNanos, long drainNanos,
                              long rows, long checksum, int runs, long peakReserved,
                              long fullComparisons) {
            this.ingestNanos = ingestNanos;
            this.sortNanos = sortNanos;
            this.drainNanos = drainNanos;
            this.rows = rows;
            this.checksum = checksum;
            this.runs = runs;
            this.peakReserved = peakReserved;
            this.fullComparisons = fullComparisons;
        }

        long totalNanos() { return ingestNanos + sortNanos + drainNanos; }
        long totalMillis() { return totalNanos() / 1_000_000; }

        String format() {
            return String.format("%8dms %12dms %10dms %10dms %10d %10d %14d",
                    ingestNanos / 1_000_000, sortNanos / 1_000_000, drainNanos / 1_000_000,
                    totalMillis(), runs, peakReserved, fullComparisons);
        }
    }

    private static final class CountingComparator implements Comparator<DataRow> {
        private final Comparator<DataRow> delegate;
        private final AtomicLong comparisons = new AtomicLong();

        private CountingComparator(Comparator<DataRow> delegate) {
            this.delegate = delegate;
        }

        @Override
        public int compare(DataRow left, DataRow right) {
            comparisons.incrementAndGet();
            return delegate.compare(left, right);
        }

        private long count() {
            return comparisons.get();
        }
    }

    private enum KeyType {
        LONG(DataType.LongType),
        STRING(DataType.StringType),
        DECIMAL(DataType.BigDecimalType),
        TIMESTAMP(DataType.TimestampType);

        private final DataType dataType;

        KeyType(DataType dataType) {
            this.dataType = dataType;
        }

        private static KeyType parse(String configured) {
            for (KeyType value : values()) {
                if (value.name().equalsIgnoreCase(configured)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("unknown rowweave.sort.keyType: " + configured);
        }
    }
}
