package io.rowweave.soak;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;

/** Streaming source, breaker graph and deterministic decimal oracle for the soak. */
final class ManagedMemorySoakWorkload {
    private static final BigDecimal FRACTION =
            new BigDecimal("0.00000000005");
    private static final DataSchema INPUT_META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema OUTPUT_META = ParallelGroupByAgg
            .of(INPUT_META, "k").sum("amount").getOutputSchema();

    private ManagedMemorySoakWorkload() { }

    static String writeSource(Path path, int rows, int keys, int seed)
            throws IOException {
        validateShape(rows, keys);
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) Files.createDirectories(parent);
        FileSink output = new FileSink(INPUT_META, path.toString());
        Throwable primary = null;
        try {
            for (int i = 0; i < rows; i++) {
                DataRow row = new DataRow(2);
                row.set(0, key(i, keys, seed));
                row.set(1, amount(i, seed));
                output.collect(row);
            }
            output.close();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            try {
                output.abort();
            } catch (Throwable abortFailure) {
                failure.addSuppressed(abortFailure);
            }
            throw failure;
        } finally {
            if (primary == null && !Files.isRegularFile(path)) {
                throw new IllegalStateException(
                        "MANAGED_MEMORY_SOURCE_NOT_PUBLISHED: " + path);
            }
        }
        return sha256(path);
    }

    static PlanGraph graph(Path source, Path output, int parallelism) {
        if (source == null || output == null || parallelism <= 0) {
            throw new IllegalArgumentException(
                    "source, output and positive parallelism are required");
        }
        PlanNode input = PlanNode.sourceFromInputs(INPUT_META,
                Collections.singletonList(
                        new FileSource(INPUT_META, source.toString())));
        PlanNode grouped = PlanNode.unary("groupBySum", OUTPUT_META, 0)
                .listParam("keys", Collections.singletonList("k"))
                .param("measure", "amount").build();
        PlanNode distinct = PlanNode.unary("distinct", OUTPUT_META, 1)
                .listParam("keys", Arrays.asList("k", "sum0")).build();
        PlanNode ordered = PlanNode.unary("orderBy", OUTPUT_META, 2)
                .listParam("sort", Arrays.asList(
                        PlanOps.encodeSortField(new SortField(0,
                                SortField.Direction.ASC,
                                SortField.NullOrder.NULLS_FIRST)),
                        PlanOps.encodeSortField(new SortField(1,
                                SortField.Direction.ASC,
                                SortField.NullOrder.NULLS_FIRST))))
                .build();
        PlanNode sink = PlanNode.sink(OUTPUT_META, 3,
                new FileSink(OUTPUT_META, output.toString())).build();
        return new PlanGraph(Arrays.asList(
                input, grouped, distinct, ordered, sink), parallelism);
    }

    static OracleResult verify(Path output, int rows, int keys, int seed)
            throws IOException {
        validateShape(rows, keys);
        long[] baseSums = new long[keys];
        long[] counts = new long[keys];
        long nullBase = 0L;
        long nullCount = 0L;
        for (int i = 0; i < rows; i++) {
            long base = baseAmount(i, seed);
            if (isNullKey(i)) {
                nullBase = Math.addExact(nullBase, base);
                nullCount++;
            } else {
                int key = keyIndex(i, keys, seed);
                baseSums[key] = Math.addExact(baseSums[key], base);
                counts[key]++;
            }
        }
        long observed = 0L;
        MessageDigest oracleDigest = sha256Digest();
        try (FileSource.Reader reader = new FileSource(
                OUTPUT_META, output.toString()).openReader()) {
            requireNext(reader, null, expected(nullBase, nullCount), observed++,
                    oracleDigest);
            for (int key = 0; key < keys; key++) {
                if (counts[key] == 0L) {
                    throw mismatch("input did not cover key " + key);
                }
                requireNext(reader, keyText(key),
                        expected(baseSums[key], counts[key]), observed++,
                        oracleDigest);
            }
            if (reader.hasNext()) {
                throw mismatch("output contains extra rows");
            }
        } catch (RuntimeException malformed) {
            if (malformed instanceof IllegalStateException
                    && malformed.getMessage() != null
                    && malformed.getMessage().startsWith(
                    "MANAGED_MEMORY_ORACLE_MISMATCH:")) {
                throw malformed;
            }
            throw mismatch("output could not be decoded", malformed);
        }
        return new OracleResult(observed, sha256(output),
                hex(oracleDigest.digest()));
    }

    private static void requireNext(FileSource.Reader reader, String key,
                                    BigDecimal amount, long ordinal,
                                    MessageDigest oracleDigest) {
        if (!reader.hasNext()) {
            throw mismatch("missing row " + ordinal);
        }
        DataRow row = reader.next();
        Object actualKey = row.get(0);
        Object actualAmount = row.get(1);
        if (!(key == null ? actualKey == null : key.equals(actualKey))
                || !(actualAmount instanceof BigDecimal)
                || amount.compareTo((BigDecimal) actualAmount) != 0
                || ((BigDecimal) actualAmount).scale() != 10) {
            throw mismatch("row " + ordinal + " differs: key="
                    + actualKey + " amount=" + actualAmount);
        }
        String canonical = (key == null ? "~null~" : key) + "\t"
                + ((BigDecimal) actualAmount).toPlainString() + "\n";
        oracleDigest.update(canonical.getBytes(StandardCharsets.UTF_8));
    }

    private static BigDecimal expected(long base, long count) {
        return BigDecimal.valueOf(base).add(
                FRACTION.multiply(BigDecimal.valueOf(count)))
                .setScale(10, RoundingMode.HALF_UP);
    }

    private static Object key(int row, int keys, int seed) {
        return isNullKey(row) ? null : keyText(keyIndex(row, keys, seed));
    }

    private static boolean isNullKey(int row) { return row % 997 == 0; }

    private static int keyIndex(int row, int keys, int seed) {
        return Math.floorMod(row + seed, keys);
    }

    private static String keyText(int key) {
        return String.format(java.util.Locale.ROOT, "k%08d", key);
    }

    private static BigDecimal amount(int row, int seed) {
        return BigDecimal.valueOf(baseAmount(row, seed)).add(FRACTION);
    }

    private static long baseAmount(int row, int seed) {
        return Math.floorMod((long) row * 31L + seed, 1_000L);
    }

    private static void validateShape(int rows, int keys) {
        if (rows <= 0 || keys <= 0 || (long) rows < (long) keys * 4L) {
            throw new IllegalArgumentException(
                    "rows must cover every key at least four times");
        }
    }

    private static String sha256(Path path) throws IOException {
        MessageDigest digest = sha256Digest();
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[64 * 1024];
            for (int read; (read = input.read(buffer)) >= 0; ) {
                digest.update(buffer, 0, read);
            }
        }
        return hex(digest.digest());
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String hex(byte[] digest) {
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte value : digest) {
            hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
            hex.append(Character.forDigit(value & 0xf, 16));
        }
        return hex.toString();
    }

    private static IllegalStateException mismatch(String detail) {
        return new IllegalStateException(
                "MANAGED_MEMORY_ORACLE_MISMATCH: " + detail);
    }

    private static IllegalStateException mismatch(
            String detail, Throwable cause) {
        return new IllegalStateException(
                "MANAGED_MEMORY_ORACLE_MISMATCH: " + detail, cause);
    }

    static final class OracleResult {
        final long rows;
        final String sha256;
        final String oracleSha256;

        OracleResult(long rows, String sha256, String oracleSha256) {
            this.rows = rows;
            this.sha256 = sha256;
            this.oracleSha256 = oracleSha256;
        }
    }
}
