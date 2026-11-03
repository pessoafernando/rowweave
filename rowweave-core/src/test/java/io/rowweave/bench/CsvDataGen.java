package io.rowweave.bench;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The CSV generator behind the large-data benchmarks, in test scope. Data is built in files on
 * disk rather than in memory or a database: a streaming BufferedWriter writes row by row, so
 * O(1) memory produces any scale without exhausting the heap. Fields are TAB separated and in
 * the column order of each table's schema.
 *
 * <p>Three tables, deterministic and reproducible:
 * <ul>
 * <li>orders.csv: id from 1 to N, dept as id modulo deptCount, region as id modulo regionCount,
 * and amount as id modulo 997</li>
 * <li>bonuses.csv: dept from 0 to deptCount-1 with bonus as dept times 10</li>
 * <li>partners.csv: id from 1 to N with pamount as id modulo 991, used by the large-to-large join on id</li>
 * </ul>
 */
public final class CsvDataGen {

    private CsvDataGen() {
    }

    /** Generates orders.csv with id, dept, region and amount, and returns the path. An existing file with a matching row count is reused, which avoids regenerating. */
    public static Path orders(Path dir, long rows, int deptCount, int regionCount) {
        Path f = dir.resolve("orders.csv");
        writeIfAbsent(f, rows, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                StringBuilder sb = new StringBuilder(64);
                for (long id = 1; id <= rows; id++) {
                    sb.setLength(0);
                    sb.append(id).append('\t')
                      .append(id % deptCount).append('\t')
                      .append(id % regionCount).append('\t')
                      .append(id % 997);
                    w.write(sb.toString());
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write orders.csv failed", e);
            }
        });
        return f;
    }

    /**
     * Generates sharded orders: {@code shards} files named orders.s{k}.csv, with rows distributed
     * round-robin by id for a total of {@code rows}. Parallel sources read them, one CsvInput per
     * shard collected into a list of sources, which is sharded inputs on a single machine and several
     * source instances in a cluster. Returns the shard paths in order from 0 to shards-1, and reuses
     * existing shards idempotently by checking the row count per shard.
     */
    public static java.util.List<Path> ordersSharded(Path dir, long rows, int deptCount, int regionCount, int shards) {
        java.util.List<Path> parts = new java.util.ArrayList<>(shards);
        // Rows per shard: the first rows modulo shards shards take one extra row, which is the round-robin split.
        for (int k = 0; k < shards; k++) {
            final int shard = k;
            long shardRows = rows / shards + (k < rows % shards ? 1 : 0);
            Path f = dir.resolve("orders.s" + k + ".csv");
            writeIfAbsent(f, shardRows, () -> {
                try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                    StringBuilder sb = new StringBuilder(64);
                    // Ids start at shard+1 and step by shards, so the union of all shards is exactly 1..rows with no repeats and none missing.
                    for (long id = shard + 1; id <= rows; id += shards) {
                        sb.setLength(0);
                        sb.append(id).append('\t')
                          .append(id % deptCount).append('\t')
                          .append(id % regionCount).append('\t')
                          .append(id % 997);
                        w.write(sb.toString());
                        w.write('\n');
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("write orders shard failed", e);
                }
            });
            parts.add(f);
        }
        return parts;
    }

    /** Generates bonuses.csv with dept and bonus, deptCount rows, which is the small side of the join. */
    public static Path bonuses(Path dir, int deptCount) {
        Path f = dir.resolve("bonuses.csv");
        writeIfAbsent(f, deptCount, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                for (int dept = 0; dept < deptCount; dept++) {
                    w.write(dept + "\t" + (dept * 10));
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write bonuses.csv failed", e);
            }
        });
        return f;
    }

    /** Generates partners.csv with id and pamount, rows of it, for the large-to-large join on id. */
    public static Path partners(Path dir, long rows) {
        Path f = dir.resolve("partners.csv");
        writeIfAbsent(f, rows, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                StringBuilder sb = new StringBuilder(32);
                for (long id = 1; id <= rows; id++) {
                    sb.setLength(0);
                    sb.append(id).append('\t').append(id % 991);
                    w.write(sb.toString());
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write partners.csv failed", e);
            }
        });
        return f;
    }

    /** Regenerates when the file is absent or its row count does not match, which is idempotent and lets a generated data set be reused across runs. */
    private static void writeIfAbsent(Path f, long expectedRows, Runnable gen) {
        try {
            if (Files.exists(f) && countLines(f) == expectedRows) {
                return;  // reuse the existing file
            }
        } catch (IOException ignored) {
        }
        gen.run();
    }

    /** Counts rows in a stream, without reading them into memory. */
    public static long countLines(Path f) throws IOException {
        long n = 0;
        try (java.io.BufferedReader r = Files.newBufferedReader(f, StandardCharsets.UTF_8)) {
            while (r.readLine() != null) {
                n++;
            }
        }
        return n;
    }
}
