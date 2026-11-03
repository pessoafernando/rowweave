package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class TenDagData {

    private TenDagData() { }

    public static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("grade", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType),
            new Field("qty", DataType.IntegerType));

    public static final DataSchema WIDE = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("grade", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType),
            new Field("qty", DataType.IntegerType),
            new Field("active", DataType.BooleanType),
            new Field("created", DataType.DateType),
            new Field("tstamp", DataType.TimestampType),
            new Field("label", DataType.StringType));

    public static final DataSchema DIM = new DataSchema(
            new Field("dept", DataType.IntegerType),
            new Field("bonus", DataType.IntegerType));

    public static final DataSchema REGION_DIM = new DataSchema(
            new Field("region", DataType.IntegerType),
            new Field("rname", DataType.StringType));

    public static final DataSchema LABEL_DIM = new DataSchema(
            new Field("label", DataType.StringType),
            new Field("weight", DataType.IntegerType));

    public static String orders(Path dir, String name, long rows) {
        Path f = dir.resolve(name + ".csv");
        writeIfAbsent(f, rows, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                StringBuilder sb = new StringBuilder(64);
                for (long id = 1; id <= rows; id++) {
                    appendOrderRow(sb, id);
                    w.write(sb.toString());
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write " + name + " failed", e);
            }
        });
        return f.toString();
    }

    private static final long DAY_MILLIS = 86_400_000L;
    private static final long WIDE_EPOCH = 1_600_000_000_000L;

    public static String wide(Path dir, String name, long rows) {
        Path f = dir.resolve(name + ".csv");
        writeIfAbsent(f, rows, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                StringBuilder sb = new StringBuilder(96);
                for (long id = 1; id <= rows; id++) {
                    appendWideRow(sb, id);
                    w.write(sb.toString());
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write " + name + " failed", e);
            }
        });
        return f.toString();
    }

    private static void appendWideRow(StringBuilder sb, long id) {
        sb.setLength(0);
        long createdMillis = WIDE_EPOCH + (id % 10) * DAY_MILLIS;
        long tsMillis = WIDE_EPOCH + (id % 20) * 1000L;
        int tsNanos = (int) ((id % 7) * 1_000_000);
        sb.append(id).append('\t')
          .append(id % 50).append('\t')
          .append(id % 10).append('\t')
          .append(id % 5).append('\t')
          .append(id % 1000).append('\t')
          .append(id % 20).append('\t')
          .append(id % 2 == 0).append('\t')                    // active Boolean
          .append(createdMillis).append('\t')                  // created Date(epoch millis)
          .append(tsMillis).append(':').append(tsNanos).append('\t') // tstamp Timestamp(millis:nanos)
          .append("L").append(id % 8);                         // label String
    }

    public static String supplement(Path dir, String name, long rows, long idOffset) {
        Path f = dir.resolve(name + ".csv");
        writeIfAbsent(f, rows, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                StringBuilder sb = new StringBuilder(64);
                for (long i = 1; i <= rows; i++) {
                    appendOrderRow(sb, idOffset + i);
                    w.write(sb.toString());
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write " + name + " failed", e);
            }
        });
        return f.toString();
    }

    private static void appendOrderRow(StringBuilder sb, long id) {
        sb.setLength(0);
        sb.append(id).append('\t')
          .append(id % 50).append('\t')
          .append(id % 10).append('\t')
          .append(id % 5).append('\t')
          .append(id % 1000).append('\t')
          .append(id % 20);
    }

    public static String dim(Path dir) {
        Path f = dir.resolve("dim.csv");
        writeIfAbsent(f, 50, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                for (int dept = 0; dept < 50; dept++) {
                    w.write(dept + "\t" + (dept * 10));
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write dim failed", e);
            }
        });
        return f.toString();
    }

    public static String regionDim(Path dir) {
        Path f = dir.resolve("region_dim.csv");
        writeIfAbsent(f, 10, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                for (int r = 0; r < 10; r++) {
                    w.write(r + "\tR" + r);
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write region_dim failed", e);
            }
        });
        return f.toString();
    }

    public static String labelDim(Path dir) {
        Path f = dir.resolve("label_dim.csv");
        writeIfAbsent(f, 8, () -> {
            try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
                for (int i = 0; i < 8; i++) {
                    w.write("L" + i + "\t" + (i * 100));
                    w.write('\n');
                }
            } catch (IOException e) {
                throw new UncheckedIOException("write label_dim failed", e);
            }
        });
        return f.toString();
    }

    private static void writeIfAbsent(Path f, long expectedRows, Runnable gen) {
        if (Files.exists(f) && countLines(f.toString()) == expectedRows) {
            return;
        }
        gen.run();
    }

    static long countLines(String path) {
        long n = 0;
        try (java.io.BufferedReader r = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
            while (r.readLine() != null) {
                n++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("countLines failed", e);
        }
        return n;
    }
}

