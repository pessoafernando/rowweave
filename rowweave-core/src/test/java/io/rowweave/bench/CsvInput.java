package io.rowweave.bench;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The streaming CSV source behind the large-data benchmarks, in test scope. It is deliberately
 * the lightest possible source: it holds only the file path and the schema, which describes the
 * data rather than carrying it, so it satisfies the {@link Source} Serializable contract and can
 * be serialized out to a separate worker process, where each worker opens the path itself through
 * a file system shared on the same machine.
 *
 * <p>Purely streaming: {@code iterator()} holds a BufferedReader and reads line by line,
 * splitting on TAB into a DataRow, so memory is O(row length) rather than O(N), and the stream
 * closes itself at the end. The CSV has no header and the field order is the schema column order;
 * {@code \N} means a null value, matching the FileSink convention.
 *
 * <p>The data lives in files rather than memory or a database: {@link CsvDataGen} writes N rows
 * to disk once, and the test JVM holds no rows.
 */
public final class CsvInput implements Source {

    private static final long serialVersionUID = 1L;
    private static final String NULL_TOKEN = "\\N";

    private final String path;
    private final DataSchema dataSchema;

    public CsvInput(String path, DataSchema dataSchema) {
        this.path = path;
        this.dataSchema = dataSchema;
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public Iterator<DataRow> iterator() {
        try {
            BufferedReader reader = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);
            return new LineIterator(reader, dataSchema);
        } catch (IOException e) {
            throw new UncheckedIOException("CsvInput open failed: " + path, e);
        }
    }

    /** Row cursor: next() reads and parses one line, and the stream closes itself at the end so no resource leaks. */
    private static final class LineIterator implements Iterator<DataRow> {
        private final BufferedReader reader;
        private final DataSchema meta;
        private String pending;
        private boolean closed;

        LineIterator(BufferedReader reader, DataSchema meta) {
            this.reader = reader;
            this.meta = meta;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (pending != null) {
                return true;
            }
            try {
                pending = reader.readLine();
                if (pending == null) {
                    close();
                    return false;
                }
                return true;
            } catch (IOException e) {
                close();
                throw new UncheckedIOException("CsvInput read failed", e);
            }
        }

        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String line = pending;
            pending = null;
            return parse(line, meta);
        }

        private void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                reader.close();
            } catch (IOException ignored) {
            }
        }
    }

    /** Parses a TAB-separated row into a DataRow by the schema column types, mapping the null marker to null and keeping the BigDecimal scale. */
    private static DataRow parse(String line, DataSchema meta) {
        int n = meta.getFieldCount();
        DataRow row = new DataRow(n);
        int start = 0;
        for (int c = 0; c < n; c++) {
            int tab = line.indexOf('\t', start);
            String tok = (c == n - 1) ? line.substring(start) : line.substring(start, tab);
            start = tab + 1;
            row.set(c, tok.equals(NULL_TOKEN) ? null : decode(tok, meta.getField(c)));
        }
        return row;
    }

    private static Object decode(String tok, Field field) {
        DataType type = field.getDataType();
        switch (type) {
            case StringType:  return tok;
            case IntegerType: return Integer.parseInt(tok);
            case LongType:    return Long.parseLong(tok);
            case BigDecimalType: return new BigDecimal(tok);
            case BooleanType: return Boolean.parseBoolean(tok);
            case DateType: return new java.util.Date(Long.parseLong(tok));  // epoch millis
            case TimestampType: {  // millis and nanos in two fields, matching FileSink
                int sep = tok.indexOf(':');
                java.sql.Timestamp ts = new java.sql.Timestamp(Long.parseLong(tok.substring(0, sep)));
                ts.setNanos(Integer.parseInt(tok.substring(sep + 1)));
                return ts;
            }
            default: throw new IllegalArgumentException("CsvInput: unsupported column type " + type);
        }
    }
}
