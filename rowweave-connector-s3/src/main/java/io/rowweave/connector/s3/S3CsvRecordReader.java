package io.rowweave.connector.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

final class S3CsvRecordReader implements AutoCloseable {
    private final InputStream input;
    private final int maximumBytes;

    S3CsvRecordReader(InputStream input, int maximumBytes) {
        this.input = input;
        this.maximumBytes = maximumBytes;
    }

    List<String> next() throws IOException {
        List<String> fields = new ArrayList<>();
        ByteArrayOutputStream field = new ByteArrayOutputStream(128);
        boolean inQuotes = false;
        boolean afterQuote = false;
        boolean fieldStarted = false;
        boolean any = false;
        int bytes = 0;
        while (true) {
            int value = input.read();
            if (value < 0) {
                if (!any) return null;
                if (inQuotes) throw invalid();
                fields.add(text(field));
                return fields;
            }
            any = true;
            if (++bytes > maximumBytes) throw limit();
            if (inQuotes) {
                if (value == '"') {
                    inQuotes = false;
                    afterQuote = true;
                } else {
                    field.write(value);
                }
                continue;
            }
            if (afterQuote) {
                if (value == '"') {
                    field.write('"');
                    inQuotes = true;
                    afterQuote = false;
                } else if (value == ',') {
                    fields.add(text(field));
                    field.reset();
                    fieldStarted = false;
                    afterQuote = false;
                } else if (value == '\r') {
                    requireLf(++bytes);
                    fields.add(text(field));
                    return fields;
                } else {
                    throw invalid();
                }
                continue;
            }
            if (!fieldStarted && value == '"') {
                inQuotes = true;
                fieldStarted = true;
            } else if (value == ',') {
                fields.add(text(field));
                field.reset();
                fieldStarted = false;
            } else if (value == '\r') {
                requireLf(++bytes);
                fields.add(text(field));
                return fields;
            } else if (value == '\n' || value == '"') {
                throw invalid();
            } else {
                fieldStarted = true;
                field.write(value);
            }
        }
    }

    private void requireLf(int bytes) throws IOException {
        if (bytes > maximumBytes) throw limit();
        if (input.read() != '\n') throw invalid();
    }

    private static String text(ByteArrayOutputStream field) {
        return S3CodecSupport.decodeUtf8(field.toByteArray(),
                "S3_ROW_INVALID");
    }

    @Override public void close() throws IOException { input.close(); }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("S3_ROW_INVALID");
    }

    private static IllegalArgumentException limit() {
        return new IllegalArgumentException("S3_ROW_LIMIT");
    }
}
