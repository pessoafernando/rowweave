package io.rowweave.connector.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

final class S3BoundedLineReader implements AutoCloseable {
    private final InputStream input;
    private final int maximumBytes;

    S3BoundedLineReader(InputStream input, int maximumBytes) {
        this.input = input;
        this.maximumBytes = maximumBytes;
    }

    byte[] next() throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream(
                Math.min(maximumBytes, 4096));
        boolean readAny = false;
        while (true) {
            int value = input.read();
            if (value < 0) {
                if (!readAny) return null;
                break;
            }
            readAny = true;
            if (value == '\n') break;
            if (line.size() >= maximumBytes) {
                throw new IllegalArgumentException("S3_ROW_LIMIT");
            }
            line.write(value);
        }
        byte[] result = line.toByteArray();
        if (result.length > 0 && result[result.length - 1] == '\r') {
            result = java.util.Arrays.copyOf(result, result.length - 1);
        }
        if (result.length == 0) {
            throw new IllegalArgumentException("S3_ROW_INVALID");
        }
        return result;
    }

    @Override public void close() throws IOException { input.close(); }
}
