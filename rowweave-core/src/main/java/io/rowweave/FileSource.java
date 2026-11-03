package io.rowweave;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A serializable file Source description in the {@link FileSink} format.
 *
 * <p>The executor reads line by line through {@link FileInputExecutor}, so memory is O(one row width). To read directly, use
 * {@link #openReader()} with try-with-resources; a full traversal closes automatically at EOF, and stopping early must close explicitly.
 * Each reader owns its file handle, and FileSource itself holds no execution state, so it is safe to serialize and send to a Worker.
 */
public final class FileSource implements Source {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;
    private final String path;

    public FileSource(DataSchema dataSchema, String path) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("dataSchema must not be null");
        }
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path must not be empty");
        }
        this.dataSchema = dataSchema;
        this.path = path;
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    public String path() {
        return path;
    }

    /** Opens an independent, closeable reader with a one-row lookahead. */
    public Reader openReader() {
        return new Reader(dataSchema, path);
    }

    /**
     * The Iterable compatibility entry point. The return type is covariant to a closeable Reader, so when an enhanced-for loop breaks early the
     * caller cannot rely on an automatic close, and an explicit {@link #openReader()} with try-with-resources is preferred.
     */
    @Override
    public Reader iterator() {
        return openReader();
    }

    /** A lazy one-row reader; it closes automatically on a parse or I/O failure and at EOF. */
    public static final class Reader implements Iterator<DataRow>, AutoCloseable {
        private final DataSchema dataSchema;
        private BufferedReader reader;
        private String nextLine;
        private boolean prepared;
        private boolean closed;

        private Reader(DataSchema dataSchema, String path) {
            this.dataSchema = dataSchema;
            try {
                this.reader = new BufferedReader(new InputStreamReader(
                        Files.newInputStream(Paths.get(path)), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException("failed to open FileSource: " + path, e);
            }
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (prepared) {
                return nextLine != null;
            }
            try {
                nextLine = reader.readLine();
                prepared = true;
                if (nextLine == null) {
                    close();
                    return false;
                }
                return true;
            } catch (IOException e) {
                UncheckedIOException failure = new UncheckedIOException("failed to read FileSource", e);
                closeAfterFailure(failure);
                throw failure;
            }
        }

        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("FileSource is exhausted or closed");
            }
            String line = nextLine;
            nextLine = null;
            prepared = false;
            try {
                return FileSink.decodeRow(dataSchema, line);
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(failure);
                throw failure;
            }
        }

        private void closeAfterFailure(Throwable primary) {
            try {
                close();
            } catch (Throwable closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            prepared = false;
            nextLine = null;
            BufferedReader current = reader;
            reader = null;
            if (current != null) {
                try {
                    current.close();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to close FileSource", e);
                }
            }
        }
    }
}
