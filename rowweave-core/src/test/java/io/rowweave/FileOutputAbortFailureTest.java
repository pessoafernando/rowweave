package io.rowweave;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A close failure must neither skip attempt cleanup nor damage the previous published version. */
class FileOutputAbortFailureTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void closeFailureStillDeletesAttemptAndPreservesCommittedFile(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        byte[] committed = "committed\n".getBytes(StandardCharsets.UTF_8);
        Files.write(target, committed);
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(1));
        Path attempt = onlyAttemptFile(tmp, target);
        ((BufferedWriter) currentWriter(output)).close();
        FailingCloseWriter writer = new FailingCloseWriter();
        injectWriter(output, writer);

        UncheckedIOException error = assertThrows(UncheckedIOException.class, output::abort);

        assertTrue(error.getMessage().contains("failed to close FileSink on abort"));
        assertFalse(Files.exists(attempt), "close must still delete the partial result of this attempt even when it fails");
        assertEquals(new String(committed, StandardCharsets.UTF_8),
                new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
        assertEquals(1, writer.closeCalls.get());
        assertNull(currentWriter(output), "the unknown-state writer must be cleared after a close attempt");
        assertDoesNotThrow(output::abort, "with the attempt cleaned and the writer cleared, a second abort is idempotent");
        assertEquals(1, writer.closeCalls.get(), "a second abort must not re-close the failed writer");
    }

    @Test
    void attemptCleanupFailureDominatesAndSuppressesCloseFailure(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, "committed\n".getBytes(StandardCharsets.UTF_8));
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(2));
        Path attempt = onlyAttemptFile(tmp, target);
        ((BufferedWriter) currentWriter(output)).close();
        Files.delete(attempt);
        Files.createDirectory(attempt);
        Files.write(attempt.resolve("child"), new byte[]{1});
        FailingCloseWriter writer = new FailingCloseWriter();
        injectWriter(output, writer);

        UncheckedIOException error = assertThrows(UncheckedIOException.class, output::abort);

        assertTrue(error.getMessage().contains("failed to delete FileSink attempt"),
                "while the attempt file still exists the cleanup failure must be the primary exception");
        assertEquals(1, error.getSuppressed().length);
        assertInstanceOf(UncheckedIOException.class, error.getSuppressed()[0]);
        assertTrue(error.getSuppressed()[0].getMessage().contains("failed to close FileSink on abort"));
        assertTrue(Files.exists(attempt), "the fault injection must genuinely keep the attempt undeletable");
        assertTrue(Files.exists(target), "a cleanup failure must not delete the last published version either");
        assertEquals(1, writer.closeCalls.get());
        assertNull(currentWriter(output));
    }

    private static DataRow row(long value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static Path onlyAttemptFile(Path directory, Path target) throws Exception {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            java.util.List<Path> attempts = files
                    .filter(path -> !path.equals(target))
                    .collect(java.util.stream.Collectors.toList());
            assertEquals(1, attempts.size(), "expected exactly one private attempt file");
            return attempts.get(0);
        }
    }

    private static void injectWriter(FileSink output, BufferedWriter writer) throws Exception {
        writerField().set(output, writer);
    }

    private static Object currentWriter(FileSink output) throws Exception {
        return writerField().get(output);
    }

    private static java.lang.reflect.Field writerField() throws Exception {
        java.lang.reflect.Field field = FileSink.class.getDeclaredField("writer");
        field.setAccessible(true);
        return field;
    }

    private static final class FailingCloseWriter extends BufferedWriter {
        private final AtomicInteger closeCalls = new AtomicInteger();

        private FailingCloseWriter() {
            super(new StringWriter());
        }

        @Override
        public void close() throws IOException {
            closeCalls.incrementAndGet();
            throw new IOException("injected close failure");
        }
    }
}
