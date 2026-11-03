package io.rowweave;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A FileSink result is read through an O(1) closeable Reader and can re-enter as a Source. */
class FileInputTest {

    private static final DataSchema LONG_META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void validFirstRowIsReturnedBeforeMalformedTailIsParsed(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("lazy.tsv");
        Files.write(file, "1\nnot-a-long\n".getBytes(StandardCharsets.UTF_8));

        try (FileSource.Reader reader = new FileSource(LONG_META, file.toString()).openReader()) {
            assertTrue(reader.hasNext());
            assertEquals(1L, reader.next().get(0));
            assertTrue(reader.hasNext(), "lookahead reads text lines only and must not decode the trailing line early");
            assertThrows(NumberFormatException.class, reader::next);
            assertFalse(reader.hasNext(), "a parse failure must close automatically and reach a terminal state");
        }
    }

    @Test
    void earlyCloseIsIdempotentAndStopsFurtherReads(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("early.tsv");
        Files.write(file, "1\n2\n".getBytes(StandardCharsets.UTF_8));
        FileSource.Reader reader = new FileSource(LONG_META, file.toString()).openReader();
        assertEquals(1L, reader.next().get(0));

        reader.close();
        assertDoesNotThrow(reader::close);
        assertFalse(reader.hasNext());
        assertThrows(NoSuchElementException.class, reader::next);
        Files.delete(file);  // Windows also requires the handle to be released.
    }

    @Test
    void readersReplayIndependentlyAndEmptyFileEndsImmediately(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("replay.tsv");
        Files.write(file, "1\n2\n".getBytes(StandardCharsets.UTF_8));
        FileSource input = new FileSource(LONG_META, file.toString());

        try (FileSource.Reader first = input.openReader(); FileSource.Reader second = input.iterator()) {
            assertEquals(1L, first.next().get(0));
            assertEquals(1L, second.next().get(0));
            assertEquals(2L, first.next().get(0));
            assertEquals(2L, second.next().get(0));
            assertFalse(first.hasNext());
            assertFalse(second.hasNext());
        }

        Path empty = tmp.resolve("empty.tsv");
        Files.write(empty, new byte[0]);
        try (FileSource.Reader reader = new FileSource(LONG_META, empty.toString()).openReader()) {
            assertFalse(reader.hasNext());
        }
    }

    @Test
    void fileOutputToFileInputPreservesAllSupportedTypesAndNulls(@TempDir Path tmp) {
        DataSchema meta = new DataSchema(
                new Field("s", DataType.StringType),
                new Field("d", DataType.BigDecimalType),
                new Field("b", DataType.BooleanType),
                new Field("i", DataType.IntegerType),
                new Field("l", DataType.LongType),
                new Field("ts", DataType.TimestampType),
                new Field("date", DataType.DateType));
        Timestamp timestamp = new Timestamp(1_725_000_000_123L);
        timestamp.setNanos(987_654_321);
        Date date = new Date(1_700_000_000_456L);
        DataRow values = row("slash\\tab\tline\n", new BigDecimal("1.2300000000"), true, 7, 9L, timestamp, date);
        DataRow nulls = row(null, null, null, null, null, null, null);
        Path file = tmp.resolve("types.tsv");
        FileSink output = new FileSink(meta, file.toString());
        output.collect(values);
        output.collect(nulls);
        output.close();

        try (FileSource.Reader reader = new FileSource(meta, file.toString()).openReader()) {
            assertRowEquals(values, reader.next());
            assertRowEquals(nulls, reader.next());
            assertFalse(reader.hasNext());
        }
    }

    @Test
    void fileRoundTripPreservesNegativeBigDecimalScale(@TempDir Path tmp) throws Exception {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        List<BigDecimal> values = Arrays.asList(
                new BigDecimal("1E+30"),
                new BigDecimal("0E+30"),
                new BigDecimal("1E-30"),
                new BigDecimal("1.2300"));
        Path file = tmp.resolve("decimal-scale.tsv");
        FileSink output = new FileSink(meta, file.toString());
        for (BigDecimal value : values) {
            output.collect(row(value));
        }
        output.close();

        assertEquals(Arrays.asList("1E+30", "0E+30", "0.000000000000000000000000000001", "1.2300"),
                Files.readAllLines(file, StandardCharsets.UTF_8));
        try (FileSource.Reader reader = new FileSource(meta, file.toString()).openReader()) {
            for (BigDecimal expected : values) {
                assertEquals(expected, reader.next().getBigDecimal(0));
            }
            assertFalse(reader.hasNext());
        }
    }

    @Test
    void factoryUsesDedicatedExecutorAndSerializationCarriesOnlyDescription(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("factory.tsv");
        Files.write(file, "1\nbroken\n".getBytes(StandardCharsets.UTF_8));
        FileSource copy = roundTrip(new FileSource(LONG_META, file.toString()));

        SourceExecutor<?> executor = SourceExecutorFactory.create(copy);
        assertInstanceOf(FileInputExecutor.class, executor);
        executor.open();
        assertTrue(executor.hasNext());
        assertEquals(1L, executor.next().get(0));
        assertDoesNotThrow(executor::close, "an early close must not parse a corrupted trailing line");
        assertEquals(file.toString(), copy.path());
    }

    @Test
    void localJobCanReingestFileOutputWithoutMaterializingRows(@TempDir Path tmp) {
        Path sourcePath = tmp.resolve("source.tsv");
        FileSink source = new FileSink(LONG_META, sourcePath.toString());
        source.collect(row(1L));
        source.collect(row(2L));
        source.collect(row(3L));
        source.close();

        Path resultPath = tmp.resolve("result.tsv");
        ExecutionEnvironment session = RowWeave.createEnvironment("file-input-local");
        session.fromSource(new FileSource(LONG_META, sourcePath.toString()))
                .filter("id >= 2")
                .sinkTo(new FileSink(LONG_META, resultPath.toString()));
        session.execute();

        try (FileSource.Reader result = new FileSource(LONG_META, resultPath.toString()).openReader()) {
            assertEquals(2L, result.next().get(0));
            assertEquals(3L, result.next().get(0));
            assertFalse(result.hasNext());
        }
    }

    @Test
    void inputArrayTreatsEachFileAsAnApplicationProvidedSplit(@TempDir Path tmp) {
        Path firstPath = writeLongs(tmp.resolve("part-0.tsv"), 1L, 3L);
        Path secondPath = writeLongs(tmp.resolve("part-1.tsv"), 2L, 4L);
        Path resultPath = tmp.resolve("all.tsv");

        ExecutionEnvironment session = RowWeave.createEnvironment("file-input-splits");
        session.fromSource(
                        new FileSource(LONG_META, firstPath.toString()),
                        new FileSource(LONG_META, secondPath.toString()))
                .sinkTo(new FileSink(LONG_META, resultPath.toString()));
        session.execute();

        List<Long> actual = new ArrayList<>();
        try (FileSource.Reader result = new FileSource(LONG_META, resultPath.toString()).openReader()) {
            while (result.hasNext()) {
                actual.add((Long) result.next().get(0));
            }
        }
        Collections.sort(actual);
        assertEquals(Arrays.asList(1L, 2L, 3L, 4L), actual);
    }

    private static Path writeLongs(Path path, Long... values) {
        FileSink output = new FileSink(LONG_META, path.toString());
        for (Long value : values) {
            output.collect(row(value));
        }
        output.close();
        return path;
    }

    private static DataRow row(Object... cells) {
        DataRow row = new DataRow(cells.length);
        for (int i = 0; i < cells.length; i++) {
            row.set(i, cells[i]);
        }
        return row;
    }

    private static void assertRowEquals(DataRow expected, DataRow actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), "cell " + i);
            if (expected.get(i) instanceof BigDecimal) {
                assertEquals(((BigDecimal) expected.get(i)).scale(), ((BigDecimal) actual.get(i)).scale());
            }
            if (expected.get(i) instanceof Timestamp) {
                assertEquals(((Timestamp) expected.get(i)).getNanos(), ((Timestamp) actual.get(i)).getNanos());
            }
        }
    }

    private static FileSource roundTrip(FileSource input) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(input);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (FileSource) in.readObject();
        }
    }
}
