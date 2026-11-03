package io.rowweave;

import io.rowweave.mem.RowSizeEstimator;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A deprecated materialized read and a List source inherit the same hard limit. */
class BoundedCompatibilityReadTest {

    private static final DataSchema META = new DataSchema(
            new Field("v", DataType.IntegerType));

    @TempDir
    Path tempDir;

    @Test
    void explicitFileReadLimitsRowsAndNeverReturnsPartialResults() {
        Path file = writeRows("explicit-rows.tsv", 1, 2);

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> FileSink.read(
                        META, file.toString(),
                        CompatibilityDataLimits.of(1, 1024 * 1024)));

        assertEquals(CompatibilityDataLimitException.Kind.ROWS, failure.kind());
        assertEquals(2, failure.observed());
        assertEquals("FileSink.read", failure.owner());
    }

    @Test
    void explicitFileReadCreditsArrayCapacityAndReturnsReadOnlyView() {
        DataRow sample = row(7);
        Path file = writeRows("explicit-bytes.tsv", 7);
        long exactBytes = RowSizeEstimator.estimateReferenceArray(1)
                + RowSizeEstimator.estimateRow(sample);

        List<DataRow> exact = FileSink.read(
                META, file.toString(), CompatibilityDataLimits.of(1, exactBytes));
        assertEquals(7, exact.get(0).get(0));
        assertThrows(UnsupportedOperationException.class, () -> exact.add(row(8)));

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> FileSink.read(
                        META, file.toString(),
                        CompatibilityDataLimits.of(1, exactBytes - 1)));
        assertEquals(CompatibilityDataLimitException.Kind.BYTES, failure.kind());
        assertEquals(exactBytes, failure.observed());
    }

    @Test
    void legacyFileReadUsesTheNonOptionalDefaultRowLimitAndClosesOnFailure()
            throws Exception {
        Path file = tempDir.resolve("default-limit.tsv");
        try (BufferedWriter writer = Files.newBufferedWriter(
                file, StandardCharsets.UTF_8)) {
            for (int i = 0; i <= CompatibilityDataLimits.DEFAULT_MAX_ROWS; i++) {
                writer.write("1\n");
            }
        }

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> FileSink.read(META, file.toString()));
        assertEquals(CompatibilityDataLimitException.Kind.ROWS, failure.kind());
        assertEquals((long) CompatibilityDataLimits.DEFAULT_MAX_ROWS + 1L,
                failure.observed());

        Files.delete(file);
        assertFalse(Files.exists(file), "read failure must close its FileSource.Reader");
    }

    @Test
    void planNodeListSourceRejectsOversizeBeforeReadingAnyRow() {
        List<DataRow> oversized = new AbstractList<DataRow>() {
            @Override
            public DataRow get(int index) {
                throw new AssertionError("row-limit preflight must not read the source list");
            }

            @Override
            public int size() {
                return CompatibilityDataLimits.DEFAULT_MAX_ROWS + 1;
            }
        };

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> PlanNode.source(META, oversized));
        assertEquals(CompatibilityDataLimitException.Kind.ROWS, failure.kind());
        assertEquals("CollectionSource", failure.owner());
    }

    private Path writeRows(String name, int... values) {
        Path path = tempDir.resolve(name);
        FileSink output = new FileSink(META, path.toString());
        for (int value : values) {
            output.collect(row(value));
        }
        output.close();
        return path;
    }

    private static DataRow row(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }
}
