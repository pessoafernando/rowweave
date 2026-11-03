package io.rowweave;

import io.rowweave.mem.RowSizeEstimator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A local result compatibility view carries both a row count and a resident-bytes guardrail. */
class DataSetOutputLimitsTest {

    private static final DataSchema META = new DataSchema(
            new Field("v", DataType.IntegerType));

    @Test
    void rowLimitRejectsCurrentRowWithoutPublishingIt() {
        CollectingSink output = new CollectingSink(
                META, CompatibilityDataLimits.of(1, 1024 * 1024));
        output.collect(row(1));

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> output.collect(row(2)));

        assertEquals(CompatibilityDataLimitException.Kind.ROWS, failure.kind());
        assertEquals(1, failure.limit());
        assertEquals(2, failure.observed());
        assertEquals("CollectingSink", failure.owner());
        assertEquals(1, output.getRows().size());
        assertEquals(1, output.getRows().get(0).get(0));
    }

    @Test
    void byteLimitIncludesOwnedArrayCapacityAndRejectsBeforeStore() {
        DataRow row = row(7);
        long exactBytes = RowSizeEstimator.estimateReferenceArray(1)
                + RowSizeEstimator.estimateRow(row);
        CollectingSink exact = new CollectingSink(
                META, CompatibilityDataLimits.of(1, exactBytes));
        exact.collect(row);
        assertEquals(1, exact.getRows().size());

        CollectingSink tooSmall = new CollectingSink(
                META, CompatibilityDataLimits.of(1, exactBytes - 1));
        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> tooSmall.collect(row));
        assertEquals(CompatibilityDataLimitException.Kind.BYTES, failure.kind());
        assertEquals(exactBytes, failure.observed());
        assertEquals(0, tooSmall.getRows().size());
    }

    @Test
    void growthCapacityIsCreditedBeforeNinthRowIsStored() {
        DataRow sample = row(1);
        long rowBytes = RowSizeEstimator.estimateRow(sample);
        long onlyEightSlots = RowSizeEstimator.estimateReferenceArray(8)
                + rowBytes * 9L;
        CollectingSink output = new CollectingSink(
                META, CompatibilityDataLimits.of(9, onlyEightSlots));
        for (int i = 0; i < 8; i++) {
            output.collect(row(i));
        }

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> output.collect(row(9)));

        assertEquals(CompatibilityDataLimitException.Kind.BYTES, failure.kind());
        assertEquals(RowSizeEstimator.estimateReferenceArray(9) + rowBytes * 9L,
                failure.observed());
        assertEquals(8, output.getRows().size());
    }

    @Test
    void rowsViewIsReadOnlyAndRemainsLiveWithoutAFullCopy() {
        CollectingSink output = new CollectingSink(META);
        List<DataRow> view = output.getRows();
        output.collect(row(1));

        assertEquals(1, view.size());
        assertThrows(UnsupportedOperationException.class, () -> view.add(row(2)));
        assertThrows(UnsupportedOperationException.class, () -> view.set(0, row(3)));
    }

    @Test
    void rollbackRestoresRowCreditAndRetainsReusableArrayCapacity() {
        DataRow sample = row(1);
        long budget = RowSizeEstimator.estimateReferenceArray(2)
                + RowSizeEstimator.estimateRow(sample) * 2L;
        CollectingSink output = new CollectingSink(
                META, CompatibilityDataLimits.of(2, budget));
        output.collect(row(1));
        output.collect(row(2));
        output.rollbackCommit();
        assertEquals(0, output.getRows().size());

        output.collect(row(3));
        output.collect(row(4));
        assertEquals(2, output.getRows().size());
        assertEquals(3, output.getRows().get(0).get(0));
        assertEquals(4, output.getRows().get(1).get(0));
    }

    @Test
    void serializationPreservesExplicitLimitsAndCollectedRows() throws Exception {
        DataRow sample = row(1);
        long budget = RowSizeEstimator.estimateReferenceArray(1)
                + RowSizeEstimator.estimateRow(sample);
        CollectingSink output = new CollectingSink(
                META, CompatibilityDataLimits.of(1, budget));
        output.collect(sample);

        CollectingSink copy = roundTrip(output);
        assertEquals(1, copy.getRows().size());
        assertThrows(CompatibilityDataLimitException.class,
                () -> copy.collect(row(2)));
        assertEquals(1, copy.getRows().size());
    }

    private static DataRow row(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static CollectingSink roundTrip(CollectingSink output) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(output);
        }
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return (CollectingSink) in.readObject();
        }
    }
}
