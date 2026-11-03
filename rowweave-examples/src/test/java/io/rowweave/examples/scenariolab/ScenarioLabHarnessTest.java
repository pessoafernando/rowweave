package io.rowweave.examples.scenariolab;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScenarioLabHarnessTest {

    @Test
    void comparisonDoesNotEraseNumericRuntimeTypesOrDecimalScale() {
        DataSchema decimalMeta = new DataSchema(new Field("v", DataType.BigDecimalType));
        assertFalse(RowCompare.compare(decimalMeta,
                Collections.singletonList(row(new BigDecimal("1.0"))),
                Collections.singletonList(row(new BigDecimal("1.00"))), false).pass);

        DataSchema longMeta = new DataSchema(new Field("v", DataType.LongType));
        assertFalse(RowCompare.compare(longMeta,
                Collections.singletonList(row(1L)),
                Collections.singletonList(row(1)), false).pass);
    }

    @Test
    void negativeChecksRequireExactClassAndStableMessageFromCauseChain() {
        IllegalStateException wrapped = new IllegalStateException(
                "submit failed", new IllegalArgumentException("union schema mismatch at field 1"));
        ScenarioLab.assertExpectedFailure(
                wrapped, IllegalStateException.class, "union schema mismatch");

        assertThrows(AssertionError.class, () -> ScenarioLab.assertExpectedFailure(
                wrapped, IllegalArgumentException.class, "union schema mismatch"));
        assertThrows(AssertionError.class, () -> ScenarioLab.assertExpectedFailure(
                wrapped, IllegalStateException.class, "unrelated message"));
        assertThrows(IllegalArgumentException.class, () -> ScenarioDef.throwsOf(
                "id", "title", IllegalArgumentException.class, "", () -> { }));
    }

    @Test
    void equalExactRowsStillPass() {
        DataSchema meta = new DataSchema(new Field("v", DataType.BigDecimalType));
        assertTrue(RowCompare.compare(meta,
                Collections.singletonList(row(new BigDecimal("1.00"))),
                Collections.singletonList(row(new BigDecimal("1.00"))), false).pass);
    }

    @Test
    void batchConstructionFailureIsCountedOnce(@TempDir Path tmp) throws Exception {
        BatchDesign broken = new BatchDesign() {
            @Override public int batchNo() { return 1; }
            @Override public String title() { return "broken"; }
            @Override public List<ScenarioDef> scenarios() {
                throw new IllegalStateException("scenario construction failed");
            }
        };

        ScenarioLabRunner.RunSummary summary =
                ScenarioLabRunner.run(Collections.singletonList(broken), tmp.resolve("run"));

        assertEquals(1, summary.outcomes.size());
        assertEquals(0, summary.passed);
        assertEquals(0, summary.failed);
        assertEquals(1, summary.errored);
    }

    private static DataRow row(Object value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }
}
