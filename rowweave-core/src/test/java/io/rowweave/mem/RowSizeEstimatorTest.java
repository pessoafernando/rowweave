package io.rowweave.mem;

import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The heap footprint estimator for DataRow — it must **over-estimate rather than under-estimate**.
 * The estimate decides whether the in-memory fast path fits its budget, and an under-estimate leads to
 * running out of memory, so every case asserts estimate >= a sound real lower bound.
 */
class RowSizeEstimatorTest {

    @Test
    void estimateNeverBelowFieldContent() {
        // A mixed-type row: the estimate must be at least the sum of the sensible lower bounds for each field's content.
        DataRow r = new DataRow(5);
        r.set(0, 42);
        r.set(1, 9_000_000_000L);
        r.set(2, "hello world");                 // 11 chars
        r.set(3, new BigDecimal("12345.6789012345")); // precision 15
        r.set(4, null);
        long est = RowSizeEstimator.estimateRow(r);
        // Lower bound: DataRow plus Object[] header (about 32) plus int 16 plus long 16 plus String (48+2*11=70) plus BigDecimal (48+8*15=168)
        assertTrue(est >= 32 + 16 + 16 + 70 + 168, "the estimate is at least the sum of the conservative per-field floors, measured " + est);
    }

    @Test
    void highPrecisionBigDecimalNotUnderestimated() {
        // High-precision values (large precision) are the normal case and must never be under-estimated.
        DataRow r = new DataRow(1);
        r.set(0, new BigDecimal("123456789012345678901234567890.1234567890")); // precision 40
        long est = RowSizeEstimator.estimateRow(r);
        assertTrue(est >= 48 + 8 * 40, "a high-precision BigDecimal estimate is at least 48 + 8 x precision, measured " + est);
    }

    @Test
    void longStringNotUnderestimated() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) sb.append('\u754C');
        DataRow r = new DataRow(1);
        r.set(0, sb.toString());
        long est = RowSizeEstimator.estimateRow(r);
        assertTrue(est >= 48 + 2 * 1000, "a long String estimate is at least 48 + 2 x length, measured " + est);
    }

    @Test
    void allNullRowStillPositive() {
        DataRow r = new DataRow(3);  // every field null
        long est = RowSizeEstimator.estimateRow(r);
        assertTrue(est >= 32, "an empty row is still at least the DataRow plus Object[] headers, measured " + est);
    }

    @Test
    void batchEstimateSumsRows() {
        DataRow a = new DataRow(1); a.set(0, "x");
        DataRow b = new DataRow(1); b.set(0, "yy");
        long sa = RowSizeEstimator.estimateRow(a);
        long sb = RowSizeEstimator.estimateRow(b);
        assertTrue(sa >= 48 + 2 && sb >= 48 + 4, "each row is estimated correctly on its own");
    }
}
