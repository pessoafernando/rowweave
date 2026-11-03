package io.rowweave.examples;

import io.rowweave.CollectionSource;
import io.rowweave.DataRow;
import io.rowweave.DataSet;
import io.rowweave.DataSchema;
import io.rowweave.DataType;
import io.rowweave.BatchSession;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.RowWeave;
import io.rowweave.Source;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Minimal Batch job: filter, group, aggregate, order, and write a file sink.
 *
 * <p>The accumulator stays at full BigDecimal precision. RowWeave applies the
 * documented output scale (10 digits, HALF_UP) once at the aggregate output.
 */
public final class BatchFirstJobExample {

    private static final DataSchema INPUT_SCHEMA = new DataSchema(
            new Field("categoryKey", DataType.StringType),
            new Field("quantity", DataType.BigDecimalType));

    private BatchFirstJobExample() {
    }

    /** Runs the example and writes tab-separated output to {@code outputPath}. */
    public static void run(List<DataRow> inputRows, String outputPath) {
        BatchSession session = RowWeave.createBatchSession("first-job");
        Source input = new CollectionSource(INPUT_SCHEMA, new ArrayList<>(inputRows));

        DataSet totals = session.fromSource(input)
                .filter(row -> row.getBigDecimal(1)
                        .compareTo(new BigDecimal("3")) >= 0)
                .groupBy("categoryKey")
                .sum("quantity")
                .orderBy("categoryKey");

        totals.sinkTo(new FileSink(totals.getSchema(), outputPath));
        session.execute();
    }

    /** Creates one input row without copying or retaining the caller's schema. */
    public static DataRow row(String categoryKey, String quantity) {
        DataRow row = new DataRow(2);
        row.set(0, categoryKey);
        row.set(1, quantity == null ? null : new BigDecimal(quantity));
        return row;
    }

    /** Small deterministic input used by {@code main} and the test. */
    public static List<DataRow> sampleRows() {
        return Arrays.asList(
                row("alpha", "10.123456789123"),
                row("alpha", "0.00000000006"),
                row("alpha", "3"),
                row("alpha", "0.5"),
                row("beta", "4"),
                row("beta", "2.25"),
                row("gamma", "3.123456789123"));
    }

    /** Runs the sample. Supply one unused output path as the only argument. */
    public static void main(String[] arguments) {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("usage: BatchFirstJobExample <unused-output.tsv>");
        }
        run(sampleRows(), arguments[0]);
        System.out.println("Wrote " + arguments[0]);
    }
}
