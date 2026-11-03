package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
  * Decimal rollup for a common analytics shape: group measures by region and segment,
  * order by those dimensions, and write the result to a file with bit-for-bit round trips.
 *
  * <p>This example demonstrates the numeric-correctness rules:
 * <ul>
  * <li>Amounts use {@link DataType#BigDecimalType}; {@code sum} accumulates at full precision and applies {@code setScale(10, HALF_UP)} once
  * before output (for example, {@code 300.0000000000}).</li>
  * <li>{@link FileSink} writes BigDecimal values with {@code toPlainString()}, preserving scale for bit-identical reads.</li>
  * <li>Result order is chosen explicitly with {@code orderBy}; equivalent result sets do not imply an order guarantee.</li>
 * </ul>
 *
  * <p>{@link FileSink} is a worker-writable public Sink. Its path is serializable and sent to workers,
  * so it works locally and in a cluster; read results back with {@link FileSource} instead of returning them through the client.
 */
public final class AmountRollupExample {

        /** Detail schema: region, segment, and BigDecimal amount. */
    private static final DataSchema DETAIL_META = new DataSchema(
            new Field("region", DataType.StringType),
            new Field("segment", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

    private AmountRollupExample() {
    }

    /**
          * Runs a decimal rollup by (region, segment), orders by region and segment, and writes {@code outputPath}.
          * The result schema is [region, segment, sum0(BigDecimal)].
 *
          * @param details detail rows (region, segment, amount)
          * @param outputPath result file path (tab-separated, bit-identical round trip)
     */
    public static void run(List<DataRow> details, String outputPath) {
        BatchSession session = RowWeave.createBatchSession("amount-rollup");

        DataSet rollup = session.fromSource(detailInput(details))
                                // Group and sum by multiple dimensions: groupBy(region, segment).sum(amount).
                                // Result = [region, segment, sum0]; sum0 uses BigDecimal scale 10 and HALF_UP.
                .groupBy("region", "segment").sum("amount")
                                // Order by dimensions; null values use DEFAULT semantics (string null sorts first).
                .orderBy("region", "segment");

                // FileSink is a worker-writable public Sink and preserves BigDecimal scale on disk.
        rollup.sinkTo(new FileSink(rollup.getSchema(), outputPath));

        session.execute();
    }

        /** Convenience factory for one detail row. */
    public static DataRow detail(String region, String segment, String amount) {
        DataRow r = new DataRow(3);
        r.set(0, region);
        r.set(1, segment);
        r.set(2, amount == null ? null : new BigDecimal(amount));
        return r;
    }

    private static Source detailInput(List<DataRow> details) {
                // Defensively copy into a replayable CollectionSource; a Source must support repeated traversal.
        return new CollectionSource(DETAIL_META, new ArrayList<>(details));
    }

    public static void main(String[] args) {
        List<DataRow> details = new ArrayList<>();
        details.add(detail("C1", "alpha", "100.00"));
        details.add(detail("C1", "alpha", "200.00"));
        details.add(detail("C1", "beta", "50.50"));
        details.add(detail("C2", "alpha", "300.30"));
        String out = System.getProperty("java.io.tmpdir") + "/rowweave-amount-rollup.tsv";
        run(details, out);
                // Verify the read-back: C1/alpha=300.0000000000, C1/beta=50.5000000000, C2/alpha=300.3000000000.
        FileSource result = new FileSource(new DataSchema(
                new Field("region", DataType.StringType),
                new Field("segment", DataType.StringType),
                new Field("sum0", DataType.BigDecimalType)), out);
        try (FileSource.Reader rows = result.openReader()) {
            while (rows.hasNext()) {
                DataRow r = rows.next();
                System.out.println(r.getString(0) + "\t" + r.getString(1) + "\t" + r.getBigDecimal(2));
            }
        }
    }
}
