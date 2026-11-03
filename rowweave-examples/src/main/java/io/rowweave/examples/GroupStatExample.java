package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.GroupReduceFunction;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
  * Custom {@link GroupReduceFunction} that computes count and amount total by region when built-in sum/count cannot emit both derived columns at once.
 *
  * <p>This example demonstrates three custom-function rules:
 * <ul>
  * <li><b>{@code getResultSchema()} is the sole source of the output schema</b>; the engine builds downstream schema from it, so it cannot return null.
  * This output is {@code [region, cnt, total]}.</li>
  * <li><b>Emit row by row</b>: each group emits through {@code collector.collect(row)}; do not accumulate a List and return it later.</li>
  * <li><b>Be serializable and do not capture non-serializable state</b>: {@link #SCALE} is a primitive int;
  * functions must never hold connections, loggers, or large external objects because workers serialize them.</li>
 * </ul>
 *
  * <p>Decimal precision: accumulate {@link BigDecimal} at full precision and call {@code setScale(10, HALF_UP)} once at final output.
 */
public final class GroupStatExample {

        /** Detail schema: region and amount. */
    private static final DataSchema DETAIL_META = new DataSchema(
            new Field("region", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

        /** Decimal output scale; a primitive serializable int. */
    private static final int SCALE = 10;

    private GroupStatExample() {
    }

        /** Computes [region, cnt, total] per region and writes {@code outputPath}. */
    public static void run(List<DataRow> details, String outputPath) {
        ExecutionEnvironment session = RowWeave.createEnvironment("group-stat");

        DataSet stat = session.fromSource(detailInput(details))
                .groupBy("region")
                                .reduceGroup(new RegionStat());   // Custom reduction emits cnt and total together.

        stat.sinkTo(new FileSink(stat.getSchema(), outputPath));
        session.execute();
    }

        /** Custom GroupReduceFunction: turns one region's details into [region, cnt, total]. */
    static final class RegionStat implements GroupReduceFunction {

                /** The sole source of the output schema; it cannot return null. */
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(
                    new Field("region", DataType.StringType),
                    new Field("cnt", DataType.IntegerType),
                    new Field("total", DataType.BigDecimalType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector collector) {
            String region = null;
            int cnt = 0;
                        BigDecimal total = BigDecimal.ZERO;   // Accumulate at full precision.
            for (DataRow r : rows) {
                region = r.getString(0);
                BigDecimal amt = r.getBigDecimal(1);
                                if (amt != null) {                // Null amounts do not contribute to the total.
                    total = total.add(amt);
                }
                cnt++;
            }
            DataRow out = new DataRow(3);
            out.set(0, region);
            out.set(1, cnt);
                        out.set(2, total.setScale(SCALE, RoundingMode.HALF_UP)); // Round once before output.
                        collector.collect(out);               // Emit rows; do not accumulate a List.
        }
    }

        /** Convenience factory for one detail row. */
    public static DataRow detail(String region, String amount) {
        DataRow r = new DataRow(2);
        r.set(0, region);
        r.set(1, amount == null ? null : new BigDecimal(amount));
        return r;
    }

    private static Source detailInput(List<DataRow> details) {
        return new CollectionSource(DETAIL_META, new ArrayList<>(details));
    }

    public static void main(String[] args) {
        List<DataRow> details = new ArrayList<>();
        details.add(detail("C1", "100.00"));
        details.add(detail("C1", "200.00"));
        details.add(detail("C2", "50.50"));

        String out = System.getProperty("java.io.tmpdir") + "/rowweave-group-stat.tsv";
        run(details, out);
                // Verify the read-back: C1 -> cnt=2 total=300.0000000000; C2 -> cnt=1 total=50.5000000000.
        FileSource result = new FileSource(new DataSchema(
                new Field("region", DataType.StringType),
                new Field("cnt", DataType.IntegerType),
                new Field("total", DataType.BigDecimalType)), out);
        try (FileSource.Reader rows = result.openReader()) {
            while (rows.hasNext()) {
                DataRow r = rows.next();
                System.out.println(r.getString(0) + "\tcnt=" + r.getInteger(1) + "\ttotal=" + r.getBigDecimal(2));
            }
        }
    }
}
