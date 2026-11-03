package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
  * Dimension enrichment: left join an event dataset with a region dimension, add the region label to
 * each event, and write the result to a file.
 *
  * <p>This example demonstrates core join semantics:
 * <ul>
  * <li><b>leftJoin</b> keeps every left row and fills unmatched right columns with {@code null}.
 * Unlike an inner join, it does not silently drop events that have no dimension record.</li>
  * <li><b>{@code .on("region")}</b> is a single-key equality join on the same-named columns and uses sort-merge join.
  * The output schema is {@code [region, amount, region, regionName]}:
  * each side retains its own join key, so region appears twice.</li>
  * <li><b>Null keys never match</b>: an event with a null region remains unmatched even if the dimension has null,
  * because null does not equal null in a join; the outer side fills null.</li>
 * </ul>
 *
  * <p>After joining, {@code select} removes the duplicated right region column and keeps {@code [region, amount, regionName]}.
  * A null regionName means the region had no dimension record.
 */
public final class JoinDimEnrichExample {

        /** Event schema: region key and amount. */
    private static final DataSchema EVENT_META = new DataSchema(
            new Field("region", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

        /** Region dimension schema: region key and region name. */
    private static final DataSchema DIM_META = new DataSchema(
            new Field("region", DataType.StringType),
            new Field("regionName", DataType.StringType));

    private JoinDimEnrichExample() {
    }

    /**
          * Left joins events with the region dimension and writes [region, amount, regionName] to {@code outputPath}.
          * Events without a dimension match retain regionName = null.
 *
          * @param events event rows (region, amount)
          * @param dims region dimension rows (region, regionName)
          * @param outputPath result file path
     */
    public static void run(List<DataRow> events, List<DataRow> dims, String outputPath) {
        ExecutionEnvironment session = RowWeave.createEnvironment("join-dim-enrich");

        DataSet eventsDs = session.fromSource(eventInput(events));
        DataSet dimsDs = session.fromSource(dimInput(dims));

                // leftJoin + on("region") keeps all left rows and fills unmatched right values with null.
                // Join output schema = [region(left), amount, region(right), regionName], four columns.
        DataSet joined = eventsDs.leftJoin(dimsDs).on("region");

                // Remove the duplicated right region (index 2), leaving [region, amount, regionName].
        DataSet projected = joined.select(0, 1, 3);

        projected.sinkTo(new FileSink(projected.getSchema(), outputPath));
        session.execute();
    }

        /** Convenience factory for one event row. */
    public static DataRow event(String region, String amount) {
        DataRow r = new DataRow(2);
        r.set(0, region);
        r.set(1, amount == null ? null : new BigDecimal(amount));
        return r;
    }

        /** Convenience factory for one dimension row. */
    public static DataRow dim(String region, String regionName) {
        DataRow r = new DataRow(2);
        r.set(0, region);
        r.set(1, regionName);
        return r;
    }

    private static Source eventInput(List<DataRow> events) {
        return new CollectionSource(EVENT_META, new ArrayList<>(events));
    }

    private static Source dimInput(List<DataRow> dims) {
        return new CollectionSource(DIM_META, new ArrayList<>(dims));
    }

    public static void main(String[] args) {
        List<DataRow> events = new ArrayList<>();
        events.add(event("C1", "100.00"));
        events.add(event("C2", "200.00"));
                events.add(event("C9", "50.00"));   // C9 has no dimension record, so regionName is null.

        List<DataRow> dims = new ArrayList<>();
                dims.add(dim("C1", "North"));
                dims.add(dim("C2", "Harbor"));

        String out = System.getProperty("java.io.tmpdir") + "/rowweave-join-dim-enrich.tsv";
        run(events, dims, out);
                // Verify: C1/100/North, C2/200/Harbor, C9/50/null (no dimension match).
        FileSource result = new FileSource(new DataSchema(
                new Field("region", DataType.StringType),
                new Field("amount", DataType.BigDecimalType),
                new Field("regionName", DataType.StringType)), out);
        try (FileSource.Reader rows = result.openReader()) {
            while (rows.hasNext()) {
                DataRow r = rows.next();
                System.out.println(r.getString(0) + "\t" + r.getBigDecimal(1) + "\t" + r.getString(2));
            }
        }
    }
}
