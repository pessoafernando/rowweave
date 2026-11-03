package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FlatMapFunction;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.PrintSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.util.ArrayList;
import java.util.List;

/**
  * WordCount starter example: split text lines into words and count occurrences per word.
 *
  * <p>This example shows the four core engine steps:
 * <ol>
  * <li>{@link RowWeave#createBatchSession(String)} creates a Batch session.</li>
  * <li>{@link BatchSession#fromSource(Source)} creates a source.</li>
  * <li>Transformations chain flatMap splitting and groupBy counting.</li>
  * <li>{@link DataSet#sinkTo} registers a sink and {@link BatchSession#execute()} submits execution.</li>
 * </ol>
 *
  * <p>Data pushes from Source to Sink without materializing {@code List<DataRow>}; flatMap emits each word
  * through {@link Collector#collect} instead of accumulating a List first.
 *
  * <p>flatMap intentionally retains the upstream schema: the single input column remains named {@code token},
  * so groupBy uses {@code "token"}.
 */
public final class WordCountExample {

        /** Source and intermediate schema: one token column, either a whole line or one word. */
    private static final DataSchema TOKEN_META = new DataSchema(
            new Field("token", DataType.StringType));

    private WordCountExample() {
    }

    /**
          * Runs WordCount: splits lines on whitespace, counts each word, and prints the result to stdout.
 *
          * @param lines input lines, one record per line
     */
    public static void run(List<String> lines) {
        BatchSession session = RowWeave.createBatchSession("wordcount");

                // Wrap each line as a single-column DataRow in a replayable Source.
        DataSet source = session.fromSource(lineInput(lines));

                // flatMap turns one input row into N word rows and emits them directly.
        FlatMapFunction split = (row, collector) -> {
            String line = row.getString(0);
            if (line == null) {
                return;
            }
            for (String word : line.split("\\s+")) {
                if (!word.isEmpty()) {
                    DataRow out = new DataRow(1);
                    out.set(0, word);
                    collector.collect(out);
                }
            }
        };

                // groupBy("token").count() counts occurrences as Long values.
        DataSet counts = source
                .flatMap(split)
                .groupBy("token")
                .count();

                // PrintSink is a serializable public Sink and falls back to worker stdout in cluster mode.
        counts.sinkTo(new PrintSink(counts.getSchema()));

        session.execute();
    }

        /** Wraps String lines as a replayable single-token Source. */
    private static Source lineInput(List<String> lines) {
        List<DataRow> rows = new ArrayList<>(lines.size());
        for (String line : lines) {
            DataRow r = new DataRow(1);
            r.set(0, line);
            rows.add(r);
        }
        return new CollectionSource(TOKEN_META, rows);
    }

    public static void main(String[] args) {
        List<String> lines = new ArrayList<>();
        lines.add("hello world");
        lines.add("hello rowweave");
        lines.add("rowweave push pipeline");
        run(lines);
    }
}
