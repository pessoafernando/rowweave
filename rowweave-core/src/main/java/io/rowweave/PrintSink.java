package io.rowweave;

import java.io.PrintStream;

/**
 * A Sink that prints each result row line by line, for debugging: one record per line, fields separated by commas.
 *
 * <p>A Sink must be serializable. {@code out} is marked {@code transient} (a PrintStream is not serializable), so after
 * deserialization on a Worker {@code out==null} and the code falls back to {@code System.out}, which lands on the Worker stdout - all a debug sink needs.
 */
public final class PrintSink implements Sink {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;
    private transient PrintStream out;

    public PrintSink(DataSchema dataSchema) {
        this(dataSchema, System.out);
    }

    public PrintSink(DataSchema dataSchema, PrintStream out) {
        this.dataSchema = dataSchema;
        this.out = out;
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public void collect(DataRow row) {
        PrintStream o = (out != null) ? out : System.out; // after deserialization on a Worker, fall back to stdout
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(row.get(i));
        }
        o.println(sb.toString());
    }

    @Override
    public boolean isWorkerWritable() {
        return true;
    }
}
