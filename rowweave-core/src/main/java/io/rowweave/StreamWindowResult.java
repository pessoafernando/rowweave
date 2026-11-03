package io.rowweave;

/** Public graph views for a generic window main and declared late output. */
public final class StreamWindowResult {
    private final DataStream main;
    private final DataStream late;

    StreamWindowResult(DataStream main,
            io.rowweave.stream.api.StreamOutputTag lateOutput) {
        if (main == null || lateOutput == null) {
            throw new IllegalArgumentException(
                    "window and late output are required");
        }
        this.main = main;
        this.late = DataStream.taggedView(main, lateOutput);
    }

    public DataStream main() { return main; }
    public DataStream late() { return late; }
}
