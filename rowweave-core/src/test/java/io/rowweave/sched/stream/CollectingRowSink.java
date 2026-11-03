package io.rowweave.sched.stream;

import io.rowweave.DataRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link RowSink} that collects the pushed events in memory, for tests and for taking
 * results inside a session. It records the RECORD rows, whether an EOS or an Error was
 * seen, and counts barriers. Collecting does materialize RECORD rows into a List, so this
 * is only for sinks whose result is known to be bounded, such as a test assertion or the
 * point before the public API boundary at toIterator; it must not relay rows inside a
 * streaming operator chain.
 *
 * <p>Architectural invariant: CollectingRowSink may be used only from src/test.
 */
public final class CollectingRowSink implements RowSink {

    private final List<DataRow> rows = new ArrayList<>();
    private boolean sawEndOfStream;
    private boolean sawError;
    private int barrierCount;
    private Throwable error;

    @Override
    public void emit(StreamEvent event) {
        switch (event.kind()) {
            case RECORD:
                rows.add(event.row());
                break;
            case END_OF_STREAM:
                sawEndOfStream = true;
                break;
            case BARRIER:
                barrierCount++;
                break;
            case ERROR:
                sawError = true;
                error = event.cause();
                break;
            case CANCEL:
            default:
                break;
        }
    }

    public List<DataRow> rows() {
        return Collections.unmodifiableList(rows);
    }

    public boolean sawEndOfStream() {
        return sawEndOfStream;
    }

    public boolean sawError() {
        return sawError;
    }

    public Throwable error() {
        return error;
    }

    public int barrierCount() {
        return barrierCount;
    }
}
