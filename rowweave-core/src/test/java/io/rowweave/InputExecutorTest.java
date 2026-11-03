package io.rowweave;

import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Source as description and SourceExecutor as execution are separate concerns, where a SourceExecutor is a reader with open, hasNext, next and close;
 * the SourceExecutorFactory routes a Source to an executor, falling back to the IterableInputExecutor bridge when no dedicated executor is registered.
 * RowStream.fromSource goes through the executor, and a final close covers the AutoCloseable path.
 */
class InputExecutorTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.StringType));

    private static DataRow row(String k) {
        DataRow r = new DataRow(1);
        r.set(0, k);
        return r;
    }

    private static List<DataRow> rows(String... ks) {
        List<DataRow> rs = new ArrayList<>();
        for (String k : ks) rs.add(row(k));
        return rs;
    }

    /** The factory builds an executor for a built-in Source as a fallback bridge, and open, hasNext and next then close read everything correctly. */
    @Test
    void factoryCreatesExecutorReadsAll() {
        Source in = new CollectionSource(META, rows("a", "b", "c"));
        SourceExecutor exec = SourceExecutorFactory.create(in);
        exec.open();
        List<String> got = new ArrayList<>();
        while (exec.hasNext()) {
            got.add((String) exec.next().get(0));
        }
        exec.close();
        assertEquals(Arrays.asList("a", "b", "c"), got, "executor reads every row");
    }

    /** executor.next() yields a fresh DataRow every call, so neighbouring rows never share a reference, honoring the contract that a row is not mutated after emit. */
    @Test
    void nextProducesFreshRowNotReused() {
        Source in = new CollectionSource(META, rows("x", "y"));
        SourceExecutor exec = SourceExecutorFactory.create(in);
        exec.open();
        DataRow first = exec.next();
        DataRow second = exec.next();
        exec.close();
        assertNotSame(first, second, "two consecutive rows must be distinct DataRow references (no object reuse)");
        assertEquals("x", first.get(0), "the first row keeps its value after the second row is read (no reuse)");
        assertEquals("y", second.get(0));
    }

    /** RowStream.fromSource goes through the executor, reads everything, and closes once exhausted, so no resource leaks. */
    @Test
    void rowStreamFromInputReadsAllAndCloses() throws Exception {
        AtomicInteger closed = new AtomicInteger();
        Source probe = closeProbeInput(rows("p", "q"), closed);
        List<String> got = new ArrayList<>();
        try (RowStream.CloseableRowStream rs = RowStream.fromSource(probe)) {
            StreamEvent e;
            while ((e = rs.next()) != null) {
                if (e.isRecord()) got.add((String) e.row().get(0));
            }
        }
        assertEquals(Arrays.asList("p", "q"), got);
        assertEquals(1, closed.get(), "stream closed -> executor.close invoked once");
    }

    /** A final close: an exception mid-read or an early shutdown still reaches executor.close, as try-with-resources requires. */
    @Test
    void closeCalledOnEarlyClose() throws Exception {
        AtomicInteger closed = new AtomicInteger();
        Source probe = closeProbeInput(rows("a", "b", "c"), closed);
        RowStream.CloseableRowStream rs = RowStream.fromSource(probe);
        rs.next();  // reads one row then closes
        rs.close();
        assertEquals(1, closed.get(), "early close -> executor.close invoked");
        rs.close();  // idempotent
        assertEquals(1, closed.get(), "close is idempotent and never closes twice");
    }

    /** fromInputs chains several Sources, reads them in full and closes all of them. */
    @Test
    void fromInputsConcatAllAndCloses() throws Exception {
        AtomicInteger closed = new AtomicInteger();
        Source a = closeProbeInput(rows("1", "2"), closed);
        Source b = closeProbeInput(rows("3"), closed);
        List<String> got = new ArrayList<>();
        try (RowStream.CloseableRowStream rs = RowStream.fromInputs(Arrays.asList(a, b))) {
            StreamEvent e;
            while ((e = rs.next()) != null) {
                if (e.isRecord()) got.add((String) e.row().get(0));
            }
        }
        assertEquals(Arrays.asList("1", "2", "3"), got, "chained sources read every row");
        assertEquals(2, closed.get(), "each of the two source executors closes once");
    }

    /** A Source that records its close, which the bridged executor close should call back. */
    private static Source closeProbeInput(List<DataRow> data, AtomicInteger closedCounter) {
        return new CloseObservableInput(META, data, closedCounter);
    }

    /** Implements Source and AutoCloseable, so a bridged executor close should invoke its close(). */
    static final class CloseObservableInput implements Source, AutoCloseable {
        private final DataSchema meta;
        private final List<DataRow> data;
        private final AtomicInteger closed;
        CloseObservableInput(DataSchema meta, List<DataRow> data, AtomicInteger closed) {
            this.meta = meta; this.data = data; this.closed = closed;
        }
        @Override public DataSchema getSchema() { return meta; }
        @Override public Iterator<DataRow> iterator() { return data.iterator(); }
        @Override public void close() { closed.incrementAndGet(); }
    }
}
