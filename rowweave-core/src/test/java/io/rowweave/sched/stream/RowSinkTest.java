package io.rowweave.sched.stream;

import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RowSinkTest {

    @Test
    void emitRowWrapsAsRecord() {
        List<StreamEvent> seen = new ArrayList<>();
        RowSink sink = seen::add;
        DataRow row = new DataRow(1);
        row.set(0, "x");
        sink.emitRow(row);
        assertEquals(1, seen.size());
        assertTrue(seen.get(0).isRecord());
        assertSame(row, seen.get(0).row());
    }

    @Test
    void endOfStreamEmitsEosEvent() {
        List<StreamEvent> seen = new ArrayList<>();
        RowSink sink = seen::add;
        sink.endOfStream();
        assertEquals(1, seen.size());
        assertTrue(seen.get(0).isEndOfStream());
    }

    @Test
    void collectingRowSinkRecordsRowsAndSignals() {
        CollectingRowSink sink = new CollectingRowSink();
        DataRow a = new DataRow(1);
        a.set(0, "a");
        sink.emitRow(a);
        sink.emit(StreamEvent.barrier(7L));
        sink.endOfStream();
        assertEquals(1, sink.rows().size());
        assertEquals("a", sink.rows().get(0).get(0));
        assertEquals(1, sink.barrierCount());
        assertTrue(sink.sawEndOfStream());
        assertFalse(sink.sawError());
    }

    @Test
    void collectingRowSinkCapturesError() {
        CollectingRowSink sink = new CollectingRowSink();
        Throwable t = new RuntimeException("boom");
        sink.emit(StreamEvent.error(t));
        assertTrue(sink.sawError());
        assertSame(t, sink.error());
    }
}
