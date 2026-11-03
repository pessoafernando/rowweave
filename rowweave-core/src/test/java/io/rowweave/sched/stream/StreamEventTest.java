package io.rowweave.sched.stream;

import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StreamEventTest {

    @Test
    void recordCarriesRowAndPredicates() {
        DataRow row = new DataRow(1);
        row.set(0, "x");
        StreamEvent e = StreamEvent.record(row);
        assertEquals(StreamEvent.Kind.RECORD, e.kind());
        assertTrue(e.isRecord());
        assertFalse(e.isEndOfStream());
        assertFalse(e.isBarrier());
        assertFalse(e.isError());
        assertFalse(e.isCancel());
        assertSame(row, e.row());
    }

    @Test
    void recordRejectsNullRow() {
        assertThrows(IllegalArgumentException.class, () -> StreamEvent.record(null));
    }

    @Test
    void endOfStream() {
        StreamEvent e = StreamEvent.endOfStream();
        assertTrue(e.isEndOfStream());
        assertEquals(StreamEvent.Kind.END_OF_STREAM, e.kind());
        assertFalse(e.isRecord());
    }

    @Test
    void barrierCarriesBatchId() {
        StreamEvent e = StreamEvent.barrier(42L);
        assertTrue(e.isBarrier());
        assertEquals(42L, e.batchId());
    }

    @Test
    void errorCarriesCause() {
        Throwable t = new IllegalStateException("boom");
        StreamEvent e = StreamEvent.error(t);
        assertTrue(e.isError());
        assertSame(t, e.cause());
    }

    @Test
    void cancel() {
        StreamEvent e = StreamEvent.cancel();
        assertTrue(e.isCancel());
        assertEquals(StreamEvent.Kind.CANCEL, e.kind());
    }

    @Test
    void accessorsGuardWrongKind() {
        StreamEvent eos = StreamEvent.endOfStream();
        assertThrows(IllegalStateException.class, eos::row);
        assertThrows(IllegalStateException.class, eos::batchId);
        assertThrows(IllegalStateException.class, eos::cause);
    }
}
