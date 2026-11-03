package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deadlock guard for batched flush. It sends far more rows than the credit window and
 * shows that batched flush does not deadlock, because the flush-before-park invariant holds:
 * a sender never parks while holding unflushed rows. Were the batch buffer not flushed before
 * credit ran out, the peer would receive nothing, return no credit, and the sender would park
 * forever, which the timeout on this test would catch. It also shows that no row is lost or
 * corrupted, since every row arrives and matches bit for bit in order, and that close drains,
 * so a residual batch is flushed before the EOS and the last rows are not lost.
 */
class SocketChannelBatchFlushTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(int id) {
        DataRow r = new DataRow(2);
        r.set(0, id);
        r.set(1, new java.math.BigDecimal(id % 100));
        return r;
    }

/** A small window of 4 against 2000 rows, which forces the credit and batching interaction: if batched flush broke credit backpressure the run would hang, and the timeout would catch it. */
    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void batchFlushFarExceedsWindowNoDeadlockAllRowsDelivered() throws Exception {
        final int window = 4;  // a tiny window, to force credit parks
        final int total = 2000;  // far beyond the window and the batch boundaries

        SocketChannelSource src = new SocketChannelSource(META, window);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        AtomicReference<Throwable> producerErr = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    sink.send(StreamEvent.record(row(i)));
                }
                sink.close();  // close drains: a residual batch must flush before the EOS
            } catch (Throwable t) {
                producerErr.set(t);
            }
        }, "batch-producer");
        producer.start();

        // Drain row by row, checking the order and that the values match bit for bit, since a single point-to-point connection preserves order.
        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) {
                assertEquals(got, e.row().get(0), "row " + got + " id matches row for row with nothing lost or reordered");
                assertEquals(0, new java.math.BigDecimal(got % 100).compareTo((java.math.BigDecimal) e.row().get(1)),
                        "row " + got + " amt");
                got++;
            } else if (e.isError()) {
                fail("a clean batch flush raises no error: " + e.cause());
            }
        }
        producer.join(5000);
        assertFalse(producer.isAlive(), "the producer finishes instead of hanging because the flush before parking works");
        assertNull(producerErr.get(), "the producer threw nothing: " + producerErr.get());
        assertEquals(total, got, "all " + total + " rows arrive and the close drain loses no trailing batch");
        src.close();
    }
}
