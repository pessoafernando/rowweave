package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fixes the latency that streaming sinks used to have. Under a large window of 1 MiB, a slow
 * streaming operator sends a few small rows one at a time, nowhere near the byte threshold that
 * fills the window, and the records must reach the downstream within a bounded time through a
 * timed flush, instead of being held until the window fills.
 *
 * <p>The cause was that {@code SocketChannelSink.writeFrame} flushed only once the staging
 * buffer reached the byte threshold, which equals the window. A slow streaming AddField at 5ms
 * per small row never accumulates 1 MiB, so the first frame sat for tens of seconds, the
 * downstream received nothing, and the task stayed READY. The timed flush releases whatever is
 * batched once flushIntervalMs has passed even below the byte threshold, which bounds the
 * first-row latency and lets the downstream move from READY to RUNNING in time.
 */
class SocketChannelTimedFlushTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(int id) {
        DataRow r = new DataRow(2);
        r.set(0, id);
        r.set(1, new java.math.BigDecimal(id));
        return r;
    }

    /**
     * A large window of 1 MiB, only 3 small rows of a few tens of bytes, and the producer keeps
     * the connection open after sending. With a timed flush the downstream receives all 3 rows
     * within 2s; on the byte threshold alone it would receive nothing inside that window.
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void fewSmallRowsFlushedByTimeNotHeldUntilWindowFills() throws Exception {
        final int window = 1 << 20;  // 1 MiB, which is the production MasterServer default

        SocketChannelSource src = new SocketChannelSource(META, window);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        AtomicInteger received = new AtomicInteger();
        AtomicReference<Throwable> readerErr = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                StreamEvent e;
                while ((e = src.poll()) != null) {
                    if (e.isRecord()) {
                        received.incrementAndGet();
                    } else if (e.isError()) {
                        readerErr.set(new RuntimeException("unexpected error frame", e.cause()));
                        return;
                    }
                }
            } catch (Throwable t) {
                readerErr.set(t);
            }
        }, "timed-flush-reader");
        reader.setDaemon(true);
        reader.start();

        // Slow streaming: send 3 rows one at a time with a short pause between them, then keep the connection open, as an upstream still computing later rows would.
        for (int i = 0; i < 3; i++) {
            sink.send(StreamEvent.record(row(i)));
            Thread.sleep(20);
        }

        // The timed flush should release the 3 rows within about flushIntervalMs, so allow a generous 2s, far less than the time needed to fill 1 MiB.
        long deadline = System.currentTimeMillis() + 2000;
        while (received.get() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertNull(readerErr.get(), "the reader threw nothing: " + readerErr.get());
        assertEquals(3, received.get(),
                "3 small rows are flushed within a bounded time by the time slice, even below the 1 MiB byte threshold");

        sink.close();
        reader.join(2000);
        assertFalse(reader.isAlive(), "the reader ends normally once the sink sends EOS");
        src.close();
    }
}
