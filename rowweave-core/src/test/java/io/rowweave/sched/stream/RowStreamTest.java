package io.rowweave.sched.stream;

import io.rowweave.DataRow;
import io.rowweave.sched.Channel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RowStreamTest {

    private static DataRow row(Object v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    @Test
    void fromIterableEmitsRecordsThenNull() {
        RowStream s = RowStream.fromIterable(Arrays.asList(row("a"), row("b")));
        StreamEvent e1 = s.next();
        assertTrue(e1.isRecord());
        assertEquals("a", e1.row().get(0));
        StreamEvent e2 = s.next();
        assertEquals("b", e2.row().get(0));
        assertNull(s.next());
    }

    @Test
    void emptyIterableYieldsNullImmediately() {
        RowStream s = RowStream.fromIterable(new ArrayList<>());
        assertNull(s.next());
    }

    @Test
    void toIteratorReturnsRecordRows() {
        RowStream s = RowStream.fromIterable(Arrays.asList(row(1), row(2), row(3)));
        Iterator<DataRow> it = s.toIterator();
        List<Object> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.next().get(0));
        }
        assertEquals(Arrays.asList(1, 2, 3), out);
    }

    @Test
    void toIteratorStopsAtEndOfStream() {
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row("a")),
                StreamEvent.endOfStream(),
                StreamEvent.record(row("after-eos")));
        Iterator<StreamEvent> src = events.iterator();
        RowStream s = () -> src.hasNext() ? src.next() : null;
        Iterator<DataRow> it = s.toIterator();
        assertTrue(it.hasNext());
        assertEquals("a", it.next().get(0));
        assertFalse(it.hasNext());
    }

    @Test
    void toIteratorThrowsOnError() {
        // toIterator pre-fetches eagerly: returning record "a" pulls the next event (the error),
        // so the throw surfaces on the it.next() that emits "a".
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row("a")),
                StreamEvent.error(boom));
        Iterator<StreamEvent> src = events.iterator();
        RowStream s = () -> src.hasNext() ? src.next() : null;
        Iterator<DataRow> it = s.toIterator();
        RuntimeException ex = assertThrows(RuntimeException.class, it::next);
        assertSame(boom, ex.getCause());
    }

    @Test
    void toIteratorTransparentToBarrier() {
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.barrier(1L),
                StreamEvent.record(row("a")),
                StreamEvent.barrier(2L),
                StreamEvent.record(row("b")));
        Iterator<StreamEvent> src = events.iterator();
        RowStream s = () -> src.hasNext() ? src.next() : null;
        List<Object> out = new ArrayList<>();
        Iterator<DataRow> it = s.toIterator();
        while (it.hasNext()) {
            out.add(it.next().get(0));
        }
        assertEquals(Arrays.asList("a", "b"), out);
    }

    @Test
    void channelAdapterAdvertisesCancellationAndWakesBlockedPoll() throws Exception {
        CountDownLatch polled = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicReference<Throwable> cause = new AtomicReference<>();
        Channel channel = new Channel() {
            @Override public void send(StreamEvent event) { }
            @Override public StreamEvent poll() throws InterruptedException {
                polled.countDown();
                released.await();
                return StreamEvent.error(cause.get());
            }
            @Override public void close() { released.countDown(); }
            @Override public void fail(Throwable failure) {
                cause.set(failure);
                released.countDown();
            }
        };
        RowStream stream = RowStream.fromChannel(channel);
        Thread reader = new Thread(() -> {
            StreamEvent event = stream.next();
            assertTrue(event.isError());
        });
        reader.start();
        assertTrue(polled.await(1, TimeUnit.SECONDS));

        assertTrue(stream.supportsCancellation());
        assertTrue(stream.cancel(new CancellationException("stop")));

        reader.join(1000);
        assertFalse(reader.isAlive(), "cancel must wake channel.poll()");
    }
}
