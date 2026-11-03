package io.rowweave.sched;

import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static java.time.Duration.ofSeconds;

class LocalChannelTest {

    private static DataRow row(Object v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    @Test
    void sendThenPollReturnsRecordsInOrder() {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            Channel ch = new LocalChannel(8);
            ch.send(StreamEvent.record(row("a")));
            ch.send(StreamEvent.record(row("b")));
            ch.close();
            assertEquals("a", ch.poll().row().get(0));
            assertEquals("b", ch.poll().row().get(0));
            assertNull(ch.poll()); // EOS after drain
        });
    }

    @Test
    void pollReturnsNullAtEndOfStreamAndStaysNull() {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            Channel ch = new LocalChannel(4);
            ch.close();
            assertNull(ch.poll());
            assertNull(ch.poll()); // re-arm: still null for the single consumer
        });
    }

    @Test
    void sendOnClosedChannelThrows() {
        Channel ch = new LocalChannel(4);
        ch.close();
        assertThrows(IllegalStateException.class, () -> ch.send(StreamEvent.record(row("x"))));
    }

    @Test
    void parkCountIncrementsWhenFull() throws InterruptedException {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            Channel ch = new LocalChannel(1);
            ch.send(StreamEvent.record(row("a"))); // fills the single slot, no park
            Thread producer = new Thread(() -> ch.send(StreamEvent.record(row("b")))); // must park
            producer.start();
            // wait until producer is parked on the full queue
            long deadline = System.currentTimeMillis() + 3000;
            while (ch.parkCount() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertTrue(ch.parkCount() > 0, "send on full channel should park");
            assertEquals("a", ch.poll().row().get(0)); // free a slot -> producer proceeds
            producer.join(2000);
            assertEquals("b", ch.poll().row().get(0));
        });
    }
}
