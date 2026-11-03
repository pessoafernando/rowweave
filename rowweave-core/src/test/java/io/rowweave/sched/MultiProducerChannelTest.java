package io.rowweave.sched;

import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MultiProducerChannel: N producers writing together, the close latch, and the race with a
 * terminal poison. Cluster wiring uses N=1, but this is tested with N at least 2 because that
 * is the only reason the primitive exists.
 */
class MultiProducerChannelTest {

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    @Test
    void terminalObserverFiresOnceOnlyAtGlobalTerminalAndIsolatesFailures() {
        MultiProducerChannel ch = new MultiProducerChannel(4, 2);
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        ch.onTerminal(() -> {
            first.incrementAndGet();
            throw new IllegalStateException("observer boom");
        });
        ch.onTerminal(second::incrementAndGet);

        ch.producerDone();
        assertEquals(0, first.get(), "one producer finishing must not masquerade as the shared channel terminal state");
        assertDoesNotThrow(ch::producerDone, "an observer throwing must not break the EOS publication or the observers after it");
        ch.close();
        ch.fail(new RuntimeException("late poison"));

        assertEquals(1, first.get());
        assertEquals(1, second.get(), "close or a late poison does not notify the terminal state twice");
    }

    @Test
    void terminalObserverCoversClosePoisonAndLateRegistration() {
        MultiProducerChannel closed = new MultiProducerChannel(4, 1);
        AtomicInteger closeNotifications = new AtomicInteger();
        closed.onTerminal(closeNotifications::incrementAndGet);
        closed.close();
        closed.onTerminal(closeNotifications::incrementAndGet);
        assertEquals(2, closeNotifications.get(), "a registration before and after close each gets exactly one notification");

        MultiProducerChannel poisoned = new MultiProducerChannel(4, 1);
        AtomicInteger poisonNotifications = new AtomicInteger();
        poisoned.onTerminal(poisonNotifications::incrementAndGet);
        poisoned.fail(new RuntimeException("boom"));
        poisoned.onTerminal(poisonNotifications::incrementAndGet);
        assertEquals(2, poisonNotifications.get(), "a registration before and after a poison each gets exactly one notification");
    }

    @Test
    void terminalObserverRegistrationLinearizesWithProducerCompletion() throws Exception {
        for (int iteration = 0; iteration < 200; iteration++) {
            MultiProducerChannel ch = new MultiProducerChannel(4, 1);
            AtomicInteger notifications = new AtomicInteger();
            CyclicBarrier start = new CyclicBarrier(3);
            Thread register = new Thread(() -> {
                await(start);
                ch.onTerminal(notifications::incrementAndGet);
            });
            Thread complete = new Thread(() -> {
                await(start);
                ch.producerDone();
            });
            register.start();
            complete.start();
            await(start);
            register.join(2000);
            complete.join(2000);
            assertFalse(register.isAlive());
            assertFalse(complete.isAlive());
            assertEquals(1, notifications.get(), "the register and terminate race resolves exactly once (iteration=" + iteration + ")");
        }
    }

    @Test
    void recordBatchRespectsCapacityAndDrainsInOrder() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(2, 1);
        StreamEvent[] batch = {
                StreamEvent.record(row(1)),
                StreamEvent.record(row(2)),
                StreamEvent.record(row(3))
        };
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                ch.sendRecords(batch, batch.length);
                ch.producerDone();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        producer.start();

        awaitPark(ch);
        assertTrue(producer.isAlive(), "a batch larger than the capacity makes the producer wait and never crosses the hard boundary");
        assertEquals(1, ch.poll().row().get(0));
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertNull(failure.get());
        assertEquals(2, ch.poll().row().get(0));
        assertEquals(3, ch.poll().row().get(0));
        assertNull(ch.poll());
    }

    @Test
    void failWakesPartiallyEnqueuedRecordBatchAndPoisonWins() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(1, 1);
        StreamEvent[] batch = {StreamEvent.record(row(1)), StreamEvent.record(row(2))};
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                ch.sendRecords(batch, batch.length);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        producer.start();

        awaitPark(ch);
        RuntimeException root = new RuntimeException("boom");
        ch.fail(root);
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertInstanceOf(ChannelPoisonedException.class, failure.get());
        assertSame(root, failure.get().getCause(), "a batch producer keeps the first poison cause of the channel");
        assertTrue(ch.poll().isError(), "a fail clears the partial batch and leaves only the error terminal state");
    }

    @Test
    void invalidOrByteBoundRecordBatchFailsBeforeAnyEnqueue() throws Exception {
        MultiProducerChannel local = new MultiProducerChannel(4, 1);
        assertThrows(IllegalArgumentException.class,
                () -> local.sendRecords(new StreamEvent[]{StreamEvent.record(row(1))}, -1));
        assertThrows(IllegalArgumentException.class,
                () -> local.sendRecords(new StreamEvent[]{StreamEvent.record(row(1))}, 2));
        assertThrows(IllegalArgumentException.class,
                () -> local.sendRecords(new StreamEvent[]{StreamEvent.record(row(1)), null}, 2));
        assertThrows(IllegalArgumentException.class,
                () -> local.sendRecords(new StreamEvent[]{StreamEvent.record(row(1)), StreamEvent.endOfStream()}, 2));
        local.send(StreamEvent.record(row(9)));
        assertEquals(9, local.poll().row().get(0), "an illegal batch leaves no partial record behind");

        MultiProducerChannel byteBound = new MultiProducerChannel(4, 1, 128);
        assertThrows(IllegalStateException.class,
                () -> byteBound.sendRecords(new StreamEvent[]{StreamEvent.record(row(1))}, 1));
        assertTrue(byteBound.offer(StreamEvent.record(row(7)), 16));
        assertEquals(7, byteBound.poll().row().get(0), "after refusing a batch the byte-credit path still works");
    }

    private static void awaitPark(MultiProducerChannel channel) throws InterruptedException {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (channel.parkCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertTrue(channel.parkCount() > 0, "producer did not park before deadline");
    }

/** The close, which emits the EOS, happens only after every producer is done: two producers, each done once. */
    @Test
    void closeLatchAllProducersDone() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(64, 2);
        ch.send(StreamEvent.record(row(1)));
        ch.producerDone();  // one producer done, not closed yet
        ch.send(StreamEvent.record(row(2)));
        ch.producerDone();  // both producers done, so close

        List<Integer> got = new ArrayList<>();
        StreamEvent e;
        while ((e = ch.poll()) != null) {
            got.add((Integer) e.row().get(0));
        }
        assertEquals(2, got.size());
    }

/** A single producer being done does not close, so the consumer must not see an EOS. */
    @Test
    void partialProducerDoneDoesNotClose() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(64, 2);
        ch.send(StreamEvent.record(row(1)));
        ch.producerDone(); // 1/2 done
/** One live producer remains, so nothing closes, and poll returns the record because the queue is not empty. */
        StreamEvent e = ch.poll();
        assertNotNull(e);
        assertTrue(e.isRecord());
    }

/** The terminal poison wins over a late close: after a fail, producerDone reaching zero must not append an EOS after the error. */
    @Test
    void poisonWinsOverLateClose() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(64, 2);
        ch.send(StreamEvent.record(row(1)));
        RuntimeException first = new RuntimeException("first");
        ch.fail(first);  // poison: clear the records, emit the error
        ch.fail(new RuntimeException("second"));  // already poisoned, so this is strictly idempotent and must not replace first
        ch.producerDone();  // already poisoned, so a no-op
        ch.producerDone();  // still a no-op, so no EOS is emitted

        StreamEvent e = ch.poll();
        assertNotNull(e);
        assertTrue(e.isError(), "head must be error, not EOS/record");
        assertSame(first, e.cause(), "a repeated fail does not overwrite the first poison cause");
    }

/** A fail after close still poisons, since fail is the terminal state: close emitted an EOS, and the fail must clear it and emit the error. */
    @Test
    void failAfterCloseStillPoisons() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(64, 1);
        ch.send(StreamEvent.record(row(1)));
        ch.producerDone();  // close, so the EOS is queued behind the record
        RuntimeException root = new RuntimeException("late boom");
        ch.fail(root);
        StreamEvent e = ch.poll();
        assertNotNull(e);
        assertTrue(e.isError());
        assertSame(root, e.cause(), "the first poison still wins over a normal EOS that arrived earlier");
    }

/** A send parks on a full channel, a fail wakes it and it throws ChannelPoisonedException, so nothing deadlocks. */
    @Test
    void failWakesParkedSend() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(1, 2);
        ch.send(StreamEvent.record(row(1)));  // fill it
        AtomicInteger poisonSeen = new AtomicInteger();
        AtomicReference<Throwable> senderFailure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                ch.send(StreamEvent.record(row(2)));  // parks, since the channel is full
            } catch (ChannelPoisonedException ex) {
                senderFailure.set(ex);
                poisonSeen.incrementAndGet();
            }
        });
        producer.start();
        Thread.sleep(50);  // let the producer park
        RuntimeException root = new RuntimeException("boom");
        ch.fail(root);
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertEquals(1, poisonSeen.get());
        assertSame(root, senderFailure.get().getCause(), "a parked producer keeps the first poison cause of the channel");
    }

/** Concurrent producerDone from at least two producers racing a fail: whatever the interleaving, the terminal state is consistent, either all EOS or the error, never a mix. */
    @Test
    void concurrentProducerDoneVsFailRace() throws Exception {
        for (int iter = 0; iter < 200; iter++) {
            final int n = 3;
            MultiProducerChannel ch = new MultiProducerChannel(64, n);
            ch.send(StreamEvent.record(row(99)));
            CyclicBarrier barrier = new CyclicBarrier(n + 1);
            List<Thread> threads = new ArrayList<>();
/** Start n-1 producerDone calls and one fail together. */
            for (int i = 0; i < n - 1; i++) {
                threads.add(new Thread(() -> {
                    await(barrier);
                    ch.producerDone();
                }));
            }
            threads.add(new Thread(() -> {
                await(barrier);
                ch.fail(new RuntimeException("race boom"));
            }));
            for (Thread t : threads) t.start();
            await(barrier);  // release them all at once
            for (Thread t : threads) t.join(2000);

/** Since a fail took part the state must be poisoned, so the first non-record from poll must be the error, never an EOS after an error. */
            StreamEvent e;
            boolean sawError = false;
            int polls = 0;
            while ((e = ch.poll()) != null && polls++ < 10) {
                if (e.isError()) {
                    sawError = true;
                    break;
                }
            }
            assertTrue(sawError, "a terminal state reached through a fail always shows the error sentinel (iter " + iter + ")");
        }
    }

    private static void await(CyclicBarrier b) {
        try {
            b.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

/** ---- non-blocking offer, which the server demux uses on an EventLoop thread and must never block ---- */

/** An offer below capacity is queued and returns true, and poll can take it out. */
    @Test
    void offerAcceptsWhenNotFull() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(4, 1);
        assertTrue(ch.offer(StreamEvent.record(row(1)), 0), "an offer below capacity succeeds");
        assertEquals(1, ch.poll().row().get(0));
    }

/** An offer at capacity returns false without blocking and without parking the EventLoop; once room frees up, a later offer succeeds. */
    @Test
    void offerReturnsFalseWhenFullNeverBlocks() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(2, 1);
        assertTrue(ch.offer(StreamEvent.record(row(1)), 0));
        assertTrue(ch.offer(StreamEvent.record(row(2)), 0));
/** Full, at capacity 2 with a non-empty queue, so offer returns false immediately without blocking. */
        long t0 = System.nanoTime();
        assertFalse(ch.offer(StreamEvent.record(row(3)), 0), "a full offer returns false without blocking");
        assertTrue((System.nanoTime() - t0) < 100_000_000L, "the offer returns immediately without blocking");
        ch.poll();  // free one slot
        assertTrue(ch.offer(StreamEvent.record(row(3)), 0), "once space is freed the offer succeeds");
    }

/** An empty queue always admits one event as the atomic unit, so an offer succeeds even when the bytes exceed maxBytes. */
    @Test
    void offerEmptyQueueAlwaysAccepts() {
        MultiProducerChannel ch = new MultiProducerChannel(64, 1, 10 /*maxBytes*/);
        assertTrue(ch.offer(StreamEvent.record(row(1)), 1000), "an empty queue admits a record even past the byte limit because one record is the atomic unit");
    }

/** An offer to a poisoned channel throws, matching send, since the channel is terminal. */
    @Test
    void offerOnPoisonedThrows() {
        MultiProducerChannel ch = new MultiProducerChannel(4, 1);
        RuntimeException root = new RuntimeException("boom");
        ch.fail(root);
        ChannelPoisonedException failure = assertThrows(
                ChannelPoisonedException.class, () -> ch.offer(StreamEvent.record(row(1)), 0));
        assertSame(root, failure.getCause());
    }

/** Wire credit is returned only once the consumer has really taken the record out, never when it is queued off the network. */
    @Test
    void consumedCallbackRunsAfterPollOutsideQueueingPath() throws Exception {
        MultiProducerChannel ch = new MultiProducerChannel(4, 1, 128);
        AtomicInteger returned = new AtomicInteger();

        ch.send(StreamEvent.record(row(1)), 37, () -> returned.addAndGet(37));
        assertEquals(0, returned.get(), "enqueuing alone returns no credit");

        assertEquals(1, ch.poll().row().get(0));
        assertEquals(37, returned.get(), "credit for the exact wire bytes comes back only once the consumer really polls");
    }

/** Clearing the queue on failure must not look like consumption; the target ERROR or the physical failure is what wakes the sender. */
    @Test
    void poisonDropsQueuedConsumedCallbacks() {
        MultiProducerChannel ch = new MultiProducerChannel(4, 1, 128);
        AtomicInteger returned = new AtomicInteger();
        ch.send(StreamEvent.record(row(1)), 37, returned::incrementAndGet);

        ch.fail(new RuntimeException("boom"));

        assertEquals(0, returned.get(), "records cleared by a fail were never consumed so no credit is returned");
    }
}
