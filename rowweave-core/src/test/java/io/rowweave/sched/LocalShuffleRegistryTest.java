package io.rowweave.sched;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The in-process rendezvous of one WorkerServer: a downstream stage pre-creates and registers
 * its inbound {@link Channel}, and the upstream stage's FanoutSink looks it up by key with
 * await and a timeout. This matches ShuffleRegistry.awaitPartitions on the socket path, and the
 * await cannot be dropped because the assignment order is not the order in which the threads
 * run.
 */
class LocalShuffleRegistryTest {

    @Test
    void closeDrainsEntriesWakesWaitersAndPermanentlyRejectsResourceAcquisition() throws Exception {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        MultiProducerChannel active = new MultiProducerChannel(16, 1);
        reg.register("active", active);
        AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                reg.await("waiting", TimeUnit.MINUTES.toMillis(1));
            } catch (Throwable failure) {
                waiterFailure.set(failure);
            }
        }, "local-registry-close-waiter");
        waiter.start();
        while (reg.entryCount() < 2) {
            Thread.yield();
        }

        AutoCloseable closeable = assertInstanceOf(AutoCloseable.class, reg,
                "the local registry has an explicit lifecycle that the Worker closes at the end");
        closeable.close();
        waiter.join(1_000L);

        assertFalse(waiter.isAlive());
        assertTrue(waiterFailure.get() instanceof IllegalStateException);
        assertTrue(active.poll().isError(), "close poisons and frees the active channel");
        assertEquals(0, reg.entryCount());
        assertEquals(0, reg.activeChannelCount());

        MultiProducerChannel late = new MultiProducerChannel(16, 1);
        assertThrows(IllegalStateException.class, () -> reg.register("late", late));
        assertTrue(late.poll().isError());
        assertThrows(IllegalStateException.class,
                () -> reg.await("late-await", TimeUnit.MINUTES.toMillis(1)));
        reg.cancel("late-cleanup", new IllegalStateException("late cleanup"));
        reg.unregister("late-cleanup");
        reg.purgeExpired();
        assertEquals(0, reg.entryCount(), "terminal cleanup creates no new tombstone");
    }

    @Test
    void normalUnregisterRetainsLateAwaitFenceUntilBoundedExpiry() throws Exception {
        AtomicLong now = new AtomicLong(1_000L);
        LocalShuffleRegistry reg = new LocalShuffleRegistry(100L, 8, now::get);
        Channel channel = new MultiProducerChannel(16, 1);
        reg.register("normal", channel);
        assertSame(channel, reg.await("normal", 100));

        reg.unregister("normal");

        assertEquals(1, reg.entryCount(), "a normal terminal state absorbs a late producer await inside the grace window");
        assertEquals(1, reg.activeChannelCount(), "the same completed channel reference is kept through the grace window");
        assertSame(channel, reg.await("normal", 100), "a late await must not land on a fresh slot and wait forever");
        Channel duplicate = new MultiProducerChannel(16, 1);
        assertThrows(IllegalStateException.class, () -> reg.register("normal", duplicate),
                "a successful tombstone is not overwritten by a late register");
        assertTrue(duplicate.poll().isError(), "a refused late channel fails loudly");

        now.addAndGet(99L);
        reg.purgeExpired();
        assertEquals(1, reg.entryCount());
        now.incrementAndGet();
        reg.purgeExpired();
        assertEquals(0, reg.entryCount(), "a normal terminal state converges out of the long-lived registry after the TTL");
        assertEquals(0, reg.activeChannelCount());

        for (int i = 0; i < 1_000; i++) {
            String key = "normal-" + i;
            Channel next = new MultiProducerChannel(16, 1);
            reg.register(key, next);
            reg.unregister(key);
            now.addAndGet(100L);
            reg.purgeExpired();
        }
        assertEquals(0, reg.entryCount(), "normal terminal states do not accumulate for the life of the Worker");
    }

    @Test
    void cancelledAndTimedOutSlotsExpireOnlyAfterRetention() {
        AtomicLong now = new AtomicLong(1_000L);
        LocalShuffleRegistry reg = new LocalShuffleRegistry(100L, 8, now::get);
        Channel registered = new MultiProducerChannel(16, 1);
        IllegalStateException cancelled = new IllegalStateException("cancelled");
        reg.register("cancelled", registered);
        reg.cancel("cancelled", cancelled);

        assertEquals(1, reg.entryCount());
        assertEquals(0, reg.activeChannelCount(), "a cancel tombstone clears the large channel reference");
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await("cancelled", 1)));
        Channel late = new MultiProducerChannel(16, 1);
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.register("cancelled", late)));
        reg.unregister("cancelled");
        assertEquals(1, reg.entryCount(), "a normal unregister must not delete the cancel tombstone too early");

        IllegalStateException timeout = assertThrows(IllegalStateException.class,
                () -> reg.await("timeout", 1));
        assertSame(timeout, assertThrows(IllegalStateException.class,
                () -> reg.await("timeout", 1)), "a timeout is also a stable terminal state and leaves no empty slot");
        assertEquals(2, reg.entryCount());

        now.addAndGet(99L);
        reg.purgeExpired();
        assertEquals(2, reg.entryCount(), "while the TTL is running the late-arrival window stays intact");
        now.incrementAndGet();
        reg.purgeExpired();
        assertEquals(0, reg.entryCount(), "once the TTL expires every tombstone is released");

        Channel reused = new MultiProducerChannel(16, 1);
        reg.register("cancelled", reused);
        assertSame(reused, reg.await("cancelled", 100), "after the retention window the key can open a slot of a new generation");
    }

    @Test
    void concurrentNewKeysRespectHardLimitWhileExistingKeysStillWork() throws Exception {
        AtomicLong now = new AtomicLong(1_000L);
        LocalShuffleRegistry reg = new LocalShuffleRegistry(100L, 4, now::get);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 16; i++) {
                final int id = i;
                futures.add(pool.submit(() -> {
                    Channel channel = new MultiProducerChannel(16, 1);
                    try {
                        reg.register("cap-" + id, channel);
                        accepted.incrementAndGet();
                    } catch (IllegalStateException expected) {
                        rejected.incrementAndGet();
                        assertTrue(assertDoesNotThrow(channel::poll).isError(), "a refused new channel is poisoned");
                    }
                }));
            }
            for (Future<?> future : futures) future.get();
        } finally {
            pool.shutdownNow();
        }
        assertEquals(4, accepted.get());
        assertEquals(12, rejected.get());
        assertEquals(4, reg.entryCount(), "concurrent slot creation never exceeds maxEntries");
    }

    @Test
    void fullRegistryPreservesExistingTerminalSemanticsAndRecoversAfterTtl() {
        AtomicLong now = new AtomicLong(1_000L);
        LocalShuffleRegistry reg = new LocalShuffleRegistry(100L, 1, now::get);
        Channel current = new MultiProducerChannel(16, 1);
        reg.register("existing", current);
        assertSame(current, reg.await("existing", 1));

        IllegalStateException cancelled = new IllegalStateException("cancelled");
        reg.cancel("existing", cancelled);
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await("existing", 1)), "a FULL registry does not hide the cancelled terminal state of an existing key");
        Channel rejected = new MultiProducerChannel(16, 1);
        assertTrue(assertThrows(IllegalStateException.class,
                () -> reg.register("new", rejected)).getMessage().contains("full"));

        now.addAndGet(100L);
        reg.purgeExpired();
        Channel next = new MultiProducerChannel(16, 1);
        reg.register("new", next);
        assertSame(next, reg.await("new", 1), "capacity is restored once expired tombstones are released");
    }

    @Test
    void invalidLifecycleBoundsFailFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShuffleRegistry(0L, 1, System::currentTimeMillis));
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShuffleRegistry(1L, 0, System::currentTimeMillis));
    }

    /** Register then look up: the same channel is returned at once. */
    @Test
    void registerThenAwaitReturnsSameChannel() {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        Channel ch = new MultiProducerChannel(16, 1);
        reg.register("job1#s2#i0#e0", ch);
        Channel got = reg.await("job1#s2#i0#e0", 1000);
        assertSame(ch, got, "await returns the very inbound channel the downstream registered");
    }

    @Test
    void activeKeyRejectsDifferentChannelWithoutReplacingPublishedIdentity() throws Exception {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        Channel first = new MultiProducerChannel(16, 1);
        Channel duplicate = new MultiProducerChannel(16, 1);
        reg.register("stable-identity", first);
        assertSame(first, reg.await("stable-identity", 100));

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> reg.register("stable-identity", duplicate),
                "a published rendezvous identity is not silently replaced by another channel");

        assertTrue(rejected.getMessage().contains("different channel"));
        assertTrue(duplicate.poll().isError(), "a rejected newcomer is poisoned and cannot keep serving as a valid inbound");
        assertSame(first, reg.await("stable-identity", 100), "current and future await keep observing the first binding");
        assertDoesNotThrow(() -> reg.register("stable-identity", first), "repeatedly registering the same instance is strictly idempotent");
        assertSame(first, reg.await("stable-identity", 100));
    }

    /** Look up then register: await blocks first and is woken when the registration arrives, which tolerates threads running out of order. */
    @Test
    void awaitBlocksUntilRegister() throws Exception {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        Channel ch = new MultiProducerChannel(16, 1);
        AtomicReference<Channel> result = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            started.countDown();
            result.set(reg.await("k", 2000));
        });
        waiter.start();
        started.await();
        Thread.sleep(50);  // let the waiter enter the await block
        reg.register("k", ch);
        waiter.join(2000);
        assertSame(ch, result.get(), "a registration arriving after await also wakes it and returns the same channel");
    }

    /** Await past its timeout without a registration throws instead of hanging, matching ShuffleRegistry.awaitPartitions. */
    @Test
    void awaitTimesOutNotHang() {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        long t0 = System.currentTimeMillis();
        assertThrows(IllegalStateException.class, () -> reg.await("missing", 100),
                "await without a registration throws on timeout instead of hanging");
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed < 2000, "it returns near the timeout instead of hanging; measured " + elapsed + "ms");
    }

    /** Different keys stay isolated from one another. */
    @Test
    void distinctKeysIsolated() {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        Channel a = new MultiProducerChannel(16, 1);
        Channel b = new MultiProducerChannel(16, 1);
        reg.register("a", a);
        reg.register("b", b);
        assertSame(a, reg.await("a", 500));
        assertSame(b, reg.await("b", 500));
    }

    /** While a source is stuck in the outbound local await, a targeted cancel must wake it at once and a late registration must not resurrect it. */
    @Test
    void cancelKeyWakesCurrentAndFutureAwaitAndRejectsLateRegister() throws Exception {
        LocalShuffleRegistry reg = new LocalShuffleRegistry();
        IllegalStateException cancelled = new IllegalStateException("stage cancelled");
        AtomicReference<Throwable> current = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                reg.await("cancelled-key", TimeUnit.MINUTES.toMillis(1));
            } catch (Throwable t) {
                current.set(t);
            }
        }, "local-cancel-waiter");
        waiter.start();
        Thread.sleep(50L);

        reg.cancel("cancelled-key", cancelled);
        waiter.join(1000L);

        assertFalse(waiter.isAlive(), "cancelling a key wakes the pending await immediately");
        assertSame(cancelled, current.get(), "the current await sees the original cancellation cause");
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await("cancelled-key", TimeUnit.MINUTES.toMillis(1))),
                "a future await sees the same cancellation cause");
        Channel late = new MultiProducerChannel(16, 1);
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.register("cancelled-key", late)),
                "a late register cannot revive a cancelled key");
        assertTrue(late.poll().isError(), "a refused late local channel is poisoned");
    }
}
