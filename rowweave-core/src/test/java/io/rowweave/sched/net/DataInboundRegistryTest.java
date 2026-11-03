package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The per-worker demux registry. When a frame arrives, the server uses its globally unique
 * targetId to find the downstream stage's inbound {@link MultiProducerChannel} and its
 * DataSchema, reproducing the register and await parking semantics of LocalShuffleRegistry.
 * The jobId is only used by unregisterJob for cleanup, since the worker registry is long-lived
 * and entries are cleared when a job reaches a terminal state.
 */
class DataInboundRegistryTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static MultiProducerChannel chan() {
        return new MultiProducerChannel(1024, 1, 0);
    }

    @Test
    void normalUnregisterReleasesEntriesWithoutGrowth() {
        DataInboundRegistry reg = new DataInboundRegistry();
        for (int i = 0; i < 1_000; i++) {
            UUID target = new UUID(1L, i);
            reg.register(target, "job-" + i, chan(), META);
            reg.unregisterJob("job-" + i);
        }
        assertEquals(0, reg.entryCount(), "normal stage terminal states accumulate no mux targets");
        assertEquals(0, reg.activeEntryCount());
        assertEquals(0, reg.jobCount(), "the jobId grouping index is released at the same time");
    }

    @Test
    void cancelledAndTimedOutTargetsExpireOnlyAfterRetention() {
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(100L, 8, now::get);
        UUID cancelledTarget = new UUID(2L, 1L);
        IllegalStateException cancelled = new IllegalStateException("cancelled");
        reg.register(cancelledTarget, "cancel-job", chan(), META);
        reg.cancel(cancelledTarget, cancelled);
        reg.unregisterJob("cancel-job");

        assertEquals(1, reg.entryCount());
        assertEquals(0, reg.activeEntryCount(), "a cancel tombstone clears the channel and meta Entry");
        assertEquals(0, reg.jobCount());
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await(cancelledTarget, 1)));
        MultiProducerChannel late = chan();
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.register(cancelledTarget, "late", late, META)));

        UUID timeoutTarget = new UUID(2L, 2L);
        IllegalStateException timeout = assertThrows(IllegalStateException.class,
                () -> reg.await(timeoutTarget, 1));
        assertSame(timeout, assertThrows(IllegalStateException.class,
                () -> reg.await(timeoutTarget, 1)));
        assertEquals(2, reg.entryCount());

        now.addAndGet(99L);
        reg.purgeExpired();
        assertEquals(2, reg.entryCount());
        now.incrementAndGet();
        reg.purgeExpired();
        assertEquals(0, reg.entryCount());
    }

    @Test
    void concurrentNewTargetsRespectHardLimit() throws Exception {
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(100L, 4, now::get);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 16; i++) {
                final int id = i;
                futures.add(pool.submit(() -> {
                    MultiProducerChannel channel = chan();
                    try {
                        reg.register(new UUID(3L, id), "cap-" + id, channel, META);
                        accepted.incrementAndGet();
                    } catch (IllegalStateException expected) {
                        rejected.incrementAndGet();
                        assertTrue(assertDoesNotThrow(channel::poll).isError(), "a refused new mux channel is poisoned");
                    }
                }));
            }
            for (Future<?> future : futures) future.get();
        } finally {
            pool.shutdownNow();
        }
        assertEquals(4, accepted.get());
        assertEquals(12, rejected.get());
        assertEquals(4, reg.entryCount());
    }

    @Test
    void fullRegistryPreservesExistingTerminalSemanticsAndRecoversAfterTtl() {
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(100L, 1, now::get);
        UUID existing = new UUID(4L, 1L);
        reg.register(existing, "job", chan(), META);
        IllegalStateException cancelled = new IllegalStateException("cancelled");
        reg.cancel(existing, cancelled);
        reg.unregisterJob("job");
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await(existing, 1)));

        MultiProducerChannel rejected = chan();
        assertTrue(assertThrows(IllegalStateException.class,
                () -> reg.register(new UUID(4L, 2L), "new", rejected, META))
                .getMessage().contains("full"));
        now.addAndGet(100L);
        reg.purgeExpired();

        UUID next = new UUID(4L, 3L);
        MultiProducerChannel channel = chan();
        reg.register(next, "next", channel, META);
        assertSame(channel, reg.await(next, 1).channel);
    }

    @Test
    void invalidLifecycleBoundsFailFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new DataInboundRegistry(0L, 1, System::currentTimeMillis));
        assertThrows(IllegalArgumentException.class,
                () -> new DataInboundRegistry(1L, 0, System::currentTimeMillis));
    }

    @Test
    void invalidRegistrationIdentityFailsBeforeRegistryMutation() {
        DataInboundRegistry reg = new DataInboundRegistry();
        assertThrows(IllegalArgumentException.class,
                () -> reg.register(null, "job", chan(), META));
        assertThrows(IllegalArgumentException.class,
                () -> reg.register(new UUID(0L, 41L), null, chan(), META));
        assertEquals(0, reg.entryCount());
        assertEquals(0, reg.jobCount());
    }

    @Test
    void unregisterJobWaitsForInFlightPublicationAndLeavesNoActiveTarget() throws Exception {
        CountDownLatch registerInsideLifecycle = new CountDownLatch(1);
        CountDownLatch releaseRegister = new CountDownLatch(1);
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(100L, 8, () -> {
            registerInsideLifecycle.countDown();
            try {
                if (!releaseRegister.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test did not release registration");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("registration interrupted", interrupted);
            }
            return now.get();
        });
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> registering = pool.submit(() ->
                reg.register(new UUID(0L, 42L), "terminal-job", chan(), META));
        assertTrue(registerInsideLifecycle.await(1, TimeUnit.SECONDS),
                "registration reaches the lifecycle critical section before publication");

        CountDownLatch cleanupInvoked = new CountDownLatch(1);
        Future<?> cleanup = pool.submit(() -> {
            cleanupInvoked.countDown();
            reg.unregisterJob("terminal-job");
        });
        assertTrue(cleanupInvoked.await(1, TimeUnit.SECONDS));
        boolean returnedBeforePublicationFinished;
        try {
            cleanup.get(150, TimeUnit.MILLISECONDS);
            returnedBeforePublicationFinished = true;
        } catch (TimeoutException expectedBarrier) {
            returnedBeforePublicationFinished = false;
        } finally {
            releaseRegister.countDown();
        }
        try {
            try {
                registering.get(2, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException cleanupWon) {
                assertTrue(cleanupWon.getCause() instanceof IllegalStateException);
                assertTrue(cleanupWon.getCause().getMessage().contains("terminal"),
                        "when cleanup wins the linearization point an in-flight registration is refused by the terminal barrier");
            }
            cleanup.get(2, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertFalse(returnedBeforePublicationFinished,
                "unregisterJob must not return early past a registration that already entered the lifecycle critical section");
        assertEquals(0, reg.activeEntryCount(), "no active target is left behind once the terminal barrier returns");
        assertEquals(0, reg.jobCount(), "nothing is written back into the Job index once the terminal barrier returns");
    }

    @Test
    void completedJobRejectsLateTargetUntilRetentionExpires() throws Exception {
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(100L, 8, now::get);
        reg.unregisterJob("terminal-job");

        MultiProducerChannel late = chan();
        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> reg.register(new UUID(0L, 43L), "terminal-job", late, META),
                "a target arriving after the Job terminal barrier cannot revive the stage");
        assertTrue(rejected.getMessage().contains("terminal"));
        assertTrue(late.poll().isError(), "a newcomer channel that registers late is poisoned");
        assertEquals(0, reg.activeEntryCount());
        assertEquals(0, reg.jobCount());

        now.addAndGet(100L);
        reg.purgeExpired();
        MultiProducerChannel reusedAfterRetention = chan();
        assertDoesNotThrow(() -> reg.register(
                new UUID(0L, 44L), "terminal-job", reusedAfterRetention, META),
                "once bounded retention expires the Job identity is not held forever");
        assertSame(reusedAfterRetention, reg.await(new UUID(0L, 44L), 1L).channel);
    }

    @Test
    void repeatedJobCleanupDoesNotExtendTerminalRetention() {
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(100L, 8, now::get);
        reg.unregisterJob("terminal-job");
        now.addAndGet(50L);
        reg.unregisterJob("terminal-job");

        now.addAndGet(50L);
        reg.purgeExpired();
        MultiProducerChannel reused = chan();
        assertDoesNotThrow(() -> reg.register(new UUID(0L, 45L), "terminal-job", reused, META),
                "an idempotent cleanup does not extend the Job tombstone without bound");
    }

    @Test
    void terminalJobTombstonesRespectRegistryCapacity() {
        AtomicLong now = new AtomicLong(1_000L);
        DataInboundRegistry reg = new DataInboundRegistry(1_000L, 2, now::get);
        reg.unregisterJob("job-0");
        now.incrementAndGet();
        reg.unregisterJob("job-1");
        now.incrementAndGet();
        reg.unregisterJob("job-2");

        assertThrows(IllegalStateException.class,
                () -> reg.register(new UUID(0L, 46L), "job-2", chan(), META),
                "the newest terminal state stays protected by its tombstone");
        MultiProducerChannel oldestReused = chan();
        assertDoesNotThrow(() -> reg.register(new UUID(0L, 47L), "job-0", oldestReused, META),
                "under capacity pressure only the oldest Job tombstone is evicted and the registry does not grow without bound");
        assertEquals(1, reg.activeEntryCount());
        assertEquals(1, reg.jobCount());
    }

    /** Registering before awaiting hits the same channel and meta immediately. */
    @Test
    void registerThenAwaitHits() {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c = chan();
        reg.register(new UUID(0L,5L), "job1", c, META);
        DataInboundRegistry.Entry e = reg.await(new UUID(0L,5L), 1000);
        assertSame(c, e.channel, "the channel matched by the registration");
        assertSame(META, e.dataSchema, "the schema matched by the registration");
    }

    @Test
    void activeTargetRejectsDifferentChannelAndJobWithoutReplacingPublishedEntry() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        UUID target = new UUID(0L, 6L);
        MultiProducerChannel first = chan();
        MultiProducerChannel duplicate = chan();
        reg.register(target, "winner-job", first, META, 2, 256L);
        DataInboundRegistry.Entry published = reg.await(target, 100L);

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> reg.register(target, "loser-job", duplicate, META, 2, 256L),
                "a published remote target is not silently replaced by another channel or job");

        assertTrue(rejected.getMessage().contains("conflicting registration"));
        assertTrue(duplicate.poll().isError(), "a newcomer on a different channel is poisoned");
        assertSame(published, reg.await(target, 100L), "current and future handlers observe the same Entry");
        assertSame(first, reg.await(target, 100L).channel);
        assertEquals(1, reg.jobCount(), "a failed duplicate leaves the second Job index untouched");
        reg.unregisterJob("loser-job");
        assertSame(published, reg.await(target, 100L), "cleanup of the losing Job does not delete the winner target");
    }

    @Test
    void sameChannelRequiresEquivalentProtocolContractForIdempotency() {
        DataInboundRegistry reg = new DataInboundRegistry();
        UUID target = new UUID(0L, 61L);
        MultiProducerChannel channel = chan();
        reg.register(target, "job", channel, META, 2, 256L);
        DataInboundRegistry.Entry published = reg.await(target, 100L);

        assertThrows(IllegalStateException.class,
                () -> reg.register(target, "other-job", channel, META, 2, 256L),
                "the same channel cannot move the target to another Job either");
        assertThrows(IllegalStateException.class,
                () -> reg.register(target, "job", channel, META, 2, 512L),
                "the same channel cannot silently change the credit window");
        DataSchema differentSchema = new DataSchema(new Field("v", DataType.LongType));
        assertThrows(IllegalStateException.class,
                () -> reg.register(target, "job", channel, differentSchema, 2, 256L),
                "the same channel cannot silently change the decoding schema");
        assertSame(published, reg.await(target, 100L), "a conflicting registration neither replaces the winner Entry nor poisons it");
        DataRow row = new DataRow(1);
        row.set(0, 7);
        assertDoesNotThrow(() -> channel.send(StreamEvent.record(row)), "a contract conflict does not poison the winner channel");
        assertSame(row, assertDoesNotThrow(channel::poll).row());

        DataSchema equivalentSchema = new DataSchema(new Field("v", DataType.IntegerType));
        assertDoesNotThrow(() -> reg.register(target, "job", channel, equivalentSchema, 2, 256L),
                "registering the same channel with a structurally equivalent schema is idempotent");
        assertSame(published, reg.await(target, 100L), "an idempotent retry creates no new Entry identity");
        assertEquals(1, reg.jobCount());
    }

    /** targetId is globally unique, so different targets stay independent and no jobId compound key is needed. */
    @Test
    void distinctTargetsIndependent() {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel a = chan();
        MultiProducerChannel b = chan();
        reg.register(new UUID(0L,7L), "jobA", a, META);
        reg.register(new UUID(0L,8L), "jobB", b, META);
        assertSame(a, reg.await(new UUID(0L,7L), 1000).channel);
        assertSame(b, reg.await(new UUID(0L,8L), 1000).channel);
    }

    /** An await that arrives before the register, that is a frame that arrives early, parks until the register wakes it. */
    @Test
    void awaitBeforeRegisterParksUntilRegistered() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c = chan();
        AtomicReference<DataInboundRegistry.Entry> got = new AtomicReference<>();
        Thread waiter = new Thread(() -> got.set(reg.await(new UUID(0L,9L), 3000)));
        waiter.setDaemon(true);
        waiter.start();
        Thread.sleep(100);  // let the waiter park
        assertNull(got.get(), "await blocks until the register arrives");
        reg.register(new UUID(0L,9L), "job1", c, META);
        waiter.join(2000);
        assertNotNull(got.get(), "the arriving register wakes the await");
        assertSame(c, got.get().channel);
    }

    /** An await that times out without a register throws rather than hanging. */
    @Test
    void awaitTimeoutThrows() {
        DataInboundRegistry reg = new DataInboundRegistry();
        assertThrows(IllegalStateException.class, () -> reg.await(new UUID(0L,1L), 150));
    }

    /** Unregistering a terminal job removes every target of that job, which keeps the long-lived registry from leaking. */
    @Test
    void unregisterJobRemovesAllTargets() {
        DataInboundRegistry reg = new DataInboundRegistry();
        reg.register(new UUID(0L,1L), "job1", chan(), META);
        reg.register(new UUID(0L,2L), "job1", chan(), META);
        reg.unregisterJob("job1");
        assertThrows(IllegalStateException.class, () -> reg.await(new UUID(0L,1L), 100));
        assertThrows(IllegalStateException.class, () -> reg.await(new UUID(0L,2L), 100));
    }

    /** Server teardown must cancel the awaits of unknown targets right away, without waiting out the full await timeout. */
    @Test
    void closeWakesPendingAwaitAndRejectsLaterRegistration() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                reg.await(new UUID(0L, 99L), TimeUnit.MINUTES.toMillis(1));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "registry-close-waiter");
        waiter.start();
        Thread.sleep(50L);

        reg.close();
        waiter.join(1000L);

        assertFalse(waiter.isAlive(), "closing the registry wakes the pending await");
        assertTrue(failure.get() instanceof IllegalStateException);
        MultiProducerChannel late = chan();
        assertThrows(IllegalStateException.class,
                () -> reg.register(new UUID(0L, 100L), "late-job", late, META));
        assertTrue(late.poll().isError(), "a channel whose registration is refused after close is poisoned");
    }

    /** Closing the registry poisons the registered channels, which releases the bounded send parks of DataDemuxHandler. */
    @Test
    void closePoisonsRegisteredChannels() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel channel = chan();
        reg.register(new UUID(0L, 101L), "job", channel, META);
        reg.close();
        assertTrue(channel.poll().isError());
        assertEquals(0, reg.entryCount(), "every slot strong reference is dropped before close returns");
        assertEquals(0, reg.activeEntryCount());
        assertEquals(0, reg.jobCount(), "the job index is cleared before close returns");

        reg.cancel(new UUID(0L, 102L), new IllegalStateException("late cleanup"));
        reg.unregisterJob("late-job");
        reg.purgeExpired();
        assertEquals(0, reg.entryCount(), "terminal cleanup creates no new tombstone");
    }

    /** A targeted cancel must wake both the current and any later await, and a late register must not overwrite the cancelled terminal state. */
    @Test
    void cancelTargetWakesCurrentAndFutureAwaitAndRejectsLateRegister() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        UUID target = new UUID(0L, 102L);
        IllegalStateException cancelled = new IllegalStateException("stage cancelled");
        AtomicReference<Throwable> current = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                reg.await(target, TimeUnit.MINUTES.toMillis(1));
            } catch (Throwable t) {
                current.set(t);
            }
        }, "target-cancel-waiter");
        waiter.start();
        Thread.sleep(50L);

        reg.cancel(target, cancelled);
        waiter.join(1000L);

        assertFalse(waiter.isAlive(), "cancelling a target wakes the pending await immediately");
        assertSame(cancelled, current.get(), "the current await sees the original cancellation cause");
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await(target, TimeUnit.MINUTES.toMillis(1))),
                "a future await sees the same cancellation cause");
        MultiProducerChannel late = chan();
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.register(target, "late-job", late, META)),
                "a late register cannot revive a cancelled target");
        assertTrue(late.poll().isError(), "a refused late channel is poisoned");
    }

    /** Once a registered target is cancelled, the consumer's unregister must not delete the tombstone early and leave a late producer waiting again. */
    @Test
    void unregisterJobPreservesCancelledTargetTombstone() {
        DataInboundRegistry reg = new DataInboundRegistry();
        UUID target = new UUID(0L, 103L);
        IllegalStateException cancelled = new IllegalStateException("job cancelled");
        reg.register(target, "job-cancelled", chan(), META);
        reg.cancel(target, cancelled);

        reg.unregisterJob("job-cancelled");

        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.await(target, TimeUnit.MINUTES.toMillis(1))));
        MultiProducerChannel late = chan();
        assertSame(cancelled, assertThrows(IllegalStateException.class,
                () -> reg.register(target, "late", late, META)));
    }
}
