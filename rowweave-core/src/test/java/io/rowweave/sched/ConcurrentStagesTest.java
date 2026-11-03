package io.rowweave.sched;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConcurrentStages.runFailFast, the general concurrent orchestration primitive
 * moved down from the join fast path in Master. A success path runs every stage, and when a
 * stage throws every channel is poisoned to release the parks and the real cause is passed
 * through rather than a consequential poisoning error.
 */
class ConcurrentStagesTest {

    /** All stages succeed: the onFinish wrap-up runs and nothing throws. */
    @Test
    void allSucceed() {
        AtomicInteger done = new AtomicInteger();
        List<Task> tasks = Arrays.asList(
                new Task("a", done::incrementAndGet, c -> { }),
                new Task("b", done::incrementAndGet, c -> { }),
                new Task("c", done::incrementAndGet, c -> { }));
        ConcurrentStages.runFailFast(tasks, Arrays.asList("a", "b", "c"), java.util.Collections.emptyList());
        assertEquals(3, done.get());
    }

    /** One stage throws: the real cause is passed through and every channel is poisoned, which releases the other parked sends. */
    @Test
    void oneFailsPoisonsChannelsAndPropagatesRootCause() throws Exception {
        LocalChannel ch = new LocalChannel(1);
        ch.send(io.rowweave.sched.stream.StreamEvent.record(new io.rowweave.DataRow(0)));  // fill it, so the next send parks

        // Stage A sends into a full channel and parks, until the poisoning makes it throw ChannelPoisonedException.
        Task parked = new Task("parked", () -> {
            ch.send(io.rowweave.sched.stream.StreamEvent.record(new io.rowweave.DataRow(0)));
        }, c -> { });
        // Stage B throws the real cause.
        Task boom = new Task("boom", () -> { throw new IllegalStateException("real boom"); }, c -> { });

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                ConcurrentStages.runFailFast(Arrays.asList(parked, boom),
                        Arrays.asList("parked", "boom"), Arrays.asList(ch)));
        // The real cause passes through, not a consequential poisoning error: the message carries the stage name boom.
        assertTrue(ex.getMessage().contains("boom"), "the fail-fast surfaces the stage that really failed, observed: " + ex.getMessage());
        assertTrue(parked.isCompleted(), "the parked stage is poisoned and completes instead of deadlocking");
    }

    @Test
    void poisonCleanupFailureCannotSkipTaskCancellationAndJoin() {
        RuntimeException business = new RuntimeException("business failed");
        RuntimeException poisonCleanup = new RuntimeException("channel poison failed");
        CountDownLatch parkedStarted = new CountDownLatch(1);
        Task parked = new Task("parked", () -> {
            parkedStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, cause -> { });
        Task boom = new Task("boom", () -> {
            try {
                assertTrue(parkedStarted.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            throw business;
        }, cause -> { });
        Channel badCleanup = new Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { }
            @Override public void fail(Throwable cause) { throw poisonCleanup; }
        };

        RuntimeException observed = assertThrows(RuntimeException.class, () ->
                ConcurrentStages.runFailFast(Arrays.asList(parked, boom),
                        Arrays.asList("parked", "boom"),
                        Arrays.asList(badCleanup, badCleanup)));

        assertSame(business, observed.getCause());
        assertArrayEquals(new Throwable[]{poisonCleanup}, business.getSuppressed());
        assertTrue(parked.isCompleted(),
                "channel cleanup failure must not bypass task interrupt and join");
    }

    @Test
    void teardownFailureFromEarlierTaskCannotReplaceFrozenBusinessPrimary() {
        RuntimeException teardownFailure = new RuntimeException("earlier task teardown failed");
        RuntimeException business = new RuntimeException("later task business failed");
        CountDownLatch earlierStarted = new CountDownLatch(1);
        Task earlier = new Task("earlier", () -> {
            earlierStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw teardownFailure;
            }
        }, cause -> { });
        Task later = new Task("later", () -> {
            try {
                assertTrue(earlierStarted.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            throw business;
        }, cause -> { });

        RuntimeException observed = assertThrows(RuntimeException.class, () ->
                ConcurrentStages.runFailFast(Arrays.asList(earlier, later),
                        Arrays.asList("earlier", "later"), Collections.emptyList()));

        assertSame(business, observed.getCause());
        assertArrayEquals(new Throwable[]{teardownFailure}, business.getSuppressed());
        assertTrue(earlier.isCompleted());
    }

    @Test
    void ownerInterruptCancelsAndJoinsEveryStartedTaskBeforeReturning() throws Exception {
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch neverReleased = new CountDownLatch(1);
        RuntimeException firstFailure = new RuntimeException("first child interrupted");
        RuntimeException secondFailure = new RuntimeException("second child interrupted");
        java.util.function.Function<RuntimeException, Runnable> blocking = failure -> () -> {
            started.countDown();
            try {
                neverReleased.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw failure;
            }
        };
        Task first = new Task("first", blocking.apply(firstFailure), cause -> { });
        Task second = new Task("second", blocking.apply(secondFailure), cause -> { });
        List<Task> tasks = Arrays.asList(first, second);
        AtomicBoolean allJoinedAtReturn = new AtomicBoolean();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread owner = new Thread(() -> {
            try {
                ConcurrentStages.runFailFast(
                        tasks, Arrays.asList("first", "second"), java.util.Collections.emptyList());
            } catch (Throwable failure) {
                observed.set(failure);
            } finally {
                allJoinedAtReturn.set(first.isCompleted() && second.isCompleted());
            }
        }, "concurrent-stages-owner");
        try {
            owner.start();
            assertTrue(started.await(10, TimeUnit.SECONDS));
            owner.interrupt();
            owner.join(10_000);

            assertFalse(owner.isAlive());
            assertTrue(allJoinedAtReturn.get(),
                    "interrupted owner must not leave detached stage threads behind");
            assertNotNull(observed.get());
            assertArrayEquals(new Throwable[]{firstFailure, secondFailure},
                    observed.get().getSuppressed(),
                    "child failures must retain stable task order and exact identity");
            assertTrue(owner.isInterrupted(), "owner interrupt status must be restored");
        } finally {
            first.interrupt();
            second.interrupt();
            neverReleased.countDown();
            owner.join(10_000);
        }
    }
}
