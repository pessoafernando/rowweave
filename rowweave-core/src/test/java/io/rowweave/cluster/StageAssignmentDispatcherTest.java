package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StageAssignmentDispatcherTest {

    @Test
    void invokesConcurrentlyButReturnsInputOrder() throws Exception {
        CountDownLatch entered = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        List<Callable<Integer>> calls = Arrays.asList(3, 2, 1, 0).stream()
                .<Callable<Integer>>map(value -> () -> {
                    int now = active.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    entered.countDown();
                    assertTrue(release.await(2, TimeUnit.SECONDS));
                    active.decrementAndGet();
                    return value;
                }).collect(java.util.stream.Collectors.toList());

        try (StageAssignmentDispatcher dispatcher = new StageAssignmentDispatcher(4)) {
            java.util.concurrent.CompletableFuture<List<Integer>> result =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> dispatcher.invokeAll(calls));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(Arrays.asList(3, 2, 1, 0), result.get(1, TimeUnit.SECONDS));
            assertEquals(4, peak.get());
        }
    }

    @Test
    void failureWaitsForAllTasksAndKeepsDeterministicPrimaryCause() {
        CountDownLatch slowFinished = new CountDownLatch(1);
        IllegalStateException first = new IllegalStateException("first-by-input-order");
        IllegalArgumentException second = new IllegalArgumentException("second");
        List<Callable<Integer>> calls = Arrays.asList(
                () -> { throw first; },
                () -> {
                    try { Thread.sleep(80L); } finally { slowFinished.countDown(); }
                    return 1;
                },
                () -> { throw second; });

        try (StageAssignmentDispatcher dispatcher = new StageAssignmentDispatcher(3)) {
            StageAssignmentDispatcher.DispatchException failure = assertThrows(
                    StageAssignmentDispatcher.DispatchException.class,
                    () -> dispatcher.invokeAll(calls));
            assertSame(first, failure.getCause());
            assertEquals(1, failure.getSuppressed().length);
            assertSame(second, failure.getSuppressed()[0]);
            assertEquals(0L, slowFinished.getCount(), "every in-flight task has really exited before the throw");
        }
    }

    @Test
    void interruptDoesNotHangOnCancelledBeforeRunTask() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        StageAssignmentDispatcher dispatcher = new StageAssignmentDispatcher(1);
        Thread caller = new Thread(() -> assertThrows(
                StageAssignmentDispatcher.DispatchException.class,
                () -> dispatcher.invokeAll(Arrays.asList(
                        () -> {
                            firstEntered.countDown();
                            while (releaseFirst.getCount() > 0) {
                                try {
                                    releaseFirst.await();
                                } catch (InterruptedException ignored) {
                                }
                            }
                            return 1;
                        },
                        () -> 2))), "interrupted-dispatch-caller");
        caller.setDaemon(true);
        try {
            caller.start();
            assertTrue(firstEntered.await(1, TimeUnit.SECONDS));
            caller.interrupt();
            Thread.sleep(100L);
            releaseFirst.countDown();
            caller.join(1500L);
            assertFalse(caller.isAlive(), "cancelling a task that never started still releases the structured-join completion");
        } finally {
            releaseFirst.countDown();
            dispatcher.close();
        }
    }
}

