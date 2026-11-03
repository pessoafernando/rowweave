package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryMasterLocatorSubscriptionTest {

    @Test
    void existingPermanentListenerMethodKeepsItsBinaryDescriptor() throws Exception {
        Method method = RegistryMasterLocator.class.getMethod(
                "addLeaderChangeListener", Runnable.class);
        assertEquals(void.class, method.getReturnType());
    }

    @Test
    void subscriptionCloseIsIdempotentAndStopsFutureCallbacks() {
        InProcessRegistry registry = new InProcessRegistry();
        RegistryMasterLocator locator = new RegistryMasterLocator(registry);
        AtomicInteger callbacks = new AtomicInteger();

        RegistryMasterLocator.LeaderChangeSubscription subscription =
                locator.subscribeLeaderChanges(callbacks::incrementAndGet);
        fireLeaderChange(registry, 19001);
        assertEquals(1, callbacks.get());

        subscription.close();
        subscription.close();
        fireLeaderChange(registry, 19002);
        assertEquals(1, callbacks.get());
    }

    @Test
    void failedInitialWatchLeavesNoListenerAndCanRetry() {
        FailFirstWatchRegistry registry = new FailFirstWatchRegistry();
        RegistryMasterLocator locator = new RegistryMasterLocator(registry);
        AtomicInteger failedListener = new AtomicInteger();
        RuntimeException watchFailure = assertThrows(RuntimeException.class,
                () -> locator.subscribeLeaderChanges(failedListener::incrementAndGet));
        assertEquals("watch-failure", watchFailure.getMessage());

        AtomicInteger activeListener = new AtomicInteger();
        RegistryMasterLocator.LeaderChangeSubscription subscription =
                locator.subscribeLeaderChanges(activeListener::incrementAndGet);
        fireLeaderChange(registry.delegate, 19003);

        assertEquals(0, failedListener.get());
        assertEquals(1, activeListener.get());
        assertEquals(2, registry.watchAttempts.get());
        subscription.close();
    }

    @Test
    @Timeout(10)
    void closeWaitsForEnteredCallbackThenLinearizesBeforeFutureEvents()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RegistryMasterLocator locator = new RegistryMasterLocator(registry);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        RegistryMasterLocator.LeaderChangeSubscription subscription =
                locator.subscribeLeaderChanges(() -> {
                    callbacks.incrementAndGet();
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> event = executor.submit(() -> fireLeaderChange(registry, 19004));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<?> close = executor.submit(subscription::close);
            Thread.sleep(50L);
            assertFalse(close.isDone(), "close returned while callback still owned the subscription");

            release.countDown();
            event.get(2, TimeUnit.SECONDS);
            close.get(2, TimeUnit.SECONDS);
            fireLeaderChange(registry, 19005);
            assertEquals(1, callbacks.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private static void fireLeaderChange(ServiceRegistry registry, int port) {
        registry.register(new RegistryWorker(
                MasterServer.MASTER_LEADER_ROLE_ID, "127.0.0.1", port, 0));
    }

    private static final class FailFirstWatchRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final AtomicInteger watchAttempts = new AtomicInteger();

        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }

        @Override
        public void watch(WatchListener listener) {
            if (watchAttempts.incrementAndGet() == 1) {
                throw new RuntimeException("watch-failure");
            }
            delegate.watch(listener);
        }
    }
}
