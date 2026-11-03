package io.rowweave.sched.net;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.Channel;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Physical connection reuse for a worker pair, consumer credit and head-of-line isolation between targets. */
class MuxConnectionPoolTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow row = new DataRow(1);
        row.set(0, v);
        return row;
    }

    @Test
    void reportsSanitizedPhysicalConnectionReuseAndThroughput() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(MuxConnectionPool.class);
        Level previous = logger.getLevel();
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        logger.setLevel(Level.INFO);
        events.start();
        logger.addAppender(events);
        UUID firstTarget = new UUID(190L, 1L);
        UUID secondTarget = new UUID(190L, 2L);
        DataInboundRegistry registry = new DataInboundRegistry();
        registry.register(firstTarget, "job", new MultiProducerChannel(8, 1, 4096), META);
        registry.register(secondTarget, "job", new MultiProducerChannel(8, 1, 4096), META);
        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            Channel first = pool.acquire(
                    META, "127.0.0.1", server.port(), firstTarget, 4096, 0, 0);
            Channel second = pool.acquire(
                    META, "127.0.0.1", server.port(), secondTarget, 4096, 0, 0);
            first.send(StreamEvent.record(row(1)));
            second.send(StreamEvent.record(row(2)));
            first.close();
            second.close();

            List<String> messages = new ArrayList<>();
            for (ILoggingEvent event : events.list) {
                messages.add(event.getFormattedMessage());
            }
            String joined = String.join("\n", messages);
            assertTrue(joined.contains("ROWWEAVE_SHUFFLE_CONNECTION_OPEN"), joined);
            assertTrue(joined.contains("ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME"), joined);
            assertTrue(joined.contains("protocol=LOOPBACK_PLAIN_V0"), joined);
            assertTrue(joined.contains("logicalStreams=2"), joined);
            assertTrue(joined.contains("records=2"), joined);
            assertTrue(joined.contains("wireBytes="), joined);
            assertTrue(joined.contains("connectNanos="), joined);
            assertTrue(joined.contains("activeDataNanos="), joined);
            assertFalse(joined.contains(firstTarget.toString()), joined);
            assertFalse(joined.contains(secondTarget.toString()), joined);
        } finally {
            logger.detachAppender(events);
            logger.setLevel(previous);
            events.stop();
        }
    }

    @Test
    void reusesOnePhysicalConnectionForTargetsAndLogicalProducers() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        List<UUID> targets = new ArrayList<>();
        List<MultiProducerChannel> inbound = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            UUID target = new UUID(90L, i + 1L);
            MultiProducerChannel channel = new MultiProducerChannel(64, 2, 4096);
            registry.register(target, "job", channel, META, 2, 4096);
            targets.add(target);
            inbound.add(channel);
        }

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            List<Channel> handles = new ArrayList<>();
            for (int producer = 0; producer < 2; producer++) {
                for (int target = 0; target < 4; target++) {
                    Channel handle = pool.acquire(
                            META, "127.0.0.1", server.port(), targets.get(target), 4096, 0, 0);
                    handle.send(StreamEvent.record(row(producer * 10 + target)));
                    handles.add(handle);
                }
            }

            assertEquals(1, pool.connectionCount(), "the same worker pair keeps exactly one physical connection");
            assertEquals(1, server.acceptedConnectionCount(), "the server accepts one connection only");
            handles.forEach(Channel::close);

            for (int target = 0; target < 4; target++) {
                assertEquals(target, inbound.get(target).poll().row().get(0));
                assertEquals(10 + target, inbound.get(target).poll().row().get(0));
                assertNull(inbound.get(target).poll(), "the target ends only after both logical producers send EOS");
            }
            assertEquals(0, pool.connectionCount(), "the physical connection is released once the last handle closes");
        }
    }

    @Test
    void senderWaitsUntilConsumerReturnsExactWireCredit() throws Exception {
        UUID target = new UUID(90L, 100L);
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel inbound = new MultiProducerChannel(8, 1, 1);
        registry.register(target, "job", inbound, META, 1, 1);
        ExecutorService sender = Executors.newSingleThreadExecutor();

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            Channel handle = pool.acquire(META, "127.0.0.1", server.port(), target, 1, 0, 0);
            handle.send(StreamEvent.record(row(1)));  // a single row may overshoot the window
            Future<?> second = sender.submit(() -> handle.send(StreamEvent.record(row(2))));

            Thread.sleep(100L);
            assertFalse(second.isDone(), "no credit returns early while the consumer has not polled the first row");
            assertEquals(1, inbound.poll().row().get(0));
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> second.get());
            assertEquals(2, inbound.poll().row().get(0));
            handle.close();
            assertNull(inbound.poll());
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void blockedTargetDoesNotHeadOfLineBlockSiblingTarget() throws Exception {
        UUID slowTarget = new UUID(90L, 200L);
        UUID fastTarget = new UUID(90L, 201L);
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel slow = new MultiProducerChannel(1, 1, 4096);
        MultiProducerChannel fast = new MultiProducerChannel(8, 1, 4096);
        registry.register(slowTarget, "job", slow, META, 1, 4096);
        registry.register(fastTarget, "job", fast, META, 1, 4096);

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            Channel slowHandle = pool.acquire(META, "127.0.0.1", server.port(), slowTarget, 4096, 0, 0);
            Channel fastHandle = pool.acquire(META, "127.0.0.1", server.port(), fastTarget, 4096, 0, 0);
            slowHandle.send(StreamEvent.record(row(1)));
            slowHandle.send(StreamEvent.record(row(2)));  // the target ingress blocks on the slow channel of capacity 1
            fastHandle.send(StreamEvent.record(row(99)));
            fastHandle.close();

            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                assertEquals(99, fast.poll().row().get(0));
                assertNull(fast.poll(), "the sibling target completes on its own");
            });

            assertEquals(1, slow.poll().row().get(0));
            assertEquals(2, slow.poll().row().get(0));
            slowHandle.close();
            assertNull(slow.poll());
        }
    }

    @Test
    void openBeforeRegistrationKeepsRecordParkedUntilTargetAppears() throws Exception {
        UUID target = new UUID(90L, 300L);
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel inbound = new MultiProducerChannel(8, 1, 64);
        ExecutorService sender = Executors.newSingleThreadExecutor();

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            Channel handle = pool.acquire(META, "127.0.0.1", server.port(), target, 64, 0, 0);
            Future<?> send = sender.submit(() -> handle.send(StreamEvent.record(row(7))));
            Thread.sleep(100L);
            assertFalse(send.isDone(), "an unregistered target stays in OPEN and its RECORD stops at zero credit");

            registry.register(target, "late-job", inbound, META, 1, 64);
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> send.get());
            assertEquals(7, inbound.poll().row().get(0));
            handle.close();
            assertNull(inbound.poll());
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void logicalTargetFailureDoesNotPoisonSiblingOnSharedConnection() throws Exception {
        UUID failedTarget = new UUID(90L, 400L);
        UUID healthyTarget = new UUID(90L, 401L);
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel failed = new MultiProducerChannel(8, 1, 256);
        MultiProducerChannel healthy = new MultiProducerChannel(8, 1, 256);
        registry.register(failedTarget, "job", failed, META, 1, 256);
        registry.register(healthyTarget, "job", healthy, META, 1, 256);

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            Channel failedHandle = pool.acquire(
                    META, "127.0.0.1", server.port(), failedTarget, 256, 0, 0);
            Channel healthyHandle = pool.acquire(
                    META, "127.0.0.1", server.port(), healthyTarget, 256, 0, 0);
            failedHandle.fail(new IllegalStateException("target-only"));
            assertTrue(failed.poll().isError(), "a failed target fails loudly");
            assertEquals(1, pool.connectionCount(), "the physical connection stays while a sibling target is active");

            healthyHandle.send(StreamEvent.record(row(88)));
            healthyHandle.close();
            assertEquals(88, healthy.poll().row().get(0));
            assertNull(healthy.poll(), "the sibling target is untouched by a logical failure");
        }
    }

    @Test
    void concurrentLogicalProducersKeepOpenRecordEosWireOrder() throws Exception {
        UUID target = new UUID(90L, 500L);
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel inbound = new MultiProducerChannel(4096, 4, 1 << 20);
        registry.register(target, "job", inbound, META, 4, 1 << 20);
        ExecutorService producers = Executors.newFixedThreadPool(4);

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int producer = 0; producer < 4; producer++) {
                final int producerId = producer;
                futures.add(producers.submit(() -> {
                    Channel handle = pool.acquire(
                            META, "127.0.0.1", server.port(), target, 1 << 20, 0, 0);
                    for (int row = 0; row < 500; row++) {
                        handle.send(StreamEvent.record(row(producerId * 1000 + row)));
                    }
                    handle.close();
                }));
            }
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            int rows = 0;
            while (inbound.poll() != null) {
                rows++;
            }
            assertEquals(2000, rows, "concurrent OPEN, RECORD and EOS are neither reordered nor dropped on the shared wire");
            assertEquals(1, server.acceptedConnectionCount());
        } finally {
            producers.shutdownNow();
        }
    }

    @Test
    void repeatedWideTargetChurnKeepsFramesAndCreditsAlignedAcrossConnectionGenerations()
            throws Exception {
        int rounds = 10;
        int targetCount = 8;
        int producersPerTarget = 4;
        int rowsPerProducer = 16;
        DataInboundRegistry registry = new DataInboundRegistry();
        ExecutorService producers = Executors.newFixedThreadPool(targetCount * producersPerTarget);

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            for (int round = 0; round < rounds; round++) {
                String jobId = "mux-churn-" + round;
                List<UUID> targets = new ArrayList<>();
                List<MultiProducerChannel> inbound = new ArrayList<>();
                for (int target = 0; target < targetCount; target++) {
                    UUID targetId = new UUID(132L + round, target + 1L);
                    MultiProducerChannel channel = new MultiProducerChannel(
                            producersPerTarget * rowsPerProducer + 1,
                            producersPerTarget, 1 << 20);
                    registry.register(targetId, jobId, channel, META,
                            producersPerTarget, 1 << 20);
                    targets.add(targetId);
                    inbound.add(channel);
                }

                CountDownLatch ready = new CountDownLatch(targetCount * producersPerTarget);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int target = 0; target < targetCount; target++) {
                    for (int producer = 0; producer < producersPerTarget; producer++) {
                        final int targetIndex = target;
                        final int producerIndex = producer;
                        final int roundIndex = round;
                        futures.add(producers.submit(() -> {
                            ready.countDown();
                            try {
                                start.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(interrupted);
                            }
                            Channel handle = pool.acquire(META, "127.0.0.1", server.port(),
                                    targets.get(targetIndex), 1 << 20, 0, 0);
                            for (int row = 0; row < rowsPerProducer; row++) {
                                handle.send(StreamEvent.record(row(
                                        roundIndex * 1_000_000
                                                + targetIndex * 10_000
                                                + producerIndex * 100
                                                + row)));
                            }
                            handle.close();
                        }));
                    }
                }
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
                assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                    for (MultiProducerChannel channel : inbound) {
                        int rows = 0;
                        while (channel.poll() != null) {
                            rows++;
                        }
                        assertEquals(producersPerTarget * rowsPerProducer, rows,
                                "swapping the connection generation across targets and producers loses no frame and mixes up none");
                    }
                });
                registry.unregisterJob(jobId);
            }
            assertEquals(0, pool.connectionCount());
            assertEquals(0, registry.entryCount());
            assertTrue(server.acceptedConnectionCount() >= 1);
        } finally {
            producers.shutdownNow();
        }
    }

    @Test
    void poolClosePoisonsTargetsAndWakesSendersWaitingForCredit() throws Exception {
        UUID target = new UUID(90L, 600L);
        DataInboundRegistry registry = new DataInboundRegistry();
        ExecutorService sender = Executors.newSingleThreadExecutor();
        MuxConnectionPool pool = new MuxConnectionPool();
        try (WorkerDataServer server = new WorkerDataServer(registry, 5000)) {
            Channel handle = pool.acquire(META, "127.0.0.1", server.port(), target, 64, 0, 0);
            Future<?> blocked = sender.submit(() -> handle.send(StreamEvent.record(row(1))));
            Thread.sleep(100L);
            assertFalse(blocked.isDone());

            pool.close();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> blocked.get(2, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof io.rowweave.sched.ChannelPoisonedException,
                    "pool teardown wakes a sender at zero credit instead of parking it forever");
        } finally {
            pool.close();
            sender.shutdownNow();
        }
    }

/** OPEN guarantees remote ordering at zero credit, so acquire must not wait synchronously on an EventLoop flush. */
    @Test
    void acquireDoesNotBlockStageThreadWhenOutboundEventLoopIsBusy() throws Exception {
        UUID firstTarget = new UUID(91L, 1L);
        UUID secondTarget = new UUID(91L, 2L);
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel firstInbound = new MultiProducerChannel(8, 1, 256);
        MultiProducerChannel secondInbound = new MultiProducerChannel(8, 1, 256);
        registry.register(firstTarget, "job", firstInbound, META, 1, 256);
        registry.register(secondTarget, "job", secondInbound, META, 1, 256);
        NioEventLoopGroup outbound = new NioEventLoopGroup(1,
                new NamedDaemonThreadFactory("rowweave-mux-open-test"));
        CountDownLatch ioBlocked = new CountDownLatch(1);
        CountDownLatch releaseIo = new CountDownLatch(1);

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             MuxConnectionPool pool = new MuxConnectionPool(outbound)) {
            Channel first = pool.acquire(META, "127.0.0.1", server.port(), firstTarget, 256, 0, 0);
            outbound.next().execute(() -> {
                ioBlocked.countDown();
                try {
                    releaseIo.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(ioBlocked.await(1, TimeUnit.SECONDS));

            Channel second = null;
            try {
                second = assertTimeoutPreemptively(Duration.ofMillis(250), () ->
                        pool.acquire(META, "127.0.0.1", server.port(), secondTarget, 256, 0, 0));
            } finally {
                releaseIo.countDown();
            }
            second.send(StreamEvent.record(row(2)));
            second.close();
            assertEquals(2, secondInbound.poll().row().get(0));
            assertNull(secondInbound.poll());
            first.close();
            assertNull(firstInbound.poll());
        } finally {
            releaseIo.countDown();
        }
    }
}
