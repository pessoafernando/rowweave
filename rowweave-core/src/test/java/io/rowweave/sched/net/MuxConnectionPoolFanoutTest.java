package io.rowweave.sched.net;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataRow;
import io.rowweave.DataSchema;
import io.rowweave.sched.Channel;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * High-fanout multiplexing: one physical worker-pair connection carries four targets times
 * two logical producers, which is eight logical streams.
 *
 * <p>Three things are pinned down. Multiplexing is measured, not inferred from the absence
 * of errors, so the physical connection lifetime records logicalStreams=8. Wire credit is
 * returned per target: draining one target's consumer wakes only that target's sender,
 * leaving sibling targets parked, and in the end no row is missing, which is what
 * {@code wireCreditIsReturnedToItsOwnTargetOnly} shows, so a credit leak or a credit that
 * crosses targets fails here. On the ordinary mux path the physical connection is released
 * by reference counting after the last logical handle closes, with no outstanding-credit
 * zero gate, since that gate belongs only to the retiring path of an authenticated cohort,
 * which no test covers. And losing the physical connection is one event that poisons every
 * logical stream, waking each parked sender so none stays parked forever on zero credit.
 */
class MuxConnectionPoolFanoutTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));
    private static final int TARGETS = 4;
    private static final int PRODUCERS_PER_TARGET = 2;
    private static final int ROWS_PER_PRODUCER = 5;
    private static final long WINDOW_BYTES = 1L << 20;

    private static DataRow row(int v) {
        DataRow row = new DataRow(1);
        row.set(0, v);
        return row;
    }

    private static List<String> appendLogs(ListAppender<ILoggingEvent> events) {
        List<String> messages = new ArrayList<>();
        for (ILoggingEvent event : events.list) {
            messages.add(event.getFormattedMessage());
        }
        return messages;
    }

    @Test
    void wideFanoutSharesOnePhysicalConnectionAndCountsEveryLogicalStream() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(MuxConnectionPool.class);
        Level previous = logger.getLevel();
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        logger.setLevel(Level.INFO);
        events.start();
        logger.addAppender(events);

        DataInboundRegistry registry = new DataInboundRegistry();
        List<UUID> targetIds = new ArrayList<>();
        List<MultiProducerChannel> inbound = new ArrayList<>();
        for (int t = 0; t < TARGETS; t++) {
            UUID targetId = new UUID(240L, t + 1L);
            MultiProducerChannel channel = new MultiProducerChannel(
                    PRODUCERS_PER_TARGET * ROWS_PER_PRODUCER + 2,
                    PRODUCERS_PER_TARGET, WINDOW_BYTES);
            registry.register(targetId, "mux-fanout", channel, META,
                    PRODUCERS_PER_TARGET, WINDOW_BYTES);
            targetIds.add(targetId);
            inbound.add(channel);
        }

        try (WorkerDataServer server = new WorkerDataServer(registry, 10_000L);
             MuxConnectionPool pool = new MuxConnectionPool()) {
            List<Channel> handles = new ArrayList<>();
            for (int producer = 0; producer < PRODUCERS_PER_TARGET; producer++) {
                for (int t = 0; t < TARGETS; t++) {
                    Channel handle = pool.acquire(META, "127.0.0.1", server.port(),
                            targetIds.get(t), (int) WINDOW_BYTES, 0, 0);
                    for (int r = 0; r < ROWS_PER_PRODUCER; r++) {
                        handle.send(StreamEvent.record(row(producer * 100 + t * 10 + r)));
                    }
                    handles.add(handle);
                }
            }
            assertEquals(1, pool.connectionCount(),
                    TARGETS + " targets x " + PRODUCERS_PER_TARGET + " producers must share one physical connection");
            assertEquals(1, server.acceptedConnectionCount(), "the server accepts exactly one physical connection");
            for (Channel handle : handles) {
                handle.close();
            }

            assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
                for (int t = 0; t < TARGETS; t++) {
                    MultiProducerChannel channel = inbound.get(t);
                    int received = 0;
                    while (channel.poll() != null) {
                        received++;
                    }
                    assertEquals(PRODUCERS_PER_TARGET * ROWS_PER_PRODUCER, received,
                            "target " + t + " must collect every row from both producers, no more and no fewer");
                }
            });
            assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                    assertEquals(0, pool.connectionCount(),
                            "the physical connection is dropped once the last logical handle is released, which is the reference-count gate"));

            List<String> messages = assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
                for (int attempt = 0; attempt < 200; attempt++) {
                    String joined = String.join("\n", appendLogs(events));
                    if (joined.contains("ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME")) {
                        return appendLogs(events);
                    }
                    TimeUnit.MILLISECONDS.sleep(50L);
                }
                return appendLogs(events);
            });
            String joined = String.join("\n", messages);
            assertTrue(joined.contains("ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME"), joined);
            assertTrue(joined.contains("logicalStreams="
                            + (TARGETS * PRODUCERS_PER_TARGET)),
                    "the reuse has to be measured rather than inferred from the connection count: " + joined);
            assertTrue(joined.contains("records="
                            + (TARGETS * PRODUCERS_PER_TARGET * ROWS_PER_PRODUCER)),
                    "every logical stream on the same physical connection counts toward the total: " + joined);
        } finally {
            logger.detachAppender(events);
            logger.setLevel(previous);
            events.stop();
        }
    }

    @Test
    void physicalConnectionLossPoisonsEveryLogicalStreamAndWakesEveryParkedSender() throws Exception {
        // None of the four targets is registered, so after OPEN the receiver grants no credit at all and every sender must park.
        // MuxConnectionPoolTest.openBeforeRegistrationKeepsRecordParkedUntilTargetAppears already pins the parking itself.
        // What this case shows is different: when the same physical connection disappears, those parked logical streams must all be poisoned and woken at once.
        DataInboundRegistry registry = new DataInboundRegistry();
        List<UUID> targetIds = new ArrayList<>();
        for (int t = 0; t < TARGETS; t++) {
            targetIds.add(new UUID(241L, t + 1L));
        }

        MuxConnectionPool pool = new MuxConnectionPool();
        ExecutorService senders = Executors.newFixedThreadPool(TARGETS);
        try (WorkerDataServer server = new WorkerDataServer(registry, 10_000L)) {
            List<Channel> handles = new ArrayList<>();
            List<Future<?>> parked = new ArrayList<>();
            for (int t = 0; t < TARGETS; t++) {
                Channel handle = pool.acquire(META, "127.0.0.1", server.port(),
                        targetIds.get(t), 64, 0, 0);
                handles.add(handle);
                final int index = t;
                parked.add(senders.submit(
                        () -> handle.send(StreamEvent.record(row(index)))));
            }
            assertEquals(1, pool.connectionCount());
            Thread.sleep(200L);
            for (Future<?> sender : parked) {
                assertFalse(sender.isDone(), "with no credit available the sender is parked, which is the premise of the test");
            }

            server.close();

            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                for (Future<?> sender : parked) {
                    ExecutionException failure = assertThrows(ExecutionException.class,
                            () -> sender.get(10, TimeUnit.SECONDS));
                    assertTrue(failure.getCause() instanceof ChannelPoisonedException,
                            "losing one physical connection poisons every logical stream it carried and wakes the sender, observed: "
                                    + failure.getCause());
                }
            });
            for (int t = 0; t < TARGETS; t++) {
                final Channel handle = handles.get(t);
                assertThrows(ChannelPoisonedException.class,
                        () -> handle.send(StreamEvent.record(row(999))),
                        "poisoning is terminal, so writes to target " + t + " must not succeed silently afterwards");
            }
            // Cleanup ceiling: once the failure is delivered, every logical handle releases normally, so a stage thread's finally is not stuck on a terminal handle,
            // and the physical connection disappears once the last handle is released.
            for (Channel handle : handles) {
                handle.close();
            }
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    assertEquals(0, pool.connectionCount(),
                            "the physical connection is dropped once every logical handle is released"));
        } finally {
            pool.close();
            senders.shutdownNow();
        }
    }

    @Test
    void wireCreditIsReturnedToItsOwnTargetOnly() throws Exception {
        // Four targets share one physical connection with one producer each, and the window is only 64 bytes while 40 rows far exceed it,
        // so every sender must park and can only wake on a CREDIT frame returned by that same target's consumer after draining.
        // Credit recorded against the wrong target, crossing into a sibling stream or leaking would fail the last assertion.
        final int rows = 40;
        final int windowBytes = 64;
        DataInboundRegistry registry = new DataInboundRegistry();
        List<UUID> targetIds = new ArrayList<>();
        List<MultiProducerChannel> inbound = new ArrayList<>();
        for (int t = 0; t < TARGETS; t++) {
            UUID targetId = new UUID(242L, t + 1L);
            MultiProducerChannel channel =
                    new MultiProducerChannel(rows + 2, 1, windowBytes);
            registry.register(targetId, "mux-credit", channel, META, 1, windowBytes);
            targetIds.add(targetId);
            inbound.add(channel);
        }

        MuxConnectionPool pool = new MuxConnectionPool();
        ExecutorService senders = Executors.newFixedThreadPool(TARGETS);
        try (WorkerDataServer server = new WorkerDataServer(registry, 10_000L)) {
            List<Channel> handles = new ArrayList<>();
            List<Future<?>> writers = new ArrayList<>();
            for (int t = 0; t < TARGETS; t++) {
                final int index = t;
                Channel handle = pool.acquire(META, "127.0.0.1", server.port(),
                        targetIds.get(t), windowBytes, 0, 0);
                handles.add(handle);
                writers.add(senders.submit(() -> {
                    for (int r = 0; r < rows; r++) {
                        handle.send(StreamEvent.record(row(index * 1000 + r)));
                    }
                    return null;
                }));
            }
            assertEquals(1, pool.connectionCount(),
                    "credit accounting also keeps a single physical connection carrying " + TARGETS + " targets");

            Thread.sleep(200L);
            for (Future<?> writer : writers) {
                assertFalse(writer.isDone(),
                        "a window of " + windowBytes + " bytes cannot hold " + rows + " rows so the sender is parked, which is the premise of the test");
            }

            int drained = drain(inbound.get(0), rows);
            assertEquals(rows, drained, "target 0 collects all of its own rows");
            assertTimeoutPreemptively(Duration.ofSeconds(20),
                    () -> writers.get(0).get(20, TimeUnit.SECONDS));
            for (int t = 1; t < TARGETS; t++) {
                assertFalse(writers.get(t).isDone(),
                        "returning credit to target 0 must not wake the sender of sibling target " + t + " sender");
            }

            for (int t = 1; t < TARGETS; t++) {
                assertEquals(rows, drain(inbound.get(t), rows),
                        "target " + t + " must collect all of its own rows once drained");
            }
            assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
                for (Future<?> writer : writers) {
                    writer.get(20, TimeUnit.SECONDS);
                }
            });

            for (Channel handle : handles) {
                handle.close();
            }
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    assertEquals(0, pool.connectionCount(),
                            "the physical connection is dropped once every logical handle is released"));
        } finally {
            pool.close();
            senders.shutdownNow();
        }
    }

/** Bounded drain of one target's inbound channel, returning the row count and failing on a timeout instead of silently receiving fewer rows. */
    private static int drain(MultiProducerChannel channel, int expected) throws Exception {
        int received = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        while (received < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("only " + received + " of " + expected
                        + " rows arrived before the bounded drain window");
            }
            if (channel.poll() != null) {
                received++;
            } else {
                TimeUnit.MILLISECONDS.sleep(1L);
            }
        }
        return received;
    }
}
