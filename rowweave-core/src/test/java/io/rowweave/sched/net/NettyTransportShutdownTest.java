package io.rowweave.sched.net;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Closing the Netty data plane finishes the pipeline and the executor teardown before it returns. */
class NettyTransportShutdownTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final String NETTY_CONTEXT_LOGGER = "io.netty.channel.AbstractChannelHandlerContext";

    @Test
    void receiverFailClose_doesNotSubmitPipelineEventsToTerminatedExecutor() throws Exception {
        Set<Long> preExistingTransportThreads = transportThreadIds();
        Logger logger = (Logger) LoggerFactory.getLogger(NETTY_CONTEXT_LOGGER);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            // Several rounds amplify the deterministic race: source.fail closes the channel asynchronously and the handler executor is stopped right after,
            // so the I/O EventLoop is rejected when it then dispatches channelInactive and channelUnregistered.
            for (int i = 0; i < 3; i++) {
                SocketChannelSource source = new SocketChannelSource(META, 1);
                SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", source.port(), 1);
                DataRow row = new DataRow(1);
                row.set(0, i);
                List<Throwable> failures = new CopyOnWriteArrayList<>();
                Thread producer = new Thread(() -> {
                    try {
                        while (true) {
                            sink.send(StreamEvent.record(row));
                        }
                    } catch (Throwable expectedAfterReceiverFail) {
                        failures.add(expectedAfterReceiverFail);
                    }
                }, "netty-shutdown-test-producer");
                producer.start();
                Thread.sleep(10L);
                source.fail(new RuntimeException("injected downstream failure"));
                producer.join(TimeUnit.SECONDS.toMillis(5));
                assertFalse(producer.isAlive(), "sender close must complete");
                assertFalse(failures.isEmpty(), "receiver fail must unpark and poison sender");
                sink.fail(new RuntimeException("test cleanup"));
                source.close();
            }

            awaitCondition(() -> hasExecutorTerminatedWarning(appender.list)
                            || transportThreadsExcept(preExistingTransportThreads).isEmpty(),
                    TimeUnit.SECONDS.toMillis(2));

            List<String> rejected = appender.list.stream()
                    .filter(NettyTransportShutdownTest::isExecutorTerminatedWarning)
                    .map(e -> e.getFormattedMessage() + "\n" + String.valueOf(e.getThrowableProxy()))
                    .collect(Collectors.toList());
            assertTrue(rejected.isEmpty(),
                    "no pipeline event is dispatched to a terminated executor after close: " + rejected);
            assertTrue(transportThreadsExcept(preExistingTransportThreads).isEmpty(),
                    "the transport executors this test created have terminated when close returns, still alive: "
                            + transportThreadsExcept(preExistingTransportThreads));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

/** When stage stop and the pipeline finally close or fail at the same time, teardown runs once and the other callers wait for that same result. */
    @Test
    void concurrentCloseAndFail_areIdempotentAndDoNotDeadlock() throws Exception {
        Set<Long> preExistingTransportThreads = transportThreadIds();
        SocketChannelSource source = new SocketChannelSource(META, 8);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", source.port(), 8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> unexpected = new AtomicReference<>();
        Thread sourceClose = new Thread(() -> runAfter(start, source::close, unexpected), "source-close-race");
        Thread sourceFail = new Thread(() -> runAfter(start,
                () -> source.fail(new RuntimeException("injected failure")), unexpected), "source-fail-race");
        Thread sinkClose = new Thread(() -> runAfter(start, sink::close, unexpected), "sink-close-race");
        Thread sinkFail = new Thread(() -> runAfter(start,
                () -> sink.fail(new RuntimeException("injected failure")), unexpected), "sink-fail-race");
        Thread[] racers = {sourceClose, sourceFail, sinkClose, sinkFail};
        for (Thread racer : racers) {
            racer.start();
        }
        start.countDown();
        for (Thread racer : racers) {
            racer.join(5000L);
        }

        assertFalse(anyAlive(racers), "concurrent close and fail do not stall on a repeated executor shutdown");
        assertTrue(unexpected.get() == null, "concurrent teardown throws nothing: " + unexpected.get());
        assertTrue(transportThreadsExcept(preExistingTransportThreads).isEmpty(),
                "concurrent teardown leaves no transport thread from this test behind: "
                        + transportThreadsExcept(preExistingTransportThreads));
    }

    private static void runAfter(CountDownLatch start, Runnable action, AtomicReference<Throwable> unexpected) {
        try {
            start.await();
            action.run();
        } catch (Throwable t) {
            unexpected.compareAndSet(null, t);
        }
    }

    private static boolean anyAlive(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread.isAlive()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasExecutorTerminatedWarning(List<ILoggingEvent> events) {
        return events.stream().anyMatch(NettyTransportShutdownTest::isExecutorTerminatedWarning);
    }

    private static boolean isExecutorTerminatedWarning(ILoggingEvent event) {
        if (!event.getLevel().isGreaterOrEqual(Level.WARN)) {
            return false;
        }
        String rendered = event.getFormattedMessage() + " " + String.valueOf(event.getThrowableProxy());
        return rendered.contains("event executor terminated")
                || rendered.contains("Failed to submit an exceptionCaught() event");
    }

    private static Set<Long> transportThreadIds() {
        Set<Long> ids = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && isTransportThread(thread.getName())) {
                ids.add(thread.getId());
            }
        }
        return ids;
    }

    private static List<String> transportThreadsExcept(Set<Long> excludedIds) {
        List<String> names = new CopyOnWriteArrayList<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            String name = thread.getName();
            if (thread.isAlive() && isTransportThread(name) && !excludedIds.contains(thread.getId())) {
                names.add(name);
            }
        }
        return names;
    }

    private static boolean isTransportThread(String name) {
        return name.startsWith("rowweave-src-") || name.startsWith("rowweave-sink-");
    }

    private static void awaitCondition(Check check, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!check.test() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
    }

    private interface Check {
        boolean test();
    }
}
