package io.rowweave.internal;

import io.rowweave.BatchExecutableSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.SourceExecutor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequentialInputExecutorCursorTest {

    private static final DataSchema META =
            new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void cancellationDuringOpenRejectsLatePublicationAndClosesOnce() throws Exception {
        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch releaseOpen = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        AtomicBoolean executorClosed = new AtomicBoolean();
        TestInput input = new TestInput(self -> new SourceExecutor<TestInput>(self, META) {
            @Override public void open() {
                openStarted.countDown();
                await(releaseOpen);
            }
            @Override public boolean hasNext() { return false; }
            @Override public DataRow next() { throw new AssertionError("unreachable"); }
            @Override public void close() {
                if (executorClosed.compareAndSet(false, true)) closes.incrementAndGet();
                releaseOpen.countDown();
            }
        });
        SequentialInputExecutorCursor cursor =
                new SequentialInputExecutorCursor(Collections.singletonList(input));
        cursor.open();
        FutureTask<Boolean> reading = new FutureTask<>(cursor::hasNext);
        Thread reader = new Thread(reading, "cursor-open-race");
        try {
            reader.start();
            assertTrue(openStarted.await(10, TimeUnit.SECONDS));

            cursor.close();
            assertEquals(1, closes.get(),
                    "cancel must actively close an executor whose open is still blocked");

            assertFalse(reading.get(10, TimeUnit.SECONDS));
            assertEquals(1, closes.get(),
                    "late open must be rejected, closed and never published after cancellation");
        } finally {
            releaseOpen.countDown();
            cursor.close();
            reader.join(10_000);
        }
    }

    @Test
    void cancellationDuringHasNextOwnsTheOnlyCloseAndDoesNotNpe() throws Exception {
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        TestInput input = new TestInput(self -> new SourceExecutor<TestInput>(self, META) {
            @Override public void open() { }
            @Override public boolean hasNext() {
                readStarted.countDown();
                await(releaseRead);
                return false;
            }
            @Override public DataRow next() { throw new AssertionError("unreachable"); }
            @Override public void close() {
                closes.incrementAndGet();
                releaseRead.countDown();
            }
        });
        SequentialInputExecutorCursor cursor =
                new SequentialInputExecutorCursor(Collections.singletonList(input));
        cursor.open();
        FutureTask<Boolean> reading = new FutureTask<>(cursor::hasNext);
        Thread reader = new Thread(reading, "cursor-read-race");
        try {
            reader.start();
            assertTrue(readStarted.await(10, TimeUnit.SECONDS));

            cursor.close();

            assertFalse(reading.get(10, TimeUnit.SECONDS));
            assertEquals(1, closes.get(),
                    "cancel and exhaustion must atomically detach one close owner");
        } finally {
            releaseRead.countDown();
            cursor.close();
            reader.join(10_000);
        }
    }

    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private interface ExecutorFactory {
        SourceExecutor<?> create(TestInput input);
    }

    private static final class TestInput implements BatchExecutableSource {
        private final ExecutorFactory factory;

        private TestInput(ExecutorFactory factory) {
            this.factory = factory;
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return null; }
        @Override public SourceExecutor<?> createSourceExecutor() { return factory.create(this); }
    }
}
