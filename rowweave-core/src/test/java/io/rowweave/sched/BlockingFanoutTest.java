package io.rowweave.sched;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

/**
 * A BLOCKING fan-out breaks the deadlock cycle. The fork output is materialized and each
 * group is replayed concurrently on its own thread.
 *
 * <p>One group's channel is held parked for a long time, which stands in for a sort-merge
 * join draining the left side while the right side has no credit, and two things are
 * verified: the other groups still finish and reach EOS, and the materialize-and-replay fork
 * is not blocked by the parked group, so that group also receives everything once it is
 * released later. A serial broadcast or a sequentially interleaved replay would stall
 * everything as soon as one group parks, and this test separates the two.
 */
class BlockingFanoutTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));

    private static DataRow row(int k) {
        DataRow r = new DataRow(1);
        r.set(0, k);
        return r;
    }

    private static RowStream streamOf(List<DataRow> rows) {
        java.util.Iterator<DataRow> it = rows.iterator();
        return () -> it.hasNext() ? StreamEvent.record(it.next()) : null;
    }

    /** A collecting channel: send appends, close records the EOS, used by the single-instance group with no partitioner. */
    private static final class CollectChannel implements Channel {
        final List<Integer> got = new ArrayList<>();
        volatile boolean closed;
        @Override public synchronized void send(StreamEvent e) { got.add((Integer) e.row().get(0)); }
        @Override public StreamEvent poll() { return null; }
        @Override public synchronized void close() { closed = true; }
    }

    /** A channel that parks on command: until the release latch opens, send blocks once the gate row is reached, which stands in for a downstream at zero credit that is not draining. */
    private static final class GatedChannel implements Channel {
        final List<Integer> got = new ArrayList<>();
        volatile boolean closed;
        private final int gate;
        private final CountDownLatch release;
        GatedChannel(int gate, CountDownLatch release) { this.gate = gate; this.release = release; }
        @Override public void send(StreamEvent e) {
            synchronized (this) { got.add((Integer) e.row().get(0)); }
            if (got.size() >= gate) {
                try { release.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        @Override public StreamEvent poll() { return null; }
        @Override public void close() { closed = true; }
    }

    /**
     * One group's channel parks for a long time while another still finishes with its EOS, and
     * the parked group receives everything once released later. This shows each group runs on its
     * own thread and none blocks another.
     */
    @Test
    void parkedGroupDoesNotBlockOthers() {
        assertTimeoutPreemptively(ofSeconds(15), () -> {
            List<DataRow> src = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                src.add(row(i));
            }
            CountDownLatch release = new CountDownLatch(1);
            GatedChannel parked = new GatedChannel(10, release);  // parks after ten rows
            CollectChannel free = new CollectChannel();

            BlockingFanout.Group gParked = new BlockingFanout.Group(Arrays.asList((Channel) parked), null);
            BlockingFanout.Group gFree = new BlockingFanout.Group(Arrays.asList((Channel) free), null);

            try (SpillStore store = new MemorySpillStore()) {
                MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
                // materializeAndReplay runs on a background thread, which joins both replay threads, so the parked group holds it at the release latch.
                AtomicInteger emitted = new AtomicInteger(-1);
                Thread runner = new Thread(() -> emitted.set((int) BlockingFanout.materializeAndReplay(
                        streamOf(src), buf, Arrays.asList(gParked, gFree), "test")));
                runner.start();

                // The free group should collect all 100 rows plus its EOS quickly, even while the parked group is stuck at row ten, which shows neither blocks the other.
                long deadline = System.currentTimeMillis() + 10_000;
                while ((free.got.size() < 100 || !free.closed) && System.currentTimeMillis() < deadline) {
                    TimeUnit.MILLISECONDS.sleep(20);
                }
                assertEquals(100, free.got.size(), "the free group receives all 100 rows without being held up by the parked group");
                assertTrue(free.closed, "the free group receives EOS");
                assertTrue(parked.got.size() >= 10 && parked.got.size() <= 10 + 1,
                        "the parked group is held at the gate (about 10 rows) and has not finished");
                assertEquals(-1, emitted.get(), "materializeAndReplay is still waiting on the parked group and has not returned");

                // Release the parked group, which finishes it and lets materializeAndReplay return.
                release.countDown();
                runner.join(5_000);
                assertEquals(100, parked.got.size(), "after the release the parked group also receives all 100 rows");
                assertTrue(parked.closed, "the parked group receives EOS");
                assertEquals(100, emitted.get(), "the emitted count equals the 100 materialized rows");
            }
        });
    }

    /** Several groups replay concurrently and each receives the full content, since the cursors are independent and the content agrees. */
    @Test
    void allGroupsReceiveFullData() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            List<DataRow> src = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                src.add(row(i));
            }
            CollectChannel c1 = new CollectChannel();
            CollectChannel c2 = new CollectChannel();
            CollectChannel c3 = new CollectChannel();
            List<BlockingFanout.Group> groups = Arrays.asList(
                    new BlockingFanout.Group(Arrays.asList((Channel) c1), null),
                    new BlockingFanout.Group(Arrays.asList((Channel) c2), null),
                    new BlockingFanout.Group(Arrays.asList((Channel) c3), null));
            try (SpillStore store = new MemorySpillStore()) {
                MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
                long emitted = BlockingFanout.materializeAndReplay(streamOf(src), buf, groups, "multi");
                assertEquals(500, emitted);
                for (CollectChannel c : Arrays.asList(c1, c2, c3)) {
                    assertEquals(500, c.got.size(), "each group receives all 500 rows");
                    assertTrue(c.closed, "each group receives EOS");
                    assertEquals(0, (int) c.got.get(0));
                    assertEquals(499, (int) c.got.get(499), "the order survives (materialized order equals append order)");
                }
            }
        });
    }

    @Test
    void ownerAwareCloseFailurePoisonsEveryGroupOnceAndRetainsCleanupEvidence() {
        RuntimeException closeFailure = new RuntimeException("group close failed");
        RuntimeException failCleanup = new RuntimeException("group fail cleanup failed");
        AtomicInteger firstCloses = new AtomicInteger();
        AtomicInteger secondCloses = new AtomicInteger();
        AtomicInteger firstFails = new AtomicInteger();
        AtomicInteger secondFails = new AtomicInteger();
        Channel discard = new Channel() {
            @Override public void send(StreamEvent event) { }
            @Override public StreamEvent poll() { return null; }
            @Override public void close() { }
        };
        List<BlockingFanout.Group> groups = Arrays.asList(
                new BlockingFanout.Group(Collections.singletonList(discard), null,
                        () -> {
                            firstCloses.incrementAndGet();
                            throw closeFailure;
                        },
                        failure -> {
                            firstFails.incrementAndGet();
                            throw failCleanup;
                        }),
                new BlockingFanout.Group(Collections.singletonList(discard), null,
                        secondCloses::incrementAndGet,
                        failure -> secondFails.incrementAndGet()));

        try (SpillStore store = new MemorySpillStore()) {
            MaterializedRowBuffer buffer = new MaterializedRowBuffer(META, store);
            RuntimeException observed = assertThrows(RuntimeException.class,
                    () -> BlockingFanout.materializeAndReplay(
                            streamOf(Collections.emptyList()), buffer, groups, "close-failure"));

            assertSame(closeFailure, observed.getCause());
            assertArrayEquals(new Throwable[]{failCleanup}, closeFailure.getSuppressed());
            assertEquals(1, firstCloses.get());
            assertTrue(secondCloses.get() <= 1,
                    "peer normal close may lose to group failure but cannot repeat");
            assertEquals(1, firstFails.get());
            assertEquals(1, secondFails.get());
        }
    }
}
