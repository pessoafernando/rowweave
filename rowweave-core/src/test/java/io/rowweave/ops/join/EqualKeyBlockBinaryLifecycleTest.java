package io.rowweave.ops.join;

import io.rowweave.CoGroupFunction;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A dispose failure on the binary temporary EqualKeyBlock must not mask the first cause, and CoGroup must clean up both blocks. */
class EqualKeyBlockBinaryLifecycleTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("v", DataType.IntegerType));

    @Test
    void sortMergeJoinKeepsDownstreamRootWhenRightBlockDisposeFails() {
        RuntimeException business = new RuntimeException("join-output-root");
        RuntimeException cleanup = new RuntimeException("right-block-dispose");
        FailingDisposeStore store = new FailingDisposeStore(cleanup);
        SortMergeJoin join = SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .withSpill(new MemoryManager(1 << 20), store, new HeapPool())
                .withEqualKeyBlockThreshold(1);

        RuntimeException actual = assertThrows(RuntimeException.class, () -> join.join(
                RowStream.fromIterable(Arrays.asList(row(1, 10))),
                RowStream.fromIterable(Arrays.asList(row(1, 20), row(1, 21))),
                event -> {
                    if (event.isRecord()) {
                        throw business;
                    }
                }));

        assertSame(business, actual);
        assertEquals(1, store.disposeCalls.get());
        assertEquals(1, business.getSuppressed().length);
        assertSame(cleanup, business.getSuppressed()[0]);
    }

    @Test
    void coGroupAttemptsBothBlockDisposalsUnderFunctionRoot() {
        RuntimeException business = new RuntimeException("cogroup-function-root");
        RuntimeException leftCleanup = new RuntimeException("left-block-dispose");
        RuntimeException rightCleanup = new RuntimeException("right-block-dispose");
        // The two pointer sorts of CoGroup each create a run first; only the two EqualKeyBlock runs that follow get an injected dispose failure.
        FailingDisposeStore store = new FailingDisposeStore(null, null, leftCleanup, rightCleanup);
        CoGroupFunction function = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return META; }
            @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right,
                                          io.rowweave.Collector out) {
                throw business;
            }
        };
        CoGroup coGroup = CoGroup.of(META, META, new String[]{"k"}, new String[]{"k"}, function)
                .withSpill(new MemoryManager(1 << 20), store, new HeapPool())
                .withEqualKeyBlockThreshold(1);

        RuntimeException actual = assertThrows(RuntimeException.class, () -> coGroup.coGroup(
                RowStream.fromIterable(Arrays.asList(row(1, 10), row(1, 11))),
                RowStream.fromIterable(Arrays.asList(row(1, 20), row(1, 21))),
                event -> { }));

        assertSame(business, actual);
        assertEquals(4, store.disposeCalls.get(), "dispose is attempted for both sort runs and both block runs");
        assertEquals(1, business.getSuppressed().length);
        assertSame(leftCleanup, business.getSuppressed()[0]);
        assertEquals(1, leftCleanup.getSuppressed().length);
        assertSame(rightCleanup, leftCleanup.getSuppressed()[0]);
    }

    private static DataRow row(int key, int value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static final class FailingDisposeStore implements SpillStore {
        private final List<RuntimeException> failures;
        private final AtomicInteger created = new AtomicInteger();
        private final AtomicInteger disposeCalls = new AtomicInteger();

        private FailingDisposeStore(RuntimeException... failures) {
            this.failures = Arrays.asList(failures);
        }

        @Override
        public SpillRun createRun() {
            int index = created.getAndIncrement();
            RuntimeException failure = failures.get(Math.min(index, failures.size() - 1));
            return new SpillRun() {
                private final List<byte[]> frames = new ArrayList<>();
                @Override public void append(byte[] frame) { frames.add(frame.clone()); }
                @Override public void finishWriting() { }
                @Override public Iterator<byte[]> read() { return frames.iterator(); }
                @Override public void dispose() {
                    disposeCalls.incrementAndGet();
                    if (failure != null) {
                        throw failure;
                    }
                }
            };
        }

        @Override public void close() { }
    }
}
