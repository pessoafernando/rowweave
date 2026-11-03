package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.PipelineExecutor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A group build publishes only after its reservation and every candidate resource have succeeded. */
class GroupBuildSpillAtomicityTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.LongType));

    private static final class LongSum implements Accumulator {
        private long total;

        @Override
        public byte[] writeState() {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeLong(total);
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
            return bytes.toByteArray();
        }

        @Override
        public void mergeState(byte[] state) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(state))) {
                total += in.readLong();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
    }

    private static byte[] key(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Supplier<Object[]> accumulators() {
        return () -> new Object[]{new LongSum()};
    }

    @Test
    void productionSourceUsesOneReservationPathAndTerminalGuards() throws IOException {
        String source = productionSource();
        assertTrue(source.contains("memoryManager.reserve(PER_GROUP_RESERVE)"));
        assertTrue(!source.contains("recordSelfSpill"));
        assertTrue(source.contains("ensureUsable();"));
        assertTrue(source.contains("failStop(failure)"));
        assertTrue(source.contains("Phase 1: append the complete batch"));
        assertTrue(source.contains("Phase 2: the batch is complete"));
    }

    @Test
    void budgetSmallerThanOneGroupFailsWithoutUnaccountedEntry() {
        MemoryManager memory = new MemoryManager(63);
        HeapPool pool = new HeapPool();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, new MemorySpillStore());

        assertThrows(MemoryBudgetExceededException.class, () -> buffer.getOrCreate(key("a")));
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0L, memory.peakReservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0L, buffer.groupCount());
    }

    @Test
    void factoryFailureRollsBackCandidateAndBufferRemainsUsable() {
        MemoryManager memory = new MemoryManager(128);
        HeapPool pool = new HeapPool();
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException factoryRoot = new IllegalStateException("factory-root");
        Supplier<Object[]> failOnce = () -> {
            if (calls.getAndIncrement() == 0) {
                throw factoryRoot;
            }
            return new Object[]{new LongSum()};
        };
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, failOnce, memory, pool, new MemorySpillStore());

        assertSame(factoryRoot, assertThrows(IllegalStateException.class,
                () -> buffer.getOrCreate(key("a"))));
        assertEquals(0L, memory.reservedBytes(), "a failed candidate leaves no phantom reservation");
        assertEquals(0, pool.outstanding(), "a failed candidate returns its segments");
        assertEquals(0L, buffer.groupCount(), "nothing is published to the entry slot before the factory succeeds");

        Object[] recovered = buffer.getOrCreate(key("a"));
        ((LongSum) recovered[0]).total = 7;
        assertEquals(1L, buffer.groupCount());
        assertEquals(64L, memory.reservedBytes());
        assertEquals(1, pool.outstanding());
        buffer.spill();
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    @Test
    void segmentWriteFailureRollsBackAndCanRetry() {
        MemoryManager memory = new MemoryManager(128);
        FailOnceWritePool pool = new FailOnceWritePool();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, new MemorySpillStore());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> buffer.getOrCreate(key("a")));
        assertEquals("segment-write-root", failure.getMessage());
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0L, buffer.groupCount());

        buffer.getOrCreate(key("a"));
        assertEquals(1L, buffer.groupCount());
        buffer.spill();
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    @Test
    void candidateFreeCleanupFailureTurnsPrepareRootIntoTerminal() {
        MemoryManager memory = new MemoryManager(128);
        FreeFailingPool pool = new FreeFailingPool();
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException root = new IllegalStateException("prepare-root");
        Supplier<Object[]> failOnce = () -> {
            if (calls.getAndIncrement() == 0) {
                throw root;
            }
            return new Object[]{new LongSum()};
        };
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, failOnce, memory, pool, new MemorySpillStore());

        assertSame(root, assertThrows(IllegalStateException.class,
                () -> buffer.getOrCreate(key("a"))));
        assertEquals(1, root.getSuppressed().length);
        assertEquals("candidate-free-cleanup", root.getSuppressed()[0].getMessage());
        assertEquals(1, pool.freeCalls.get(), "the direct owner tries to free a failed candidate at most once");
        assertEquals(0L, memory.reservedBytes(), "the other cleanup categories still run");
        assertTrue(pool.outstanding() > 0, "a free primitive that keeps failing does not falsely report the pool as drained");

        assertTerminal(buffer, memory, root);
        assertEquals(1, pool.freeCalls.get(), "the terminal check does not retry a candidate of unknown ownership");
    }

    @Test
    void candidateReleaseCleanupFailureTurnsPrepareRootIntoTerminal() {
        MemoryManager memory = new MemoryManager(128);
        HeapPool pool = new HeapPool();
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException root = new IllegalStateException("prepare-release-root");
        Supplier<Object[]> corruptReservationOnce = () -> {
            if (calls.getAndIncrement() == 0) {
                memory.releaseExact(64);
                throw root;
            }
            return new Object[]{new LongSum()};
        };
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, corruptReservationOnce, memory, pool, new MemorySpillStore());

        assertSame(root, assertThrows(IllegalStateException.class,
                () -> buffer.getOrCreate(key("a"))));
        assertEquals(1, root.getSuppressed().length);
        assertTrue(root.getSuppressed()[0].getMessage().contains("release exceeds reservation"));
        assertEquals(0, pool.outstanding(), "segment cleanup still succeeds when the memory manager cleanup fails");
        assertEquals(0L, memory.reservedBytes());

        assertTerminal(buffer, memory, root);
    }

    @Test
    void spillSegmentFreeFailureBecomesTerminalWithoutRetryingUnknownOwnership() {
        MemoryManager memory = new MemoryManager(128);
        FreeFailingPool pool = new FreeFailingPool();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, new MemorySpillStore());
        buffer.getOrCreate(key("spill-free"));

        IllegalStateException root = assertThrows(IllegalStateException.class, buffer::spill);
        assertEquals("candidate-free-cleanup", root.getMessage());
        assertEquals(1, pool.freeCalls.get(), "the spill owner frees at most once directly after removing the slot");
        assertEquals(0L, memory.reservedBytes(), "a pool fault does not block memory manager cleanup");
        assertTrue(pool.outstanding() > 0, "a failing free primitive does not falsely report the corresponding pool as drained");
        assertTerminal(buffer, memory, root);
        assertEquals(1, pool.freeCalls.get(), "a terminal operation does not retry a segment of unknown ownership");
    }

    @Test
    void partialSpillFailureAbortsHistoryBeforeCloseAndBecomesTerminal() {
        MemoryManager memory = new MemoryManager(1024);
        CountingPool pool = new CountingPool();
        PartialAppendFailingStore store = new PartialAppendFailingStore(pool.freeCalls::get);
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, store);

        buffer.getOrCreate(key("history"));
        buffer.spill();
        assertEquals(1, pool.freeCalls.get(), "a first spill round completes successfully");
        List<byte[]> current = keysInDifferentPartitions();
        buffer.getOrCreate(current.get(0));
        buffer.getOrCreate(current.get(1));

        IllegalStateException root = assertThrows(IllegalStateException.class, buffer::spill);
        assertSame(store.appendRoot, root);
        assertEquals(1, store.freeCallsAtRoot,
                "no segment of the current round is released before every append finishes, and at the moment of failure only the earlier round has been freed");
        assertEquals(2, root.getSuppressed().length);
        assertEquals("run-abort-cleanup", root.getSuppressed()[0].getMessage());
        assertEquals("store-close-cleanup", root.getSuppressed()[1].getMessage());
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, store.openWriters.get());
        assertEquals(store.created.get(), store.abortCalls.get(), "every known run is aborted independently");
        assertEquals(1, store.closeCalls.get());

        assertTerminal(buffer, memory, root);
        assertEquals(1, store.closeCalls.get(), "close and cleanup are not repeated after the terminal state");
    }

    @Test
    void topLevelFinishFailureClosesStoreAndBecomesTerminal() {
        MemoryManager memory = new MemoryManager(64);
        TopLevelFinishFailingStore store = new TopLevelFinishFailingStore();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store);
        buffer.getOrCreate(key("finish-failure"));
        buffer.spill();

        assertSame(store.finishRoot, assertThrows(IllegalStateException.class, buffer::iterator));
        assertTrue(store.closed, "a failed top-level finish closes the whole spill store");
        assertTerminal(buffer, memory, store.finishRoot);
    }

    @Test
    void capturedStreamingIteratorObservesLaterRepartitionFailureAsTerminal() {
        MemoryManager memory = new MemoryManager(128);
        ArmableRepartitionStore store = new ArmableRepartitionStore();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store, 1);
        for (byte[] candidate : keysInPartition(0, 2)) {
            buffer.getOrCreate(candidate);
        }
        for (byte[] candidate : keysInPartition(15, 3)) {
            buffer.getOrCreate(candidate);
        }

        java.util.Iterator<GroupAggBuffer.Group> iterator = buffer.iterator();
        store.arm();
        assertTrue(iterator.hasNext());
        iterator.next();

        assertSame(store.repartitionRoot,
                assertThrows(IllegalStateException.class, iterator::next));
        assertTrue(store.closed, "a failed merge or repartition closes the whole spill store");
        assertSame(store.repartitionRoot,
                assertThrows(IllegalStateException.class, iterator::hasNext));
        assertSame(store.repartitionRoot,
                assertThrows(IllegalStateException.class, iterator::next));
        assertTerminal(buffer, memory, store.repartitionRoot);
    }

    @Test
    void buildSpillFailureAbortsPipelineBeforeAnyOutput() {
        MemoryManager memory = new MemoryManager(64);
        CountingPool pool = new CountingPool();
        PartialAppendFailingStore store = new PartialAppendFailingStore(pool.freeCalls::get);
        TrackingOutput output = new TrackingOutput();
        PipelineExecutor executor = new PipelineExecutor()
                .addStage("source", PipelineExecutor.source(Collections.singletonList(new DataRow(0))))
                .addStage("group-build", PipelineExecutor.streamingBreaker((upstream, sink) -> {
                    while (upstream.next() != null) {
                        // drain source before the breaker starts producing
                    }
                    OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                            META, new String[]{"k"}, accumulators(), memory, pool, store);
                    for (int i = 0; i < 4; i++) {
                        buffer.getOrCreate(key("pipeline-key-" + i));
                    }
                    for (GroupAggBuffer.Group ignored : buffer) {
                        sink.emitRow(new DataRow(0));
                    }
                    sink.endOfStream();
                }))
                .addStage("output", PipelineExecutor.outputStage(output));

        assertThrows(RuntimeException.class, executor::run);
        assertEquals(0, output.collected.get(), "a failed build leaks no partial result");
        assertTrue(output.aborted, "the pipeline aborts the Sink");
        assertTrue(!output.closed, "a failure does not take the successful close path");
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    private static void assertTerminal(OpenAddressingGroupAggBuffer buffer, MemoryManager memory,
                                       IllegalStateException root) {
        assertSame(root, assertThrows(IllegalStateException.class,
                () -> buffer.getOrCreate(key("after-terminal"))));
        assertSame(root, assertThrows(IllegalStateException.class, buffer::spill));
        assertSame(root, assertThrows(IllegalStateException.class, buffer::groupCount));
        assertSame(root, assertThrows(IllegalStateException.class, buffer::iterator));
        assertTrue(buffer.recursiveRepartitions() >= 0, "the diagnostic probe stays readable after the terminal state");

        memory.reserve(memory.budget());
        assertSame(root, assertThrows(IllegalStateException.class, () -> memory.reserve(1)),
                "a registered terminal spillable keeps the owning operator memory manager failed");
        memory.releaseExact(memory.budget());
    }

    private static List<byte[]> keysInDifferentPartitions() {
        List<byte[]> found = new ArrayList<>();
        int firstPartition = -1;
        for (int i = 0; found.size() < 2; i++) {
            byte[] candidate = key("partition-key-" + i);
            int partition = Math.floorMod(Arrays.hashCode(candidate), 16);
            if (firstPartition < 0) {
                firstPartition = partition;
                found.add(candidate);
            } else if (partition != firstPartition) {
                found.add(candidate);
            }
        }
        return found;
    }

    private static List<byte[]> keysInPartition(int partition, int count) {
        List<byte[]> found = new ArrayList<>();
        for (int i = 0; found.size() < count; i++) {
            byte[] candidate = key("partition-" + partition + "-key-" + i);
            if (Math.floorMod(Arrays.hashCode(candidate), 16) == partition) {
                found.add(candidate);
            }
        }
        return found;
    }

    private static String productionSource() throws IOException {
        Path cwd = Paths.get("").toAbsolutePath();
        Path source = cwd.resolve("src/main/java/io/rowweave/mem/OpenAddressingGroupAggBuffer.java");
        if (!Files.exists(source)) {
            source = cwd.resolve(
                    "rowweave-core/src/main/java/io/rowweave/mem/OpenAddressingGroupAggBuffer.java");
        }
        return new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
    }

    private static final class TopLevelFinishFailingStore implements SpillStore {
        private final MemorySpillStore delegate = new MemorySpillStore();
        private final IllegalStateException finishRoot = new IllegalStateException("top-finish-root");
        private boolean closed;

        @Override
        public SpillRun createRun() {
            SpillRun run = delegate.createRun();
            return new SpillRun() {
                @Override public void append(byte[] frame) { run.append(frame); }

                @Override
                public void finishWriting() {
                    run.finishWriting();
                    throw finishRoot;
                }

                @Override public java.util.Iterator<byte[]> read() { return run.read(); }
                @Override public void abort() { run.abort(); }
            };
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }
    }

    private static final class ArmableRepartitionStore implements SpillStore {
        private final MemorySpillStore delegate = new MemorySpillStore();
        private final IllegalStateException repartitionRoot =
                new IllegalStateException("repartition-append-root");
        private boolean armed;
        private boolean closed;

        void arm() {
            armed = true;
        }

        @Override
        public SpillRun createRun() {
            SpillRun run = delegate.createRun();
            boolean failAppend = armed;
            return new SpillRun() {
                @Override
                public void append(byte[] frame) {
                    if (failAppend) {
                        throw repartitionRoot;
                    }
                    run.append(frame);
                }

                @Override public void finishWriting() { run.finishWriting(); }
                @Override public java.util.Iterator<byte[]> read() { return run.read(); }
                @Override public void abort() { run.abort(); }
            };
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }
    }

    private static final class TrackingOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger collected = new AtomicInteger();
        private boolean closed;
        private boolean aborted;

        @Override public DataSchema getSchema() { return new DataSchema(); }

        @Override
        public void collect(DataRow row) {
            collected.incrementAndGet();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void abort() {
            aborted = true;
        }
    }

    private static class CountingPool implements MemorySegmentPool {
        final HeapPool delegate = new HeapPool();
        final AtomicInteger freeCalls = new AtomicInteger();

        @Override
        public MemorySegment allocate(int size) {
            return delegate.allocate(size);
        }

        @Override
        public void free(MemorySegment segment) {
            freeCalls.incrementAndGet();
            delegate.free(segment);
        }

        @Override
        public int outstanding() {
            return delegate.outstanding();
        }
    }

    private static final class PartialAppendFailingStore implements SpillStore {
        private final MemorySpillStore delegate = new MemorySpillStore();
        private final IntSupplier freeCalls;
        private final List<TrackedRun> runs = new ArrayList<>();
        private final AtomicInteger appends = new AtomicInteger();
        private final AtomicInteger created = new AtomicInteger();
        private final AtomicInteger abortCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicInteger openWriters = new AtomicInteger();
        private final AtomicInteger abortCleanupOnce = new AtomicInteger();
        private final IllegalStateException appendRoot = new IllegalStateException("spill-append-root");
        private int freeCallsAtRoot = -1;

        PartialAppendFailingStore(IntSupplier freeCalls) {
            this.freeCalls = freeCalls;
        }

        @Override
        public SpillRun createRun() {
            TrackedRun tracked = new TrackedRun(delegate.createRun());
            runs.add(tracked);
            created.incrementAndGet();
            openWriters.incrementAndGet();
            return tracked;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            delegate.close();
            for (TrackedRun run : runs) {
                run.finishOwner();
            }
            throw new IllegalStateException("store-close-cleanup");
        }

        private final class TrackedRun implements SpillRun {
            private final SpillRun run;
            private boolean writing = true;

            TrackedRun(SpillRun run) {
                this.run = run;
            }

            @Override
            public void append(byte[] frame) {
                if (appends.incrementAndGet() == 3) {
                    freeCallsAtRoot = freeCalls.getAsInt();
                    throw appendRoot;
                }
                run.append(frame);
            }

            @Override
            public void finishWriting() {
                run.finishWriting();
                finishOwner();
            }

            @Override
            public java.util.Iterator<byte[]> read() {
                return run.read();
            }

            @Override
            public void abort() {
                abortCalls.incrementAndGet();
                run.abort();
                finishOwner();
                if (abortCleanupOnce.getAndIncrement() == 0) {
                    throw new IllegalStateException("run-abort-cleanup");
                }
            }

            void finishOwner() {
                if (writing) {
                    writing = false;
                    openWriters.decrementAndGet();
                }
            }
        }
    }

    private static final class FailOnceWritePool implements MemorySegmentPool {
        private final HeapPool delegate = new HeapPool();
        private final AtomicInteger allocations = new AtomicInteger();

        @Override
        public MemorySegment allocate(int size) {
            MemorySegment segment = delegate.allocate(size);
            if (allocations.getAndIncrement() != 0) {
                return segment;
            }
            return new DelegatingSegment(segment) {
                @Override
                public void putBytes(int offset, byte[] src) {
                    throw new IllegalStateException("segment-write-root");
                }
            };
        }

        @Override
        public void free(MemorySegment segment) {
            delegate.free(unwrap(segment));
        }

        @Override
        public int outstanding() {
            return delegate.outstanding();
        }
    }

    private static final class FreeFailingPool implements MemorySegmentPool {
        private final HeapPool delegate = new HeapPool();
        private final AtomicInteger freeCalls = new AtomicInteger();

        @Override
        public MemorySegment allocate(int size) {
            return delegate.allocate(size);
        }

        @Override
        public void free(MemorySegment segment) {
            freeCalls.incrementAndGet();
            throw new IllegalStateException("candidate-free-cleanup");
        }

        @Override
        public int outstanding() {
            return delegate.outstanding();
        }
    }

    private static class DelegatingSegment implements MemorySegment {
        private final MemorySegment delegate;

        DelegatingSegment(MemorySegment delegate) {
            this.delegate = delegate;
        }

        @Override public int size() { return delegate.size(); }
        @Override public void putByte(int offset, byte value) { delegate.putByte(offset, value); }
        @Override public byte getByte(int offset) { return delegate.getByte(offset); }
        @Override public void putInt(int offset, int value) { delegate.putInt(offset, value); }
        @Override public int getInt(int offset) { return delegate.getInt(offset); }
        @Override public void putLong(int offset, long value) { delegate.putLong(offset, value); }
        @Override public long getLong(int offset) { return delegate.getLong(offset); }
        @Override public void putBytes(int offset, byte[] src) { delegate.putBytes(offset, src); }
        @Override public byte[] getBytes(int offset, int length) { return delegate.getBytes(offset, length); }
        @Override public void copyTo(int offset, byte[] target, int targetOffset, int length) {
            delegate.copyTo(offset, target, targetOffset, length);
        }
        @Override public void clear() { delegate.clear(); }
    }

    private static MemorySegment unwrap(MemorySegment segment) {
        return segment instanceof DelegatingSegment
                ? ((DelegatingSegment) segment).delegate
                : segment;
    }
}
