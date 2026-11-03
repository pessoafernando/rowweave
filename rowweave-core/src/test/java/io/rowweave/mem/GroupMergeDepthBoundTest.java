package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.PipelineExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Merging spilled aggregates must never bypass managed memory at the maximum re-partition depth. */
class GroupMergeDepthBoundTest {

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

    private static Supplier<Object[]> accumulators() {
        return () -> new Object[]{new LongSum()};
    }

    /** Find the distinct keys of one first-tier spill partition, to build a run that deterministically exceeds the budget. */
    private static List<byte[]> keysInPartition(int partition, int count) {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; keys.size() < count; i++) {
            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            if (Math.floorMod(Arrays.hashCode(key), 16) == partition) {
                keys.add(key);
            }
        }
        return keys;
    }

    private static int saltedPartition(byte[] key, int salt) {
        int hash = 0x811C9DC5 ^ (salt * 0x9E3779B1);
        for (byte b : key) {
            hash = (hash ^ (b & 0xFF)) * 0x01000193;
        }
        hash ^= hash >>> 15;
        hash *= 0x85EBCA77;
        hash ^= hash >>> 13;
        return Math.floorMod(hash, 16);
    }

    private static List<byte[]> pairSplitByFirstSalt() {
        for (int partition = 0; partition < 16; partition++) {
            List<byte[]> candidates = keysInPartition(partition, 32);
            for (int i = 0; i < candidates.size(); i++) {
                for (int j = i + 1; j < candidates.size(); j++) {
                    if (saltedPartition(candidates.get(i), 1) != saltedPartition(candidates.get(j), 1)) {
                        return Arrays.asList(candidates.get(i), candidates.get(j));
                    }
                }
            }
        }
        throw new AssertionError("failed to find deterministic split pair");
    }

    @Test
    void productionSourceHasNoUnbudgetedDepthEscape() throws Exception {
        String source = productionSource();
        assertFalse(source.contains("budgeted ="));
        assertFalse(source.contains("depth < MAX_RECURSION_DEPTH"));
        assertFalse(source.contains("deepest level merges unbounded"));
        assertTrue(source.contains("frame.depth >= maxRepartitionDepth"));
        assertTrue(source.contains("throw new GroupMergeDepthExceededException"));
        assertTrue(source.contains("new MemoryBudgetExceededException"));
    }

    @Test
    void negativeDepthIsRejectedBeforeAnyResourceIsTaken(@TempDir Path temp) {
        MemoryManager memory = new MemoryManager(64);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(temp.toFile());

        assertThrows(IllegalArgumentException.class, () -> new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, store, -1));
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        store.close();
    }

    @Test
    void depthExhaustionFailsLoudAndReleasesMergeResources(@TempDir Path temp) {
        MemoryManager memory = new MemoryManager(64);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(temp.toFile());
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, store, 0);
        List<byte[]> keys = keysInPartition(0, 2);
        for (byte[] key : keys) {
            ((LongSum) buffer.getOrCreate(key)[0]).total++;
        }

        assertThrows(MemoryBudgetExceededException.class, buffer::iterator,
                "with depth=0 a first partition that does not fit fails explicitly instead of degrading into an unbudgeted map");
        assertThrows(MemoryBudgetExceededException.class, buffer::iterator,
                "an explicit depth exhaustion is not store corruption and the buffer can be diagnosed and retried");
        assertEquals(0L, buffer.spill(), "public operations still work after a non-terminal depth failure");
        assertEquals(0L, memory.reservedBytes(), "a failure releases the local merge reservation");
        assertEquals(0, store.currentOpenReaders(), "an early break on overflow still closes the reader");
        assertEquals(0, store.currentOpenWriters(), "depth exhaustion leaves no sub-run in WRITING");
        assertEquals(0, pool.outstanding(), "the build phase spills and returns its memory before the merge starts");
        store.close();
    }

    @Test
    void repartitionStillSucceedsBeforeDepthLimit(@TempDir Path temp) {
        MemoryManager memory = new MemoryManager(64);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(temp.toFile());
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, store, 1);
        for (byte[] key : pairSplitByFirstSalt()) {
            ((LongSum) buffer.getOrCreate(key)[0]).total++;
        }

        int groups = 0;
        for (GroupAggBuffer.Group ignored : buffer) {
            groups++;
        }
        assertEquals(2, groups, "a repartition still succeeds while depth remains, rather than failing at the first overflow");
        assertEquals(1, buffer.recursiveRepartitions());
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0, pool.outstanding());
        store.close();
    }

    @Test
    void consumedScratchRunsReleaseDiskWhileRootRemainsReplayable(@TempDir Path temp) throws Exception {
        MemoryManager memory = new MemoryManager(64);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(temp.toFile());
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, store, 1);
        List<byte[]> keys = pairSplitByFirstSalt();
        try {
            for (byte[] key : keys) {
                ((LongSum) buffer.getOrCreate(key)[0]).total++;
            }
            buffer.spill();
            assertEquals(1L, fileCount(temp), "one top-level partition keeps exactly one replayable root run");

            List<String> first = groupsWithSums(buffer);
            assertEquals(2, first.size());
            assertEquals(1L, fileCount(temp),
                    "after a full consumption the temporary repartition run is released and the root run stays readable");

            List<String> replay = groupsWithSums(buffer);
            assertEquals(first, replay, "releasing the temporary run changes nothing about the keys, values or order of the second iteration");
            assertEquals(1L, fileCount(temp), "repeated iterations accumulate no temporary runs");
            assertEquals(0, store.currentOpenReaders());
            assertEquals(0, store.currentOpenWriters());
        } finally {
            buffer.close();
        }
        assertEquals(0L, fileCount(temp));
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    private static List<String> groupsWithSums(OpenAddressingGroupAggBuffer buffer) {
        List<String> rows = new ArrayList<>();
        for (GroupAggBuffer.Group group : buffer) {
            rows.add(new String(group.keyBytes(), StandardCharsets.UTF_8) + ":"
                    + ((LongSum) group.accumulators()[0]).total);
        }
        return rows;
    }

    private static long fileCount(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.count();
        }
    }

    @Test
    void subRunFailureAbortsWriterAndPreservesPrimaryCause() {
        MemoryManager memory = new MemoryManager(64);
        FailingSubRunStore store = new FailingSubRunStore();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store, 1);
        for (byte[] key : keysInPartition(0, 2)) {
            ((LongSum) buffer.getOrCreate(key)[0]).total++;
        }

        IllegalStateException failure = assertThrows(IllegalStateException.class, buffer::iterator);
        assertEquals("sub-append-root", failure.getMessage(), "cleanup does not replace the first cause, which is the repartition write failure");
        assertEquals(1, failure.getSuppressed().length, "a failed abort cleanup is attached as suppressed");
        assertEquals("sub-abort-cleanup", failure.getSuppressed()[0].getMessage());
        assertEquals(0, store.openWriters(), "a failed sub-run releases its writer first even when abort throws");
        assertEquals(0L, memory.reservedBytes());
        store.close();
    }

    @Test
    void subRunFinishFailureAbortsAllRemainingWriters() {
        MemoryManager memory = new MemoryManager(64);
        FinishFailingSubRunStore store = new FinishFailingSubRunStore();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store, 1);
        for (byte[] key : keysInPartition(0, 2)) {
            ((LongSum) buffer.getOrCreate(key)[0]).total++;
        }

        IllegalStateException failure = assertThrows(IllegalStateException.class, buffer::iterator);
        assertEquals("sub-finish-root", failure.getMessage());
        assertEquals(0, store.openWriters(), "when one sub-run fails to finish the other runs in WRITING are aborted");
        assertEquals(0L, memory.reservedBytes());
        store.close();
    }

    @Test
    void readerCloseFailureIsSuppressedUnderReadRootCause() {
        MemoryManager memory = new MemoryManager(64);
        ReaderFailingStore store = new ReaderFailingStore();
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store, 0);
        ((LongSum) buffer.getOrCreate(keysInPartition(0, 1).get(0))[0]).total++;
        buffer.spill();

        IllegalStateException failure = assertThrows(IllegalStateException.class, buffer::iterator);
        assertEquals("reader-root", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("reader-close-cleanup", failure.getSuppressed()[0].getMessage());
        assertEquals(0L, memory.reservedBytes());
        store.close();
    }

    @Test
    void firstPartitionDepthFailureAbortsBeforeAnyCollect(@TempDir Path temp) {
        Path outputPath = temp.resolve("zero-partial.tsv");
        TrackingFileOutput output = new TrackingFileOutput(
                new DataSchema(new Field("k", DataType.StringType)), outputPath);
        MemoryManager memory = new MemoryManager(64);
        DiskSpillStore store = new DiskSpillStore(temp.resolve("spill").toFile());
        List<byte[]> failing = keysInPartition(0, 2);

        PipelineExecutor executor = new PipelineExecutor()
                .addStage("source", PipelineExecutor.source(Collections.singletonList(new DataRow(0))))
                .addStage("group-merge", PipelineExecutor.streamingBreaker((upstream, sink) -> {
                    while (upstream.next() != null) {
                        // A single trigger record; the test data goes straight into the buffer, so the first-tier partition is controlled exactly.
                    }
                    OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                            META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store, 0);
                    for (byte[] key : failing) {
                        ((LongSum) buffer.getOrCreate(key)[0]).total++;
                    }
                    for (GroupAggBuffer.Group group : buffer) {
                        DataRow row = new DataRow(1);
                        row.set(0, new String(group.keyBytes(), StandardCharsets.UTF_8));
                        sink.emitRow(row);
                    }
                    sink.endOfStream();
                }))
                .addStage("output", PipelineExecutor.outputStage(output));

        assertThrows(RuntimeException.class, executor::run);
        assertEquals(0, output.collected.get(), "no transient row is collected when the first partition fails");
        assertTrue(output.aborted);
        assertFalse(output.closed);
        assertFalse(Files.exists(outputPath));
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        store.close();
    }

    @Test
    void laterDepthFailureAbortsAlreadyCollectedFile(@TempDir Path temp) {
        Path outputPath = temp.resolve("partial.tsv");
        TrackingFileOutput output = new TrackingFileOutput(new DataSchema(new Field("k", DataType.StringType)), outputPath);
        MemoryManager memory = new MemoryManager(128);
        DiskSpillStore store = new DiskSpillStore(temp.resolve("spill").toFile());
        List<byte[]> early = keysInPartition(0, 2);
        List<byte[]> failing = keysInPartition(15, 3);

        PipelineExecutor executor = new PipelineExecutor()
                .addStage("source", PipelineExecutor.source(Collections.singletonList(new DataRow(0))))
                .addStage("group-merge", PipelineExecutor.streamingBreaker((upstream, sink) -> {
                    while (upstream.next() != null) {
                        // A single trigger record; the test data goes straight into the buffer, so the first-tier partition is controlled exactly.
                    }
                    OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                            META, new String[]{"k"}, accumulators(), memory, new HeapPool(), store, 0);
                    for (byte[] key : early) {
                        ((LongSum) buffer.getOrCreate(key)[0]).total++;
                    }
                    for (byte[] key : failing) {
                        ((LongSum) buffer.getOrCreate(key)[0]).total++;
                    }
                    boolean first = true;
                    for (GroupAggBuffer.Group group : buffer) {
                        DataRow row = new DataRow(1);
                        row.set(0, new String(group.keyBytes(), StandardCharsets.UTF_8));
                        sink.emitRow(row);
                        if (first) {
                            first = false;
                            output.awaitFirstCollect();
                        }
                    }
                    sink.endOfStream();
                }))
                .addStage("output", PipelineExecutor.outputStage(output));

        assertThrows(RuntimeException.class, executor::run);
        assertTrue(output.collected.get() >= 1, "the case really covers an early partition that collected before a later one failed");
        assertTrue(output.aborted, "the failure reaches the pipeline and triggers Sink.abort");
        assertFalse(output.closed, "a failure does not take the successful close and commit path");
        assertFalse(Files.exists(outputPath), "abort must not leave a partial output file behind");
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        store.close();
    }

    private static String productionSource() throws IOException {
        Path cwd = Paths.get("").toAbsolutePath();
        Path source = cwd.resolve("src/main/java/io/rowweave/mem/OpenAddressingGroupAggBuffer.java");
        if (!Files.exists(source)) {
            source = cwd.resolve("rowweave-core/src/main/java/io/rowweave/mem/OpenAddressingGroupAggBuffer.java");
        }
        return new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
    }

    private static final class TrackingFileOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;
        private final AtomicInteger collected = new AtomicInteger();
        private final CountDownLatch firstCollect = new CountDownLatch(1);
        private volatile boolean closed;
        private volatile boolean aborted;

        TrackingFileOutput(DataSchema meta, Path path) {
            this.delegate = new FileSink(meta, path.toString());
        }

        @Override
        public DataSchema getSchema() {
            return delegate.getSchema();
        }

        @Override
        public void collect(DataRow row) {
            collected.incrementAndGet();
            delegate.collect(row);
            firstCollect.countDown();
        }

        void awaitFirstCollect() {
            try {
                assertTrue(firstCollect.await(5, TimeUnit.SECONDS),
                        "output stage did not collect the early partition in time");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted while awaiting early output", interrupted);
            }
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }

        @Override
        public void abort() {
            aborted = true;
            delegate.abort();
        }
    }

    private static final class FailingSubRunStore implements SpillStore {
        private final SpillStore delegate = new MemorySpillStore();
        private final AtomicInteger created = new AtomicInteger();
        private final AtomicInteger openWriters = new AtomicInteger();

        @Override
        public SpillRun createRun() {
            int index = created.getAndIncrement();
            SpillRun run = delegate.createRun();
            openWriters.incrementAndGet();
            return new SpillRun() {
                private boolean writing = true;

                @Override
                public void append(byte[] frame) {
                    if (index > 0) {
                        throw new IllegalStateException("sub-append-root");
                    }
                    run.append(frame);
                }

                @Override
                public void finishWriting() {
                    run.finishWriting();
                    finishWriter();
                }

                @Override
                public java.util.Iterator<byte[]> read() {
                    return run.read();
                }

                @Override
                public void abort() {
                    run.abort();
                    finishWriter();
                    if (index > 0) {
                        throw new IllegalStateException("sub-abort-cleanup");
                    }
                }

                private void finishWriter() {
                    if (writing) {
                        writing = false;
                        openWriters.decrementAndGet();
                    }
                }
            };
        }

        int openWriters() {
            return openWriters.get();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class FinishFailingSubRunStore implements SpillStore {
        private final SpillStore delegate = new MemorySpillStore();
        private final AtomicInteger created = new AtomicInteger();
        private final AtomicInteger openWriters = new AtomicInteger();

        @Override
        public SpillRun createRun() {
            int index = created.getAndIncrement();
            SpillRun run = delegate.createRun();
            openWriters.incrementAndGet();
            return new SpillRun() {
                private boolean writing = true;

                @Override
                public void append(byte[] frame) {
                    run.append(frame);
                }

                @Override
                public void finishWriting() {
                    run.finishWriting();
                    finishWriter();
                    if (index > 0) {
                        throw new IllegalStateException("sub-finish-root");
                    }
                }

                @Override
                public java.util.Iterator<byte[]> read() {
                    return run.read();
                }

                @Override
                public void abort() {
                    run.abort();
                    finishWriter();
                }

                private void finishWriter() {
                    if (writing) {
                        writing = false;
                        openWriters.decrementAndGet();
                    }
                }
            };
        }

        int openWriters() {
            return openWriters.get();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class ReaderFailingStore implements SpillStore {
        private final SpillStore delegate = new MemorySpillStore();

        @Override
        public SpillRun createRun() {
            SpillRun run = delegate.createRun();
            return new SpillRun() {
                @Override
                public void append(byte[] frame) {
                    run.append(frame);
                }

                @Override
                public void finishWriting() {
                    run.finishWriting();
                }

                @Override
                public java.util.Iterator<byte[]> read() {
                    java.util.Iterator<byte[]> reader = run.read();
                    return new FailingCloseIterator(reader);
                }

                @Override
                public void abort() {
                    run.abort();
                }
            };
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class FailingCloseIterator implements java.util.Iterator<byte[]>, AutoCloseable {
        private final java.util.Iterator<byte[]> delegate;

        FailingCloseIterator(java.util.Iterator<byte[]> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            throw new IllegalStateException("reader-root");
        }

        @Override
        public byte[] next() {
            return delegate.next();
        }

        @Override
        public void close() {
            throw new IllegalStateException("reader-close-cleanup");
        }
    }
}
