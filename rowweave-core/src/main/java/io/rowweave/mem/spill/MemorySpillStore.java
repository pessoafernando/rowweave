package io.rowweave.mem.spill;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Heap SpillStore: run frames are kept in an in-memory list. Suits fast unit tests and a disk-free environment, and does not actually prevent OOM.
 */
public final class MemorySpillStore implements SpillStore {

    private final java.util.Set<MemoryRun> runs =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private volatile boolean closed;

    @Override
    public SpillRun createRun() {
        if (closed) {
            throw new IllegalStateException("spill store is closed");
        }
        MemoryRun run = new MemoryRun();
        runs.add(run);
        return run;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (MemoryRun run : new ArrayList<>(runs)) {
            run.dispose();
        }
        runs.clear();
    }

    private final class MemoryRun implements SpillRun {
        private final List<byte[]> frames = new ArrayList<>();
        private boolean finished;
        private boolean aborted;
        private boolean disposed;

        @Override
        public synchronized void append(byte[] frame) {
            ensureWritable();
            frames.add(frame.clone());
        }

        @Override
        public synchronized void finishWriting() {
            if (finished) {
                return;
            }
            ensureWritable();
            finished = true;
        }

        @Override
        public synchronized Iterator<byte[]> read() {
            if (!finished || aborted || disposed) {
                throw new IllegalStateException("read before finishWriting or after abort");
            }
            return new ArrayList<>(frames).iterator();
        }

        @Override
        public synchronized void abort() {
            if (finished || aborted) {
                return;
            }
            aborted = true;
            frames.clear();
        }

        @Override
        public synchronized void dispose() {
            if (disposed) {
                return;
            }
            disposed = true;
            aborted = !finished;
            frames.clear();
            runs.remove(this);
        }

        private void ensureWritable() {
            if (finished || aborted || disposed) {
                throw new IllegalStateException("run is not writable");
            }
        }
    }
}
