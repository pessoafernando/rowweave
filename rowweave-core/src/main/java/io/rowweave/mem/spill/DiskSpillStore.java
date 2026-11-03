package io.rowweave.mem.spill;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Disk SpillStore: each run becomes one temporary file in the configured directories (rowweave.io.tmp.dirs), frames separated by a 4-byte length prefix.
 * close deletes every run file, and together with SpillScope that guarantees no residue on any of the three paths: job success, failure or cancellation.
 *
 * <p>This is the backend that turns an in-memory simulation of spill into real disk writes, preventing OOM genuinely when the budget is far below the data volume.
 */
public final class DiskSpillStore implements BoundedReplayStore {

    /** A two-way merge is still allowed under a small managed-memory budget; the real buffer size matches the credit constant. */
    private static final int IO_BUFFER_BYTES = 1024;
    private static final int HARD_MAX_FRAME_BYTES = 256 * 1024 * 1024;
    private static final long READER_OBJECT_CREDIT = 512L;

    private final File dir;
    private final SpillFileAccountant accountant;
    private final SpillRunOutputFactory outputFactory;
    private int runSeq;

    // Observation probe: number of currently open FrameIterators (that is, underlying FileInputStream handles) plus the historical peak.
    // Lets a test assert that handles open at once during a merge stay at or below mergeFanIn, which protects against too many open files under a large number of small runs.
    private final java.util.concurrent.atomic.AtomicInteger openReaders =
            new java.util.concurrent.atomic.AtomicInteger();
    private volatile int peakOpenReaders;
    // Registers FrameIterators still open. A merge iterator abandoned before EOF never closes its handle by itself, so
    // close() closes each one as a backstop against an FD leak. Thread-safe set, because partition threads read concurrently.
    private final java.util.Set<DiskReader> liveReaders =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.Set<DiskRun> liveWriters =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.Set<DiskRun> runs =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private volatile boolean closed;

    public DiskSpillStore(File dir) {
        this(dir, null, FileOutputStream::new);
    }

    DiskSpillStore(File dir, SpillFileAccountant accountant) {
        this(dir, accountant, FileOutputStream::new);
    }

    DiskSpillStore(File dir, SpillFileAccountant accountant,
                   SpillRunOutputFactory outputFactory) {
        // mkdirs() may return false if another thread created it concurrently; treat existing dir as ok.
        dir.mkdirs();
        if (!dir.isDirectory()) {
            throw new UncheckedIOException(new IOException("cannot create spill dir: " + dir));
        }
        this.dir = dir;
        this.accountant = accountant;
        this.outputFactory = java.util.Objects.requireNonNull(outputFactory, "outputFactory");
    }

    @Override
    public BoundedReplayRun createRun() {
        // Reserve the run with the workspace before taking the store lock.
        // A concurrent job close can then reject this open before any file
        // exists, without a manager/store lock cycle.
        if (accountant != null) accountant.runOpened();
        boolean registered = false;
        try {
            synchronized (this) {
                if (closed) {
                    throw new IllegalStateException("spill store is closed");
                }
                File f = new File(dir, "run-" + (runSeq++) + "-"
                        + System.nanoTime() + ".spill");
                final DiskRun run;
                try {
                    run = new DiskRun(f);
                } catch (RuntimeException | Error openFailure) {
                    if (f.exists() && !f.delete()) {
                        openFailure.addSuppressed(
                                SpillDiskException.cleanupFailed(null));
                    }
                    throw openFailure;
                }
                runs.add(run);
                liveWriters.add(run);
                registered = true;
                return run;
            }
        } finally {
            if (!registered && accountant != null) accountant.runClosed();
        }
    }

    /** Spill runs created since construction (observation point: above zero proves that a spill really happened). The count survives close. */
    public synchronized int runsCreated() {
        return runSeq;
    }

    /** Observation: read handles currently open. */
    public int currentOpenReaders() {
        return openReaders.get();
    }

    /** Observation: historical peak of read handles open at once (asserted to stay at or below mergeFanIn). */
    public int peakOpenReaders() {
        return peakOpenReaders;
    }

    /** Write handles not yet finished or aborted. */
    public int currentOpenWriters() {
        return liveWriters.size();
    }

    private void onReaderOpen() {
        int cur = openReaders.incrementAndGet();
        if (cur > peakOpenReaders) {
            peakOpenReaders = cur; // Observation probe; an approximate concurrency peak is enough to assert on.
        }
    }

    private void onReaderClose() {
        openReaders.decrementAndGet();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed && runs.isEmpty()) {
                return;
            }
            closed = true;
        }
        Throwable failure = null;
        for (DiskRun run : new java.util.ArrayList<>(runs)) {
            try {
                run.dispose();
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null) {
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            throw (Error) failure;
        }
    }

    private final class DiskRun implements BoundedReplayRun {
        private final File file;
        private final java.util.Set<DiskReader> readers =
                java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
        private DataOutputStream out;
        private long chargedBytes;
        private State state = State.WRITING;

        DiskRun(File file) {
            this.file = file;
            try {
                OutputStream opened = outputFactory.open(file);
                this.out = new DataOutputStream(
                        new BufferedOutputStream(opened, IO_BUFFER_BYTES));
            } catch (IOException e) {
                throw writeFailure(e);
            }
        }

        @Override
        public synchronized void append(byte[] frame) {
            ensureWritable();
            if (frame == null) {
                throw new IllegalArgumentException("spill frame must not be null");
            }
            if (frame.length > HARD_MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("spill frame exceeds hard cap: " + frame.length);
            }
            long frameBytes = 4L + frame.length;
            if (accountant != null) {
                accountant.reserve(frameBytes);
                chargedBytes += frameBytes;
            }
            try {
                out.writeInt(frame.length);
                out.write(frame);
                if (accountant != null) {
                    try {
                        accountant.written(frameBytes);
                    } catch (Throwable ignoredTelemetryFailure) {
                        // Advisory telemetry must not change an already successful append.
                    }
                }
            } catch (IOException e) {
                throw writeFailure(e);
            }
        }

        @Override
        public synchronized void finishWriting() {
            if (state == State.FINISHED) {
                return;
            }
            ensureWritable();
            try {
                out.flush();
                out.close();
                out = null;
                state = State.FINISHED;
                liveWriters.remove(this);
            } catch (IOException e) {
                RuntimeException primary = writeFailure(e);
                try {
                    dispose();
                } catch (RuntimeException | Error cleanup) {
                    primary.addSuppressed(cleanup);
                }
                throw primary;
            }
        }

        @Override
        public synchronized Iterator<byte[]> read() {
            if (state != State.FINISHED) {
                throw new IllegalStateException("read before finishWriting");
            }
            return new FrameIterator(this, file);
        }

        @Override
        public synchronized BudgetedFrameReader openBudgetedReader(
                io.rowweave.mem.MemoryManager memoryManager,
                int fieldCount, int maximumFrameLength) {
            if (state != State.FINISHED) {
                throw new IllegalStateException("read before finishWriting");
            }
            if (maximumFrameLength < 0 || maximumFrameLength > HARD_MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("invalid maximum replay frame length: " + maximumFrameLength);
            }
            return new ManagedFrameReader(this, file, memoryManager, fieldCount, maximumFrameLength);
        }

        @Override
        public synchronized void abort() {
            if (state != State.WRITING) {
                return;
            }
            dispose();
        }

        @Override
        public synchronized void dispose() {
            if (state == State.DISPOSED) {
                return;
            }
            if (state != State.WRITING && state != State.FINISHED && state != State.DISPOSE_FAILED) {
                throw new IllegalStateException("cannot dispose run in state " + state);
            }
            state = State.DISPOSING;
            liveWriters.remove(this);
            if (out != null) {
                try {
                    out.close();
                } catch (IOException closeFailure) {
                    state = State.DISPOSE_FAILED;
                    throw new UncheckedIOException(closeFailure);
                } finally {
                    out = null;
                }
            }
            for (DiskReader reader : new java.util.ArrayList<>(readers)) {
                reader.close();
            }
            if (file.exists() && !file.delete()) {
                state = State.DISPOSE_FAILED;
                throw new UncheckedIOException(new IOException("cannot delete spill run: " + file));
            }
            if (accountant != null && chargedBytes != 0) {
                long released = chargedBytes;
                accountant.release(released);
                chargedBytes = 0;
            }
            runs.remove(this);
            if (accountant != null) accountant.runClosed();
            state = State.DISPOSED;
        }

        private void ensureWritable() {
            if (state != State.WRITING) {
                throw new IllegalStateException("run is not writable: " + state);
            }
        }
    }

    private enum State { WRITING, FINISHED, DISPOSING, DISPOSE_FAILED, DISPOSED }

    private RuntimeException writeFailure(IOException cause) {
        return accountant == null ? new UncheckedIOException(cause) : accountant.writeFailure(cause);
    }

    /**
     * Frame iterator over one run file. Closing happens automatically at EOF, which releases the handle.
     * It increments openReaders in the constructor and decrements it in close (observation probe), and implements {@link AutoCloseable} so a merge abandoned early can close explicitly to prevent a leak.
     */
    private interface DiskReader extends AutoCloseable {
        @Override
        void close();
    }

    final class FrameIterator implements Iterator<byte[]>, DiskReader {
        private final DiskRun owner;
        private final DataInputStream in;
        private byte[] next;
        private boolean done;
        private boolean closed;

        FrameIterator(DiskRun owner, File file) {
            this.owner = owner;
            try {
                this.in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(file), IO_BUFFER_BYTES));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            onReaderOpen();
            liveReaders.add(this); // Registered so that store.close() can still close the handle.
            owner.readers.add(this);
            advance();
        }

        private void advance() {
            try {
                int len = in.readInt();
                byte[] frame = new byte[len];
                in.readFully(frame);
                next = frame;
            } catch (EOFException eof) {
                next = null;
                done = true;
                closeQuietly();
            } catch (IOException e) {
                closeQuietly();
                throw new UncheckedIOException(e);
            }
        }

        private void closeQuietly() {
            if (closed) {
                return;
            }
            closed = true;
            onReaderClose();
            liveReaders.remove(this); // Deregisters.
            owner.readers.remove(this);
            try {
                in.close();
            } catch (IOException ignored) {
                // best effort
            }
        }

        /** Closes explicitly when a merge is abandoned before EOF, preventing a handle leak. Idempotent. */
        @Override
        public void close() {
            next = null;
            done = true;
            closeQuietly();
        }

        @Override
        public boolean hasNext() {
            return !done && next != null;
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] cur = next;
            advance();
            return cur;
        }
    }

    /** Reserves the largest frame a descriptor may hold at open time, then validates the length and allocates and reads, never prefetching the next frame. */
    private final class ManagedFrameReader implements BudgetedFrameReader, DiskReader {
        private final DiskRun owner;
        private final DataInputStream in;
        private final io.rowweave.mem.MemoryManager memoryManager;
        private final int fieldCount;
        private final int maximumFrameLength;
        private final long fileLength;
        private final long frameEscrowCredit;
        private final long readerCredit;
        private long consumed;
        private boolean closed;

        ManagedFrameReader(DiskRun owner, File file,
                           io.rowweave.mem.MemoryManager memoryManager,
                           int fieldCount, int maximumFrameLength) {
            this.owner = owner;
            this.memoryManager = java.util.Objects.requireNonNull(memoryManager, "memoryManager");
            if (fieldCount < 0) {
                throw new IllegalArgumentException("negative replay field count: " + fieldCount);
            }
            this.fieldCount = fieldCount;
            this.maximumFrameLength = maximumFrameLength;
            this.fileLength = file.length();
            long ioCredit = Math.addExact(READER_OBJECT_CREDIT,
                    io.rowweave.mem.RowSizeEstimator.estimateByteArray(IO_BUFFER_BYTES));
            this.frameEscrowCredit = Math.addExact(
                    io.rowweave.mem.RowSizeEstimator.estimateByteArray(maximumFrameLength),
                    io.rowweave.mem.RowSizeEstimator.estimateEncodedUpperBound(
                            fieldCount, maximumFrameLength));
            this.readerCredit = Math.addExact(ioCredit, frameEscrowCredit);
            if (!memoryManager.tryReserve(readerCredit)) {
                throw new io.rowweave.mem.MemoryBudgetExceededException(
                        "cannot reserve bounded replay reader credit=" + readerCredit
                                + " maximumFrameLength=" + maximumFrameLength);
            }
            DataInputStream opened = null;
            try {
                opened = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(file), IO_BUFFER_BYTES));
            } catch (IOException failure) {
                memoryManager.releaseExact(readerCredit);
                throw new UncheckedIOException(failure);
            } catch (RuntimeException | Error failure) {
                memoryManager.releaseExact(readerCredit);
                throw failure;
            }
            this.in = opened;
            onReaderOpen();
            liveReaders.add(this);
            owner.readers.add(this);
        }

        @Override
        public byte[] nextFrame() {
            ensureOpen();
            if (consumed == fileLength) {
                close();
                return null;
            }
            if (fileLength - consumed < Integer.BYTES) {
                throw fail(new UncheckedIOException(new IOException(
                        "truncated replay frame header: remaining=" + (fileLength - consumed))));
            }
            final int length;
            try {
                length = in.readInt();
                consumed += Integer.BYTES;
            } catch (IOException failure) {
                throw fail(new UncheckedIOException(failure));
            }
            long remaining = fileLength - consumed;
            if (length < 0 || length > maximumFrameLength || length > HARD_MAX_FRAME_BYTES
                    || (long) length > remaining) {
                throw fail(new IllegalArgumentException("invalid replay frame length=" + length
                        + " max=" + maximumFrameLength + " remaining=" + remaining));
            }
            long credit = Math.addExact(
                    io.rowweave.mem.RowSizeEstimator.estimateByteArray(length),
                    io.rowweave.mem.RowSizeEstimator.estimateEncodedUpperBound(fieldCount, length));
            if (credit > frameEscrowCredit) {
                throw fail(new IllegalStateException(
                        "replay frame credit exceeds descriptor escrow: credit=" + credit
                                + " escrow=" + frameEscrowCredit));
            }
            try {
                byte[] frame = new byte[length];
                in.readFully(frame);
                consumed += length;
                return frame;
            } catch (IOException | RuntimeException | Error failure) {
                if (failure instanceof IOException) {
                    throw fail(new UncheckedIOException((IOException) failure));
                }
                throw fail(failure);
            }
        }

        private RuntimeException fail(Throwable failure) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            if (failure instanceof RuntimeException) return (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
            return new RuntimeException(failure);
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("bounded replay reader is closed");
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            Throwable failure = null;
            liveReaders.remove(this);
            owner.readers.remove(this);
            onReaderClose();
            try {
                in.close();
            } catch (IOException cleanup) {
                if (failure == null) failure = new UncheckedIOException(cleanup);
                else failure.addSuppressed(cleanup);
            }
            try {
                memoryManager.releaseExact(readerCredit);
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
        }
    }
}
