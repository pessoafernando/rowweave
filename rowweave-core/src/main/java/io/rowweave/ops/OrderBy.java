package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.HeapSortBuffer;
import io.rowweave.mem.CoordinatedExternalSortBuffer;
import io.rowweave.mem.SortBuffer;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.BiFunction;

/**
 * The total-sort operator (a breaker, pure push). The parallel form is a local sort per partition (SortBuffer) plus a k-way merge across partitions.
 *
 * <p><b>The pure Push contract</b>: the inlet is {@link RowStream} and the outlet is {@link RowSink}, and {@code applyStream} is the
 * <b>only outlet</b> (there is no {@code apply/applyParallel} returning a List). Rows go straight into N SortBuffer work sets (spillable) with putRow,
 * <b>never through an intermediate {@code List<List<DataRow>>}</b> - any full materialization violates the pure Push contract.
 *
 * <p>Parallelism independence: a synthetic global seq column is appended to the sort key as an ASC tie-breaker, which makes the comparison a total order, so local sort
 * plus merge gives a result independent of parallelism and bit-identical to one stable sort at parallelism 1. The synthetic column is stripped once the merge completes.
 *
 * <p>Null sort semantics are pinned by {@link RowComparator} (numeric null equals 0, string null is smallest, NULLS FIRST/LAST configurable).
 */
public final class OrderBy {

    private final DataSchema dataSchema;
    private final SortField[] sortFields;
    private BiFunction<DataSchema, RowComparator, SortBuffer> sortBufferFactory = HeapSortBuffer::new;

    private OrderBy(DataSchema dataSchema, SortField[] sortFields) {
        this.dataSchema = dataSchema;
        this.sortFields = sortFields;
    }

    public static OrderBy of(DataSchema dataSchema, SortField... sortFields) {
        if (sortFields.length == 0) {
            throw new IllegalArgumentException("OrderBy requires at least one SortField");
        }
        return new OrderBy(dataSchema, sortFields.clone());
    }

    /** The sort fields (for describable extraction; round-trips with the constructor arguments, excluding the __seq that applyStream adds internally). */
    public SortField[] sortFields() {
        return sortFields.clone();
    }

    /**
     * Injects the SortBuffer factory: on-heap by default, and a segmented plus spilling implementation can be swapped in with no change to the operator.
     * The factory takes (augmented DataSchema, augmented comparator) and returns the sort buffer.
     */
    public OrderBy withSortBufferFactory(BiFunction<DataSchema, RowComparator, SortBuffer> factory) {
        this.sortBufferFactory = factory;
        return this;
    }

    /**
     * The only outlet (the pure Push contract): the breaker pulls upstream until null (finishing it), sorts locally per partition, and after a lazy k-way merge
     * emits rows one by one with emitRow, closing with endOfStream. Control events: an Error or Cancel mid-stream is passed to out and the operator terminates at once (it neither sorts
     * nor emits data, which is the breaker's finish-then-emit semantics). Memory holds only the SortBuffer work set (spillable) plus the k head rows of the merge heap.
     */
    public void applyStream(RowStream upstream, int parallelism, boolean parallel, RowSink out) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0");
        }
        int seqIndex = dataSchema.getFieldCount();
        DataSchema augMeta = augmentedMeta(seqIndex);
        RowComparator augCmp = augmentedComparator(seqIndex);

        SortBuffer[] buffers = new SortBuffer[parallelism];
        int created = 0;
        Throwable primary = null;
        try {
            // N SortBuffer work sets, each spillable. Rows are routed straight to putRow by global seq, never through an intermediate List<List<DataRow>>.
            for (; created < parallelism; created++) {
                buffers[created] = sortBufferFactory.apply(augMeta, augCmp);
            }

            long seq = 0;
            StreamEvent e;
            while ((e = upstream.next()) != null) {
                if (!e.isRecord()) {
                    if (e.isError() || e.isCancel()) {
                        out.emit(e); // pass the control event through; the breaker emits nothing further
                        return;
                    }
                    continue; // Barrier or anything else: absorbed (digested inside the breaker)
                }
                DataRow row = e.row();
                if (row.size() != seqIndex) {
                    throw new IllegalStateException("OrderBy input row width " + row.size()
                            + " != DataSchema field count " + seqIndex);
                }
                buffers[(int) (seq % parallelism)].putRow(augment(row, seqIndex, seq));
                seq++;
                coordinateExternalization(buffers, created);
            }

            coordinateExternalization(buffers, created); // defensive pre-sort barrier
            List<SortBuffer> sorted = io.rowweave.mem.SlotGuard.cpuPhase(
                    () -> parallel ? sortParallel(buffers) : sortSequential(buffers));
            Iterator<DataRow> merged = kWayMerge(sorted, augCmp);
            while (merged.hasNext()) {
                out.emitRow(strip(merged.next(), seqIndex));
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            cancelBuffers(buffers, created, failure);
            throw failure;
        } finally {
            closeBuffers(buffers, created, primary);
        }
    }

    private void coordinateExternalization(SortBuffer[] buffers, int count) {
        boolean anyExternal = false;
        for (int i = 0; i < count; i++) {
            if (buffers[i] instanceof CoordinatedExternalSortBuffer
                    && ((CoordinatedExternalSortBuffer) buffers[i]).externalized()) {
                anyExternal = true;
                break;
            }
        }
        if (!anyExternal) {
            return;
        }
        for (int i = 0; i < count; i++) {
            if (!(buffers[i] instanceof CoordinatedExternalSortBuffer)) {
                throw new IllegalStateException("mixed coordinated/non-coordinated SortBuffer factory at partition " + i);
            }
            ((CoordinatedExternalSortBuffer) buffers[i]).externalizeFromEscrow();
        }
    }

    private List<SortBuffer> sortSequential(SortBuffer[] buffers) {
        List<SortBuffer> sorted = new ArrayList<>(buffers.length);
        for (SortBuffer buf : buffers) {
            buf.sort();
            sorted.add(buf);
        }
        return sorted;
    }

    private List<SortBuffer> sortParallel(SortBuffer[] buffers) {
        Thread[] workers = new Thread[buffers.length];
        java.util.concurrent.atomic.AtomicReferenceArray<Throwable> originals =
                new java.util.concurrent.atomic.AtomicReferenceArray<>(buffers.length);
        java.util.concurrent.atomic.AtomicReferenceArray<Throwable> induced =
                new java.util.concurrent.atomic.AtomicReferenceArray<>(buffers.length);
        java.util.concurrent.atomic.AtomicIntegerArray cancelRequested =
                new java.util.concurrent.atomic.AtomicIntegerArray(buffers.length);
        java.util.concurrent.atomic.AtomicReference<Throwable> cancellationTrigger =
                new java.util.concurrent.atomic.AtomicReference<>();
        for (int i = 0; i < buffers.length; i++) {
            final int index = i;
            final SortBuffer buf = buffers[i];
            workers[i] = new Thread(() -> {
                try {
                    buf.sort();
                } catch (Throwable failure) {
                    if (cancelRequested.get(index) == 0) {
                        originals.compareAndSet(index, null, failure);
                        if (cancellationTrigger.compareAndSet(null, failure)) {
                            requestParallelCancel(buffers, workers, cancelRequested, index, failure);
                        }
                    } else {
                        induced.compareAndSet(index, null, failure);
                    }
                }
            }, "rowweave-sort-part-" + i);
            workers[i].setDaemon(true);
        }
        for (Thread w : workers) {
            w.start();
        }
        RuntimeException callerInterrupt = null;
        for (Thread worker : workers) {
            while (worker.isAlive()) {
                try {
                    worker.join();
                } catch (InterruptedException interrupted) {
                    if (callerInterrupt == null) {
                        callerInterrupt = new RuntimeException("parallel sort interrupted", interrupted);
                        requestParallelCancel(buffers, workers, cancelRequested, -1, callerInterrupt);
                    }
                }
            }
        }
        if (callerInterrupt != null) {
            Thread.currentThread().interrupt();
            addParallelSuppressed(callerInterrupt, originals, induced);
            throw callerInterrupt;
        }
        Throwable root = null;
        for (int i = 0; i < buffers.length; i++) {
            if (originals.get(i) != null) {
                root = originals.get(i);
                break;
            }
        }
        if (root != null) {
            addParallelSuppressed(root, originals, induced);
            throwUnchecked(root);
        }
        return new ArrayList<>(java.util.Arrays.asList(buffers));
    }

    private static void requestParallelCancel(SortBuffer[] buffers, Thread[] workers,
                                              java.util.concurrent.atomic.AtomicIntegerArray requested,
                                              int source, Throwable cause) {
        for (int i = 0; i < buffers.length; i++) {
            if (i == source) {
                continue;
            }
            requested.set(i, 1);
            try {
                buffers[i].cancel(cause);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != cause) {
                    cause.addSuppressed(cleanup);
                }
            }
            Thread worker = workers[i];
            if (worker != null && worker != Thread.currentThread()) {
                worker.interrupt();
            }
        }
    }

    private static void addParallelSuppressed(Throwable root,
                                              java.util.concurrent.atomic.AtomicReferenceArray<Throwable> originals,
                                              java.util.concurrent.atomic.AtomicReferenceArray<Throwable> induced) {
        for (int i = 0; i < originals.length(); i++) {
            Throwable original = originals.get(i);
            if (original != null && original != root) {
                root.addSuppressed(original);
            }
            Throwable cancellation = induced.get(i);
            if (cancellation != null && cancellation != root && cancellation != original) {
                root.addSuppressed(cancellation);
            }
        }
    }

    private static void cancelBuffers(SortBuffer[] buffers, int count, Throwable cause) {
        for (int i = 0; i < count; i++) {
            try {
                buffers[i].cancel(cause);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != cause) {
                    cause.addSuppressed(cleanup);
                }
            }
        }
    }

    private static void closeBuffers(SortBuffer[] buffers, int count, Throwable primary) {
        Throwable cleanupFailure = null;
        for (int i = 0; i < count; i++) {
            try {
                buffers[i].close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanupFailure == null) {
                    cleanupFailure = cleanup;
                } else if (cleanupFailure != cleanup) {
                    cleanupFailure.addSuppressed(cleanup);
                }
            }
        }
        if (cleanupFailure == null) {
            return;
        }
        if (primary != null) {
            if (primary != cleanupFailure) {
                primary.addSuppressed(cleanupFailure);
            }
            return;
        }
        throwUnchecked(cleanupFailure);
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    private DataSchema augmentedMeta(int seqIndex) {
        Field[] fields = new Field[seqIndex + 1];
        for (int i = 0; i < seqIndex; i++) {
            fields[i] = dataSchema.getField(i);
        }
        fields[seqIndex] = new Field("__seq", DataType.LongType);
        return new DataSchema(fields);
    }

    private RowComparator augmentedComparator(int seqIndex) {
        SortField[] aug = new SortField[sortFields.length + 1];
        System.arraycopy(sortFields, 0, aug, 0, sortFields.length);
        aug[sortFields.length] = new SortField(seqIndex, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        return new RowComparator(augmentedMeta(seqIndex), aug);
    }

    private DataRow augment(DataRow r, int seqIndex, long seq) {
        DataRow aug = new DataRow(seqIndex + 1);
        for (int i = 0; i < seqIndex; i++) {
            aug.set(i, r.get(i));
        }
        aug.set(seqIndex, seq);
        return aug;
    }

    private DataRow strip(DataRow aug, int seqIndex) {
        DataRow r = new DataRow(seqIndex);
        for (int i = 0; i < seqIndex; i++) {
            r.set(i, aug.get(i));
        }
        return r;
    }

    /** The lazy k-way merge: a heap drives it, and each next() pops the smallest head row and advances its cursor - nothing is materialized in full. */
    private Iterator<DataRow> kWayMerge(List<SortBuffer> sorted, RowComparator cmp) {
        PriorityQueue<Cursor> heap = new PriorityQueue<>((x, y) -> cmp.compare(x.head, y.head));
        for (SortBuffer buf : sorted) {
            Iterator<DataRow> it = buf.iterator();
            if (it.hasNext()) {
                heap.add(new Cursor(it, it.next()));
            }
        }
        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                return !heap.isEmpty();
            }

            @Override
            public DataRow next() {
                Cursor c = heap.poll();
                DataRow head = c.head;
                if (c.it.hasNext()) {
                    c.head = c.it.next();
                    heap.add(c);
                }
                return head;
            }
        };
    }

    private static final class Cursor {
        final Iterator<DataRow> it;
        DataRow head;

        Cursor(Iterator<DataRow> it, DataRow head) {
            this.it = it;
            this.head = head;
        }
    }
}
