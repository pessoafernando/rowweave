package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.mem.spill.EqualKeyBlock;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Iterator;

/**
 * Contiguous-slice map-side pre-aggregation (a breaker, pure push). The combineGroup/combineReduceGroup semantics:
 * drain the input in one pass into a spill-backed, re-readable {@link EqualKeyBlock} (spilling as soon as either the row count or the byte budget is met), count N rows, then split N into
 * <b>contiguous row ranges</b> by {@code parallelism} (the ranges follow source row order rather than a key hash, so a combine function that is not commutative stays safe),
 * and feed each range's skip/take view to the user's {@code combine}; combineReduce then hands every combine result to {@code reduce} to produce the final output.
 *
 * <p><b>Correctness contract</b>: a contiguous-slice result depends on the row order and on the slice count (=parallelism). Locally the source order is deterministic,
 * and a cluster result is bit-identical to the local one <b>only when the upstream chain is a single instance</b> (one connection, FIFO order preserved); with fan-in from several
 * instances the interleaving order is not deterministic, so the intermediate combine state is not either - that is the map-side pre-aggregation contract. The standard usage follows with an order-insensitive reduce or groupBy that merges to a deterministic result. Both paths pin the slice count to the same parallelism.
 *
 * <p>The combine partial results of combineReduce also go through an EqualKeyBlock (spill as the floor), so a function that does not shrink cannot exhaust the heap.
 */
public final class CombineGroup {

    private static final int BLOCK_THRESHOLD = 1024; // a row-count safety threshold; production is also bounded by the managed-memory byte budget

    private final DataSchema dataSchema;
    private final GroupCombineFunction combineFn;      // used by combineGroup (reduceFn=null)
    private final GroupCombineReduceFunction combineReduceFn; // used by combineReduceGroup
    private MemoryManager memoryManager;
    private SpillStore spillStore = new MemorySpillStore();
    private boolean invoked;

    private CombineGroup(DataSchema dataSchema, GroupCombineFunction combineFn, GroupCombineReduceFunction combineReduceFn) {
        this.dataSchema = dataSchema;
        this.combineFn = combineFn;
        this.combineReduceFn = combineReduceFn;
    }

    /** combineGroup: map-side pre-aggregation only (the output structure matches the input, or comes from fn.getResultSchema()). */
    public static CombineGroup of(DataSchema dataSchema, GroupCombineFunction fn) {
        return new CombineGroup(dataSchema, fn, null);
    }

    /** combineReduceGroup: combine pre-aggregation plus the final reduce. */
    public static CombineGroup ofCombineReduce(DataSchema dataSchema, GroupCombineReduceFunction fn) {
        return new CombineGroup(dataSchema, null, fn);
    }

    /** Injects the spill floor (a Worker uses the engine-managed store). */
    public CombineGroup withSpill(SpillStore store) {
        this.spillStore = store;
        return this;
    }

    /** The production path also injects the operator's managed memory, so the block is bounded by bytes. */
    public CombineGroup withSpill(MemoryManager memoryManager, SpillStore store) {
        this.memoryManager = memoryManager;
        this.spillStore = store;
        return this;
    }

    /** The combineGroup output meta: fn.getResultSchema() when non-null, otherwise the input carries over. */
    public DataSchema getCombineOutputSchema() {
        DataSchema r = combineFn.getResultSchema();
        return r != null ? r : dataSchema;
    }

    /**
     * The only outlet (the pure Push contract): the breaker drains upstream completely into an EqualKeyBlock (with spill), splits it into parallelism contiguous ranges and feeds combine;
     * combineReduce then hands the combine results to reduce. Rows are emitted one by one with emitRow, and EOS closes the stream. An Error or Cancel control event passes through.
     */
    public void applyStream(RowStream upstream, int parallelism, RowSink out) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0");
        }
        ensureSingleInvocation();
        EqualKeyBlock block = new EqualKeyBlock(dataSchema, spillStore, BLOCK_THRESHOLD, memoryManager);
        Throwable primary = null;
        try {
            long n = 0;
            StreamEvent e;
            while ((e = upstream.next()) != null) {
                if (!e.isRecord()) {
                    if (e.isError() || e.isCancel()) {
                        out.emit(e);
                        return;
                    }
                    continue; // Barrier: absorbed
                }
                block.add(e.row());
                n++;
            }
            block.finish();

            if (combineReduceFn != null) {
                applyCombineReduce(block, n, parallelism, out);
            } else {
                applyCombineOnly(block, n, parallelism, out::emitRow);
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, block, spillStore);
        }
    }

    /** combineGroup: call combine on each contiguous range and emit the results directly. */
    private void applyCombineOnly(EqualKeyBlock block, long n, int parallelism, Collector out) {
        forEachShard(block, n, parallelism, shard -> combineFn.combine(shard, out));
    }

    /** combineReduceGroup: combine per range, collect the partials into a second spillable block, then reduce them once into the final output. */
    private void applyCombineReduce(EqualKeyBlock block, long n, int parallelism, RowSink out) {
        DataSchema combineMeta = dataSchema; // the combine output structure matches the input (the GroupCombineReduceFunction contract)
        EqualKeyBlock partials = new EqualKeyBlock(
                combineMeta, spillStore, BLOCK_THRESHOLD, memoryManager);
        Throwable primary = null;
        try {
            Collector collectPartial = partials::add;
            forEachShard(block, n, parallelism, shard -> combineReduceFn.combine(shard, collectPartial));
            partials.finish();
            combineReduceFn.reduce(partials::iterator, out::emitRow);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, partials);
        }
    }

    private synchronized void ensureSingleInvocation() {
        if (invoked) {
            throw new IllegalStateException("CombineGroup instances are single-use");
        }
        invoked = true;
    }

    /** Split n rows into parallelism contiguous ranges (base plus a distributed remainder) and hand each range's skip/take view to body. */
    private void forEachShard(EqualKeyBlock block, long n, int parallelism, java.util.function.Consumer<Iterable<DataRow>> body) {
        long base = n / parallelism;
        long rem = n % parallelism;
        long start = 0;
        for (int i = 0; i < parallelism; i++) {
            long size = base + (i < rem ? 1 : 0);
            if (size == 0) {
                continue;
            }
            final long from = start;
            final long len = size;
            body.accept(() -> rangeIterator(block, from, len)); // a re-readable range view (a function may traverse it more than once)
            start += size;
        }
    }

    /** An iterator over rows [from, from+len) of the block (skip from, take len); EqualKeyBlock.iterator keeps insertion order and is re-readable. */
    private static Iterator<DataRow> rangeIterator(EqualKeyBlock block, long from, long len) {
        Iterator<DataRow> it = block.iterator();
        for (long i = 0; i < from && it.hasNext(); i++) {
            it.next(); // skip
        }
        return new Iterator<DataRow>() {
            private long taken = 0;

            @Override
            public boolean hasNext() {
                return taken < len && it.hasNext();
            }

            @Override
            public DataRow next() {
                taken++;
                return it.next();
            }
        };
    }
}
