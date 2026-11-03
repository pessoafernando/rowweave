package io.rowweave;

import io.rowweave.mem.RowSizeEstimator;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A Sink that collects result rows into an in-memory list, for reading a result back inside the session.
 *
 * <p>A Sink must be serializable. This class collects into client memory and is <b>local only</b>: on a cluster a Sink
 * runs on a Worker and never sends rows back to the client, so cluster results are written to a FileSink and read from there. A real
 * cluster submission rejects this Sink before execution, which avoids the silent loss where the Worker copy collects successfully while the original client object stays empty.
 */
public final class CollectingSink implements Sink {

    private static final long serialVersionUID = 1L;
    private static final String LIMIT_OWNER = "CollectingSink";
    private static final DataRow[] EMPTY_ROWS = new DataRow[0];

    private final DataSchema dataSchema;
    private final CompatibilityDataLimits limits;
    private DataRow[] rows = EMPTY_ROWS;
    private int size;
    private long residentRowBytes;
    private transient int coordinatedStartSize = -1;
    private transient long coordinatedStartRowBytes;

    public CollectingSink(DataSchema dataSchema) {
        this(dataSchema, CompatibilityDataLimits.defaults());
    }

    /** Builds a local-only results collector guarded by both a row count and a resident-bytes fence. */
    public CollectingSink(DataSchema dataSchema, CompatibilityDataLimits limits) {
        this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema");
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public void collect(DataRow row) {
        if (coordinatedStartSize < 0) {
            coordinatedStartSize = size;
            coordinatedStartRowBytes = residentRowBytes;
        }

        long rowBytes = RowSizeEstimator.estimateRow(
                Objects.requireNonNull(row, "row"));
        long currentArrayBytes = ownedArrayBytes(rows.length);
        long currentBytes = Math.addExact(residentRowBytes, currentArrayBytes);
        long admittedBytes = limits.requireAdditional(
                size, currentBytes, rowBytes, LIMIT_OWNER);

        int nextCapacity = rows.length;
        if (size == rows.length) {
            nextCapacity = growCapacity(size + 1);
            long growthBytes = ownedArrayBytes(nextCapacity) - currentArrayBytes;
            admittedBytes = requireBytes(Math.addExact(admittedBytes, growthBytes));
        }

        if (nextCapacity != rows.length) {
            rows = Arrays.copyOf(rows, nextCapacity);
        }
        rows[size++] = row;
        residentRowBytes = Math.addExact(residentRowBytes, rowBytes);
    }

    @Override
    public void close() {
        coordinatedStartSize = -1;
        coordinatedStartRowBytes = 0L;
    }

    @Override
    public void abort() {
        rollbackCommit();
    }

    @Override
    public boolean supportsCoordinatedCommit() {
        return true;
    }

    @Override
    public void prepareCommit() {
        if (coordinatedStartSize < 0) {
            coordinatedStartSize = size;
            coordinatedStartRowBytes = residentRowBytes;
        }
    }

    @Override
    public void commitPrepared() {
        // Until ExecutionEnvironment.commit returns synchronously, the in-memory result publishes no transaction visibility to external threads.
    }

    @Override
    public void rollbackCommit() {
        if (coordinatedStartSize >= 0) {
            Arrays.fill(rows, coordinatedStartSize, size, null);
            size = coordinatedStartSize;
            residentRowBytes = coordinatedStartRowBytes;
            coordinatedStartSize = -1;
            coordinatedStartRowBytes = 0L;
        }
    }

    @Override
    public void finalizeCommit() {
        coordinatedStartSize = -1;
        coordinatedStartRowBytes = 0L;
    }

    public List<DataRow> getRows() {
        return Collections.unmodifiableList(new AbstractList<DataRow>() {
            @Override
            public DataRow get(int index) {
                if (index < 0 || index >= size) {
                    throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
                }
                return rows[index];
            }

            @Override
            public int size() {
                return size;
            }
        });
    }

    private int growCapacity(int minimum) {
        int grown = rows.length == 0
                ? Math.min(8, limits.maxRows())
                : rows.length + (rows.length >>> 1) + 1;
        return Math.min(limits.maxRows(), Math.max(minimum, grown));
    }

    private long requireBytes(long observedBytes) {
        if (observedBytes > limits.maxBytes()) {
            throw new CompatibilityDataLimitException(
                    CompatibilityDataLimitException.Kind.BYTES,
                    limits.maxBytes(), observedBytes, LIMIT_OWNER);
        }
        return observedBytes;
    }

    private static long ownedArrayBytes(int capacity) {
        return capacity == 0 ? 0L : RowSizeEstimator.estimateReferenceArray(capacity);
    }
}
