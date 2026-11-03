package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.Field;
import io.rowweave.MapPartitionEmissionLimit;
import io.rowweave.MapPartitionFunction;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.concurrent.CancellationException;

/** One streaming invocation over the rows assigned to one physical Batch task. */
public final class MapPartition {
    private final MapPartitionFunction function;
    private final MapPartitionEmissionLimit limit;
    private final RowSerializer outputSerializer;

    private MapPartition(
            MapPartitionFunction function, DataSchema outputMeta,
            MapPartitionEmissionLimit limit) {
        if (function == null || outputMeta == null || limit == null) {
            throw new IllegalArgumentException(
                    "map-partition function, output schema, and limits are required");
        }
        if (!sameSchema(outputMeta, function.getResultSchema())) {
            throw new IllegalArgumentException(
                    "map-partition result schema does not match its plan schema");
        }
        this.function = function;
        this.limit = limit;
        this.outputSerializer = new RowSerializer(outputMeta);
    }

    public static MapPartition of(
            MapPartitionFunction function, DataSchema outputMeta,
            MapPartitionEmissionLimit limit) {
        return new MapPartition(function, outputMeta, limit);
    }

    /**
     * Invokes the callback exactly once, including for an empty input. Rows are
     * pulled lazily and emissions are pushed after validation, without a row
     * collection or partition-sized work set.
     */
    public void apply(RowStream input, RowSink output) {
        if (input == null || output == null) {
            throw new IllegalArgumentException(
                    "map-partition input and output are required");
        }
        GuardedRows rows = new GuardedRows(input, output);
        GuardedCollector collector = new GuardedCollector(output);
        try {
            function.mapPartition(rows, collector);
            if (rows.terminalFailure != null) {
                throw rows.terminalFailure;
            }
        } finally {
            rows.active = false;
            collector.active = false;
        }

        if (rows.terminal) {
            output.endOfStream();
            return;
        }
        drainRemaining(input, output);
    }

    private static void drainRemaining(RowStream input, RowSink output) {
        StreamEvent event;
        while ((event = input.next()) != null) {
            if (event.isRecord()) {
                continue;
            }
            output.emit(event);
            if (event.isError() || event.isCancel()
                    || event.isEndOfStream()) {
                return;
            }
        }
        output.endOfStream();
    }

    private final class GuardedCollector implements Collector {
        private final RowSink output;
        private boolean active = true;
        private long emittedRows;
        private long emittedBytes;

        private GuardedCollector(RowSink output) {
            this.output = output;
        }

        @Override
        public void collect(DataRow row) {
            requireActive(active, "collector");
            if (row == null) {
                throw new IllegalArgumentException(
                        "map-partition output row must not be null");
            }
            int rowBytes = outputSerializer.serializedSize(row);
            if (emittedRows >= limit.maxOutputRowsPerPartition()) {
                throw new IllegalStateException(
                        "MAP_PARTITION_OUTPUT_ROWS_LIMIT_EXCEEDED");
            }
            if (emittedBytes > limit.maxOutputBytesPerPartition() - rowBytes) {
                throw new IllegalStateException(
                        "MAP_PARTITION_OUTPUT_BYTES_LIMIT_EXCEEDED");
            }
            output.emitRow(row);
            emittedRows++;
            emittedBytes += rowBytes;
        }
    }

    private static final class GuardedRows implements PartitionDataRowCursor {
        private final RowStream input;
        private final RowSink output;
        private boolean active = true;
        private boolean terminal;
        private RuntimeException terminalFailure;

        private GuardedRows(RowStream input, RowSink output) {
            this.input = input;
            this.output = output;
        }

        @Override
        public DataRow next() {
            requireActive(active, "cursor");
            if (terminalFailure != null) throw terminalFailure;
            if (terminal) return null;
            StreamEvent event;
            while ((event = input.next()) != null) {
                if (event.isRecord()) return event.row();
                if (event.isError()) {
                    terminal = true;
                    terminalFailure = propagate(event.cause());
                    throw terminalFailure;
                }
                if (event.isCancel()) {
                    terminal = true;
                    terminalFailure = new CancellationException(
                            "map-partition upstream was cancelled");
                    throw terminalFailure;
                }
                if (event.isEndOfStream()) {
                    terminal = true;
                    return null;
                }
                output.emit(event);
            }
            terminal = true;
            return null;
        }
    }

    private static RuntimeException propagate(Throwable cause) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException("map-partition upstream failed", cause);
    }

    private static void requireActive(boolean active, String endpoint) {
        if (!active) {
            throw new IllegalStateException(
                    "map-partition " + endpoint
                            + " is no longer valid after callback return");
        }
    }

    private static boolean sameSchema(DataSchema expected, DataSchema actual) {
        if (actual == null
                || expected.getFieldCount() != actual.getFieldCount()) {
            return false;
        }
        for (int index = 0; index < expected.getFieldCount(); index++) {
            Field left = expected.getField(index);
            Field right = actual.getField(index);
            if (right == null || !left.getName().equals(right.getName())
                    || !left.getAlias().equals(right.getAlias())
                    || left.getDataType() != right.getDataType()) {
                return false;
            }
        }
        return true;
    }
}
