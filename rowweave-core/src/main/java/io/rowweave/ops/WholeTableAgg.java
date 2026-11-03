package io.rowweave.ops;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

/**
 * The whole-table aggregation operator (no groupBy; a breaker, pure push): it computes a single scalar over the entire table (count, count(field), sum, max or min) and produces one row [value].
 *
 * <p><b>The pure Push contract</b>: the only outlet is {@code apply(RowStream, RowSink)}; the breaker finishes upstream and then emits one row
 * (it returns no List). It is describable (kind plus a field parameter, so a Worker can rebuild it) and can therefore be sent to a cluster.
 *
 * <p>Contract-sensitive points: sum goes through {@link SumAggregator} (full precision throughout, scale 10 HALF_UP, null skipped, an empty table or all-null input yields null);
 * max/min ignore null (an empty table or all-null input yields null); count() is the full row count and count(field) counts non-null values (an empty table yields 0).
 */
public final class WholeTableAgg {

    public enum Kind { COUNT_ALL, COUNT_FIELD, SUM, MAX, MIN }

    private final Kind kind;
    private final int fieldIndex; // ignored for COUNT_ALL

    private WholeTableAgg(Kind kind, int fieldIndex) {
        this.kind = kind;
        this.fieldIndex = fieldIndex;
    }

    /** Builds from a kind and a field name (null for COUNT_ALL); the field name is resolved to an index through childMeta. */
    public static WholeTableAgg of(DataSchema childMeta, Kind kind, String field) {
        int idx = -1;
        if (kind != Kind.COUNT_ALL) {
            idx = childMeta.getFieldIndex(field);
            if (idx < 0) {
                throw new IllegalArgumentException("unknown field for whole-table agg: " + field);
            }
        }
        return new WholeTableAgg(kind, idx);
    }

    /**
     * The only outlet (the pure Push contract): the breaker finishes upstream (each row feeds the accumulator), emits the whole-table scalar as one row with emitRow, and closes with endOfStream.
     * Control events: Error and Cancel pass through and terminate (nothing is produced); a Barrier is absorbed.
     */
    public void apply(RowStream upstream, RowSink out) {
        long countAll = 0;
        long countField = 0;
        SumAggregator sum = new SumAggregator();
        MaxAggregator max = new MaxAggregator();
        MinAggregator min = new MinAggregator();

        StreamEvent e;
        while ((e = upstream.next()) != null) {
            if (!e.isRecord()) {
                if (e.isError() || e.isCancel()) {
                    out.emit(e);
                    return;
                }
                continue; // Barrier: absorbed
            }
            DataRow r = e.row();
            switch (kind) {
                case COUNT_ALL:
                    countAll++;
                    break;
                case COUNT_FIELD:
                    if (r.get(fieldIndex) != null) {
                        countField++;
                    }
                    break;
                case SUM:
                    sum.add(r.getBigDecimal(fieldIndex)); // null is skipped
                    break;
                case MAX:
                    max.add(r.get(fieldIndex)); // null is ignored
                    break;
                case MIN:
                default:
                    min.add(r.get(fieldIndex));
                    break;
            }
        }

        Object value;
        switch (kind) {
            case COUNT_ALL: value = countAll; break;
            case COUNT_FIELD: value = countField; break;
            case SUM: value = sum.result(); break;
            case MAX: value = max.result(); break;
            case MIN:
            default: value = min.result(); break;
        }
        DataRow outRow = new DataRow(1);
        outRow.set(0, value);
        out.emitRow(outRow);
        out.endOfStream();
    }
}
