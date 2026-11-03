package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.GroupAggBuffer;
import io.rowweave.mem.HeapGroupAggBuffer;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Group aggregation (a breaker, pure push). Rows are grouped by key, each group accumulates sum/count/max/min, and groups are emitted with emitRow.
 *
 * <p><b>The pure Push contract</b>: the only outlet is {@code aggregate(RowStream, RowSink)} (there is no apply, applySalted,
 * applyShuffled or any other outlet returning a List). One {@link GroupAggBuffer} aggregates directly (spill is handled inside the buffer), with no parallel partitioning
 * (parallelism plus N combine buffers plus a merge belongs to the cluster path, not to this operator).
 *
 * <p>The correctness ground: SumAggregator accumulates at full precision throughout and rounds once before output (scale 10 HALF_UP), independent of order. null participates in
 * grouping (a null key forms its own group); count(*) counts every row including nulls while countField counts only non-null ones; max/min ignore null and return null for an empty group.
 *
 * <p>Expression aggregation (sumExpr/countExpr) is wired to the expr engine by the caller at the graph layer through {@code DataSet}; this class takes field names only.
 *
 * <p>Output row shape = [key fields..., aggregate results...]. The heap/no-spill path usually keeps first-seen group order, while the spill path only guarantees determinism.
 * Anything outside that depends on group order must state an explicit {@code orderBy}.
 */
public final class ParallelGroupByAgg {

    private enum Kind { SUM, COUNT, MAX, MIN }

    private static final class AggSpec {
        final Kind kind;
        final int measureIndex; // -1 = count(*)

        AggSpec(Kind kind, int measureIndex) {
            this.kind = kind;
            this.measureIndex = measureIndex;
        }
    }

    private final DataSchema dataSchema;
    private final int[] keyIndices;
    private final DataSchema keyMeta;
    private final List<AggSpec> aggs = new ArrayList<>();
    private Function<Supplier<Object[]>, GroupAggBuffer> bufferFactory = HeapGroupAggBuffer::new;

    private ParallelGroupByAgg(DataSchema dataSchema, int[] keyIndices) {
        this.dataSchema = dataSchema;
        this.keyIndices = keyIndices;
        Field[] keyFieldDefs = new Field[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            keyFieldDefs[i] = dataSchema.getField(keyIndices[i]);
        }
        this.keyMeta = new DataSchema(keyFieldDefs);
    }

    public static ParallelGroupByAgg of(DataSchema dataSchema, String... keyFields) {
        int[] idx = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            int found = dataSchema.getFieldIndex(keyFields[i]);
            if (found < 0) {
                throw new IllegalArgumentException("Unknown key field: " + keyFields[i]);
            }
            idx[i] = found;
        }
        return new ParallelGroupByAgg(dataSchema, idx);
    }

    /**
     * Injects the GroupAggBuffer factory: on-heap by default, and a segmented plus spilling implementation can be swapped in with no change to the operator.
     * The factory takes an accumulator-array factory and returns the keyed accumulator table.
     */
    public ParallelGroupByAgg withBufferFactory(Function<Supplier<Object[]>, GroupAggBuffer> bufferFactory) {
        this.bufferFactory = bufferFactory;
        return this;
    }

    public ParallelGroupByAgg sum(String measureField) {
        aggs.add(new AggSpec(Kind.SUM, requireField(measureField)));
        return this;
    }

    /** count(*): counts every row (null measures included). */
    public ParallelGroupByAgg count() {
        aggs.add(new AggSpec(Kind.COUNT, -1));
        return this;
    }

    /** count(field): counts rows whose field is not null (the SQL count(field) semantics). */
    public ParallelGroupByAgg countField(String measureField) {
        aggs.add(new AggSpec(Kind.COUNT, requireField(measureField)));
        return this;
    }

    /** max(field): ignores null and returns null for an empty group. */
    public ParallelGroupByAgg max(String measureField) {
        aggs.add(new AggSpec(Kind.MAX, requireField(measureField)));
        return this;
    }

    /** min(field): ignores null and returns null for an empty group. */
    public ParallelGroupByAgg min(String measureField) {
        aggs.add(new AggSpec(Kind.MIN, requireField(measureField)));
        return this;
    }

    private int requireField(String measureField) {
        int found = dataSchema.getFieldIndex(measureField);
        if (found < 0) {
            throw new IllegalArgumentException("Unknown measure field: " + measureField);
        }
        return found;
    }

    // ---- configuration getters, used by DataSet to build a describable PlanNode (typeId + params). ----

    /** The grouping key field names, in order (reverse-looked up from index to name, round-tripping with the constructor arguments). */
    public List<String> keyFieldNames() {
        List<String> names = new ArrayList<>(keyIndices.length);
        for (int idx : keyIndices) {
            names.add(dataSchema.getField(idx).getName());
        }
        return names;
    }

    /** How many aggregates there are (describable supports a single aggregate only; multi-aggregate describable is a later step). */
    public int aggCount() {
        return aggs.size();
    }

    /** The typeId of a single aggregate (groupBySum/groupByCount/groupByMax/groupByMin); anything else throws. */
    public String singleAggTypeId() {
        AggSpec spec = requireSingleAgg();
        switch (spec.kind) {
            case SUM: return "groupBySum";
            case COUNT: return "groupByCount";
            case MAX: return "groupByMax";
            case MIN:
            default: return "groupByMin";
        }
    }

    /** The measure field name of a single aggregate (count(*) returns null); reverse-looked up from index to name, round-tripping with the constructor arguments. */
    public String singleAggMeasureField() {
        AggSpec spec = requireSingleAgg();
        return spec.measureIndex < 0 ? null : dataSchema.getField(spec.measureIndex).getName();
    }

    private AggSpec requireSingleAgg() {
        if (aggs.size() != 1) {
            throw new IllegalStateException("describable extraction supports single agg only, got " + aggs.size());
        }
        return aggs.get(0);
    }

    // ---- multi-aggregate describable: two parallel lists (kinds + measures), ordered by the getOutputSchema column order. ----

    /** Each aggregate's kind name (SUM/COUNT/MAX/MIN), in column order. */
    public List<String> aggKindNames() {
        List<String> kinds = new ArrayList<>(aggs.size());
        for (AggSpec spec : aggs) {
            kinds.add(spec.kind.name());
        }
        return kinds;
    }

    /** Each aggregate's measure field name, in column order; count(*) becomes an empty string (measureIndex<0). Reverse-looked up from index to name, round-tripping with the constructor arguments. */
    public List<String> aggMeasureFields() {
        List<String> measures = new ArrayList<>(aggs.size());
        for (AggSpec spec : aggs) {
            measures.add(spec.measureIndex < 0 ? "" : dataSchema.getField(spec.measureIndex).getName());
        }
        return measures;
    }

    /**
     * The output DataSchema: [key fields..., each agg result column]. Key fields carry over from the source; agg column types are sum to BigDecimalType,
     * count to LongType, and max/min to the measure field's type. An agg column name is the kind prefix plus its position (the name of a grouped-aggregate output column is not semantically significant, since downstream reads by position).
     */
    public DataSchema getOutputSchema() {
        Field[] fields = new Field[keyIndices.length + aggs.size()];
        int c = 0;
        for (int keyIndex : keyIndices) {
            fields[c++] = dataSchema.getField(keyIndex);
        }
        for (int i = 0; i < aggs.size(); i++) {
            AggSpec spec = aggs.get(i);
            DataType type;
            String prefix;
            switch (spec.kind) {
                case SUM: type = DataType.BigDecimalType; prefix = "sum"; break;
                case COUNT: type = DataType.LongType; prefix = "count"; break;
                case MAX: type = dataSchema.getField(spec.measureIndex).getDataType(); prefix = "max"; break;
                case MIN:
                default: type = dataSchema.getField(spec.measureIndex).getDataType(); prefix = "min"; break;
            }
            fields[c++] = new Field(prefix + i, type);
        }
        return new DataSchema(fields);
    }

    /**
     * The only outlet (the pure Push contract): the breaker pulls upstream until null (finishing it), aggregates by key into one GroupAggBuffer
     * (the buffer handles spill), then builds each group's result row and emits it, closing with endOfStream. Control events: Error and Cancel pass through and then
     * terminate (nothing is produced); a Barrier is absorbed.
     */
    public void aggregate(RowStream upstream, RowSink out) {
        RowSerializer keySerializer = new RowSerializer(keyMeta);
        boolean directProjectedKeys = KeyProjectionMode.enabled();
        GroupAggBuffer buf = bufferFactory.apply(this::newAccumulators);
        Throwable primary = null;
        try {
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
                byte[] keyBytes = directProjectedKeys
                        ? keySerializer.serializeProjected(r, keyIndices)
                        : keySerializer.serialize(keyRowOf(r));
                accumulate(buf.getOrCreate(keyBytes), r);
            }

            for (GroupAggBuffer.Group g : buf) {
                out.emitRow(buildResultRow(keySerializer.deserialize(g.keyBytes()), g.accumulators()));
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, buf);
        }
    }

    private DataRow keyRowOf(DataRow row) {
        DataRow keyRow = new DataRow(keyIndices.length);
        for (int i = 0; i < keyIndices.length; i++) {
            keyRow.set(i, row.get(keyIndices[i]));
        }
        return keyRow;
    }

    private Object[] newAccumulators() {
        Object[] accs = new Object[aggs.size()];
        for (int i = 0; i < aggs.size(); i++) {
            switch (aggs.get(i).kind) {
                case SUM: accs[i] = new SumAggregator(); break;
                case MAX: accs[i] = new MaxAggregator(); break;
                case MIN: accs[i] = new MinAggregator(); break;
                case COUNT:
                default: accs[i] = new CountAggregator(); break;
            }
        }
        return accs;
    }

    private void accumulate(Object[] accs, DataRow row) {
        for (int i = 0; i < aggs.size(); i++) {
            AggSpec spec = aggs.get(i);
            switch (spec.kind) {
                case SUM:
                    ((SumAggregator) accs[i]).add(row.getBigDecimal(spec.measureIndex)); // null -> skipped
                    break;
                case MAX:
                    ((MaxAggregator) accs[i]).add(row.get(spec.measureIndex)); // null -> ignored
                    break;
                case MIN:
                    ((MinAggregator) accs[i]).add(row.get(spec.measureIndex)); // null -> ignored
                    break;
                case COUNT:
                default:
                    if (spec.measureIndex >= 0) {
                        if (row.get(spec.measureIndex) != null) {
                            ((CountAggregator) accs[i]).add(row); // count(field): non-null only
                        }
                    } else {
                        ((CountAggregator) accs[i]).add(row); // count(*): every row
                    }
                    break;
            }
        }
    }

    private DataRow buildResultRow(DataRow keyRow, Object[] accs) {
        DataRow out = new DataRow(keyIndices.length + aggs.size());
        int col = 0;
        for (int i = 0; i < keyIndices.length; i++) {
            out.set(col++, keyRow.get(i));
        }
        for (int i = 0; i < aggs.size(); i++) {
            switch (aggs.get(i).kind) {
                case SUM:
                    out.set(col++, ((SumAggregator) accs[i]).result());
                    break;
                case MAX:
                    out.set(col++, ((MaxAggregator) accs[i]).result());
                    break;
                case MIN:
                    out.set(col++, ((MinAggregator) accs[i]).result());
                    break;
                case COUNT:
                default:
                    out.set(col++, ((CountAggregator) accs[i]).result());
                    break;
            }
        }
        return out;
    }
}
