package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.mem.GroupAggBuffer;
import io.rowweave.mem.HeapGroupAggBuffer;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * internal breaker runtime. Public visibility only allows PlanOps in a
 * different package to rebuild the versioned operator.
 */
public final class TypedQualitySummary {

    public static final String SUMMARY_VERSION = "1";
    public static final String ACCEPTED_TOTAL = "ACCEPTED_TOTAL";
    public static final String REJECTED_TOTAL = "REJECTED_TOTAL";
    public static final String REJECTED_REASON = "REJECTED_REASON";
    private static final int MAX_REASON_BUCKETS =
            RowValidation.MAX_RULES + RowValidation.MAX_FIELDS + 1;

    private static final int[] REASON_INDICES = {3, 4, 5, 6};
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));
    private static final DataSchema REASON_META = new DataSchema(
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    private enum Kind { ACCEPTED, REJECTED }

    private final Kind kind;
    private final int maxReasonBuckets;
    private Function<Supplier<Object[]>, GroupAggBuffer> bufferFactory;

    private TypedQualitySummary(Kind kind, int maxReasonBuckets) {
        this.kind = kind;
        this.maxReasonBuckets = maxReasonBuckets;
        this.bufferFactory = factory -> new HeapGroupAggBuffer(
                factory, MAX_REASON_BUCKETS);
    }

    public static TypedQualitySummary accepted() {
        return new TypedQualitySummary(Kind.ACCEPTED, 0);
    }

    public static TypedQualitySummary rejected(DataSchema rejectedMeta, int maxReasonBuckets) {
        if (!sameMeta(REJECTED_META, rejectedMeta)) {
            throw invalidConfiguration("rejected schema is noncanonical");
        }
        if (maxReasonBuckets <= 0 || maxReasonBuckets > MAX_REASON_BUCKETS) {
            throw invalidConfiguration("reason bucket bound is invalid");
        }
        return new TypedQualitySummary(Kind.REJECTED, maxReasonBuckets);
    }

    public TypedQualitySummary withBufferFactory(
            Function<Supplier<Object[]>, GroupAggBuffer> factory) {
        if (factory == null) {
            throw invalidConfiguration("buffer factory is required");
        }
        this.bufferFactory = factory;
        return this;
    }

    public void summarize(RowStream upstream, RowSink out) {
        if (kind == Kind.ACCEPTED) {
            summarizeAccepted(upstream, out);
        } else {
            summarizeRejected(upstream, out);
        }
    }

    private static void summarizeAccepted(RowStream upstream, RowSink out) {
        long count = 0L;
        StreamEvent event;
        while ((event = upstream.next()) != null) {
            if (event.isRecord()) {
                count = Math.addExact(count, 1L);
                continue;
            }
            if (event.isError() || event.isCancel()) {
                out.emit(event);
                return;
            }
        }
        out.emitRow(totalRow(ACCEPTED_TOTAL, count));
        out.endOfStream();
    }

    private void summarizeRejected(RowStream upstream, RowSink out) {
        RowSerializer reasonSerializer = new RowSerializer(REASON_META);
        GroupAggBuffer buffer = bufferFactory.apply(
                () -> new Object[]{new TypedQualityCountAccumulator()});
        if (buffer == null) {
            throw invalidRuntime("buffer factory returned null");
        }
        Throwable primary = null;
        try {
            long total = 0L;
            StreamEvent event;
            while ((event = upstream.next()) != null) {
                if (event.isRecord()) {
                    DataRow rejected = event.row();
                    verifyRejectedRow(rejected);
                    total = Math.addExact(total, 1L);
                    TypedQualityCountAccumulator count = (TypedQualityCountAccumulator)
                            buffer.getOrCreate(reasonSerializer.serializeProjected(
                                    rejected, REASON_INDICES))[0];
                    count.increment();
                    continue;
                }
                if (event.isError() || event.isCancel()) {
                    out.emit(event);
                    return;
                }
            }

            long observed = buffer.groupCount();
            if (observed > maxReasonBuckets) {
                throw invalidRuntime("observed reason buckets exceed the Plan bound");
            }
            out.emitRow(totalRow(REJECTED_TOTAL, total));
            for (GroupAggBuffer.Group group : buffer) {
                DataRow reason = reasonSerializer.deserialize(group.keyBytes());
                TypedQualityCountAccumulator count =
                        (TypedQualityCountAccumulator) group.accumulators()[0];
                out.emitRow(reasonRow(reason, count.result()));
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, buffer);
        }
    }

    private static void verifyRejectedRow(DataRow row) {
        if (row == null || row.size() != REJECTED_META.getFieldCount()
                || !(row.get(3) instanceof String)
                || !(row.get(4) instanceof String)
                || row.get(5) == null || row.get(5).getClass() != Integer.class
                || !(row.get(6) instanceof String)) {
            throw invalidRuntime("rejected reason row is noncanonical");
        }
    }

    private static DataRow totalRow(String kind, long count) {
        DataRow row = new DataRow(TypedQualityManifest.summarySchema().getFieldCount());
        row.setValues(new Object[]{kind, "", "", -1, "", count});
        return row;
    }

    private static DataRow reasonRow(DataRow reason, long count) {
        DataRow row = new DataRow(TypedQualityManifest.summarySchema().getFieldCount());
        row.setValues(new Object[]{REJECTED_REASON, reason.get(0), reason.get(1),
                reason.get(2), reason.get(3), count});
        return row;
    }

    private static boolean sameMeta(DataSchema left, DataSchema right) {
        return right != null
                && SchemaFingerprint.of(left).equals(SchemaFingerprint.of(right));
    }

    private static IllegalArgumentException invalidConfiguration(String detail) {
        return new IllegalArgumentException("QUALITY_SUMMARY_INVALID: " + detail);
    }

    private static IllegalStateException invalidRuntime(String detail) {
        return new IllegalStateException("QUALITY_SUMMARY_RUNTIME_INVALID: " + detail);
    }
}
