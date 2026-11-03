package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.OpenAddressingGroupAggBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exact, bounded and spillable typed-quality summaries. */
class TypedQualitySummaryRuntimeTest {

    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    @Test
    void acceptedSummaryAlwaysEmitsOneExactTotalIncludingEmptyInput() {
        assertSummary(run(TypedQualitySummary.accepted(), Collections.emptyList()).rows().get(0),
                "ACCEPTED_TOTAL", "", "", -1, "", 0L);

        CollectingRowSink three = run(TypedQualitySummary.accepted(), Arrays.asList(
                row("a"), row("b"), row("c")));
        assertEquals(1, three.rows().size());
        assertSummary(three.rows().get(0),
                "ACCEPTED_TOTAL", "", "", -1, "", 3L);
        assertTrue(three.sawEndOfStream());
    }

    @Test
    void rejectedSummaryEmitsTotalAndOneExactBucketPerCanonicalReason() {
        CollectingRowSink empty = run(
                TypedQualitySummary.rejected(REJECTED_META, 1), Collections.emptyList());
        assertEquals(1, empty.rows().size());
        assertSummary(empty.rows().get(0),
                "REJECTED_TOTAL", "", "", -1, "", 0L);

        TypedQualitySummary summary = TypedQualitySummary.rejected(REJECTED_META, 4);
        CollectingRowSink output = run(summary, Arrays.asList(
                rejected(0, "QUALITY_RULE_VIOLATION", "adaptor.required", 0, "NOT_NULL"),
                rejected(1, "QUALITY_RULE_VIOLATION", "period.range", 1,
                        "INTEGER_RANGE_CLOSED"),
                rejected(2, "QUALITY_RULE_VIOLATION", "period.range", 1,
                        "INTEGER_RANGE_CLOSED")));

        assertEquals(3, output.rows().size());
        Map<String, DataRow> byKindAndRule = new HashMap<>();
        for (DataRow result : output.rows()) {
            byKindAndRule.put(result.getString(0) + "|" + result.getString(2), result);
        }
        assertSummary(byKindAndRule.get("REJECTED_TOTAL|"),
                "REJECTED_TOTAL", "", "", -1, "", 3L);
        assertSummary(byKindAndRule.get("REJECTED_REASON|adaptor.required"),
                "REJECTED_REASON", "QUALITY_RULE_VIOLATION", "adaptor.required",
                0, "NOT_NULL", 1L);
        assertSummary(byKindAndRule.get("REJECTED_REASON|period.range"),
                "REJECTED_REASON", "QUALITY_RULE_VIOLATION", "period.range",
                1, "INTEGER_RANGE_CLOSED", 2L);
        assertTrue(output.sawEndOfStream());
    }

    @Test
    void rejectedSummarySpillsWithoutChangingExactBuckets() {
        MemoryManager memory = new MemoryManager(128);
        TypedQualitySummary summary = TypedQualitySummary.rejected(REJECTED_META, 80)
                .withBufferFactory(factory -> new OpenAddressingGroupAggBuffer(
                        REJECTED_META,
                        new String[]{"quality_error_code", "quality_rule_id",
                                "quality_field_index", "quality_detail"},
                        factory, memory, new HeapPool()));
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            rows.add(rejected(i, "QUALITY_RULE_VIOLATION", "rule." + i,
                    i % 3, "NOT_NULL"));
            rows.add(rejected(i + 100, "QUALITY_RULE_VIOLATION", "rule." + i,
                    i % 3, "NOT_NULL"));
        }

        CollectingRowSink output = run(summary, rows);

        assertEquals(61, output.rows().size());
        assertEquals(120L, output.rows().stream()
                .filter(row -> "REJECTED_TOTAL".equals(row.getString(0)))
                .findFirst().orElseThrow(AssertionError::new).getLong(5));
        assertEquals(120L, output.rows().stream()
                .filter(row -> "REJECTED_REASON".equals(row.getString(0)))
                .mapToLong(row -> row.getLong(5)).sum());
        assertTrue(memory.spillCount() > 0, "tiny managed budget must exercise spill merge");
    }

    @Test
    void rejectsMoreObservedReasonBucketsThanThePlanBoundBeforeEmitting() {
        TypedQualitySummary summary = TypedQualitySummary.rejected(REJECTED_META, 1);
        CollectingRowSink output = new CollectingRowSink();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> summary.summarize(RowStream.fromIterable(Arrays.asList(
                        rejected(0, "QUALITY_RULE_VIOLATION", "rule.a", 0, "NOT_NULL"),
                        rejected(1, "QUALITY_RULE_VIOLATION", "rule.b", 1, "NOT_NULL"))),
                        output));

        assertTrue(failure.getMessage().startsWith("QUALITY_SUMMARY_RUNTIME_INVALID:"),
                failure::getMessage);
        assertTrue(output.rows().isEmpty());
        assertFalse(output.sawEndOfStream());
    }

    @Test
    void breakerForwardsTerminalFailureWithoutPublishingPartialCounts() {
        RuntimeException cause = new RuntimeException("upstream failure");
        List<TypedQualitySummary> summaries = Arrays.asList(
                TypedQualitySummary.accepted(),
                TypedQualitySummary.rejected(REJECTED_META, 2));
        for (int i = 0; i < summaries.size(); i++) {
            TypedQualitySummary summary = summaries.get(i);
            Iterator<StreamEvent> events = Arrays.asList(
                    StreamEvent.record(i == 0
                            ? row("accepted")
                            : rejected(0, "QUALITY_RULE_VIOLATION", "rule.a", 0, "NOT_NULL")),
                    StreamEvent.error(cause)).iterator();
            CollectingRowSink output = new CollectingRowSink();

            summary.summarize(() -> events.hasNext() ? events.next() : null, output);

            assertTrue(output.rows().isEmpty());
            assertTrue(output.sawError());
            assertSame(cause, output.error());
            assertFalse(output.sawEndOfStream());
        }
    }

    @Test
    void exactCounterRejectsAddMergeAndCorruptStateOverflow() {
        TypedQualityCountAccumulator count = new TypedQualityCountAccumulator();
        count.mergeState(ByteBuffer.allocate(Long.BYTES).putLong(Long.MAX_VALUE).array());
        assertThrows(ArithmeticException.class, count::increment);

        TypedQualityCountAccumulator one = new TypedQualityCountAccumulator();
        one.increment();
        assertThrows(ArithmeticException.class,
                () -> one.mergeState(ByteBuffer.allocate(Long.BYTES)
                        .putLong(Long.MAX_VALUE).array()));
        assertThrows(IllegalArgumentException.class,
                () -> one.mergeState(new byte[Long.BYTES - 1]));
        assertThrows(IllegalArgumentException.class,
                () -> one.mergeState(ByteBuffer.allocate(Long.BYTES).putLong(-1L).array()));
    }

    @Test
    void rejectedSummaryClosesItsWorkingSetOnSuccessAndFailure() {
        AtomicBoolean successClosed = new AtomicBoolean();
        TypedQualitySummary success = TypedQualitySummary.rejected(REJECTED_META, 2)
                .withBufferFactory(factory -> trackingBuffer(factory, successClosed));
        run(success, Collections.singletonList(rejected(
                0, "QUALITY_RULE_VIOLATION", "rule.a", 0, "NOT_NULL")));
        assertTrue(successClosed.get());

        AtomicBoolean failureClosed = new AtomicBoolean();
        TypedQualitySummary failure = TypedQualitySummary.rejected(REJECTED_META, 1)
                .withBufferFactory(factory -> trackingBuffer(factory, failureClosed));
        assertThrows(IllegalStateException.class, () -> run(failure, Arrays.asList(
                rejected(0, "QUALITY_RULE_VIOLATION", "rule.a", 0, "NOT_NULL"),
                rejected(1, "QUALITY_RULE_VIOLATION", "rule.b", 1, "NOT_NULL"))));
        assertTrue(failureClosed.get());
    }

    private static CollectingRowSink run(TypedQualitySummary summary, List<DataRow> rows) {
        CollectingRowSink output = new CollectingRowSink();
        summary.summarize(RowStream.fromIterable(rows), output);
        return output;
    }

    private static io.rowweave.mem.GroupAggBuffer trackingBuffer(
            Supplier<Object[]> factory, AtomicBoolean closed) {
        io.rowweave.mem.GroupAggBuffer delegate =
                new io.rowweave.mem.HeapGroupAggBuffer(factory, 64);
        return new io.rowweave.mem.GroupAggBuffer() {
            @Override public Object[] getOrCreate(byte[] keyBytes) {
                return delegate.getOrCreate(keyBytes);
            }
            @Override public long groupCount() { return delegate.groupCount(); }
            @Override public Iterator<Group> iterator() { return delegate.iterator(); }
            @Override public void close() {
                delegate.close();
                closed.set(true);
            }
        };
    }

    private static DataRow rejected(long ordinal, String code, String rule, int field, String detail) {
        return row("ledger.source", repeat('a', 64), ordinal, code, rule, field, detail);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void assertSummary(DataRow row, String kind, String code, String rule,
                                      int field, String detail, long count) {
        assertEquals(kind, row.getString(0));
        assertEquals(code, row.getString(1));
        assertEquals(rule, row.getString(2));
        assertEquals(field, row.getInteger(3));
        assertEquals(detail, row.getString(4));
        assertEquals(count, row.getLong(5));
    }
}
