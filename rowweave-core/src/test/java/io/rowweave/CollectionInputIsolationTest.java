package io.rowweave;

import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.mem.RowSizeEstimator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** An in-memory Source is a replayable snapshot that leaks no mutable DataRow or Date reference. */
class CollectionInputIsolationTest {

    private static final DataSchema VALUE_META = new DataSchema(new Field("v", DataType.IntegerType));
    private static final DataSchema TIME_META = new DataSchema(
            new Field("d", DataType.DateType), new Field("ts", DataType.TimestampType));

    @Test
    void constructorAndEveryIteratorAreIndependentSnapshots() {
        Date date = new Date(1_700_000_000_123L);
        Timestamp timestamp = new Timestamp(1_700_000_000_456L);
        timestamp.setNanos(456_789_123);
        long expectedDateMillis = date.getTime();
        long expectedTimestampMillis = timestamp.getTime();
        int expectedNanos = timestamp.getNanos();

        DataRow original = new DataRow(2);
        original.set(0, date);
        original.set(1, timestamp);
        CollectionSource input = new CollectionSource(TIME_META, Collections.singletonList(original));

        original.set(0, new Date(9L));
        date.setTime(10L);
        timestamp.setTime(11L);
        timestamp.setNanos(12);

        DataRow first = input.iterator().next();
        assertNotSame(original, first, "CollectionSource must not expose the caller DataRow");
        assertEquals(expectedDateMillis, first.getDate(0).getTime());
        assertEquals(expectedTimestampMillis, first.getTimestamp(1).getTime());
        assertEquals(expectedNanos, first.getTimestamp(1).getNanos());

        first.set(0, new Date(20L));
        first.getTimestamp(1).setTime(21L);
        first.getTimestamp(1).setNanos(22);

        DataRow replay = input.iterator().next();
        assertNotSame(first, replay, "each iterator must produce an independent DataRow");
        assertEquals(expectedDateMillis, replay.getDate(0).getTime());
        assertEquals(expectedTimestampMillis, replay.getTimestamp(1).getTime());
        assertEquals(expectedNanos, replay.getTimestamp(1).getNanos());
        assertNotSame(first.getTimestamp(1), replay.getTimestamp(1), "mutable Timestamp values must also be isolated");
    }

    @Test
    void samePlanCanReplayAfterMutatingMap() {
        DataRow sourceRow = new DataRow(1);
        sourceRow.set(0, 1);
        CollectionSource input = new CollectionSource(VALUE_META, Collections.singletonList(sourceRow));
        MapFunction mutating = row -> {
            row.set(0, ((Integer) row.get(0)) + 1);
            return row;
        };
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(VALUE_META, Collections.<Source>singletonList(input)),
                PlanNode.unaryFn("map", VALUE_META, 0, mutating)));

        assertEquals(Collections.singletonList(2), executeValues(graph));
        assertEquals(Collections.singletonList(2), executeValues(graph),
                "an in-place map during the first run must not affect the second source replay");
        assertEquals(1, sourceRow.get(0), "execution must not modify the caller row in place");
    }

    @Test
    void serializationRoundTripPreservesReplayIsolation() throws Exception {
        Timestamp timestamp = new Timestamp(1_700_000_000_789L);
        timestamp.setNanos(789_123_456);
        DataRow row = new DataRow(2);
        row.set(0, new Date(123L));
        row.set(1, timestamp);
        CollectionSource copy = roundTrip(new CollectionSource(TIME_META, Collections.singletonList(row)));

        DataRow first = copy.iterator().next();
        first.getDate(0).setTime(999L);
        first.getTimestamp(1).setNanos(1);
        DataRow replay = copy.iterator().next();

        assertEquals(123L, replay.getDate(0).getTime());
        assertEquals(789_123_456, replay.getTimestamp(1).getNanos());
        assertNotSame(first, replay);
        assertNotSame(first.getDate(0), replay.getDate(0));
    }

    @Test
    void explicitRowLimitRejectsFromCollectionSizeBeforeIteration() {
        Collection<DataRow> oversized = new AbstractCollection<DataRow>() {
            @Override
            public Iterator<DataRow> iterator() {
                throw new AssertionError("row-limit preflight must not iterate");
            }

            @Override
            public int size() {
                return 2;
            }
        };

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> new CollectionSource(
                        VALUE_META, oversized, CompatibilityDataLimits.of(1, 1024)));

        assertEquals(CompatibilityDataLimitException.Kind.ROWS, failure.kind());
        assertEquals(1, failure.limit());
        assertEquals(2, failure.observed());
        assertEquals("CollectionSource", failure.owner());
    }

    @Test
    void explicitByteLimitIncludesTheResidentReferenceArrayAndRows() {
        DataRow row = new DataRow(1);
        row.set(0, "bounded");
        long exactBytes = RowSizeEstimator.estimateReferenceArray(1)
                + RowSizeEstimator.estimateRow(row);

        CollectionSource exact = new CollectionSource(
                VALUE_META, Collections.singletonList(row),
                CompatibilityDataLimits.of(1, exactBytes));
        assertEquals("bounded", exact.iterator().next().get(0));

        CompatibilityDataLimitException failure = assertThrows(
                CompatibilityDataLimitException.class,
                () -> new CollectionSource(
                        VALUE_META, Collections.singletonList(row),
                        CompatibilityDataLimits.of(1, exactBytes - 1)));
        assertEquals(CompatibilityDataLimitException.Kind.BYTES, failure.kind());
        assertEquals(exactBytes - 1, failure.limit());
        assertEquals(exactBytes, failure.observed());
    }

    @Test
    void explicitLimitsSurviveSerializationWithReplayIsolation() throws Exception {
        DataRow row = new DataRow(1);
        row.set(0, 7);
        long exactBytes = RowSizeEstimator.estimateReferenceArray(1)
                + RowSizeEstimator.estimateRow(row);
        CollectionSource copy = roundTrip(new CollectionSource(
                VALUE_META, Collections.singletonList(row),
                CompatibilityDataLimits.of(1, exactBytes)));

        DataRow first = copy.iterator().next();
        first.set(0, 9);
        assertEquals(7, copy.iterator().next().get(0));
    }

    @Test
    void iterableInputSnapshotsAReusedIteratorRow() {
        Iterable<DataRow> reusingRows = () -> new Iterator<DataRow>() {
            private final DataRow reuse = new DataRow(1);
            private int next = 1;

            @Override
            public boolean hasNext() {
                return next <= 2;
            }

            @Override
            public DataRow next() {
                reuse.set(0, next++);
                return reuse;
            }
        };

        Iterator<DataRow> iterator = IterableSource.of(VALUE_META, reusingRows).iterator();
        DataRow first = iterator.next();
        DataRow second = iterator.next();

        assertNotSame(first, second, "an underlying reused row must not leak through IterableSource");
        assertEquals(1, first.get(0), "reading the next row must not overwrite a row already emitted");
        assertEquals(2, second.get(0));
    }

    private static List<Integer> executeValues(PlanGraph graph) {
        java.util.ArrayList<Integer> out = new java.util.ArrayList<>();
        new PushDagExecutor().execute(graph, row -> out.add((Integer) row.get(0)));
        return out;
    }

    private static CollectionSource roundTrip(CollectionSource input) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(input);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (CollectionSource) in.readObject();
        }
    }
}
