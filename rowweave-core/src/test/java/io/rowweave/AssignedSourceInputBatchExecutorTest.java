package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** assigned source batches own every child executor lifecycle. */
class AssignedSourceInputBatchExecutorTest {

    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.IntegerType));

    @BeforeAll
    static void registerProbeExecutor() {
        SourceExecutorFactory.register(ProbeInput.class, ProbeExecutor::new);
    }

    @Test
    void assignedBatchOpensDrainsAndClosesOneChildAtATimeInBindingOrder() {
        ProbeState first = state("first", 1, 2);
        ProbeState second = state("second", 3);
        AssignedSourceBatch batch = batch(first, second);

        SourceExecutor<?> executor = SourceExecutorFactory.create(batch);
        assertInstanceOf(AssignedSourceInputBatchExecutor.class, executor);
        executor.open();
        List<Integer> rows = new ArrayList<>();
        while (executor.hasNext()) {
            rows.add((Integer) executor.next().get(0));
        }
        executor.close();
        executor.close();

        assertEquals(Arrays.asList(1, 2, 3), rows);
        assertEquals(Arrays.asList(
                "create:first", "open:first", "close:first",
                "create:second", "open:second", "close:second"),
                combined(first, second));
        assertEquals(1, first.opens);
        assertEquals(1, first.closes);
        assertEquals(1, second.opens);
        assertEquals(1, second.closes);
        assertThrows(UnsupportedOperationException.class, batch::iterator,
                "execution batch must never fall back to raw Iterable Source");
    }

    @Test
    void earlyCloseOnlyClosesTheActiveChildAndNeverTouchesLaterSplits() {
        ProbeState first = state("first", 1, 2);
        ProbeState second = state("second", 3);
        ProbeState third = state("third", 4);
        SourceExecutor<?> executor = SourceExecutorFactory.create(batch(first, second, third));
        executor.open();

        assertTrue(executor.hasNext());
        assertEquals(1, executor.next().get(0));
        executor.close();
        assertFalse(executor.hasNext());

        assertEquals(1, first.opens);
        assertEquals(1, first.closes);
        assertEquals(0, second.opens);
        assertEquals(0, second.closes);
        assertEquals(0, third.opens);
        assertEquals(0, third.closes);
    }

    @Test
    void openReadAndHasNextFailuresCloseCurrentAndSuppressCloseFailure() {
        assertPrimaryAndSuppressed(FailurePoint.OPEN);
        assertPrimaryAndSuppressed(FailurePoint.HAS_NEXT);
        assertPrimaryAndSuppressed(FailurePoint.NEXT);
    }

    @Test
    void concatCompatibilityUsesTheSameCloseAwareExecutorInsteadOfIterableFallback() {
        ProbeState first = state("concat-first", 5);
        ProbeState second = state("concat-second", 6);
        ConcatSource concat = new ConcatSource(Arrays.<Source>asList(
                new ProbeInput(first), new ProbeInput(second)));

        SourceExecutor<?> executor = SourceExecutorFactory.create(concat);
        assertInstanceOf(ConcatInputExecutor.class, executor);
        executor.open();
        assertTrue(executor.hasNext());
        assertEquals(5, executor.next().get(0));
        executor.close();

        assertEquals(1, first.closes);
        assertEquals(0, second.opens);
        assertEquals(0, second.closes);
    }

    private static void assertPrimaryAndSuppressed(FailurePoint point) {
        ProbeState first = state("failing-" + point, 1);
        first.failurePoint = point;
        first.closeFails = true;
        ProbeState unopened = state("unopened-" + point, 2);
        SourceExecutor<?> executor = SourceExecutorFactory.create(batch(first, unopened));
        executor.open();

        IllegalStateException failure;
        if (point == FailurePoint.OPEN) {
            failure = assertThrows(IllegalStateException.class, executor::hasNext);
        } else if (point == FailurePoint.HAS_NEXT) {
            failure = assertThrows(IllegalStateException.class, executor::hasNext);
        } else {
            assertTrue(executor.hasNext());
            failure = assertThrows(IllegalStateException.class, executor::next);
        }
        assertTrue(failure.getMessage().contains("primary-" + point));
        assertEquals(1, failure.getSuppressed().length);
        assertTrue(failure.getSuppressed()[0].getMessage().contains("close-failing-" + point));
        assertEquals(1, first.closes);
        assertEquals(0, unopened.opens);
        assertEquals(0, unopened.closes);
        executor.close();
    }

    private static AssignedSourceBatch batch(ProbeState... states) {
        List<BatchSourceSplitBinding> bindings = new ArrayList<>();
        for (int i = 0; i < states.length; i++) {
            ProbeInput input = new ProbeInput(states[i]);
            bindings.add(new BatchSourceSplitBinding(input,
                    new BatchSourceSplitDescriptor(
                            1, "probe", 1, "probe-" + i,
                            SchemaFingerprint.of(META), "snapshot-" + i,
                            BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT)));
        }
        return new AssignedSourceBatch(bindings);
    }

    private static ProbeState state(String name, Integer... values) {
        return new ProbeState(name, Arrays.asList(values));
    }

    private static List<String> combined(ProbeState... states) {
        List<String> events = new ArrayList<>();
        for (ProbeState state : states) events.addAll(state.events);
        events.sort((left, right) -> Integer.compare(
                Integer.parseInt(left.substring(0, left.indexOf('|'))),
                Integer.parseInt(right.substring(0, right.indexOf('|')))));
        List<String> stripped = new ArrayList<>();
        for (String event : events) stripped.add(event.substring(event.indexOf('|') + 1));
        return stripped;
    }

    private enum FailurePoint { NONE, OPEN, HAS_NEXT, NEXT }

    private static final class ProbeState {
        private static int sequence;
        final String name;
        final List<Integer> values;
        final List<String> events = new ArrayList<>();
        FailurePoint failurePoint = FailurePoint.NONE;
        boolean closeFails;
        int opens;
        int closes;
        private ProbeState(String name, List<Integer> values) {
            this.name = name;
            this.values = values;
        }
        void event(String value) { events.add((++sequence) + "|" + value); }
    }

    private static final class ProbeInput implements Source {
        private static final long serialVersionUID = 1L;
        private final transient ProbeState state;
        private ProbeInput(ProbeState state) { this.state = state; }
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() {
            throw new AssertionError("raw iterator fallback is forbidden");
        }
    }

    private static final class ProbeExecutor extends SourceExecutor<ProbeInput> {
        private int index;
        private boolean closed;
        private ProbeExecutor(ProbeInput input) {
            super(input, META);
            input.state.event("create:" + input.state.name);
        }
        @Override public void open() {
            input.state.opens++;
            input.state.event("open:" + input.state.name);
            fail(FailurePoint.OPEN);
        }
        @Override public boolean hasNext() {
            fail(FailurePoint.HAS_NEXT);
            return index < input.state.values.size();
        }
        @Override public DataRow next() {
            fail(FailurePoint.NEXT);
            DataRow row = new DataRow(1);
            row.set(0, input.state.values.get(index++));
            return row;
        }
        @Override public void close() {
            if (closed) return;
            closed = true;
            input.state.closes++;
            input.state.event("close:" + input.state.name);
            if (input.state.closeFails) {
                throw new IllegalStateException("close-" + input.state.name);
            }
        }
        private void fail(FailurePoint point) {
            if (input.state.failurePoint == point) {
                throw new IllegalStateException("primary-" + point);
            }
        }
    }
}
