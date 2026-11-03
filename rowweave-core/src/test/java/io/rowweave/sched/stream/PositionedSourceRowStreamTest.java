package io.rowweave.sched.stream;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.SourceExecutor;
import io.rowweave.SourceExecutorFactory;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exact split identity and split-local decoded-row ordinal. */
class PositionedSourceRowStreamTest {

    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.IntegerType));

    @BeforeAll
    static void registerProbeExecutor() {
        SourceExecutorFactory.register(ProbeInput.class, ProbeExecutor::new);
    }

    @Test
    void positionIsCanonicalBoundedAndDoesNotRetainRawLocator() {
        BatchSourceSplitDescriptor descriptor = descriptor(
                "domain/ledger/secret-object", "snapshot-secret-token");
        SourceRecordPosition position = SourceRecordPosition.of(
                "ledger.source", descriptor, 7L);

        assertEquals("ledger.source", position.sourceOperatorUid());
        assertEquals(hex(BatchSourceSplitDescriptorCodec.digest(descriptor)),
                position.splitIdentitySha256());
        assertEquals(7L, position.splitLocalOrdinal());
        assertEquals(position, SourceRecordPosition.of("ledger.source", descriptor, 7L));
        assertNotEquals(position, SourceRecordPosition.of("ledger.source", descriptor, 8L));
        assertFalse(position.toString().contains("secret-object"));
        assertFalse(position.toString().contains("secret-token"));

        assertInvalid(() -> SourceRecordPosition.of("", descriptor, 0L));
        assertInvalid(() -> SourceRecordPosition.of(
                Normalizer.normalize("e\u0301", Normalizer.Form.NFD), descriptor, 0L));
        assertInvalid(() -> SourceRecordPosition.of(
                String.join("", Collections.nCopies(257, "x")), descriptor, 0L));
        assertInvalid(() -> SourceRecordPosition.of("ledger.source", descriptor, -1L));
    }

    @Test
    void streamBindsRowsToOrderedSplitsAndResetsOrdinalPerSplit() {
        CollectionSource first = exact("split-a", 10, 11);
        CollectionSource second = exact("split-b", 20);
        List<BatchSourceSplitBinding> bindings = Arrays.asList(
                binding(first), binding(second));

        List<StreamEvent> events = new ArrayList<>();
        try (RowStream.CloseableRowStream stream =
                     RowStream.fromSourceBindings("ledger.source", bindings)) {
            StreamEvent event;
            while ((event = stream.next()) != null) events.add(event);
        }

        assertEquals(3, events.size());
        assertEquals(Arrays.asList(10, 11, 20), Arrays.asList(
                events.get(0).row().getInteger(0), events.get(1).row().getInteger(0),
                events.get(2).row().getInteger(0)));
        assertEquals(Arrays.asList(0L, 1L, 0L), Arrays.asList(
                events.get(0).sourcePosition().splitLocalOrdinal(),
                events.get(1).sourcePosition().splitLocalOrdinal(),
                events.get(2).sourcePosition().splitLocalOrdinal()));
        assertEquals(hex(BatchSourceSplitDescriptorCodec.digest(first.batchSourceSplit())),
                events.get(0).sourcePosition().splitIdentitySha256());
        assertEquals(hex(BatchSourceSplitDescriptorCodec.digest(second.batchSourceSplit())),
                events.get(2).sourcePosition().splitIdentitySha256());
        assertTrue(events.get(0).hasSourcePosition());
        assertSame(events.get(0).sourcePosition(), events.get(0).sourcePosition());
    }

    @Test
    void oneNextDoesNotOpenLaterSplitAndEarlyCloseIsIdempotent() {
        ProbeState first = new ProbeState("first", Arrays.asList(1, 2));
        ProbeState second = new ProbeState("second", Collections.singletonList(3));
        RowStream.CloseableRowStream stream = RowStream.fromSourceBindings(
                "probe.source", Arrays.asList(binding(first), binding(second)));

        StreamEvent firstEvent = stream.next();
        assertEquals(1, firstEvent.row().getInteger(0));
        assertEquals(0L, firstEvent.sourcePosition().splitLocalOrdinal());
        assertEquals(1, first.opens);
        assertEquals(0, second.opens);

        stream.close();
        stream.close();
        assertEquals(1, first.closes);
        assertEquals(0, second.opens);
        assertEquals(0, second.closes);
        assertEquals(null, stream.next());
    }

    @Test
    void cancellationDuringOpenRejectsLateExecutorPublication() throws Exception {
        ProbeState state = new ProbeState("open-race", Collections.singletonList(1));
        state.openStarted = new CountDownLatch(1);
        state.releaseOpen = new CountDownLatch(1);
        RowStream.CloseableRowStream stream = RowStream.fromSourceBindings(
                "probe.source", Collections.singletonList(binding(state)));
        FutureTask<StreamEvent> reading = new FutureTask<>(stream::next);
        Thread reader = new Thread(reading, "positioned-open-race");
        try {
            reader.start();
            assertTrue(state.openStarted.await(10, TimeUnit.SECONDS));

            stream.close();
            assertEquals(1, state.closes,
                    "cancel must actively close an exact executor whose open is blocked");

            assertEquals(null, reading.get(10, TimeUnit.SECONDS));
            assertEquals(1, state.closes,
                    "cancelled stream must reject and close a late-opened executor");
        } finally {
            state.releaseOpen.countDown();
            stream.close();
            reader.join(10_000);
        }
    }

    @Test
    void cancellationDuringHasNextDetachesOneCloseOwnerWithoutNpe() throws Exception {
        ProbeState state = new ProbeState("read-race", Collections.singletonList(1));
        state.readStarted = new CountDownLatch(1);
        state.releaseRead = new CountDownLatch(1);
        RowStream.CloseableRowStream stream = RowStream.fromSourceBindings(
                "probe.source", Collections.singletonList(binding(state)));
        FutureTask<StreamEvent> reading = new FutureTask<>(stream::next);
        Thread reader = new Thread(reading, "positioned-read-race");
        try {
            reader.start();
            assertTrue(state.readStarted.await(10, TimeUnit.SECONDS));

            stream.close();

            assertEquals(null, reading.get(10, TimeUnit.SECONDS));
            assertEquals(1, state.closes,
                    "cancel and exhaustion must atomically detach one exact-source owner");
        } finally {
            state.releaseRead.countDown();
            stream.close();
            reader.join(10_000);
        }
    }

    @Test
    void rejectsLegacyBeforeOpenAndPreservesPrimaryReadFailure() {
        ProbeState legacyState = new ProbeState("legacy", Collections.singletonList(1));
        BatchSourceSplitBinding legacy = new BatchSourceSplitBinding(
                new ProbeInput(legacyState), new BatchSourceSplitDescriptor(
                1, "probe", 1, "legacy", SchemaFingerprint.of(META), "snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT));
        assertInvalid(() -> RowStream.fromSourceBindings(
                "probe.source", Collections.singletonList(legacy)));
        assertEquals(0, legacyState.opens);

        ProbeState failing = new ProbeState("failing", Collections.singletonList(1));
        failing.failNext = true;
        failing.failClose = true;
        RowStream.CloseableRowStream stream = RowStream.fromSourceBindings(
                "probe.source", Collections.singletonList(binding(failing)));
        IllegalStateException failure = assertThrows(IllegalStateException.class, stream::next);
        assertTrue(failure.getMessage().contains("next-failing"));
        assertEquals(1, failure.getSuppressed().length);
        assertTrue(failure.getSuppressed()[0].getMessage().contains("close-failing"));
        assertEquals(1, failing.opens);
        assertEquals(1, failing.closes);
        stream.close();
    }

    @Test
    void ordinaryRecordHasNoPositionAndAccessorsFailClosed() {
        DataRow row = row(1);
        StreamEvent ordinary = StreamEvent.record(row);
        assertFalse(ordinary.hasSourcePosition());
        assertThrows(IllegalStateException.class, ordinary::sourcePosition);
        assertThrows(IllegalStateException.class,
                () -> StreamEvent.endOfStream().sourcePosition());

        SourceRecordPosition position = SourceRecordPosition.of(
                "ledger.source", descriptor("split", "snapshot"), 0L);
        StreamEvent positioned = StreamEvent.record(row, position);
        assertSame(row, positioned.row());
        assertSame(position, positioned.sourcePosition());
    }

    private static CollectionSource exact(String splitId, Integer... values) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer value : values) rows.add(row(value));
        return new CollectionSource(META, splitId, rows);
    }

    private static BatchSourceSplitBinding binding(CollectionSource input) {
        return new BatchSourceSplitBinding(input, input.batchSourceSplit());
    }

    private static BatchSourceSplitBinding binding(ProbeState state) {
        ProbeInput input = new ProbeInput(state);
        return new BatchSourceSplitBinding(input,
                descriptor("probe-" + state.name, "snapshot-" + state.name));
    }

    private static BatchSourceSplitDescriptor descriptor(String splitId, String snapshotId) {
        return new BatchSourceSplitDescriptor(
                1, "probe", 1, splitId, SchemaFingerprint.of(META), snapshotId,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static DataRow row(Integer value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            out.append(Character.forDigit((value >>> 4) & 0xf, 16));
            out.append(Character.forDigit(value & 0xf, 16));
        }
        return out.toString();
    }

    private static void assertInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("SOURCE_POSITION_INVALID:"),
                failure::getMessage);
    }

    private static final class ProbeState {
        final String name;
        final List<Integer> values;
        int opens;
        int closes;
        boolean failNext;
        boolean failClose;
        CountDownLatch openStarted;
        CountDownLatch releaseOpen;
        CountDownLatch readStarted;
        CountDownLatch releaseRead;

        private ProbeState(String name, List<Integer> values) {
            this.name = name;
            this.values = values;
        }
    }

    private static final class ProbeInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final transient ProbeState state;
        private final BatchSourceSplitDescriptor descriptor;

        private ProbeInput(ProbeState state) {
            this.state = state;
            this.descriptor = descriptor("probe-" + state.name, "snapshot-" + state.name);
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            throw new AssertionError("raw iterator fallback is forbidden");
        }
    }

    private static final class ProbeExecutor extends SourceExecutor<ProbeInput> {
        private int index;
        private boolean closed;

        private ProbeExecutor(ProbeInput input) {
            super(input, META);
        }

        @Override public void open() {
            input.state.opens++;
            if (input.state.openStarted != null) input.state.openStarted.countDown();
            await(input.state.releaseOpen);
        }

        @Override public boolean hasNext() {
            if (input.state.readStarted != null) input.state.readStarted.countDown();
            await(input.state.releaseRead);
            return index < input.state.values.size();
        }

        @Override public DataRow next() {
            if (input.state.failNext) {
                throw new IllegalStateException("next-" + input.state.name);
            }
            return row(input.state.values.get(index++));
        }

        @Override public void close() {
            if (closed) return;
            closed = true;
            input.state.closes++;
            if (input.state.releaseOpen != null) input.state.releaseOpen.countDown();
            if (input.state.releaseRead != null) input.state.releaseRead.countDown();
            if (input.state.failClose) {
                throw new IllegalStateException("close-" + input.state.name);
            }
        }
    }

    private static void await(CountDownLatch latch) {
        if (latch == null) return;
        boolean interrupted = false;
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}
