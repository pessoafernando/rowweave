package io.rowweave.ops;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.SourceRecordPosition;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** positioned validation consumes and strips source-local metadata. */
class PositionedRowValidationStreamingTest {

    private static final String SOURCE_UID = "ledger.source";
    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("sequence", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final BatchSourceSplitDescriptor SPLIT = descriptor(
            "domain/private-ledger-object", "private-snapshot-token");

    @Test
    void validRecordKeepsRowIdentityButStripsPositionFromANewEvent() {
        DataRow row = row("A001", 1, 0L, BigDecimal.ZERO);
        StreamEvent input = positioned(row, SPLIT, 0L);
        StreamEvent eos = StreamEvent.endOfStream();

        List<StreamEvent> output = apply(Arrays.asList(input, eos));

        assertEquals(2, output.size());
        assertNotSame(input, output.get(0));
        assertSame(row, output.get(0).row());
        assertFalse(output.get(0).hasSourcePosition());
        assertSame(eos, output.get(1));
    }

    @Test
    void defaultValidationStillForwardsTheExactInputEvent() {
        StreamEvent input = positioned(row("A001", 1, 0L, BigDecimal.ZERO), SPLIT, 0L);
        List<StreamEvent> output = new ArrayList<>();
        validation().apply(singleEventStream(input), output::add);

        assertEquals(2, output.size());
        assertSame(input, output.get(0));
        assertTrue(output.get(0).hasSourcePosition());
        assertTrue(output.get(1).isEndOfStream());
    }

    @Test
    void allQualityFailuresAppendOnlyCanonicalPositionAndEmitNothing() {
        assertLocatedFailure(row("only-one-field"),
                "QUALITY_ROW_WIDTH_MISMATCH", null);
        assertLocatedFailure(row("A", "secret-period", 0L, BigDecimal.ZERO),
                "QUALITY_VALUE_TYPE_MISMATCH", "secret-period");
        assertLocatedFailure(row(null, 1, 0L, BigDecimal.ZERO),
                "QUALITY_RULE_VIOLATION", null);
    }

    @Test
    void missingUnknownAndWrongSourcePositionsFailClosedBeforeEmit() {
        DataRow valid = row("A", 1, 0L, BigDecimal.ZERO);
        assertPositionFailure(StreamEvent.record(valid));
        assertPositionFailure(StreamEvent.record(valid,
                SourceRecordPosition.of("another.source", SPLIT, 0L)));
        BatchSourceSplitDescriptor unknown = descriptor("another-split", "another-snapshot");
        assertPositionFailure(StreamEvent.record(valid,
                SourceRecordPosition.of(SOURCE_UID, unknown, 0L)));
    }

    @Test
    void duplicateOrSkippedOrdinalFailsClosed() {
        DataRow valid = row("A", 1, 0L, BigDecimal.ZERO);
        assertPositionFailure(positioned(valid, SPLIT, 1L));

        List<StreamEvent> emitted = new ArrayList<>();
        List<StreamEvent> input = Arrays.asList(
                positioned(valid, SPLIT, 0L), positioned(valid, SPLIT, 0L));
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> applyInto(input, emitted));
        assertTrue(failure.getMessage().startsWith("QUALITY_SOURCE_POSITION_INVALID:"));
        assertEquals(1, emitted.size());
        assertFalse(emitted.get(0).hasSourcePosition());
    }

    @Test
    void barrierErrorAndCancelKeepExistingTerminalSemantics() {
        StreamEvent barrier = StreamEvent.barrier(3L);
        RuntimeException upstream = new RuntimeException("upstream-secret");
        StreamEvent error = StreamEvent.error(upstream);
        List<StreamEvent> errorOutput = apply(Arrays.asList(barrier, error,
                positioned(row("A", 1, 0L, BigDecimal.ZERO), SPLIT, 0L)));
        assertEquals(2, errorOutput.size());
        assertSame(barrier, errorOutput.get(0));
        assertSame(error, errorOutput.get(1));

        StreamEvent cancel = StreamEvent.cancel();
        List<StreamEvent> cancelOutput = apply(Arrays.asList(cancel,
                positioned(row("A", 1, 0L, BigDecimal.ZERO), SPLIT, 0L)));
        assertEquals(Collections.singletonList(cancel), cancelOutput);
    }

    private static void assertLocatedFailure(DataRow row, String prefix, String forbidden) {
        List<StreamEvent> output = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> applyInto(Collections.singletonList(positioned(row, SPLIT, 0L)), output));
        String message = failure.getMessage();
        assertTrue(message.startsWith(prefix), message);
        assertTrue(message.contains(" source=" + SOURCE_UID), message);
        assertTrue(message.matches(".* split=[0-9a-f]{64} ordinal=0$"), message);
        assertFalse(message.contains(SPLIT.splitId()), message);
        assertFalse(message.contains(SPLIT.snapshotId()), message);
        if (forbidden != null) assertFalse(message.contains(forbidden), message);
        assertTrue(output.isEmpty());
    }

    private static void assertPositionFailure(StreamEvent event) {
        List<StreamEvent> output = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> applyInto(Collections.singletonList(event), output));
        assertTrue(failure.getMessage().startsWith("QUALITY_SOURCE_POSITION_INVALID:"),
                failure::getMessage);
        assertFalse(failure.getMessage().contains(SPLIT.splitId()));
        assertFalse(failure.getMessage().contains(SPLIT.snapshotId()));
        assertTrue(output.isEmpty());
    }

    private static List<StreamEvent> apply(List<StreamEvent> input) {
        List<StreamEvent> output = new ArrayList<>();
        applyInto(input, output);
        return output;
    }

    private static void applyInto(List<StreamEvent> input, List<StreamEvent> output) {
        Iterator<StreamEvent> iterator = input.iterator();
        validation().applyWithSourcePosition(
                () -> iterator.hasNext() ? iterator.next() : null,
                output::add, SOURCE_UID, Collections.singletonList(SPLIT));
    }

    private static RowStream singleEventStream(StreamEvent event) {
        boolean[] emitted = {false};
        return () -> {
            if (emitted[0]) return null;
            emitted[0] = true;
            return event;
        };
    }

    private static RowValidation validation() {
        return RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .longRange("sequence.range", 2, 0L, 100L)
                .decimalRange("amount.range", 3,
                        new BigDecimal("-10"), new BigDecimal("10"))
                .build();
    }

    private static StreamEvent positioned(
            DataRow row, BatchSourceSplitDescriptor descriptor, long ordinal) {
        return StreamEvent.record(row,
                SourceRecordPosition.of(SOURCE_UID, descriptor, ordinal));
    }

    private static BatchSourceSplitDescriptor descriptor(String splitId, String snapshotId) {
        return new BatchSourceSplitDescriptor(
                1, "probe", 1, splitId, SchemaFingerprint.of(META), snapshotId,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
