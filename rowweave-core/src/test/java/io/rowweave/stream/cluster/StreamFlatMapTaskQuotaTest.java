package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.FlatMapFunction;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FLAT_MAP is synchronous pure Push with a per-input quota. */
class StreamFlatMapTaskQuotaTest {

    @Test
    void permitsZeroOneAndManyImmediateOutputs() {
        RecordingOutput output = new RecordingOutput();
        task((input, collector) -> { }, StreamEmissionLimit.of(3, 1_024L))
                .process(row(1L), null, output);
        task((input, collector) -> collector.collect(row(2L)),
                StreamEmissionLimit.of(3, 1_024L))
                .process(row(1L), 99L, output);
        task((input, collector) -> {
            collector.collect(row(3L));
            collector.collect(row(4L));
        }, StreamEmissionLimit.of(3, 1_024L))
                .process(row(1L), null, output);

        assertEquals(3, output.rows.size());
        assertEquals(2L, output.rows.get(0).getLong(0));
        assertEquals(Long.valueOf(99L), output.timestamps.get(0));
        assertEquals(3L, output.rows.get(1).getLong(0));
        assertEquals(4L, output.rows.get(2).getLong(0));
    }

    @Test
    void poisonsInvocationWhenRowOrByteQuotaViolationIsSwallowed() {
        RecordingOutput rows = new RecordingOutput();
        StreamOperatorTask rowBound = task((input, collector) -> {
            collector.collect(row(1L));
            collector.collect(row(2L));
            try {
                collector.collect(row(3L));
            } catch (RuntimeException ignored) {
                // User code cannot turn a quota breach back into success.
            }
        }, StreamEmissionLimit.of(2, 1_024L));
        assertThrows(IllegalStateException.class,
                () -> rowBound.process(row(0L), null, rows));
        assertEquals(2, rows.rows.size());

        RecordingOutput bytes = new RecordingOutput();
        StreamOperatorTask byteBound = task((input, collector) -> {
            try {
                collector.collect(row(1L));
            } catch (RuntimeException ignored) {
                // User code cannot hide a byte-bound breach either.
            }
        }, StreamEmissionLimit.of(2, 1L));
        assertThrows(IllegalStateException.class,
                () -> byteBound.process(row(0L), null, bytes));
        assertTrue(bytes.rows.isEmpty());
    }

    @Test
    void rejectsNullWidthAndTypeViolationsEvenWhenUserCodeCatchesThem() {
        assertPoisoned(collector -> collector.collect(null));
        assertPoisoned(collector -> collector.collect(new DataRow(0)));
        assertPoisoned(collector -> {
            DataRow wrong = new DataRow(1);
            wrong.set(0, "not-a-long");
            collector.collect(wrong);
        });
    }

    @Test
    void lateCollectorUseFailsTheOutputAttemptInsteadOfEmitting() {
        AtomicReference<Collector> retained = new AtomicReference<>();
        RecordingOutput output = new RecordingOutput();
        task((input, collector) -> retained.set(collector),
                StreamEmissionLimit.of(2, 1_024L))
                .process(row(0L), null, output);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> retained.get().collect(row(1L)));
        assertTrue(failure.getMessage().contains("after callback"));
        assertTrue(output.rows.isEmpty());
        assertNotNull(output.failure);
    }

    @Test
    void collectorCannotEmitFromAnotherThreadWhileCallbackIsStillRunning() {
        AtomicReference<Throwable> asynchronous = new AtomicReference<>();
        RecordingOutput output = new RecordingOutput();
        StreamOperatorTask task = task((input, collector) -> {
            Thread thread = new Thread(() -> {
                try {
                    collector.collect(row(1L));
                } catch (Throwable failure) {
                    asynchronous.set(failure);
                }
            }, "flat-map-async-emitter");
            thread.start();
            try {
                thread.join();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        }, StreamEmissionLimit.of(2, 1_024L));

        assertThrows(IllegalStateException.class,
                () -> task.process(row(0L), null, output));
        assertTrue(output.rows.isEmpty());
        assertNotNull(asynchronous.get());
        assertNotNull(output.failure);
    }

    private static void assertPoisoned(InvalidEmission emission) {
        RecordingOutput output = new RecordingOutput();
        StreamOperatorTask task = task((input, collector) -> {
            try {
                emission.emit(collector);
            } catch (RuntimeException ignored) {
                // Contract violation remains authoritative.
            }
        }, StreamEmissionLimit.of(2, 1_024L));
        assertThrows(IllegalArgumentException.class,
                () -> task.process(row(0L), null, output));
        assertTrue(output.rows.isEmpty());
    }

    private static StreamOperatorTask task(FlatMapFunction function,
            StreamEmissionLimit limit) {
        StreamSession session = RowWeave.createStreamSession("flat-map-quota");
        session.fromSource(source()).uid("source")
                .flatMap(reference(), schema(), limit).uid("flat-map")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(
                session.toPlanArtifact());
        return StreamOperatorTaskFactory.create(plan.unaryNodes.get(0),
                plan.source.outputSchema(), ignored -> function);
    }

    private static FunctionDeploymentReference reference() {
        byte[] config = new byte[] {1};
        return new FunctionDeploymentReference(
                digest("deployment"), "flat-map", digest("jar"),
                FlatMapFunction.class.getName(), FunctionAbi.FLAT_MAP_V1,
                "flat-map-config-v1", digest(config), config,
                new FunctionResourceProfile(1_024L, 0L, 4));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(schema(), "source",
                digest("source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink", digest("sink"), new byte[] {3});
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(new StreamField(
                "value", "value", "value", DataType.LongType, false)));
    }

    private static DataRow row(long value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static String digest(String text) {
        return digest(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    @FunctionalInterface
    private interface InvalidEmission {
        void emit(Collector collector);
    }

    private static final class RecordingOutput
            implements StreamOperatorTask.Sink {
        final List<DataRow> rows = new ArrayList<>();
        final List<Long> timestamps = new ArrayList<>();
        Throwable failure;

        @Override public void emit(DataRow row, Long eventTimestamp) {
            rows.add(row);
            timestamps.add(eventTimestamp);
        }

        @Override public void fail(Throwable failure) {
            this.failure = failure;
        }
    }
}
