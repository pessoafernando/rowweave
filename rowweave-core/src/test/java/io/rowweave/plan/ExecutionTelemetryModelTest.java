package io.rowweave.plan;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionTelemetryModelTest {

    @Test
    void stageTelemetryDistinguishesRealZeroFromUnavailableAndNotExecuted() {
        StageExecutionTelemetry zero = StageExecutionTelemetry.complete(0, 0, 0, 0, 0, 0);
        assertEquals(TelemetryCoverage.COMPLETE, zero.coverage());
        assertEquals(TelemetryCoverage.NOT_EXECUTED,
                StageExecutionTelemetry.notExecuted().coverage());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                StageExecutionTelemetry.unavailable().coverage());
        assertThrows(IllegalArgumentException.class,
                () -> StageExecutionTelemetry.complete(-1, 0, 0, 0, 0, 0));
    }

    @Test
    void accumulatorOverflowIsFailureNeutralAndNeverWraps() {
        StageExecutionTelemetryAccumulator accumulator =
                new StageExecutionTelemetryAccumulator();
        accumulator.add(StageExecutionTelemetry.complete(
                Long.MAX_VALUE, 1, 1, 1, 1, 1));

        assertDoesNotThrow(() -> accumulator.add(StageExecutionTelemetry.complete(
                1, 1, 1, 1, 1, 1)));
        StageExecutionTelemetry result = accumulator.freeze();
        assertEquals(TelemetryCoverage.UNAVAILABLE, result.coverage());
        assertEquals(0, result.managedPeakReservedBytes());
        accumulator.add(StageExecutionTelemetry.complete(1, 1, 1, 1, 1, 1));
        assertSame(result, accumulator.freeze(), "freeze is one-shot and stable");
    }

    @Test
    void executionReportCopiesLegacyArraysAndExposesTruthfulCoverage() {
        long[] legacyPartitions = {3, 7};
        ExecutionReport legacy = new ExecutionReport(10, 20, 1, 4, legacyPartitions);
        legacyPartitions[0] = 99;
        assertArrayEquals(new long[]{3, 7}, legacy.partitionRowCounts());
        assertEquals(TelemetryCoverage.UNAVAILABLE, legacy.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.UNAVAILABLE, legacy.partitionTelemetryCoverage());
        assertEquals(BudgetStatus.UNKNOWN, legacy.budgetStatus());

        StageExecutionTelemetry telemetry = StageExecutionTelemetry.complete(
                10, 20, 2, 3, 40, 16);
        PartitionDistribution distribution = new PartitionDistribution(
                2, PartitionRole.KEYED_OP, 2, TelemetryCoverage.COMPLETE,
                new boolean[]{true, true}, new long[]{3, 7});
        ExecutionReport complete = ExecutionReport.complete(
                telemetry, 4, Collections.singletonList(distribution));

        assertEquals(TelemetryCoverage.COMPLETE, complete.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.COMPLETE, complete.partitionTelemetryCoverage());
        assertEquals(BudgetStatus.WITHIN_BUDGET, complete.budgetStatus());
        assertEquals(2, complete.pressureSpillCount());
        assertEquals(3, complete.spillRunCount());
        assertEquals(40, complete.spillLogicalBytesWritten());
        assertEquals(16, complete.spillPeakReservedBytes());
        assertArrayEquals(new long[]{3, 7}, complete.partitionRowCounts());
        assertThrows(UnsupportedOperationException.class,
                () -> complete.partitionDistributions().add(distribution));

        ExecutionReport unavailable = ExecutionReport.fromTelemetry(
                StageExecutionTelemetry.unavailable(), 17, Collections.emptyList());
        assertEquals(17, unavailable.rowsOut(), "telemetry failure cannot erase result rows");
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                unavailable.resourceTelemetryCoverage());
        assertEquals(BudgetStatus.UNKNOWN, unavailable.budgetStatus());
    }

    @Test
    void partitionDistributionRejectsAmbiguousPresenceAndCopiesArrays() {
        boolean[] present = {true, true};
        long[] rows = {0, 5};
        PartitionDistribution distribution = new PartitionDistribution(
                1, PartitionRole.JOIN_CONSUMER, 2, TelemetryCoverage.COMPLETE,
                present, rows);
        present[0] = false;
        rows[1] = 99;
        assertTrue(distribution.isPresent(0));
        assertArrayEquals(new long[]{0, 5}, distribution.rowCounts());

        assertThrows(IllegalArgumentException.class, () -> new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.COMPLETE,
                new boolean[]{true, false}, new long[]{1, 0}));
        assertThrows(IllegalArgumentException.class, () -> new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.UNAVAILABLE,
                new boolean[]{true, false}, new long[]{1, 0}));
        assertThrows(IllegalArgumentException.class, () -> new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.PARTIAL,
                new boolean[]{true, false}, new long[]{1, 9}));
        assertThrows(IllegalArgumentException.class, () -> new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.PARTIAL,
                new boolean[]{true, true}, new long[]{1, 9}));
        assertThrows(IllegalArgumentException.class, () -> new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.PARTIAL,
                new boolean[]{false, false}, new long[]{0, 0}));
    }

    @Test
    void partitionAggregationOverflowIsFailureNeutralAndUnavailable() {
        PartitionDistribution distribution = PushDagExecutor.partitionDistribution(
                Arrays.asList(new long[]{Long.MAX_VALUE, 2}, new long[]{1, 3}),
                7, PartitionRole.KEYED_OP, 2);

        assertEquals(TelemetryCoverage.UNAVAILABLE, distribution.coverage());
        assertArrayEquals(new boolean[]{false, false}, distribution.presenceBitmap());
        assertArrayEquals(new long[]{0, 0}, distribution.rowCounts());
    }

    @Test
    void stageTelemetryCodecIsCanonicalBoundedAndFailsClosed() {
        StageExecutionTelemetry value = StageExecutionTelemetry.complete(
                Long.MAX_VALUE, Long.MAX_VALUE, 3, 4, 5, 6);
        byte[] encoded = StageExecutionTelemetryCodec.encode(value);
        assertTrue(encoded.length < StageExecutionTelemetryCodec.MAX_FRAME_BYTES);
        assertEquals(value, StageExecutionTelemetryCodec.decode(encoded));
        java.util.concurrent.atomic.AtomicLong resident =
                new java.util.concurrent.atomic.AtomicLong();
        assertEquals(value, StageExecutionTelemetryCodec.decode(
                encoded, resident::addAndGet));
        assertTrue(resident.get() > 0L,
                "checksum scratch and decoded value must be explicitly resident-charged");
        assertArrayEquals(encoded, StageExecutionTelemetryCodec.encode(
                StageExecutionTelemetryCodec.decode(encoded)));

        byte[] corrupt = encoded.clone();
        corrupt[10] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StageExecutionTelemetryCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> StageExecutionTelemetryCodec.decode(
                        Arrays.copyOf(encoded, encoded.length + 1)));
        assertEquals(StageExecutionTelemetry.notExecuted(),
                StageExecutionTelemetryCodec.decode(StageExecutionTelemetryCodec.encode(
                        StageExecutionTelemetry.notExecuted())));
        assertEquals(StageExecutionTelemetry.unavailable(),
                StageExecutionTelemetryCodec.decode(StageExecutionTelemetryCodec.encode(
                        StageExecutionTelemetry.unavailable())));
    }

    @Test
    void stageTelemetryCodecRejectsSemanticCorruptionWithAValidChecksum() throws Exception {
        byte[] complete = StageExecutionTelemetryCodec.encode(
                StageExecutionTelemetry.complete(1, 2, 3, 4, 5, 6));
        assertInvalidFrame(rechecksum(complete, bytes -> ByteBuffer.wrap(bytes).putInt(0, 0)));
        assertInvalidFrame(rechecksum(complete, bytes -> bytes[4] = 99));
        assertInvalidFrame(rechecksum(complete, bytes -> bytes[5] = 99));
        assertInvalidFrame(rechecksum(complete,
                bytes -> ByteBuffer.wrap(bytes).putLong(6, -1L)));

        byte[] notExecuted = StageExecutionTelemetryCodec.encode(
                StageExecutionTelemetry.notExecuted());
        assertInvalidFrame(rechecksum(notExecuted,
                bytes -> ByteBuffer.wrap(bytes).putLong(6, 1L)));
        byte[] unavailable = StageExecutionTelemetryCodec.encode(
                StageExecutionTelemetry.unavailable());
        assertInvalidFrame(rechecksum(unavailable,
                bytes -> ByteBuffer.wrap(bytes).putLong(14, 1L)));
    }

    private static void assertInvalidFrame(byte[] frame) {
        assertThrows(IllegalArgumentException.class,
                () -> StageExecutionTelemetryCodec.decode(frame));
    }

    private static byte[] rechecksum(byte[] original, FrameMutation mutation) throws Exception {
        byte[] changed = original.clone();
        mutation.apply(changed);
        int bodyLength = changed.length - 32;
        byte[] checksum = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(changed, bodyLength));
        System.arraycopy(checksum, 0, changed, bodyLength, checksum.length);
        return changed;
    }

    private interface FrameMutation {
        void apply(byte[] bytes);
    }
}
