package io.rowweave.cluster;

import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.PartitionRole;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EphemeralExecutionTelemetryAggregatorTest {

    @Test
    void aggregatesEachPhysicalAssignmentOnceAndKeepsLogicalStageDistribution() {
        ExecutionTelemetrySummary summary = EphemeralExecutionTelemetryAggregator.aggregate(
                Arrays.asList(
                        physical(0, 0, StageCommand.Role.SOURCE_STAGE, 1, 13, complete(1)),
                        physical(1, 0, StageCommand.Role.KEYED_OP, 2, 7, complete(10)),
                        physical(1, 1, StageCommand.Role.KEYED_OP, 2, 6, complete(20)),
                        physical(2, 0, StageCommand.Role.SINK, 1, 4, complete(30))));

        assertEquals(TelemetryCoverage.COMPLETE, summary.resourceCoverage());
        assertEquals(61L, summary.managedPeakReservedBytes());
        assertEquals(65L, summary.managedAcquiredBytes());
        assertEquals(69L, summary.pressureSpillCount());
        assertEquals(73L, summary.spillRunCount());
        assertEquals(77L, summary.spillLogicalBytesWritten());
        assertEquals(81L, summary.spillPeakReservedBytes());
        assertEquals(TelemetryCoverage.COMPLETE, summary.partitionCoverage());
        assertEquals(1, summary.partitionDistributions().size());
        assertEquals(PartitionRole.KEYED_OP,
                summary.partitionDistributions().get(0).role());
        assertArrayEquals(new long[]{7, 6},
                summary.partitionDistributions().get(0).rowCounts());
    }

    @Test
    void missingResourcePayloadIsPartialButDoesNotEraseCompleteRowDistribution() {
        ExecutionTelemetrySummary summary = EphemeralExecutionTelemetryAggregator.aggregate(
                Arrays.asList(
                        physical(4, 0, StageCommand.Role.JOIN_CONSUMER, 2, 9, complete(1)),
                        physical(4, 1, StageCommand.Role.JOIN_CONSUMER, 2, 0,
                                StageExecutionTelemetry.unavailable())));

        assertEquals(TelemetryCoverage.PARTIAL, summary.resourceCoverage());
        assertEquals(1L, summary.managedPeakReservedBytes());
        assertEquals(TelemetryCoverage.COMPLETE, summary.partitionCoverage());
        assertArrayEquals(new long[]{9, 0},
                summary.partitionDistributions().get(0).rowCounts());
    }

    @Test
    void anyCheckedAddOverflowInvalidatesOnlyResourceSummary() {
        ExecutionTelemetrySummary summary = EphemeralExecutionTelemetryAggregator.aggregate(
                Arrays.asList(
                        physical(5, 0, StageCommand.Role.KEYED_OP, 2, 1,
                                StageExecutionTelemetry.complete(
                                        Long.MAX_VALUE, 0, 0, 0, 0, 0)),
                        physical(5, 1, StageCommand.Role.KEYED_OP, 2, 2,
                                StageExecutionTelemetry.complete(1, 0, 0, 0, 0, 0))));

        assertEquals(TelemetryCoverage.UNAVAILABLE, summary.resourceCoverage());
        assertEquals(0L, summary.managedPeakReservedBytes());
        assertEquals(TelemetryCoverage.COMPLETE, summary.partitionCoverage());
        assertArrayEquals(new long[]{1, 2},
                summary.partitionDistributions().get(0).rowCounts());
    }

    @Test
    void globalPartitionSlotLimitFailsClosedBeforeBuildingAnOversizedSummary() {
        List<EphemeralExecutionTelemetryAggregator.Physical> physical = new ArrayList<>();
        for (int stage = 0; stage < ExecutionTelemetrySummary.MAX_DISTRIBUTIONS; stage++) {
            physical.add(physical(stage, 0, StageCommand.Role.KEYED_OP,
                    ExecutionTelemetrySummary.MAX_PARTITION_SLOTS, 1, complete(1)));
        }

        ExecutionTelemetrySummary summary =
                EphemeralExecutionTelemetryAggregator.aggregate(physical);

        assertEquals(TelemetryCoverage.COMPLETE, summary.resourceCoverage());
        assertEquals(TelemetryCoverage.UNAVAILABLE, summary.partitionCoverage());
        assertEquals(0, summary.partitionDistributions().size());
    }

    private static EphemeralExecutionTelemetryAggregator.Physical physical(
            int logicalStage, int instance, StageCommand.Role role, int parallelism,
            long inputRows, StageExecutionTelemetry telemetry) {
        return new EphemeralExecutionTelemetryAggregator.Physical(
                logicalStage, instance, role, parallelism,
                inputRows, true, telemetry);
    }

    private static StageExecutionTelemetry complete(long base) {
        return StageExecutionTelemetry.complete(
                base, base + 1, base + 2, base + 3, base + 4, base + 5);
    }
}
