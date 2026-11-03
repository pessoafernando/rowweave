package io.rowweave.cluster;

import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.PartitionDistribution;
import io.rowweave.plan.PartitionRole;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Failure-neutral aggregation of unique ephemeral physical assignment facts. */
final class EphemeralExecutionTelemetryAggregator {
    static final class Physical {
        final int logicalStage;
        final int instanceIndex;
        final StageCommand.Role role;
        final int parallelism;
        final long inputRows;
        final boolean workerTerminal;
        final StageExecutionTelemetry telemetry;

        Physical(int logicalStage, int instanceIndex, StageCommand.Role role,
                 int parallelism, long inputRows, boolean workerTerminal,
                 StageExecutionTelemetry telemetry) {
            this.logicalStage = logicalStage;
            this.instanceIndex = instanceIndex;
            this.role = role;
            this.parallelism = parallelism;
            this.inputRows = inputRows;
            this.workerTerminal = workerTerminal;
            this.telemetry = telemetry == null
                    ? StageExecutionTelemetry.unavailable() : telemetry;
        }
    }

    private static final class DistributionBuilder {
        final int logicalStage;
        final PartitionRole role;
        final int parallelism;
        final boolean[] present;
        final long[] rows;
        boolean invalid;

        DistributionBuilder(Physical physical, PartitionRole role) {
            this.logicalStage = physical.logicalStage;
            this.role = role;
            this.parallelism = physical.parallelism;
            this.present = new boolean[parallelism];
            this.rows = new long[parallelism];
        }

        void add(Physical physical, PartitionRole expectedRole) {
            if (physical.parallelism != parallelism || expectedRole != role
                    || physical.instanceIndex < 0 || physical.instanceIndex >= parallelism
                    || physical.inputRows < 0L || present[physical.instanceIndex]) {
                invalid = true;
                return;
            }
            if (physical.workerTerminal) {
                present[physical.instanceIndex] = true;
                rows[physical.instanceIndex] = physical.inputRows;
            }
        }

        PartitionDistribution build() {
            int count = 0;
            for (boolean value : present) if (value) count++;
            TelemetryCoverage coverage = count == parallelism
                    ? TelemetryCoverage.COMPLETE
                    : (count == 0 ? TelemetryCoverage.UNAVAILABLE
                    : TelemetryCoverage.PARTIAL);
            return new PartitionDistribution(logicalStage, role, parallelism,
                    coverage, present, rows);
        }
    }

    private EphemeralExecutionTelemetryAggregator() { }

    static ExecutionTelemetrySummary aggregate(Iterable<Physical> physicalAssignments) {
        if (physicalAssignments == null) {
            return ExecutionTelemetrySummary.unavailable();
        }
        long peak = 0L;
        long acquired = 0L;
        long pressure = 0L;
        long runs = 0L;
        long written = 0L;
        long spillPeak = 0L;
        int completeResources = 0;
        boolean resourceInvalid = false;
        Map<Integer, DistributionBuilder> keyed = new TreeMap<>();
        boolean partitionInvalid = false;
        int declaredPartitionSlots = 0;
        int assignmentCount = 0;
        for (Physical physical : physicalAssignments) {
            if (physical == null) {
                resourceInvalid = true;
                partitionInvalid = true;
                continue;
            }
            assignmentCount++;
            if (physical.telemetry.coverage() == TelemetryCoverage.COMPLETE) {
                completeResources++;
                try {
                    peak = Math.addExact(peak,
                            physical.telemetry.managedPeakReservedBytes());
                    acquired = Math.addExact(acquired,
                            physical.telemetry.managedAcquiredBytes());
                    pressure = Math.addExact(pressure,
                            physical.telemetry.pressureSpillCount());
                    runs = Math.addExact(runs, physical.telemetry.spillRunCount());
                    written = Math.addExact(written,
                            physical.telemetry.spillLogicalBytesWritten());
                    spillPeak = Math.addExact(spillPeak,
                            physical.telemetry.spillPeakReservedBytes());
                } catch (ArithmeticException overflow) {
                    resourceInvalid = true;
                }
            }
            PartitionRole partitionRole = partitionRole(physical.role);
            if (partitionRole == null || physical.parallelism <= 1) continue;
            if (partitionInvalid) continue;
            if (physical.parallelism > ExecutionTelemetrySummary.MAX_PARTITION_SLOTS
                    || physical.logicalStage < 0) {
                partitionInvalid = true;
                continue;
            }
            DistributionBuilder builder = keyed.get(physical.logicalStage);
            if (builder == null) {
                if (keyed.size() >= ExecutionTelemetrySummary.MAX_DISTRIBUTIONS) {
                    partitionInvalid = true;
                    continue;
                }
                if (declaredPartitionSlots
                        > ExecutionTelemetrySummary.MAX_PARTITION_SLOTS
                        - physical.parallelism) {
                    partitionInvalid = true;
                    continue;
                }
                builder = new DistributionBuilder(physical, partitionRole);
                keyed.put(physical.logicalStage, builder);
                declaredPartitionSlots += physical.parallelism;
            }
            builder.add(physical, partitionRole);
            partitionInvalid |= builder.invalid;
        }

        TelemetryCoverage resourceCoverage;
        if (resourceInvalid || completeResources == 0) {
            resourceCoverage = TelemetryCoverage.UNAVAILABLE;
            peak = acquired = pressure = runs = written = spillPeak = 0L;
        } else if (completeResources == assignmentCount) {
            resourceCoverage = TelemetryCoverage.COMPLETE;
        } else {
            resourceCoverage = TelemetryCoverage.PARTIAL;
        }

        if (assignmentCount == 0) {
            return ExecutionTelemetrySummary.unavailable();
        }
        List<PartitionDistribution> distributions = new ArrayList<>(keyed.size());
        int slots = 0;
        boolean allComplete = !keyed.isEmpty();
        boolean anyPresent = false;
        for (DistributionBuilder builder : keyed.values()) {
            if (builder.invalid) {
                partitionInvalid = true;
                break;
            }
            slots += builder.parallelism;
            if (slots > ExecutionTelemetrySummary.MAX_PARTITION_SLOTS) {
                partitionInvalid = true;
                break;
            }
            PartitionDistribution distribution = builder.build();
            distributions.add(distribution);
            allComplete &= distribution.coverage() == TelemetryCoverage.COMPLETE;
            for (int i = 0; i < distribution.parallelism(); i++) {
                anyPresent |= distribution.isPresent(i);
            }
        }
        TelemetryCoverage partitionCoverage;
        if (partitionInvalid) {
            distributions = Collections.emptyList();
            partitionCoverage = TelemetryCoverage.UNAVAILABLE;
        } else if (distributions.isEmpty()) {
            partitionCoverage = TelemetryCoverage.NOT_APPLICABLE;
        } else if (allComplete) {
            partitionCoverage = TelemetryCoverage.COMPLETE;
        } else if (anyPresent) {
            partitionCoverage = TelemetryCoverage.PARTIAL;
        } else {
            partitionCoverage = TelemetryCoverage.UNAVAILABLE;
        }
        try {
            return new ExecutionTelemetrySummary(resourceCoverage, peak, acquired,
                    pressure, runs, written, spillPeak,
                    partitionCoverage, distributions);
        } catch (RuntimeException malformed) {
            return ExecutionTelemetrySummary.unavailable();
        }
    }

    private static PartitionRole partitionRole(StageCommand.Role role) {
        if (role == StageCommand.Role.KEYED_OP) return PartitionRole.KEYED_OP;
        if (role == StageCommand.Role.JOIN_CONSUMER) return PartitionRole.JOIN_CONSUMER;
        return null;
    }
}
