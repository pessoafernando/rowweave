package io.rowweave.cluster;

import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Deterministic, failure-neutral projection of one coherent durable evidence view. */
final class DurableExecutionTelemetryResolver {
    private DurableExecutionTelemetryResolver() { }

    static ExecutionTelemetrySummary resolve(DurableExecutionEvidenceView view) {
        if (view == null || view.state() != DurableExecutionEvidenceView.State.PRESENT) {
            return ExecutionTelemetrySummary.unavailable();
        }
        try {
            RunSnapshot run = view.snapshot();
            StageAssignmentSnapshot assignments = view.assignments();
            if (run.status() != RunStatus.SUCCEEDED
                    || run.resultSummary() == null
                    || assignments == null) {
                return ExecutionTelemetrySummary.unavailable();
            }
            // Revalidate at the resolver boundary so a future authority implementation cannot
            // accidentally turn mismatched evidence into advisory facts.
            DurableExecutionEvidenceView.present(run, assignments);
            return aggregate(assignments);
        } catch (RuntimeException | LinkageError invalidEvidence) {
            return ExecutionTelemetrySummary.unavailable();
        }
    }

    private static ExecutionTelemetrySummary aggregate(StageAssignmentSnapshot assignments) {
        Map<Integer, PartitionShape> shapes = new HashMap<>();
        for (StageAssignmentRecord record : assignments.manifest().assignments()) {
            StageCommand.Role role = stageRole(record.role());
            if (role == null) continue;
            PartitionShape shape = shapes.get(record.logicalStage());
            if (shape == null) {
                shape = new PartitionShape(role);
                shapes.put(record.logicalStage(), shape);
            }
            shape.add(record, role);
        }
        List<EphemeralExecutionTelemetryAggregator.Physical> physical =
                new ArrayList<>(assignments.manifest().assignmentCount());
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            StageAssignmentRecord record = assignments.manifest().assignment(i);
            StageTerminalReceipt terminal = assignments.terminal(i);
            StageCommand.Role role = stageRole(record.role());
            PartitionShape shape = role == null ? null : shapes.get(record.logicalStage());
            if (shape != null && !shape.valid()) {
                // Feed an impossible identity to the common bounded aggregator. Resource facts
                // remain usable while partition coverage fails closed.
                physical.add(new EphemeralExecutionTelemetryAggregator.Physical(
                        -1, record.instanceIndex(), role,
                        Math.max(2, shape.parallelism()),
                        terminal == null ? 0L : terminal.inputRows(), terminal != null,
                        terminalTelemetry(terminal)));
                continue;
            }
            physical.add(new EphemeralExecutionTelemetryAggregator.Physical(
                    record.logicalStage(), record.instanceIndex(),
                    role == null ? StageCommand.Role.ROUTER : role,
                    shape == null ? 1 : shape.parallelism(),
                    terminal == null ? 0L : terminal.inputRows(), terminal != null,
                    terminalTelemetry(terminal)));
        }
        return EphemeralExecutionTelemetryAggregator.aggregate(physical);
    }

    private static StageExecutionTelemetry terminalTelemetry(StageTerminalReceipt terminal) {
        return terminal == null
                ? StageExecutionTelemetry.unavailable() : terminal.telemetry();
    }

    private static StageCommand.Role stageRole(StageAssignmentRole role) {
        if (role == StageAssignmentRole.KEYED_OP) return StageCommand.Role.KEYED_OP;
        if (role == StageAssignmentRole.JOIN_CONSUMER) return StageCommand.Role.JOIN_CONSUMER;
        return null;
    }

    private static final class PartitionShape {
        final StageCommand.Role role;
        final java.util.Set<Integer> instances = new java.util.HashSet<>();
        int maxInstance = -1;
        boolean invalid;

        PartitionShape(StageCommand.Role role) { this.role = role; }

        void add(StageAssignmentRecord record, StageCommand.Role observedRole) {
            if (role != observedRole || !instances.add(record.instanceIndex())) {
                invalid = true;
            }
            maxInstance = Math.max(maxInstance, record.instanceIndex());
        }

        int parallelism() { return maxInstance + 1; }

        boolean valid() {
            return !invalid && parallelism() == instances.size()
                    && parallelism() <= ExecutionTelemetrySummary.MAX_PARTITION_SLOTS;
        }
    }
}
