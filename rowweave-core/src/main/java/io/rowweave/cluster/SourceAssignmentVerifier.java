package io.rowweave.cluster;

import io.rowweave.AssignedSourceBatch;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.SourceSplitAssignment;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentToken;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** source ownership gates before acceptance and before SourceExecutor creation. */
final class SourceAssignmentVerifier {
    private SourceAssignmentVerifier() {
    }

    static void requireRoutingMatches(
            StageAssignmentToken token, StageCommand.Role commandRole,
            SourceSplitAssignment.Context context) {
        if (token == null) throw new IllegalArgumentException("assignment token is required");
        boolean tokenOwnsSource = token.role() == StageAssignmentRole.SOURCE_STAGE
                || token.role() == StageAssignmentRole.WHOLE_GRAPH;
        boolean commandOwnsSource = commandRole == StageCommand.Role.SOURCE_STAGE
                || commandRole == StageCommand.Role.WHOLE_GRAPH;
        if (tokenOwnsSource != commandOwnsSource) {
            throw new IllegalArgumentException("assignment token/command source role mismatch");
        }
        if (!tokenOwnsSource) {
            if (context != null || token.sourceSplitCount() != 0
                    || token.sourceOwnershipDigest() != null) {
                throw new IllegalArgumentException(
                        "non-source assignment carries source ownership");
            }
            return;
        }
        if (token.protocolVersion() < 2 || !token.hasAssignmentRecordDigest()
                || context == null || token.sourceSplitCount() <= 0
                || token.sourceOwnershipDigest() == null
                || token.logicalStage() != context.logicalStage()
                || token.instanceIndex() != context.instanceIndex()
                || token.sourceSplitCount() != context.sourceSplitCount()
                || !MessageDigest.isEqual(
                token.sourceOwnershipDigest(), context.ownershipDigest())) {
            throw new IllegalArgumentException("source ownership routing mismatch");
        }
    }

    static void requireMaterializedMatches(StageCommand command) {
        SourceSplitAssignment.Context context = command.sourceSplitAssignment();
        if (context == null) return;
        List<List<BatchSourceSplitBinding>> bindingGroups = new ArrayList<>();
        if (command.role() == StageCommand.Role.SOURCE_STAGE) {
            if (!(command.input() instanceof AssignedSourceBatch)
                    || context.sourceGroups().size() != 1) {
                throw new IllegalArgumentException(
                        "source stage requires one assigned source batch");
            }
            bindingGroups.add(((AssignedSourceBatch) command.input()).bindings());
        } else if (command.role() == StageCommand.Role.WHOLE_GRAPH) {
            PlanGraph graph = command.fragment();
            int groupIndex = 0;
            for (int nodeIndex = 0; nodeIndex < graph.nodeCount(); nodeIndex++) {
                PlanNode node = graph.node(nodeIndex);
                if (node.kind() != PlanNode.Kind.SOURCE
                        || node.sourceSplitBindings().isEmpty()) {
                    continue;
                }
                if (groupIndex >= context.sourceGroups().size()
                        || !operatorId(node, nodeIndex).equals(
                        context.sourceGroups().get(groupIndex).sourceOperatorId())) {
                    throw new IllegalArgumentException(
                            "whole-graph source operator ownership mismatch");
                }
                bindingGroups.add(node.sourceSplitBindings());
                groupIndex++;
            }
        } else {
            throw new IllegalArgumentException(
                    "source ownership is invalid for command role " + command.role());
        }
        context.bind(bindingGroups);
    }

    private static String operatorId(PlanNode node, int nodeIndex) {
        String explicit = node.param(PlanNode.OPERATOR_UID_PARAM);
        return explicit == null
                ? String.format(Locale.ROOT, "legacy-op-%06d", nodeIndex) : explicit;
    }
}
