package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointManifestCodec;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamProcessBinding;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Conservative wire-size admission for one portable graph's checkpoint proof. */
final class StreamCheckpointTransferBudget {
    private static final long IDENTITY_RESERVE = 2_048L;
    private static final long EVIDENCE_WIRE_BYTES = 162L;
    // Member fields are bounded to 256/512/256/64/64 UTF-8 bytes. Their
    // fixed fields plus the 48-byte frame total at most 1,258 bytes.
    private static final long MEMBER_FRAME_BYTES = 1_280L;
    private static final long DECISION_FRAME_BYTES = 4_144L;
    private static final long FRAME_OVERHEAD = 48L;

    private StreamCheckpointTransferBudget() { }

    static void validate(StreamProductExecutionGraph graph,
            StreamWorkerPlacement placement) {
        if (graph == null || placement == null) {
            throw new IllegalArgumentException(
                    "Stream checkpoint budget requires graph and placement");
        }
        Map<StreamTaskIdentity, Integer> incoming = new HashMap<>();
        if (graph.job.checkpointConfig().mode()
                == io.rowweave.stream.api.StreamCheckpointMode.UNALIGNED) {
            for (StreamPhysicalExecutionPlan.Lane lane
                    : StreamPhysicalExecutionPlan.from(
                    graph.executionGraph, placement).lanes()) {
                incoming.merge(lane.target(), 1, Math::addExact);
            }
        }
        long bodyBytes = IDENTITY_RESERVE + Integer.BYTES;
        int largestTaskMembers = 0;
        for (StreamWorkerPlacement.Assignment assignment
                : placement.assignments()) {
            PortableStreamNode node = graph.executionGraph.node(
                    assignment.operatorUid()).portableNode();
            int members = members(node) + incoming.getOrDefault(
                    assignment.identity(), 0);
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && assignment.subtaskIndex() == 0) {
                // The coordinator has a separate receipt but its member is
                // restored together with source subtask zero.
                bodyBytes = addReceipt(bodyBytes,
                        "stream-source-coordinator:"
                                + assignment.operatorUid(), 256, 1);
                members = Math.addExact(members, 1);
            }
            if (members < 1) continue;
            if (members > 256) {
                throw limit("per-task checkpoint evidence exceeds 256");
            }
            largestTaskMembers = Math.max(largestTaskMembers, members);
            bodyBytes = addReceipt(bodyBytes,
                    "stream-task:" + assignment.taskOrdinal(),
                    assignment.workerIncarnation().getBytes(
                            StandardCharsets.UTF_8).length,
                    members - (node.opcode() == StreamOperatorCode.SOURCE
                            && assignment.subtaskIndex() == 0 ? 1 : 0));
        }
        validateFrameSizes(bodyBytes, largestTaskMembers);
    }

    static void validateFrameSizes(long bodyBytes,
            int largestTaskMembers) {
        if (bodyBytes < 1L || largestTaskMembers < 0
                || largestTaskMembers > 256) {
            throw limit("checkpoint proof cardinality is invalid");
        }
        if (bodyBytes > CheckpointManifestCodec.MAX_FRAME_BYTES
                - FRAME_OVERHEAD) {
            throw limit("checkpoint manifest exceeds its frame lease");
        }
        long manifestFrameBytes = Math.addExact(bodyBytes,
                FRAME_OVERHEAD);
        long bundleBytes = FRAME_OVERHEAD + 4L + DECISION_FRAME_BYTES
                + 4L + manifestFrameBytes + 4L
                + (4L + MEMBER_FRAME_BYTES) * largestTaskMembers;
        if (bundleBytes
                > StreamCheckpointTransferBundleCodec.MAX_FRAME_BYTES) {
            throw limit("per-task restore proof exceeds its frame lease");
        }
    }

    private static long addReceipt(long bytes, String taskId,
            int workerIncarnationBytes, int evidenceCount) {
        if (evidenceCount < 1) return bytes;
        long taskBytes = taskId.getBytes(StandardCharsets.UTF_8).length;
        return Math.addExact(bytes, 4L + taskBytes + 8L + 4L
                + workerIncarnationBytes + 4L
                + EVIDENCE_WIRE_BYTES * evidenceCount);
    }

    private static int members(PortableStreamNode node) {
        switch (node.opcode()) {
            case SOURCE:
            case SINK:
            case EVENT_TIME_WINDOW:
            case GENERIC_WINDOW:
            case KEYED_AGGREGATE:
            case ASYNC_WAIT:
            case ASSIGN_EVENT_TIME:
                return 1;
            case TEMPORAL_JOIN:
                return 2;
            case KEYED_PROCESS:
            case KEYED_CO_PROCESS:
                StreamProcessBinding process =
                        (StreamProcessBinding) node.binding();
                return process.managedStates().size()
                        + process.timerFamilies().size()
                        + (process.broadcastState() == null ? 0 : 2)
                        + (node.opcode()
                        == StreamOperatorCode.KEYED_CO_PROCESS ? 1 : 0);
            default:
                return 0;
        }
    }

    private static IllegalArgumentException limit(String detail) {
        return new IllegalArgumentException(
                "Stream checkpoint metadata resource limit: " + detail);
    }
}
