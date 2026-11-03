package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamEdge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Immutable expansion of logical Stream edges into placed physical lanes. */
final class StreamPhysicalExecutionPlan {
    static final class Lane {
        private final StreamEdge logicalEdge;
        private final StreamTaskIdentity source;
        private final StreamTaskIdentity target;
        private final StreamWorkerPlacement.Assignment sourceAssignment;
        private final StreamWorkerPlacement.Assignment targetAssignment;
        private final String physicalChannelId;

        private Lane(StreamEdge logicalEdge,
                StreamWorkerPlacement.Assignment sourceAssignment,
                StreamWorkerPlacement.Assignment targetAssignment) {
            this(logicalEdge, sourceAssignment, targetAssignment, null);
        }

        private Lane(StreamEdge logicalEdge,
                StreamWorkerPlacement.Assignment sourceAssignment,
                StreamWorkerPlacement.Assignment targetAssignment,
                String physicalChannelId) {
            this.logicalEdge = logicalEdge;
            this.sourceAssignment = sourceAssignment;
            this.targetAssignment = targetAssignment;
            this.source = sourceAssignment.identity();
            this.target = targetAssignment.identity();
            this.physicalChannelId = physicalChannelId == null
                    ? logicalEdge.logicalChannelId() + '#'
                    + source + "->" + target : physicalChannelId;
        }

        StreamEdge logicalEdge() { return logicalEdge; }
        StreamTaskIdentity source() { return source; }
        StreamTaskIdentity target() { return target; }
        StreamEdge.Partitioning partitioning() {
            return logicalEdge.partitioning();
        }
        StreamWorkerPlacement.Assignment sourceAssignment() {
            return sourceAssignment;
        }
        StreamWorkerPlacement.Assignment targetAssignment() {
            return targetAssignment;
        }
        String physicalChannelId() { return physicalChannelId; }
        boolean crossesWorker() {
            return !sourceAssignment.workerId().equals(
                    targetAssignment.workerId())
                    || !sourceAssignment.workerIncarnation().equals(
                    targetAssignment.workerIncarnation());
        }
    }

    private final List<StreamWorkerPlacement.Assignment> tasks;
    private final List<Lane> lanes;

    private StreamPhysicalExecutionPlan(
            List<StreamWorkerPlacement.Assignment> tasks, List<Lane> lanes) {
        this.tasks = Collections.unmodifiableList(new ArrayList<>(tasks));
        this.lanes = Collections.unmodifiableList(new ArrayList<>(lanes));
    }

    static StreamPhysicalExecutionPlan from(StreamExecutionGraph graph,
            StreamWorkerPlacement placement) {
        if (graph == null || placement == null) {
            throw new IllegalArgumentException(
                    "Stream graph and placement are required");
        }
        Map<StreamTaskIdentity, StreamWorkerPlacement.Assignment> assigned =
                new LinkedHashMap<>();
        List<StreamWorkerPlacement.Assignment> tasks =
                placement.assignments();
        for (int ordinal = 0; ordinal < tasks.size(); ordinal++) {
            StreamWorkerPlacement.Assignment assignment = tasks.get(ordinal);
            if (assignment == null || assignment.taskOrdinal() != ordinal
                    || assigned.put(assignment.identity(), assignment)
                    != null) {
                throw new IllegalArgumentException(
                        "Stream physical placement is not canonical");
            }
        }
        int expectedTasks = 0;
        for (StreamExecutionGraph.Node node : graph.nodes()) {
            expectedTasks = Math.addExact(expectedTasks, node.parallelism());
            for (int subtask = 0; subtask < node.parallelism(); subtask++) {
                StreamTaskIdentity identity = new StreamTaskIdentity(
                        node.portableNode().operatorUid(), subtask);
                if (!assigned.containsKey(identity)) {
                    throw new IllegalArgumentException(
                            "Stream physical placement misses " + identity);
                }
            }
        }
        if (tasks.size() != expectedTasks) {
            throw new IllegalArgumentException(
                    "Stream physical placement contains unknown tasks");
        }

        List<Lane> lanes = new ArrayList<>();
        for (StreamExecutionGraph.Node targetNode : graph.nodes()) {
            for (StreamEdge edge : targetNode.inputEdges()) {
                StreamExecutionGraph.Node sourceNode = graph.node(
                        edge.sourceOperatorUid());
                for (int sourceIndex = 0;
                        sourceIndex < sourceNode.parallelism(); sourceIndex++) {
                    StreamWorkerPlacement.Assignment source = assigned.get(
                            new StreamTaskIdentity(
                                    edge.sourceOperatorUid(), sourceIndex));
                    int firstTarget = 0;
                    int lastTarget = targetNode.parallelism();
                    if (edge.partitioning()
                            == StreamEdge.Partitioning.FORWARD
                            && isPointwiseForward(sourceNode,
                            targetNode)) {
                        firstTarget = sourceIndex;
                        lastTarget = sourceIndex + 1;
                    }
                    for (int targetIndex = firstTarget;
                            targetIndex < lastTarget; targetIndex++) {
                        StreamWorkerPlacement.Assignment target = assigned.get(
                                new StreamTaskIdentity(
                                        edge.targetOperatorUid(), targetIndex));
                        lanes.add(new Lane(edge, source, target));
                    }
                }
            }
        }
        Map<String, Integer> occurrences = new HashMap<>();
        Set<String> legacyIds = new HashSet<>();
        for (Lane lane : lanes) {
            occurrences.merge(lane.physicalChannelId(), 1, Integer::sum);
            legacyIds.add(lane.physicalChannelId());
        }
        Set<String> assignedIds = new HashSet<>();
        List<Lane> unique = new ArrayList<>(lanes.size());
        for (Lane lane : lanes) {
            if (occurrences.get(lane.physicalChannelId()) == 1) {
                assignedIds.add(lane.physicalChannelId());
                unique.add(lane);
                continue;
            }
            // Existing runnable plans retain their checkpoint channel IDs.
            // Only a graph whose legacy IDs already collided uses an
            // edge-length-qualified identity; such a graph could not have
            // deployed or produced a durable channel cut previously.
            String marker = "@" + lane.logicalEdge().logicalChannelId()
                    .length() + ':';
            String candidate = marker + lane.physicalChannelId();
            while (legacyIds.contains(candidate)
                    || assignedIds.contains(candidate)) {
                marker = '@' + marker;
                candidate = marker + lane.physicalChannelId();
            }
            assignedIds.add(candidate);
            unique.add(new Lane(lane.logicalEdge(),
                    lane.sourceAssignment(), lane.targetAssignment(),
                    candidate));
        }
        return new StreamPhysicalExecutionPlan(tasks, unique);
    }

    private static boolean isPointwiseForward(
            StreamExecutionGraph.Node source,
            StreamExecutionGraph.Node target) {
        return source.parallelism() == target.parallelism();
    }

    List<StreamWorkerPlacement.Assignment> tasks() { return tasks; }
    List<Lane> lanes() { return lanes; }
}
