package io.rowweave.stream.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Deploys every placed node/subtask through the generic Worker task RPC.
 *
 * <p>The deployment order is deliberately independent of operator shape:
 * input listeners are prepared from sinks towards sources, then every task is
 * started in canonical identity order. Physical lanes, rather than a
 * source/sink shortcut, are the only connection description.</p>
 */
final class StreamGenericTaskDeployment {
    private StreamGenericTaskDeployment() { }

    static List<StreamTaskIdentity> deploy(
            StreamProductExecutionGraph admitted,
            byte[] artifactFrame,
            StreamWorkerPlacement placement,
            Map<String, RemoteStreamWorkerEndpoint> workers,
            int maximumEvents,
            Function<StreamWorkerPlacement.Assignment, byte[]> commandFor) {
        return deployWithTaskProof(admitted, artifactFrame, placement, workers,
                maximumEvents, ignored -> null, ignored -> null,
                commandFor);
    }

    static List<StreamTaskIdentity> deploy(
            StreamProductExecutionGraph admitted,
            byte[] artifactFrame,
            StreamWorkerPlacement placement,
            Map<String, RemoteStreamWorkerEndpoint> workers,
            int maximumEvents, byte[] checkpointBundleFrame,
            Function<StreamWorkerPlacement.Assignment, byte[]> commandFor) {
        return deployWithTaskProof(admitted, artifactFrame, placement, workers,
                maximumEvents, ignored -> checkpointBundleFrame,
                ignored -> null,
                commandFor);
    }

    static List<StreamTaskIdentity> deploy(
            StreamProductExecutionGraph admitted,
            byte[] artifactFrame,
            StreamWorkerPlacement placement,
            Map<String, RemoteStreamWorkerEndpoint> workers,
            int maximumEvents, byte[] checkpointBundleFrame,
            Function<StreamWorkerPlacement.Assignment, byte[]>
                    sourceAssignmentFor,
            Function<StreamWorkerPlacement.Assignment, byte[]> commandFor) {
        return deployWithTaskProof(admitted, artifactFrame, placement, workers,
                maximumEvents, ignored -> checkpointBundleFrame,
                sourceAssignmentFor, commandFor);
    }

    static List<StreamTaskIdentity> deployWithTaskProof(
            StreamProductExecutionGraph admitted,
            byte[] artifactFrame,
            StreamWorkerPlacement placement,
            Map<String, RemoteStreamWorkerEndpoint> workers,
            int maximumEvents,
            Function<StreamWorkerPlacement.Assignment, byte[]>
                    checkpointBundleFor,
            Function<StreamWorkerPlacement.Assignment, byte[]>
                    sourceAssignmentFor,
            Function<StreamWorkerPlacement.Assignment, byte[]> commandFor) {
        if (admitted == null || artifactFrame == null || placement == null
                || workers == null || checkpointBundleFor == null
                || sourceAssignmentFor == null
                || commandFor == null || maximumEvents <= 0) {
            throw new IllegalArgumentException(
                    "generic Stream task deployment is incomplete");
        }
        StreamExecutionGraph graph = admitted.executionGraph;
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(graph, placement);
        Map<StreamTaskIdentity, StreamWorkerPlacement.Assignment> assignments =
                new LinkedHashMap<>();
        for (StreamWorkerPlacement.Assignment assignment
                : placement.assignments()) {
            assignments.put(assignment.identity(), assignment);
        }
        Map<String, RemoteStreamWorkerEndpoint> byWorker =
                new HashMap<>(workers);
        Map<StreamTaskIdentity, List<StreamTaskLaneSpec>> inputs =
                new HashMap<>();
        Map<StreamTaskIdentity, List<StreamTaskLaneSpec>> outputs =
                new HashMap<>();
        Map<String, StreamTaskLaneSpec> endpoints = new HashMap<>();

        List<StreamExecutionGraph.Node> reverseNodes =
                new ArrayList<>(graph.nodes());
        Collections.reverse(reverseNodes);
        for (StreamExecutionGraph.Node node : reverseNodes) {
            for (int subtask = 0; subtask < node.parallelism(); subtask++) {
                StreamTaskIdentity identity = new StreamTaskIdentity(
                        node.portableNode().operatorUid(), subtask);
                List<StreamPhysicalExecutionPlan.Lane> incoming = incoming(
                        physical, identity);
                List<StreamPhysicalExecutionPlan.Lane> outgoing = outgoing(
                        physical, identity);
                List<StreamTaskLaneSpec> inputSpecs = new ArrayList<>();
                for (StreamPhysicalExecutionPlan.Lane lane : incoming) {
                    inputSpecs.add(input(lane));
                }
                List<StreamTaskLaneSpec> outputSpecs = new ArrayList<>();
                for (StreamPhysicalExecutionPlan.Lane lane : outgoing) {
                    StreamTaskLaneSpec targetEndpoint =
                            endpoints.get(lane.physicalChannelId());
                    if (targetEndpoint == null) {
                        throw new IllegalStateException(
                                "target input endpoint was not prepared for "
                                        + lane.physicalChannelId());
                    }
                    outputSpecs.add(StreamTaskLaneSpec.endpoint(
                            lane.physicalChannelId(), targetEndpoint.host(),
                            targetEndpoint.port(),
                            lane.logicalEdge().maximumFrameBytes(),
                            lane.logicalEdge().initialCreditBytes()));
                }
                StreamWorkerPlacement.Assignment assignment =
                        assignments.get(identity);
                RemoteStreamWorkerEndpoint worker = workerFor(assignment,
                        byWorker);
                List<StreamTaskLaneSpec> preparedEndpoints =
                        worker.prepareOperatorTask(commandFor.apply(assignment),
                        artifactFrame, maximumEvents,
                        identity.operatorUid(), identity.subtaskIndex(),
                                inputSpecs, outputSpecs,
                                checkpointBundleFor.apply(assignment),
                                sourceAssignmentFor.apply(assignment));
                inputs.put(identity, inputSpecs);
                outputs.put(identity, outputSpecs);
                for (StreamTaskLaneSpec endpoint : preparedEndpoints) {
                    StreamPhysicalExecutionPlan.Lane lane = laneById(
                            incoming, endpoint.laneId());
                    endpoints.put(lane.physicalChannelId(), endpoint);
                }
            }
        }

        List<StreamTaskIdentity> started = new ArrayList<>();
        List<StreamWorkerPlacement.Assignment> ordered =
                new ArrayList<>(placement.assignments());
        ordered.sort(Comparator.comparingInt(
                StreamWorkerPlacement.Assignment::taskOrdinal));
        Collections.reverse(ordered);
        try {
            for (StreamWorkerPlacement.Assignment assignment : ordered) {
                StreamTaskIdentity identity = assignment.identity();
                workerFor(assignment, byWorker).startOperatorTask(
                        commandFor.apply(assignment), artifactFrame,
                        maximumEvents, identity.operatorUid(),
                        identity.subtaskIndex(), inputs.get(identity),
                        outputs.get(identity),
                        checkpointBundleFor.apply(assignment),
                        sourceAssignmentFor.apply(assignment));
                started.add(identity);
            }
        } catch (RuntimeException failure) {
            throw failure;
        }
        return Collections.unmodifiableList(started);
    }

    private static RemoteStreamWorkerEndpoint workerFor(
            StreamWorkerPlacement.Assignment assignment,
            Map<String, RemoteStreamWorkerEndpoint> workers) {
        if (assignment == null) {
            throw new IllegalArgumentException("task placement is missing");
        }
        RemoteStreamWorkerEndpoint endpoint = workers.get(
                assignment.workerId());
        if (endpoint == null || !assignment.workerIncarnation().equals(
                endpoint.profile().workerIncarnation())) {
            throw new IllegalArgumentException(
                    "task placement references an unknown Worker incarnation");
        }
        return endpoint;
    }

    private static StreamTaskLaneSpec input(
            StreamPhysicalExecutionPlan.Lane lane) {
        return StreamTaskLaneSpec.input(lane.physicalChannelId(),
                lane.logicalEdge().maximumFrameBytes(),
                lane.logicalEdge().initialCreditBytes());
    }

    private static List<StreamPhysicalExecutionPlan.Lane> incoming(
            StreamPhysicalExecutionPlan physical, StreamTaskIdentity identity) {
        List<StreamPhysicalExecutionPlan.Lane> result = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.target().equals(identity)) result.add(lane);
        }
        return result;
    }

    private static List<StreamPhysicalExecutionPlan.Lane> outgoing(
            StreamPhysicalExecutionPlan physical, StreamTaskIdentity identity) {
        List<StreamPhysicalExecutionPlan.Lane> result = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.source().equals(identity)) result.add(lane);
        }
        return result;
    }

    private static StreamPhysicalExecutionPlan.Lane laneById(
            List<StreamPhysicalExecutionPlan.Lane> lanes, String laneId) {
        for (StreamPhysicalExecutionPlan.Lane lane : lanes) {
            if (lane.physicalChannelId().equals(laneId)) return lane;
        }
        throw new IllegalArgumentException(
                "Worker returned an unknown task input lane");
    }
}
