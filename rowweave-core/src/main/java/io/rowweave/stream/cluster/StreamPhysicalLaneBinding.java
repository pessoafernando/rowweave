package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamEdge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/** Resolves one physical lane against exact graph edge and task identities. */
final class StreamPhysicalLaneBinding {
    private static final Map<StreamExecutionGraph,
            Map<StreamTaskIdentity,
                    Map<String, StreamPhysicalExecutionPlan.Lane>>> INDEX =
            new WeakHashMap<>();

    private StreamPhysicalLaneBinding() { }

    static StreamEdge input(StreamExecutionGraph graph,
            StreamExecutionGraph.Node node,
            int targetSubtask, String laneId) {
        StreamPhysicalExecutionPlan.Lane lane = exact(
                graph, node, targetSubtask, laneId);
        if (lane != null && lane.target().operatorUid().equals(
                node.portableNode().operatorUid())
                && lane.target().subtaskIndex() == targetSubtask
                && node.inputEdges().contains(lane.logicalEdge())) {
            return lane.logicalEdge();
        }
        throw new IllegalArgumentException(
                "physical Stream input lane is not attached to an edge");
    }

    static StreamEdge output(StreamExecutionGraph graph,
            StreamExecutionGraph.Node node,
            int sourceSubtask, String laneId) {
        StreamPhysicalExecutionPlan.Lane lane = exact(
                graph, node, sourceSubtask, laneId);
        if (lane != null && lane.source().operatorUid().equals(
                node.portableNode().operatorUid())
                && lane.source().subtaskIndex() == sourceSubtask
                && node.outputEdges().contains(lane.logicalEdge())) {
            return lane.logicalEdge();
        }
        throw new IllegalArgumentException(
                "physical Stream output lane is not attached to an edge");
    }

    private static StreamPhysicalExecutionPlan.Lane exact(
            StreamExecutionGraph graph, StreamExecutionGraph.Node node,
            int subtask, String laneId) {
        if (graph == null || laneId == null) {
            throw new IllegalArgumentException(
                    "physical Stream lane identity is incomplete");
        }
        return taskIndex(graph, node, subtask).get(laneId);
    }

    /** Retains only lanes incident to this task, even for a shared graph. */
    static Map<String, StreamPhysicalExecutionPlan.Lane> taskIndex(
            StreamExecutionGraph graph, StreamExecutionGraph.Node node,
            int subtask) {
        if (graph == null || node == null || subtask < 0
                || subtask >= node.parallelism()) {
            throw new IllegalArgumentException(
                    "physical Stream task identity is incomplete");
        }
        StreamTaskIdentity identity = new StreamTaskIdentity(
                node.portableNode().operatorUid(), subtask);
        synchronized (INDEX) {
            Map<StreamTaskIdentity,
                    Map<String, StreamPhysicalExecutionPlan.Lane>> byTask =
                    INDEX.get(graph);
            if (byTask == null) {
                byTask = new LinkedHashMap<>();
                INDEX.put(graph, byTask);
            }
            Map<String, StreamPhysicalExecutionPlan.Lane> indexed =
                    byTask.get(identity);
            if (indexed == null) {
                List<StreamWorkerPlacement.Assignment> tasks =
                        new ArrayList<>();
                for (StreamExecutionGraph.Node graphNode : graph.nodes()) {
                    for (int index = 0;
                            index < graphNode.parallelism(); index++) {
                        tasks.add(new StreamWorkerPlacement.Assignment(
                                tasks.size(),
                                graphNode.portableNode().operatorUid(), index,
                                "lane-binding", "canonical"));
                    }
                }
                StreamPhysicalExecutionPlan physical =
                        StreamPhysicalExecutionPlan.from(graph,
                                new StreamWorkerPlacement(tasks));
                Map<String, StreamPhysicalExecutionPlan.Lane> fresh =
                        new LinkedHashMap<>();
                for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
                    if (!lane.source().equals(identity)
                            && !lane.target().equals(identity)) continue;
                    if (fresh.put(lane.physicalChannelId(), lane) != null) {
                        throw new IllegalStateException(
                                "physical Stream lane identity is duplicated");
                    }
                }
                indexed = Collections.unmodifiableMap(fresh);
                byTask.put(identity, indexed);
            }
            return indexed;
        }
    }

    static boolean release(StreamExecutionGraph graph,
            StreamTaskIdentity identity) {
        if (graph == null || identity == null) return false;
        synchronized (INDEX) {
            Map<StreamTaskIdentity,
                    Map<String, StreamPhysicalExecutionPlan.Lane>> byTask =
                    INDEX.get(graph);
            if (byTask == null) return false;
            boolean removed = byTask.remove(identity) != null;
            if (byTask.isEmpty()) INDEX.remove(graph);
            return removed;
        }
    }

    static StreamEdge checkpointInput(StreamExecutionGraph.Node node,
            int targetSubtask, String laneId) {
        StreamEdge match = null;
        for (StreamEdge edge : node.inputEdges()) {
            String prefix = edge.logicalChannelId() + '#'
                    + edge.sourceOperatorUid() + '#';
            String suffix = "->" + edge.targetOperatorUid() + '#'
                    + targetSubtask;
            if (matches(laneId, edge, prefix, suffix)) {
                if (match != null) throw ambiguous();
                match = edge;
            }
        }
        if (match == null) {
            throw new IllegalArgumentException(
                    "physical Stream input lane is not attached to an edge");
        }
        return match;
    }

    static StreamEdge checkpointOutput(StreamExecutionGraph.Node node,
            int sourceSubtask, String laneId) {
        StreamEdge match = null;
        for (StreamEdge edge : node.outputEdges()) {
            String prefix = edge.logicalChannelId() + '#'
                    + edge.sourceOperatorUid() + '#' + sourceSubtask
                    + "->" + edge.targetOperatorUid() + '#';
            if (matches(laneId, edge, prefix, "")) {
                if (match != null) throw ambiguous();
                match = edge;
            }
        }
        if (match == null) {
            throw new IllegalArgumentException(
                    "physical Stream output lane is not attached to an edge");
        }
        return match;
    }

    static int sourceSubtask(StreamEdge edge, String laneId) {
        String prefix = edge.logicalChannelId() + '#'
                + edge.sourceOperatorUid() + '#';
        String qualified = "@" + edge.logicalChannelId().length() + ':'
                + prefix;
        String matched = laneId.startsWith(prefix) ? prefix : null;
        while (matched == null && qualified.length() < laneId.length()) {
            if (laneId.startsWith(qualified)) matched = qualified;
            else qualified = '@' + qualified;
        }
        if (matched == null) {
            throw new IllegalArgumentException(
                    "physical Stream lane has no source identity");
        }
        int arrow = laneId.indexOf("->", matched.length());
        if (arrow < 0) {
            throw new IllegalArgumentException(
                    "physical Stream lane has no target identity");
        }
        try {
            int parsed = Integer.parseInt(laneId.substring(
                    matched.length(), arrow));
            if (parsed >= 0 && parsed < 4_096) return parsed;
        } catch (NumberFormatException invalid) {
            // Fail with the same bounded identity error below.
        }
        throw new IllegalArgumentException(
                "physical Stream lane source subtask is invalid");
    }

    private static boolean matches(String laneId, StreamEdge edge,
            String prefix,
            String suffix) {
        if (matchesPrefix(laneId, prefix, suffix)) return true;
        String qualified = "@" + edge.logicalChannelId().length() + ':'
                + prefix;
        while (qualified.length() < laneId.length()) {
            if (matchesPrefix(laneId, qualified, suffix)) return true;
            qualified = '@' + qualified;
        }
        return false;
    }

    private static boolean matchesPrefix(String laneId, String prefix,
            String suffix) {
        if (laneId == null || !laneId.startsWith(prefix)
                || !laneId.endsWith(suffix)
                || laneId.length() <= prefix.length() + suffix.length()) {
            return false;
        }
        int last = laneId.length() - suffix.length();
        String index = laneId.substring(prefix.length(), last);
        if (index.length() > 1 && index.charAt(0) == '0') return false;
        try {
            int parsed = Integer.parseInt(index);
            return parsed >= 0 && parsed < 4_096;
        } catch (NumberFormatException invalid) {
            return false;
        }
    }

    private static IllegalArgumentException ambiguous() {
        return new IllegalArgumentException(
                "physical Stream lane matches multiple logical edges");
    }
}
