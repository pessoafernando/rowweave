package io.rowweave.stream.cluster;

import io.rowweave.monitor.StageView;
import io.rowweave.stream.contract.StreamOperatorCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bounded, data-free node views derived from admitted placement and accepted samples. */
final class StreamRuntimeMonitorProjection {
    private final StreamExecutionGraph graph;
    private final Map<String, Integer> indices = new LinkedHashMap<>();
    private final Map<String, List<StreamWorkerPlacement.Assignment>> tasks = new HashMap<>();
    private final Map<String, Integer> channels = new HashMap<>();
    private final Map<StreamTaskIdentity, Long> endTimes = new HashMap<>();
    private final long startTime = System.currentTimeMillis();

    StreamRuntimeMonitorProjection(StreamExecutionGraph graph, StreamWorkerPlacement placement) {
        this.graph = graph;
        for (String uid : graph.nodeUids()) {
            indices.put(uid, indices.size());
            tasks.put(uid, new ArrayList<StreamWorkerPlacement.Assignment>());
        }
        for (StreamWorkerPlacement.Assignment task : placement.assignments()) {
            tasks.get(task.operatorUid()).add(task);
        }
        for (StreamPhysicalExecutionPlan.Lane lane
                : StreamPhysicalExecutionPlan.from(graph, placement).lanes()) {
            String id = lane.logicalEdge().logicalChannelId();
            channels.put(id, channels.getOrDefault(id, 0) | (lane.crossesWorker() ? 2 : 1));
        }
        for (List<StreamWorkerPlacement.Assignment> placed : tasks.values()) {
            placed.sort(java.util.Comparator.comparingInt(
                    StreamWorkerPlacement.Assignment::subtaskIndex));
        }
    }

    List<StageView> stages(Map<StreamTaskIdentity, StreamTaskProgress> samples,
                           Set<StreamTaskIdentity> completed) {
        List<StageView> result = new ArrayList<>(indices.size());
        for (String uid : graph.nodeUids()) {
            StreamExecutionGraph.Node node = graph.node(uid);
            List<StreamWorkerPlacement.Assignment> placed = tasks.get(uid);
            StreamOperatorCode opcode = node.portableNode().opcode();
            StageView.Builder stage = StageView.builder(indices.get(uid))
                    .role(opcode == StreamOperatorCode.SOURCE ? "SOURCE"
                            : opcode == StreamOperatorCode.SINK ? "SINK" : "STAGE")
                    .opChain(opcode.name() + " (" + uid + ")")
                    .parallelism(placed.size()).startTime(startTime);
            for (io.rowweave.stream.contract.StreamEdge edge : node.inputEdges()) {
                int channel = channels.getOrDefault(edge.logicalChannelId(), 2);
                stage.addInput(indices.get(edge.sourceOperatorUid()),
                        channel == 1 ? "local" : channel == 2 ? "remote" : "mixed");
            }
            long read = 0L;
            long written = 0L;
            long end = 0L;
            boolean allFinished = !placed.isEmpty();
            boolean anySample = false;
            for (StreamWorkerPlacement.Assignment task : placed) {
                StreamTaskProgress sample = samples.get(task.identity());
                boolean finished = completed.contains(task.identity());
                allFinished &= finished;
                anySample |= sample != null;
                long inputRows = sample == null ? 0L : sample.recordsRead();
                long outputRows = sample == null ? 0L : sample.recordsWritten();
                read = Math.addExact(read, inputRows);
                written = Math.addExact(written, outputRows);
                long taskEnd = finished ? endTimes.computeIfAbsent(task.identity(),
                        ignored -> System.currentTimeMillis()) : 0L;
                end = Math.max(end, taskEnd);
                stage.addInstance(new StageView.InstanceView(task.subtaskIndex(), task.workerId(),
                        finished ? "FINISHED" : sample == null ? "READY" : "RUNNING",
                        outputRows, sample == null ? 0L : startTime, taskEnd, inputRows));
            }
            stage.state(allFinished ? "FINISHED" : anySample ? "RUNNING" : "READY")
                    .inputRows(read).outputRows(written)
                    .workerId(placed.isEmpty() ? null : placed.get(0).workerId())
                    .endTime(allFinished ? end : 0L);
            result.add(stage.build());
        }
        return Collections.unmodifiableList(result);
    }

    static String dag(List<StageView> stages) {
        StringBuilder nodes = new StringBuilder();
        StringBuilder edges = new StringBuilder();
        for (StageView stage : stages) {
            if (nodes.length() > 0) nodes.append(',');
            nodes.append("{\"id\":").append(stage.index()).append('}');
            for (int input : stage.inputs()) {
                if (edges.length() > 0) edges.append(',');
                edges.append("{\"from\":").append(input).append(",\"to\":")
                        .append(stage.index()).append('}');
            }
        }
        return "{\"nodes\":[" + nodes + "],\"edges\":[" + edges + "]}";
    }

    static StageView terminal(StageView stage, String state, long endTime) {
        if ("FINISHED".equals(stage.state())) return stage;
        StageView.Builder result = StageView.builder(stage.index())
                .role(stage.role()).state(state).opChain(stage.opChain())
                .workerId(stage.workerId()).parallelism(stage.parallelism())
                .inputRows(stage.inputRows()).outputRows(stage.outputRows())
                .startTime(stage.startTime()).endTime(endTime).error(stage.error());
        for (int i = 0; i < stage.inputs().size(); i++) {
            result.addInput(stage.inputs().get(i), stage.inputChannels().get(i));
        }
        for (StageView.InstanceView instance : stage.instances()) {
            result.addInstance("FINISHED".equals(instance.state) ? instance
                    : new StageView.InstanceView(instance.instanceIndex, instance.workerId,
                            state, instance.outputRows, instance.startTime,
                            endTime, instance.inputRows));
        }
        return result.build();
    }
}
