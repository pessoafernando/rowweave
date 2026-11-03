package io.rowweave.pipeline;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

/** Deterministic metadata-only compiler from product outputs to execution units. */
public final class PipelineCompiler {
    private PipelineCompiler() { }

    public static PipelineExecutionPlan compile(
            PipelineDefinition definition, Collection<String> requestedOutputIds) {
        if (definition == null) {
            throw PipelineText.failure("INVALID_DEFINITION", "pipeline",
                    "definition is required");
        }
        if (requestedOutputIds == null || requestedOutputIds.isEmpty()) {
            throw PipelineText.failure("MISSING_REQUESTED_OUTPUT",
                    "compile.requestedOutputs",
                    "at least one requested output is required");
        }
        if (requestedOutputIds.size() > PipelineLimits.MAX_OUTPUTS) {
            throw PipelineText.failure("LIMIT_EXCEEDED",
                    "compile.requestedOutputs",
                    "requested output count exceeds limit");
        }

        for (int i = 0; i < definition.steps().size(); i++) {
            PipelineArtifactAdapter.validateMetadata(
                    definition.steps().get(i).artifact(),
                    "pipeline.steps[" + i + "].artifact");
        }

        Map<String, PipelineOutput> outputById = new HashMap<>();
        for (PipelineOutput output : definition.outputs()) {
            outputById.put(output.outputId(), output);
        }
        TreeSet<String> canonicalRequests = new TreeSet<>();
        for (String requested : requestedOutputIds) {
            String outputId = PipelineText.required(requested,
                    "compile.requestedOutputs", PipelineLimits.MAX_ID_BYTES);
            if (!canonicalRequests.add(outputId)) {
                throw PipelineText.failure("DUPLICATE_REQUESTED_OUTPUT",
                        "compile.requestedOutputs",
                        "requested outputs must be unique");
            }
            if (!outputById.containsKey(outputId)) {
                throw PipelineText.failure("UNKNOWN_REQUESTED_OUTPUT",
                        "compile.requestedOutputs", "unknown output: " + outputId);
            }
        }

        Map<String, PipelineStep> stepById = new HashMap<>();
        Map<String, List<PipelineEdge>> incoming = new HashMap<>();
        for (PipelineStep step : definition.steps()) {
            stepById.put(step.stepId(), step);
            incoming.put(step.stepId(), new ArrayList<PipelineEdge>());
        }
        for (PipelineEdge edge : definition.edges()) {
            incoming.get(edge.consumerStepId()).add(edge);
        }

        Set<String> reachable = new HashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        List<PipelineOutput> selectedOutputs = new ArrayList<>();
        for (String outputId : canonicalRequests) {
            PipelineOutput output = outputById.get(outputId);
            selectedOutputs.add(output);
            if (reachable.add(output.stepId())) pending.addLast(output.stepId());
        }
        while (!pending.isEmpty()) {
            String consumer = pending.removeFirst();
            for (PipelineEdge edge : incoming.get(consumer)) {
                if (reachable.add(edge.producerStepId())) {
                    pending.addLast(edge.producerStepId());
                }
            }
        }

        List<PipelineEdge> retainedEdges = new ArrayList<>();
        for (PipelineEdge edge : definition.edges()) {
            if (reachable.contains(edge.producerStepId())
                    && reachable.contains(edge.consumerStepId())) {
                retainedEdges.add(edge);
            }
        }
        retainedEdges.sort((left, right) -> left.edgeId().compareTo(right.edgeId()));

        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> downstream = new HashMap<>();
        for (String stepId : reachable) {
            indegree.put(stepId, 0);
            downstream.put(stepId, new ArrayList<String>());
        }
        for (PipelineEdge edge : retainedEdges) {
            indegree.put(edge.consumerStepId(),
                    indegree.get(edge.consumerStepId()) + 1);
            downstream.get(edge.producerStepId()).add(edge.consumerStepId());
        }
        for (List<String> values : downstream.values()) Collections.sort(values);

        PriorityQueue<String> ready = new PriorityQueue<>();
        for (String stepId : reachable) {
            if (indegree.get(stepId) == 0) ready.add(stepId);
        }
        List<PipelineExecutionUnit> orderedUnits = new ArrayList<>(reachable.size());
        while (!ready.isEmpty()) {
            String stepId = ready.remove();
            orderedUnits.add(new PipelineExecutionUnit(stepById.get(stepId)));
            for (String next : downstream.get(stepId)) {
                int remaining = indegree.get(next) - 1;
                indegree.put(next, remaining);
                if (remaining == 0) ready.add(next);
            }
        }
        if (orderedUnits.size() != reachable.size()) {
            throw PipelineText.failure("CYCLIC_PIPELINE", "pipeline.edges",
                    "Pipeline step graph must be acyclic");
        }
        return new PipelineExecutionPlan(definition, orderedUnits,
                retainedEdges, selectedOutputs);
    }
}
