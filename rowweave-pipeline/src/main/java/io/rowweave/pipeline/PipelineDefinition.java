package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Immutable, bounded composition of complete Batch/Stream PlanArtifacts. */
public final class PipelineDefinition {
    public static final int CURRENT_FORMAT_VERSION = 1;
    public static final int CURRENT_READER_VERSION = 1;
    public static final String FEATURE_COMPOSITION_V1 =
            "rowweave.pipeline.composition.v1";

    private final int formatVersion;
    private final int minimumReaderVersion;
    private final String pipelineId;
    private final String revisionId;
    private final List<String> requiredFeatures;
    private final List<String> optionalFeatures;
    private final List<PipelineStep> steps;
    private final List<PipelineEdge> edges;
    private final List<PipelineOutput> outputs;

    public static PipelineDefinitionBuilder builder(
            String pipelineId, String revisionId) {
        return new PipelineDefinitionBuilder(pipelineId, revisionId);
    }

    public PipelineDefinition(
            int formatVersion, int minimumReaderVersion,
            String pipelineId, String revisionId,
            List<String> requiredFeatures, List<String> optionalFeatures,
            List<PipelineStep> steps, List<PipelineEdge> edges,
            List<PipelineOutput> outputs) {
        if (formatVersion != CURRENT_FORMAT_VERSION
                || minimumReaderVersion < 1
                || minimumReaderVersion > CURRENT_READER_VERSION) {
            throw PipelineText.failure("UNSUPPORTED_VERSION", "pipeline.version",
                    "Pipeline definition version is not readable");
        }
        this.pipelineId = PipelineText.required(
                pipelineId, "pipeline.pipelineId", PipelineLimits.MAX_ID_BYTES);
        this.revisionId = PipelineText.required(
                revisionId, "pipeline.revisionId", PipelineLimits.MAX_ID_BYTES);
        this.requiredFeatures = features(
                requiredFeatures, "pipeline.requiredFeatures");
        this.optionalFeatures = features(
                optionalFeatures, "pipeline.optionalFeatures");
        Set<String> overlap = new HashSet<>(this.requiredFeatures);
        overlap.retainAll(this.optionalFeatures);
        if (!overlap.isEmpty()) {
            throw PipelineText.failure("DUPLICATE_FEATURE", "pipeline.features",
                    "required and optional features overlap");
        }
        if (!this.requiredFeatures.contains(FEATURE_COMPOSITION_V1)) {
            throw PipelineText.failure("MISSING_FEATURE", "pipeline.requiredFeatures",
                    "Pipeline composition v1 feature is required");
        }
        for (String feature : this.requiredFeatures) {
            if (!FEATURE_COMPOSITION_V1.equals(feature)) {
                throw PipelineText.failure("UNSUPPORTED_REQUIRED_FEATURE",
                        "pipeline.requiredFeatures",
                        "Unsupported required feature: " + feature);
            }
        }
        this.steps = copySteps(steps);
        Map<String, PipelineStep> stepById = indexSteps(this.steps);
        this.edges = copyEdges(edges);
        validateEdges(this.edges, stepById);
        requireAcyclic(this.steps, this.edges);
        this.outputs = copyOutputs(outputs);
        validateOutputs(this.outputs, stepById);
        this.formatVersion = formatVersion;
        this.minimumReaderVersion = minimumReaderVersion;
    }

    private static List<String> features(List<String> values, String path) {
        if (values == null || values.size() > PipelineLimits.MAX_FEATURES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "feature count exceeds limit");
        }
        TreeSet<String> canonical = new TreeSet<>();
        for (int i = 0; i < values.size(); i++) {
            String feature = PipelineText.required(values.get(i),
                    path + "[" + i + "]", PipelineLimits.MAX_FEATURE_BYTES);
            if (!canonical.add(feature)) {
                throw PipelineText.failure("DUPLICATE_FEATURE", path,
                        "feature IDs must be unique");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(canonical));
    }

    private static List<PipelineStep> copySteps(List<PipelineStep> values) {
        if (values == null || values.isEmpty()
                || values.size() > PipelineLimits.MAX_STEPS) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "pipeline.steps",
                    "step count is outside its limit");
        }
        List<PipelineStep> copied = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null) {
                throw PipelineText.failure("INVALID_STEP", "pipeline.steps[" + i + "]",
                        "step is required");
            }
            copied.add(values.get(i));
        }
        return Collections.unmodifiableList(copied);
    }

    private static Map<String, PipelineStep> indexSteps(List<PipelineStep> values) {
        Map<String, PipelineStep> indexed = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            PipelineStep previous = indexed.put(values.get(i).stepId(), values.get(i));
            if (previous != null) {
                throw PipelineText.failure("DUPLICATE_STEP_ID",
                        "pipeline.steps[" + i + "].stepId",
                        "step IDs must be unique");
            }
        }
        return indexed;
    }

    private static List<PipelineEdge> copyEdges(List<PipelineEdge> values) {
        if (values == null || values.size() > PipelineLimits.MAX_EDGES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "pipeline.edges",
                    "edge count exceeds limit");
        }
        List<PipelineEdge> copied = new ArrayList<>(values.size());
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < values.size(); i++) {
            PipelineEdge edge = values.get(i);
            if (edge == null) {
                throw PipelineText.failure("INVALID_EDGE", "pipeline.edges[" + i + "]",
                        "edge is required");
            }
            if (!ids.add(edge.edgeId())) {
                throw PipelineText.failure("DUPLICATE_EDGE_ID",
                        "pipeline.edges[" + i + "].edgeId",
                        "edge IDs must be unique");
            }
            copied.add(edge);
        }
        return Collections.unmodifiableList(copied);
    }

    private static void validateEdges(
            List<PipelineEdge> edges, Map<String, PipelineStep> steps) {
        Set<String> consumers = new HashSet<>();
        Map<String, String> materializations = new HashMap<>();
        for (int i = 0; i < edges.size(); i++) {
            PipelineEdge edge = edges.get(i);
            PipelineStep producer = steps.get(edge.producerStepId());
            PipelineStep consumer = steps.get(edge.consumerStepId());
            if (producer == null || consumer == null) {
                throw PipelineText.failure("UNKNOWN_STEP", "pipeline.edges[" + i + "]",
                        "edge references an unknown step");
            }
            if (producer.stepId().equals(consumer.stepId())) {
                throw PipelineText.failure("SELF_EDGE", "pipeline.edges[" + i + "]",
                        "Pipeline steps cannot depend on themselves");
            }
            OperatorDescriptor producerOperator = operator(
                    producer, edge.producerSinkOperatorId(),
                    "pipeline.edges[" + i + "].producerSinkOperatorId");
            OperatorDescriptor consumerOperator = operator(
                    consumer, edge.consumerSourceOperatorId(),
                    "pipeline.edges[" + i + "].consumerSourceOperatorId");
            if (producerOperator.kind() != OperatorDescriptor.Kind.SINK
                    || consumerOperator.kind() != OperatorDescriptor.Kind.SOURCE) {
                throw PipelineText.failure("INVALID_EDGE_ENDPOINT_KIND",
                        "pipeline.edges[" + i + "]",
                        "materialized edges must connect SINK to SOURCE");
            }
            if (!edge.schemaFingerprint().equals(producerOperator.schemaFingerprint())
                    || !edge.schemaFingerprint().equals(consumerOperator.schemaFingerprint())) {
                throw PipelineText.failure("SCHEMA_MISMATCH",
                        "pipeline.edges[" + i + "].schemaFingerprint",
                        "edge schema must equal both endpoint schemas");
            }
            String consumerKey = consumer.stepId() + "\u0000" + consumerOperator.operatorId();
            if (!consumers.add(consumerKey)) {
                throw PipelineText.failure("DUPLICATE_SOURCE_BINDING",
                        "pipeline.edges[" + i + "].consumerSourceOperatorId",
                        "one source operator accepts one materialized input");
            }
            String materializationValue = producer.stepId() + "\u0000"
                    + producerOperator.operatorId() + "\u0000" + edge.schemaFingerprint();
            String previous = materializations.putIfAbsent(
                    edge.materializationId(), materializationValue);
            if (previous != null && !previous.equals(materializationValue)) {
                throw PipelineText.failure("MATERIALIZATION_CONFLICT",
                        "pipeline.edges[" + i + "].materializationId",
                        "one materialization ID must name one producer/schema");
            }
        }
    }

    private static OperatorDescriptor operator(
            PipelineStep step, String operatorId, String path) {
        for (OperatorDescriptor operator : step.artifact().operators()) {
            if (operator.operatorId().equals(operatorId)) return operator;
        }
        throw PipelineText.failure("UNKNOWN_OPERATOR", path,
                "operator is absent from the referenced Artifact");
    }

    private static void requireAcyclic(
            List<PipelineStep> steps, List<PipelineEdge> edges) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> downstream = new HashMap<>();
        for (PipelineStep step : steps) {
            indegree.put(step.stepId(), 0);
            downstream.put(step.stepId(), new ArrayList<String>());
        }
        for (PipelineEdge edge : edges) {
            indegree.put(edge.consumerStepId(),
                    indegree.get(edge.consumerStepId()) + 1);
            downstream.get(edge.producerStepId()).add(edge.consumerStepId());
        }
        ArrayDeque<String> ready = new ArrayDeque<>();
        for (PipelineStep step : steps) {
            if (indegree.get(step.stepId()) == 0) ready.addLast(step.stepId());
        }
        int visited = 0;
        while (!ready.isEmpty()) {
            String current = ready.removeFirst();
            visited++;
            for (String next : downstream.get(current)) {
                int remaining = indegree.get(next) - 1;
                indegree.put(next, remaining);
                if (remaining == 0) ready.addLast(next);
            }
        }
        if (visited != steps.size()) {
            throw PipelineText.failure("CYCLIC_PIPELINE", "pipeline.edges",
                    "Pipeline step graph must be acyclic");
        }
    }

    private static List<PipelineOutput> copyOutputs(List<PipelineOutput> values) {
        if (values == null || values.isEmpty()
                || values.size() > PipelineLimits.MAX_OUTPUTS) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "pipeline.outputs",
                    "output count is outside its limit");
        }
        List<PipelineOutput> copied = new ArrayList<>(values.size());
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < values.size(); i++) {
            PipelineOutput output = values.get(i);
            if (output == null) {
                throw PipelineText.failure("INVALID_OUTPUT",
                        "pipeline.outputs[" + i + "]", "output is required");
            }
            if (!ids.add(output.outputId())) {
                throw PipelineText.failure("DUPLICATE_OUTPUT_ID",
                        "pipeline.outputs[" + i + "].outputId",
                        "output IDs must be unique");
            }
            copied.add(output);
        }
        return Collections.unmodifiableList(copied);
    }

    private static void validateOutputs(
            List<PipelineOutput> outputs, Map<String, PipelineStep> steps) {
        for (int i = 0; i < outputs.size(); i++) {
            PipelineOutput output = outputs.get(i);
            PipelineStep step = steps.get(output.stepId());
            if (step == null) {
                throw PipelineText.failure("UNKNOWN_STEP",
                        "pipeline.outputs[" + i + "].stepId",
                        "output references an unknown step");
            }
            OperatorDescriptor operator = operator(step, output.sinkOperatorId(),
                    "pipeline.outputs[" + i + "].sinkOperatorId");
            if (operator.kind() != OperatorDescriptor.Kind.SINK) {
                throw PipelineText.failure("INVALID_OUTPUT_ENDPOINT_KIND",
                        "pipeline.outputs[" + i + "].sinkOperatorId",
                        "Pipeline outputs must reference SINK operators");
            }
        }
    }

    public int formatVersion() { return formatVersion; }
    public int minimumReaderVersion() { return minimumReaderVersion; }
    public String pipelineId() { return pipelineId; }
    public String revisionId() { return revisionId; }
    public List<String> requiredFeatures() { return requiredFeatures; }
    public List<String> optionalFeatures() { return optionalFeatures; }
    public List<PipelineStep> steps() { return steps; }
    public List<PipelineEdge> edges() { return edges; }
    public List<PipelineOutput> outputs() { return outputs; }
    public String definitionDigest() {
        return PipelineDefinitionCodec.definitionDigest(this);
    }
}
