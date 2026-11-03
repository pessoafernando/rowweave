package io.rowweave.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable result of metadata-only Pipeline compilation. */
public final class PipelineExecutionPlan {
    private final String pipelineId;
    private final String revisionId;
    private final String definitionDigest;
    private final List<PipelineExecutionUnit> orderedUnits;
    private final List<PipelineEdge> materializedEdges;
    private final List<PipelineOutput> selectedOutputs;

    PipelineExecutionPlan(
            PipelineDefinition definition,
            List<PipelineExecutionUnit> orderedUnits,
            List<PipelineEdge> materializedEdges,
            List<PipelineOutput> selectedOutputs) {
        this.pipelineId = definition.pipelineId();
        this.revisionId = definition.revisionId();
        this.definitionDigest = definition.definitionDigest();
        this.orderedUnits = immutableCopy(orderedUnits);
        this.materializedEdges = immutableCopy(materializedEdges);
        this.selectedOutputs = immutableCopy(selectedOutputs);
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public String pipelineId() { return pipelineId; }
    public String revisionId() { return revisionId; }
    public String definitionDigest() { return definitionDigest; }
    public List<PipelineExecutionUnit> orderedUnits() { return orderedUnits; }
    public List<PipelineEdge> materializedEdges() { return materializedEdges; }
    public List<PipelineOutput> selectedOutputs() { return selectedOutputs; }
}
