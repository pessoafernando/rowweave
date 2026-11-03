package io.rowweave.cluster;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.IncrementalRuntimeGate;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;

/**
 * Client that submits a job. The only entry point is {@link #submitToSink}: a plan graph rooted
 * at a sink is submitted, the last stage of a worker writes the sink on its own machine, and
 * the client receives the written row count as a control signal rather than the rows
 * themselves, so nothing is collected into a list.
 */
public interface JobClient {

    /**
     * Submits a plan graph rooted at a sink and returns the number of rows written to the sink,
     * which is a control signal rather than data.
     */
    long submitToSink(PlanGraph sinkGraph);

    /**
     * As {@link #submitToSink}, but returns the advisory metrics in an {@link ExecutionReport}:
     * rows written, spills and skew. The default delegates to submitToSink, so a client without
     * metrics, such as the cross-process cluster client, reports only rows written and
     * leaves the rest zero or empty; the in-process client overrides it. The sink is still
     * written by a worker at the end of execution, so only value objects travel back, never
     * rows.
     */
    default ExecutionReport submitToSinkReported(PlanGraph sinkGraph) {
        long rows = submitToSink(sinkGraph);
        return new ExecutionReport(0, 0, 0, rows, null);
    }

    /**
     * Submits the whole graph in one call, so a shared subgraph feeding several outputs is
     * computed once and the graph is not split per sink. The SINK nodes inside the graph are
     * the output points, and there may be several of them.<p>The default dispatches strictly:
     * with exactly one SINK this delegates to {@link #submitToSinkReported}, which keeps the
     * current behaviour and its parallelism; with more than one SINK an implementation must
     * override it, computing a shared subgraph once and broadcasting to each sink, and a client
     * that does not override refuses a multi-sink graph rather than dropping a sink in silence.
     * The ExecutionReport covers the whole job, with rowsOut the total written across every
     * output.
     */
    default ExecutionReport submit(PlanGraph fullGraph) {
        IncrementalRuntimeGate.rejectGenericExecution(fullGraph);
        if (fullGraph.sinkCount() <= 1) {
            return submitToSinkReported(fullGraph);  // the single sink fast path is the current behaviour
        }
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support whole-graph submission with multiple sinks (sinkCount="
                + "); override in LocalJobClient for tee mode; cluster multi-sink jobs use whole-graph submission");
    }

    /** Versioned submission boundary; an older client keeps using the plan graph implementation once validated. */
    default ExecutionReport submit(PlanArtifact artifact) {
        return submit(LegacyPlanArtifactAdapter.unwrapValidated(artifact));
    }
}
