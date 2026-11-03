package io.rowweave.cluster;

import io.rowweave.Sink;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

/** Capability guard, serialized to the worker, over the sinks a whole graph needs in order to run. */
final class ClusterOutputValidator {

    private ClusterOutputValidator() {
    }

    static void validate(PlanGraph graph) {
        if (graph == null) {
            throw new IllegalArgumentException("cluster plan graph must not be null");
        }
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) {
                continue;
            }
            Sink output = node.output();
            if (output == null) {
                throw new IllegalArgumentException("cluster SINK node " + i
                        + " has no Sink; legacy collecting/client-return sinks are local-only");
            }
            // ProviderBatchSink is an inert, durable descriptor/config proxy.
            // The Worker never calls Sink.collect on it: StageTask resolves the
            // admitted provider and opens a ProviderSinkWriteSession instead.
            if (!output.isWorkerWritable()
                    && output.getClass()
                    != io.rowweave.ProviderBatchSink.class) {
                throw new IllegalArgumentException("cluster SINK node " + i + " Sink "
                        + output.getClass().getName()
                        + " is not worker-writable; use a worker-writable Sink or LocalJobClient");
            }
        }
    }
}
