package io.rowweave.plan;

import io.rowweave.Sink;

import java.util.HashMap;
import java.util.Map;

/** Preflight for conflicting exclusive Sink destinations across a complete PlanGraph. */
public final class OutputDestinationValidator {

    private OutputDestinationValidator() {
    }

    /** A read-only scan over the SINK nodes; it must run before any Source, Worker, slot or Sink staging side effect. */
    public static void validate(PlanGraph graph) {
        if (graph == null) {
            throw new IllegalArgumentException("plan graph must not be null");
        }
        Map<String, DestinationOwner> owners = new HashMap<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK || node.output() == null) {
                continue;
            }
            Sink output = node.output();
            String destinationId = output.exclusiveDestinationId();
            if (destinationId == null) {
                continue;
            }
            if (destinationId.isEmpty()) {
                throw new IllegalArgumentException("SINK node " + i + " Sink "
                        + output.getClass().getName() + " returned an empty exclusive destination id");
            }
            DestinationOwner first = owners.putIfAbsent(
                    destinationId, new DestinationOwner(i, output.getClass().getName()));
            if (first != null) {
                throw new IllegalArgumentException("duplicate output destination " + destinationId
                        + " for SINK nodes " + first.nodeIndex + " (" + first.outputType + ") and "
                        + i + " (" + output.getClass().getName() + ")");
            }
        }
    }

    private static final class DestinationOwner {
        private final int nodeIndex;
        private final String outputType;

        private DestinationOwner(int nodeIndex, String outputType) {
            this.nodeIndex = nodeIndex;
            this.outputType = outputType;
        }
    }
}
