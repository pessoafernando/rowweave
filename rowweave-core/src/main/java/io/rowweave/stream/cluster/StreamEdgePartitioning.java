package io.rowweave.stream.cluster;

import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamOutputTagDescriptor;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamWindowBinding;
import io.rowweave.stream.contract.PortableStreamPlan;

import java.util.List;

/** Resolves the schema and partition key carried by one logical edge. */
final class StreamEdgePartitioning {
    private StreamEdgePartitioning() { }

    static List<String> keyFields(StreamExecutionGraph graph,
            StreamEdge edge) {
        PortableStreamNode source = graph.node(edge.sourceOperatorUid())
                .portableNode();
        PortableStreamNode target = graph.node(edge.targetOperatorUid())
                .portableNode();
        List<String> sourceKeys = outputStableKeyFields(
                source, edge.sourceOutputTagId());
        if (target.opcode() == StreamOperatorCode.KEY_BY) {
            return target.stableKeyFields();
        }
        return sourceKeys.isEmpty()
                ? target.stableKeyFields() : sourceKeys;
    }

    static PortableRowSchema outputSchema(StreamExecutionGraph graph,
            StreamEdge edge) {
        return outputSchema(graph.node(edge.sourceOperatorUid())
                .portableNode(), edge.sourceOutputTagId());
    }

    static List<String> outputStableKeyFields(StreamExecutionGraph graph,
            StreamEdge edge) {
        return outputStableKeyFields(graph.node(edge.sourceOperatorUid())
                .portableNode(), edge.sourceOutputTagId());
    }

    static PortableStreamPlan.ChangelogMode outputChangelogMode(
            StreamExecutionGraph graph, StreamEdge edge) {
        PortableStreamNode source = graph.node(edge.sourceOperatorUid())
                .portableNode();
        if (edge.sourceOutputTagId() == null) {
            return graph.node(source.operatorUid()).changelogMode();
        }
        StreamOutputTagDescriptor tag = outputTag(
                source, edge.sourceOutputTagId());
        if (tag == null) {
            throw new IllegalArgumentException(
                    "Stream tagged output changelog is unavailable");
        }
        switch (tag.changelogMode()) {
            case APPEND:
                return PortableStreamPlan.ChangelogMode.APPEND;
            case UPSERT:
                return PortableStreamPlan.ChangelogMode.UPSERT;
            case RETRACT:
                return PortableStreamPlan.ChangelogMode.RETRACT;
            default:
                throw new IllegalStateException(
                        "unknown Stream tagged output changelog");
        }
    }

    static boolean producesUpsert(StreamExecutionGraph graph,
            StreamEdge edge) {
        return outputChangelogMode(graph, edge)
                == PortableStreamPlan.ChangelogMode.UPSERT;
    }

    private static PortableRowSchema outputSchema(
            PortableStreamNode producer, String outputTagId) {
        if (outputTagId == null) return producer.outputSchema();
        StreamOutputTagDescriptor tag = outputTag(producer, outputTagId);
        if (tag != null) return tag.schema();
        throw new IllegalArgumentException(
                "Stream tagged output schema is unavailable");
    }

    private static List<String> outputStableKeyFields(
            PortableStreamNode producer, String outputTagId) {
        if (outputTagId == null) return producer.stableKeyFields();
        StreamOutputTagDescriptor tag = outputTag(producer, outputTagId);
        if (tag != null) return tag.stableKeyFields();
        throw new IllegalArgumentException(
                "Stream tagged output stable key is unavailable");
    }

    private static StreamOutputTagDescriptor outputTag(
            PortableStreamNode producer, String outputTagId) {
        if (producer.binding() instanceof StreamProcessBinding) {
            for (StreamOutputTagDescriptor tag
                    : ((StreamProcessBinding) producer.binding())
                    .outputTags()) {
                if (tag.tagId().equals(outputTagId)) return tag;
            }
        } else if (producer.binding() instanceof StreamWindowBinding) {
            StreamOutputTagDescriptor tag = ((StreamWindowBinding)
                    producer.binding()).lateOutputTag();
            if (tag != null && tag.tagId().equals(outputTagId)) return tag;
        }
        return null;
    }
}
