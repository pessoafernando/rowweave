package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.runtime.ManagedKeyedValueState;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamChannelMetrics;

import java.util.ArrayList;
import java.util.List;
import java.security.MessageDigest;

/** Pure-Push hash partitioner from one fixed split into canonical key groups. */
final class StreamParallelKeyedOutput implements StreamChannelOutput {
    private final List<StreamChannelOutput> partitions;
    private final RowSerializer rowSerializer;
    private final RowSerializer keySerializer;
    private final int[] keyIndices;
    private final int maximumParallelism;
    private final PortableStreamPlan.ChangelogMode changelogMode;
    private final int maximumFrameBytes;
    private final long[] sequences;

    StreamParallelKeyedOutput(StreamExecutionGraph.Node keyedNode,
            List<? extends StreamChannelOutput> partitions) {
        this(keyedNode, keyedNode == null || keyedNode.portableNode() == null
                ? null : keyedNode.portableNode().outputSchema(),
                keyedNode == null || keyedNode.portableNode() == null
                        ? null : keyedNode.portableNode().stableKeyFields(),
                partitions, PortableStreamPlan.ChangelogMode.APPEND,
                Integer.MAX_VALUE);
    }

    StreamParallelKeyedOutput(StreamExecutionGraph.Node keyedNode,
            PortableRowSchema inputSchema,
            List<? extends StreamChannelOutput> partitions) {
        this(keyedNode, inputSchema,
                keyedNode == null || keyedNode.portableNode() == null
                        ? null : keyedNode.portableNode().stableKeyFields(),
                partitions, PortableStreamPlan.ChangelogMode.APPEND,
                Integer.MAX_VALUE);
    }

    StreamParallelKeyedOutput(StreamExecutionGraph.Node targetNode,
            PortableRowSchema inputSchema, List<String> partitionKeyFields,
            List<? extends StreamChannelOutput> partitions,
            PortableStreamPlan.ChangelogMode changelogMode,
            int maximumFrameBytes) {
        if (targetNode == null || partitions == null
                || partitions.size() != targetNode.parallelism()
                || targetNode.portableNode() == null
                || inputSchema == null
                || targetNode.maximumParallelism() < targetNode.parallelism()
                || partitionKeyFields == null
                || partitionKeyFields.isEmpty()
                || changelogMode == null
                || maximumFrameBytes <= 0) {
            throw new IllegalArgumentException(
                    "parallel keyed Stream route is incomplete");
        }
        this.partitions = new ArrayList<>(partitions);
        this.maximumParallelism = targetNode.maximumParallelism();
        this.changelogMode = changelogMode;
        this.maximumFrameBytes = maximumFrameBytes;
        PortableRowSchema schema = inputSchema;
        this.rowSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(schema));
        this.keyIndices = indices(schema, partitionKeyFields);
        this.keySerializer = new RowSerializer(PortablePlanCompiler.toDataSchema(
                keySchema(schema, keyIndices)));
        this.sequences = new long[partitions.size()];
    }

    @Override public StreamChannelIdentity channelIdentity() {
        return partitions.get(0).channelIdentity();
    }

    @Override public void send(StreamEnvelope envelope) {
        if (envelope == null || !channelIdentity().equals(
                envelope.channelIdentity())) {
            throw new IllegalArgumentException(
                    "parallel keyed envelope belongs to another attempt");
        }
        if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
            byte[] rowFrame = envelope.recordFrame();
            byte[] key;
            if (changelogMode == PortableStreamPlan.ChangelogMode.UPSERT) {
                StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                        rowFrame, maximumFrameBytes, maximumFrameBytes);
                key = record.stableKeyFrame();
                keySerializer.deserialize(key);
                if (record.hasAfterImage()) {
                    DataRow row = rowSerializer.deserialize(
                            record.afterImageFrame());
                    byte[] projected = keySerializer.serializeProjected(
                            row, keyIndices);
                    if (!MessageDigest.isEqual(key, projected)) {
                        throw new IllegalArgumentException(
                                "UPSERT stable key differs from its after-image");
                    }
                }
            } else if (changelogMode
                    == PortableStreamPlan.ChangelogMode.RETRACT) {
                StreamChangeRecord record = StreamChangeRecordCodec.decode(
                        rowFrame, maximumFrameBytes, maximumFrameBytes,
                        maximumFrameBytes);
                if (record.hasStableKey()) {
                    key = record.stableKeyFrame();
                    keySerializer.deserialize(key);
                    byte[] image = record.hasBeforeImage()
                            ? record.beforeImageFrame()
                            : record.hasAfterImage()
                            ? record.afterImageFrame() : null;
                    if (image != null) {
                        validateProjectedKey(key, image, "RETRACT");
                    }
                } else {
                    DataRow row = rowSerializer.deserialize(
                            record.afterImageFrame());
                    key = keySerializer.serializeProjected(row, keyIndices);
                }
            } else {
                DataRow row = rowSerializer.deserialize(rowFrame);
                key = keySerializer.serializeProjected(row, keyIndices);
            }
            int keyGroup = ManagedKeyedValueState.keyGroup(
                    key, maximumParallelism);
            int target = Math.min(partitions.size() - 1,
                    (int) ((long) keyGroup * partitions.size()
                            / maximumParallelism));
            partitions.get(target).send(rebind(
                    envelope, partitions.get(target).channelIdentity(),
                    next(target)));
            return;
        }
        for (int target = 0; target < partitions.size(); target++) {
            partitions.get(target).send(rebind(
                    envelope, partitions.get(target).channelIdentity(),
                    next(target)));
        }
    }

    private void validateProjectedKey(byte[] key, byte[] rowFrame,
            String changelog) {
        DataRow row = rowSerializer.deserialize(rowFrame);
        byte[] projected = keySerializer.serializeProjected(row, keyIndices);
        if (!MessageDigest.isEqual(key, projected)) {
            throw new IllegalArgumentException(changelog
                    + " stable key differs from its row image");
        }
    }

    @Override public void close() {
        for (StreamChannelOutput output : partitions) output.close();
    }

    @Override public void cancel() {
        for (StreamChannelOutput output : partitions) output.cancel();
    }

    @Override public void fail(Throwable cause) {
        for (StreamChannelOutput output : partitions) output.fail(cause);
    }

    @Override public StreamChannelMetrics metrics() {
        return StreamChannelMetrics.aggregateOutputs(partitions);
    }

    private long next(int target) {
        return sequences[target] = Math.addExact(sequences[target], 1L);
    }

    static StreamEnvelope rebind(StreamEnvelope source,
            StreamChannelIdentity channel, long sequence) {
        switch (source.kind()) {
            case RECORD:
                return source.hasEventTimestamp()
                        ? StreamEnvelope.record(channel, sequence,
                        source.timestamp(), source.recordFrame())
                        : StreamEnvelope.record(channel, sequence,
                        source.recordFrame());
            case WATERMARK:
                return StreamEnvelope.watermark(
                        channel, sequence, source.timestamp());
            case CHECKPOINT_BARRIER:
                return StreamEnvelope.checkpointBarrier(
                        channel, sequence, source.checkpointIdentity());
            case WATERMARK_STATUS:
                return StreamEnvelope.watermarkStatus(channel, sequence,
                        source.watermarkStatus(),
                        source.watermarkStatusGeneration());
            case END_OF_INPUT:
                return StreamEnvelope.endOfInput(channel, sequence);
            case ASYNC_COMPLETION:
                return StreamEnvelope.asyncCompletion(
                        channel, sequence, source.operationIdentity());
            case CANCEL:
                return StreamEnvelope.cancel(channel, sequence);
            case ERROR:
                return StreamEnvelope.error(
                        channel, sequence, source.errorCode());
            default:
                throw new IllegalArgumentException(
                        "unknown Stream envelope kind");
        }
    }

    private static int[] indices(
            PortableRowSchema schema, List<String> fieldIds) {
        int[] result = new int[fieldIds.size()];
        for (int offset = 0; offset < result.length; offset++) {
            int found = -1;
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(
                        fieldIds.get(offset))) {
                    found = index;
                    break;
                }
            }
            if (found < 0) {
                throw new IllegalArgumentException(
                        "parallel keyed field binding is missing");
            }
            result[offset] = found;
        }
        return result;
    }

    private static PortableRowSchema keySchema(
            PortableRowSchema schema, int[] indices) {
        List<PortableField> fields = new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.field(index));
        return new PortableRowSchema(fields);
    }
}
