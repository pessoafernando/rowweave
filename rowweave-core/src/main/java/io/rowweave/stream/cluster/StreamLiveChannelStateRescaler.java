package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamChannelStateCodec;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.ManagedKeyedValueState;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Rebinds bounded unaligned lane cuts to one target task generation. */
final class StreamLiveChannelStateRescaler {
    private StreamLiveChannelStateRescaler() { }

    static List<StreamCheckpointMember> rescale(
            StreamProductExecutionGraph target,
            StreamWorkerPlacement targetPlacement,
            CheckpointIdentity targetCheckpoint, long targetChannelEpoch,
            long targetAssignmentGeneration,
            List<StreamCheckpointMember> sourceMembers,
            FileCheckpointPayloadStore payloadStore) {
        return rescale(null, null, target, targetPlacement,
                targetCheckpoint, targetChannelEpoch,
                targetAssignmentGeneration, sourceMembers, payloadStore);
    }

    static List<StreamCheckpointMember> rescale(
            StreamProductExecutionGraph source,
            StreamWorkerPlacement sourcePlacement,
            StreamProductExecutionGraph target,
            StreamWorkerPlacement targetPlacement,
            CheckpointIdentity targetCheckpoint, long targetChannelEpoch,
            long targetAssignmentGeneration,
            List<StreamCheckpointMember> sourceMembers,
            FileCheckpointPayloadStore payloadStore) {
        if (target == null || targetPlacement == null
                || targetCheckpoint == null || targetChannelEpoch < 1L
                || targetAssignmentGeneration < 1L
                || sourceMembers == null || payloadStore == null) {
            throw new IllegalArgumentException(
                    "live channel-state migration is incomplete");
        }
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(
                        target.executionGraph, targetPlacement);
        Map<String, StreamPhysicalExecutionPlan.Lane> sourceLanes = null;
        if (source != null || sourcePlacement != null) {
            if (source == null || sourcePlacement == null
                    || !source.artifact.artifactId().equals(
                    target.artifact.artifactId())) {
                throw new IllegalArgumentException(
                        "live source lane authority is incomplete");
            }
            sourceLanes = new LinkedHashMap<>();
            for (StreamPhysicalExecutionPlan.Lane lane
                    : StreamPhysicalExecutionPlan.from(
                    source.executionGraph, sourcePlacement).lanes()) {
                if (sourceLanes.put(lane.physicalChannelId(), lane) != null) {
                    throw new IllegalArgumentException(
                            "live source lane authority is ambiguous");
                }
            }
        }
        Map<String, List<StreamPhysicalExecutionPlan.Lane>> lanesByEdge =
                new LinkedHashMap<>();
        Map<StreamPhysicalExecutionPlan.Lane,
                List<List<StreamEnvelope>>> replayStreams =
                new LinkedHashMap<>();
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            lanesByEdge.computeIfAbsent(
                    lane.logicalEdge().logicalChannelId(),
                    ignored -> new ArrayList<>()).add(lane);
            replayStreams.put(lane, new ArrayList<>());
        }
        List<OldChannel> oldChannels = new ArrayList<>();
        Map<String, Integer> oldSourceParallelism = new LinkedHashMap<>();
        for (StreamCheckpointMember member : sourceMembers) {
            if (member.kind() != StreamCheckpointMember.Kind.CHANNEL_STATE) {
                continue;
            }
            StreamExecutionGraph.Node node = target.executionGraph.node(
                    member.operatorUid());
            StreamChannelState state = StreamChannelStateCodec.decode(
                    read(member, payloadStore), target.maximumEvents(),
                    node.resourceRequest().checkpointBytes(),
                    maximumChannelFrameBytes(node));
            StreamPhysicalExecutionPlan.Lane sourceLane = sourceLanes == null
                    ? null : sourceLanes.get(member.subjectId());
            if (sourceLanes != null && (sourceLane == null
                    || !sourceLane.target().operatorUid().equals(
                    member.operatorUid())
                    || sourceLane.target().subtaskIndex()
                    != member.subtaskIndex())) {
                throw new IllegalArgumentException(
                        "live channel state is not in the source placement");
            }
            String edgeId = sourceLane == null
                    ? StreamPhysicalLaneBinding.checkpointInput(node,
                    member.subtaskIndex(), member.subjectId())
                    .logicalChannelId()
                    : sourceLane.logicalEdge().logicalChannelId();
            List<StreamPhysicalExecutionPlan.Lane> targetLanes =
                    lanesByEdge.get(edgeId);
            if (targetLanes == null || targetLanes.isEmpty()) {
                throw new IllegalArgumentException(
                        "live channel state references an unknown edge");
            }
            int sourceIndex = sourceLane == null
                    ? StreamPhysicalLaneBinding.sourceSubtask(
                    targetLanes.get(0).logicalEdge(), member.subjectId())
                    : sourceLane.source().subtaskIndex();
            oldSourceParallelism.merge(edgeId, sourceIndex + 1,
                    Math::max);
            oldChannels.add(new OldChannel(member.subjectId(), edgeId,
                    sourceIndex, state.envelopes()));
        }
        if (oldChannels.isEmpty()) {
            throw new IllegalArgumentException(
                    "unaligned live rescale has no channel-state cut");
        }
        Set<String> broadcastFrames = new HashSet<>();
        for (OldChannel old : oldChannels) {
            List<StreamPhysicalExecutionPlan.Lane> targetLanes =
                    lanesByEdge.get(old.edgeId);
            StreamEdge edge = targetLanes.get(0).logicalEdge();
            int sourceParallelism = target.executionGraph.node(
                    edge.sourceOperatorUid()).parallelism();
            int mappedSource = Math.min(sourceParallelism - 1,
                    (int) ((long) old.sourceIndex * sourceParallelism
                            / oldSourceParallelism.get(old.edgeId)));
            Map<StreamPhysicalExecutionPlan.Lane, List<StreamEnvelope>>
                    projected = new LinkedHashMap<>();
            for (StreamPhysicalExecutionPlan.Lane lane : targetLanes) {
                if (lane.source().subtaskIndex() == mappedSource) {
                    List<StreamEnvelope> stream = new ArrayList<>();
                    replayStreams.get(lane).add(stream);
                    projected.put(lane, stream);
                }
            }
            for (StreamEnvelope envelope : old.envelopes) {
                if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
                    if (edge.partitioning()
                            == StreamEdge.Partitioning.BROADCAST
                            && !broadcastFrames.add(broadcastIdentity(
                            old, envelope))) {
                        continue;
                    }
                    routeRecord(target, edge, projected,
                            old.channelId, envelope);
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.WATERMARK
                        || envelope.kind()
                        == StreamEnvelope.Kind.WATERMARK_STATUS
                        || envelope.kind()
                        == StreamEnvelope.Kind.ASYNC_COMPLETION) {
                    for (List<StreamEnvelope> stream : projected.values()) {
                        stream.add(envelope);
                    }
                } else {
                    throw new IllegalArgumentException(
                            "live channel state contains an unsupported frame");
                }
            }
        }
        List<StreamCheckpointMember> result = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            StreamExecutionGraph.Node targetNode = target.executionGraph.node(
                    lane.target().operatorUid());
            StreamChannelIdentity channel = new StreamChannelIdentity(
                    targetCheckpoint.runId(), targetCheckpoint.attemptId(),
                    targetCheckpoint.fenceToken(),
                    targetAssignmentGeneration,
                    lane.physicalChannelId(), targetChannelEpoch);
            List<StreamEnvelope> replay = mergeStreams(
                    replayStreams.get(lane));
            StreamChannelState state = StreamChannelState.capture(
                    targetCheckpoint, channel, 0L,
                    Math.addExact(replay.size(), 1L),
                    rebind(replay, channel),
                    target.maximumEvents(),
                    targetNode.resourceRequest().checkpointBytes(),
                    maximumChannelFrameBytes(targetNode));
            FileCheckpointPayloadStore.StoredPayload stored = store(
                    StreamChannelStateCodec.encode(state), payloadStore);
            result.add(StreamCheckpointMember.channelState(
                    lane.target().operatorUid(),
                    lane.target().subtaskIndex(),
                    lane.physicalChannelId(), targetChannelEpoch,
                    stored.digest(), stored.contentBytes()));
        }
        return Collections.unmodifiableList(result);
    }

    private static void routeRecord(StreamProductExecutionGraph target,
            StreamEdge edge,
            Map<StreamPhysicalExecutionPlan.Lane,
                    List<StreamEnvelope>> projected,
            String sourceChannelId, StreamEnvelope envelope) {
        StreamExecutionGraph.Node targetNode = target.executionGraph.node(
                edge.targetOperatorUid());
        if (edge.partitioning() == StreamEdge.Partitioning.BROADCAST) {
            Set<Integer> routed = new HashSet<>();
            for (StreamPhysicalExecutionPlan.Lane lane : projected.keySet()) {
                if (routed.add(lane.target().subtaskIndex())) {
                    projected.get(lane).add(envelope);
                }
            }
            if (routed.size() != targetNode.parallelism()) {
                throw new IllegalStateException(
                        "live broadcast channel routing is incomplete");
            }
            return;
        }
        if (edge.partitioning() == StreamEdge.Partitioning.FORWARD
                && projected.size() == 1) {
            projected.values().iterator().next().add(envelope);
            return;
        }
        int targetSubtask;
        if (edge.partitioning() == StreamEdge.Partitioning.HASH) {
            byte[] key = stableKey(target, edge, envelope.recordFrame());
            int keyGroup = ManagedKeyedValueState.keyGroup(
                    key, targetNode.maximumParallelism());
            targetSubtask = Math.min(targetNode.parallelism() - 1,
                    (int) ((long) keyGroup * targetNode.parallelism()
                            / targetNode.maximumParallelism()));
        } else {
            int hash = 31 * sourceChannelId.hashCode()
                    + java.util.Arrays.hashCode(
                    StreamEnvelopeCodec.encode(envelope));
            targetSubtask = Math.floorMod(hash, targetNode.parallelism());
        }
        for (StreamPhysicalExecutionPlan.Lane lane : projected.keySet()) {
            if (lane.target().subtaskIndex() == targetSubtask) {
                projected.get(lane).add(envelope);
                return;
            }
        }
        throw new IllegalStateException(
                "live channel-state target lane is unavailable");
    }

    private static List<StreamEnvelope> mergeStreams(
            List<List<StreamEnvelope>> streams) {
        if (streams.isEmpty()) return Collections.emptyList();
        if (streams.size() == 1) return streams.get(0);
        List<StreamEnvelope> result = new ArrayList<>();
        int[] offsets = new int[streams.size()];
        long[] watermarks = new long[streams.size()];
        StreamEnvelope.WatermarkStatus[] statuses =
                new StreamEnvelope.WatermarkStatus[streams.size()];
        java.util.Arrays.fill(watermarks, Long.MIN_VALUE);
        java.util.Arrays.fill(statuses,
                StreamEnvelope.WatermarkStatus.ACTIVE);
        long emittedWatermark = Long.MIN_VALUE;
        long maximumStatusGeneration = 0L;
        StreamEnvelope lastStatus = null;
        Set<String> completions = new HashSet<>();
        boolean pending;
        do {
            pending = false;
            for (int index = 0; index < streams.size(); index++) {
                List<StreamEnvelope> stream = streams.get(index);
                if (offsets[index] >= stream.size()) continue;
                pending = true;
                StreamEnvelope frame = stream.get(offsets[index]++);
                if (frame.kind() == StreamEnvelope.Kind.RECORD) {
                    result.add(frame);
                } else if (frame.kind()
                        == StreamEnvelope.Kind.ASYNC_COMPLETION) {
                    if (completions.add(frame.operationIdentity())) {
                        result.add(frame);
                    }
                } else if (frame.kind() == StreamEnvelope.Kind.WATERMARK) {
                    if (frame.timestamp() < watermarks[index]) {
                        throw new IllegalArgumentException(
                                "live channel watermark regressed on its source lane");
                    }
                    watermarks[index] = frame.timestamp();
                } else if (frame.kind()
                        == StreamEnvelope.Kind.WATERMARK_STATUS) {
                    statuses[index] = frame.watermarkStatus();
                    maximumStatusGeneration = Math.max(
                            maximumStatusGeneration,
                            frame.watermarkStatusGeneration());
                    lastStatus = frame;
                }
                long minimum = Long.MAX_VALUE;
                boolean active = false;
                for (int lane = 0; lane < streams.size(); lane++) {
                    if (statuses[lane]
                            == StreamEnvelope.WatermarkStatus.IDLE) continue;
                    active = true;
                    minimum = Math.min(minimum, watermarks[lane]);
                }
                if (active && minimum > emittedWatermark) {
                    emittedWatermark = minimum;
                    result.add(StreamEnvelope.watermark(
                            frame.channelIdentity(), frame.sequence(), minimum));
                }
            }
        } while (pending);
        // Different old tasks may have unrelated status generations. Replaying
        // their transient transitions would either regress the merged lane or
        // invent a generation that the resumed source can later repeat. The
        // conservative cut carries only the final aggregate status at the
        // highest observed generation; transient idleness never advances the
        // restored task's logical status.
        if (lastStatus != null) {
            boolean idle = true;
            for (StreamEnvelope.WatermarkStatus status : statuses) {
                idle &= status == StreamEnvelope.WatermarkStatus.IDLE;
            }
            result.add(StreamEnvelope.watermarkStatus(
                    lastStatus.channelIdentity(), lastStatus.sequence(),
                    idle ? StreamEnvelope.WatermarkStatus.IDLE
                            : StreamEnvelope.WatermarkStatus.ACTIVE,
                    maximumStatusGeneration));
        }
        return result;
    }

    private static final class OldChannel {
        final String channelId;
        final String edgeId;
        final int sourceIndex;
        final List<StreamEnvelope> envelopes;

        private OldChannel(String channelId, String edgeId,
                int sourceIndex, List<StreamEnvelope> envelopes) {
            this.channelId = channelId;
            this.edgeId = edgeId;
            this.sourceIndex = sourceIndex;
            this.envelopes = envelopes;
        }
    }

    private static byte[] stableKey(StreamProductExecutionGraph target,
            StreamEdge edge, byte[] recordFrame) {
        PortableRowSchema schema = StreamEdgePartitioning.outputSchema(
                target.executionGraph, edge);
        List<String> keyFields = StreamEdgePartitioning.keyFields(
                target.executionGraph, edge);
        if (keyFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "HASH channel state has no stable key contract");
        }
        int[] indices = indices(schema, keyFields);
        RowSerializer rows = new RowSerializer(
                PortablePlanCompiler.toDataSchema(schema));
        RowSerializer keys = new RowSerializer(
                PortablePlanCompiler.toDataSchema(keySchema(schema, indices)));
        switch (StreamEdgePartitioning.outputChangelogMode(
                target.executionGraph, edge)) {
            case UPSERT:
                StreamUpsertRecord upsert = StreamUpsertRecordCodec.decode(
                        recordFrame, edge.maximumFrameBytes(),
                        edge.maximumFrameBytes());
                return upsert.stableKeyFrame();
            case RETRACT:
                StreamChangeRecord change = StreamChangeRecordCodec.decode(
                        recordFrame, edge.maximumFrameBytes(),
                        edge.maximumFrameBytes(), edge.maximumFrameBytes());
                if (change.hasStableKey()) return change.stableKeyFrame();
                byte[] image = change.hasBeforeImage()
                        ? change.beforeImageFrame()
                        : change.afterImageFrame();
                return keys.serializeProjected(
                        rows.deserialize(image), indices);
            case APPEND:
                DataRow row = rows.deserialize(recordFrame);
                return keys.serializeProjected(row, indices);
            default:
                throw new IllegalStateException(
                        "unknown Stream channel changelog mode");
        }
    }

    private static List<StreamEnvelope> rebind(
            List<StreamEnvelope> source, StreamChannelIdentity target) {
        List<StreamEnvelope> result = new ArrayList<>(source.size());
        long sequence = 0L;
        for (StreamEnvelope envelope : source) {
            result.add(StreamParallelKeyedOutput.rebind(
                    envelope, target, ++sequence));
        }
        return result;
    }

    private static String broadcastIdentity(
            OldChannel channel, StreamEnvelope envelope) {
        // Deduplicate fan-out copies from the same old source lane without
        // parsing delimiter-bearing logical edge IDs.
        return channel.edgeId.length() + ":" + channel.edgeId + ':'
                + channel.sourceIndex + ':' + envelope.sequence();
    }

    private static int[] indices(PortableRowSchema schema,
            List<String> fields) {
        int[] result = new int[fields.size()];
        for (int offset = 0; offset < fields.size(); offset++) {
            result[offset] = -1;
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(fields.get(offset))) {
                    result[offset] = index;
                    break;
                }
            }
            if (result[offset] < 0) {
                throw new IllegalArgumentException(
                        "channel-state key field is unavailable");
            }
        }
        return result;
    }

    private static PortableRowSchema keySchema(PortableRowSchema schema,
            int[] indices) {
        List<io.rowweave.portable.PortableField> fields =
                new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.field(index));
        return new PortableRowSchema(fields);
    }

    private static int maximumChannelFrameBytes(
            StreamExecutionGraph.Node node) {
        long framed = Math.min(
                node.resourceRequest().checkpointBytes(),
                Math.addExact((long) node.resourceRequest()
                        .maximumFrameBytes(), 64L * 1024L));
        return (int) Math.min(Integer.MAX_VALUE, framed);
    }

    private static byte[] read(StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        Path staged = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-live-channel-", ".restore");
            payloadStore.restore(
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes()),
                    staged);
            return Files.readAllBytes(staged);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "live channel-state restore failed", failure);
        } finally {
            delete(staged);
        }
    }

    private static FileCheckpointPayloadStore.StoredPayload store(
            byte[] frame, FileCheckpointPayloadStore payloadStore) {
        Path staged = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-live-channel-", ".snapshot");
            Files.write(staged, frame);
            return payloadStore.store(staged);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "live channel-state publication failed", failure);
        } finally {
            delete(staged);
        }
    }

    private static void delete(Path staged) {
        if (staged == null) return;
        try {
            Files.deleteIfExists(staged);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "live channel-state staging cleanup failed", failure);
        }
    }
}
