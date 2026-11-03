package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.contract.StreamCheckpointMember;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Runs one node/subtask against transport-neutral input and output lanes. */
final class StreamRemoteOperatorTaskRunner {
    private StreamRemoteOperatorTaskRunner() { }

    static StreamOperatorTask start(StreamExecutionGraph graph,
            String artifactId, PortableStreamNode node,
            List<? extends StreamChannelInput> inputs,
            List<StreamTaskLaneSpec> inputSpecs,
            List<StreamTaskOutput> outputs, StreamFunctionResolver resolver,
            int subtaskIndex,
            FileCheckpointPayloadStore payloadStore,
            List<StreamCheckpointMember> restoreMembers,
            boolean restore) {
        if (graph == null || artifactId == null || node == null
                || inputs == null || outputs == null || outputs.isEmpty()
                || resolver == null
                || subtaskIndex < 0 || subtaskIndex >= graph.node(
                node.operatorUid()).parallelism()) {
            throw new IllegalArgumentException(
                    "Stream remote operator task is incomplete");
        }
        StreamNodeTaskContext context = StreamNodeTaskContext.from(
                artifactId, graph, node.operatorUid());
        List<? extends StreamChannelInput> logicalInputs =
                mergeLogicalInputs(graph, node, subtaskIndex, inputs, inputSpecs,
                StreamOperatorTaskFactory.supportsMailbox(node.opcode()));
        if (StreamOperatorTaskFactory.supportsRouting(node.opcode())) {
            StreamChannelOutput mainOutput = mainOutput(outputs);
            StreamOperatorTask task = StreamOperatorTaskFactory.createRouting(
                    node, () -> new StreamUnionOperatorTask(node, logicalInputs,
                            mainOutput, payloadStore, restoreMembers,
                            restore,
                            subtaskIndex,
                            io.rowweave.stream.runtime
                                    .OfflineKeyedStateRescaler.keyGroupStart(
                                    subtaskIndex, context.parallelism(),
                                    context.maximumParallelism()),
                            io.rowweave.stream.runtime
                                    .OfflineKeyedStateRescaler.keyGroupEnd(
                                    subtaskIndex, context.parallelism(),
                                    context.maximumParallelism())));
            task.start();
            return task;
        }
        if (StreamOperatorTaskFactory.supportsMailbox(node.opcode())) {
            if (logicalInputs.isEmpty()) {
                throw new IllegalArgumentException(
                        "Stream mailbox task requires input lanes");
            }
            int start = io.rowweave.stream.runtime.OfflineKeyedStateRescaler
                    .keyGroupStart(subtaskIndex, context.parallelism(),
                            context.maximumParallelism());
            int end = io.rowweave.stream.runtime.OfflineKeyedStateRescaler
                    .keyGroupEnd(subtaskIndex, context.parallelism(),
                            context.maximumParallelism());
            StreamOperatorTask task = StreamOperatorTaskFactory.createMailbox(
                    artifactId + '-' + node.operatorUid() + '-'
                            + subtaskIndex, context, logicalInputs,
                    outputs, resolver, payloadStore,
                    restoreMembers, restore, subtaskIndex, start, end);
            task.start();
            return task;
        }
        if (!StreamOperatorTaskFactory.supportsInline(node.opcode())
                || logicalInputs.size() != 1) {
            throw new UnsupportedOperationException(
                    "Stream node task runner requires one inline input");
        }
        int keyGroupStart = io.rowweave.stream.runtime
                .OfflineKeyedStateRescaler.keyGroupStart(
                        subtaskIndex, context.parallelism(),
                        context.maximumParallelism());
        int keyGroupEnd = io.rowweave.stream.runtime
                .OfflineKeyedStateRescaler.keyGroupEnd(
                        subtaskIndex, context.parallelism(),
                        context.maximumParallelism());
        StreamOperatorTask delegate = StreamOperatorTaskFactory.create(node,
                context.inputs().get(0).schema(), resolver,
                context.maximumParallelism(),
                graph.portableJob().plan().deliveryGuarantee(), subtaskIndex,
                keyGroupStart, keyGroupEnd);
        StreamChannelOutput mainOutput = mainOutput(outputs);
        StreamCheckpointMember restoreMember = inlineRestoreMember(
                restoreMembers, node.operatorUid(), subtaskIndex,
                delegate.hasCheckpointState(), restore);
        InlineTask task = new InlineTask(delegate, graph, context, node,
                logicalInputs.get(0), mainOutput, payloadStore,
                restoreMember, restore, subtaskIndex);
        task.start();
        return task;
    }

    private static StreamCheckpointMember inlineRestoreMember(
            List<StreamCheckpointMember> members, String operatorUid,
            int subtaskIndex, boolean stateful, boolean restoring) {
        StreamCheckpointMember found = null;
        for (StreamCheckpointMember member : members) {
            if (member.kind() != StreamCheckpointMember.Kind.STATE
                    || !member.operatorUid().equals(operatorUid)
                    || member.subtaskIndex() != subtaskIndex) continue;
            if (found != null) {
                throw new IllegalArgumentException(
                        "Stream inline restore contains duplicate state");
            }
            found = member;
        }
        if (restoring && stateful && found == null) {
            throw new IllegalArgumentException(
                    "Stream inline restore state is missing");
        }
        if ((!restoring || !stateful) && found != null) {
            throw new IllegalArgumentException(
                    "Stream inline restore state is unexpected");
        }
        return found;
    }

    private static StreamChannelOutput mainOutput(
            List<StreamTaskOutput> outputs) {
        for (StreamTaskOutput output : outputs) {
            if (output.outputTagId == null) return output.output;
        }
        throw new IllegalArgumentException(
                "generic operator task has no main output route");
    }

    private static List<? extends StreamChannelInput> mergeLogicalInputs(
            StreamExecutionGraph graph, PortableStreamNode node,
            int subtaskIndex,
            List<? extends StreamChannelInput> inputs,
            List<StreamTaskLaneSpec> inputSpecs, boolean unwrapUpsert) {
        if (inputSpecs == null || inputSpecs.size() != inputs.size()) {
            throw new IllegalArgumentException(
                    "generic operator input lane metadata is incomplete");
        }
        Map<String, List<StreamChannelInput>> grouped = new LinkedHashMap<>();
        for (StreamEdge edge : graph.node(node.operatorUid()).inputEdges()) {
            grouped.put(edge.logicalChannelId(), new ArrayList<>());
        }
        for (int index = 0; index < inputSpecs.size(); index++) {
            String logical = StreamPhysicalLaneBinding.input(
                    graph, graph.node(node.operatorUid()), subtaskIndex,
                    inputSpecs.get(index).laneId()).logicalChannelId();
            grouped.get(logical).add(inputs.get(index));
        }
        List<StreamChannelInput> result = new ArrayList<>();
        for (StreamEdge edge
                : graph.node(node.operatorUid()).inputEdges()) {
            List<StreamChannelInput> lanes = grouped.get(
                    edge.logicalChannelId());
            if (lanes.isEmpty()) {
                throw new IllegalArgumentException(
                        "generic operator logical input has no physical lanes");
            }
            StreamChannelInput logical = lanes.size() == 1 ? lanes.get(0)
                    : new StreamInputGateChannel(lanes);
            if (unwrapUpsert
                    && StreamEdgePartitioning.producesUpsert(graph, edge)) {
                logical = new StreamUpsertInputChannel(
                        logical, edge.maximumFrameBytes());
            }
            result.add(logical);
        }
        return result;
    }

    private static void runInline(InlineTask runtime,
            StreamExecutionGraph graph, StreamNodeTaskContext context,
            PortableStreamNode node,
            StreamChannelInput input, StreamChannelOutput output) {
        StreamOperatorTask task = runtime.delegate;
        RowSerializer inputSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(
                        context.inputs().get(0).schema()));
        RowSerializer outputSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(node.outputSchema()));
        io.rowweave.stream.contract.PortableStreamPlan.ChangelogMode
                inputChangelog = context.inputs().get(0).changelogMode();
        boolean preserveUpsert = inputChangelog
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.UPSERT;
        boolean preserveRetract = inputChangelog
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.RETRACT;
        int[] inputKeyIndices = preserveUpsert || preserveRetract ? keyIndices(
                context.inputs().get(0).schema(),
                context.inputs().get(0).stableKeyFields()) : new int[0];
        RowSerializer inputKeySerializer = preserveUpsert || preserveRetract
                ? new RowSerializer(PortablePlanCompiler.toDataSchema(
                keySchema(context.inputs().get(0).schema(),
                        inputKeyIndices))) : null;
        int[] outputKeyIndices = preserveUpsert || preserveRetract
                ? keyIndices(node) : new int[0];
        RowSerializer outputKeySerializer = preserveUpsert || preserveRetract
                ? new RowSerializer(PortablePlanCompiler.toDataSchema(
                keySchema(node.outputSchema(), outputKeyIndices))) : null;
        AtomicReference<byte[]> currentStableKey = new AtomicReference<>();
        AtomicReference<StreamChangeRecord.Kind> currentChangeKind =
                new AtomicReference<>();
        AtomicLong sequence = new AtomicLong();
        try {
            StreamOperatorTask.Sink emitted = new StreamOperatorTask.Sink() {
                @Override public void emit(DataRow result, Long timestamp) {
                    long next = sequence.incrementAndGet();
                    byte[] frame = outputSerializer.serialize(result);
                    if (preserveUpsert) {
                        byte[] outputKey = outputKeySerializer
                                .serializeProjected(result, outputKeyIndices);
                        byte[] expectedKey = currentStableKey.get();
                        if (node.opcode() != StreamOperatorCode.ASYNC_WAIT
                                && (expectedKey == null
                                || !MessageDigest.isEqual(
                                expectedKey, outputKey))) {
                            throw new IllegalArgumentException(
                                    "Stream unary operator changed its stable output key");
                        }
                        frame = StreamUpsertRecordCodec.encode(
                                StreamUpsertRecord.afterImage(
                                        outputKey, frame));
                    } else if (preserveRetract) {
                        byte[] outputKey = outputKeySerializer
                                .serializeProjected(result, outputKeyIndices);
                        StreamChangeRecord.Kind kind = currentChangeKind.get();
                        if (kind == null) {
                            throw new IllegalStateException(
                                    "Stream RETRACT emission has no input change");
                        }
                        switch (kind) {
                            case INSERT:
                                frame = StreamChangeRecordCodec.encode(
                                        StreamChangeRecord.insert(frame));
                                break;
                            case UPDATE_BEFORE:
                                frame = StreamChangeRecordCodec.encode(
                                        StreamChangeRecord.updateBefore(
                                                outputKey, frame));
                                break;
                            case UPDATE_AFTER:
                                frame = StreamChangeRecordCodec.encode(
                                        StreamChangeRecord.updateAfter(
                                                outputKey, frame));
                                break;
                            case DELETE:
                                frame = StreamChangeRecordCodec.encode(
                                        StreamChangeRecord.delete(
                                                outputKey, frame));
                                break;
                            default:
                                throw new IllegalStateException(
                                        "unknown Stream RETRACT change kind");
                        }
                    }
                    output.send(timestamp == null
                            ? StreamEnvelope.record(output.channelIdentity(),
                            next, frame)
                            : StreamEnvelope.record(output.channelIdentity(),
                            next, timestamp, frame));
                }

                @Override public void emitWatermark(long timestamp) {
                    output.send(StreamEnvelope.watermark(
                            output.channelIdentity(), sequence.incrementAndGet(),
                            timestamp));
                }

                @Override public void emitWatermarkStatus(
                        StreamEnvelope.WatermarkStatus status, long generation) {
                    output.send(StreamEnvelope.watermarkStatus(
                            output.channelIdentity(), sequence.incrementAndGet(),
                            status, generation));
                }

                @Override public void emitEnd() {
                    output.send(StreamEnvelope.endOfInput(
                            output.channelIdentity(), sequence.incrementAndGet()));
                }

                @Override public void fail(Throwable cause) {
                    output.fail(cause);
                }
            };
            while (true) {
                StreamEnvelope envelope;
                if (task.hasIdleTimer()) {
                    long now = System.nanoTime();
                    long waitMillis = task.idleDelayMillis(now);
                    if (waitMillis < 1L) waitMillis = 1L;
                    if (waitMillis > 1_000L) waitMillis = 1_000L;
                    envelope = input.poll(waitMillis);
                    task.onIdleTimer(System.nanoTime(), emitted);
                    if (envelope == null) continue;
                } else {
                    envelope = input.receive();
                }
                if (envelope == null) {
                    runtime.completeNormally();
                    return;
                }
                if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
                    byte[] inputFrame = envelope.recordFrame();
                    byte[] stableKey = null;
                    if (preserveUpsert) {
                        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                                inputFrame, context.maximumFrameBytes(),
                                context.maximumFrameBytes());
                        if (!record.hasAfterImage()) {
                            output.send(StreamEnvelope.record(
                                    output.channelIdentity(),
                                    sequence.incrementAndGet(), inputFrame));
                            continue;
                        }
                        stableKey = record.stableKeyFrame();
                        inputFrame = record.afterImageFrame();
                    } else if (preserveRetract) {
                        StreamChangeRecord record = StreamChangeRecordCodec
                                .decode(inputFrame,
                                        context.maximumFrameBytes(),
                                        context.maximumFrameBytes(),
                                        context.maximumFrameBytes());
                        currentChangeKind.set(record.kind());
                        if (record.hasStableKey()) {
                            stableKey = record.stableKeyFrame();
                        }
                        if (record.hasBeforeImage()) {
                            inputFrame = record.beforeImageFrame();
                        } else if (record.hasAfterImage()) {
                            inputFrame = record.afterImageFrame();
                        } else {
                            throw new UnsupportedOperationException(
                                    "key-only DELETE cannot enter a row-transforming Stream operator");
                        }
                    }
                    DataRow row = inputSerializer.deserialize(inputFrame);
                    if (preserveUpsert || (preserveRetract
                            && stableKey != null)) {
                        byte[] projected = inputKeySerializer
                                .serializeProjected(row, inputKeyIndices);
                        if (!MessageDigest.isEqual(stableKey, projected)) {
                            throw new IllegalArgumentException(
                                    "Stream change key differs from its image row");
                        }
                        currentStableKey.set(stableKey);
                    }
                    try {
                        task.process(row,
                                envelope.hasEventTimestamp()
                                        ? envelope.timestamp() : null, emitted);
                    } finally {
                        currentStableKey.set(null);
                        currentChangeKind.set(null);
                    }
                } else if (envelope.kind() == StreamEnvelope.Kind.WATERMARK) {
                    if (task.onWatermark(envelope.timestamp(), emitted)) {
                        emitted.emitWatermark(envelope.timestamp());
                    }
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.WATERMARK_STATUS) {
                    if (task.onWatermarkStatus(envelope.watermarkStatus(),
                            envelope.watermarkStatusGeneration(), emitted)) {
                        emitted.emitWatermarkStatus(envelope.watermarkStatus(),
                                envelope.watermarkStatusGeneration());
                    }
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.END_OF_INPUT) {
                    if (task.onEnd(emitted)) {
                        emitted.emitEnd();
                        output.close();
                        runtime.completeNormally();
                        return;
                    }
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
                    List<StreamCheckpointMember> members =
                            runtime.snapshot(envelope.checkpointIdentity());
                    output.send(StreamParallelKeyedOutput.rebind(envelope,
                            output.channelIdentity(),
                            sequence.incrementAndGet()));
                    runtime.observeCheckpoint(
                            envelope.checkpointIdentity(), members);
                } else {
                    output.send(StreamParallelKeyedOutput.rebind(envelope,
                            output.channelIdentity(),
                            sequence.incrementAndGet()));
                }
            }
        } catch (Throwable failure) {
            if (runtime.cancelled.get()) return;
            task.close();
            output.fail(failure);
            runtime.fail(failure);
        }
    }

    private static final class InlineTask implements StreamOperatorTask {
        private final StreamOperatorTask delegate;
        private final StreamChannelInput input;
        private final StreamChannelOutput output;
        private final FileCheckpointPayloadStore payloadStore;
        private final StreamCheckpointMember restoreMember;
        private final boolean restoring;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final Thread thread;
        private volatile Throwable failure;
        private volatile boolean completed;
        private CheckpointIdentity lastCheckpoint;
        private List<StreamCheckpointMember> lastMembers =
                Collections.emptyList();
        private CheckpointRequest pendingCheckpoint;

        private InlineTask(StreamOperatorTask delegate,
                StreamExecutionGraph graph, StreamNodeTaskContext context,
                PortableStreamNode node,
                StreamChannelInput input, StreamChannelOutput output,
                FileCheckpointPayloadStore payloadStore,
                StreamCheckpointMember restoreMember, boolean restoring,
                int subtaskIndex) {
            this.delegate = delegate;
            this.input = input;
            this.output = output;
            this.payloadStore = payloadStore;
            this.restoreMember = restoreMember;
            this.restoring = restoring;
            this.thread = new Thread(() -> runInline(this, graph, context, node,
                    input, output), "rowweave-stream-operator-"
                    + node.operatorUid() + '-' + subtaskIndex);
            thread.setDaemon(true);
        }

        @Override public String operatorUid() {
            return delegate.operatorUid();
        }

        @Override public StreamTaskResourceUsage resourceUsage() {
            return delegate.resourceUsage();
        }

        @Override public void start() {
            if (restoring && delegate.hasCheckpointState()) {
                delegate.restore(restoreMember, payloadStore);
            }
            delegate.start();
            thread.start();
        }

        @Override public synchronized CompletableFuture<
                List<StreamCheckpointMember>> checkpoint(
                CheckpointIdentity checkpoint) {
            if (checkpoint == null) {
                throw new IllegalArgumentException("checkpoint is required");
            }
            if (!sameAttempt(checkpoint)) {
                return failedFuture(new IllegalArgumentException(
                        "inline checkpoint does not belong to the task"));
            }
            if (checkpoint.equals(lastCheckpoint)) {
                return CompletableFuture.completedFuture(lastMembers);
            }
            if (lastCheckpoint != null && checkpoint.checkpointSequence()
                    <= lastCheckpoint.checkpointSequence()) {
                return failedFuture(new IllegalArgumentException(
                        "inline checkpoint sequence was already observed"));
            }
            if (failure != null) return failedFuture(failure);
            if (cancelled.get()) return failedFuture(
                    new IllegalStateException("inline task is canceled"));
            if (completed) {
                try {
                    List<StreamCheckpointMember> members = snapshot(checkpoint);
                    lastCheckpoint = checkpoint;
                    lastMembers = members;
                    return CompletableFuture.completedFuture(members);
                } catch (Throwable thrown) {
                    return failedFuture(thrown);
                }
            }
            if (pendingCheckpoint != null) {
                if (pendingCheckpoint.checkpoint.equals(checkpoint)) {
                    return pendingCheckpoint.result;
                }
                return failedFuture(new IllegalStateException(
                        "inline task already has a checkpoint in flight"));
            }
            pendingCheckpoint = new CheckpointRequest(checkpoint);
            return pendingCheckpoint.result;
        }

        private List<StreamCheckpointMember> snapshot(
                CheckpointIdentity checkpoint) {
            if (!delegate.hasCheckpointState()) return Collections.emptyList();
            StreamCheckpointMember member = delegate.snapshot(
                    checkpoint, payloadStore);
            if (member == null) {
                throw new IllegalStateException(
                        "inline task snapshot returned no member");
            }
            return Collections.singletonList(member);
        }

        private synchronized void observeCheckpoint(
                CheckpointIdentity checkpoint,
                List<StreamCheckpointMember> members) {
            if (!sameAttempt(checkpoint) || lastCheckpoint != null
                    && checkpoint.checkpointSequence()
                    <= lastCheckpoint.checkpointSequence()) {
                throw new IllegalArgumentException(
                        "inline checkpoint barrier is stale or foreign");
            }
            lastCheckpoint = checkpoint;
            lastMembers = members;
            CheckpointRequest pending = pendingCheckpoint;
            pendingCheckpoint = null;
            if (pending == null) return;
            if (pending.checkpoint.equals(checkpoint)) {
                pending.result.complete(members);
            } else {
                pending.result.completeExceptionally(
                        new IllegalArgumentException(
                                "inline checkpoint barrier differs from trigger"));
            }
        }

        private boolean sameAttempt(CheckpointIdentity checkpoint) {
            return input.channelIdentity().runId().equals(checkpoint.runId())
                    && input.channelIdentity().attemptId().equals(
                    checkpoint.attemptId())
                    && input.channelIdentity().fenceToken()
                    == checkpoint.fenceToken();
        }

        private synchronized void completeNormally() {
            completed = true;
            CheckpointRequest pending = pendingCheckpoint;
            pendingCheckpoint = null;
            if (pending != null) {
                try {
                    List<StreamCheckpointMember> members = snapshot(
                            pending.checkpoint);
                    lastCheckpoint = pending.checkpoint;
                    lastMembers = members;
                    pending.result.complete(members);
                } catch (Throwable thrown) {
                    failure = thrown;
                    pending.result.completeExceptionally(thrown);
                }
            }
            delegate.close();
        }

        private synchronized void fail(Throwable thrown) {
            if (failure == null) failure = thrown;
            CheckpointRequest pending = pendingCheckpoint;
            pendingCheckpoint = null;
            if (pending != null) pending.result.completeExceptionally(thrown);
        }

        @Override public void cancel() {
            if (!cancelled.compareAndSet(false, true)) return;
            input.cancel();
            output.cancel();
            delegate.close();
            thread.interrupt();
            rejectCheckpoint(
                    new IllegalStateException("inline task is canceled"));
        }

        @Override public void close() { cancel(); }

        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            thread.join(timeoutMillis);
        }

        @Override public Throwable failure() { return failure; }
        @Override public boolean completed() { return completed; }
        @Override public boolean hasCheckpointState() {
            return delegate.hasCheckpointState();
        }

        private static <T> CompletableFuture<T> failedFuture(
                Throwable failure) {
            CompletableFuture<T> result = new CompletableFuture<>();
            result.completeExceptionally(failure);
            return result;
        }

        private synchronized void rejectCheckpoint(Throwable thrown) {
            CheckpointRequest pending = pendingCheckpoint;
            pendingCheckpoint = null;
            if (pending != null) pending.result.completeExceptionally(thrown);
        }

        private static final class CheckpointRequest {
            final CheckpointIdentity checkpoint;
            final CompletableFuture<List<StreamCheckpointMember>> result =
                    new CompletableFuture<>();

            private CheckpointRequest(CheckpointIdentity checkpoint) {
                this.checkpoint = checkpoint;
            }
        }
    }

    private static int[] keyIndices(PortableStreamNode node) {
        return keyIndices(node.outputSchema(), node.stableKeyFields());
    }

    private static int[] keyIndices(PortableRowSchema schema,
            List<String> stableKeys) {
        int[] result = new int[stableKeys.size()];
        for (int offset = 0; offset < result.length; offset++) {
            String fieldId = stableKeys.get(offset);
            boolean found = false;
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(fieldId)) {
                    result[offset] = index;
                    found = true;
                    break;
                }
            }
            if (!found) throw new IllegalArgumentException(
                    "Stream UPSERT output stable key field is unavailable");
        }
        return result;
    }

    private static PortableRowSchema keySchema(PortableRowSchema schema,
            int[] indices) {
        List<PortableField> fields = new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.field(index));
        return new PortableRowSchema(fields);
    }
}
