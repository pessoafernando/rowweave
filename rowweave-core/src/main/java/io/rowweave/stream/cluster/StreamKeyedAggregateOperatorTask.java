package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamAggregateFunction;
import io.rowweave.RetractableStreamAggregateFunction;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamFunctionBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.ManagedKeyedValueState;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Mailbox owner for one incremental managed keyed aggregate. */
final class StreamKeyedAggregateOperatorTask implements StreamOperatorTask {
    private static final byte[] VALUE_NAMESPACE = new byte[0];

    private final StreamNodeTaskContext context;
    private final StreamChannelInput input;
    private final List<Route> routes;
    private final StreamAggregateFunction function;
    private final FileCheckpointPayloadStore checkpointPayloadStore;
    private final StreamCheckpointMember restoreMember;
    private final int subtaskIndex;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();
    private volatile CheckpointIdentity lastCheckpoint;
    private volatile StreamCheckpointMember lastCheckpointMember;
    private volatile StreamCheckpointMember terminalMember;
    private volatile Throwable failure;
    private volatile boolean completed;
    private volatile MemoryManager managedMemory;
    private volatile boolean resourceUsageReady;

    StreamKeyedAggregateOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember) {
        this(taskName, context, input, outputs, resolver,
                checkpointPayloadStore, restoreMember, 0, 0,
                context == null ? -1
                        : context.maximumParallelism() - 1);
    }

    StreamKeyedAggregateOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd) {
        if (taskName == null || taskName.isEmpty() || context == null
                || input == null || outputs == null || resolver == null
                || outputs.isEmpty()
                || subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= context.maximumParallelism()
                || context.node().opcode()
                != StreamOperatorCode.KEYED_AGGREGATE
                || context.inputs().size() != 1
                || context.inputs().get(0).stableKeyFields().isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream aggregate task runtime is incomplete");
        }
        this.context = context;
        this.input = input;
        this.checkpointPayloadStore = checkpointPayloadStore;
        this.restoreMember = restoreMember;
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        Object resolved = resolver.resolve(((StreamFunctionBinding)
                context.node().binding()).deploymentReference());
        if (!(resolved instanceof StreamAggregateFunction)) {
            throw new IllegalArgumentException(
                    "resolved Stream aggregate ABI differs");
        }
        if (context.inputs().get(0).changelogMode()
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.RETRACT
                && !(resolved instanceof RetractableStreamAggregateFunction)) {
            throw new IllegalArgumentException(
                    "resolved Stream aggregate is not retractable");
        }
        this.function = (StreamAggregateFunction) resolved;
        List<Route> copied = new ArrayList<>();
        for (StreamTaskOutput output : outputs) {
            if (output.outputTagId != null) {
                throw new IllegalArgumentException(
                        "Stream aggregate has no tagged output");
            }
            copied.add(new Route(output.output));
        }
        this.routes = Collections.unmodifiableList(copied);
        this.thread = new Thread(this::run,
                "rowweave-stream-aggregate-" + taskName);
    }

    @Override public String operatorUid() {
        return context.node().operatorUid();
    }

    @Override public void start() { thread.start(); }

    @Override public CompletableFuture<List<StreamCheckpointMember>> checkpoint(
            CheckpointIdentity checkpoint) {
        return triggerCheckpoint(checkpoint).thenApply(
                Collections::singletonList);
    }

    synchronized CompletableFuture<StreamCheckpointMember> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint is required");
        }
        CheckpointIdentity completedCheckpoint = lastCheckpoint;
        if (checkpoint.equals(completedCheckpoint)) {
            return CompletableFuture.completedFuture(lastCheckpointMember);
        }
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (!sameAttempt(checkpoint)) {
            request.reject(new IllegalArgumentException(
                    "checkpoint does not belong to the aggregate task"));
            return request.result;
        }
        if (completedCheckpoint != null
                && checkpoint.checkpointSequence()
                <= completedCheckpoint.checkpointSequence()) {
            request.reject(new IllegalArgumentException(
                    "aggregate checkpoint sequence was already observed"));
            return request.result;
        }
        if (terminalMember != null && !cancellationRequested.get()) {
            lastCheckpointMember = terminalMember;
            lastCheckpoint = checkpoint;
            return CompletableFuture.completedFuture(terminalMember);
        }
        if (completed || cancellationRequested.get()) {
            request.reject(new IllegalStateException(
                    "aggregate task is already complete"));
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "aggregate task already has a checkpoint in flight"));
            return request.result;
        }
        CheckpointIdentity observed = lastCheckpoint;
        if (observed != completedCheckpoint
                && pendingCheckpoint.compareAndSet(request, null)) {
            if (observed.equals(checkpoint)) {
                request.result.complete(lastCheckpointMember);
            } else {
                request.reject(new IllegalArgumentException(
                        "aggregate checkpoint barrier differs from the trigger"));
            }
        }
        return request.result;
    }

    @Override public void cancel() {
        if (cancellationRequested.compareAndSet(false, true)) {
            input.cancel();
            for (Route route : routes) route.output.cancel();
            thread.interrupt();
        }
    }

    @Override public void awaitCompletion(long timeoutMillis)
            throws InterruptedException {
        thread.join(timeoutMillis);
    }

    @Override public Throwable failure() { return failure; }
    @Override public boolean completed() { return completed; }
    @Override public StreamTaskResourceUsage resourceUsage() {
        MemoryManager memory = managedMemory;
        return !resourceUsageReady || memory == null
                ? StreamTaskResourceUsage.unavailable()
                : StreamTaskResourceUsage.complete(
                0, memory.peakReservedBytes(), 0, 0);
    }

    private void run() {
        Path spillRoot = null;
        ManagedKeyedValueState state = null;
        try {
            AggregateBinding binding = AggregateBinding.from(context);
            StreamResourceProfile resources = context.resources();
            spillRoot = Files.createTempDirectory("rowweave-stream-aggregate-");
            managedMemory = new MemoryManager(
                    resources.managedStateBytes());
            state = new ManagedKeyedValueState(
                    context.artifactId(),
                    binding.aggregate.operatorUid(),
                    binding.aggregate.stateDescriptors().get(0),
                    context.maximumParallelism(), keyGroupStart,
                    keyGroupEnd,
                    managedMemory,
                    spillRoot, resources.spillDiskBytes(),
                    resources.maximumFrameBytes(),
                    resources.maximumFrameBytes(),
                    System::currentTimeMillis);
            resourceUsageReady = true;
            if (restoreMember != null) {
                state.restore(subtaskIndex, restoreMember,
                        checkpointPayloadStore);
            }
            while (!cancellationRequested.get()) {
                StreamEnvelope envelope = input.receive();
                if (envelope == null) return;
                switch (envelope.kind()) {
                    case RECORD:
                        process(binding, state, envelope);
                        break;
                    case WATERMARK:
                        state.advanceWatermark(envelope.timestamp());
                        broadcast(envelope);
                        break;
                    case END_OF_INPUT:
                        publishTerminalMember(state);
                        broadcast(envelope);
                        return;
                    case WATERMARK_STATUS:
                    case ASYNC_COMPLETION:
                        broadcast(envelope);
                        break;
                    case CHECKPOINT_BARRIER:
                        observeCheckpoint(state,
                                envelope.checkpointIdentity());
                        broadcast(envelope);
                        break;
                    case CANCEL:
                        cancel();
                        return;
                    case ERROR:
                        throw new IllegalStateException(
                                "upstream Stream task reported an error");
                    default:
                        throw new UnsupportedOperationException(
                                "unsupported Stream aggregate event");
                }
            }
        } catch (InterruptedException interrupted) {
            if (!cancellationRequested.get()) fail(interrupted);
            else Thread.currentThread().interrupt();
        } catch (Throwable thrown) {
            if (!cancellationRequested.get()) fail(thrown);
        } finally {
            CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
            if (pending != null) {
                pending.reject(new IllegalStateException(
                        "aggregate task ended before checkpoint barrier"));
            }
            if (state != null) {
                try {
                    state.close();
                } catch (Throwable closeFailure) {
                    if (failure == null && !cancellationRequested.get()) {
                        fail(closeFailure);
                    } else if (failure != null) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (spillRoot != null) {
                try {
                    Files.deleteIfExists(spillRoot);
                } catch (Throwable cleanupFailure) {
                    if (failure != null) failure.addSuppressed(cleanupFailure);
                }
            }
            completed = true;
        }
    }

    private void process(AggregateBinding binding,
            ManagedKeyedValueState state, StreamEnvelope envelope) {
        if (binding.retractInput) {
            processRetract(binding, state, envelope);
            return;
        }
        DataRow inputRow = binding.inputSerializer.deserialize(
                envelope.recordFrame());
        validate(inputRow, binding.inputSchema);
        if (binding.eventTimeTtl && !envelope.hasEventTimestamp()) {
            throw new IllegalArgumentException(
                    "event-time aggregate state requires record timestamps");
        }
        byte[] key = binding.inputKeySerializer.serializeProjected(
                inputRow, binding.inputKeyIndices);
        byte[] current = state.get(key, VALUE_NAMESPACE);
        if (current == null) current = requireAccumulator(
                function.createAccumulator(), binding.maximumFrameBytes);
        byte[] replacement = requireAccumulator(function.add(
                inputRow, current.clone()), binding.maximumFrameBytes);
        long eventTime = envelope.hasEventTimestamp()
                ? envelope.timestamp() : Long.MIN_VALUE;
        state.put(key, VALUE_NAMESPACE, replacement, eventTime);
        DataRow result = function.getResult(replacement.clone());
        validate(result, binding.outputSchema);
        byte[] outputKey = binding.outputKeySerializer.serializeProjected(
                result, binding.outputKeyIndices);
        if (!MessageDigest.isEqual(key, outputKey)) {
            throw new IllegalArgumentException(
                    "Stream aggregate changed its stable output key");
        }
        byte[] afterImage = binding.outputSerializer.serialize(result);
        byte[] upsert = StreamUpsertRecordCodec.encode(
                StreamUpsertRecord.afterImage(outputKey, afterImage));
        for (Route route : routes) route.sendRecord(upsert, envelope);
    }

    private void processRetract(AggregateBinding binding,
            ManagedKeyedValueState state, StreamEnvelope envelope) {
        StreamChangeRecord change = StreamChangeRecordCodec.decode(
                envelope.recordFrame(), binding.maximumFrameBytes,
                binding.maximumFrameBytes, binding.maximumFrameBytes);
        if (!change.hasBeforeImage() && !change.hasAfterImage()) {
            throw new UnsupportedOperationException(
                    "key-only DELETE cannot enter a reversible aggregate");
        }
        byte[] inputFrame = change.hasBeforeImage()
                ? change.beforeImageFrame() : change.afterImageFrame();
        DataRow inputRow = binding.inputSerializer.deserialize(inputFrame);
        validate(inputRow, binding.inputSchema);
        if (binding.eventTimeTtl && !envelope.hasEventTimestamp()) {
            throw new IllegalArgumentException(
                    "event-time aggregate state requires record timestamps");
        }
        byte[] key = binding.inputKeySerializer.serializeProjected(
                inputRow, binding.inputKeyIndices);
        if (change.hasStableKey() && !MessageDigest.isEqual(
                key, change.stableKeyFrame())) {
            throw new IllegalArgumentException(
                    "RETRACT aggregate key differs from its row image");
        }
        byte[] current = state.get(key, VALUE_NAMESPACE);
        boolean add = change.kind() == StreamChangeRecord.Kind.INSERT
                || change.kind() == StreamChangeRecord.Kind.UPDATE_AFTER;
        RetractableStreamAggregateFunction retractable =
                (RetractableStreamAggregateFunction) function;
        DataRow before = current == null ? null
                : function.getResult(current.clone());
        if (!add && current == null) {
            throw new IllegalArgumentException(
                    "RETRACT aggregate removed an absent key");
        }
        if (current == null) {
            current = requireAccumulator(function.createAccumulator(),
                    binding.maximumFrameBytes);
        }
        byte[] replacement = requireAccumulator(add
                ? function.add(inputRow, current.clone())
                : retractable.retract(inputRow, current.clone()),
                binding.maximumFrameBytes);
        boolean empty = !add && retractable.isEmpty(replacement.clone());
        DataRow after = empty ? null
                : function.getResult(replacement.clone());
        if (before != null) validate(before, binding.outputSchema);
        if (after != null) validate(after, binding.outputSchema);
        byte[] outputKey = binding.outputKeySerializer.serializeProjected(
                after == null ? before : after, binding.outputKeyIndices);
        if (!MessageDigest.isEqual(key, outputKey)) {
            throw new IllegalArgumentException(
                    "Stream aggregate changed its stable output key");
        }
        long eventTime = envelope.hasEventTimestamp()
                ? envelope.timestamp() : Long.MIN_VALUE;
        if (empty) {
            state.remove(key, VALUE_NAMESPACE);
        } else {
            state.put(key, VALUE_NAMESPACE, replacement, eventTime);
        }
        if (before == null) {
            sendChange(StreamChangeRecord.insert(
                    binding.outputSerializer.serialize(after)), envelope);
        } else if (after == null) {
            sendChange(StreamChangeRecord.delete(outputKey,
                    binding.outputSerializer.serialize(before)), envelope);
        } else {
            sendChange(StreamChangeRecord.updateBefore(outputKey,
                    binding.outputSerializer.serialize(before)), envelope);
            sendChange(StreamChangeRecord.updateAfter(outputKey,
                    binding.outputSerializer.serialize(after)), envelope);
        }
    }

    private void sendChange(StreamChangeRecord change,
            StreamEnvelope source) {
        byte[] frame = StreamChangeRecordCodec.encode(change);
        for (Route route : routes) route.sendRecord(frame, source);
    }

    private static byte[] requireAccumulator(byte[] accumulator, int maximum) {
        if (accumulator == null || accumulator.length == 0
                || accumulator.length > maximum) {
            throw new IllegalArgumentException(
                    "Stream aggregate accumulator exceeds its declared bound");
        }
        return accumulator.clone();
    }

    private void broadcast(StreamEnvelope envelope) {
        for (Route route : routes) route.sendSignal(envelope);
    }

    private void fail(Throwable thrown) {
        failure = thrown;
        input.fail(thrown);
        for (Route route : routes) route.output.fail(thrown);
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return input.channelIdentity().runId().equals(checkpoint.runId())
                && input.channelIdentity().attemptId().equals(
                checkpoint.attemptId())
                && input.channelIdentity().fenceToken()
                == checkpoint.fenceToken();
    }

    private void observeCheckpoint(ManagedKeyedValueState state,
            CheckpointIdentity checkpoint) {
        if (checkpointPayloadStore == null || !sameAttempt(checkpoint)) {
            throw new IllegalArgumentException(
                    "aggregate checkpoint runtime or scope is invalid");
        }
        CheckpointIdentity previous = lastCheckpoint;
        if (previous != null && checkpoint.checkpointSequence()
                <= previous.checkpointSequence()) {
            throw new IllegalArgumentException(
                    "aggregate checkpoint sequence did not advance");
        }
        StreamCheckpointMember member = state.snapshot(
                subtaskIndex, checkpointPayloadStore);
        lastCheckpointMember = member;
        lastCheckpoint = checkpoint;
        CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
        if (pending != null) {
            if (pending.checkpoint.equals(checkpoint)) {
                pending.result.complete(member);
            } else {
                pending.reject(new IllegalArgumentException(
                        "aggregate checkpoint barrier differs from the trigger"));
            }
        }
    }

    private void publishTerminalMember(ManagedKeyedValueState state) {
        if (checkpointPayloadStore == null) return;
        StreamCheckpointMember member = state.snapshot(
                subtaskIndex, checkpointPayloadStore);
        synchronized (this) {
            terminalMember = member;
            CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
            if (pending != null) {
                lastCheckpointMember = member;
                lastCheckpoint = pending.checkpoint;
                pending.result.complete(member);
            }
        }
    }

    private static void validate(DataRow row, PortableRowSchema schema) {
        if (row == null || row.size() != schema.fieldCount()) {
            throw new IllegalArgumentException(
                    "Stream aggregate row width differs from its declared schema");
        }
        for (int index = 0; index < schema.fieldCount(); index++) {
            PortableField field = schema.field(index);
            if (!field.nullable() && row.get(index) == null) {
                throw new IllegalArgumentException(
                        "Stream aggregate produced null for a non-null field");
            }
        }
    }

    private static final class AggregateBinding {
        final PortableStreamNode aggregate;
        final PortableRowSchema inputSchema;
        final PortableRowSchema outputSchema;
        final int[] inputKeyIndices;
        final int[] outputKeyIndices;
        final RowSerializer inputSerializer;
        final RowSerializer inputKeySerializer;
        final RowSerializer outputSerializer;
        final RowSerializer outputKeySerializer;
        final int maximumFrameBytes;
        final boolean eventTimeTtl;
        final boolean retractInput;

        private AggregateBinding(StreamNodeTaskContext context) {
            this.aggregate = context.node();
            StreamNodeTaskContext.Source input = context.inputs().get(0);
            this.inputSchema = input.schema();
            this.outputSchema = aggregate.outputSchema();
            this.inputKeyIndices = indices(
                    inputSchema, input.stableKeyFields());
            this.outputKeyIndices = indices(
                    outputSchema, aggregate.stableKeyFields());
            this.inputSerializer = serializer(inputSchema);
            this.inputKeySerializer = serializer(keySchema(
                    inputSchema, inputKeyIndices));
            this.outputSerializer = serializer(outputSchema);
            this.outputKeySerializer = serializer(keySchema(
                    outputSchema, outputKeyIndices));
            this.maximumFrameBytes = context.maximumFrameBytes();
            this.eventTimeTtl = aggregate.stateDescriptors().get(0).ttlMode()
                    == io.rowweave.stream.contract.StreamStateDescriptor
                    .TtlMode.EVENT_TIME;
            this.retractInput = input.changelogMode()
                    == io.rowweave.stream.contract.PortableStreamPlan
                    .ChangelogMode.RETRACT;
        }

        static AggregateBinding from(StreamNodeTaskContext context) {
            if (context.inputs().size() != 1
                    || context.inputs().get(0).stableKeyFields().isEmpty()
                    || context.node().opcode()
                    != StreamOperatorCode.KEYED_AGGREGATE) {
                throw new UnsupportedOperationException(
                        "Stream aggregate node contract is invalid");
            }
            return new AggregateBinding(context);
        }

        private static RowSerializer serializer(PortableRowSchema schema) {
            return new RowSerializer(PortablePlanCompiler.toDataSchema(schema));
        }

        private static PortableRowSchema keySchema(
                PortableRowSchema schema, int[] indices) {
            List<PortableField> fields = new ArrayList<>();
            for (int index : indices) fields.add(schema.field(index));
            return new PortableRowSchema(fields);
        }

        private static int[] indices(
                PortableRowSchema schema, List<String> fieldIds) {
            int[] result = new int[fieldIds.size()];
            for (int offset = 0; offset < result.length; offset++) {
                result[offset] = index(schema, fieldIds.get(offset));
            }
            return result;
        }

        private static int index(PortableRowSchema schema, String fieldId) {
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(fieldId)) return index;
            }
            throw new IllegalArgumentException(
                    "Stream aggregate stable key binding is missing");
        }
    }

    private static final class Route {
        final StreamChannelOutput output;
        long sequence;

        private Route(StreamChannelOutput output) { this.output = output; }

        void sendRecord(byte[] frame, StreamEnvelope source) {
            long next = next();
            output.send(source.hasEventTimestamp()
                    ? StreamEnvelope.record(output.channelIdentity(), next,
                    source.timestamp(), frame)
                    : StreamEnvelope.record(output.channelIdentity(), next, frame));
        }

        void sendSignal(StreamEnvelope source) {
            long next = next();
            switch (source.kind()) {
                case WATERMARK:
                    output.send(StreamEnvelope.watermark(
                            output.channelIdentity(), next, source.timestamp()));
                    break;
                case WATERMARK_STATUS:
                    output.send(StreamEnvelope.watermarkStatus(
                            output.channelIdentity(), next,
                            source.watermarkStatus(),
                            source.watermarkStatusGeneration()));
                    break;
                case CHECKPOINT_BARRIER:
                    output.send(StreamEnvelope.checkpointBarrier(
                            output.channelIdentity(), next,
                            source.checkpointIdentity()));
                    break;
                case END_OF_INPUT:
                    output.send(StreamEnvelope.endOfInput(
                            output.channelIdentity(), next));
                    break;
                case ASYNC_COMPLETION:
                    output.send(StreamEnvelope.asyncCompletion(
                            output.channelIdentity(), next,
                            source.operationIdentity()));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "unsupported Stream aggregate signal");
            }
        }

        private long next() {
            sequence = Math.addExact(sequence, 1L);
            return sequence;
        }
    }

    private static final class CheckpointRequest {
        final CheckpointIdentity checkpoint;
        final CompletableFuture<StreamCheckpointMember> result =
                new CompletableFuture<>();

        private CheckpointRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }

        void reject(Throwable failure) {
            result.completeExceptionally(failure);
        }
    }
}
