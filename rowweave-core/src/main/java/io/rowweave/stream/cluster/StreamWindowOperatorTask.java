package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
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
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FinalEventTimeDecimalWindow;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Dedicated mailbox owner for one keyed final event-time decimal window. */
final class StreamWindowOperatorTask implements StreamOperatorTask {
    private final StreamNodeTaskContext context;
    private final StreamChannelInput input;
    private final List<Route> routes;
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
    private volatile int maximumObservedWindows;
    private volatile boolean resourceUsageReady;

    StreamWindowOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            List<StreamTaskOutput> outputs) {
        this(taskName, context, input, outputs, null, null,
                0, 0, context == null ? -1
                        : context.maximumParallelism() - 1);
    }

    StreamWindowOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            List<StreamTaskOutput> outputs,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember) {
        this(taskName, context, input, outputs,
                checkpointPayloadStore,
                restoreMember, 0, 0,
                context == null ? -1
                        : context.maximumParallelism() - 1);
    }

    StreamWindowOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            List<StreamTaskOutput> outputs,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd) {
        if (taskName == null || taskName.isEmpty() || context == null
                || input == null || outputs == null
                || outputs.isEmpty()
                || subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= context.maximumParallelism()
                || context.node().opcode()
                != StreamOperatorCode.EVENT_TIME_WINDOW
                || context.inputs().size() != 1
                || context.inputs().get(0).stableKeyFields().isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream window task runtime is incomplete");
        }
        this.context = context;
        this.input = input;
        this.checkpointPayloadStore = checkpointPayloadStore;
        this.restoreMember = restoreMember;
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        List<Route> copied = new ArrayList<>();
        for (StreamTaskOutput output : outputs) {
            if (output.outputTagId != null) {
                throw new IllegalArgumentException(
                        "Stream window has no tagged output");
            }
            copied.add(new Route(output.output));
        }
        this.routes = Collections.unmodifiableList(copied);
        this.thread = new Thread(this::run,
                "rowweave-stream-window-" + taskName);
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
                    "checkpoint does not belong to the window task"));
            return request.result;
        }
        if (completedCheckpoint != null
                && checkpoint.checkpointSequence()
                <= completedCheckpoint.checkpointSequence()) {
            request.reject(new IllegalArgumentException(
                    "window checkpoint sequence was already observed"));
            return request.result;
        }
        if (terminalMember != null && !cancellationRequested.get()) {
            lastCheckpointMember = terminalMember;
            lastCheckpoint = checkpoint;
            return CompletableFuture.completedFuture(terminalMember);
        }
        if (completed || cancellationRequested.get()) {
            request.reject(new IllegalStateException(
                    "window task is already complete"));
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "window task already has a checkpoint in flight"));
            return request.result;
        }
        CheckpointIdentity observed = lastCheckpoint;
        if (observed != completedCheckpoint
                && pendingCheckpoint.compareAndSet(request, null)) {
            if (observed.equals(checkpoint)) {
                request.result.complete(lastCheckpointMember);
            } else {
                request.reject(new IllegalArgumentException(
                        "window checkpoint barrier differs from the trigger"));
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
                : StreamTaskResourceUsage.complete(0,
                memory.peakReservedBytes(), maximumObservedWindows,
                maximumObservedWindows);
    }
    boolean isCompleted() { return completed; }

    private void run() {
        Path spillRoot = null;
        FinalEventTimeDecimalWindow window = null;
        try {
            WindowBinding binding = WindowBinding.from(context);
            StreamResourceProfile resources = context.resources();
            spillRoot = Files.createTempDirectory("rowweave-stream-window-");
            managedMemory = new MemoryManager(
                    resources.managedStateBytes());
            window = new FinalEventTimeDecimalWindow(
                    context.artifactId(), binding.window.operatorUid(),
                    binding.window.stateDescriptors().get(0),
                    context.maximumParallelism(), keyGroupStart,
                    keyGroupEnd,
                    binding.windowType, binding.sizeMillis,
                    binding.slideMillis,
                    Math.min(1024, resources.windowCount()),
                    resources.timerCount(),
                    managedMemory,
                    spillRoot, resources.spillDiskBytes(),
                    resources.maximumFrameBytes(),
                    resources.maximumFrameBytes(),
                    result -> emitResult(binding, result));
            resourceUsageReady = true;
            if (restoreMember != null) {
                window.restore(subtaskIndex, restoreMember,
                        checkpointPayloadStore);
            }
            while (!cancellationRequested.get()) {
                StreamEnvelope envelope = input.receive();
                if (envelope == null) {
                    observeResources(window);
                    return;
                }
                switch (envelope.kind()) {
                    case RECORD:
                        binding.process(window, envelope);
                        break;
                    case WATERMARK:
                        window.advanceWatermark(envelope.timestamp());
                        broadcast(envelope);
                        break;
                    case WATERMARK_STATUS:
                        window.onWatermarkStatus(envelope.watermarkStatus(),
                                envelope.watermarkStatusGeneration());
                        broadcast(envelope);
                        break;
                    case CHECKPOINT_BARRIER:
                        observeCheckpoint(window,
                                envelope.checkpointIdentity());
                        broadcast(envelope);
                        break;
                    case END_OF_INPUT:
                        window.finishInput();
                        publishTerminalMember(window);
                        broadcast(envelope);
                        observeResources(window);
                        return;
                    case ASYNC_COMPLETION:
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
                                "unsupported Stream window event");
                }
                observeResources(window);
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
                        "window task ended before checkpoint barrier"));
            }
            if (window != null) {
                try {
                    observeResources(window);
                    window.close();
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

    private void observeResources(FinalEventTimeDecimalWindow window) {
        maximumObservedWindows = Math.max(maximumObservedWindows,
                window.maximumObservedWindowCount());
    }

    private void emitResult(WindowBinding binding,
            FinalEventTimeDecimalWindow.Result result) {
        DataRow key = binding.keySerializer.deserialize(result.canonicalKey());
        DataRow output = new DataRow(binding.outputSchema.fieldCount());
        for (int index = 0; index < binding.keyOutputIndices.length; index++) {
            output.set(binding.keyOutputIndices[index], key.get(index));
        }
        output.set(binding.startOutputIndex, result.windowStart());
        output.set(binding.endOutputIndex, result.windowEnd());
        output.set(binding.valueOutputIndex, result.sum());
        byte[] frame = binding.outputSerializer.serialize(output);
        for (Route route : routes) route.sendRecord(frame);
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

    private void observeCheckpoint(FinalEventTimeDecimalWindow window,
            CheckpointIdentity checkpoint) {
        if (checkpointPayloadStore == null || !sameAttempt(checkpoint)) {
            throw new IllegalArgumentException(
                    "window checkpoint runtime or scope is invalid");
        }
        CheckpointIdentity previous = lastCheckpoint;
        if (previous != null && checkpoint.checkpointSequence()
                <= previous.checkpointSequence()) {
            throw new IllegalArgumentException(
                    "window checkpoint sequence did not advance");
        }
        StreamCheckpointMember member = window.snapshot(
                subtaskIndex, checkpointPayloadStore);
        lastCheckpointMember = member;
        lastCheckpoint = checkpoint;
        CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
        if (pending != null) {
            if (pending.checkpoint.equals(checkpoint)) {
                pending.result.complete(member);
            } else {
                pending.reject(new IllegalArgumentException(
                        "window checkpoint barrier differs from the trigger"));
            }
        }
    }

    private void publishTerminalMember(FinalEventTimeDecimalWindow window) {
        if (checkpointPayloadStore == null) return;
        StreamCheckpointMember member = window.snapshot(
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

    private static final class WindowBinding {
        final PortableStreamNode window;
        final PortableRowSchema inputSchema;
        final PortableRowSchema outputSchema;
        final int[] keyInputIndices;
        final int[] keyOutputIndices;
        final int eventTimeIndex;
        final int valueInputIndex;
        final int startOutputIndex;
        final int endOutputIndex;
        final int valueOutputIndex;
        final long sizeMillis;
        final long slideMillis;
        final FinalEventTimeDecimalWindow.WindowType windowType;
        final RowSerializer inputSerializer;
        final RowSerializer keySerializer;
        final RowSerializer outputSerializer;

        private WindowBinding(PortableRowSchema inputSchema,
                PortableStreamNode window, int[] keyInputIndices,
                int[] keyOutputIndices, int eventTimeIndex,
                int valueInputIndex, int startOutputIndex,
                int endOutputIndex, int valueOutputIndex) {
            this.window = window;
            this.inputSchema = inputSchema;
            this.outputSchema = window.outputSchema();
            this.keyInputIndices = keyInputIndices;
            this.keyOutputIndices = keyOutputIndices;
            this.eventTimeIndex = eventTimeIndex;
            this.valueInputIndex = valueInputIndex;
            this.startOutputIndex = startOutputIndex;
            this.endOutputIndex = endOutputIndex;
            this.valueOutputIndex = valueOutputIndex;
            this.sizeMillis = Long.parseLong(
                    window.scalarParameters().get("window.sizeMillis"));
            this.slideMillis = Long.parseLong(
                    window.scalarParameters().get("window.slideMillis"));
            this.windowType = "TUMBLING".equals(
                    window.scalarParameters().get("window.type"))
                    ? FinalEventTimeDecimalWindow.WindowType.TUMBLING
                    : FinalEventTimeDecimalWindow.WindowType.SLIDING;
            this.inputSerializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(inputSchema));
            PortableRowSchema keySchema = keySchema(
                    inputSchema, keyInputIndices);
            this.keySerializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(keySchema));
            this.outputSerializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(outputSchema));
        }

        static WindowBinding from(StreamNodeTaskContext context) {
            PortableStreamNode window = context.node();
            StreamNodeTaskContext.Source input = context.inputs().size() == 1
                    ? context.inputs().get(0) : null;
            if (input == null
                    || input.stableKeyFields().isEmpty()
                    || window.opcode()
                    != StreamOperatorCode.EVENT_TIME_WINDOW) {
                throw new UnsupportedOperationException(
                        "Stream window node contract is invalid");
            }
            int[] keyInputs = indices(
                    input.schema(), input.stableKeyFields());
            int[] keyOutputs = indices(
                    window.outputSchema(), input.stableKeyFields());
            Map<String, String> parameters = window.scalarParameters();
            return new WindowBinding(input.schema(), window,
                    keyInputs, keyOutputs, index(input.schema(),
                            parameters.get("window.eventTimeField")),
                    index(input.schema(),
                            parameters.get("window.valueField")),
                    index(window.outputSchema(),
                            parameters.get("window.startOutputField")),
                    index(window.outputSchema(),
                            parameters.get("window.endOutputField")),
                    index(window.outputSchema(),
                            parameters.get("window.valueOutputField")));
        }

        void process(FinalEventTimeDecimalWindow runtime,
                StreamEnvelope envelope) {
            if (!envelope.hasEventTimestamp()) {
                throw new IllegalArgumentException(
                        "event-time Stream record lacks provider timestamp");
            }
            DataRow row = inputSerializer.deserialize(envelope.recordFrame());
            Timestamp fieldTimestamp = row.getTimestamp(eventTimeIndex);
            if (fieldTimestamp.getTime() != envelope.timestamp()) {
                throw new IllegalArgumentException(
                        "provider timestamp differs from the bound event-time field");
            }
            byte[] key = keySerializer.serializeProjected(row, keyInputIndices);
            runtime.process(key, (BigDecimal) row.get(valueInputIndex),
                    envelope.timestamp());
        }

        private static PortableRowSchema keySchema(
                PortableRowSchema input, int[] indices) {
            List<PortableField> fields = new ArrayList<>();
            for (int index : indices) fields.add(input.field(index));
            return new PortableRowSchema(fields);
        }

        private static int[] indices(
                PortableRowSchema schema, List<String> fieldIds) {
            int[] result = new int[fieldIds.size()];
            for (int index = 0; index < result.length; index++) {
                result[index] = index(schema, fieldIds.get(index));
            }
            return result;
        }

        private static int index(PortableRowSchema schema, String fieldId) {
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(fieldId)) return index;
            }
            throw new IllegalArgumentException(
                    "Stream window field binding is missing");
        }
    }

    private static final class Route {
        final StreamChannelOutput output;
        long sequence;

        private Route(StreamChannelOutput output) { this.output = output; }

        void sendRecord(byte[] frame) {
            output.send(StreamEnvelope.record(
                    output.channelIdentity(), next(), frame));
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
                            "unsupported Stream window signal");
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
