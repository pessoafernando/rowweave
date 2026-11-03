package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.RetractableStreamWindowAggregateFunction;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.StreamWindowCollector;
import io.rowweave.StreamWindowContext;
import io.rowweave.StreamWindowProcessFunction;
import io.rowweave.StreamWindowDataRowCursor;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.contract.StreamWindowBinding;
import io.rowweave.stream.contract.StreamWindowResourceDescriptor;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.ManagedKeyedValueState;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Mailbox owner for one bounded generic-window subtask. */
final class StreamGenericWindowOperatorTask implements StreamOperatorTask {
    private static final int WINDOW_NAMESPACE_BYTES = 16;
    private static final int WINDOW_HEADER_BYTES = 16;
    private static final int ROW_NAMESPACE_BYTES = 25;
    private static final byte[] COUNT_NAMESPACE = new byte[] {0x43};
    private static final byte ROW_NAMESPACE_PREFIX = 0x52;

    private final StreamNodeTaskContext context;
    private final StreamChannelInput input;
    private final List<Route> routes;
    private final WindowContract contract;
    private final StreamWindowAggregateFunction aggregateFunction;
    private final StreamWindowProcessFunction processFunction;
    private final FileCheckpointPayloadStore checkpointPayloadStore;
    private final StreamCheckpointMember restoreMember;
    private final int subtaskIndex;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final LongSupplier processingClock;
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
    private volatile int maximumObservedTimers;
    private volatile boolean resourceUsageReady;
    private long watermark = Long.MIN_VALUE;
    private long processingTimeFloor = Long.MIN_VALUE;

    StreamGenericWindowOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            Map<String, StreamChannelOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd) {
        this(taskName, context, input, sinkOutputs(context, outputs), resolver,
                checkpointPayloadStore, restoreMember, subtaskIndex,
                keyGroupStart, keyGroupEnd, System::currentTimeMillis);
    }

    StreamGenericWindowOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            Map<String, StreamChannelOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd,
            LongSupplier processingClock) {
        this(taskName, context, input, sinkOutputs(context, outputs), resolver,
                checkpointPayloadStore, restoreMember, subtaskIndex,
                keyGroupStart, keyGroupEnd, processingClock);
    }

    static StreamGenericWindowOperatorTask composed(String taskName,
            StreamNodeTaskContext context,
            StreamChannelInput input, List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd) {
        return new StreamGenericWindowOperatorTask(taskName, context,
                input, outputs, resolver, checkpointPayloadStore, restoreMember,
                subtaskIndex, keyGroupStart, keyGroupEnd,
                System::currentTimeMillis);
    }

    private StreamGenericWindowOperatorTask(String taskName,
            StreamNodeTaskContext context,
            StreamChannelInput input,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            StreamCheckpointMember restoreMember, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd,
            LongSupplier processingClock) {
        if (taskName == null || taskName.isEmpty() || context == null
                || input == null || outputs == null || resolver == null
                || processingClock == null
                || outputs.isEmpty()
                || subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= context.maximumParallelism()
                || context.node().opcode()
                != StreamOperatorCode.GENERIC_WINDOW
                || context.inputs().size() != 1
                || context.inputs().get(0).stableKeyFields().isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream generic window task runtime is incomplete");
        }
        this.context = context;
        this.input = input;
        this.checkpointPayloadStore = checkpointPayloadStore;
        this.restoreMember = restoreMember;
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.processingClock = processingClock;
        this.contract = WindowContract.from(context);
        Object resolved = resolver.resolve(
                contract.binding.deploymentReference());
        boolean aggregate = contract.binding.functionKind()
                == StreamWindowBinding.FunctionKind.AGGREGATE;
        if (aggregate && !(resolved instanceof StreamWindowAggregateFunction)
                || !aggregate
                && !(resolved instanceof StreamWindowProcessFunction)
                || contract.retractInput
                && !(resolved instanceof
                RetractableStreamWindowAggregateFunction)) {
            throw new IllegalArgumentException(
                    "resolved Stream generic window ABI differs");
        }
        this.aggregateFunction = aggregate
                ? (StreamWindowAggregateFunction) resolved : null;
        this.processFunction = aggregate ? null
                : (StreamWindowProcessFunction) resolved;
        List<Route> copied = new ArrayList<>();
        for (StreamTaskOutput output : outputs) {
            if (output.outputTagId != null
                    && (contract.binding.lateOutputTag() == null
                    || !output.outputTagId.equals(
                    contract.binding.lateOutputTag().tagId()))) {
                throw new IllegalArgumentException(
                        "Stream generic window output route is unknown");
            }
            copied.add(new Route(output.output, output.outputTagId));
        }
        this.routes = Collections.unmodifiableList(copied);
        this.thread = new Thread(this::run,
                "rowweave-stream-generic-window-" + taskName);
    }

    private static List<StreamTaskOutput> sinkOutputs(
            StreamNodeTaskContext context,
            Map<String, StreamChannelOutput> outputs) {
        if (context == null || outputs == null
                || outputs.size() != context.outputs().size()) {
            throw new IllegalArgumentException(
                    "Stream generic window Sink routes are incomplete");
        }
        List<StreamTaskOutput> routes = new ArrayList<>();
        for (StreamNodeTaskContext.Sink route : context.outputs()) {
            StreamChannelOutput output = outputs.get(
                    route.consumer().operatorUid());
            if (output == null) {
                throw new IllegalArgumentException(
                        "Stream generic window Sink route is missing");
            }
            routes.add(new StreamTaskOutput(
                    route.edge().sourceOutputTagId(), output));
        }
        return routes;
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
                    "checkpoint does not belong to the generic window task"));
            return request.result;
        }
        if (completedCheckpoint != null
                && checkpoint.checkpointSequence()
                <= completedCheckpoint.checkpointSequence()) {
            request.reject(new IllegalArgumentException(
                    "generic window checkpoint sequence was already observed"));
            return request.result;
        }
        if (terminalMember != null && !cancellationRequested.get()) {
            lastCheckpointMember = terminalMember;
            lastCheckpoint = checkpoint;
            return CompletableFuture.completedFuture(terminalMember);
        }
        if (completed || cancellationRequested.get()) {
            request.reject(new IllegalStateException(
                    "generic window task is already complete"));
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "generic window task already has a checkpoint in flight"));
            return request.result;
        }
        CheckpointIdentity observed = lastCheckpoint;
        if (observed != completedCheckpoint
                && pendingCheckpoint.compareAndSet(request, null)) {
            if (observed.equals(checkpoint)) {
                request.result.complete(lastCheckpointMember);
            } else {
                request.reject(new IllegalArgumentException(
                        "generic window barrier differs from the trigger"));
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
                memory.peakReservedBytes(), maximumObservedTimers,
                maximumObservedWindows);
    }

    boolean isCompleted() { return completed; }

    private void run() {
        Path spillRoot = null;
        ManagedKeyedValueState state = null;
        try {
            requireInitialRuntime(contract);
            StreamResourceProfile resources = context.resources();
            spillRoot = Files.createTempDirectory(
                    "rowweave-stream-generic-window-");
            managedMemory = new MemoryManager(
                    resources.managedStateBytes());
            state = new ManagedKeyedValueState(
                    context.artifactId(),
                    contract.window.operatorUid(),
                    contract.window.stateDescriptors().get(0),
                    context.maximumParallelism(), keyGroupStart, keyGroupEnd,
                    managedMemory,
                    spillRoot, resources.spillDiskBytes(),
                    resources.maximumFrameBytes(),
                    resources.maximumFrameBytes(), this::processingTime);
            resourceUsageReady = true;
            if (restoreMember != null) {
                state.restore(subtaskIndex, restoreMember,
                        checkpointPayloadStore);
                watermark = state.currentWatermark();
            }
            while (!cancellationRequested.get()) {
                StreamEnvelope envelope = receiveNext(contract, state);
                if (envelope == null) {
                    if (processingTimeWindow(contract.binding)) continue;
                    return;
                }
                switch (envelope.kind()) {
                    case RECORD:
                        process(contract, state, envelope);
                        break;
                    case WATERMARK:
                        advanceWatermark(contract, state,
                                envelope.timestamp());
                        broadcast(envelope);
                        break;
                    case WATERMARK_STATUS:
                    case ASYNC_COMPLETION:
                        broadcast(envelope);
                        break;
                    case END_OF_INPUT:
                        if (eventTimeWindow(contract.binding)) {
                            advanceWatermark(contract, state,
                                    Long.MAX_VALUE);
                        } else if (countWindow(contract.binding)) {
                            flushCountWindows(contract, state);
                        } else if (processingTimeWindow(contract.binding)) {
                            flushProcessingWindows(contract, state);
                        }
                        publishTerminalMember(state);
                        broadcast(envelope);
                        return;
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
                                "unsupported generic window event");
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
                        "generic window task ended before checkpoint barrier"));
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

    private static void requireInitialRuntime(WindowContract contract) {
        StreamWindowBinding binding = contract.binding;
        if ((binding.functionKind()
                != StreamWindowBinding.FunctionKind.AGGREGATE
                && binding.functionKind()
                != StreamWindowBinding.FunctionKind.PROCESS)
                || (binding.assignerKind()
                != StreamWindowBinding.AssignerKind.EVENT_TIME_TUMBLING
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.EVENT_TIME_SLIDING
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.EVENT_TIME_SESSION
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.PROCESSING_TIME_TUMBLING
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.PROCESSING_TIME_SLIDING
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.PROCESSING_TIME_SESSION
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.COUNT_TUMBLING
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.COUNT_SLIDING)
                || (countWindow(binding)
                ? binding.triggerKind()
                != StreamWindowBinding.TriggerKind.COUNT
                : processingTimeWindow(binding)
                ? binding.triggerKind()
                != StreamWindowBinding.TriggerKind.PROCESSING_TIME
                : binding.triggerKind()
                != StreamWindowBinding.TriggerKind.EVENT_TIME_FINAL)) {
            throw new UnsupportedOperationException(
                    "generic window runtime shape is not implemented");
        }
    }

    private StreamEnvelope receiveNext(WindowContract contract,
            ManagedKeyedValueState state) throws InterruptedException {
        if (!processingTimeWindow(contract.binding)) return input.receive();
        long now = processingTime();
        fireDueProcessingWindows(contract, state, now);
        return input.poll(processingWindowWaitMillis(state, now));
    }

    private void process(WindowContract contract,
            ManagedKeyedValueState state, StreamEnvelope envelope) {
        StreamChangeRecord change = null;
        byte[] recordFrame = envelope.recordFrame();
        boolean add = true;
        if (contract.retractInput) {
            change = StreamChangeRecordCodec.decode(recordFrame,
                    context.maximumFrameBytes(), context.maximumFrameBytes(),
                    context.maximumFrameBytes());
            if (!change.hasBeforeImage() && !change.hasAfterImage()) {
                throw new UnsupportedOperationException(
                        "key-only DELETE cannot enter a reversible window");
            }
            recordFrame = change.hasBeforeImage()
                    ? change.beforeImageFrame() : change.afterImageFrame();
            add = change.kind() == StreamChangeRecord.Kind.INSERT
                    || change.kind()
                    == StreamChangeRecord.Kind.UPDATE_AFTER;
        }
        DataRow row = contract.inputSerializer.deserialize(
                recordFrame);
        validate(row, contract.inputSchema,
                "generic window input row");
        if (eventTimeWindow(contract.binding)) {
            if (!envelope.hasEventTimestamp()) {
                throw new IllegalArgumentException(
                        "event-time window record lacks provider timestamp");
            }
            Timestamp fieldTimestamp = row.getTimestamp(
                    contract.eventTimeIndex);
            if (fieldTimestamp == null
                    || fieldTimestamp.getTime() != envelope.timestamp()) {
                throw new IllegalArgumentException(
                        "provider timestamp differs from the window field");
            }
        }
        byte[] key = contract.inputKeySerializer.serializeProjected(
                row, contract.inputKeyIndices);
        if (change != null && change.hasStableKey()
                && !MessageDigest.isEqual(
                key, change.stableKeyFrame())) {
            throw new IllegalArgumentException(
                    "RETRACT window key differs from its row image");
        }
        long assignmentTime = processingTimeWindow(contract.binding)
                ? processingTime() : eventTimeWindow(contract.binding)
                ? envelope.timestamp() : Long.MIN_VALUE;
        if (sessionWindow(contract.binding)) {
            if (processFunction == null) {
                processSession(contract, state, envelope, row, key,
                        assignmentTime);
            } else {
                processFullSession(contract, state, envelope, row, key,
                        assignmentTime);
            }
            return;
        }
        if (processFunction != null) {
            processFullWindow(contract, state, envelope, row, key,
                    assignmentTime);
            return;
        }
        byte[] countValue = countWindow(contract.binding)
                ? state.get(key, COUNT_NAMESPACE) : null;
        long ordinal = countWindow(contract.binding)
                ? countOrdinal(countValue) : -1L;
        if (ordinal == Long.MAX_VALUE) {
            throw new IllegalStateException(
                    "generic count-window ordinal is exhausted");
        }
        WindowMembership membership = countWindow(contract.binding)
                ? countMembership(contract.binding, ordinal)
                : membership(contract.binding, assignmentTime);
        byte[][] replacements = new byte[membership.starts.length][];
        boolean[] removals = new boolean[membership.starts.length];
        WindowUsage usage = usage(state, key);
        int pendingWindows = 0;
        long pendingBytes = countWindow(contract.binding)
                && countValue == null
                ? key.length + (long) COUNT_NAMESPACE.length + Long.BYTES
                : 0L;
        boolean accepted = false;
        for (int index = 0; index < membership.starts.length; index++) {
            if (eventTimeWindow(contract.binding)
                    && expired(membership.ends[index],
                    contract.binding.allowedLatenessMillis(), watermark)) {
                continue;
            }
            accepted = true;
            byte[] namespace = namespace(
                    membership.starts[index], membership.ends[index]);
            byte[] encoded = state.get(key, namespace);
            long rows = encoded == null ? 0L : rowCount(encoded);
            boolean fired = encoded != null && fired(encoded)
                    && eventTimeWindow(contract.binding);
            long nextTrigger = processingTimeWindow(contract.binding)
                    ? encoded == null
                    ? nextProcessingTrigger(contract.binding,
                    membership.starts[index], assignmentTime)
                    : nextTrigger(encoded)
                    : Long.MAX_VALUE;
            if (add && rows >= contract.resources.maximumRowsPerWindow()) {
                throw new IllegalStateException(
                        "generic window row quota exceeded");
            }
            if (!add && encoded == null) {
                throw new IllegalArgumentException(
                        "RETRACT window removed an absent row");
            }
            byte[] accumulator = encoded == null
                    ? requireAccumulator(
                    aggregateFunction.createAccumulator(), contract)
                    : accumulator(encoded);
            byte[] replacement = requireAccumulator(
                    add ? aggregateFunction.add(
                    row.copy(), accumulator.clone())
                    : ((RetractableStreamWindowAggregateFunction)
                    aggregateFunction).retract(
                    row.copy(), accumulator.clone()),
                    contract);
            long replacementRows = add
                    ? Math.addExact(rows, 1L)
                    : Math.subtractExact(rows, 1L);
            boolean empty = !add
                    && ((RetractableStreamWindowAggregateFunction)
                    aggregateFunction).isEmpty(replacement.clone());
            if (empty != (replacementRows == 0L)) {
                throw new IllegalArgumentException(
                        "RETRACT window accumulator emptiness differs from its row count");
            }
            if (empty) {
                removals[index] = true;
            } else {
                replacements[index] = windowValue(
                        replacementRows, fired, nextTrigger,
                        replacement, contract);
            }
            if (removals[index]) {
                pendingWindows = Math.subtractExact(pendingWindows, 1);
                pendingBytes = Math.subtractExact(pendingBytes,
                        key.length + WINDOW_NAMESPACE_BYTES
                                + (long) encoded.length);
            } else if (encoded == null) {
                pendingWindows = Math.addExact(pendingWindows, 1);
                pendingBytes = Math.addExact(pendingBytes,
                        key.length + WINDOW_NAMESPACE_BYTES
                                + (long) replacements[index].length);
            } else {
                pendingBytes = Math.addExact(pendingBytes,
                        replacements[index].length - (long) encoded.length);
            }
            if (Math.addExact(usage.keyWindows, pendingWindows)
                    > contract.resources.maximumActiveWindowsPerKey()
                    || Math.addExact(usage.totalBytes, pendingBytes)
                    > contract.resources.maximumTotalBytes()) {
                throw new IllegalStateException(
                        "generic window active state quota exceeded");
            }
            observeActiveWindows(Math.addExact(
                    usage.totalWindows, pendingWindows));
        }
        if (!accepted) {
            emitLate(contract, row, envelope);
            return;
        }
        try {
            for (int index = 0; index < membership.starts.length; index++) {
                byte[] namespace = namespace(membership.starts[index],
                        membership.ends[index]);
                if (removals[index]) {
                    state.remove(key, namespace);
                } else if (replacements[index] != null) {
                    state.put(key, namespace, replacements[index],
                            envelope.hasEventTimestamp()
                                    ? envelope.timestamp()
                                    : Long.MIN_VALUE);
                }
            }
            if (countWindow(contract.binding) && add) {
                state.put(key, COUNT_NAMESPACE,
                        ByteBuffer.allocate(Long.BYTES)
                                .putLong(Math.addExact(ordinal, 1L)).array(),
                        Long.MIN_VALUE);
            }
        } catch (RuntimeException mutationFailure) {
            throw new IllegalStateException(
                    "generic window state mutation failed",
                    mutationFailure);
        }
        for (int index = 0; index < membership.starts.length; index++) {
            if (replacements[index] != null
                    && eventTimeWindow(contract.binding)
                    && membership.ends[index] <= watermark) {
                fireWindow(contract, state, key,
                        namespace(membership.starts[index],
                                membership.ends[index]),
                        membership.starts[index], membership.ends[index]);
            } else if (replacements[index] != null
                    && countWindow(contract.binding)) {
                long rows = rowCount(replacements[index]);
                boolean finalWindow = ordinal == Long.MAX_VALUE
                        || ordinal + 1L >= membership.ends[index];
                if (rows % contract.binding.triggerIntervalOrCount() == 0L
                        || finalWindow) {
                    fireCountWindow(contract, state, key,
                            namespace(membership.starts[index],
                                    membership.ends[index]),
                            membership.starts[index], membership.ends[index],
                            finalWindow);
                }
            }
        }
    }

    private void processFullWindow(WindowContract contract,
            ManagedKeyedValueState state, StreamEnvelope envelope,
            DataRow row, byte[] key, long assignmentTime) {
        byte[] countValue = countWindow(contract.binding)
                ? state.get(key, COUNT_NAMESPACE) : null;
        long ordinal = countWindow(contract.binding)
                ? countOrdinal(countValue) : -1L;
        if (ordinal == Long.MAX_VALUE) {
            throw new IllegalStateException(
                    "generic count-window ordinal is exhausted");
        }
        WindowMembership membership = countWindow(contract.binding)
                ? countMembership(contract.binding, ordinal)
                : membership(contract.binding, assignmentTime);
        byte[] rowFrame = contract.inputSerializer.serialize(row);
        byte[][] replacements = new byte[membership.starts.length][];
        long[] rowOrdinals = new long[membership.starts.length];
        WindowUsage usage = usage(state, key);
        int pendingWindows = 0;
        long pendingBytes = countWindow(contract.binding)
                && countValue == null
                ? key.length + (long) COUNT_NAMESPACE.length + Long.BYTES
                : 0L;
        boolean accepted = false;
        for (int index = 0; index < membership.starts.length; index++) {
            long start = membership.starts[index];
            long end = membership.ends[index];
            if (eventTimeWindow(contract.binding)
                    && expired(end,
                    contract.binding.allowedLatenessMillis(), watermark)) {
                continue;
            }
            accepted = true;
            byte[] namespace = namespace(start, end);
            byte[] encoded = state.get(key, namespace);
            long rows = encoded == null ? 0L : rowCount(encoded);
            if (rows >= contract.resources.maximumRowsPerWindow()) {
                throw new IllegalStateException(
                        "generic process-window row quota exceeded");
            }
            long trigger = processingTimeWindow(contract.binding)
                    ? encoded == null
                    ? nextProcessingTrigger(
                    contract.binding, start, end, assignmentTime)
                    : nextTrigger(encoded) : Long.MAX_VALUE;
            boolean fired = encoded != null && fired(encoded)
                    && eventTimeWindow(contract.binding);
            replacements[index] = windowValue(
                    Math.addExact(rows, 1L), fired, trigger,
                    new byte[0], contract);
            rowOrdinals[index] = rows;
            long windowBytes = processWindowBytes(
                    state, key, start, end, encoded);
            if (Math.addExact(windowBytes, rowFrame.length)
                    > contract.resources.maximumWindowBytes()) {
                throw new IllegalStateException(
                        "generic process-window byte quota exceeded");
            }
            if (encoded == null) {
                pendingWindows = Math.addExact(pendingWindows, 1);
                pendingBytes = Math.addExact(pendingBytes,
                        key.length + WINDOW_NAMESPACE_BYTES
                                + (long) replacements[index].length);
            } else {
                pendingBytes = Math.addExact(pendingBytes,
                        replacements[index].length - (long) encoded.length);
            }
            pendingBytes = Math.addExact(pendingBytes,
                    key.length + (long) ROW_NAMESPACE_BYTES
                            + rowFrame.length);
            if (Math.addExact(usage.keyWindows, pendingWindows)
                    > contract.resources.maximumActiveWindowsPerKey()
                    || Math.addExact(usage.totalBytes, pendingBytes)
                    > contract.resources.maximumTotalBytes()) {
                throw new IllegalStateException(
                        "generic process-window active state quota exceeded");
            }
            observeActiveWindows(Math.addExact(
                    usage.totalWindows, pendingWindows));
        }
        if (!accepted) {
            emitLate(contract, row, envelope);
            return;
        }
        try {
            for (int index = 0; index < membership.starts.length; index++) {
                if (replacements[index] == null) continue;
                long start = membership.starts[index];
                long end = membership.ends[index];
                state.put(key, rowNamespace(start, end,
                                rowOrdinals[index]), rowFrame,
                        envelope.hasEventTimestamp()
                                ? envelope.timestamp() : Long.MIN_VALUE);
                state.put(key, namespace(start, end), replacements[index],
                        envelope.hasEventTimestamp()
                                ? envelope.timestamp() : Long.MIN_VALUE);
            }
            if (countWindow(contract.binding)) {
                state.put(key, COUNT_NAMESPACE,
                        ByteBuffer.allocate(Long.BYTES)
                                .putLong(Math.addExact(ordinal, 1L)).array(),
                        Long.MIN_VALUE);
            }
        } catch (RuntimeException mutationFailure) {
            throw new IllegalStateException(
                    "generic process-window state mutation failed",
                    mutationFailure);
        }
        for (int index = 0; index < membership.starts.length; index++) {
            if (replacements[index] == null) continue;
            byte[] namespace = namespace(
                    membership.starts[index], membership.ends[index]);
            if (eventTimeWindow(contract.binding)
                    && membership.ends[index] <= watermark) {
                fireWindow(contract, state, key, namespace,
                        membership.starts[index], membership.ends[index]);
            } else if (countWindow(contract.binding)) {
                long rows = rowCount(replacements[index]);
                boolean finalWindow = ordinal + 1L
                        >= membership.ends[index];
                if (rows % contract.binding.triggerIntervalOrCount() == 0L
                        || finalWindow) {
                    fireCountWindow(contract, state, key, namespace,
                            membership.starts[index], membership.ends[index],
                            finalWindow);
                }
            }
        }
    }

    private long processWindowBytes(ManagedKeyedValueState state,
            byte[] key, long start, long end, byte[] metadata) {
        long bytes = metadata == null ? 0L : metadata.length;
        long rows = metadata == null ? 0L : rowCount(metadata);
        for (long ordinal = 0L; ordinal < rows; ordinal++) {
            byte[] row = state.get(key, rowNamespace(start, end, ordinal));
            if (row == null) {
                throw new IllegalStateException(
                        "generic process-window row state is incomplete");
            }
            bytes = Math.addExact(bytes, row.length);
        }
        return bytes;
    }

    private void processFullSession(WindowContract contract,
            ManagedKeyedValueState state, StreamEnvelope envelope,
            DataRow row, byte[] key, long assignmentTime) {
        SessionMerge merge = sessionMerge(contract, state, key,
                assignmentTime);
        if (eventTimeWindow(contract.binding)
                && expired(merge.end,
                contract.binding.allowedLatenessMillis(), watermark)) {
            emitLate(contract, row, envelope);
            return;
        }
        byte[] newRow = contract.inputSerializer.serialize(row);
        long rows = 1L;
        long rowBytes = newRow.length;
        long removedBytes = 0L;
        for (int index = 0; index < merge.count; index++) {
            byte[] metadata = merge.values[index];
            long oldRows = rowCount(metadata);
            rows = Math.addExact(rows, oldRows);
            removedBytes = Math.addExact(removedBytes,
                    key.length + (long) WINDOW_NAMESPACE_BYTES
                            + metadata.length);
            ByteBuffer interval = ByteBuffer.wrap(merge.namespaces[index]);
            long oldStart = interval.getLong();
            long oldEnd = interval.getLong();
            for (long ordinal = 0L; ordinal < oldRows; ordinal++) {
                byte[] frame = state.get(key,
                        rowNamespace(oldStart, oldEnd, ordinal));
                if (frame == null) {
                    throw new IllegalStateException(
                            "generic process-session row state is incomplete");
                }
                rowBytes = Math.addExact(rowBytes, frame.length);
                removedBytes = Math.addExact(removedBytes,
                        key.length + (long) ROW_NAMESPACE_BYTES
                                + frame.length);
            }
        }
        if (rows > contract.resources.maximumRowsPerWindow()
                || Math.addExact(WINDOW_HEADER_BYTES, rowBytes)
                > contract.resources.maximumWindowBytes()) {
            throw new IllegalStateException(
                    "generic process-session window quota exceeded");
        }
        long trigger = processingTimeWindow(contract.binding)
                ? nextProcessingTrigger(contract.binding,
                merge.start, merge.end, assignmentTime) : Long.MAX_VALUE;
        byte[] replacement = windowValue(
                rows, false, trigger, new byte[0], contract);
        long replacementBytes = key.length + (long) WINDOW_NAMESPACE_BYTES
                + replacement.length;
        replacementBytes = Math.addExact(replacementBytes,
                Math.multiplyExact(rows,
                        key.length + (long) ROW_NAMESPACE_BYTES));
        replacementBytes = Math.addExact(replacementBytes, rowBytes);
        WindowUsage usage = usage(state, key);
        long finalBytes = Math.addExact(
                Math.subtractExact(usage.totalBytes, removedBytes),
                replacementBytes);
        if (usage.keyWindows - merge.count + 1
                > contract.resources.maximumActiveWindowsPerKey()
                || finalBytes > contract.resources.maximumTotalBytes()) {
            throw new IllegalStateException(
                    "generic process-session active state quota exceeded");
        }
        observeActiveWindows(Math.addExact(
                Math.subtractExact(usage.totalWindows, merge.count), 1));
        try {
            long targetOrdinal = 0L;
            for (int index = 0; index < merge.count; index++) {
                byte[] metadata = merge.values[index];
                long oldRows = rowCount(metadata);
                ByteBuffer interval = ByteBuffer.wrap(
                        merge.namespaces[index]);
                long oldStart = interval.getLong();
                long oldEnd = interval.getLong();
                boolean sameWindow = oldStart == merge.start
                        && oldEnd == merge.end;
                for (long ordinal = 0L; ordinal < oldRows; ordinal++) {
                    byte[] source = rowNamespace(
                            oldStart, oldEnd, ordinal);
                    byte[] target = rowNamespace(
                            merge.start, merge.end, targetOrdinal++);
                    if (!Arrays.equals(source, target)) {
                        byte[] frame = state.get(key, source);
                        if (frame == null) {
                            throw new IllegalStateException(
                                    "generic process-session row state is incomplete");
                        }
                        state.put(key, target, frame,
                                envelope.hasEventTimestamp()
                                        ? envelope.timestamp()
                                        : Long.MIN_VALUE);
                    }
                }
                if (!sameWindow) {
                    for (long ordinal = 0L; ordinal < oldRows; ordinal++) {
                        state.remove(key, rowNamespace(
                                oldStart, oldEnd, ordinal));
                    }
                    state.remove(key, merge.namespaces[index]);
                }
            }
            state.put(key, rowNamespace(
                            merge.start, merge.end, targetOrdinal), newRow,
                    envelope.hasEventTimestamp()
                            ? envelope.timestamp() : Long.MIN_VALUE);
            byte[] namespace = namespace(merge.start, merge.end);
            state.put(key, namespace, replacement,
                    envelope.hasEventTimestamp()
                            ? envelope.timestamp() : Long.MIN_VALUE);
            if (eventTimeWindow(contract.binding)
                    && merge.end <= watermark) {
                fireWindow(contract, state, key, namespace,
                        merge.start, merge.end);
            }
        } catch (RuntimeException mutationFailure) {
            throw new IllegalStateException(
                    "generic process-session state mutation failed",
                    mutationFailure);
        }
    }

    private void processSession(WindowContract contract,
            ManagedKeyedValueState state, StreamEnvelope envelope,
            DataRow row, byte[] key, long assignmentTime) {
        SessionMerge merge = sessionMerge(contract, state, key,
                assignmentTime);
        if (eventTimeWindow(contract.binding)
                && expired(merge.end,
                contract.binding.allowedLatenessMillis(), watermark)) {
            emitLate(contract, row, envelope);
            return;
        }
        long rows = 1L;
        byte[] accumulator = merge.count == 0
                ? requireAccumulator(
                aggregateFunction.createAccumulator(), contract)
                : accumulator(merge.values[0]);
        long removedBytes = 0L;
        for (int index = 0; index < merge.count; index++) {
            byte[] encoded = merge.values[index];
            rows = Math.addExact(rows, rowCount(encoded));
            if (index > 0) {
                accumulator = requireAccumulator(aggregateFunction.merge(
                        accumulator.clone(), accumulator(encoded)), contract);
            }
            removedBytes = Math.addExact(removedBytes,
                    key.length + (long) merge.namespaces[index].length
                            + encoded.length);
        }
        if (rows > contract.resources.maximumRowsPerWindow()) {
            throw new IllegalStateException(
                    "generic session-window row quota exceeded");
        }
        accumulator = requireAccumulator(aggregateFunction.add(
                row.copy(), accumulator.clone()), contract);
        long trigger = processingTimeWindow(contract.binding)
                ? nextProcessingTrigger(contract.binding,
                merge.start, merge.end, assignmentTime) : Long.MAX_VALUE;
        byte[] replacement = windowValue(
                rows, false, trigger, accumulator, contract);
        WindowUsage usage = usage(state, key);
        long replacementBytes = key.length + (long) WINDOW_NAMESPACE_BYTES
                + replacement.length;
        if (usage.keyWindows - merge.count + 1
                > contract.resources.maximumActiveWindowsPerKey()
                || Math.subtractExact(usage.totalBytes, removedBytes)
                + replacementBytes > contract.resources.maximumTotalBytes()) {
            throw new IllegalStateException(
                    "generic session-window active state quota exceeded");
        }
        observeActiveWindows(Math.addExact(
                Math.subtractExact(usage.totalWindows, merge.count), 1));
        try {
            for (int index = 0; index < merge.count; index++) {
                removeWindow(state, key, merge.namespaces[index]);
            }
            byte[] namespace = namespace(merge.start, merge.end);
            state.put(key, namespace, replacement,
                    envelope.hasEventTimestamp()
                            ? envelope.timestamp() : Long.MIN_VALUE);
            if (eventTimeWindow(contract.binding)
                    && merge.end <= watermark) {
                fireWindow(contract, state, key, namespace,
                        merge.start, merge.end);
            }
        } catch (RuntimeException mutationFailure) {
            throw new IllegalStateException(
                    "generic session-window state mutation failed",
                    mutationFailure);
        }
    }

    private SessionMerge sessionMerge(WindowContract contract,
            ManagedKeyedValueState state, byte[] key, long timestamp) {
        long start = timestamp;
        long end;
        try {
            end = Math.addExact(timestamp, contract.binding.sizeOrGap());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "generic session-window interval overflowed", overflow);
        }
        int limit = contract.resources.maximumSessionMergeFanIn();
        byte[][] namespaces = new byte[limit][];
        byte[][] values = new byte[limit][];
        int count = 0;
        while (true) {
            SessionCandidate selected = null;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         state.entries(key)) {
                while (cursor.advance()) {
                    byte[] candidateNamespace = cursor.namespace();
                    if (candidateNamespace.length != WINDOW_NAMESPACE_BYTES
                            || contains(namespaces, count,
                            candidateNamespace)) {
                        continue;
                    }
                    ByteBuffer interval = ByteBuffer.wrap(candidateNamespace);
                    long candidateStart = interval.getLong();
                    long candidateEnd = interval.getLong();
                    if (candidateEnd <= candidateStart
                            || candidateStart > end
                            || candidateEnd < start) {
                        continue;
                    }
                    SessionCandidate candidate = new SessionCandidate(
                            candidateNamespace, cursor.value(),
                            candidateStart, candidateEnd);
                    if (selected == null
                            || candidate.compareTo(selected) < 0) {
                        selected = candidate;
                    }
                }
            }
            if (selected == null) break;
            if (count >= limit) {
                throw new IllegalStateException(
                        "generic session-window merge fan-in exceeded");
            }
            namespaces[count] = selected.namespace;
            values[count] = selected.value;
            count++;
            start = Math.min(start, selected.start);
            end = Math.max(end, selected.end);
        }
        return new SessionMerge(start, end, namespaces, values, count);
    }

    private static boolean contains(byte[][] values, int count,
            byte[] selected) {
        for (int index = 0; index < count; index++) {
            if (Arrays.equals(values[index], selected)) return true;
        }
        return false;
    }

    private long countOrdinal(byte[] encoded) {
        if (encoded == null) return 0L;
        if (encoded.length != Long.BYTES) {
            throw new IllegalStateException(
                    "generic count-window ordinal is corrupt");
        }
        long ordinal = ByteBuffer.wrap(encoded).getLong();
        if (ordinal < 0L) {
            throw new IllegalStateException(
                    "generic count-window ordinal is corrupt");
        }
        return ordinal;
    }

    private void fireCountWindow(WindowContract contract,
            ManagedKeyedValueState state, byte[] key, byte[] namespace,
            long start, long end, boolean finalWindow) {
        byte[] encoded = state.get(key, namespace);
        if (encoded == null) return;
        emitWindow(contract, state, new DueWindow(
                key, namespace, start, end), encoded);
        if (finalWindow || contract.binding.purging()
                || contract.binding.accumulationMode()
                == StreamWindowBinding.AccumulationMode.DISCARDING) {
            removeWindow(state, key, namespace);
        } else {
            state.put(key, namespace, windowValue(rowCount(encoded), true,
                    accumulator(encoded), contract), Long.MIN_VALUE);
        }
    }

    private void flushCountWindows(WindowContract contract,
            ManagedKeyedValueState state) {
        while (true) {
            DueWindow due = nextDueWindow(state, Long.MAX_VALUE);
            if (due == null) break;
            fireCountWindow(contract, state, due.key, due.namespace,
                    due.start, due.end, true);
        }
    }

    private void fireDueProcessingWindows(WindowContract contract,
            ManagedKeyedValueState state, long now) {
        while (true) {
            ProcessingDueWindow due = nextProcessingDueWindow(state, now);
            if (due == null) return;
            byte[] encoded = state.get(due.window.key,
                    due.window.namespace);
            if (encoded == null) continue;
            if (!fired(encoded)) {
                emitWindow(contract, state, due.window, encoded);
            }
            if (due.window.end <= now || contract.binding.purging()
                    || contract.binding.accumulationMode()
                    == StreamWindowBinding.AccumulationMode.DISCARDING) {
                removeWindow(state, due.window.key, due.window.namespace);
            } else {
                state.put(due.window.key, due.window.namespace,
                        windowValue(rowCount(encoded), true,
                                nextProcessingTrigger(contract.binding,
                                        due.window.start, due.window.end, now),
                                accumulator(encoded), contract),
                        Long.MIN_VALUE);
            }
        }
    }

    private void flushProcessingWindows(WindowContract contract,
            ManagedKeyedValueState state) {
        fireDueProcessingWindows(contract, state, Long.MAX_VALUE);
    }

    private ProcessingDueWindow nextProcessingDueWindow(
            ManagedKeyedValueState state, long now) {
        ProcessingDueWindow selected = null;
        try (ManagedKeyedValueState.EntryCursor cursor = state.entries()) {
            while (cursor.advance()) {
                byte[] namespace = cursor.namespace();
                if (Arrays.equals(namespace, COUNT_NAMESPACE)
                        || rowNamespace(namespace)) continue;
                if (namespace.length != WINDOW_NAMESPACE_BYTES) {
                    throw new IllegalStateException(
                            "generic window namespace is corrupt");
                }
                ByteBuffer interval = ByteBuffer.wrap(namespace);
                long start = interval.getLong();
                long end = interval.getLong();
                long trigger = nextTrigger(cursor.value());
                long dueAt = Math.min(end, trigger);
                if (end <= start || trigger <= start || dueAt > now) continue;
                ProcessingDueWindow candidate = new ProcessingDueWindow(
                        new DueWindow(cursor.canonicalKey(), namespace,
                                start, end), dueAt);
                if (selected == null || candidate.compareTo(selected,
                        context.maximumParallelism()) < 0) {
                    selected = candidate;
                }
            }
        }
        return selected;
    }

    private long processingWindowWaitMillis(
            ManagedKeyedValueState state, long now) {
        long earliest = Long.MAX_VALUE;
        try (ManagedKeyedValueState.EntryCursor cursor = state.entries()) {
            while (cursor.advance()) {
                byte[] namespace = cursor.namespace();
                if (Arrays.equals(namespace, COUNT_NAMESPACE)
                        || rowNamespace(namespace)) continue;
                if (namespace.length != WINDOW_NAMESPACE_BYTES) {
                    throw new IllegalStateException(
                            "generic window namespace is corrupt");
                }
                ByteBuffer interval = ByteBuffer.wrap(namespace);
                long start = interval.getLong();
                long end = interval.getLong();
                long trigger = nextTrigger(cursor.value());
                if (end <= start || trigger <= start) {
                    throw new IllegalStateException(
                            "generic processing window timer is corrupt");
                }
                earliest = Math.min(earliest, Math.min(end, trigger));
            }
        }
        if (earliest == Long.MAX_VALUE) return 1_000L;
        if (earliest <= now) return 1L;
        return Math.max(1L, Math.min(1_000L, earliest - now));
    }

    private WindowUsage usage(ManagedKeyedValueState state,
            byte[] selectedKey) {
        int keyWindows = 0;
        int totalWindows = 0;
        long totalBytes = 0L;
        try (ManagedKeyedValueState.EntryCursor cursor = state.entries()) {
            while (cursor.advance()) {
                byte[] key = cursor.canonicalKey();
                byte[] namespace = cursor.namespace();
                byte[] value = cursor.value();
                if (namespace.length == WINDOW_NAMESPACE_BYTES) {
                    totalWindows = Math.addExact(totalWindows, 1);
                    if (Arrays.equals(key, selectedKey)) keyWindows++;
                }
                totalBytes = Math.addExact(totalBytes,
                        key.length + (long) namespace.length + value.length);
            }
        }
        return new WindowUsage(keyWindows, totalWindows, totalBytes);
    }

    private void observeActiveWindows(int currentWindows) {
        if (currentWindows < 0) {
            throw new IllegalStateException(
                    "generic window count accounting underflowed");
        }
        maximumObservedWindows = Math.max(
                maximumObservedWindows, currentWindows);
        if (!countWindow(contract.binding)) {
            maximumObservedTimers = Math.max(
                    maximumObservedTimers, currentWindows);
        }
    }

    private void advanceWatermark(WindowContract contract,
            ManagedKeyedValueState state, long nextWatermark) {
        if (nextWatermark < watermark) {
            throw new IllegalArgumentException(
                    "generic window watermark regressed");
        }
        watermark = nextWatermark;
        while (true) {
            DueWindow due = nextDueWindow(state, nextWatermark);
            if (due == null) break;
            fireWindow(contract, state, due.key, due.namespace,
                    due.start, due.end);
        }
        while (true) {
            DueWindow cleanup = nextCleanupWindow(
                    contract, state, nextWatermark);
            if (cleanup == null) break;
            removeWindow(state, cleanup.key, cleanup.namespace);
        }
        state.advanceWatermark(nextWatermark);
    }

    private void fireWindow(WindowContract contract,
            ManagedKeyedValueState state, byte[] key, byte[] namespace,
            long start, long end) {
        byte[] encoded = state.get(key, namespace);
        if (encoded == null) return;
        emitWindow(contract, state, new DueWindow(
                key, namespace, start, end), encoded);
        if (contract.binding.accumulationMode()
                == StreamWindowBinding.AccumulationMode.ACCUMULATING
                && contract.binding.allowedLatenessMillis() > 0L
                && !contract.binding.purging()) {
            state.put(key, namespace, windowValue(rowCount(encoded), true,
                    accumulator(encoded), contract), end);
        } else {
            removeWindow(state, key, namespace);
        }
    }

    private DueWindow nextDueWindow(
            ManagedKeyedValueState state, long dueTime) {
        DueWindow selected = null;
        try (ManagedKeyedValueState.EntryCursor cursor = state.entries()) {
            while (cursor.advance()) {
                byte[] namespace = cursor.namespace();
                if (Arrays.equals(namespace, COUNT_NAMESPACE)
                        || rowNamespace(namespace)) continue;
                if (namespace.length != WINDOW_NAMESPACE_BYTES) {
                    throw new IllegalStateException(
                            "generic window namespace is corrupt");
                }
                ByteBuffer interval = ByteBuffer.wrap(namespace);
                long start = interval.getLong();
                long end = interval.getLong();
                if (end <= start || end > dueTime
                        || fired(cursor.value())) continue;
                DueWindow candidate = new DueWindow(
                        cursor.canonicalKey(), namespace, start, end);
                if (selected == null
                        || candidate.compareTo(selected,
                        context.maximumParallelism()) < 0) {
                    selected = candidate;
                }
            }
        }
        return selected;
    }

    private DueWindow nextCleanupWindow(WindowContract contract,
            ManagedKeyedValueState state, long dueTime) {
        DueWindow selected = null;
        try (ManagedKeyedValueState.EntryCursor cursor = state.entries()) {
            while (cursor.advance()) {
                byte[] namespace = cursor.namespace();
                if (Arrays.equals(namespace, COUNT_NAMESPACE)
                        || rowNamespace(namespace)) continue;
                if (namespace.length != WINDOW_NAMESPACE_BYTES) {
                    throw new IllegalStateException(
                            "generic window namespace is corrupt");
                }
                ByteBuffer interval = ByteBuffer.wrap(namespace);
                long start = interval.getLong();
                long end = interval.getLong();
                if (end <= start || !fired(cursor.value())
                        || !expired(end,
                        contract.binding.allowedLatenessMillis(), dueTime)) {
                    continue;
                }
                DueWindow candidate = new DueWindow(
                        cursor.canonicalKey(), namespace, start, end);
                if (selected == null || candidate.compareTo(selected,
                        context.maximumParallelism()) < 0) {
                    selected = candidate;
                }
            }
        }
        return selected;
    }

    private void emitLate(WindowContract contract, DataRow row,
            StreamEnvelope envelope) {
        io.rowweave.stream.contract.StreamOutputTagDescriptor late =
                contract.binding.lateOutputTag();
        if (late == null) {
            throw new IllegalStateException(
                    "record is late and no late output is declared");
        }
        byte[] frame = contract.inputSerializer.serialize(row);
        if (frame.length > late.maximumLeftOutputBytes()) {
            throw new IllegalStateException(
                    "late window output quota exceeded");
        }
        if (late.changelogMode()
                == io.rowweave.stream.contract.StreamOutputTagDescriptor
                .ChangelogMode.UPSERT) {
            byte[] outputKey = contract.lateOutputKeySerializer
                    .serializeProjected(row,
                            contract.lateOutputKeyIndices);
            frame = StreamUpsertRecordCodec.encode(
                    StreamUpsertRecord.afterImage(outputKey, frame));
        }
        boolean routed = false;
        for (Route route : routes) {
            if (late.tagId().equals(route.outputTagId)) {
                route.sendRecord(frame, envelope.timestamp());
                routed = true;
            }
        }
        if (!routed) {
            throw new IllegalStateException(
                    "declared late window output is not consumed");
        }
    }

    private void emitAggregate(WindowContract contract, DueWindow due,
            byte[] accumulator) {
        DataRow key = contract.inputKeySerializer.deserialize(due.key);
        WindowContext context = new WindowContext(
                key, due.start, due.end);
        DataRow result;
        try {
            result = aggregateFunction.getResult(
                    accumulator.clone(), context);
        } finally {
            context.close();
        }
        emitResult(contract, due, result, new TriggerQuota(contract));
    }

    private void emitWindow(WindowContract contract,
            ManagedKeyedValueState state, DueWindow due, byte[] metadata) {
        if (aggregateFunction != null) {
            emitAggregate(contract, due, accumulator(metadata));
            return;
        }
        WindowContext context = new WindowContext(
                contract.inputKeySerializer.deserialize(due.key),
                due.start, due.end);
        ProcessRowCursor cursor = new ProcessRowCursor(
                state, contract, due, rowCount(metadata));
        WindowCollector collector = new WindowCollector(
                contract, due, new TriggerQuota(contract));
        try {
            processFunction.process(context, cursor, collector);
        } finally {
            collector.close();
            cursor.close();
            context.close();
        }
    }

    private void emitResult(WindowContract contract, DueWindow due,
            DataRow result, TriggerQuota quota) {
        validate(result, contract.outputSchema,
                "generic window result row");
        byte[] frame = contract.outputSerializer.serialize(result);
        if (context.changelogMode()
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.RETRACT) {
            frame = StreamChangeRecordCodec.encode(
                    StreamChangeRecord.insert(frame));
        } else if (context.changelogMode()
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.UPSERT) {
            byte[] outputKey = contract.outputKeySerializer
                    .serializeProjected(result, contract.outputKeyIndices);
            frame = StreamUpsertRecordCodec.encode(
                    StreamUpsertRecord.afterImage(outputKey, frame));
        }
        quota.accept(frame.length);
        for (Route route : routes) {
            if (route.outputTagId == null) route.sendRecord(frame);
        }
    }

    private void removeWindow(ManagedKeyedValueState state,
            byte[] key, byte[] namespace) {
        byte[] metadata = state.get(key, namespace);
        if (metadata == null) return;
        if (processFunction != null) {
            ByteBuffer interval = ByteBuffer.wrap(namespace);
            long start = interval.getLong();
            long end = interval.getLong();
            long rows = rowCount(metadata);
            for (long ordinal = 0L; ordinal < rows; ordinal++) {
                state.remove(key, rowNamespace(start, end, ordinal));
            }
        }
        state.remove(key, namespace);
    }

    private byte[] requireAccumulator(byte[] value,
            WindowContract contract) {
        if (value == null || value.length == 0
                || value.length + WINDOW_HEADER_BYTES
                > contract.resources.maximumWindowBytes()
                || value.length + WINDOW_HEADER_BYTES
                > context.resources().maximumFrameBytes()) {
            throw new IllegalArgumentException(
                    "generic window accumulator exceeds its declared bound");
        }
        return value.clone();
    }

    private static byte[] windowValue(long rows, boolean fired,
            byte[] accumulator,
            WindowContract contract) {
        return windowValue(rows, fired, Long.MAX_VALUE,
                accumulator, contract);
    }

    private static byte[] windowValue(long rows, boolean fired,
            long nextTrigger, byte[] accumulator,
            WindowContract contract) {
        long bytes = WINDOW_HEADER_BYTES + (long) accumulator.length;
        if (rows <= 0L || bytes > contract.resources.maximumWindowBytes()
                || bytes > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "generic window value exceeds its declared bound");
        }
        return ByteBuffer.allocate((int) bytes).putLong(
                fired ? -rows : rows)
                .putLong(nextTrigger)
                .put(accumulator).array();
    }

    private static long rowCount(byte[] value) {
        if (value.length < WINDOW_HEADER_BYTES) {
            throw new IllegalStateException(
                    "generic window state value is corrupt");
        }
        long stored = ByteBuffer.wrap(
                value, 0, Long.BYTES).getLong();
        if (stored == 0L || stored == Long.MIN_VALUE) {
            throw new IllegalStateException(
                    "generic window row count is corrupt");
        }
        return Math.abs(stored);
    }

    private static boolean fired(byte[] value) {
        rowCount(value);
        return ByteBuffer.wrap(value, 0, Long.BYTES).getLong() < 0L;
    }

    private static long nextTrigger(byte[] value) {
        rowCount(value);
        return ByteBuffer.wrap(value, Long.BYTES, Long.BYTES).getLong();
    }

    private static byte[] accumulator(byte[] value) {
        rowCount(value);
        return Arrays.copyOfRange(value, WINDOW_HEADER_BYTES, value.length);
    }

    private static WindowMembership membership(
            StreamWindowBinding binding, long timestamp) {
        long slide = tumblingWindow(binding)
                ? binding.sizeOrGap() : binding.slide();
        try {
            long lastStart = Math.multiplyExact(
                    Math.floorDiv(timestamp, slide), slide);
            long countValue = 1L + (binding.sizeOrGap() - 1L) / slide;
            if (countValue > Integer.MAX_VALUE) {
                throw new ArithmeticException("window fan-out overflow");
            }
            int count = (int) countValue;
            long[] starts = new long[count];
            long[] ends = new long[count];
            for (int index = 0; index < count; index++) {
                long start = Math.subtractExact(lastStart,
                        Math.multiplyExact((long) index, slide));
                long end = Math.addExact(start, binding.sizeOrGap());
                if (timestamp < start || timestamp >= end) {
                    throw new ArithmeticException(
                            "timestamp is outside derived window");
                }
                starts[index] = start;
                ends[index] = end;
            }
            return new WindowMembership(starts, ends);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "generic window interval arithmetic overflowed",
                    overflow);
        }
    }

    private static WindowMembership countMembership(
            StreamWindowBinding binding, long ordinal) {
        long slide = binding.assignerKind()
                == StreamWindowBinding.AssignerKind.COUNT_TUMBLING
                ? binding.sizeOrGap() : binding.slide();
        try {
            long lastStart = Math.multiplyExact(
                    Math.floorDiv(ordinal, slide), slide);
            long countValue = 1L + (binding.sizeOrGap() - 1L) / slide;
            if (countValue > Integer.MAX_VALUE) {
                throw new ArithmeticException("count window fan-out overflow");
            }
            long[] starts = new long[(int) countValue];
            long[] ends = new long[(int) countValue];
            int accepted = 0;
            for (int index = 0; index < starts.length; index++) {
                long start = Math.subtractExact(lastStart,
                        Math.multiplyExact((long) index, slide));
                if (start < 0L) continue;
                long end = Math.addExact(start, binding.sizeOrGap());
                if (ordinal >= start && ordinal < end) {
                    starts[accepted] = start;
                    ends[accepted] = end;
                    accepted++;
                }
            }
            return new WindowMembership(Arrays.copyOf(starts, accepted),
                    Arrays.copyOf(ends, accepted));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "generic count-window arithmetic overflowed", overflow);
        }
    }

    private static boolean eventTimeWindow(StreamWindowBinding binding) {
        return binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_SLIDING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_SESSION;
    }

    private static boolean countWindow(StreamWindowBinding binding) {
        return binding.assignerKind()
                == StreamWindowBinding.AssignerKind.COUNT_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.COUNT_SLIDING;
    }

    private static boolean sessionWindow(StreamWindowBinding binding) {
        return binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_SESSION
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.PROCESSING_TIME_SESSION;
    }

    private static boolean processingTimeWindow(StreamWindowBinding binding) {
        return binding.assignerKind()
                == StreamWindowBinding.AssignerKind.PROCESSING_TIME_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.PROCESSING_TIME_SLIDING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.PROCESSING_TIME_SESSION;
    }

    private static boolean tumblingWindow(StreamWindowBinding binding) {
        return binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.PROCESSING_TIME_TUMBLING;
    }

    private static long nextProcessingTrigger(StreamWindowBinding binding,
            long start, long now) {
        long end;
        try {
            end = Math.addExact(start, binding.sizeOrGap());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "generic processing-window timer overflowed", overflow);
        }
        return nextProcessingTrigger(binding, start, end, now);
    }

    private static long nextProcessingTrigger(StreamWindowBinding binding,
            long start, long end, long now) {
        try {
            long interval = binding.triggerIntervalOrCount();
            long elapsed = Math.subtractExact(now, start);
            long ticks = Math.addExact(Math.floorDiv(elapsed, interval), 1L);
            long candidate = Math.addExact(start,
                    Math.multiplyExact(ticks, interval));
            return Math.min(candidate, end);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "generic processing-window timer overflowed", overflow);
        }
    }

    private static byte[] namespace(long start, long end) {
        return ByteBuffer.allocate(WINDOW_NAMESPACE_BYTES)
                .putLong(start).putLong(end).array();
    }

    private static byte[] rowNamespace(long start, long end, long ordinal) {
        if (ordinal < 0L) {
            throw new IllegalArgumentException(
                    "generic process-window row ordinal is invalid");
        }
        return ByteBuffer.allocate(ROW_NAMESPACE_BYTES)
                .put(ROW_NAMESPACE_PREFIX).putLong(start).putLong(end)
                .putLong(ordinal).array();
    }

    private static boolean rowNamespace(byte[] namespace) {
        return namespace.length == ROW_NAMESPACE_BYTES
                && namespace[0] == ROW_NAMESPACE_PREFIX;
    }

    private static boolean expired(
            long end, long allowedLateness, long currentWatermark) {
        long cleanup = end > Long.MAX_VALUE - allowedLateness
                ? Long.MAX_VALUE : end + allowedLateness;
        return currentWatermark >= cleanup;
    }

    private void broadcast(StreamEnvelope envelope) {
        for (Route route : routes) route.sendSignal(envelope);
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
                    "generic window checkpoint runtime or scope is invalid");
        }
        CheckpointIdentity previous = lastCheckpoint;
        if (previous != null && checkpoint.checkpointSequence()
                <= previous.checkpointSequence()) {
            throw new IllegalArgumentException(
                    "generic window checkpoint sequence did not advance");
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
                        "generic window barrier differs from the trigger"));
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

    private void fail(Throwable thrown) {
        failure = thrown;
        input.fail(thrown);
        for (Route route : routes) route.output.fail(thrown);
    }

    private long processingTime() {
        long now = processingClock.getAsLong();
        if (now > processingTimeFloor) processingTimeFloor = now;
        return processingTimeFloor;
    }

    private final class ProcessRowCursor implements StreamWindowDataRowCursor {
        private final ManagedKeyedValueState state;
        private final WindowContract contract;
        private final DueWindow window;
        private final long rows;
        private final Thread callbackThread = Thread.currentThread();
        private long ordinal = -1L;
        private DataRow current;
        private boolean active = true;

        private ProcessRowCursor(ManagedKeyedValueState state,
                WindowContract contract, DueWindow window, long rows) {
            this.state = state;
            this.contract = contract;
            this.window = window;
            this.rows = rows;
        }

        @Override public boolean advance() {
            checkActive();
            if (ordinal + 1L >= rows) {
                ordinal = rows;
                current = null;
                return false;
            }
            ordinal++;
            byte[] frame = state.get(window.key, rowNamespace(
                    window.start, window.end, ordinal));
            if (frame == null) {
                throw new IllegalStateException(
                        "generic process-window row state is incomplete");
            }
            current = contract.inputSerializer.deserialize(frame);
            validate(current, contract.inputSchema,
                    "generic process-window cursor row");
            return true;
        }

        @Override public DataRow row() {
            checkActive();
            if (current == null) {
                throw new IllegalStateException(
                        "generic process-window cursor has no current row");
            }
            return current.copy();
        }

        @Override public void close() {
            active = false;
            current = null;
        }

        private void checkActive() {
            if (!active || Thread.currentThread() != callbackThread) {
                throw new IllegalStateException(
                        "generic process-window cursor escaped its callback");
            }
        }
    }

    private final class WindowCollector implements StreamWindowCollector {
        private final WindowContract contract;
        private final DueWindow window;
        private final TriggerQuota quota;
        private final Thread callbackThread = Thread.currentThread();
        private boolean active = true;

        private WindowCollector(WindowContract contract, DueWindow window,
                TriggerQuota quota) {
            this.contract = contract;
            this.window = window;
            this.quota = quota;
        }

        @Override public void collect(DataRow row) {
            checkActive();
            emitResult(contract, window, row, quota);
        }

        private void close() { active = false; }

        private void checkActive() {
            if (!active || Thread.currentThread() != callbackThread) {
                throw new IllegalStateException(
                        "generic window collector escaped its callback");
            }
        }
    }

    private static final class TriggerQuota {
        private final int maximumOutputs;
        private final long maximumBytes;
        private int outputs;
        private long bytes;

        private TriggerQuota(WindowContract contract) {
            this.maximumOutputs = contract.resources.maximumTriggerOutputs();
            this.maximumBytes =
                    contract.resources.maximumTriggerOutputBytes();
        }

        private void accept(int frameBytes) {
            outputs = Math.addExact(outputs, 1);
            bytes = Math.addExact(bytes, frameBytes);
            if (outputs > maximumOutputs || bytes > maximumBytes) {
                throw new IllegalStateException(
                        "generic window trigger output quota exceeded");
            }
        }
    }

    private final class WindowContext implements StreamWindowContext {
        private final DataRow key;
        private final long start;
        private final long end;
        private final Thread callbackThread = Thread.currentThread();
        private final long callbackProcessingTime = processingTime();
        private boolean active = true;

        private WindowContext(DataRow key, long start, long end) {
            this.key = key.copy();
            this.start = start;
            this.end = end;
        }

        @Override public DataRow currentKey() {
            checkActive();
            return key.copy();
        }

        @Override public long windowStart() {
            checkActive();
            return start;
        }

        @Override public long windowEnd() {
            checkActive();
            return end;
        }

        @Override public long currentWatermark() {
            checkActive();
            return watermark;
        }

        @Override public long currentProcessingTime() {
            checkActive();
            return callbackProcessingTime;
        }

        private void close() { active = false; }

        private void checkActive() {
            if (!active || Thread.currentThread() != callbackThread) {
                throw new IllegalStateException(
                        "generic window context escaped its callback");
            }
        }
    }

    private static final class WindowContract {
        final PortableStreamNode window;
        final StreamWindowBinding binding;
        final StreamWindowResourceDescriptor resources;
        final PortableRowSchema inputSchema;
        final PortableRowSchema outputSchema;
        final int[] inputKeyIndices;
        final int[] outputKeyIndices;
        final int eventTimeIndex;
        final RowSerializer inputSerializer;
        final RowSerializer inputKeySerializer;
        final RowSerializer outputSerializer;
        final RowSerializer outputKeySerializer;
        final int[] lateOutputKeyIndices;
        final RowSerializer lateOutputKeySerializer;
        final boolean retractInput;

        private WindowContract(StreamNodeTaskContext context) {
            PortableStreamNode window = context.node();
            this.window = window;
            this.binding = (StreamWindowBinding) window.binding();
            this.resources = binding.resources();
            StreamNodeTaskContext.Source input = context.inputs().get(0);
            this.inputSchema = input.schema();
            this.retractInput = input.changelogMode()
                    == io.rowweave.stream.contract.PortableStreamPlan
                    .ChangelogMode.RETRACT;
            this.outputSchema = window.outputSchema();
            this.inputKeyIndices = indices(
                    inputSchema, input.stableKeyFields());
            this.outputKeyIndices = indices(
                    outputSchema, window.stableKeyFields());
            this.eventTimeIndex = binding.eventTimeFieldId() == null ? -1
                    : index(inputSchema, binding.eventTimeFieldId());
            this.inputSerializer = serializer(inputSchema);
            this.inputKeySerializer = serializer(keySchema(
                    inputSchema, inputKeyIndices));
            this.outputSerializer = serializer(outputSchema);
            this.outputKeySerializer = serializer(keySchema(
                    outputSchema, outputKeyIndices));
            io.rowweave.stream.contract.StreamOutputTagDescriptor late =
                    binding.lateOutputTag();
            if (late != null && !late.schema().equals(inputSchema)) {
                throw new IllegalArgumentException(
                        "late window output schema must equal its input");
            }
            this.lateOutputKeyIndices = late == null ? new int[0]
                    : indices(inputSchema, late.stableKeyFields());
            this.lateOutputKeySerializer = late == null
                    || late.changelogMode()
                    != io.rowweave.stream.contract
                    .StreamOutputTagDescriptor.ChangelogMode.UPSERT ? null
                    : serializer(keySchema(
                    inputSchema, lateOutputKeyIndices));
        }

        static WindowContract from(StreamNodeTaskContext context) {
            PortableStreamNode window = context == null
                    ? null : context.node();
            if (window == null || window.opcode()
                    != StreamOperatorCode.GENERIC_WINDOW
                    || context.inputs().size() != 1
                    || context.inputs().get(0).stableKeyFields().isEmpty()) {
                throw new UnsupportedOperationException(
                        "generic window node contract is invalid");
            }
            return new WindowContract(context);
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
                    "generic window field binding is missing");
        }
    }

    private static void validate(DataRow row, PortableRowSchema schema,
            String subject) {
        if (row == null || row.size() != schema.fieldCount()) {
            throw new IllegalArgumentException(
                    subject + " width differs from its schema");
        }
        for (int index = 0; index < schema.fieldCount(); index++) {
            if (!schema.field(index).nullable() && row.get(index) == null) {
                throw new IllegalArgumentException(
                        subject + " contains null in a non-null field");
            }
        }
    }

    private static int compare(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            int compared = Integer.compare(
                    left[index] & 0xff, right[index] & 0xff);
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length, right.length);
    }

    private static final class WindowMembership {
        final long[] starts;
        final long[] ends;

        private WindowMembership(long[] starts, long[] ends) {
            this.starts = starts;
            this.ends = ends;
        }
    }

    private static final class WindowUsage {
        final int keyWindows;
        final int totalWindows;
        final long totalBytes;

        private WindowUsage(int keyWindows, int totalWindows,
                long totalBytes) {
            this.keyWindows = keyWindows;
            this.totalWindows = totalWindows;
            this.totalBytes = totalBytes;
        }
    }

    private static final class SessionMerge {
        final long start;
        final long end;
        final byte[][] namespaces;
        final byte[][] values;
        final int count;

        private SessionMerge(long start, long end, byte[][] namespaces,
                byte[][] values, int count) {
            this.start = start;
            this.end = end;
            this.namespaces = namespaces;
            this.values = values;
            this.count = count;
        }
    }

    private static final class SessionCandidate {
        final byte[] namespace;
        final byte[] value;
        final long start;
        final long end;

        private SessionCandidate(byte[] namespace, byte[] value,
                long start, long end) {
            this.namespace = namespace;
            this.value = value;
            this.start = start;
            this.end = end;
        }

        int compareTo(SessionCandidate other) {
            int compared = Long.compare(start, other.start);
            if (compared != 0) return compared;
            return Long.compare(end, other.end);
        }
    }

    private static final class DueWindow {
        final byte[] key;
        final byte[] namespace;
        final long start;
        final long end;

        private DueWindow(byte[] key, byte[] namespace,
                long start, long end) {
            this.key = key;
            this.namespace = namespace;
            this.start = start;
            this.end = end;
        }

        int compareTo(DueWindow other, int maxParallelism) {
            int compared = Long.compare(end, other.end);
            if (compared != 0) return compared;
            compared = Integer.compare(
                    ManagedKeyedValueState.keyGroup(key, maxParallelism),
                    ManagedKeyedValueState.keyGroup(
                            other.key, maxParallelism));
            if (compared != 0) return compared;
            compared = compare(key, other.key);
            if (compared != 0) return compared;
            return Long.compare(start, other.start);
        }
    }

    private static final class ProcessingDueWindow {
        final DueWindow window;
        final long dueAt;

        private ProcessingDueWindow(DueWindow window, long dueAt) {
            this.window = window;
            this.dueAt = dueAt;
        }

        int compareTo(ProcessingDueWindow other, int maxParallelism) {
            int compared = Long.compare(dueAt, other.dueAt);
            return compared != 0 ? compared
                    : window.compareTo(other.window, maxParallelism);
        }
    }

    private static final class Route {
        final StreamChannelOutput output;
        final String outputTagId;
        long sequence;

        private Route(StreamChannelOutput output, String outputTagId) {
            this.output = output;
            this.outputTagId = outputTagId;
        }

        void sendRecord(byte[] frame) {
            output.send(StreamEnvelope.record(output.channelIdentity(),
                    next(), frame));
        }

        void sendRecord(byte[] frame, long timestamp) {
            output.send(StreamEnvelope.record(output.channelIdentity(),
                    next(), timestamp, frame));
        }

        void sendSignal(StreamEnvelope source) {
            long next = next();
            switch (source.kind()) {
                case WATERMARK:
                    output.send(StreamEnvelope.watermark(
                            output.channelIdentity(), next,
                            source.timestamp()));
                    break;
                case WATERMARK_STATUS:
                    output.send(StreamEnvelope.watermarkStatus(
                            output.channelIdentity(), next,
                            source.watermarkStatus(),
                            source.watermarkStatusGeneration()));
                    break;
                case END_OF_INPUT:
                    output.send(StreamEnvelope.endOfInput(
                            output.channelIdentity(), next));
                    break;
                case CHECKPOINT_BARRIER:
                    output.send(StreamEnvelope.checkpointBarrier(
                            output.channelIdentity(), next,
                            source.checkpointIdentity()));
                    break;
                case ASYNC_COMPLETION:
                    output.send(StreamEnvelope.asyncCompletion(
                            output.channelIdentity(), next,
                            source.operationIdentity()));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "unsupported generic window signal");
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

        void reject(Throwable cause) {
            result.completeExceptionally(cause);
        }
    }
}
