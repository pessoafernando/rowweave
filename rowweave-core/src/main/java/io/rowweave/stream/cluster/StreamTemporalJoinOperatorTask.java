package io.rowweave.stream.cluster;

import io.rowweave.CoGroupFunction;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamFunctionBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.BoundedEventTimeTemporalJoin;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamTimeService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Single mailbox owner for a finite two-source event-time temporal join. */
final class StreamTemporalJoinOperatorTask implements StreamOperatorTask {
    private static final long INPUT_POLL_MILLIS = 10L;
    private static final long INDEX_ENTRY_OVERHEAD_BYTES = 256L;

    private final StreamNodeTaskContext context;
    private final StreamChannelInput leftInput;
    private final StreamChannelInput rightInput;
    private final List<Route> routes;
    private final CoGroupFunction function;
    private final FileCheckpointPayloadStore checkpointPayloadStore;
    private final List<StreamCheckpointMember> restoreMembers;
    private final int subtaskIndex;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();
    private volatile Throwable failure;
    private boolean pollLeftFirst = true;
    private long outputWatermark = Long.MIN_VALUE;
    private volatile CheckpointIdentity lastCheckpoint;
    private volatile List<StreamCheckpointMember> lastCheckpointMembers;
    private volatile List<StreamCheckpointMember> terminalMembers;
    private volatile boolean leftEnded;
    private volatile boolean rightEnded;
    private volatile boolean completed;
    private volatile long maximumObservedStateBytes;
    private volatile boolean resourceUsageReady;
    private boolean leftBarrier;
    private boolean rightBarrier;
    private CheckpointIdentity preBarrierCheckpoint;
    private boolean preLeftBarrier;
    private boolean preRightBarrier;

    StreamTemporalJoinOperatorTask(String taskName,
            StreamNodeTaskContext context,
            StreamChannelInput leftInput,
            StreamChannelInput rightInput,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore) {
        this(taskName, context, leftInput, rightInput, outputs, resolver,
                checkpointPayloadStore, Collections.emptyList());
    }

    StreamTemporalJoinOperatorTask(String taskName,
            StreamNodeTaskContext context,
            StreamChannelInput leftInput,
            StreamChannelInput rightInput,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers) {
        this(taskName, context, leftInput, rightInput, outputs, resolver,
                checkpointPayloadStore, restoreMembers, 0, 0,
                context == null ? 0 : context.maximumParallelism() - 1);
    }

    StreamTemporalJoinOperatorTask(String taskName,
            StreamNodeTaskContext context,
            StreamChannelInput leftInput,
            StreamChannelInput rightInput,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        if (taskName == null || taskName.isEmpty() || context == null
                || leftInput == null || rightInput == null
                || outputs == null || resolver == null
                || outputs.isEmpty()
                || context.node().opcode()
                != StreamOperatorCode.TEMPORAL_JOIN
                || context.inputs().size() != 2
                || restoreMembers == null
                || subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= context.maximumParallelism()
                || (!restoreMembers.isEmpty()
                && (restoreMembers.size() != 2
                || checkpointPayloadStore == null))) {
            throw new IllegalArgumentException(
                    "Stream temporal join task runtime is incomplete");
        }
        this.context = context;
        this.leftInput = leftInput;
        this.rightInput = rightInput;
        this.checkpointPayloadStore = checkpointPayloadStore;
        this.restoreMembers = Collections.unmodifiableList(
                new ArrayList<>(restoreMembers));
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        Object resolved = resolver.resolve(((StreamFunctionBinding)
                context.node().binding()).deploymentReference());
        if (!(resolved instanceof CoGroupFunction)) {
            throw new IllegalArgumentException(
                    "resolved Stream temporal join ABI differs");
        }
        this.function = (CoGroupFunction) resolved;
        List<Route> copied = new ArrayList<>();
        for (StreamTaskOutput output : outputs) {
            if (output.outputTagId != null) {
                throw new IllegalArgumentException(
                        "Stream temporal join output tag is unknown");
            }
            copied.add(new Route(output.output));
        }
        this.routes = Collections.unmodifiableList(copied);
        this.thread = new Thread(this::run,
                "rowweave-stream-temporal-join-" + taskName);
    }

    @Override public String operatorUid() {
        return context.node().operatorUid();
    }

    @Override public void start() { thread.start(); }

    @Override public CompletableFuture<List<StreamCheckpointMember>> checkpoint(
            CheckpointIdentity checkpoint) {
        return triggerCheckpoint(checkpoint);
    }

    @Override public void cancel() {
        if (cancellationRequested.compareAndSet(false, true)) {
            leftInput.cancel();
            rightInput.cancel();
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
        return resourceUsageReady
                ? StreamTaskResourceUsage.complete(
                0, maximumObservedStateBytes, 0, 0)
                : StreamTaskResourceUsage.unavailable();
    }

    synchronized CompletableFuture<List<StreamCheckpointMember>> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint is required");
        }
        if (checkpoint.equals(lastCheckpoint)) {
            return CompletableFuture.completedFuture(lastCheckpointMembers);
        }
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (cancellationRequested.get() || failure != null) {
            request.reject(new IllegalStateException(
                    "temporal join task is not active"));
            return request.result;
        }
        if (!sameAttempt(checkpoint)) {
            request.reject(new IllegalArgumentException(
                    "checkpoint does not belong to the temporal task"));
            return request.result;
        }
        if (lastCheckpoint != null
                && checkpoint.checkpointSequence()
                <= lastCheckpoint.checkpointSequence()) {
            request.reject(new IllegalArgumentException(
                    "temporal checkpoint sequence was already observed"));
            return request.result;
        }
        if (terminalMembers != null && !cancellationRequested.get()) {
            lastCheckpointMembers = terminalMembers;
            lastCheckpoint = checkpoint;
            return CompletableFuture.completedFuture(terminalMembers);
        }
        if (completed) {
            request.reject(new IllegalStateException(
                    "temporal join task is already complete"));
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "temporal join task already has a checkpoint in flight"));
            return request.result;
        }
        synchronized (this) {
            if (preBarrierCheckpoint != null) {
                if (!preBarrierCheckpoint.equals(checkpoint)) {
                    request.reject(new IllegalArgumentException(
                            "temporal checkpoint barrier differs from its trigger"));
                } else {
                    leftBarrier = preLeftBarrier;
                    rightBarrier = preRightBarrier;
                    preBarrierCheckpoint = null;
                    preLeftBarrier = false;
                    preRightBarrier = false;
                }
            }
        }
        return request.result;
    }

    private void run() {
        Path spillRoot = null;
        BoundedEventTimeTemporalJoin join = null;
        try {
            Binding binding = Binding.from(context);
            StreamResourceProfile resources = context.resources();
            spillRoot = Files.createTempDirectory("rowweave-stream-temporal-");
            long entryCharge = Math.addExact(
                    resources.maximumFrameBytes(),
                    INDEX_ENTRY_OVERHEAD_BYTES);
            long entryBudget = Math.max(1L,
                    resources.managedStateBytes() / entryCharge);
            int maximumEntries = (int) Math.min(
                    Integer.MAX_VALUE, entryBudget);
            StreamTimeService time = new StreamTimeService(
                    2, 1, System::currentTimeMillis, ignored -> { });
            join = new BoundedEventTimeTemporalJoin(
                    binding.join.operatorUid(), binding.lowerBoundMillis,
                    binding.upperBoundMillis,
                    context.maximumParallelism(), keyGroupStart,
                    keyGroupEnd, maximumEntries,
                    resources.spillDiskBytes(), resources.maximumFrameBytes(),
                    resources.maximumFrameBytes(), spillRoot,
                    (left, right) -> {
                        emitMatches(binding, left, right, false);
                        return new byte[] {1};
                    }, ignored -> { });
            resourceUsageReady = true;
            if (!restoreMembers.isEmpty()) {
                join.restore(subtaskIndex, context.node().stateDescriptors(),
                        restoreMembers, checkpointPayloadStore);
            }
            while (!cancellationRequested.get()) {
                completeCheckpointIfAligned(join);
                PolledInput input = pollInput();
                if (input != null) {
                    process(binding, join, time,
                            input.left, input.envelope);
                    maximumObservedStateBytes = Math.max(
                            maximumObservedStateBytes,
                            join.maximumObservedBytes());
                }
                completeCheckpointIfAligned(join);
                if (leftEnded && rightEnded) {
                    join.advanceLeftWatermark(Long.MAX_VALUE);
                    join.advanceRightWatermark(Long.MAX_VALUE);
                    emitCombinedWatermark(Long.MAX_VALUE);
                    publishTerminalMembers(join);
                    for (Route route : routes) route.sendEnd();
                    return;
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
                        "temporal task ended before checkpoint alignment"));
            }
            if (join != null) {
                try {
                    join.close();
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

    private PolledInput pollInput() throws InterruptedException {
        boolean left = leftEnded || leftBarrier ? false
                : rightEnded || rightBarrier || pollLeftFirst;
        if (!leftBarrier && !rightBarrier) pollLeftFirst = !pollLeftFirst;
        StreamEnvelope envelope = (left ? leftInput : rightInput)
                .poll(INPUT_POLL_MILLIS);
        if (envelope != null) return new PolledInput(left, envelope);
        boolean other = !left;
        if ((other && (leftBarrier || leftEnded))
                || (!other && (rightBarrier || rightEnded))) return null;
        envelope = (other ? leftInput : rightInput).poll(INPUT_POLL_MILLIS);
        return envelope == null ? null : new PolledInput(other, envelope);
    }

    private void process(Binding binding,
            BoundedEventTimeTemporalJoin join, StreamTimeService time,
            boolean left,
            StreamEnvelope envelope) {
        switch (envelope.kind()) {
            case RECORD:
                if (!envelope.hasEventTimestamp()) {
                    throw new IllegalArgumentException(
                            "temporal join record lacks an event timestamp");
                }
                boolean retractInput = left
                        ? binding.leftRetract : binding.rightRetract;
                StreamChangeRecord change = null;
                byte[] recordFrame = envelope.recordFrame();
                boolean add = true;
                if (retractInput) {
                    change = StreamChangeRecordCodec.decode(recordFrame,
                            context.maximumFrameBytes(),
                            context.maximumFrameBytes(),
                            context.maximumFrameBytes());
                    if (!change.hasBeforeImage()
                            && !change.hasAfterImage()) {
                        throw new UnsupportedOperationException(
                                "key-only DELETE cannot enter a temporal join");
                    }
                    recordFrame = change.hasBeforeImage()
                            ? change.beforeImageFrame()
                            : change.afterImageFrame();
                    add = change.kind() == StreamChangeRecord.Kind.INSERT
                            || change.kind()
                            == StreamChangeRecord.Kind.UPDATE_AFTER;
                }
                DataRow row = (left ? binding.leftSerializer
                        : binding.rightSerializer).deserialize(recordFrame);
                PortableRowSchema schema = left
                        ? binding.leftSchema : binding.rightSchema;
                validate(row, schema);
                byte[] key = (left ? binding.leftKeySerializer
                        : binding.rightKeySerializer).serializeProjected(
                        row, left ? binding.leftKeyIndices
                                : binding.rightKeyIndices);
                if (change != null && change.hasStableKey()
                        && !MessageDigest.isEqual(
                        key, change.stableKeyFrame())) {
                    throw new IllegalArgumentException(
                            "RETRACT temporal key differs from its row image");
                }
                if (left && add) {
                    join.onLeft(key, envelope.timestamp(), recordFrame);
                } else if (!left && add) {
                    join.onRight(key, envelope.timestamp(), recordFrame);
                } else if (left) {
                    join.retractLeft(key, envelope.timestamp(), recordFrame,
                            (leftFrame, rightFrame) -> {
                                emitMatches(binding, leftFrame,
                                        rightFrame, true);
                                return new byte[] {1};
                            });
                } else {
                    join.retractRight(key, envelope.timestamp(), recordFrame,
                            (leftFrame, rightFrame) -> {
                                emitMatches(binding, leftFrame,
                                        rightFrame, true);
                                return new byte[] {1};
                            });
                }
                break;
            case WATERMARK:
                if (left) {
                    join.advanceLeftWatermark(envelope.timestamp());
                } else {
                    join.advanceRightWatermark(envelope.timestamp());
                }
                if (left ? rightEnded : leftEnded) {
                    join.reclaimAfterOppositeEnd(leftEnded);
                }
                time.onWatermark(left ? 0 : 1, envelope.timestamp());
                emitCombinedWatermark(time.operatorWatermark());
                break;
            case WATERMARK_STATUS:
                time.onWatermarkStatus(left ? 0 : 1,
                        envelope.watermarkStatus(),
                        envelope.watermarkStatusGeneration());
                emitCombinedWatermark(time.operatorWatermark());
                break;
            case CHECKPOINT_BARRIER:
                observeBarrier(join, left,
                        envelope.checkpointIdentity());
                break;
            case CANCEL:
                cancel();
                break;
            case ERROR:
                throw new IllegalStateException(
                        "upstream temporal source reported an error");
            case END_OF_INPUT:
                if (left ? leftEnded : rightEnded) return;
                if (left) leftEnded = true;
                else rightEnded = true;
                join.reclaimAfterOppositeEnd(left);
                time.excludeInput(left ? 0 : 1);
                emitCombinedWatermark(time.operatorWatermark());
                break;
            case ASYNC_COMPLETION:
                throw new UnsupportedOperationException(
                        "resident temporal source emitted a terminal event");
            default:
                throw new UnsupportedOperationException(
                        "unsupported temporal join input event");
        }
    }

    private void emitMatches(Binding binding,
            byte[] leftFrame, byte[] rightFrame, boolean retract) {
        DataRow left = binding.leftSerializer.deserialize(leftFrame);
        DataRow right = binding.rightSerializer.deserialize(rightFrame);
        byte[] expectedKey = binding.leftKeySerializer.serializeProjected(
                left, binding.leftKeyIndices);
        int[] emitted = new int[1];
        function.coGroup(new SingletonRows(left), new SingletonRows(right), row -> {
            if (++emitted[0] > context.resources().mailboxEvents()) {
                throw new IllegalArgumentException(
                        "temporal join function exceeded its bounded output");
            }
            validate(row, binding.outputSchema);
            byte[] outputKey = binding.outputKeySerializer.serializeProjected(
                    row, binding.outputKeyIndices);
            if (!MessageDigest.isEqual(expectedKey, outputKey)) {
                throw new IllegalArgumentException(
                        "temporal join changed its stable output key");
            }
            byte[] afterImage = binding.outputSerializer.serialize(row);
            byte[] output;
            if (context.changelogMode()
                    == io.rowweave.stream.contract.PortableStreamPlan
                    .ChangelogMode.RETRACT) {
                output = StreamChangeRecordCodec.encode(retract
                        ? StreamChangeRecord.delete(outputKey, afterImage)
                        : StreamChangeRecord.insert(afterImage));
            } else {
                output = StreamUpsertRecordCodec.encode(
                        StreamUpsertRecord.afterImage(outputKey, afterImage));
            }
            for (Route route : routes) route.sendRecord(output);
        });
    }

    private void emitCombinedWatermark(long combined) {
        if (combined == Long.MIN_VALUE) return;
        if (combined <= outputWatermark) return;
        outputWatermark = combined;
        for (Route route : routes) route.sendWatermark(combined);
    }

    private void observeBarrier(BoundedEventTimeTemporalJoin join,
            boolean left, CheckpointIdentity checkpoint) {
        CheckpointRequest pending = pendingCheckpoint.get();
        if (!sameAttempt(checkpoint) || checkpointPayloadStore == null) {
            throw new IllegalArgumentException(
                    "temporal checkpoint barrier differs from its trigger");
        }
        if (pending == null) {
            synchronized (this) {
                if (lastCheckpoint != null
                        && lastCheckpoint.equals(checkpoint)) return;
                if (preBarrierCheckpoint == null) preBarrierCheckpoint = checkpoint;
                else if (!preBarrierCheckpoint.equals(checkpoint)) {
                    throw new IllegalArgumentException(
                            "temporal checkpoint barrier differs from its trigger");
                }
                if (left) preLeftBarrier = true; else preRightBarrier = true;
            }
            return;
        }
        if (!pending.checkpoint.equals(checkpoint)) {
            throw new IllegalArgumentException(
                    "temporal checkpoint barrier differs from its trigger");
        }
        if (left ? leftBarrier : rightBarrier) {
            throw new IllegalArgumentException(
                    "temporal checkpoint barrier is duplicated");
        }
        if (left) leftBarrier = true;
        else rightBarrier = true;
        if ((!leftBarrier && !leftEnded)
                || (!rightBarrier && !rightEnded)) return;
        completeCheckpointIfAligned(join);
    }

    private void completeCheckpointIfAligned(BoundedEventTimeTemporalJoin join) {
        CheckpointRequest pending = pendingCheckpoint.get();
        if (pending == null || (!leftBarrier && !leftEnded)
                || (!rightBarrier && !rightEnded)) return;
        CheckpointIdentity checkpoint = pending.checkpoint;
        List<StreamCheckpointMember> members = join.snapshot(
                subtaskIndex, context.node().stateDescriptors(),
                checkpointPayloadStore);
        for (Route route : routes) route.sendCheckpoint(checkpoint);
        lastCheckpointMembers = members;
        lastCheckpoint = checkpoint;
        leftBarrier = false;
        rightBarrier = false;
        if (!pendingCheckpoint.compareAndSet(pending, null)) {
            throw new IllegalStateException(
                    "temporal checkpoint trigger changed during alignment");
        }
        pending.result.complete(members);
    }

    private void publishTerminalMembers(BoundedEventTimeTemporalJoin join) {
        if (checkpointPayloadStore == null) return;
        List<StreamCheckpointMember> members = join.snapshot(
                subtaskIndex, context.node().stateDescriptors(),
                checkpointPayloadStore);
        synchronized (this) {
            terminalMembers = members;
            CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
            if (pending != null) {
                lastCheckpointMembers = members;
                lastCheckpoint = pending.checkpoint;
                pending.result.complete(members);
            }
        }
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return leftInput.channelIdentity().runId().equals(checkpoint.runId())
                && leftInput.channelIdentity().attemptId().equals(
                checkpoint.attemptId())
                && leftInput.channelIdentity().fenceToken()
                == checkpoint.fenceToken();
    }

    private void fail(Throwable thrown) {
        failure = thrown;
        leftInput.fail(thrown);
        rightInput.fail(thrown);
        for (Route route : routes) route.output.fail(thrown);
    }

    private static void validate(DataRow row, PortableRowSchema schema) {
        if (row == null || row.size() != schema.fieldCount()) {
            throw new IllegalArgumentException(
                    "Stream temporal row width differs from its declared schema");
        }
        for (int index = 0; index < schema.fieldCount(); index++) {
            if (!schema.field(index).nullable() && row.get(index) == null) {
                throw new IllegalArgumentException(
                        "Stream temporal row has a null non-null field");
            }
        }
    }

    private static final class Binding {
        final PortableStreamNode join;
        final PortableRowSchema leftSchema;
        final PortableRowSchema rightSchema;
        final PortableRowSchema outputSchema;
        final int[] leftKeyIndices;
        final int[] rightKeyIndices;
        final int[] outputKeyIndices;
        final RowSerializer leftSerializer;
        final RowSerializer rightSerializer;
        final RowSerializer leftKeySerializer;
        final RowSerializer rightKeySerializer;
        final RowSerializer outputSerializer;
        final RowSerializer outputKeySerializer;
        final long lowerBoundMillis;
        final long upperBoundMillis;
        final boolean leftRetract;
        final boolean rightRetract;

        private Binding(PortableStreamNode join,
                StreamNodeTaskContext.Source left,
                StreamNodeTaskContext.Source right) {
            this.join = join;
            this.leftSchema = left.schema();
            this.rightSchema = right.schema();
            this.outputSchema = join.outputSchema();
            this.leftRetract = left.changelogMode()
                    == io.rowweave.stream.contract.PortableStreamPlan
                    .ChangelogMode.RETRACT;
            this.rightRetract = right.changelogMode()
                    == io.rowweave.stream.contract.PortableStreamPlan
                    .ChangelogMode.RETRACT;
            this.leftKeyIndices = indices(leftSchema,
                    Collections.singletonList(join.scalarParameters().get(
                            "join.leftKeyField")));
            this.rightKeyIndices = indices(rightSchema,
                    Collections.singletonList(join.scalarParameters().get(
                            "join.rightKeyField")));
            this.outputKeyIndices = indices(
                    outputSchema, join.stableKeyFields());
            this.leftSerializer = serializer(leftSchema);
            this.rightSerializer = serializer(rightSchema);
            this.leftKeySerializer = serializer(
                    keySchema(leftSchema, leftKeyIndices));
            this.rightKeySerializer = serializer(
                    keySchema(rightSchema, rightKeyIndices));
            this.outputSerializer = serializer(outputSchema);
            this.outputKeySerializer = serializer(
                    keySchema(outputSchema, outputKeyIndices));
            this.lowerBoundMillis = Long.parseLong(
                    join.scalarParameters().get("join.lowerBoundMillis"));
            this.upperBoundMillis = Long.parseLong(
                    join.scalarParameters().get("join.upperBoundMillis"));
        }

        static Binding from(StreamNodeTaskContext context) {
            PortableStreamNode join = context.node();
            if (join.opcode()
                    != StreamOperatorCode.TEMPORAL_JOIN) {
                throw new UnsupportedOperationException(
                        "Stream temporal node contract is invalid");
            }
            if (context.inputs().size() != 2) {
                throw new UnsupportedOperationException(
                        "Stream temporal inputs are missing");
            }
            return new Binding(join,
                    context.inputs().get(0), context.inputs().get(1));
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
                    "Stream temporal stable key binding is missing");
        }
    }

    private static final class SingletonRows implements Iterable<DataRow> {
        private final DataRow row;
        private SingletonRows(DataRow row) { this.row = row; }

        @Override public Iterator<DataRow> iterator() {
            return new Iterator<DataRow>() {
                private boolean available = true;
                @Override public boolean hasNext() { return available; }
                @Override public DataRow next() {
                    if (!available) throw new NoSuchElementException();
                    available = false;
                    return row;
                }
            };
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

        void sendWatermark(long watermark) {
            output.send(StreamEnvelope.watermark(
                    output.channelIdentity(), next(), watermark));
        }

        void sendCheckpoint(CheckpointIdentity checkpoint) {
            output.send(StreamEnvelope.checkpointBarrier(
                    output.channelIdentity(), next(), checkpoint));
        }

        void sendEnd() {
            output.send(StreamEnvelope.endOfInput(
                    output.channelIdentity(), next()));
        }

        private long next() {
            sequence = Math.addExact(sequence, 1L);
            return sequence;
        }
    }

    private static final class PolledInput {
        final boolean left;
        final StreamEnvelope envelope;

        private PolledInput(boolean left, StreamEnvelope envelope) {
            this.left = left;
            this.envelope = envelope;
        }
    }

    private static final class CheckpointRequest {
        final CheckpointIdentity checkpoint;
        final CompletableFuture<List<StreamCheckpointMember>> result =
                new CompletableFuture<>();

        private CheckpointRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }

        void reject(Throwable failure) {
            result.completeExceptionally(failure);
        }
    }
}
