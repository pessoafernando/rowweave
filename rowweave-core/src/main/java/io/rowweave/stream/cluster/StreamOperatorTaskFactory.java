package io.rowweave.stream.cluster;

import io.rowweave.Collector;
import io.rowweave.FilterFunction;
import io.rowweave.FlatMapFunction;
import io.rowweave.MapFunction;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamCalcBinding;
import io.rowweave.stream.contract.StreamFunctionBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamTimeBinding;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalResidentCdcSourceTask;
import io.rowweave.stream.runtime.LocalStreamSinkTask;
import io.rowweave.stream.runtime.LocalStreamSourceTask;
import io.rowweave.stream.runtime.LocalTransactionalStreamSinkTask;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamCheckpointAuthority;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Creates one validated Worker task for each admitted portable operator. */
final class StreamOperatorTaskFactory {
    private static final byte[] EVENT_TIME_STATE_MAGIC =
            "RWXXET01".getBytes(StandardCharsets.US_ASCII);
    private static final String EVENT_TIME_STATE_NAME =
            "event-time-assignment-generator";
    private static final String EVENT_TIME_STATE_CODEC =
            "rowweave.stream.event-time-assignment-state.v1";
    private static final int EVENT_TIME_STATE_VERSION = 1;

    private StreamOperatorTaskFactory() { }

    static boolean supportsInline(StreamOperatorCode opcode) {
        return opcode == StreamOperatorCode.MAP
                || opcode == StreamOperatorCode.FILTER
                || opcode == StreamOperatorCode.CALC
                || opcode == StreamOperatorCode.FLAT_MAP
                || opcode == StreamOperatorCode.KEY_BY
                || opcode == StreamOperatorCode.ASSIGN_EVENT_TIME
                || opcode == StreamOperatorCode.ASYNC_WAIT;
    }

    static boolean supportsMailbox(StreamOperatorCode opcode) {
        return opcode == StreamOperatorCode.KEYED_AGGREGATE
                || opcode == StreamOperatorCode.EVENT_TIME_WINDOW
                || opcode == StreamOperatorCode.GENERIC_WINDOW
                || opcode == StreamOperatorCode.KEYED_PROCESS
                || opcode == StreamOperatorCode.KEYED_CO_PROCESS
                || opcode == StreamOperatorCode.TEMPORAL_JOIN;
    }

    static boolean supportsRouting(StreamOperatorCode opcode) {
        return opcode == StreamOperatorCode.UNION;
    }

    static boolean supportsEndpoint(StreamOperatorCode opcode) {
        return opcode == StreamOperatorCode.SOURCE
                || opcode == StreamOperatorCode.SINK;
    }

    static StreamOperatorTask create(PortableStreamNode node,
            PortableRowSchema inputSchema, StreamFunctionResolver resolver) {
        return create(node, inputSchema, resolver,
                io.rowweave.stream.contract.PortableStreamPlan
                        .MAX_PARALLELISM);
    }

    static StreamOperatorTask create(PortableStreamNode node,
            PortableRowSchema inputSchema, StreamFunctionResolver resolver,
            int maximumParallelism) {
        return create(node, inputSchema, resolver, maximumParallelism,
                PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE);
    }

    static StreamOperatorTask create(PortableStreamNode node,
            PortableRowSchema inputSchema, StreamFunctionResolver resolver,
            int maximumParallelism,
            PortableStreamPlan.DeliveryGuarantee delivery) {
        return create(node, inputSchema, resolver, maximumParallelism,
                delivery, 0, 0, maximumParallelism - 1);
    }

    static StreamOperatorTask create(PortableStreamNode node,
            PortableRowSchema inputSchema, StreamFunctionResolver resolver,
            int maximumParallelism,
            PortableStreamPlan.DeliveryGuarantee delivery,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        if (delivery == null) {
            throw new IllegalArgumentException(
                    "Stream delivery guarantee is required");
        }
        if (subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= maximumParallelism) {
            throw new IllegalArgumentException(
                    "Stream inline task partition is invalid");
        }
        if (node.opcode() == StreamOperatorCode.CALC) {
            return new CalcTask(node, inputSchema);
        }
        if (node.opcode() == StreamOperatorCode.KEY_BY) {
            return new PassThroughTask(node);
        }
        if (node.opcode() == StreamOperatorCode.ASSIGN_EVENT_TIME) {
            return new EventTimeTask(node, inputSchema, maximumParallelism,
                    subtaskIndex, keyGroupStart, keyGroupEnd);
        }
        if (node.opcode() == StreamOperatorCode.ASYNC_WAIT) {
            StreamAsyncBinding binding = (StreamAsyncBinding) node.binding();
            if (delivery == PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE
                    && !resolver.supportsAsyncReplay(
                    binding.deploymentReference(), replaySpec(binding))) {
                throw new UnsupportedOperationException(
                        "Stream ASYNC_WAIT replay conformance is not proven");
            }
            Serializable function = resolver.resolve(
                    binding.deploymentReference());
            require(function instanceof io.rowweave.StreamAsyncFunction,
                    "ASYNC_WAIT");
            return new StreamAsyncWaitOperatorTask(node, inputSchema,
                    (io.rowweave.StreamAsyncFunction) function,
                    maximumParallelism, subtaskIndex,
                    keyGroupStart, keyGroupEnd);
        }
        if (node.opcode() == StreamOperatorCode.MAP
                || node.opcode() == StreamOperatorCode.FILTER
                || node.opcode() == StreamOperatorCode.FLAT_MAP) {
            Serializable function = resolver.resolve(
                    ((StreamFunctionBinding) node.binding())
                            .deploymentReference());
            if (node.opcode() == StreamOperatorCode.MAP) {
                require(function instanceof MapFunction, "MAP");
                return new MapTask(node, (MapFunction) function);
            }
            if (node.opcode() == StreamOperatorCode.FILTER) {
                require(function instanceof FilterFunction, "FILTER");
                return new FilterTask(node, (FilterFunction) function);
            }
            require(function instanceof FlatMapFunction, "FLAT_MAP");
            return new FlatMapTask(node, (FlatMapFunction) function);
        }
        throw new UnsupportedOperationException(
                "Stream operator factory has no task for " + node.opcode());
    }

    static StreamOperatorTask createRouting(PortableStreamNode node,
            Supplier<? extends StreamOperatorTask> constructor) {
        if (node == null || constructor == null
                || !supportsRouting(node.opcode())) {
            throw new IllegalArgumentException(
                    "Stream routing task contract is invalid");
        }
        StreamOperatorTask task = constructor.get();
        if (task == null || !node.operatorUid().equals(task.operatorUid())) {
            throw new IllegalArgumentException(
                    "Stream routing task identity differs from its node");
        }
        return task;
    }

    static StreamOperatorTask createEndpoint(PortableStreamNode node,
            LocalStreamSourceTask task) {
        requireEndpoint(node, StreamOperatorCode.SOURCE, task);
        return new SourceTask(node.operatorUid(), task);
    }

    static StreamOperatorTask createEndpoint(PortableStreamNode node,
            LocalResidentCdcSourceTask task) {
        requireEndpoint(node, StreamOperatorCode.SOURCE, task);
        return new ResidentSourceTask(node.operatorUid(), task);
    }

    static StreamOperatorTask createEndpoint(PortableStreamNode node,
            LocalStreamSinkTask task) {
        requireEndpoint(node, StreamOperatorCode.SINK, task);
        return new SinkTask(node.operatorUid(), task);
    }

    static StreamOperatorTask createEndpoint(PortableStreamNode node,
            LocalTransactionalStreamSinkTask task) {
        requireEndpoint(node, StreamOperatorCode.SINK, task);
        return new TransactionalSinkTask(node.operatorUid(), task);
    }

    private static void requireEndpoint(PortableStreamNode node,
            StreamOperatorCode expected, Object task) {
        if (node == null || node.opcode() != expected || task == null) {
            throw new IllegalArgumentException(
                    "Stream endpoint task contract is invalid");
        }
    }

    static StreamOperatorTask createMailbox(String taskName,
            StreamNodeTaskContext context,
            List<? extends StreamChannelInput> inputs,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            boolean restoring, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd) {
        if (context == null || inputs == null || outputs == null
                || resolver == null || restoreMembers == null) {
            throw new IllegalArgumentException(
                    "Stream mailbox task runtime is incomplete");
        }
        List<StreamCheckpointMember> taskRestore = stateMembers(
                restoreMembers, context.node().operatorUid(), subtaskIndex);
        switch (context.node().opcode()) {
            case KEYED_AGGREGATE:
                return new StreamKeyedAggregateOperatorTask(taskName,
                        context, onlyInput(inputs), outputs, resolver,
                        checkpointPayloadStore,
                        singleState(taskRestore, restoring), subtaskIndex,
                        keyGroupStart, keyGroupEnd);
            case EVENT_TIME_WINDOW:
                return new StreamWindowOperatorTask(taskName, context,
                        onlyInput(inputs), outputs, checkpointPayloadStore,
                        singleState(taskRestore, restoring), subtaskIndex,
                        keyGroupStart, keyGroupEnd);
            case GENERIC_WINDOW:
                return StreamGenericWindowOperatorTask.composed(taskName,
                        context, onlyInput(inputs), outputs, resolver,
                        checkpointPayloadStore,
                        singleState(taskRestore, restoring), subtaskIndex,
                        keyGroupStart, keyGroupEnd);
            case KEYED_PROCESS:
            case KEYED_CO_PROCESS:
                requireRestore(taskRestore, restoring,
                        "keyed process restore state is missing");
                return StreamKeyedProcessOperatorTask.composed(taskName,
                        context, inputs, outputs, resolver,
                        checkpointPayloadStore, taskRestore, subtaskIndex,
                        keyGroupStart, keyGroupEnd);
            case TEMPORAL_JOIN:
                requireRestore(taskRestore, restoring,
                        "temporal join restore state is missing");
                if (inputs.size() != 2) {
                    throw new IllegalArgumentException(
                            "temporal join requires two mailbox lanes");
                }
                return new StreamTemporalJoinOperatorTask(taskName, context,
                        inputs.get(0), inputs.get(1), outputs, resolver,
                        checkpointPayloadStore, taskRestore, subtaskIndex,
                        keyGroupStart, keyGroupEnd);
            default:
                throw new UnsupportedOperationException(
                        "Stream mailbox factory has no task for "
                                + context.node().opcode());
        }
    }

    private static StreamChannelInput onlyInput(
            List<? extends StreamChannelInput> inputs) {
        if (inputs.size() != 1 || inputs.get(0) == null) {
            throw new IllegalArgumentException(
                    "Stream unary mailbox task requires one input");
        }
        return inputs.get(0);
    }

    private static List<StreamCheckpointMember> stateMembers(
            List<StreamCheckpointMember> members, String operatorUid,
            int subtaskIndex) {
        List<StreamCheckpointMember> result = new ArrayList<>();
        for (StreamCheckpointMember member : members) {
            if (member.kind() == StreamCheckpointMember.Kind.STATE
                    && member.operatorUid().equals(operatorUid)
                    && member.subtaskIndex() == subtaskIndex) {
                result.add(member);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static StreamCheckpointMember singleState(
            List<StreamCheckpointMember> members, boolean required) {
        if (members.size() > 1) {
            throw new IllegalArgumentException(
                    "Stream restore contains duplicate task state");
        }
        requireRestore(members, required,
                "Stream restore task state is missing");
        return members.isEmpty() ? null : members.get(0);
    }

    private static void requireRestore(
            List<StreamCheckpointMember> members, boolean required,
            String message) {
        if (required && members.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    private static StreamAsyncReplaySpec replaySpec(
            StreamAsyncBinding binding) {
        if (binding.replayMode()
                == StreamAsyncBinding.ReplayMode.IDENTITY_KEYED) {
            return StreamAsyncReplaySpec.identityKeyed(
                    binding.replayAuthorityId(),
                    binding.replayAuthorityDigest());
        }
        if (binding.replayMode()
                == StreamAsyncBinding.ReplayMode.IMMUTABLE_VERSION) {
            return StreamAsyncReplaySpec.immutableVersion(
                    binding.replayAuthorityId(),
                    binding.replayAuthorityDigest());
        }
        return StreamAsyncReplaySpec.atLeastOnce();
    }

    private static void require(boolean valid, String kind) {
        if (!valid) {
            throw new IllegalArgumentException(
                    "resolved Stream " + kind + " ABI differs");
        }
    }

    private abstract static class BaseTask implements StreamOperatorTask {
        final PortableStreamNode node;
        final PortableRowSchema outputSchema;

        BaseTask(PortableStreamNode node) {
            this.node = node;
            this.outputSchema = node.outputSchema();
        }

        @Override public String operatorUid() { return node.operatorUid(); }

        @Override public StreamTaskResourceUsage resourceUsage() {
            return StreamTaskResourceUsage.zero();
        }

        final void emit(DataRow row, Long eventTimestamp, Sink output) {
            validate(row, outputSchema);
            output.emit(row, eventTimestamp);
        }
    }

    private static final class MapTask extends BaseTask {
        private final MapFunction function;
        MapTask(PortableStreamNode node, MapFunction function) {
            super(node); this.function = function;
        }
        @Override public void process(DataRow input, Long eventTimestamp,
                Sink output) {
            DataRow result = function.map(input);
            if (result == null) {
                throw new IllegalArgumentException(
                        "Stream MAP returned a null row");
            }
            emit(result, eventTimestamp, output);
        }
    }

    private static final class PassThroughTask extends BaseTask {
        PassThroughTask(PortableStreamNode node) { super(node); }

        @Override public void process(DataRow input, Long eventTimestamp,
                Sink output) {
            emit(input, eventTimestamp, output);
        }
    }

    private static final class FilterTask extends BaseTask {
        private final FilterFunction function;
        FilterTask(PortableStreamNode node, FilterFunction function) {
            super(node); this.function = function;
        }
        @Override public void process(DataRow input, Long eventTimestamp,
                Sink output) {
            if (function.filter(input)) emit(input, eventTimestamp, output);
        }
    }

    private static final class FlatMapTask extends BaseTask {
        private final FlatMapFunction function;
        private final int maximumRows;
        private final long maximumBytes;
        private final RowSerializer serializer;

        FlatMapTask(PortableStreamNode node, FlatMapFunction function) {
            super(node);
            this.function = function;
            this.maximumRows = Integer.parseInt(node.scalarParameters().get(
                    "flatMap.maxOutputsPerInput"));
            this.maximumBytes = Long.parseLong(node.scalarParameters().get(
                    "flatMap.maxOutputBytesPerInput"));
            this.serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(outputSchema));
        }

        @Override public void process(DataRow input, Long eventTimestamp,
                Sink output) {
            QuotaCollector collector = new QuotaCollector(
                    eventTimestamp, output);
            try {
                function.flatMap(input, collector);
            } finally {
                collector.close();
            }
            collector.requireHealthy();
        }

        private final class QuotaCollector implements Collector {
            private final Long eventTimestamp;
            private final Sink output;
            private final Thread callbackThread = Thread.currentThread();
            private final AtomicBoolean active = new AtomicBoolean(true);
            private final AtomicReference<RuntimeException> poison =
                    new AtomicReference<>();
            private int rows;
            private long bytes;

            private QuotaCollector(Long eventTimestamp, Sink output) {
                this.eventTimestamp = eventTimestamp;
                this.output = output;
            }

            @Override public synchronized void collect(DataRow row) {
                if (!active.get()
                        || Thread.currentThread() != callbackThread) {
                    throw rejectAsynchronousEmission(!active.get()
                            ? "Stream FLAT_MAP collector emitted after callback"
                            : "Stream FLAT_MAP collector emitted outside callback thread");
                }
                RuntimeException prior = poison.get();
                if (prior != null) throw prior;
                try {
                    validate(row, outputSchema);
                    int emittedRows = Math.addExact(rows, 1);
                    long emittedBytes = Math.addExact(bytes,
                            serializer.serializedSize(row));
                    if (emittedRows > maximumRows
                            || emittedBytes > maximumBytes) {
                        throw new IllegalStateException(
                                "Stream FLAT_MAP output quota exceeded");
                    }
                    rows = emittedRows;
                    bytes = emittedBytes;
                    output.emit(row, eventTimestamp);
                } catch (RuntimeException failure) {
                    poison.compareAndSet(null, failure);
                    throw failure;
                }
            }

            private void close() {
                active.set(false);
            }

            private void requireHealthy() {
                RuntimeException failure = poison.get();
                if (failure != null) throw failure;
            }

            private IllegalStateException rejectAsynchronousEmission(
                    String message) {
                IllegalStateException failure = new IllegalStateException(
                        message);
                poison.compareAndSet(null, failure);
                try {
                    output.fail(failure);
                } catch (RuntimeException | Error outputFailure) {
                    if (outputFailure != failure) {
                        failure.addSuppressed(outputFailure);
                    }
                }
                return failure;
            }
        }
    }

    private static final class CalcTask extends BaseTask {
        private final StreamCalcBinding binding;
        private final StreamExpressionEvaluator evaluator;

        CalcTask(PortableStreamNode node, PortableRowSchema inputSchema) {
            super(node);
            this.binding = (StreamCalcBinding) node.binding();
            this.evaluator = new StreamExpressionEvaluator(inputSchema);
        }

        @Override public void process(DataRow input, Long eventTimestamp,
                Sink output) {
            Object predicate = binding.predicate() == null ? Boolean.TRUE
                    : evaluator.evaluate(binding.predicate(), input);
            if (!Boolean.TRUE.equals(predicate)) return;
            DataRow result = new DataRow(binding.projections().size());
            for (int index = 0; index < binding.projections().size(); index++) {
                StreamCalcBinding.Projection projection =
                        binding.projections().get(index);
                Object value = evaluator.evaluate(
                        projection.expression(), input);
                if (value != null && projection.decimalOutputBoundary()) {
                    value = ((BigDecimal) value).setScale(
                            10, RoundingMode.HALF_UP);
                }
                result.set(index, value);
            }
            emit(result, eventTimestamp, output);
        }
    }

    private static final class EventTimeTask extends BaseTask {
        private final StreamTimeBinding binding;
        private final StreamExpressionEvaluator evaluator;
        private final int subtaskIndex;
        private final int keyGroupStart;
        private final int keyGroupEnd;
        private final long idleTimeoutNanos;
        private long maximumTimestamp = Long.MIN_VALUE;
        private long lastWatermark = Long.MIN_VALUE;
        private long idleDeadlineNanos;
        private long statusGeneration;
        private boolean timerInitialized;
        private boolean idle;

        EventTimeTask(PortableStreamNode node,
                PortableRowSchema inputSchema, int maximumParallelism,
                int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
            super(node);
            if (maximumParallelism <= 0
                    || maximumParallelism
                    > io.rowweave.stream.contract.PortableStreamPlan
                    .MAX_PARALLELISM) {
                throw new IllegalArgumentException(
                        "Stream event-time maximum parallelism is invalid");
            }
            this.binding = (StreamTimeBinding) node.binding();
            this.evaluator = new StreamExpressionEvaluator(inputSchema);
            this.subtaskIndex = subtaskIndex;
            this.keyGroupStart = keyGroupStart;
            this.keyGroupEnd = keyGroupEnd;
            this.idleTimeoutNanos = millisToNanos(
                    binding.idleTimeoutMillis());
        }

        @Override public synchronized void process(DataRow input,
                Long ignoredTimestamp,
                Sink output) {
            Object extracted = evaluator.evaluate(
                    binding.timestampExpression(), input);
            if (extracted == null) {
                if (binding.invalidTimestampPolicy()
                        == StreamTimeBinding.InvalidTimestampPolicy.DROP) {
                    return;
                }
                throw new IllegalArgumentException(
                        "Stream event-time expression returned null");
            }
            if (!(extracted instanceof java.util.Date)) {
                throw new IllegalArgumentException(
                        "Stream event-time expression returned an invalid value");
            }
            long timestamp = ((java.util.Date) extracted).getTime();
            long now = System.nanoTime();
            if (idle) {
                idle = false;
                statusGeneration = Math.addExact(statusGeneration, 1L);
                output.emitWatermarkStatus(
                        StreamEnvelope.WatermarkStatus.ACTIVE,
                        statusGeneration);
            }
            timerInitialized = true;
            idleDeadlineNanos = saturatingAdd(now, idleTimeoutNanos);
            emit(input, timestamp, output);
            maximumTimestamp = Math.max(maximumTimestamp, timestamp);
            long outOfOrder = binding.maximumOutOfOrderMillis();
            long watermark = maximumTimestamp < Long.MIN_VALUE + outOfOrder
                    ? Long.MIN_VALUE : maximumTimestamp - outOfOrder;
            if (watermark > lastWatermark) {
                lastWatermark = watermark;
                output.emitWatermark(watermark);
            }
        }

        @Override public boolean hasIdleTimer() { return true; }

        @Override public synchronized StreamTaskResourceUsage resourceUsage() {
            return StreamTaskResourceUsage.complete(
                    0, 0L, timerInitialized ? 1 : 0, 0);
        }

        @Override public synchronized void start(long nowNanos) {
            if (!timerInitialized && !idle) {
                timerInitialized = true;
                idleDeadlineNanos = saturatingAdd(
                        nowNanos, idleTimeoutNanos);
            }
        }

        @Override public synchronized long idleDelayMillis(long nowNanos) {
            if (idle || !timerInitialized) return Long.MAX_VALUE;
            long remaining = idleDeadlineNanos - nowNanos;
            if (remaining <= 0L) return 0L;
            return Math.max(1L, Math.min(100L,
                    (remaining + 999_999L) / 1_000_000L));
        }

        @Override public synchronized void onIdleTimer(
                long nowNanos, Sink output) {
            if (idle || !timerInitialized
                    || nowNanos - idleDeadlineNanos < 0L) return;
            idle = true;
            statusGeneration = Math.addExact(statusGeneration, 1L);
            output.emitWatermarkStatus(StreamEnvelope.WatermarkStatus.IDLE,
                    statusGeneration);
        }

        @Override public boolean hasCheckpointState() { return true; }

        @Override public synchronized StreamCheckpointMember snapshot(
                CheckpointIdentity checkpoint,
                FileCheckpointPayloadStore payloadStore) {
            if (checkpoint == null || payloadStore == null) {
                throw new IllegalArgumentException(
                        "Stream event-time checkpoint runtime is unavailable");
            }
            long now = System.nanoTime();
            long remainingNanos = idle || !timerInitialized ? 0L
                    : Math.max(0L, idleDeadlineNanos - now);
            long remainingMillis = Math.min(binding.idleTimeoutMillis(),
                    (remainingNanos + 999_999L) / 1_000_000L);
            Path staged = null;
            Throwable primary = null;
            try {
                staged = Files.createTempFile(
                        "rowweave-stream-event-time-", ".snapshot");
                try (DataOutputStream output = new DataOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(staged,
                                StandardOpenOption.TRUNCATE_EXISTING)))) {
                    output.write(EVENT_TIME_STATE_MAGIC);
                    output.writeInt(EVENT_TIME_STATE_VERSION);
                    output.writeLong(maximumTimestamp);
                    output.writeLong(lastWatermark);
                    output.writeLong(statusGeneration);
                    output.writeBoolean(idle);
                    output.writeLong(remainingMillis);
                }
                FileCheckpointPayloadStore.StoredPayload stored =
                        payloadStore.store(staged);
                return StreamCheckpointMember.stateHandle(
                        node.operatorUid(), subtaskIndex, keyGroupStart,
                        keyGroupEnd, EVENT_TIME_STATE_NAME,
                        EVENT_TIME_STATE_CODEC, EVENT_TIME_STATE_VERSION,
                        stored.digest(), stored.contentBytes());
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } catch (IOException failure) {
                primary = failure;
                throw new IllegalStateException(
                        "Stream event-time checkpoint staging failed", failure);
            } finally {
                delete(staged, primary);
            }
        }

        @Override public synchronized void restore(
                StreamCheckpointMember member,
                FileCheckpointPayloadStore payloadStore) {
            validateRestoreMember(member, payloadStore);
            Path staged = null;
            Throwable primary = null;
            try {
                staged = Files.createTempFile(
                        "rowweave-stream-event-time-", ".restore");
                payloadStore.restore(
                        new FileCheckpointPayloadStore.StoredPayload(
                                member.contentDigest(), member.contentBytes()),
                        staged);
                try (DataInputStream input = new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(staged)))) {
                    byte[] magic = new byte[EVENT_TIME_STATE_MAGIC.length];
                    input.readFully(magic);
                    if (!java.util.Arrays.equals(
                            EVENT_TIME_STATE_MAGIC, magic)
                            || input.readInt() != EVENT_TIME_STATE_VERSION) {
                        throw new IllegalArgumentException(
                                "Stream event-time checkpoint header is invalid");
                    }
                    long restoredMaximum = input.readLong();
                    long restoredWatermark = input.readLong();
                    long restoredGeneration = input.readLong();
                    boolean restoredIdle = input.readBoolean();
                    long remainingMillis = input.readLong();
                    if (input.read() != -1 || restoredGeneration < 0L
                            || restoredWatermark > restoredMaximum
                            || remainingMillis < 0L
                            || remainingMillis > binding.idleTimeoutMillis()
                            || (restoredIdle && remainingMillis != 0L)) {
                        throw new IllegalArgumentException(
                                "Stream event-time checkpoint state is invalid");
                    }
                    maximumTimestamp = restoredMaximum;
                    lastWatermark = restoredWatermark;
                    statusGeneration = restoredGeneration;
                    idle = restoredIdle;
                    timerInitialized = true;
                    idleDeadlineNanos = saturatingAdd(System.nanoTime(),
                            millisToNanos(remainingMillis));
                }
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } catch (IOException failure) {
                primary = failure;
                throw new IllegalStateException(
                        "Stream event-time checkpoint restore failed", failure);
            } finally {
                delete(staged, primary);
            }
        }

        private void validateRestoreMember(StreamCheckpointMember member,
                FileCheckpointPayloadStore payloadStore) {
            if (member == null || payloadStore == null
                    || member.kind() != StreamCheckpointMember.Kind.STATE
                    || !node.operatorUid().equals(member.operatorUid())
                    || member.subtaskIndex() != subtaskIndex
                    || member.keyGroupStart() != keyGroupStart
                    || member.keyGroupEnd() != keyGroupEnd
                    || !EVENT_TIME_STATE_NAME.equals(member.subjectId())
                    || !EVENT_TIME_STATE_CODEC.equals(member.codecId())
                    || member.schemaVersion() != EVENT_TIME_STATE_VERSION) {
                throw new IllegalArgumentException(
                        "Stream event-time restore member is incompatible");
            }
        }
    }

    private abstract static class EndpointTask implements StreamOperatorTask {
        private final String operatorUid;

        EndpointTask(String operatorUid) { this.operatorUid = operatorUid; }

        @Override public final String operatorUid() { return operatorUid; }

        @Override public final StreamTaskResourceUsage resourceUsage() {
            return StreamTaskResourceUsage.zero();
        }
    }

    private static final class SourceTask extends EndpointTask {
        private final LocalStreamSourceTask delegate;

        SourceTask(String operatorUid, LocalStreamSourceTask delegate) {
            super(operatorUid);
            this.delegate = delegate;
        }

        @Override public void start() { delegate.start(); }
        @Override public void cancel() { delegate.cancel(); }
        @Override public void close() { delegate.cancel(); }
        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            delegate.awaitCompletion(timeoutMillis);
        }
        @Override public Throwable failure() { return delegate.failure(); }
        @Override public boolean completed() {
            return delegate.isCompleted();
        }
        @Override public void assignSourceSplit(
                io.rowweave.stream.contract.StreamSourceSplit split) {
            delegate.assignDynamicSplit(split);
        }
        @Override public CompletableFuture<Void> revokeSourceSplit(
                String splitId) {
            return delegate.revokeDynamicSplit(splitId);
        }
        @Override public io.rowweave.stream.contract.StreamSourceSplit
        completedSourceSplit() {
            return delegate.completedDynamicSplit();
        }
        @Override public CompletableFuture<List<StreamCheckpointMember>>
        checkpoint(CheckpointIdentity checkpoint) {
            return singleton(delegate.triggerCheckpoint(checkpoint));
        }
    }

    private static final class ResidentSourceTask extends EndpointTask {
        private final LocalResidentCdcSourceTask delegate;

        ResidentSourceTask(String operatorUid,
                LocalResidentCdcSourceTask delegate) {
            super(operatorUid);
            this.delegate = delegate;
        }

        @Override public void start() { delegate.start(); }
        @Override public void cancel() { delegate.cancel(); }
        @Override public void close() { delegate.cancel(); }
        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            delegate.awaitCompletion(timeoutMillis);
        }
        @Override public Throwable failure() { return delegate.failure(); }
        @Override public CompletableFuture<List<StreamCheckpointMember>>
        checkpoint(CheckpointIdentity checkpoint) {
            return singleton(delegate.triggerCheckpoint(checkpoint));
        }
        @Override public CompletableFuture<Void> checkpointCompleted(
                CheckpointIdentity checkpoint) {
            return delegate.notifyCheckpointComplete(checkpoint);
        }
    }

    private static final class SinkTask extends EndpointTask {
        private final LocalStreamSinkTask delegate;

        SinkTask(String operatorUid, LocalStreamSinkTask delegate) {
            super(operatorUid);
            this.delegate = delegate;
        }

        @Override public void start() { delegate.start(); }
        @Override public void cancel() { delegate.cancel(); }
        @Override public void close() { delegate.cancel(); }
        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            delegate.awaitCompletion(timeoutMillis);
        }
        @Override public Throwable failure() { return delegate.failure(); }
        @Override public boolean completed() {
            return delegate.isCompleted();
        }
        @Override public CompletableFuture<List<StreamCheckpointMember>>
        checkpoint(CheckpointIdentity checkpoint) {
            return singleton(delegate.triggerCheckpoint(checkpoint));
        }
    }

    private static final class TransactionalSinkTask extends EndpointTask {
        private final LocalTransactionalStreamSinkTask delegate;

        TransactionalSinkTask(String operatorUid,
                LocalTransactionalStreamSinkTask delegate) {
            super(operatorUid);
            this.delegate = delegate;
        }

        @Override public void start() { delegate.start(); }
        @Override public void cancel() { delegate.cancel(); }
        @Override public void close() { delegate.cancel(); }
        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            delegate.awaitCompletion(timeoutMillis);
        }
        @Override public Throwable failure() { return delegate.failure(); }
        @Override public boolean completed() {
            return delegate.isCompleted();
        }
        @Override public CheckpointIdentity preparedCheckpoint() {
            return delegate.preparedCheckpoint();
        }
        @Override public boolean terminalInputSeen() {
            return delegate.terminalInputSeen();
        }
        @Override public void reconcile(CheckpointDecision decision,
                StreamCheckpointAuthority authority) {
            delegate.reconcile(decision, authority);
        }
        @Override public CompletableFuture<List<StreamCheckpointMember>>
        checkpoint(CheckpointIdentity checkpoint) {
            return singleton(delegate.triggerCheckpoint(checkpoint));
        }
    }

    private static CompletableFuture<List<StreamCheckpointMember>> singleton(
            CompletableFuture<StreamCheckpointMember> member) {
        return member.thenApply(Collections::singletonList);
    }

    private static long millisToNanos(long millis) {
        try {
            return Math.multiplyExact(millis, 1_000_000L);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static long saturatingAdd(long left, long right) {
        long result = left + right;
        return ((left ^ result) & (right ^ result)) < 0L
                ? Long.MAX_VALUE : result;
    }

    private static void delete(Path path, Throwable primary) {
        if (path == null) return;
        try {
            Files.deleteIfExists(path);
        } catch (IOException failure) {
            if (primary != null) primary.addSuppressed(failure);
            else throw new IllegalStateException(
                    "Stream event-time checkpoint staging cleanup failed",
                    failure);
        }
    }

    private static void validate(DataRow row, PortableRowSchema schema) {
        if (row == null || row.size() != schema.fieldCount()) {
            throw new IllegalArgumentException(
                    "Stream function row width differs from its declared schema");
        }
        for (int index = 0; index < schema.fieldCount(); index++) {
            PortableField field = schema.field(index);
            Object value = row.get(index);
            if (!field.nullable() && value == null) {
                throw new IllegalArgumentException(
                        "Stream function produced null for a non-null field");
            }
            if (value != null && !matches(field.dataType(), value)) {
                throw new IllegalArgumentException(
                        "Stream function field type differs from its declared schema");
            }
        }
    }

    private static boolean matches(
            io.rowweave.portable.PortableDataType type, Object value) {
        switch (type) {
            case BOOLEAN: return value instanceof Boolean;
            case STRING: return value instanceof String;
            case INTEGER: return value instanceof Integer;
            case LONG: return value instanceof Long;
            case BIG_DECIMAL: return value instanceof BigDecimal;
            case DATE: return value instanceof java.util.Date;
            case TIMESTAMP: return value instanceof Timestamp;
            case NULL: return false;
            default: throw new AssertionError(type);
        }
    }
}
