package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamAggregatingState;
import io.rowweave.StreamBroadcastProcessContext;
import io.rowweave.StreamBroadcastState;
import io.rowweave.StreamBroadcastTimerContext;
import io.rowweave.StreamCollector;
import io.rowweave.StreamKeyedBroadcastProcessContext;
import io.rowweave.StreamKeyedBroadcastProcessFunction;
import io.rowweave.StreamKeyedCoProcessFunction;
import io.rowweave.StreamKeyedProcessFunction;
import io.rowweave.StreamListState;
import io.rowweave.StreamMapState;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamProcessException;
import io.rowweave.StreamReadOnlyBroadcastState;
import io.rowweave.StreamStateCursor;
import io.rowweave.StreamTimerContext;
import io.rowweave.StreamTimerService;
import io.rowweave.StreamValueState;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamTimerSpec;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamBroadcastStateDescriptor;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamManagedStateDescriptor;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamOutputTagDescriptor;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamTimerDescriptor;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.ManagedKeyedValueState;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Single-thread mailbox owner for one public keyed process subtask. */
final class StreamKeyedProcessOperatorTask implements StreamOperatorTask {
    private static final byte[] VALUE_NAMESPACE = new byte[0];
    private static final byte[] TIMER_VALUE = new byte[] {1};
    private static final String TIMER_CODEC = "rowweave.stream.keyed-timer.v1";
    private static final String BROADCAST_STATE_PREFIX =
            "__rowweave_broadcast__/";
    private static final String BROADCAST_DIGEST_PREFIX =
            "__rowweave_broadcast_digest__/";
    private static final String BROADCAST_DIGEST_CODEC =
            "rowweave.stream.broadcast-digest.v1";
    private static final String MULTI_INPUT_GATE_STATE =
            "__rowweave_gate__/inputs";
    private static final String MULTI_INPUT_GATE_CODEC =
            "rowweave.stream.multi-input-gate.v1";
    private static final int BROADCAST_DIGEST_MAGIC = 0x41424447;
    private static final int BROADCAST_DIGEST_VERSION = 1;

    private final StreamNodeTaskContext context;
    private final StreamChannelInput input;
    private final List<StreamChannelInput> inputs;
    private final StreamMultiInputGate multiInputGate;
    private final List<Route> routes;
    private final StreamKeyedProcessFunction function;
    private final StreamKeyedCoProcessFunction coProcessFunction;
    private final StreamKeyedBroadcastProcessFunction broadcastFunction;
    private final ProcessContract contract;
    private final FileCheckpointPayloadStore checkpointPayloadStore;
    private final List<StreamCheckpointMember> restoreMembers;
    private final int subtaskIndex;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();
    private volatile CheckpointIdentity lastCheckpoint;
    private volatile List<StreamCheckpointMember> lastCheckpointMembers =
            Collections.emptyList();
    private volatile List<StreamCheckpointMember> terminalMembers;
    private volatile BroadcastSlot activeBroadcast;
    private volatile Throwable failure;
    private volatile boolean completed;
    private volatile MemoryManager managedMemory;
    private volatile boolean resourceUsageReady;
    private volatile List<TimerSlot> activeTimers = Collections.emptyList();
    private volatile int maximumObservedTimerCount;
    private long watermark = Long.MIN_VALUE;
    private long processingTimeFloor = Long.MIN_VALUE;

    StreamKeyedProcessOperatorTask(String taskName,
            StreamNodeTaskContext context, StreamChannelInput input,
            Map<String, StreamChannelOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        this(taskName, context, Collections.singletonList(input), outputs,
                resolver, checkpointPayloadStore, restoreMembers,
                subtaskIndex, keyGroupStart, keyGroupEnd);
    }

    StreamKeyedProcessOperatorTask(String taskName,
            StreamNodeTaskContext context,
            List<? extends StreamChannelInput> inputs,
            Map<String, StreamChannelOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        this(taskName, context, inputs, sinkOutputs(context, outputs), resolver,
                checkpointPayloadStore, restoreMembers, subtaskIndex,
                keyGroupStart, keyGroupEnd);
    }

    static StreamKeyedProcessOperatorTask composed(String taskName,
            StreamNodeTaskContext context,
            List<? extends StreamChannelInput> inputs,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        return new StreamKeyedProcessOperatorTask(taskName, context, inputs,
                outputs, resolver, checkpointPayloadStore, restoreMembers,
                subtaskIndex, keyGroupStart, keyGroupEnd);
    }

    private StreamKeyedProcessOperatorTask(String taskName,
            StreamNodeTaskContext context,
            List<? extends StreamChannelInput> inputs,
            List<StreamTaskOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        if (taskName == null || taskName.isEmpty() || context == null
                || inputs == null || inputs.isEmpty()
                || outputs == null || outputs.isEmpty() || resolver == null
                || restoreMembers == null
                || subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= context.maximumParallelism()
                || context.node().opcode()
                != StreamOperatorCode.KEYED_PROCESS
                && context.node().opcode()
                != StreamOperatorCode.KEYED_CO_PROCESS) {
            throw new IllegalArgumentException(
                    "Stream keyed process task runtime is incomplete");
        }
        this.context = context;
        List<StreamChannelInput> copiedInputs = new ArrayList<>(inputs.size());
        for (StreamChannelInput current : inputs) {
            if (current == null) {
                throw new IllegalArgumentException(
                        "Stream keyed process input is missing");
            }
            copiedInputs.add(current);
        }
        this.inputs = Collections.unmodifiableList(copiedInputs);
        this.input = copiedInputs.get(0);
        this.checkpointPayloadStore = checkpointPayloadStore;
        this.restoreMembers = Collections.unmodifiableList(
                new ArrayList<>(restoreMembers));
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.contract = ProcessContract.from(context);
        if (contract.inputCount() != copiedInputs.size()) {
            throw new IllegalArgumentException(
                    "Stream keyed process input count differs from its plan");
        }
        if (copiedInputs.size() == 1) {
            this.multiInputGate = null;
        } else {
            this.multiInputGate = new StreamMultiInputGate(copiedInputs);
        }
        Object resolved = resolver.resolve(
                contract.binding.deploymentReference());
        if ((contract.broadcast
                && !(resolved instanceof
                StreamKeyedBroadcastProcessFunction))
                || (contract.coProcess && !contract.broadcast
                && !(resolved instanceof StreamKeyedCoProcessFunction))
                || (!contract.coProcess
                && !(resolved instanceof StreamKeyedProcessFunction))) {
            throw new IllegalArgumentException(
                    "resolved Stream keyed process ABI differs");
        }
        this.function = contract.coProcess ? null
                : (StreamKeyedProcessFunction) resolved;
        this.coProcessFunction = contract.coProcess && !contract.broadcast
                ? (StreamKeyedCoProcessFunction) resolved : null;
        this.broadcastFunction = contract.broadcast
                ? (StreamKeyedBroadcastProcessFunction) resolved : null;
        List<Route> copied = new ArrayList<>();
        for (StreamTaskOutput output : outputs) {
            OutputContract port = contract.output(output.outputTagId);
            copied.add(new Route(output.output, port));
        }
        this.routes = Collections.unmodifiableList(copied);
        this.thread = new Thread(this::run,
                "rowweave-stream-keyed-process-" + taskName);
    }

    private static List<StreamTaskOutput> sinkOutputs(
            StreamNodeTaskContext context,
            Map<String, StreamChannelOutput> outputs) {
        if (context == null || outputs == null
                || outputs.size() != context.outputs().size()) {
            throw new IllegalArgumentException(
                    "Stream keyed process Sink routes are incomplete");
        }
        List<StreamTaskOutput> routes = new ArrayList<>();
        for (StreamNodeTaskContext.Sink route : context.outputs()) {
            StreamChannelOutput output = outputs.get(
                    route.consumer().operatorUid());
            if (output == null) {
                throw new IllegalArgumentException(
                        "Stream keyed process Sink route is missing");
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
        return triggerCheckpoint(checkpoint);
    }

    synchronized CompletableFuture<List<StreamCheckpointMember>> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint is required");
        }
        CheckpointIdentity completedCheckpoint = lastCheckpoint;
        if (checkpoint.equals(completedCheckpoint)) {
            return CompletableFuture.completedFuture(lastCheckpointMembers);
        }
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (!sameAttempt(checkpoint)) {
            request.reject(new IllegalArgumentException(
                    "checkpoint does not belong to the keyed process task"));
            return request.result;
        }
        if (completedCheckpoint != null
                && checkpoint.checkpointSequence()
                <= completedCheckpoint.checkpointSequence()) {
            request.reject(new IllegalArgumentException(
                    "keyed process checkpoint sequence was already observed"));
            return request.result;
        }
        if (terminalMembers != null && !cancellationRequested.get()) {
            lastCheckpointMembers = terminalMembers;
            lastCheckpoint = checkpoint;
            return CompletableFuture.completedFuture(terminalMembers);
        }
        if (completed || cancellationRequested.get()) {
            request.reject(new IllegalStateException(
                    "keyed process task is already complete"));
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "keyed process task already has a checkpoint in flight"));
            return request.result;
        }
        CheckpointIdentity observed = lastCheckpoint;
        if (observed != completedCheckpoint
                && pendingCheckpoint.compareAndSet(request, null)) {
            if (observed.equals(checkpoint)) {
                request.result.complete(lastCheckpointMembers);
            } else {
                request.reject(new IllegalArgumentException(
                        "keyed process checkpoint barrier differs from the trigger"));
            }
        }
        return request.result;
    }

    @Override public void cancel() {
        if (cancellationRequested.compareAndSet(false, true)) {
            for (StreamChannelInput current : inputs) current.cancel();
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
                memory.peakReservedBytes(), maximumObservedTimerCount, 0);
    }

    private void run() {
        Path spillRoot = null;
        List<StateSlot> states = Collections.emptyList();
        List<TimerSlot> timers = Collections.emptyList();
        BroadcastSlot broadcast = null;
        try {
            spillRoot = Files.createTempDirectory("rowweave-stream-process-");
            int backendCount = Math.addExact(
                    contract.binding.managedStates().size(),
                    contract.binding.timerFamilies().size());
            if (contract.broadcast) backendCount = Math.addExact(
                    backendCount, 1);
            MemoryManager memory = new MemoryManager(
                    context.resources().managedStateBytes());
            managedMemory = memory;
            states = openStates(spillRoot, memory, backendCount);
            timers = openTimers(spillRoot, memory, backendCount);
            activeTimers = timers;
            broadcast = openBroadcast(spillRoot, memory, backendCount);
            activeBroadcast = broadcast;
            restore(states, timers, broadcast);
            observeTimerCount();
            resourceUsageReady = true;
            Map<String, StateSlot> statesByName = stateMap(states);
            Map<String, TimerSlot> timersByName = timerMap(timers);
            boolean hasProcessingTimers = hasProcessingTimers(timers);
            if (contract.immutableBroadcast()
                    && !multiInputGate.inputEnded(1)) {
                bootstrapBroadcast(statesByName, timersByName, broadcast);
            }
            while (!cancellationRequested.get()) {
                InputEvent next = receiveNext(statesByName, timers,
                        timersByName, hasProcessingTimers);
                if (next == null) {
                    if (hasProcessingTimers) continue;
                    return;
                }
                StreamEnvelope envelope = next.envelope;
                switch (envelope.kind()) {
                    case RECORD:
                        process(statesByName, timersByName,
                                broadcast, next.inputIndex, envelope);
                        break;
                    case WATERMARK:
                        advanceWatermark(states, statesByName, timers,
                                timersByName, envelope.timestamp());
                        broadcast(envelope);
                        break;
                    case END_OF_INPUT:
                        advanceWatermark(states, statesByName, timers,
                                timersByName, Long.MAX_VALUE);
                        publishTerminalMembers(states, timers, broadcast);
                        broadcast(envelope);
                        return;
                    case WATERMARK_STATUS:
                    case ASYNC_COMPLETION:
                        broadcast(envelope);
                        break;
                    case CHECKPOINT_BARRIER:
                        observeCheckpoint(states, timers, broadcast,
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
                                "unsupported keyed process event");
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
                        "keyed process task ended before checkpoint barrier"));
            }
            closeStates(states);
            closeTimers(timers);
            closeBroadcast(broadcast);
            activeBroadcast = null;
            if (spillRoot != null) {
                try {
                    Files.deleteIfExists(spillRoot);
                } catch (Throwable cleanupFailure) {
                    recordCleanupFailure(cleanupFailure);
                }
            }
            completed = true;
        }
    }

    private InputEvent receiveNext(Map<String, StateSlot> states,
            List<TimerSlot> timers, Map<String, TimerSlot> timersByName,
            boolean hasProcessingTimers) throws InterruptedException {
        if (!hasProcessingTimers) {
            if (multiInputGate == null) {
                StreamEnvelope envelope = input.receive();
                return envelope == null ? null
                        : new InputEvent(0, envelope);
            }
            return event(multiInputGate.take());
        }
        long now = processingTime();
        fireDueTimers(states, timers, timersByName,
                StreamTimerDescriptor.TimeDomain.PROCESSING_TIME, now);
        long waitMillis = processingTimerWaitMillis(timers, now);
        if (multiInputGate == null) {
            StreamEnvelope envelope = input.poll(waitMillis);
            return envelope == null ? null : new InputEvent(0, envelope);
        }
        return event(multiInputGate.poll(waitMillis));
    }

    private static InputEvent event(StreamMultiInputGate.Event event) {
        return event == null ? null
                : new InputEvent(event.inputIndex(), event.envelope());
    }

    private static boolean hasProcessingTimers(List<TimerSlot> timers) {
        for (TimerSlot timer : timers) {
            if (timer.descriptor.timeDomain()
                    == StreamTimerDescriptor.TimeDomain.PROCESSING_TIME) {
                return true;
            }
        }
        return false;
    }

    private void observeTimerCount() {
        int current = 0;
        for (TimerSlot timer : activeTimers) {
            current = Math.addExact(current, timer.currentCount);
        }
        maximumObservedTimerCount = Math.max(
                maximumObservedTimerCount, current);
    }

    private long processingTimerWaitMillis(List<TimerSlot> timers, long now) {
        long earliest = Long.MAX_VALUE;
        for (TimerSlot timer : timers) {
            if (timer.descriptor.timeDomain()
                    != StreamTimerDescriptor.TimeDomain.PROCESSING_TIME) {
                continue;
            }
            try (ManagedKeyedValueState.EntryCursor cursor =
                         timer.backend.entries()) {
                while (cursor.advance()) {
                    byte[] namespace = cursor.namespace();
                    if (namespace.length < Long.BYTES) {
                        throw processFailure(
                                StreamProcessException.Code
                                        .INVALID_STATE_VALUE,
                                "keyed timer namespace is corrupt");
                    }
                    earliest = Math.min(earliest, ByteBuffer.wrap(
                            namespace, 0, Long.BYTES).getLong());
                }
            }
        }
        if (earliest == Long.MAX_VALUE) return 1_000L;
        if (earliest <= now) return 1L;
        return Math.max(1L, Math.min(1_000L, earliest - now));
    }

    private List<StateSlot> openStates(Path spillRoot, MemoryManager memory,
            int backendCount) {
        if (contract.binding.managedStates().isEmpty()) {
            return Collections.emptyList();
        }
        StreamResourceProfile resources = context.resources();
        List<StateSlot> states = new ArrayList<>();
        long stateSpillBytes = Math.max(1L,
                resources.spillDiskBytes() / backendCount);
        for (StreamManagedStateDescriptor managed
                : contract.binding.managedStates()) {
            StreamStateDescriptor descriptor = managed.state();
            int valueBytes = (int) Math.min(Integer.MAX_VALUE,
                    managed.maximumEntryBytes());
            ManagedKeyedValueState backend = new ManagedKeyedValueState(
                    context.artifactId(),
                    contract.node.operatorUid(), descriptor,
                    context.maximumParallelism(), keyGroupStart, keyGroupEnd,
                    memory, spillRoot, stateSpillBytes,
                    context.resources().maximumFrameBytes(), valueBytes,
                    this::processingTime);
            states.add(new StateSlot(managed, backend));
        }
        return Collections.unmodifiableList(states);
    }

    private List<TimerSlot> openTimers(Path spillRoot, MemoryManager memory,
            int backendCount) {
        if (contract.binding.timerFamilies().isEmpty()) {
            return Collections.emptyList();
        }
        StreamResourceProfile resources = context.resources();
        List<TimerSlot> timers = new ArrayList<>();
        long spillBytes = Math.max(1L,
                resources.spillDiskBytes() / backendCount);
        for (StreamTimerDescriptor timer : contract.binding.timerFamilies()) {
            StreamStateDescriptor state = timerStateDescriptor(timer);
            ManagedKeyedValueState backend = new ManagedKeyedValueState(
                    context.artifactId(),
                    contract.node.operatorUid(), state,
                    context.maximumParallelism(), keyGroupStart, keyGroupEnd,
                    memory, spillRoot, spillBytes,
                    context.resources().maximumFrameBytes(),
                    TIMER_VALUE.length, this::processingTime);
            timers.add(new TimerSlot(timer, backend));
        }
        return Collections.unmodifiableList(timers);
    }

    private BroadcastSlot openBroadcast(Path spillRoot, MemoryManager memory,
            int backendCount) {
        if (!contract.broadcast) return null;
        StreamBroadcastStateDescriptor descriptor =
                contract.broadcastState;
        StreamStateDescriptor state = broadcastStateDescriptor(descriptor);
        long spillBytes = Math.max(1L,
                context.resources().spillDiskBytes() / backendCount);
        ManagedKeyedValueState backend = new ManagedKeyedValueState(
                context.artifactId(), contract.node.operatorUid(),
                state, context.maximumParallelism(), keyGroupStart,
                keyGroupEnd, memory, spillRoot, spillBytes,
                context.resources().maximumFrameBytes(),
                (int) Math.min(Integer.MAX_VALUE,
                        descriptor.maximumUpdateBytes()),
                this::processingTime);
        return new BroadcastSlot(descriptor, state, backend,
                broadcastCanonicalKey());
    }

    private byte[] broadcastCanonicalKey() {
        for (int candidate = 0; candidate < Integer.MAX_VALUE; candidate++) {
            byte[] key = ByteBuffer.allocate(Integer.BYTES)
                    .putInt(candidate).array();
            int keyGroup = ManagedKeyedValueState.keyGroup(
                    key, context.maximumParallelism());
            if (keyGroup >= keyGroupStart && keyGroup <= keyGroupEnd) {
                return key;
            }
        }
        throw new IllegalStateException(
                "broadcast state has no owned canonical key group");
    }

    private void restore(List<StateSlot> states, List<TimerSlot> timers,
            BroadcastSlot broadcast) {
        if (restoreMembers.isEmpty()) return;
        if (checkpointPayloadStore == null
                || restoreMembers.size()
                != Math.addExact(Math.addExact(states.size(), timers.size()),
                Math.addExact(broadcast == null ? 0 : 2,
                        multiInputGate == null ? 0 : 1))) {
            throw new IllegalArgumentException(
                    "keyed process restore state set is incomplete");
        }
        Map<String, StreamCheckpointMember> members = new HashMap<>();
        for (StreamCheckpointMember member : restoreMembers) {
            if (members.put(member.subjectId(), member) != null) {
                throw new IllegalArgumentException(
                        "keyed process restore contains duplicate state");
            }
        }
        for (StateSlot state : states) {
            StreamCheckpointMember member = members.remove(
                    state.descriptor.state().stateName());
            if (member == null) {
                throw new IllegalArgumentException(
                        "keyed process restore state is missing");
            }
            state.backend.restore(subtaskIndex, member,
                    checkpointPayloadStore);
        }
        for (TimerSlot timer : timers) {
            StreamCheckpointMember member = members.remove(
                    timerStateName(timer.descriptor.timerFamilyId()));
            if (member == null) {
                throw new IllegalArgumentException(
                        "keyed process restore timer is missing");
            }
            timer.backend.restore(subtaskIndex, member,
                    checkpointPayloadStore);
            timer.observeRestored();
        }
        if (broadcast != null) {
            StreamCheckpointMember member = members.remove(
                    broadcast.state.stateName());
            if (member == null) {
                throw new IllegalArgumentException(
                        "keyed process restore broadcast state is missing");
            }
            broadcast.backend.restore(subtaskIndex, member,
                    checkpointPayloadStore);
            StreamCheckpointMember digest = members.remove(
                    broadcastDigestStateName(broadcast));
            if (digest == null) {
                throw new IllegalArgumentException(
                        "keyed process restore broadcast digest is missing");
            }
            restoreBroadcastDigest(broadcast, digest);
        }
        if (multiInputGate != null) {
            StreamCheckpointMember member = members.remove(
                    MULTI_INPUT_GATE_STATE);
            if (member == null) {
                throw new IllegalArgumentException(
                        "keyed process restore input gate state is missing");
            }
            restoreGate(member);
        }
        if (!members.isEmpty()) {
            throw new IllegalArgumentException(
                    "keyed process restore contains unknown state");
        }
    }

    private StreamCheckpointMember snapshotGate() {
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile("rowweave-stream-gate-", ".snapshot");
            Files.write(staged, multiInputGate.snapshotState());
            FileCheckpointPayloadStore.StoredPayload stored =
                    checkpointPayloadStore.store(staged);
            return StreamCheckpointMember.stateHandle(
                    contract.node.operatorUid(), subtaskIndex,
                    keyGroupStart, keyGroupEnd, MULTI_INPUT_GATE_STATE,
                    MULTI_INPUT_GATE_CODEC, 1,
                    stored.digest(), stored.contentBytes());
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (java.io.IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "multi-input gate snapshot staging failed", failure);
        } finally {
            deleteGateStaging(staged, primary);
        }
    }

    private StreamCheckpointMember snapshotBroadcastDigest(
            BroadcastSlot broadcast) {
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-broadcast-digest-", ".snapshot");
            Files.write(staged, broadcast.logicalDigest());
            FileCheckpointPayloadStore.StoredPayload stored =
                    checkpointPayloadStore.store(staged);
            return StreamCheckpointMember.stateHandle(
                    contract.node.operatorUid(), subtaskIndex,
                    keyGroupStart, keyGroupEnd,
                    broadcastDigestStateName(broadcast),
                    BROADCAST_DIGEST_CODEC, 1,
                    stored.digest(), stored.contentBytes());
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (java.io.IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "broadcast digest staging failed", failure);
        } finally {
            deleteGateStaging(staged, primary);
        }
    }

    private void restoreBroadcastDigest(BroadcastSlot broadcast,
            StreamCheckpointMember member) {
        if (member.kind() != StreamCheckpointMember.Kind.STATE
                || !contract.node.operatorUid().equals(member.operatorUid())
                || member.subtaskIndex() != subtaskIndex
                || member.keyGroupStart() != keyGroupStart
                || member.keyGroupEnd() != keyGroupEnd
                || !broadcastDigestStateName(broadcast).equals(
                        member.subjectId())
                || !BROADCAST_DIGEST_CODEC.equals(member.codecId())
                || member.schemaVersion() != 1) {
            throw new IllegalArgumentException(
                    "broadcast digest restore member is incompatible");
        }
        Path restored = null;
        Throwable primary = null;
        try {
            restored = Files.createTempFile(
                    "rowweave-stream-broadcast-digest-", ".restore");
            checkpointPayloadStore.restore(
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes()),
                    restored);
            if (!Arrays.equals(Files.readAllBytes(restored),
                    broadcast.logicalDigest())) {
                throw new IllegalArgumentException(
                        "broadcast logical digest differs after restore");
            }
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (java.io.IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "broadcast digest restore staging failed", failure);
        } finally {
            deleteGateStaging(restored, primary);
        }
    }

    private static String broadcastDigestStateName(BroadcastSlot broadcast) {
        return BROADCAST_DIGEST_PREFIX + broadcast.descriptor.stateName();
    }

    private void restoreGate(StreamCheckpointMember member) {
        if (member.kind() != StreamCheckpointMember.Kind.STATE
                || !contract.node.operatorUid().equals(member.operatorUid())
                || member.subtaskIndex() != subtaskIndex
                || member.keyGroupStart() != keyGroupStart
                || member.keyGroupEnd() != keyGroupEnd
                || !MULTI_INPUT_GATE_STATE.equals(member.subjectId())
                || !MULTI_INPUT_GATE_CODEC.equals(member.codecId())
                || member.schemaVersion() != 1) {
            throw new IllegalArgumentException(
                    "multi-input gate restore member is incompatible");
        }
        Path restored = null;
        Throwable primary = null;
        try {
            restored = Files.createTempFile(
                    "rowweave-stream-gate-", ".restore");
            checkpointPayloadStore.restore(
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes()),
                    restored);
            multiInputGate.restoreState(Files.readAllBytes(restored));
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (java.io.IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "multi-input gate restore staging failed", failure);
        } finally {
            deleteGateStaging(restored, primary);
        }
    }

    private static void deleteGateStaging(Path staged, Throwable primary) {
        if (staged == null) return;
        try {
            Files.deleteIfExists(staged);
        } catch (java.io.IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw new IllegalStateException(
                    "multi-input gate staging cleanup failed",
                    cleanupFailure);
        }
    }

    private void bootstrapBroadcast(Map<String, StateSlot> states,
            Map<String, TimerSlot> timers, BroadcastSlot broadcast)
            throws InterruptedException {
        StreamChannelInput right = inputs.get(1);
        while (!cancellationRequested.get()) {
            StreamEnvelope envelope = right.receive();
            if (envelope == null) {
                throw new IllegalStateException(
                        "immutable broadcast input closed before end-of-input");
            }
            switch (envelope.kind()) {
                case RECORD:
                    process(states, timers, broadcast, 1, envelope);
                    break;
                case WATERMARK:
                case WATERMARK_STATUS:
                case ASYNC_COMPLETION:
                    break;
                case END_OF_INPUT:
                    multiInputGate.markEnded(1, envelope);
                    return;
                case CANCEL:
                    cancel();
                    return;
                case ERROR:
                    throw new IllegalStateException(
                            "broadcast bootstrap reported an upstream error");
                case CHECKPOINT_BARRIER:
                    throw new IllegalStateException(
                            "checkpoint cannot overtake immutable broadcast bootstrap");
                default:
                    throw new UnsupportedOperationException(
                            "unsupported broadcast bootstrap event");
            }
        }
    }

    private void process(Map<String, StateSlot> states,
            Map<String, TimerSlot> timers, BroadcastSlot broadcast,
            int inputIndex,
            StreamEnvelope envelope) {
        DataRow row = contract.inputSerializer(inputIndex).deserialize(
                envelope.recordFrame());
        validate(row, contract.inputSchema(inputIndex));
        if (contract.broadcast && inputIndex == 1) {
            BroadcastCallbackContext context =
                    new BroadcastCallbackContext(broadcast,
                            envelope.hasEventTimestamp()
                                    ? Long.valueOf(envelope.timestamp())
                                    : null);
            CallbackCollector collector = new CallbackCollector(context,
                    envelope.hasEventTimestamp()
                            ? Long.valueOf(envelope.timestamp()) : null,
                    false, inputIndex);
            try {
                broadcastFunction.processBroadcast(row, context, collector);
            } finally {
                collector.close();
                context.close();
            }
            collector.requireHealthy();
            return;
        }
        byte[] key = contract.inputKeySerializer(inputIndex)
                .serializeProjected(row, contract.inputKeyIndices(inputIndex));
        DataRow keyRow = project(row, contract.inputKeyIndices(inputIndex));
        CallbackContext context = new CallbackContext(states, timers,
                broadcast, key, keyRow,
                envelope.hasEventTimestamp()
                        ? Long.valueOf(envelope.timestamp()) : null);
        CallbackCollector collector = new CallbackCollector(context,
                envelope.hasEventTimestamp()
                        ? Long.valueOf(envelope.timestamp()) : null,
                false, inputIndex);
        try {
            if (broadcastFunction != null) {
                broadcastFunction.processKeyed(row, context, collector);
            } else if (coProcessFunction == null) {
                function.processElement(row, context, collector);
            } else if (inputIndex == 0) {
                coProcessFunction.processLeft(row, context, collector);
            } else {
                coProcessFunction.processRight(row, context, collector);
            }
        } finally {
            collector.close();
            context.close();
        }
        collector.requireHealthy();
    }

    private void emit(OutputContract port, DataRow row,
            Long eventTimestamp) {
        validate(row, port.schema);
        byte[] frame = port.frame(row);
        boolean routed = false;
        for (Route route : routes) {
            if (Objects.equals(route.port.tagId, port.tagId)) {
                route.sendRecord(frame, eventTimestamp);
                routed = true;
            }
        }
        if (!routed) {
            throw processFailure(
                    StreamProcessException.Code.UNDECLARED_OUTPUT_TAG,
                    "keyed process output port is not consumed");
        }
    }

    private void advanceWatermark(List<StateSlot> states,
            Map<String, StateSlot> statesByName,
            List<TimerSlot> timers, Map<String, TimerSlot> timersByName,
            long next) {
        if (next < watermark) {
            throw new IllegalArgumentException(
                    "keyed process watermark regressed");
        }
        watermark = next;
        for (TimerSlot timer : timers) timer.backend.advanceWatermark(next);
        fireDueTimers(statesByName, timers, timersByName,
                StreamTimerDescriptor.TimeDomain.EVENT_TIME, next);
        for (StateSlot state : states) state.backend.advanceWatermark(next);
    }

    private void fireDueTimers(Map<String, StateSlot> states,
            List<TimerSlot> timers, Map<String, TimerSlot> timersByName,
            StreamTimerDescriptor.TimeDomain domain, long dueTime) {
        while (true) {
            TimerCandidate selected = null;
            for (TimerSlot timer : timers) {
                if (timer.descriptor.timeDomain() != domain) continue;
                try (ManagedKeyedValueState.EntryCursor cursor =
                             timer.backend.entries()) {
                    while (cursor.advance()) {
                        byte[] storedNamespace = cursor.namespace();
                        if (storedNamespace.length < Long.BYTES) {
                            throw processFailure(
                                    StreamProcessException.Code
                                            .INVALID_STATE_VALUE,
                                    "keyed timer namespace is corrupt");
                        }
                        long timestamp = ByteBuffer.wrap(
                                storedNamespace, 0, Long.BYTES).getLong();
                        if (timestamp > dueTime) continue;
                        TimerCandidate candidate = new TimerCandidate(timer,
                                cursor.canonicalKey(), storedNamespace,
                                timestamp);
                        if (selected == null
                                || candidate.compareTo(selected,
                                context.maximumParallelism()) < 0) {
                            selected = candidate;
                        }
                    }
                }
            }
            if (selected == null) return;
            selected.timer.deleteStored(
                    selected.key, selected.storedNamespace);
            observeTimerCount();
            DataRow keyRow = contract.inputKeySerializer(0).deserialize(
                    selected.key);
            TimerCallbackContext context = new TimerCallbackContext(
                    states, timersByName, activeBroadcast,
                    selected.key, keyRow, selected);
            Long outputTimestamp = domain
                    == StreamTimerDescriptor.TimeDomain.EVENT_TIME
                    ? Long.valueOf(selected.timestamp) : null;
            CallbackCollector collector = new CallbackCollector(
                    context, outputTimestamp, true, -1);
            try {
                if (broadcastFunction != null) {
                    broadcastFunction.onTimer(
                            selected.timestamp, context, collector);
                } else if (coProcessFunction == null) {
                    function.onTimer(selected.timestamp, context, collector);
                } else {
                    coProcessFunction.onTimer(
                            selected.timestamp, context, collector);
                }
            } finally {
                collector.close();
                context.close();
            }
            collector.requireHealthy();
        }
    }

    private long processingTime() {
        long now = System.currentTimeMillis();
        if (now > processingTimeFloor) processingTimeFloor = now;
        return processingTimeFloor;
    }

    private void observeCheckpoint(List<StateSlot> states,
            List<TimerSlot> timers, BroadcastSlot broadcast,
            CheckpointIdentity checkpoint) {
        if (checkpointPayloadStore == null || !sameAttempt(checkpoint)) {
            throw new IllegalArgumentException(
                    "keyed process checkpoint runtime or scope is invalid");
        }
        CheckpointIdentity previous = lastCheckpoint;
        if (previous != null && checkpoint.checkpointSequence()
                <= previous.checkpointSequence()) {
            throw new IllegalArgumentException(
                    "keyed process checkpoint sequence did not advance");
        }
        List<StreamCheckpointMember> members = snapshotMembers(
                states, timers, broadcast);
        lastCheckpointMembers = members;
        lastCheckpoint = checkpoint;
        CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
        if (pending != null) {
            if (pending.checkpoint.equals(checkpoint)) {
                pending.result.complete(lastCheckpointMembers);
            } else {
                pending.reject(new IllegalArgumentException(
                        "keyed process checkpoint barrier differs from the trigger"));
            }
        }
    }

    private List<StreamCheckpointMember> snapshotMembers(
            List<StateSlot> states, List<TimerSlot> timers,
            BroadcastSlot broadcast) {
        List<StreamCheckpointMember> members = new ArrayList<>(
                Math.addExact(Math.addExact(states.size(), timers.size()),
                        Math.addExact(broadcast == null ? 0 : 2,
                                multiInputGate == null ? 0 : 1)));
        for (StateSlot state : states) {
            members.add(state.backend.snapshot(
                    subtaskIndex, checkpointPayloadStore));
        }
        for (TimerSlot timer : timers) {
            members.add(timer.backend.snapshot(
                    subtaskIndex, checkpointPayloadStore));
        }
        if (broadcast != null) {
            members.add(broadcast.backend.snapshot(
                    subtaskIndex, checkpointPayloadStore));
            members.add(snapshotBroadcastDigest(broadcast));
        }
        if (multiInputGate != null) members.add(snapshotGate());
        return Collections.unmodifiableList(members);
    }

    private void publishTerminalMembers(List<StateSlot> states,
            List<TimerSlot> timers, BroadcastSlot broadcast) {
        if (checkpointPayloadStore == null) return;
        List<StreamCheckpointMember> members = snapshotMembers(
                states, timers, broadcast);
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
        for (StreamChannelInput current : inputs) {
            if (!current.channelIdentity().runId().equals(checkpoint.runId())
                    || !current.channelIdentity().attemptId().equals(
                    checkpoint.attemptId())
                    || current.channelIdentity().fenceToken()
                    != checkpoint.fenceToken()) return false;
        }
        return true;
    }

    private void broadcast(StreamEnvelope envelope) {
        for (Route route : routes) route.sendSignal(envelope);
    }

    private void fail(Throwable thrown) {
        failure = thrown;
        for (StreamChannelInput current : inputs) current.fail(thrown);
        for (Route route : routes) route.output.fail(thrown);
    }

    private void closeStates(List<StateSlot> states) {
        for (StateSlot state : states) {
            try {
                state.backend.close();
            } catch (Throwable closeFailure) {
                recordCleanupFailure(closeFailure);
            }
        }
    }

    private void closeTimers(List<TimerSlot> timers) {
        for (TimerSlot timer : timers) {
            try {
                timer.backend.close();
            } catch (Throwable closeFailure) {
                recordCleanupFailure(closeFailure);
            }
        }
    }

    private void closeBroadcast(BroadcastSlot broadcast) {
        if (broadcast == null) return;
        try {
            broadcast.backend.close();
        } catch (Throwable closeFailure) {
            recordCleanupFailure(closeFailure);
        }
    }

    private void recordCleanupFailure(Throwable cleanupFailure) {
        if (failure == null && !cancellationRequested.get()) {
            fail(cleanupFailure);
        } else if (failure != null) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private static Map<String, StateSlot> stateMap(List<StateSlot> states) {
        Map<String, StateSlot> result = new LinkedHashMap<>();
        for (StateSlot state : states) {
            result.put(state.descriptor.state().stateName(), state);
        }
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, TimerSlot> timerMap(List<TimerSlot> timers) {
        Map<String, TimerSlot> result = new LinkedHashMap<>();
        for (TimerSlot timer : timers) {
            result.put(timer.descriptor.timerFamilyId(), timer);
        }
        return Collections.unmodifiableMap(result);
    }

    private static DataRow project(DataRow row, int[] indices) {
        DataRow key = new DataRow(indices.length);
        for (int index = 0; index < indices.length; index++) {
            key.set(index, row.get(indices[index]));
        }
        return key;
    }

    private static void validate(DataRow row, PortableRowSchema schema) {
        if (row == null || row.size() != schema.fieldCount()) {
            throw new IllegalArgumentException(
                    "keyed process row width differs from its declared schema");
        }
        for (int index = 0; index < schema.fieldCount(); index++) {
            if (!schema.field(index).nullable() && row.get(index) == null) {
                throw new IllegalArgumentException(
                        "keyed process produced null for a non-null field");
            }
        }
    }

    private static StreamProcessException processFailure(
            StreamProcessException.Code code, String message) {
        return new StreamProcessException(code, message);
    }

    private interface CallbackScope {
        void requireActive();
    }

    private class CallbackContext
            implements StreamKeyedBroadcastProcessContext, CallbackScope {
        private final Map<String, StateSlot> states;
        private final Map<String, TimerSlot> timers;
        private final BroadcastSlot broadcast;
        private final byte[] key;
        private final DataRow keyRow;
        private final Long eventTimestamp;
        private final long callbackProcessingTime;
        private final Thread callbackThread = Thread.currentThread();
        private final Map<String, CursorAccess> cursors =
                new LinkedHashMap<>();
        private boolean active = true;

        private CallbackContext(Map<String, StateSlot> states,
                Map<String, TimerSlot> timers,
                BroadcastSlot broadcast,
                byte[] key, DataRow keyRow, Long eventTimestamp) {
            this.states = states;
            this.timers = timers;
            this.broadcast = broadcast;
            this.key = key.clone();
            this.keyRow = keyRow.copy();
            this.eventTimestamp = eventTimestamp;
            this.callbackProcessingTime = processingTime();
        }

        @Override public DataRow currentKey() {
            checkActive();
            return keyRow.copy();
        }

        @Override public boolean hasRecordTimestamp() {
            checkActive();
            return eventTimestamp != null;
        }

        @Override public long recordTimestamp() {
            checkActive();
            if (eventTimestamp == null) {
                throw processFailure(
                        StreamProcessException.Code.MISSING_RECORD_TIMESTAMP,
                        "record has no event timestamp");
            }
            return eventTimestamp;
        }

        @Override public long currentWatermark() {
            checkActive();
            return watermark;
        }

        @Override public long currentProcessingTime() {
            checkActive();
            return callbackProcessingTime;
        }

        @Override public StreamTimerService timerService() {
            checkActive();
            return new TimerAccess(this);
        }

        @Override public StreamValueState valueState(String stateName) {
            StateSlot state = state(stateName,
                    StreamStateDescriptor.Kind.KEYED_VALUE);
            return new ValueAccess(this, state);
        }

        @Override public StreamMapState mapState(String stateName) {
            return new MapAccess(this, state(stateName,
                    StreamStateDescriptor.Kind.KEYED_MAP));
        }

        @Override public StreamListState listState(String stateName) {
            return new ListAccess(this, state(stateName,
                    StreamStateDescriptor.Kind.KEYED_LIST));
        }

        @Override public StreamAggregatingState aggregatingState(
                String stateName) {
            return new AggregatingAccess(this, state(stateName,
                    StreamStateDescriptor.Kind.KEYED_AGGREGATING));
        }

        @Override public StreamReadOnlyBroadcastState broadcastState(
                String stateName) {
            checkActive();
            requireBroadcastName(broadcast, stateName);
            return new ReadOnlyBroadcastAccess(this, broadcast);
        }

        private StateSlot state(String name,
                StreamStateDescriptor.Kind expected) {
            checkActive();
            StateSlot state = states.get(name);
            if (state == null) {
                throw processFailure(
                        StreamProcessException.Code.UNDECLARED_STATE,
                        "keyed process accessed undeclared state: " + name);
            }
            if (state.descriptor.state().kind() != expected) {
                throw processFailure(
                        StreamProcessException.Code.STATE_KIND_MISMATCH,
                        "keyed process state kind differs: " + name);
            }
            return state;
        }

        private long stateEventTime() {
            if (eventTimestamp != null) return eventTimestamp;
            return Long.MIN_VALUE;
        }

        final void checkActive() {
            if (!active || Thread.currentThread() != callbackThread) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "keyed process context escaped its mailbox callback");
            }
        }

        @Override public void requireActive() { checkActive(); }

        private CursorAccess cursor(StateSlot state, boolean keyed) {
            checkActive();
            String name = state.descriptor.state().stateName();
            CursorAccess existing = cursors.get(name);
            if (existing != null && !existing.closed) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "only one state cursor may be open per callback state");
            }
            CursorAccess cursor = new CursorAccess(this, state, keyed);
            cursors.put(name, cursor);
            return cursor;
        }

        private void closed(CursorAccess cursor) {
            cursors.remove(cursor.state.descriptor.state().stateName());
        }

        void close() {
            for (CursorAccess cursor
                    : new ArrayList<>(cursors.values())) {
                cursor.close();
            }
            active = false;
        }
    }

    private final class BroadcastCallbackContext
            implements StreamBroadcastProcessContext, CallbackScope {
        private final BroadcastSlot broadcast;
        private final Long eventTimestamp;
        private final long callbackProcessingTime = processingTime();
        private final Thread callbackThread = Thread.currentThread();
        private final List<BroadcastCursor> cursors = new ArrayList<>();
        private boolean active = true;

        private BroadcastCallbackContext(BroadcastSlot broadcast,
                Long eventTimestamp) {
            this.broadcast = broadcast;
            this.eventTimestamp = eventTimestamp;
        }

        @Override public boolean hasRecordTimestamp() {
            requireActive();
            return eventTimestamp != null;
        }

        @Override public long recordTimestamp() {
            requireActive();
            if (eventTimestamp == null) {
                throw processFailure(
                        StreamProcessException.Code.MISSING_RECORD_TIMESTAMP,
                        "broadcast record has no event timestamp");
            }
            return eventTimestamp;
        }

        @Override public long currentWatermark() {
            requireActive();
            return watermark;
        }

        @Override public long currentProcessingTime() {
            requireActive();
            return callbackProcessingTime;
        }

        @Override public StreamBroadcastState broadcastState(
                String stateName) {
            requireActive();
            requireBroadcastName(broadcast, stateName);
            return new MutableBroadcastAccess(this, broadcast);
        }

        @Override public void requireActive() {
            if (!active || Thread.currentThread() != callbackThread) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "broadcast context escaped its mailbox callback");
            }
        }

        private void register(BroadcastCursor cursor) {
            requireActive();
            cursors.add(cursor);
        }

        private void close() {
            for (BroadcastCursor cursor : new ArrayList<>(cursors)) {
                cursor.close();
            }
            active = false;
        }
    }

    private final class TimerCallbackContext extends CallbackContext
            implements StreamBroadcastTimerContext {
        private final TimerCandidate timer;

        private TimerCallbackContext(Map<String, StateSlot> states,
                Map<String, TimerSlot> timers, BroadcastSlot broadcast,
                byte[] key, DataRow keyRow,
                TimerCandidate timer) {
            super(states, timers, broadcast, key, keyRow, null);
            this.timer = timer;
        }

        @Override public String timerFamilyId() {
            checkActive();
            return timer.timer.descriptor.timerFamilyId();
        }

        @Override public byte[] timerNamespace() {
            checkActive();
            return decodeTimerNamespace(timer.storedNamespace);
        }

        @Override public StreamTimerSpec.TimeDomain timeDomain() {
            checkActive();
            return timer.timer.descriptor.timeDomain()
                    == StreamTimerDescriptor.TimeDomain.EVENT_TIME
                    ? StreamTimerSpec.TimeDomain.EVENT_TIME
                    : StreamTimerSpec.TimeDomain.PROCESSING_TIME;
        }

        @Override public long timerTimestamp() {
            checkActive();
            return timer.timestamp;
        }
    }

    private static final class ValueAccess implements StreamValueState {
        private final CallbackContext context;
        private final StateSlot state;

        private ValueAccess(CallbackContext context, StateSlot state) {
            this.context = context;
            this.state = state;
        }

        @Override public byte[] value() {
            context.checkActive();
            return state.get(context.key, VALUE_NAMESPACE);
        }

        @Override public void update(byte[] value) {
            context.checkActive();
            if (value == null) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "managed state replacement must not be null");
            }
            if (state.descriptor.state().ttlMode()
                    == StreamStateDescriptor.TtlMode.EVENT_TIME
                    && context.eventTimestamp == null) {
                throw processFailure(
                        StreamProcessException.Code.MISSING_RECORD_TIMESTAMP,
                        "event-time state update requires a record timestamp");
            }
            state.put(context.key, VALUE_NAMESPACE, value,
                    context.stateEventTime());
        }

        @Override public void clear() {
            context.checkActive();
            state.remove(context.key, VALUE_NAMESPACE);
        }
    }

    private static final class AggregatingAccess
            implements StreamAggregatingState {
        private final ValueAccess delegate;

        private AggregatingAccess(CallbackContext context, StateSlot state) {
            this.delegate = new ValueAccess(context, state);
        }

        @Override public byte[] value() { return delegate.value(); }
        @Override public void update(byte[] value) { delegate.update(value); }
        @Override public void clear() { delegate.clear(); }
    }

    private static final class MapAccess implements StreamMapState {
        private final CallbackContext context;
        private final StateSlot state;

        private MapAccess(CallbackContext context, StateSlot state) {
            this.context = context;
            this.state = state;
        }

        @Override public byte[] get(byte[] mapKey) {
            requireMapKey(mapKey);
            return state.get(context.key, mapKey);
        }

        @Override public boolean contains(byte[] mapKey) {
            return get(mapKey) != null;
        }

        @Override public void put(byte[] mapKey, byte[] value) {
            requireMapKey(mapKey);
            requireValue(value);
            state.put(context.key, mapKey, value,
                    context.stateEventTime());
        }

        @Override public void remove(byte[] mapKey) {
            requireMapKey(mapKey);
            state.remove(context.key, mapKey);
        }

        @Override public StreamStateCursor entries() {
            context.checkActive();
            return context.cursor(state, true);
        }

        @Override public void clear() {
            context.checkActive();
            state.clear(context.key);
        }

        private void requireMapKey(byte[] mapKey) {
            context.checkActive();
            if (mapKey == null) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "MapState key must not be null");
            }
        }

        private void requireValue(byte[] value) {
            if (value == null) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "MapState value must not be null");
            }
        }
    }

    private static final class ListAccess implements StreamListState {
        private final CallbackContext context;
        private final StateSlot state;

        private ListAccess(CallbackContext context, StateSlot state) {
            this.context = context;
            this.state = state;
        }

        @Override public void add(byte[] value) {
            context.checkActive();
            if (value == null) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "ListState value must not be null");
            }
            long next = 0L;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         state.backend.entries(context.key)) {
                while (cursor.advance()) {
                    byte[] namespace = cursor.namespace();
                    if (namespace.length != Long.BYTES) {
                        throw processFailure(
                                StreamProcessException.Code.INVALID_STATE_VALUE,
                                "ListState index is corrupt");
                    }
                    long index = java.nio.ByteBuffer.wrap(namespace).getLong();
                    if (index < 0L || index == Long.MAX_VALUE) {
                        throw processFailure(
                                StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                                "ListState index space is exhausted");
                    }
                    next = Math.max(next, index + 1L);
                }
            }
            state.put(context.key,
                    java.nio.ByteBuffer.allocate(Long.BYTES)
                            .putLong(next).array(),
                    value, context.stateEventTime());
        }

        @Override public StreamStateCursor values() {
            context.checkActive();
            return context.cursor(state, false);
        }

        @Override public void clear() {
            context.checkActive();
            state.clear(context.key);
        }
    }

    private static final class CursorAccess implements StreamStateCursor {
        private final CallbackContext context;
        private final StateSlot state;
        private final boolean keyed;
        private byte[] lastNamespace;
        private byte[] currentNamespace;
        private byte[] currentValue;
        private boolean closed;

        private CursorAccess(CallbackContext context,
                StateSlot state, boolean keyed) {
            this.context = context;
            this.state = state;
            this.keyed = keyed;
        }

        @Override public boolean advance() {
            checkOpen();
            byte[] candidateNamespace = null;
            byte[] candidateValue = null;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         state.backend.entries(context.key)) {
                while (cursor.advance()) {
                    byte[] namespace = cursor.namespace();
                    if (lastNamespace != null
                            && compare(namespace, lastNamespace) <= 0) {
                        continue;
                    }
                    if (candidateNamespace == null
                            || compare(namespace, candidateNamespace) < 0) {
                        candidateNamespace = namespace;
                        candidateValue = cursor.value();
                    }
                }
            }
            currentNamespace = candidateNamespace;
            currentValue = candidateValue;
            if (candidateNamespace == null) return false;
            lastNamespace = candidateNamespace.clone();
            return true;
        }

        @Override public boolean hasKey() {
            checkCurrent();
            return keyed;
        }

        @Override public byte[] key() {
            checkCurrent();
            return keyed ? currentNamespace.clone() : null;
        }

        @Override public byte[] value() {
            checkCurrent();
            return currentValue.clone();
        }

        @Override public void close() {
            if (closed) return;
            context.checkActive();
            closed = true;
            currentNamespace = null;
            currentValue = null;
            context.closed(this);
        }

        private void checkOpen() {
            context.checkActive();
            if (closed) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "managed state cursor is closed");
            }
        }

        private void checkCurrent() {
            checkOpen();
            if (currentNamespace == null) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "managed state cursor has no current value");
            }
        }
    }

    private static class ReadOnlyBroadcastAccess
            implements StreamReadOnlyBroadcastState {
        final CallbackScope context;
        final BroadcastSlot broadcast;

        private ReadOnlyBroadcastAccess(CallbackScope context,
                BroadcastSlot broadcast) {
            this.context = context;
            this.broadcast = broadcast;
        }

        @Override public byte[] get(byte[] key) {
            context.requireActive();
            return broadcast.get(key);
        }

        @Override public boolean contains(byte[] key) {
            return get(key) != null;
        }

        @Override public StreamStateCursor entries() {
            context.requireActive();
            BroadcastCursor cursor = new BroadcastCursor(
                    context, broadcast);
            if (context instanceof BroadcastCallbackContext) {
                ((BroadcastCallbackContext) context).register(cursor);
            }
            return cursor;
        }
    }

    private static final class MutableBroadcastAccess
            extends ReadOnlyBroadcastAccess implements StreamBroadcastState {
        private MutableBroadcastAccess(CallbackScope context,
                BroadcastSlot broadcast) {
            super(context, broadcast);
        }

        @Override public void put(byte[] key, byte[] value) {
            context.requireActive();
            broadcast.put(key, value);
        }

        @Override public void remove(byte[] key) {
            context.requireActive();
            broadcast.remove(key);
        }

        @Override public void clear() {
            context.requireActive();
            broadcast.clear();
        }
    }

    private static final class BroadcastCursor implements StreamStateCursor {
        private final CallbackScope context;
        private final BroadcastSlot broadcast;
        private byte[] lastKey;
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean closed;

        private BroadcastCursor(CallbackScope context,
                BroadcastSlot broadcast) {
            this.context = context;
            this.broadcast = broadcast;
        }

        @Override public boolean advance() {
            checkOpen();
            byte[] selectedKey = null;
            byte[] selectedValue = null;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         broadcast.backend.entries(
                                 broadcast.canonicalKey)) {
                while (cursor.advance()) {
                    byte[] key = cursor.namespace();
                    if (lastKey != null && compare(key, lastKey) <= 0) {
                        continue;
                    }
                    if (selectedKey == null
                            || compare(key, selectedKey) < 0) {
                        selectedKey = key;
                        selectedValue = cursor.value();
                    }
                }
            }
            currentKey = selectedKey;
            currentValue = selectedValue;
            if (selectedKey == null) return false;
            lastKey = selectedKey.clone();
            return true;
        }

        @Override public boolean hasKey() {
            checkCurrent();
            return true;
        }

        @Override public byte[] key() {
            checkCurrent();
            return currentKey.clone();
        }

        @Override public byte[] value() {
            checkCurrent();
            return currentValue.clone();
        }

        @Override public void close() {
            if (closed) return;
            context.requireActive();
            closed = true;
            currentKey = null;
            currentValue = null;
        }

        private void checkOpen() {
            context.requireActive();
            if (closed) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "broadcast state cursor is closed");
            }
        }

        private void checkCurrent() {
            checkOpen();
            if (currentKey == null) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "broadcast state cursor has no current value");
            }
        }
    }

    private final class TimerAccess implements StreamTimerService {
        private final CallbackContext context;

        private TimerAccess(CallbackContext context) {
            this.context = context;
        }

        @Override public void registerTimer(String timerFamilyId,
                byte[] namespace, long timestamp) {
            TimerSlot timer = timer(timerFamilyId, namespace);
            rejectReentrantDueTimer(timer, timestamp);
            timer.register(context.key, namespace, timestamp);
            observeTimerCount();
        }

        @Override public void deleteTimer(String timerFamilyId,
                byte[] namespace, long timestamp) {
            TimerSlot timer = timer(timerFamilyId, namespace);
            timer.delete(context.key, namespace, timestamp);
            observeTimerCount();
        }

        private TimerSlot timer(String timerFamilyId, byte[] namespace) {
            context.checkActive();
            if (timerFamilyId == null || namespace == null) {
                throw processFailure(
                        StreamProcessException.Code.UNDECLARED_TIMER,
                        "keyed process timer identity is invalid");
            }
            TimerSlot timer = context.timers.get(timerFamilyId);
            if (timer == null) {
                throw processFailure(
                        StreamProcessException.Code.UNDECLARED_TIMER,
                        "keyed process accessed undeclared timer: "
                                + timerFamilyId);
            }
            return timer;
        }

        private void rejectReentrantDueTimer(
                TimerSlot timer, long timestamp) {
            if (!(context instanceof TimerCallbackContext)) return;
            boolean due = timer.descriptor.timeDomain()
                    == StreamTimerDescriptor.TimeDomain.EVENT_TIME
                    ? timestamp <= watermark
                    : timestamp <= context.callbackProcessingTime;
            if (due) {
                throw processFailure(
                        StreamProcessException.Code
                                .TIMER_REENTRANCE_REJECTED,
                        "timer callback cannot register an already-due timer");
            }
        }
    }

    private final class CallbackCollector implements StreamCollector {
        private final CallbackScope context;
        private final Long eventTimestamp;
        private final boolean timerCallback;
        private final int inputIndex;
        private final Thread callbackThread = Thread.currentThread();
        private final Map<String, Counter> counters = new HashMap<>();
        private final AtomicReference<RuntimeException> poison =
                new AtomicReference<>();
        private boolean active = true;

        private CallbackCollector(CallbackScope context,
                Long eventTimestamp, boolean timerCallback,
                int inputIndex) {
            this.context = context;
            this.eventTimestamp = eventTimestamp;
            this.timerCallback = timerCallback;
            this.inputIndex = inputIndex;
        }

        @Override public synchronized void collect(DataRow row) {
            collect(null, row);
        }

        @Override public synchronized void collect(
                StreamOutputTag outputTag, DataRow row) {
            checkActive();
            RuntimeException prior = poison.get();
            if (prior != null) throw prior;
            try {
                String tagId = outputTag == null ? null : outputTag.tagId();
                OutputContract port = contract.output(tagId);
                validate(row, port.schema);
                Counter counter = counters.get(tagId);
                if (counter == null) {
                    counter = new Counter();
                    counters.put(tagId, counter);
                }
                int rows = Math.addExact(counter.rows, 1);
                long bytes = Math.addExact(counter.bytes,
                        port.serializer.serializedSize(row));
                int maximumRows = timerCallback
                        ? port.maximumTimerOutputs
                        : inputIndex == 1
                        ? port.maximumRightOutputs
                        : port.maximumElementOutputs;
                long maximumBytes = timerCallback
                        ? port.maximumTimerOutputBytes
                        : inputIndex == 1
                        ? port.maximumRightOutputBytes
                        : port.maximumElementOutputBytes;
                if (rows > maximumRows || bytes > maximumBytes) {
                    throw processFailure(
                            StreamProcessException.Code.OUTPUT_QUOTA_EXCEEDED,
                            "keyed process output quota exceeded");
                }
                counter.rows = rows;
                counter.bytes = bytes;
                emit(port, row, eventTimestamp);
            } catch (RuntimeException failed) {
                poison.compareAndSet(null, failed);
                throw failed;
            }
        }

        private void checkActive() {
            context.requireActive();
            if (!active || Thread.currentThread() != callbackThread) {
                throw processFailure(
                        StreamProcessException.Code.CALLBACK_SCOPE_VIOLATION,
                        "keyed process collector escaped its callback");
            }
        }

        private void close() { active = false; }

        private void requireHealthy() {
            RuntimeException failed = poison.get();
            if (failed != null) throw failed;
        }
    }

    private static final class Counter {
        int rows;
        long bytes;
    }

    private static final class InputEvent {
        final int inputIndex;
        final StreamEnvelope envelope;

        private InputEvent(int inputIndex, StreamEnvelope envelope) {
            this.inputIndex = inputIndex;
            this.envelope = envelope;
        }
    }

    private static final class BroadcastSlot {
        final StreamBroadcastStateDescriptor descriptor;
        final StreamStateDescriptor state;
        final ManagedKeyedValueState backend;
        final byte[] canonicalKey;

        private BroadcastSlot(StreamBroadcastStateDescriptor descriptor,
                StreamStateDescriptor state,
                ManagedKeyedValueState backend, byte[] canonicalKey) {
            this.descriptor = descriptor;
            this.state = state;
            this.backend = backend;
            this.canonicalKey = canonicalKey;
        }

        byte[] get(byte[] key) {
            requireKey(key);
            return backend.get(canonicalKey, key);
        }

        void put(byte[] key, byte[] value) {
            requireKey(key);
            if (value == null) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "broadcast state value must not be null");
            }
            long updateBytes;
            try {
                updateBytes = Math.addExact(key.length,
                        (long) value.length);
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "broadcast update byte count overflowed");
            }
            if (updateBytes > descriptor.maximumUpdateBytes()) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "broadcast update exceeds its byte bound");
            }
            byte[] previous = backend.get(canonicalKey, key);
            Usage usage = usage();
            long entries = usage.totalEntries
                    + (previous == null ? 1L : 0L);
            long previousBytes = previous == null ? 0L
                    : key.length + (long) previous.length;
            long totalBytes;
            try {
                totalBytes = Math.addExact(
                        Math.subtractExact(usage.totalBytes, previousBytes),
                        updateBytes);
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "broadcast state byte count overflowed");
            }
            if (entries > descriptor.maximumEntries()
                    || totalBytes > descriptor.maximumTotalBytes()) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "broadcast state exceeds its entries or byte bound");
            }
            backend.put(canonicalKey, key, value.clone(), Long.MIN_VALUE);
        }

        void remove(byte[] key) {
            requireKey(key);
            backend.remove(canonicalKey, key);
        }

        void clear() {
            while (true) {
                byte[] key = null;
                try (ManagedKeyedValueState.EntryCursor cursor =
                             backend.entries(canonicalKey)) {
                    if (cursor.advance()) key = cursor.namespace();
                }
                if (key == null) return;
                backend.remove(canonicalKey, key);
            }
        }

        byte[] logicalDigest() {
            byte[] xor = new byte[32];
            byte[] sum = new byte[32];
            long entries = 0L;
            long bytes = 0L;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         backend.entries(canonicalKey)) {
                while (cursor.advance()) {
                    byte[] key = cursor.namespace();
                    byte[] value = cursor.value();
                    MessageDigest entry = sha256();
                    updateLength(entry, key.length);
                    entry.update(key);
                    updateLength(entry, value.length);
                    entry.update(value);
                    byte[] digest = entry.digest();
                    addDigest(sum, digest);
                    for (int index = 0; index < xor.length; index++) {
                        xor[index] ^= digest[index];
                    }
                    entries = Math.addExact(entries, 1L);
                    bytes = Math.addExact(bytes,
                            key.length + (long) value.length);
                }
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "broadcast digest accounting overflowed");
            }
            MessageDigest contract = sha256();
            updateText(contract, descriptor.stateName());
            updateText(contract, descriptor.authorityKind().name());
            updateText(contract, descriptor.snapshotOrVersionId());
            updateText(contract, descriptor.authorityDigest());
            updateText(contract, descriptor.serializerContractId());
            updateLength(contract, descriptor.stateSchemaVersion());
            updateLong(contract, descriptor.maximumEntries());
            updateLong(contract, descriptor.maximumTotalBytes());
            updateLong(contract, descriptor.maximumUpdateBytes());
            updateLength(contract, descriptor.maximumRetainedVersions());
            return ByteBuffer.allocate(120)
                    .putInt(BROADCAST_DIGEST_MAGIC)
                    .putInt(BROADCAST_DIGEST_VERSION)
                    .put(contract.digest())
                    .putLong(entries)
                    .putLong(bytes)
                    .put(xor)
                    .put(sum)
                    .array();
        }

        private Usage usage() {
            long entries = 0L;
            long bytes = 0L;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         backend.entries(canonicalKey)) {
                while (cursor.advance()) {
                    entries = Math.addExact(entries, 1L);
                    bytes = Math.addExact(bytes,
                            cursor.namespace().length
                                    + (long) cursor.value().length);
                }
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "broadcast state usage accounting overflowed");
            }
            return new Usage(entries, bytes, 0);
        }

        private static void requireKey(byte[] key) {
            if (key == null || key.length == 0) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "broadcast state key must not be empty");
            }
        }
    }

    private static final class StateSlot {
        final StreamManagedStateDescriptor descriptor;
        final ManagedKeyedValueState backend;

        private StateSlot(StreamManagedStateDescriptor descriptor,
                ManagedKeyedValueState backend) {
            this.descriptor = descriptor;
            this.backend = backend;
        }

        byte[] get(byte[] key, byte[] namespace) {
            return backend.get(key, namespace);
        }

        void put(byte[] key, byte[] namespace, byte[] value,
                long eventTimestamp) {
            if (namespace == null || value == null) {
                throw processFailure(
                        StreamProcessException.Code.INVALID_STATE_VALUE,
                        "managed state key and value must not be null");
            }
            long entryBytes;
            try {
                entryBytes = Math.addExact(namespace.length,
                        (long) value.length);
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "managed state entry byte count overflowed");
            }
            if (entryBytes > descriptor.maximumEntryBytes()) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "managed state entry byte quota exceeded");
            }
            byte[] previous = backend.get(key, namespace);
            Usage usage = usage(key);
            long entries = usage.totalEntries
                    + (previous == null ? 1L : 0L);
            int perKey = usage.entriesForKey
                    + (previous == null ? 1 : 0);
            long oldBytes = previous == null ? 0L
                    : key.length + (long) namespace.length
                    + previous.length;
            long nextBytes;
            try {
                nextBytes = Math.addExact(
                        Math.subtractExact(usage.totalBytes, oldBytes),
                        key.length + entryBytes);
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "managed state total byte count overflowed");
            }
            if (perKey > descriptor.maximumEntriesPerKey()
                    || entries > descriptor.maximumTotalEntries()
                    || nextBytes > descriptor.maximumTotalBytes()) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "managed state entries or byte quota exceeded");
            }
            backend.put(key, namespace, value.clone(), eventTimestamp);
        }

        void remove(byte[] key, byte[] namespace) {
            backend.remove(key, namespace);
        }

        void clear(byte[] key) {
            while (true) {
                byte[] namespace = null;
                try (ManagedKeyedValueState.EntryCursor cursor =
                             backend.entries(key)) {
                    if (cursor.advance()) namespace = cursor.namespace();
                }
                if (namespace == null) return;
                backend.remove(key, namespace);
            }
        }

        private Usage usage(byte[] selectedKey) {
            long totalEntries = 0L;
            long totalBytes = 0L;
            int entriesForKey = 0;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         backend.entries()) {
                while (cursor.advance()) {
                    byte[] key = cursor.canonicalKey();
                    byte[] namespace = cursor.namespace();
                    byte[] value = cursor.value();
                    totalEntries = Math.addExact(totalEntries, 1L);
                    totalBytes = Math.addExact(totalBytes,
                            key.length + (long) namespace.length
                                    + value.length);
                    if (java.util.Arrays.equals(key, selectedKey)) {
                        entriesForKey = Math.addExact(entriesForKey, 1);
                    }
                }
            } catch (ArithmeticException overflow) {
                throw processFailure(
                        StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                        "managed state usage accounting overflowed");
            }
            return new Usage(totalEntries, totalBytes, entriesForKey);
        }
    }

    private static final class TimerSlot {
        final StreamTimerDescriptor descriptor;
        final ManagedKeyedValueState backend;
        private int currentCount;
        private int maximumObservedCount;

        private TimerSlot(StreamTimerDescriptor descriptor,
                ManagedKeyedValueState backend) {
            this.descriptor = descriptor;
            this.backend = backend;
        }

        void register(byte[] key, byte[] namespace, long timestamp) {
            byte[] storedNamespace = encodeTimerNamespace(
                    timestamp, namespace);
            if (backend.get(key, storedNamespace) != null) return;
            Usage usage = usage(key);
            long nextEntries;
            long nextBytes;
            try {
                nextEntries = Math.addExact(usage.totalEntries, 1L);
                nextBytes = Math.addExact(usage.totalBytes,
                        key.length + (long) storedNamespace.length
                                + TIMER_VALUE.length);
            } catch (ArithmeticException overflow) {
                throw timerQuota("keyed timer usage accounting overflowed");
            }
            if (usage.entriesForKey >= descriptor.maximumTimersPerKey()
                    || nextEntries > descriptor.maximumTotalTimers()
                    || nextBytes > descriptor.maximumTotalBytes()) {
                throw timerQuota(
                        "keyed timer count or byte quota exceeded");
            }
            backend.put(key, storedNamespace, TIMER_VALUE, Long.MIN_VALUE);
            currentCount = Math.toIntExact(nextEntries);
            maximumObservedCount = Math.max(
                    maximumObservedCount, currentCount);
        }

        void delete(byte[] key, byte[] namespace, long timestamp) {
            deleteStored(key, encodeTimerNamespace(timestamp, namespace));
        }

        void deleteStored(byte[] key, byte[] storedNamespace) {
            if (backend.get(key, storedNamespace) == null) return;
            backend.remove(key, storedNamespace);
            currentCount = Math.subtractExact(currentCount, 1);
        }

        void observeRestored() {
            currentCount = Math.toIntExact(usage(new byte[0]).totalEntries);
            maximumObservedCount = Math.max(
                    maximumObservedCount, currentCount);
        }

        private Usage usage(byte[] selectedKey) {
            long totalEntries = 0L;
            long totalBytes = 0L;
            int entriesForKey = 0;
            try (ManagedKeyedValueState.EntryCursor cursor =
                         backend.entries()) {
                while (cursor.advance()) {
                    byte[] key = cursor.canonicalKey();
                    byte[] namespace = cursor.namespace();
                    byte[] value = cursor.value();
                    totalEntries = Math.addExact(totalEntries, 1L);
                    totalBytes = Math.addExact(totalBytes,
                            key.length + (long) namespace.length
                                    + value.length);
                    if (java.util.Arrays.equals(key, selectedKey)) {
                        entriesForKey = Math.addExact(entriesForKey, 1);
                    }
                }
            } catch (ArithmeticException overflow) {
                throw timerQuota("keyed timer usage accounting overflowed");
            }
            return new Usage(totalEntries, totalBytes, entriesForKey);
        }

        private static StreamProcessException timerQuota(String message) {
            return processFailure(
                    StreamProcessException.Code.TIMER_QUOTA_EXCEEDED,
                    message);
        }
    }

    private static final class TimerCandidate {
        final TimerSlot timer;
        final byte[] key;
        final byte[] storedNamespace;
        final long timestamp;

        private TimerCandidate(TimerSlot timer, byte[] key,
                byte[] storedNamespace, long timestamp) {
            this.timer = timer;
            this.key = key;
            this.storedNamespace = storedNamespace;
            this.timestamp = timestamp;
        }

        int compareTo(TimerCandidate other, int maxParallelism) {
            int compared = Long.compare(timestamp, other.timestamp);
            if (compared != 0) return compared;
            compared = Integer.compare(
                    ManagedKeyedValueState.keyGroup(key, maxParallelism),
                    ManagedKeyedValueState.keyGroup(
                            other.key, maxParallelism));
            if (compared != 0) return compared;
            compared = compare(key, other.key);
            if (compared != 0) return compared;
            compared = timer.descriptor.timerFamilyId().compareTo(
                    other.timer.descriptor.timerFamilyId());
            if (compared != 0) return compared;
            return compare(decodeTimerNamespace(storedNamespace),
                    decodeTimerNamespace(other.storedNamespace));
        }
    }

    private static final class Usage {
        final long totalEntries;
        final long totalBytes;
        final int entriesForKey;

        private Usage(long totalEntries, long totalBytes,
                int entriesForKey) {
            this.totalEntries = totalEntries;
            this.totalBytes = totalBytes;
            this.entriesForKey = entriesForKey;
        }
    }

    private static final class ProcessContract {
        final PortableStreamNode node;
        final StreamProcessBinding binding;
        final boolean coProcess;
        final boolean broadcast;
        final StreamBroadcastStateDescriptor broadcastState;
        final List<PortableRowSchema> inputSchemas;
        final List<int[]> inputKeyIndices;
        final List<RowSerializer> inputSerializers;
        final List<RowSerializer> inputKeySerializers;
        final OutputContract mainOutput;
        final Map<String, OutputContract> taggedOutputs;

        private ProcessContract(StreamNodeTaskContext context) {
            this.node = context.node();
            this.binding = (StreamProcessBinding) node.binding();
            this.coProcess = node.opcode()
                    == StreamOperatorCode.KEYED_CO_PROCESS;
            this.broadcastState = binding.broadcastState();
            this.broadcast = broadcastState != null;
            List<PortableRowSchema> schemas = new ArrayList<>();
            List<int[]> keyIndices = new ArrayList<>();
            List<RowSerializer> serializers = new ArrayList<>();
            List<RowSerializer> keySerializers = new ArrayList<>();
            for (int inputIndex = 0; inputIndex < context.inputs().size();
                    inputIndex++) {
                StreamNodeTaskContext.Source inputContract =
                        context.inputs().get(inputIndex);
                PortableStreamNode input = inputContract.producer();
                if (inputContract.edge().targetInputIndex() != inputIndex
                        || !node.inputOperatorUids().get(inputIndex).equals(
                        input.operatorUid())) {
                    throw new UnsupportedOperationException(
                            "keyed process input contract is inconsistent");
                }
                boolean broadcastInput = broadcast && inputIndex == 1;
                if (!broadcastInput
                        && inputContract.stableKeyFields().isEmpty()) {
                    throw new UnsupportedOperationException(
                            "keyed process input has no stable key contract");
                }
                PortableRowSchema schema = inputContract.schema();
                int[] indices = broadcastInput ? new int[0]
                        : indices(schema, inputContract.stableKeyFields());
                schemas.add(schema);
                keyIndices.add(indices);
                serializers.add(serializer(schema));
                keySerializers.add(broadcastInput ? null
                        : serializer(keySchema(schema, indices)));
            }
            this.inputSchemas = Collections.unmodifiableList(schemas);
            this.inputKeyIndices = Collections.unmodifiableList(keyIndices);
            this.inputSerializers = Collections.unmodifiableList(serializers);
            this.inputKeySerializers = Collections.unmodifiableList(
                    keySerializers);
            this.mainOutput = new OutputContract(null, node.outputSchema(),
                    node.stableKeyFields(),
                    context.changelogMode()
                            == io.rowweave.stream.contract.PortableStreamPlan
                            .ChangelogMode.UPSERT,
                    binding.maximumElementOutputs(),
                    binding.maximumElementOutputBytes(),
                    binding.maximumRightOutputs(),
                    binding.maximumRightOutputBytes(),
                    binding.maximumTimerOutputs(),
                    binding.maximumTimerOutputBytes());
            Map<String, OutputContract> tags = new LinkedHashMap<>();
            for (StreamOutputTagDescriptor tag : binding.outputTags()) {
                tags.put(tag.tagId(), new OutputContract(tag.tagId(),
                        tag.schema(), tag.stableKeyFields(),
                        tag.changelogMode()
                                == StreamOutputTagDescriptor.ChangelogMode
                                .UPSERT,
                        tag.maximumLeftOutputs(),
                        tag.maximumLeftOutputBytes(),
                        tag.maximumRightOutputs(),
                        tag.maximumRightOutputBytes(),
                        tag.maximumTimerOutputs(),
                        tag.maximumTimerOutputBytes()));
            }
            this.taggedOutputs = Collections.unmodifiableMap(tags);
        }

        static ProcessContract from(StreamNodeTaskContext context) {
            PortableStreamNode process = context == null
                    ? null : context.node();
            if (process == null
                    || process.opcode() != StreamOperatorCode.KEYED_PROCESS
                    && process.opcode()
                    != StreamOperatorCode.KEYED_CO_PROCESS
                    || !(process.binding()
                    instanceof StreamProcessBinding)
                    || process.inputOperatorUids().size()
                    != context.inputs().size()) {
                throw new UnsupportedOperationException(
                        "Stream keyed process node contract is invalid");
            }
            return new ProcessContract(context);
        }

        int inputCount() { return inputSchemas.size(); }

        boolean immutableBroadcast() {
            return broadcast && broadcastState.authorityKind()
                    == StreamBroadcastStateDescriptor.AuthorityKind
                    .IMMUTABLE_SNAPSHOT;
        }

        PortableRowSchema inputSchema(int inputIndex) {
            return inputSchemas.get(inputIndex);
        }

        int[] inputKeyIndices(int inputIndex) {
            return inputKeyIndices.get(inputIndex);
        }

        RowSerializer inputSerializer(int inputIndex) {
            return inputSerializers.get(inputIndex);
        }

        RowSerializer inputKeySerializer(int inputIndex) {
            return inputKeySerializers.get(inputIndex);
        }

        OutputContract output(String tagId) {
            if (tagId == null) return mainOutput;
            OutputContract output = taggedOutputs.get(tagId);
            if (output == null) {
                throw processFailure(
                        StreamProcessException.Code.UNDECLARED_OUTPUT_TAG,
                        "keyed process emitted undeclared tag: " + tagId);
            }
            return output;
        }
    }

    private static final class OutputContract {
        final String tagId;
        final PortableRowSchema schema;
        final RowSerializer serializer;
        final RowSerializer keySerializer;
        final int[] keyIndices;
        final boolean upsert;
        final int maximumElementOutputs;
        final long maximumElementOutputBytes;
        final int maximumRightOutputs;
        final long maximumRightOutputBytes;
        final int maximumTimerOutputs;
        final long maximumTimerOutputBytes;

        private OutputContract(String tagId, PortableRowSchema schema,
                List<String> stableKeys, boolean upsert,
                int maximumElementOutputs,
                long maximumElementOutputBytes,
                int maximumRightOutputs,
                long maximumRightOutputBytes,
                int maximumTimerOutputs,
                long maximumTimerOutputBytes) {
            this.tagId = tagId;
            this.schema = schema;
            this.serializer = serializer(schema);
            this.keyIndices = indices(schema, stableKeys);
            this.upsert = upsert;
            this.maximumElementOutputs = maximumElementOutputs;
            this.maximumElementOutputBytes = maximumElementOutputBytes;
            this.maximumRightOutputs = maximumRightOutputs;
            this.maximumRightOutputBytes = maximumRightOutputBytes;
            this.maximumTimerOutputs = maximumTimerOutputs;
            this.maximumTimerOutputBytes = maximumTimerOutputBytes;
            if (upsert && keyIndices.length == 0) {
                throw new UnsupportedOperationException(
                        "UPSERT keyed process output requires stable keys");
            }
            this.keySerializer = upsert
                    ? serializer(keySchema(schema, keyIndices)) : null;
        }

        byte[] frame(DataRow row) {
            byte[] afterImage = serializer.serialize(row);
            if (!upsert) return afterImage;
            byte[] key = keySerializer.serializeProjected(row, keyIndices);
            return StreamUpsertRecordCodec.encode(
                    StreamUpsertRecord.afterImage(key, afterImage));
        }
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
                "keyed process stable key binding is missing");
    }

    static String timerStateName(String timerFamilyId) {
        return StreamProcessBinding.timerStateName(timerFamilyId);
    }

    static StreamStateDescriptor timerStateDescriptor(
            StreamTimerDescriptor timer) {
        if (timer == null) {
            throw new IllegalArgumentException("timer descriptor is required");
        }
        return new StreamStateDescriptor(
                timerStateName(timer.timerFamilyId()),
                StreamStateDescriptor.Kind.KEYED_MAP, TIMER_CODEC, 1,
                StreamStateDescriptor.TtlMode.NONE, 0L);
    }

    static StreamStateDescriptor broadcastStateDescriptor(
            StreamBroadcastStateDescriptor broadcast) {
        if (broadcast == null) {
            throw new IllegalArgumentException(
                    "broadcast state descriptor is required");
        }
        return new StreamStateDescriptor(
                BROADCAST_STATE_PREFIX + broadcast.stateName(),
                StreamStateDescriptor.Kind.KEYED_MAP,
                broadcast.serializerContractId(),
                broadcast.stateSchemaVersion(),
                StreamStateDescriptor.TtlMode.NONE, 0L);
    }

    private static void requireBroadcastName(BroadcastSlot broadcast,
            String stateName) {
        if (broadcast == null || stateName == null
                || !stateName.equals(broadcast.descriptor.stateName())) {
            throw processFailure(
                    StreamProcessException.Code.UNDECLARED_STATE,
                    "keyed process accessed undeclared broadcast state: "
                            + stateName);
        }
    }

    private static byte[] encodeTimerNamespace(
            long timestamp, byte[] namespace) {
        if (namespace == null) {
            throw processFailure(
                    StreamProcessException.Code.UNDECLARED_TIMER,
                    "keyed process timer namespace is required");
        }
        int length;
        try {
            length = Math.addExact(Long.BYTES, namespace.length);
        } catch (ArithmeticException overflow) {
            throw processFailure(
                    StreamProcessException.Code.TIMER_QUOTA_EXCEEDED,
                    "keyed timer namespace is too large");
        }
        return ByteBuffer.allocate(length).putLong(timestamp)
                .put(namespace).array();
    }

    private static byte[] decodeTimerNamespace(byte[] storedNamespace) {
        if (storedNamespace == null
                || storedNamespace.length < Long.BYTES) {
            throw processFailure(
                    StreamProcessException.Code.INVALID_STATE_VALUE,
                    "keyed timer namespace is corrupt");
        }
        byte[] namespace = new byte[storedNamespace.length - Long.BYTES];
        System.arraycopy(storedNamespace, Long.BYTES,
                namespace, 0, namespace.length);
        return namespace;
    }

    private static int compare(byte[] left, byte[] right) {
        int common = Math.min(left.length, right.length);
        for (int index = 0; index < common; index++) {
            int compared = Integer.compare(
                    left[index] & 0xff, right[index] & 0xff);
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length, right.length);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void updateText(MessageDigest digest, String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        updateLength(digest, encoded.length);
        digest.update(encoded);
    }

    private static void updateLength(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static void updateLong(MessageDigest digest, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            digest.update((byte) (value >>> shift));
        }
    }

    private static void addDigest(byte[] sum, byte[] value) {
        int carry = 0;
        for (int index = sum.length - 1; index >= 0; index--) {
            int next = (sum[index] & 0xff)
                    + (value[index] & 0xff) + carry;
            sum[index] = (byte) next;
            carry = next >>> 8;
        }
    }

    private static final class Route {
        final StreamChannelOutput output;
        final OutputContract port;
        long sequence;

        private Route(StreamChannelOutput output, OutputContract port) {
            this.output = output;
            this.port = port;
        }

        void sendRecord(byte[] frame, Long eventTimestamp) {
            long next = next();
            output.send(eventTimestamp == null
                    ? StreamEnvelope.record(
                    output.channelIdentity(), next, frame)
                    : StreamEnvelope.record(output.channelIdentity(), next,
                    eventTimestamp, frame));
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
                            "unsupported keyed process signal");
            }
        }

        private long next() {
            sequence = Math.addExact(sequence, 1L);
            return sequence;
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
