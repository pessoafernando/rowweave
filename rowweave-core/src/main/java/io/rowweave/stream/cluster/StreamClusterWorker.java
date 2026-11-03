package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.DynamicDurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.LocalStreamSinkTask;
import io.rowweave.stream.runtime.LocalTransactionalStreamSinkTask;
import io.rowweave.stream.runtime.LocalResidentCdcSourceTask;
import io.rowweave.stream.runtime.ResidentCdcStreamSourceProvider;
import io.rowweave.stream.runtime.LocalStreamSourceTask;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamSourceCheckpointRuntime;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One Stream-capable Worker incarnation with worker-local provider registries. */
public final class StreamClusterWorker {
    private final StreamWorkerCapabilityProfile profile;
    private final Map<String, DurableStreamSourceProvider> sourceProviders;
    private final Map<String, EagerStreamSinkProvider> sinkProviders;
    private final Map<String, TransactionalStreamSinkProvider>
            transactionalSinkProviders;
    private final Map<String, ControlFence> controlFences =
            new LinkedHashMap<>();
    private final int maximumControlFences;

    public StreamClusterWorker(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders) {
        this(profile, sourceProviders, sinkProviders,
                Collections.emptyMap());
    }

    public StreamClusterWorker(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders) {
        if (profile == null || sourceProviders == null || sinkProviders == null) {
            throw new IllegalArgumentException(
                    "Stream worker profile and provider registries are required");
        }
        this.profile = profile;
        this.maximumControlFences = Math.addExact(
                profile.taskSlots(), 4_096);
        this.sourceProviders = immutableSources(sourceProviders);
        this.sinkProviders = immutableSinks(sinkProviders);
        this.transactionalSinkProviders =
                immutableTransactionalSinks(transactionalSinkProviders);
    }

    public StreamWorkerCapabilityProfile profile() {
        return profile;
    }

    public synchronized StreamControlMessage acceptControl(byte[] frame) {
        StreamControlMessage command = StreamControlMessageCodec.decode(frame);
        if (!profile.workerIncarnation().equals(
                command.workerIncarnation())) {
            throw failure(StreamClusterProtocolException.Code
                            .WORKER_INCARNATION_MISMATCH,
                    "Stream control targets another worker incarnation");
        }
        ControlFence current = controlFences.get(command.runId());
        if (current == null) {
            if (command.kind() != StreamControlMessage.Kind.START
                    && command.kind() != StreamControlMessage.Kind.RESTORE) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "first Stream worker command must establish an attempt");
            }
            admitControlFence(command);
            return command;
        }
        if (command.coordinatorTerm() < current.coordinatorTerm) {
            throw failure(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    "Stream control coordinator term is stale");
        }
        if (command.coordinatorTerm() == current.coordinatorTerm
                && !command.coordinatorIncarnation().equals(
                        current.coordinatorIncarnation)) {
            throw failure(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    "Stream control coordinator incarnation differs");
        }
        boolean sameAttempt = command.runId().equals(current.runId)
                && command.attemptId().equals(current.attemptId)
                && command.fenceToken() == current.fenceToken
                && command.assignmentGeneration()
                == current.assignmentGeneration;
        if (command.coordinatorTerm() > current.coordinatorTerm) {
            if (!sameAttempt && !isAdvancingRestore(command, current)) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "replacement coordinator did not preserve or advance the attempt");
            }
            controlFences.put(command.runId(), ControlFence.from(command));
            return command;
        }
        if (!sameAttempt) {
            if (!isAdvancingRestore(command, current)) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "Stream control attempt fence is stale");
            }
            controlFences.put(command.runId(), ControlFence.from(command));
            return command;
        }
        if (command.commandSequence() <= current.commandSequence) {
            throw failure(StreamClusterProtocolException.Code.COMMAND_REPLAY,
                    "Stream control command sequence was already accepted");
        }
        controlFences.put(command.runId(), ControlFence.from(command));
        return command;
    }

    /** Accepts one fenced control command once per Worker and replays the
     * exact same command for additional node/subtask participants. */
    synchronized StreamControlMessage acceptParticipantControl(byte[] frame) {
        StreamControlMessage command = StreamControlMessageCodec.decode(frame);
        try {
            return acceptControl(frame);
        } catch (StreamClusterProtocolException replay) {
            if (replay.code()
                    != StreamClusterProtocolException.Code.COMMAND_REPLAY
                    || controlFences.get(command.runId()) == null
                    || !controlFences.get(command.runId())
                    .sameCommand(command)) {
                throw replay;
            }
            return command;
        }
    }

    /** Establishes an old-attempt fence on a replacement Worker only after
     * the caller has verified an authoritative durable commit decision. */
    synchronized StreamControlMessage acceptRecoveryControl(byte[] frame) {
        StreamControlMessage command = StreamControlMessageCodec.decode(frame);
        if (command.kind() != StreamControlMessage.Kind.CHECKPOINT_TRIGGER) {
            throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                    "Stream recovery control requires a checkpoint decision");
        }
        if (!profile.workerIncarnation().equals(
                command.workerIncarnation())) {
            throw failure(StreamClusterProtocolException.Code
                            .WORKER_INCARNATION_MISMATCH,
                    "Stream recovery targets another worker incarnation");
        }
        if (!controlFences.containsKey(command.runId())) {
            return command;
        }
        ControlFence current = controlFences.get(command.runId());
        if (command.coordinatorTerm() < current.coordinatorTerm) {
            throw failure(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    "Stream recovery coordinator term is stale");
        }
        if (command.coordinatorTerm() == current.coordinatorTerm
                && !command.coordinatorIncarnation().equals(
                current.coordinatorIncarnation)) {
            throw failure(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    "Stream recovery coordinator incarnation differs");
        }
        if (!current.sameAttempt(command)) {
            throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                    "Stream recovery decision targets another attempt");
        }
        // A durable commit decision is an independent, idempotent authority.
        // Recovery must never claim or advance the ordinary task-control
        // sequence: a replacement coordinator still has to establish its
        // normal START/RESTORE fence through acceptControl().
        return command;
    }

    synchronized void abandonAttempt(StreamControlMessage command) {
        if (command == null) return;
        ControlFence current = controlFences.get(command.runId());
        if (current != null && current.sameAttempt(command)
                && (current.kind == StreamControlMessage.Kind.START
                || current.kind == StreamControlMessage.Kind.RESTORE)) {
            controlFences.remove(command.runId());
        }
    }

    private void admitControlFence(StreamControlMessage command) {
        if (controlFences.size() >= maximumControlFences) {
            String terminal = null;
            for (Map.Entry<String, ControlFence> entry
                    : controlFences.entrySet()) {
                if (entry.getValue().kind
                        == StreamControlMessage.Kind.CANCEL) {
                    terminal = entry.getKey();
                    break;
                }
            }
            if (terminal != null) controlFences.remove(terminal);
        }
        if (controlFences.size() >= maximumControlFences) {
            throw failure(
                    StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                    "Stream Worker control fence capacity is exhausted");
        }
        controlFences.put(command.runId(), ControlFence.from(command));
    }

    void requireSourceProvider(StreamSourceBinding binding) {
        DurableStreamSourceProvider provider =
                sourceProviders.get(binding.providerKey());
        if (provider == null
                || !binding.providerKey().equals(provider.providerKey())
                || !binding.providerContractDigest().equals(
                        provider.contractDigest())) {
            throw new IllegalArgumentException(
                    "required worker-local Stream source provider is unavailable");
        }
    }

    DynamicDurableStreamSourceProvider dynamicSourceProvider(
            StreamSourceBinding binding) {
        requireSourceProvider(binding);
        DurableStreamSourceProvider provider =
                sourceProviders.get(binding.providerKey());
        if (!binding.dynamicDiscovery()
                || !(provider instanceof DynamicDurableStreamSourceProvider)) {
            throw new IllegalArgumentException(
                    "required dynamic Stream source provider is unavailable");
        }
        return (DynamicDurableStreamSourceProvider) provider;
    }

    void requireSinkProvider(StreamSinkBinding binding) {
        String actual = binding.visibilityProfile()
                == StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL
                ? contract(transactionalSinkProviders.get(binding.providerKey()))
                : contract(sinkProviders.get(binding.providerKey()));
        if (actual == null
                || !binding.providerContractDigest().equals(actual)) {
            throw new IllegalArgumentException(
                    "required worker-local Stream Sink provider is unavailable");
        }
    }

    LocalStreamSourceTask sourceTask(
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamChannelOutput output) {
        return sourceTask(operatorUid, binding, split, output, 0L);
    }

    LocalStreamSourceTask sourceTask(
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamChannelOutput output,
            long idleTimeoutMillis) {
        return sourceTask(operatorUid, binding, split, output,
                null, idleTimeoutMillis);
    }

    LocalStreamSourceTask sourceTask(
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            long idleTimeoutMillis) {
        return sourceTask(operatorUid, binding, split, output,
                checkpointRuntime, idleTimeoutMillis,
                binding.dynamicDiscovery());
    }

    LocalStreamSourceTask sourceTask(
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            long idleTimeoutMillis, boolean coordinatorManaged) {
        return sourceTask(operatorUid, binding, split, output,
                checkpointRuntime, idleTimeoutMillis, coordinatorManaged, false);
    }

    LocalStreamSourceTask sourceTask(
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            long idleTimeoutMillis, boolean coordinatorManaged,
            boolean declaredBoundedTermination) {
        requireSourceProvider(binding);
        return new LocalStreamSourceTask(
                taskName(operatorUid), operatorUid, binding, split,
                sourceProviders.get(binding.providerKey()), output,
                checkpointRuntime, idleTimeoutMillis, coordinatorManaged,
                declaredBoundedTermination);
    }

    LocalResidentCdcSourceTask residentCdcSourceTask(
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            int maximumFrameBytes,
            io.rowweave.portable.PortableRowSchema outputSchema,
            java.util.List<String> stableKeyFields) {
        requireSourceProvider(binding);
        DurableStreamSourceProvider provider =
                sourceProviders.get(binding.providerKey());
        if (!(provider instanceof ResidentCdcStreamSourceProvider)) {
            throw new IllegalArgumentException(
                    "required Stream source provider has no resident CDC SPI");
        }
        return new LocalResidentCdcSourceTask(
                taskName(operatorUid), operatorUid, binding, split,
                (ResidentCdcStreamSourceProvider) provider, output,
                checkpointRuntime, maximumFrameBytes, outputSchema,
                stableKeyFields);
    }

    LocalStreamSinkTask sinkTask(
            String operatorUid, StreamSinkBinding binding,
            StreamChannelInput input,
            java.util.function.Consumer<Throwable> onFailure) {
        return sinkTask(operatorUid, binding, input, onFailure,
                ignored -> { });
    }

    LocalTransactionalStreamSinkTask transactionalSinkTask(
            String operatorUid, StreamSinkBinding binding,
            StreamChannelInput input,
            java.util.function.Consumer<Throwable> onFailure,
            java.util.function.Consumer<byte[]> recordValidator) {
        return transactionalSinkTask(operatorUid, 0, binding, input,
                onFailure, recordValidator);
    }

    LocalTransactionalStreamSinkTask transactionalSinkTask(
            String operatorUid, int subtaskIndex,
            StreamSinkBinding binding, StreamChannelInput input,
            java.util.function.Consumer<Throwable> onFailure,
            java.util.function.Consumer<byte[]> recordValidator) {
        return transactionalSinkTask(operatorUid, subtaskIndex, binding,
                input, onFailure, recordValidator, false);
    }

    LocalTransactionalStreamSinkTask transactionalSinkTask(
            String operatorUid, int subtaskIndex,
            StreamSinkBinding binding, StreamChannelInput input,
            java.util.function.Consumer<Throwable> onFailure,
            java.util.function.Consumer<byte[]> recordValidator,
            boolean terminalInputAlreadyEnded) {
        requireSinkProvider(binding);
        return new LocalTransactionalStreamSinkTask(
                taskName(operatorUid, subtaskIndex), operatorUid, subtaskIndex,
                binding,
                transactionalSinkProviders.get(binding.providerKey()), input,
                onFailure, recordValidator, terminalInputAlreadyEnded);
    }

    TransactionalStreamSinkProvider transactionalSinkProvider(
            StreamSinkBinding binding) {
        requireSinkProvider(binding);
        return transactionalSinkProviders.get(binding.providerKey());
    }

    LocalStreamSinkTask sinkTask(
            String operatorUid, StreamSinkBinding binding,
            StreamChannelInput input,
            java.util.function.Consumer<Throwable> onFailure,
            java.util.function.Consumer<byte[]> recordValidator) {
        return sinkTask(operatorUid, 0, binding, input, onFailure,
                recordValidator);
    }

    LocalStreamSinkTask sinkTask(
            String operatorUid, int subtaskIndex, StreamSinkBinding binding,
            StreamChannelInput input,
            java.util.function.Consumer<Throwable> onFailure,
            java.util.function.Consumer<byte[]> recordValidator) {
        requireSinkProvider(binding);
        return new LocalStreamSinkTask(
                taskName(operatorUid, subtaskIndex), operatorUid, subtaskIndex,
                binding,
                sinkProviders.get(binding.providerKey()), input, onFailure,
                recordValidator);
    }

    private String taskName(String operatorUid) {
        return profile.workerId() + "-" + operatorUid;
    }

    private String taskName(String operatorUid, int subtaskIndex) {
        return taskName(operatorUid) + '-' + subtaskIndex;
    }

    private static Map<String, DurableStreamSourceProvider> immutableSources(
            Map<String, DurableStreamSourceProvider> providers) {
        Map<String, DurableStreamSourceProvider> copy = new LinkedHashMap<>();
        for (Map.Entry<String, DurableStreamSourceProvider> entry
                : providers.entrySet()) {
            DurableStreamSourceProvider provider = entry.getValue();
            if (entry.getKey() == null || provider == null
                    || !entry.getKey().equals(provider.providerKey())) {
                throw new IllegalArgumentException(
                        "Stream source provider registry entry is invalid");
            }
            copy.put(entry.getKey(), provider);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, EagerStreamSinkProvider> immutableSinks(
            Map<String, EagerStreamSinkProvider> providers) {
        Map<String, EagerStreamSinkProvider> copy = new LinkedHashMap<>();
        for (Map.Entry<String, EagerStreamSinkProvider> entry
                : providers.entrySet()) {
            EagerStreamSinkProvider provider = entry.getValue();
            if (entry.getKey() == null || provider == null
                    || !entry.getKey().equals(provider.providerKey())) {
                throw new IllegalArgumentException(
                        "Stream Sink provider registry entry is invalid");
            }
            copy.put(entry.getKey(), provider);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, TransactionalStreamSinkProvider>
            immutableTransactionalSinks(
            Map<String, TransactionalStreamSinkProvider> providers) {
        if (providers == null) {
            throw new IllegalArgumentException(
                    "transactional Stream Sink providers are required");
        }
        Map<String, TransactionalStreamSinkProvider> copy =
                new LinkedHashMap<>();
        for (Map.Entry<String, TransactionalStreamSinkProvider> entry
                : providers.entrySet()) {
            TransactionalStreamSinkProvider provider = entry.getValue();
            if (entry.getKey() == null || provider == null
                    || !entry.getKey().equals(provider.providerKey())) {
                throw new IllegalArgumentException(
                        "transactional Stream Sink registry entry is invalid");
            }
            copy.put(entry.getKey(), provider);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static String contract(EagerStreamSinkProvider provider) {
        return provider == null ? null : provider.contractDigest();
    }

    private static String contract(TransactionalStreamSinkProvider provider) {
        return provider == null ? null : provider.contractDigest();
    }

    private static boolean isAdvancingRestore(
            StreamControlMessage command, ControlFence current) {
        return command.runId().equals(current.runId)
                && (command.kind() == StreamControlMessage.Kind.RESTORE
                || command.kind() == StreamControlMessage.Kind.START)
                && !command.attemptId().equals(current.attemptId)
                && command.fenceToken() > current.fenceToken
                && command.assignmentGeneration()
                        > current.assignmentGeneration;
    }

    private static StreamClusterProtocolException failure(
            StreamClusterProtocolException.Code code, String message) {
        return new StreamClusterProtocolException(code, message);
    }

    private static final class ControlFence {
        final long coordinatorTerm;
        final String coordinatorIncarnation;
        final String runId;
        final String attemptId;
        final long fenceToken;
        final long assignmentGeneration;
        final long commandSequence;
        final StreamControlMessage.Kind kind;
        final String commandDigest;
        final byte[] payload;

        private ControlFence(StreamControlMessage command) {
            this.coordinatorTerm = command.coordinatorTerm();
            this.coordinatorIncarnation = command.coordinatorIncarnation();
            this.runId = command.runId();
            this.attemptId = command.attemptId();
            this.fenceToken = command.fenceToken();
            this.assignmentGeneration = command.assignmentGeneration();
            this.commandSequence = command.commandSequence();
            this.kind = command.kind();
            this.commandDigest = command.commandDigest();
            this.payload = command.payload();
        }

        boolean sameCommand(StreamControlMessage command) {
            return coordinatorTerm == command.coordinatorTerm()
                    && coordinatorIncarnation.equals(
                    command.coordinatorIncarnation())
                    && runId.equals(command.runId())
                    && attemptId.equals(command.attemptId())
                    && fenceToken == command.fenceToken()
                    && assignmentGeneration == command.assignmentGeneration()
                    && commandSequence == command.commandSequence()
                    && kind == command.kind()
                    && commandDigest.equals(command.commandDigest())
                    && java.util.Arrays.equals(payload, command.payload());
        }

        boolean sameAttempt(StreamControlMessage command) {
            return runId.equals(command.runId())
                    && attemptId.equals(command.attemptId())
                    && fenceToken == command.fenceToken()
                    && assignmentGeneration
                    == command.assignmentGeneration()
                    && commandDigest.equals(command.commandDigest());
        }

        static ControlFence from(StreamControlMessage command) {
            return new ControlFence(command);
        }

    }
}
