package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCdcEnumeratorState;
import io.rowweave.stream.contract.StreamCdcEnumeratorStateCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Mailbox-owned public resident CDC source using the canonical enumerator. */
public final class LocalResidentCdcSourceTask {
    private final String taskName;
    private final String sourceOperatorUid;
    private final StreamSourceBinding binding;
    private final StreamSourceSplit split;
    private final ResidentCdcStreamSourceProvider provider;
    private final StreamChannelOutput output;
    private final StreamSourceCheckpointRuntime checkpointRuntime;
    private final int maximumFrameBytes;
    private final StreamUpsertKeyValidator upsertKeyValidator;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();
    private final AtomicReference<CompletionRequest> pendingCompletion =
            new AtomicReference<>();
    private final AtomicLong wakeGeneration = new AtomicLong();

    private volatile StreamCdcSourceReader reader;
    private volatile Throwable failure;
    private volatile boolean completed;
    private long outputSequence;
    private long lastCheckpointSequence;
    private long consumedWakeGeneration;
    private volatile PreparedCompletion preparedCompletion;
    private volatile long lastCompletedCheckpointSequence;

    public LocalResidentCdcSourceTask(String taskName,
            String sourceOperatorUid, StreamSourceBinding binding,
            StreamSourceSplit split,
            ResidentCdcStreamSourceProvider provider,
            StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            int maximumFrameBytes) {
        throw new IllegalArgumentException(
                "resident CDC source requires an exact output key contract");
    }

    public LocalResidentCdcSourceTask(String taskName,
            String sourceOperatorUid, StreamSourceBinding binding,
            StreamSourceSplit split,
            ResidentCdcStreamSourceProvider provider,
            StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            int maximumFrameBytes, PortableRowSchema outputSchema,
            List<String> stableKeyFields) {
        if (taskName == null || taskName.isEmpty()
                || sourceOperatorUid == null || sourceOperatorUid.isEmpty()
                || binding == null || split == null || provider == null
                || output == null || checkpointRuntime == null
                || maximumFrameBytes < 1
                || maximumFrameBytes
                > io.rowweave.stream.contract.StreamEdge.MAX_FRAME_BYTES
                || outputSchema == null || stableKeyFields == null
                || stableKeyFields.isEmpty()
                || !"STREAM_CDC_ENUMERATOR_V1".equals(
                split.positionCodecId())
                || !binding.fixedSplits().contains(split)
                || !binding.providerKey().equals(provider.providerKey())
                || !binding.providerContractDigest().equals(
                provider.contractDigest())) {
            throw new IllegalArgumentException(
                    "resident CDC source task configuration is invalid");
        }
        this.taskName = taskName;
        this.sourceOperatorUid = sourceOperatorUid;
        this.binding = binding;
        this.split = split;
        this.provider = provider;
        this.output = output;
        this.checkpointRuntime = checkpointRuntime;
        this.maximumFrameBytes = maximumFrameBytes;
        this.upsertKeyValidator = new StreamUpsertKeyValidator(
                outputSchema, stableKeyFields, maximumFrameBytes);
        this.thread = new Thread(this::run,
                "rowweave-stream-resident-cdc-" + taskName);
    }

    public void start() { thread.start(); }

    public CompletableFuture<StreamCheckpointMember> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (checkpoint == null || !sameAttempt(checkpoint)
                || !checkpointRuntime.artifactId().equals(
                checkpoint.artifactId())) {
            request.reject(new IllegalArgumentException(
                    "checkpoint does not belong to the resident CDC task"));
            return request.result;
        }
        if (completed || checkpoint.checkpointSequence()
                <= lastCheckpointSequence
                || !pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "resident CDC checkpoint cannot be accepted"));
            return request.result;
        }
        StreamCdcSourceReader current = reader;
        wakeGeneration.incrementAndGet();
        if (current != null) current.wakeUp();
        return request.result;
    }

    public CompletableFuture<Void> notifyCheckpointComplete(
            CheckpointIdentity checkpoint) {
        CompletionRequest request = new CompletionRequest(checkpoint);
        if (checkpoint == null || !sameAttempt(checkpoint)
                || !checkpointRuntime.artifactId().equals(
                checkpoint.artifactId())) {
            request.reject(new IllegalArgumentException(
                    "checkpoint does not belong to the resident CDC task"));
            return request.result;
        }
        if (checkpoint.checkpointSequence()
                <= lastCompletedCheckpointSequence) {
            request.result.complete(null);
            return request.result;
        }
        PreparedCompletion prepared = preparedCompletion;
        if (completed || prepared == null
                || !prepared.checkpoint.equals(checkpoint)
                || !pendingCompletion.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "resident CDC checkpoint completion is unavailable"));
            return request.result;
        }
        StreamCdcSourceReader current = reader;
        wakeGeneration.incrementAndGet();
        if (current != null) current.wakeUp();
        return request.result;
    }

    public void cancel() {
        if (!cancellationRequested.compareAndSet(false, true)) return;
        output.cancel();
        StreamCdcSourceReader current = reader;
        if (current != null) current.cancel();
        thread.interrupt();
    }

    public void awaitCompletion(long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
    }

    public Throwable failure() { return failure; }

    private void run() {
        StreamCdcSourceReader current = null;
        try {
            StreamCdcEnumeratorState descriptor = descriptor();
            StreamSourcePositionCheckpoint positions =
                    checkpointRuntime.positions(sourceOperatorUid, binding,
                            split, 0L, initialFrame(descriptor));
            if (checkpointRuntime.restoreMember() != null) {
                checkpointRuntime.restorePosition(positions);
            }
            StreamCdcEnumeratorState state = StreamCdcEnumeratorStateCodec
                    .decode(positions.currentPositionFrame());
            validateState(descriptor, state);
            ResidentStreamCdcEnumerator enumerator =
                    new ResidentStreamCdcEnumerator(state,
                            maximumFrameBytes(), this::emitRecord);
            current = provider.openCdc(context(state));
            if (current == null) {
                throw new IllegalArgumentException(
                        "resident CDC provider returned no reader");
            }
            reader = current;
            boolean checkpointBoundary = true;
            while (!cancellationRequested.get()) {
                emitPendingCompletion(current);
                if (checkpointBoundary) {
                    emitPendingCheckpoint(enumerator, positions);
                }
                StreamCdcSourceEvent event = current.next();
                if (event == null) {
                    if (cancellationRequested.get()) return;
                    long requestedWake = wakeGeneration.get();
                    if (requestedWake > consumedWakeGeneration) {
                        consumedWakeGeneration = requestedWake;
                        continue;
                    }
                    if (pendingCompletion.get() != null) continue;
                    if (pendingCheckpoint.get() != null) {
                        if (checkpointBoundary) {
                            emitPendingCheckpoint(enumerator, positions);
                        }
                        continue;
                    }
                    throw new IllegalStateException(
                            "resident CDC source ended unexpectedly");
                }
                switch (event.kind()) {
                    case SNAPSHOT_AFTER_IMAGE:
                        StreamUpsertRecord snapshot = decode(
                                event.upsertFrame());
                        enumerator.onSnapshotRecord(
                                event.splitId(), snapshot);
                        positions.recordEmitted(
                                positions.currentPositionFrame());
                        checkpointBoundary = false;
                        break;
                    case SNAPSHOT_SPLIT_COMPLETE:
                        enumerator.completeSnapshotSplit(event.splitId());
                        positions.updatePosition(StreamCdcEnumeratorStateCodec
                                .encode(enumerator.checkpointState()));
                        checkpointBoundary = true;
                        break;
                    case SNAPSHOT_HANDOFF_COMPLETE:
                        enumerator.completeSnapshotAndHandoff(
                                event.splitId(),
                                event.completedExternalPositionFrame());
                        positions.updatePosition(StreamCdcEnumeratorStateCodec
                                .encode(enumerator.checkpointState()));
                        checkpointBoundary = true;
                        break;
                    case HANDOFF_COMPLETE:
                        enumerator.completeHandoff();
                        positions.updatePosition(StreamCdcEnumeratorStateCodec
                                .encode(enumerator.checkpointState()));
                        checkpointBoundary = true;
                        break;
                    case LOG_RECORD:
                        if (event.hasCompletedExternalPosition()) {
                            enumerator.onLogRecord(event.logSequence(),
                                    decode(event.upsertFrame()),
                                    event.completedExternalPositionFrame());
                            positions.recordEmitted(
                                    StreamCdcEnumeratorStateCodec.encode(
                                            enumerator.checkpointState()));
                            checkpointBoundary = true;
                        } else if (descriptor.version()
                                == StreamCdcEnumeratorState.CURRENT_VERSION) {
                            enumerator.onLogRecord(event.logSequence(),
                                    decode(event.upsertFrame()), null);
                            positions.recordEmitted(
                                    positions.currentPositionFrame());
                            checkpointBoundary = false;
                        } else {
                            enumerator.onLogRecord(event.logSequence(),
                                    decode(event.upsertFrame()));
                            positions.recordEmitted(
                                    StreamCdcEnumeratorStateCodec.encode(
                                            enumerator.checkpointState()));
                            checkpointBoundary = true;
                        }
                        break;
                    case LOG_POSITION:
                        enumerator.onLogPosition(
                                event.completedExternalPositionFrame());
                        positions.updatePosition(
                                StreamCdcEnumeratorStateCodec.encode(
                                        enumerator.checkpointState()));
                        checkpointBoundary = true;
                        break;
                    default:
                        throw new AssertionError(event.kind());
                }
            }
        } catch (StreamChannelCancelledException cancelled) {
            if (!cancellationRequested.get()) fail(cancelled);
        } catch (InterruptedException interrupted) {
            if (!cancellationRequested.get()) {
                fail(interrupted);
                Thread.currentThread().interrupt();
            }
        } catch (Throwable thrown) {
            if (!cancellationRequested.get()) fail(thrown);
        } finally {
            CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
            if (pending != null) pending.reject(new IllegalStateException(
                    "resident CDC source ended before checkpoint"));
            CompletionRequest completion = pendingCompletion.getAndSet(null);
            if (completion != null) completion.reject(
                    new IllegalStateException(
                            "resident CDC source ended before checkpoint completion"));
            reader = null;
            if (current != null) {
                try {
                    current.close();
                } catch (Throwable closeFailure) {
                    if (!cancellationRequested.get() && failure == null) {
                        fail(closeFailure);
                    }
                }
            }
            completed = true;
        }
    }

    private void emitPendingCheckpoint(
            ResidentStreamCdcEnumerator enumerator,
            StreamSourcePositionCheckpoint positions) {
        CheckpointRequest request = pendingCheckpoint.getAndSet(null);
        if (request == null) return;
        StreamCdcEnumeratorState state = enumerator.checkpointState();
        positions.updatePosition(StreamCdcEnumeratorStateCodec.encode(state));
        StreamCheckpointMember member = positions.snapshot();
        preparedCompletion = state.hasExternalPosition()
                ? new PreparedCompletion(request.checkpoint,
                new StreamCdcCheckpointCompletion(
                        request.checkpoint.checkpointSequence(),
                        state.lastLogSequence(),
                        state.externalPositionFrame()))
                : null;
        outputSequence = Math.addExact(outputSequence, 1L);
        output.send(StreamEnvelope.checkpointBarrier(
                output.channelIdentity(), outputSequence,
                request.checkpoint));
        lastCheckpointSequence = request.checkpoint.checkpointSequence();
        request.result.complete(member);
    }

    private void emitPendingCompletion(StreamCdcSourceReader current) {
        CompletionRequest request = pendingCompletion.getAndSet(null);
        if (request == null) return;
        PreparedCompletion prepared = preparedCompletion;
        if (prepared == null || !prepared.checkpoint.equals(
                request.checkpoint)) {
            request.reject(new IllegalStateException(
                    "resident CDC checkpoint completion was not prepared"));
            return;
        }
        try {
            current.notifyCheckpointComplete(prepared.completion);
            lastCompletedCheckpointSequence =
                    request.checkpoint.checkpointSequence();
            request.result.complete(null);
        } catch (Exception failure) {
            request.reject(failure);
        } catch (Error failure) {
            request.reject(failure);
            throw failure;
        }
    }

    private void emitRecord(byte[] frame) {
        outputSequence = Math.addExact(outputSequence, 1L);
        output.send(StreamEnvelope.record(
                output.channelIdentity(), outputSequence, frame));
    }

    private StreamCdcEnumeratorState descriptor() {
        StreamCdcEnumeratorState value = StreamCdcEnumeratorStateCodec.decode(
                split.descriptorFrame());
        if (!"resident-cdc-descriptor".equals(value.sourceOperatorUid())
                || value.phase() != StreamCdcEnumeratorState.Phase.SNAPSHOT
                || !value.completedSplits().isEmpty()) {
            throw new IllegalArgumentException(
                    "resident CDC descriptor is not an initial cut");
        }
        return value;
    }

    private byte[] initialFrame(StreamCdcEnumeratorState descriptor) {
        StreamCdcEnumeratorState initial = descriptor.version()
                == StreamCdcEnumeratorState.CURRENT_VERSION
                ? StreamCdcEnumeratorState.initialV2(sourceOperatorUid,
                descriptor.snapshotId(), descriptor.snapshotCutSequence(),
                descriptor.expectedSplitIds())
                : StreamCdcEnumeratorState.initial(sourceOperatorUid,
                descriptor.snapshotId(), descriptor.snapshotCutSequence(),
                descriptor.expectedSplitIds());
        return StreamCdcEnumeratorStateCodec.encode(initial);
    }

    private void validateState(StreamCdcEnumeratorState descriptor,
            StreamCdcEnumeratorState state) {
        if (!sourceOperatorUid.equals(state.sourceOperatorUid())
                || !descriptor.snapshotId().equals(state.snapshotId())
                || descriptor.snapshotCutSequence()
                != state.snapshotCutSequence()
                || !descriptor.expectedSplitIds().equals(
                state.expectedSplitIds())) {
            throw new IllegalArgumentException(
                    "resident CDC restored state differs from its Artifact");
        }
    }

    private StreamCdcSourceContext context(StreamCdcEnumeratorState state) {
        List<String> completedSplits = new ArrayList<>();
        for (StreamCdcEnumeratorState.CompletedSplit completedSplit
                : state.completedSplits()) {
            completedSplits.add(completedSplit.splitId());
        }
        return new StreamCdcSourceContext(sourceOperatorUid,
                binding.configurationFrame(), state.snapshotId(),
                state.snapshotCutSequence(), state.expectedSplitIds(),
                completedSplits,
                state.phase() == StreamCdcEnumeratorState.Phase.SNAPSHOT
                        ? StreamCdcSourceContext.Phase.SNAPSHOT
                        : StreamCdcSourceContext.Phase.LOG,
                state.lastLogSequence(),
                state.hasExternalPosition()
                        ? state.externalPositionFrame() : null,
                io.rowweave.stream.spi.StreamSecretAccess.unavailable());
    }

    private StreamUpsertRecord decode(byte[] frame) {
        return upsertKeyValidator.validate(frame);
    }

    private int maximumFrameBytes() {
        return maximumFrameBytes;
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return output.channelIdentity().runId().equals(checkpoint.runId())
                && output.channelIdentity().attemptId().equals(
                checkpoint.attemptId())
                && output.channelIdentity().fenceToken()
                == checkpoint.fenceToken();
    }

    private void fail(Throwable thrown) {
        failure = thrown;
        CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
        if (pending != null) pending.reject(thrown);
        output.fail(thrown);
    }

    private static final class CheckpointRequest {
        private final CheckpointIdentity checkpoint;
        private final CompletableFuture<StreamCheckpointMember> result =
                new CompletableFuture<>();
        private CheckpointRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }
        private void reject(Throwable thrown) {
            result.completeExceptionally(thrown);
        }
    }

    private static final class CompletionRequest {
        private final CheckpointIdentity checkpoint;
        private final CompletableFuture<Void> result =
                new CompletableFuture<>();
        private CompletionRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }
        private void reject(Throwable thrown) {
            result.completeExceptionally(thrown);
        }
    }

    private static final class PreparedCompletion {
        private final CheckpointIdentity checkpoint;
        private final StreamCdcCheckpointCompletion completion;
        private PreparedCompletion(CheckpointIdentity checkpoint,
                StreamCdcCheckpointCompletion completion) {
            this.checkpoint = checkpoint;
            this.completion = completion;
        }
    }
}
