package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSinkBinding;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Single-thread mailbox task for the 2 eager at-least-once Sink profile. */
public final class LocalStreamSinkTask {
    private final EagerStreamSinkProvider provider;
    private final String sinkOperatorUid;
    private final int subtaskIndex;
    private final StreamSinkBinding binding;
    private final StreamSinkOpenRequest openRequest;
    private final StreamChannelInput mailbox;
    private final Consumer<Throwable> onFailure;
    private final Consumer<byte[]> recordValidator;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();
    private volatile EagerStreamSinkWriter writer;
    private volatile CheckpointIdentity lastCheckpoint;
    private volatile StreamCheckpointMember lastCheckpointMember;
    private volatile boolean completed;
    private volatile Throwable failure;

    public LocalStreamSinkTask(
            String taskName, String sinkOperatorUid,
            StreamSinkBinding binding, EagerStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure) {
        this(taskName, sinkOperatorUid, 0, binding, provider, mailbox,
                onFailure, ignored -> { });
    }

    public LocalStreamSinkTask(
            String taskName, String sinkOperatorUid,
            StreamSinkBinding binding, EagerStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure,
            Consumer<byte[]> recordValidator) {
        this(taskName, sinkOperatorUid, 0, binding, provider, mailbox,
                onFailure, recordValidator);
    }

    public LocalStreamSinkTask(
            String taskName, String sinkOperatorUid, int subtaskIndex,
            StreamSinkBinding binding, EagerStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure) {
        this(taskName, sinkOperatorUid, subtaskIndex, binding, provider,
                mailbox, onFailure, ignored -> { });
    }

    public LocalStreamSinkTask(
            String taskName, String sinkOperatorUid, int subtaskIndex,
            StreamSinkBinding binding, EagerStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure,
            Consumer<byte[]> recordValidator) {
        if (taskName == null || taskName.isEmpty()
                || sinkOperatorUid == null || sinkOperatorUid.isEmpty()
                || subtaskIndex < 0
                || binding == null || provider == null
                || mailbox == null || onFailure == null
                || recordValidator == null) {
            throw new IllegalArgumentException(
                    "Sink task identity, contract and runtime are required");
        }
        if (binding.visibilityProfile()
                != StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE) {
            throw new UnsupportedOperationException(
                    "This sink task supports only eager at-least-once Stream Sinks");
        }
        if (!binding.providerKey().equals(provider.providerKey())
                || !binding.providerContractDigest().equals(
                        provider.contractDigest())) {
            throw new IllegalArgumentException(
                    "Stream Sink provider contract mismatch");
        }
        this.provider = provider;
        this.sinkOperatorUid = sinkOperatorUid;
        this.subtaskIndex = subtaskIndex;
        this.binding = binding;
        this.mailbox = mailbox;
        this.onFailure = onFailure;
        this.recordValidator = recordValidator;
        this.openRequest = new StreamSinkOpenRequest(
                sinkOperatorUid, subtaskIndex, binding,
                mailbox.channelIdentity());
        this.thread = new Thread(this::runSink,
                "rowweave-stream-sink-" + taskName);
    }

    public void start() { thread.start(); }

    public CompletableFuture<StreamCheckpointMember> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint is required");
        }
        CheckpointIdentity completedCheckpoint = lastCheckpoint;
        if (checkpoint.equals(completedCheckpoint)) {
            return CompletableFuture.completedFuture(lastCheckpointMember);
        }
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (completed && failure == null
                && !cancellationRequested.get()
                && sameAttempt(checkpoint)
                && (completedCheckpoint == null
                || checkpoint.checkpointSequence()
                > completedCheckpoint.checkpointSequence())) {
            return CompletableFuture.completedFuture(
                    StreamCheckpointMember.eagerSinkCapability(
                            sinkOperatorUid, subtaskIndex,
                            binding.providerContractDigest(),
                            binding.providerContractDigest()));
        }
        if (completed || cancellationRequested.get()) {
            request.reject(new IllegalStateException(
                    "Sink task is already complete"));
            return request.result;
        }
        if (!sameAttempt(checkpoint)) {
            request.reject(new IllegalArgumentException(
                    "checkpoint does not belong to the Sink task attempt"));
            return request.result;
        }
        if (completedCheckpoint != null
                && checkpoint.checkpointSequence()
                <= completedCheckpoint.checkpointSequence()) {
            request.reject(new IllegalArgumentException(
                    "Sink checkpoint sequence was already observed"));
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(new IllegalStateException(
                    "Sink task already has a checkpoint in flight"));
            return request.result;
        }
        CheckpointIdentity observed = lastCheckpoint;
        if (observed != completedCheckpoint
                && pendingCheckpoint.compareAndSet(request, null)) {
            if (observed.equals(checkpoint)) {
                request.result.complete(lastCheckpointMember);
            } else {
                request.reject(new IllegalArgumentException(
                        "Sink checkpoint barrier differs from the trigger"));
            }
        }
        return request.result;
    }

    public void cancel() {
        if (cancellationRequested.compareAndSet(false, true)) {
            mailbox.cancel();
            EagerStreamSinkWriter current = writer;
            try {
                if (current != null) current.cancel();
            } finally {
                thread.interrupt();
            }
        }
    }

    public void awaitCompletion(long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
    }

    public boolean isCompleted() { return completed; }
    public Throwable failure() { return failure; }

    private void runSink() {
        EagerStreamSinkWriter current = null;
        try {
            if (cancellationRequested.get()) return;
            current = provider.open(openRequest);
            if (current == null) {
                throw new IllegalStateException(
                        "Stream Sink provider returned no writer");
            }
            writer = current;
            while (!cancellationRequested.get()) {
                StreamEnvelope envelope = mailbox.receive();
                if (envelope == null) return;
                if (envelope.kind() == StreamEnvelope.Kind.WATERMARK
                        || envelope.kind()
                        == StreamEnvelope.Kind.WATERMARK_STATUS) {
                    // Progress signals are ordered with records on the lane,
                    // but an eager Sink has no state to mutate for them.
                    continue;
                }
                if (envelope.kind()
                        == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
                    observeCheckpoint(envelope.checkpointIdentity());
                    continue;
                }
                if (envelope.kind() == StreamEnvelope.Kind.END_OF_INPUT) {
                    return;
                }
                if (envelope.kind() != StreamEnvelope.Kind.RECORD) {
                    throw new UnsupportedOperationException(
                            "eager Stream Sink received an unsupported signal");
                }
                byte[] recordFrame = envelope.recordFrame();
                recordValidator.accept(recordFrame.clone());
                current.write(recordFrame);
            }
        } catch (StreamChannelCancelledException cancelled) {
            // Attempt cancellation is a normal task lifecycle terminal.
        } catch (StreamChannelFailedException channelFailure) {
            if (!cancellationRequested.get()) {
                recordFailure(channelFailure.getCause());
            }
        } catch (InterruptedException interrupted) {
            if (!cancellationRequested.get()) {
                recordFailure(interrupted);
                Thread.currentThread().interrupt();
            }
        } catch (Throwable thrown) {
            if (!cancellationRequested.get()) recordFailure(thrown);
        } finally {
            CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
            if (pending != null) {
                pending.reject(new IllegalStateException(
                        "Sink task ended before checkpoint barrier"));
            }
            writer = null;
            if (current != null) {
                try {
                    current.close();
                } catch (Throwable closeFailure) {
                    if (!cancellationRequested.get() && failure == null) {
                        recordFailure(closeFailure);
                    } else if (failure != null && failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            completed = true;
        }
    }

    private void recordFailure(Throwable thrown) {
        if (failure != null) return;
        failure = thrown;
        try {
            // Cancel the producer before poisoning the lane can let it race
            // through close without observing the cancellation callback.
            onFailure.accept(thrown);
        } catch (Throwable callbackFailure) {
            if (callbackFailure != thrown) thrown.addSuppressed(callbackFailure);
        } finally {
            mailbox.fail(thrown);
        }
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return mailbox.channelIdentity().runId().equals(checkpoint.runId())
                && mailbox.channelIdentity().attemptId().equals(
                        checkpoint.attemptId())
                && mailbox.channelIdentity().fenceToken()
                == checkpoint.fenceToken();
    }

    private void observeCheckpoint(CheckpointIdentity checkpoint) {
        if (!sameAttempt(checkpoint)) {
            throw new IllegalArgumentException(
                    "checkpoint barrier belongs to another Sink attempt");
        }
        CheckpointIdentity previous = lastCheckpoint;
        if (previous != null && checkpoint.checkpointSequence()
                <= previous.checkpointSequence()) {
            throw new IllegalArgumentException(
                    "Sink checkpoint barrier sequence did not advance");
        }
        StreamCheckpointMember member =
                StreamCheckpointMember.eagerSinkCapability(
                        sinkOperatorUid, subtaskIndex,
                        binding.providerContractDigest(),
                        binding.providerContractDigest());
        lastCheckpointMember = member;
        lastCheckpoint = checkpoint;
        CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
        if (pending != null) {
            if (pending.checkpoint.equals(checkpoint)) {
                pending.result.complete(member);
            } else {
                pending.reject(new IllegalArgumentException(
                        "Sink checkpoint barrier differs from the trigger"));
            }
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
