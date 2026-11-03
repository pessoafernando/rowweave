package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSinkBinding;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** One mailbox-owned transactional Sink segment, ending at one aligned barrier. */
public final class LocalTransactionalStreamSinkTask {
    private final String taskName;
    private final String operatorUid;
    private final int subtaskIndex;
    private final StreamSinkBinding binding;
    private final TransactionalStreamSinkProvider provider;
    private final StreamChannelInput mailbox;
    private final Consumer<Throwable> onFailure;
    private final Consumer<byte[]> recordValidator;
    private final boolean terminalInputAlreadyEnded;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();

    private volatile CheckpointIdentity preparedCheckpoint;
    private volatile StreamCheckpointMember preparedMember;
    private volatile boolean completed;
    private volatile Throwable failure;
    private volatile boolean terminalInputSeen;

    public LocalTransactionalStreamSinkTask(
            String taskName, String operatorUid, StreamSinkBinding binding,
            TransactionalStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure,
            Consumer<byte[]> recordValidator) {
        this(taskName, operatorUid, 0, binding, provider, mailbox,
                onFailure, recordValidator);
    }

    public LocalTransactionalStreamSinkTask(
            String taskName, String operatorUid, int subtaskIndex,
            StreamSinkBinding binding,
            TransactionalStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure,
            Consumer<byte[]> recordValidator) {
        this(taskName, operatorUid, subtaskIndex, binding, provider,
                mailbox, onFailure, recordValidator, false);
    }

    public LocalTransactionalStreamSinkTask(
            String taskName, String operatorUid, int subtaskIndex,
            StreamSinkBinding binding,
            TransactionalStreamSinkProvider provider,
            StreamChannelInput mailbox, Consumer<Throwable> onFailure,
            Consumer<byte[]> recordValidator,
            boolean terminalInputAlreadyEnded) {
        if (empty(taskName) || empty(operatorUid) || binding == null
                || subtaskIndex < 0
                || provider == null || mailbox == null || onFailure == null
                || recordValidator == null) {
            throw new IllegalArgumentException(
                    "transactional Sink task configuration is required");
        }
        if (binding.visibilityProfile()
                != StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL
                || !binding.providerKey().equals(provider.providerKey())
                || !binding.providerContractDigest().equals(
                        provider.contractDigest())) {
            throw new IllegalArgumentException(
                    "transactional Sink task provider contract mismatch");
        }
        this.taskName = taskName;
        this.operatorUid = operatorUid;
        this.subtaskIndex = subtaskIndex;
        this.binding = binding;
        this.provider = provider;
        this.mailbox = mailbox;
        this.onFailure = onFailure;
        this.recordValidator = recordValidator;
        this.terminalInputAlreadyEnded = terminalInputAlreadyEnded;
        this.thread = new Thread(this::run,
                "rowweave-stream-transactional-sink-" + taskName);
    }

    public void start() { thread.start(); }

    public synchronized CompletableFuture<StreamCheckpointMember> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null || !sameAttempt(checkpoint)) {
            return rejected(new IllegalArgumentException(
                    "checkpoint does not belong to the transactional Sink task"));
        }
        if (checkpoint.equals(preparedCheckpoint)) {
            return CompletableFuture.completedFuture(preparedMember);
        }
        if (completed || cancellationRequested.get()) {
            return rejected(new IllegalStateException(
                    "transactional Sink segment is already complete"));
        }
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            return rejected(new IllegalStateException(
                    "transactional Sink already has a checkpoint in flight"));
        }
        notifyAll();
        return request.result;
    }

    public void reconcile(CheckpointDecision decision,
            StreamCheckpointAuthority authority) {
        CheckpointIdentity checkpoint = preparedCheckpoint;
        StreamCheckpointMember member = preparedMember;
        if (checkpoint == null || member == null || decision == null
                || !checkpoint.equals(decision.checkpointIdentity())) {
            throw new IllegalArgumentException(
                    "transactional Sink reconcile checkpoint is invalid");
        }
        CheckpointTransactionalSink recovered =
                CheckpointTransactionalSink.recover(
                        operatorUid, subtaskIndex, checkpoint.runId(),
                        checkpoint.attemptId(), checkpoint.fenceToken(),
                        mailbox.channelIdentity().assignmentGeneration(),
                        binding, provider, checkpoint, member);
        try {
            recovered.reconcile(decision, authority);
        } finally {
            recovered.close();
        }
    }

    public void abort(StreamCheckpointAuthority authority) {
        CheckpointIdentity checkpoint = preparedCheckpoint;
        StreamCheckpointMember member = preparedMember;
        if (checkpoint == null || member == null) return;
        CheckpointTransactionalSink recovered =
                CheckpointTransactionalSink.recover(
                        operatorUid, subtaskIndex, checkpoint.runId(),
                        checkpoint.attemptId(), checkpoint.fenceToken(),
                        mailbox.channelIdentity().assignmentGeneration(),
                        binding, provider, checkpoint, member);
        try {
            recovered.abort(authority);
        } finally {
            recovered.close();
        }
    }

    public void cancel() {
        cancellationRequested.set(true);
        thread.interrupt();
    }

    public void awaitCompletion(long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
    }

    public boolean isCompleted() { return completed; }
    public Throwable failure() { return failure; }
    public CheckpointIdentity preparedCheckpoint() { return preparedCheckpoint; }
    public boolean terminalInputSeen() { return terminalInputSeen; }

    private void run() {
        CheckpointTransactionalSink transaction = null;
        try {
            io.rowweave.stream.contract.StreamChannelIdentity channel =
                    mailbox.channelIdentity();
            transaction = new CheckpointTransactionalSink(
                    operatorUid, subtaskIndex, channel.runId(),
                    channel.attemptId(),
                    channel.fenceToken(), channel.assignmentGeneration(),
                    binding, provider);
            if (terminalInputAlreadyEnded) {
                terminalInputSeen = true;
                awaitTerminalCheckpoint(transaction);
                return;
            }
            while (!cancellationRequested.get()) {
                StreamEnvelope envelope = mailbox.receive();
                if (envelope == null) {
                    throw new IllegalStateException(
                            "transactional Sink input ended before a checkpoint");
                }
                if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
                    byte[] frame = envelope.recordFrame();
                    recordValidator.accept(frame.clone());
                    transaction.write(frame);
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
                    observeCheckpoint(transaction,
                            envelope.checkpointIdentity());
                    return;
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.END_OF_INPUT) {
                    terminalInputSeen = true;
                    awaitTerminalCheckpoint(transaction);
                    return;
                } else if (envelope.kind() != StreamEnvelope.Kind.WATERMARK
                        && envelope.kind()
                        != StreamEnvelope.Kind.WATERMARK_STATUS) {
                    throw new UnsupportedOperationException(
                            "transactional Sink received an unsupported signal");
                }
            }
        } catch (StreamChannelCancelledException cancelled) {
            if (!cancellationRequested.get()) recordFailure(cancelled);
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
            CheckpointRequest pending;
            synchronized (this) {
                completed = true;
                pending = pendingCheckpoint.getAndSet(null);
            }
            if (pending != null) {
                pending.result.completeExceptionally(new IllegalStateException(
                        "transactional Sink ended before checkpoint barrier"));
            }
            if (transaction != null) transaction.close();
        }
    }

    private void awaitTerminalCheckpoint(
            CheckpointTransactionalSink transaction)
            throws InterruptedException {
        CheckpointRequest request;
        synchronized (this) {
            while ((request = pendingCheckpoint.get()) == null
                    && !cancellationRequested.get()) {
                wait();
            }
        }
        if (!cancellationRequested.get()) {
            // END is emitted only after all physical inputs have drained.
            // A terminal Source returns its position on the next trigger but
            // cannot emit another barrier, so this is the final logical cut.
            observeCheckpoint(transaction, request.checkpoint);
        }
    }

    private void observeCheckpoint(
            CheckpointTransactionalSink transaction,
            CheckpointIdentity checkpoint) {
        if (!sameAttempt(checkpoint)) {
            throw new IllegalArgumentException(
                    "transactional Sink barrier belongs to another attempt");
        }
        StreamCheckpointMember member = transaction.prepare(checkpoint);
        CheckpointRequest request;
        synchronized (this) {
            request = pendingCheckpoint.get();
            if (request != null && !request.checkpoint.equals(checkpoint)) {
                throw new IllegalArgumentException(
                        "transactional Sink barrier differs from its trigger");
            }
            pendingCheckpoint.set(null);
            preparedCheckpoint = checkpoint;
            preparedMember = member;
        }
        if (request != null) request.result.complete(member);
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        io.rowweave.stream.contract.StreamChannelIdentity channel =
                mailbox.channelIdentity();
        return channel.runId().equals(checkpoint.runId())
                && channel.attemptId().equals(checkpoint.attemptId())
                && channel.fenceToken() == checkpoint.fenceToken();
    }

    private void recordFailure(Throwable thrown) {
        if (failure != null) return;
        failure = thrown;
        try {
            onFailure.accept(thrown);
        } catch (Throwable callbackFailure) {
            if (callbackFailure != thrown) thrown.addSuppressed(callbackFailure);
        } finally {
            mailbox.fail(thrown);
        }
    }

    private static <T> CompletableFuture<T> rejected(Throwable failure) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(failure);
        return result;
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    private static final class CheckpointRequest {
        private final CheckpointIdentity checkpoint;
        private final CompletableFuture<StreamCheckpointMember> result =
                new CompletableFuture<>();
        private CheckpointRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }
    }
}
