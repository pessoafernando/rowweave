package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Resident source task; provider calls are owned by its dedicated thread. */
public final class LocalStreamSourceTask {
    private final DurableStreamSourceProvider provider;
    private final StreamSourceOpenRequest openRequest;
    private final String sourceOperatorUid;
    private final StreamSourceBinding binding;
    private final StreamSourceSplit split;
    private final boolean coordinatorManaged;
    private final boolean declaredBoundedTermination;
    private final java.util.concurrent.ArrayBlockingQueue<StreamSourceSplit>
            dynamicAssignments;
    private final StreamSourceCheckpointRuntime checkpointRuntime;
    private final long idleTimeoutMillis;
    private final StreamChannelOutput output;
    private final Thread thread;
    private final Thread idleWatchdog;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<CheckpointRequest> pendingCheckpoint =
            new AtomicReference<>();
    private final AtomicReference<StreamSourceSplit> completedDynamicSplit =
            new AtomicReference<>();
    private final AtomicReference<SplitRevocation> splitRevocation =
            new AtomicReference<>();
    private final Object startupLock = new Object();
    private final CompletableFuture<Void> readiness = new CompletableFuture<>();
    private volatile StreamSourceReader reader;
    private boolean initialOpeningStarted;
    private boolean initialSplitRevoked;
    private volatile StreamSourceSplit activeDynamicSplit;
    private volatile long lastProgressNanos;
    private volatile boolean watermarkIdle;
    private long watermarkStatusGeneration;
    private long lastWatermark = Long.MIN_VALUE;
    private volatile Throwable watchdogFailure;
    private volatile long lastCheckpointSequence;
    private volatile StreamCheckpointMember terminalCheckpointMember;
    private volatile boolean completed;
    private volatile Throwable failure;

    public LocalStreamSourceTask(
            String taskName, String sourceOperatorUid,
            StreamSourceBinding binding, StreamSourceSplit split,
            DurableStreamSourceProvider provider,
            StreamChannelOutput output) {
        this(taskName, sourceOperatorUid, binding, split, provider, output,
                null, 0L);
    }

    public LocalStreamSourceTask(
            String taskName, String sourceOperatorUid,
            StreamSourceBinding binding, StreamSourceSplit split,
            DurableStreamSourceProvider provider, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime) {
        this(taskName, sourceOperatorUid, binding, split, provider, output,
                checkpointRuntime, 0L);
    }

    public LocalStreamSourceTask(
            String taskName, String sourceOperatorUid,
            StreamSourceBinding binding, StreamSourceSplit split,
            DurableStreamSourceProvider provider, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            long idleTimeoutMillis) {
        this(taskName, sourceOperatorUid, binding, split, provider, output,
                checkpointRuntime, idleTimeoutMillis,
                binding != null && binding.dynamicDiscovery());
    }

    public LocalStreamSourceTask(
            String taskName, String sourceOperatorUid,
            StreamSourceBinding binding, StreamSourceSplit split,
            DurableStreamSourceProvider provider, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            long idleTimeoutMillis, boolean coordinatorManaged) {
        this(taskName, sourceOperatorUid, binding, split, provider, output,
                checkpointRuntime, idleTimeoutMillis, coordinatorManaged, false);
    }

    public LocalStreamSourceTask(
            String taskName, String sourceOperatorUid,
            StreamSourceBinding binding, StreamSourceSplit split,
            DurableStreamSourceProvider provider, StreamChannelOutput output,
            StreamSourceCheckpointRuntime checkpointRuntime,
            long idleTimeoutMillis, boolean coordinatorManaged,
            boolean declaredBoundedTermination) {
        if (taskName == null || taskName.isEmpty()
                || sourceOperatorUid == null || sourceOperatorUid.isEmpty()
                || binding == null
                || provider == null || output == null
                || idleTimeoutMillis < 0L) {
            throw new IllegalArgumentException(
                    "source task identity, contract and runtime are required");
        }
        if (!binding.providerKey().equals(provider.providerKey())
                || !binding.providerContractDigest().equals(
                        provider.contractDigest())) {
            throw new IllegalArgumentException(
                    "Stream source provider contract mismatch");
        }
        if (split != null && !binding.dynamicDiscovery()
                && !binding.fixedSplits().contains(split)) {
            throw new IllegalArgumentException(
                    "Stream source split is not frozen in the binding");
        }
        this.provider = provider;
        this.sourceOperatorUid = sourceOperatorUid;
        this.binding = binding;
        this.split = split;
        this.coordinatorManaged = coordinatorManaged;
        this.declaredBoundedTermination = declaredBoundedTermination;
        this.dynamicAssignments = coordinatorManaged
                ? new java.util.concurrent.ArrayBlockingQueue<>(1) : null;
        this.checkpointRuntime = checkpointRuntime;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.output = output;
        this.openRequest = new StreamSourceOpenRequest(
                sourceOperatorUid, binding, split, output.channelIdentity());
        this.thread = new Thread(this::runSource,
                "rowweave-stream-source-" + taskName);
        this.idleWatchdog = idleTimeoutMillis == 0L ? null : new Thread(
                this::runIdleWatchdog,
                "rowweave-stream-source-watchdog-" + taskName);
    }

    /** Delivers one coordinator-fenced split to an idle dynamic Source task. */
    public void assignDynamicSplit(StreamSourceSplit assignment) {
        if (dynamicAssignments == null || assignment == null) {
            throw new IllegalArgumentException(
                    "dynamic source split assignment is invalid");
        }
        StreamSourceSplit previous = completedDynamicSplit.getAndSet(null);
        if (!dynamicAssignments.offer(assignment)) {
            completedDynamicSplit.compareAndSet(null, previous);
            throw new IllegalStateException(
                    "dynamic source task already has a pending split");
        }
        if (previous != null) terminalCheckpointMember = null;
    }

    /** Returns the last normally completed dynamic split until reassignment. */
    public StreamSourceSplit completedDynamicSplit() {
        return completedDynamicSplit.get();
    }

    /** Stops a coordinator-owned split and completes only after its reader closes. */
    public CompletableFuture<Void> revokeDynamicSplit(String splitId) {
        if (!coordinatorManaged || splitId == null || splitId.isEmpty()) {
            throw new IllegalArgumentException(
                    "dynamic source revocation is invalid");
        }
        synchronized (startupLock) {
            if (!initialOpeningStarted && !initialSplitRevoked
                    && split != null
                    && splitId.equals(split.splitId())) {
                initialSplitRevoked = true;
                completedDynamicSplit.set(null);
                terminalCheckpointMember = null;
                return CompletableFuture.completedFuture(null);
            }
        }
        SplitRevocation request = new SplitRevocation(splitId);
        if (!splitRevocation.compareAndSet(null, request)) {
            SplitRevocation existing = splitRevocation.get();
            if (existing != null && existing.splitId.equals(splitId)) {
                return existing.done;
            }
            throw new IllegalStateException(
                    "another source split revocation is pending");
        }
        StreamSourceSplit active = activeDynamicSplit;
        StreamSourceSplit finished = completedDynamicSplit.get();
        if (finished != null && splitId.equals(finished.splitId())
                && reader == null) {
            completedDynamicSplit.set(null);
            terminalCheckpointMember = null;
            finishRevocation(request);
            return request.done;
        }
        if (active == null) {
            for (StreamSourceSplit queued : dynamicAssignments) {
                if (splitId.equals(queued.splitId())
                        && dynamicAssignments.remove(queued)) {
                    completedDynamicSplit.set(null);
                    terminalCheckpointMember = null;
                    finishRevocation(request);
                    return request.done;
                }
            }
        } else if (!splitId.equals(active.splitId())) {
            splitRevocation.compareAndSet(request, null);
            throw new IllegalStateException(
                    "source revocation differs from active split");
        }
        StreamSourceReader current = reader;
        if (current != null) {
            try {
                current.cancel();
            } catch (RuntimeException failure) {
                request.done.completeExceptionally(failure);
            }
        }
        return request.done;
    }

    private boolean isRevoked(StreamSourceSplit split) {
        SplitRevocation request = splitRevocation.get();
        return request != null && request.splitId.equals(split.splitId());
    }

    private void finishRevocation(SplitRevocation request) {
        splitRevocation.compareAndSet(request, null);
        request.done.complete(null);
    }

    public void start() {
        lastProgressNanos = System.nanoTime();
        if (idleWatchdog != null) idleWatchdog.start();
        thread.start();
    }

    public CompletableFuture<StreamCheckpointMember> triggerCheckpoint(
            CheckpointIdentity checkpoint) {
        CheckpointRequest request = new CheckpointRequest(checkpoint);
        if (checkpointRuntime == null) {
            request.reject(new UnsupportedOperationException(
                    "source task was not configured for checkpoints"));
            return request.result;
        }
        if (!sameAttempt(checkpoint)
                || !checkpointRuntime.artifactId().equals(
                        checkpoint.artifactId())) {
            request.reject(failure(
                    StreamCheckpointException.Code.CHECKPOINT_MISMATCH,
                    "checkpoint does not belong to the source task attempt"));
            return request.result;
        }
        if (checkpoint.checkpointSequence() <= lastCheckpointSequence) {
            request.reject(failure(
                    StreamCheckpointException.Code.CHECKPOINT_SEQUENCE_REPLAY,
                    "source checkpoint sequence was already emitted"));
            return request.result;
        }
        if (completed) {
            completeTerminalCheckpoint(request);
            return request.result;
        }
        if (!pendingCheckpoint.compareAndSet(null, request)) {
            request.reject(failure(
                    StreamCheckpointException.Code.CHECKPOINT_IN_FLIGHT,
                    "source task already has a checkpoint in flight"));
            return request.result;
        }
        StreamSourceReader current = reader;
        if (current != null) {
            try {
                current.wakeUp();
            } catch (Throwable wakeFailure) {
                if (pendingCheckpoint.compareAndSet(request, null)) {
                    request.reject(wakeFailure);
                }
            }
        }
        return request.result;
    }

    private StreamCheckpointMember idleCheckpointMember() {
        return StreamCheckpointMember.sourcePosition(
                sourceOperatorUid, checkpointRuntime == null ? 0
                        : checkpointRuntime.subtaskIndex(),
                "idle-source", "idle-v1", 0L,
                "0000000000000000000000000000000000000000000000000000000000000000",
                0L);
    }

    public void cancel() {
        if (cancellationRequested.compareAndSet(false, true)) {
            StreamSourceReader current = reader;
            output.cancel();
            try {
                if (current != null) current.cancel();
            } finally {
                thread.interrupt();
                if (idleWatchdog != null) idleWatchdog.interrupt();
            }
        }
    }

    public void awaitCompletion(long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
        if (idleWatchdog != null) idleWatchdog.join(timeoutMillis);
    }

    void awaitReady() {
        readiness.join();
    }

    public boolean isCompleted() { return completed; }
    public Throwable failure() { return failure; }

    private void runSource() {
        StreamSourceReader current = null;
        try {
            if (cancellationRequested.get()) return;
            StreamSourceSplit activeSplit;
            synchronized (startupLock) {
                activeSplit = initialSplitRevoked ? null : split;
                initialOpeningStarted = true;
            }
            long sequence = 0L;
            boolean restorePending = checkpointRuntime != null
                    && checkpointRuntime.restoreMember() != null
                    && activeSplit != null;
            if (coordinatorManaged) readiness.complete(null);
            while (!cancellationRequested.get()) {
                if (activeSplit == null && coordinatorManaged) {
                    reader = null;
                    while (activeSplit == null
                            && !cancellationRequested.get()) {
                        long afterCheckpoint = emitPendingIdleCheckpoint(
                                sequence);
                        if (afterCheckpoint != sequence) {
                            sequence = afterCheckpoint;
                        }
                        activeSplit = dynamicAssignments.poll(
                                50L, java.util.concurrent.TimeUnit.MILLISECONDS);
                    }
                    if (activeSplit == null) return;
                    terminalCheckpointMember = null;
                }
                if (activeSplit == null) {
                    terminalCheckpointMember = idleCheckpointMember();
                    output.send(StreamEnvelope.endOfInput(
                            output.channelIdentity(), sequence + 1L));
                    return;
                }
                if (coordinatorManaged) activeDynamicSplit = activeSplit;
                if (coordinatorManaged && isRevoked(activeSplit)) {
                    SplitRevocation revocation = splitRevocation.get();
                    activeDynamicSplit = null;
                    completedDynamicSplit.set(null);
                    terminalCheckpointMember = null;
                    if (revocation != null) finishRevocation(revocation);
                    restorePending = false;
                    activeSplit = null;
                    continue;
                }
                StreamSourcePositionCheckpoint positions = null;
                StreamSourceOpenRequest request = new StreamSourceOpenRequest(
                        sourceOperatorUid, binding, activeSplit,
                        openRequest.outputChannelIdentity());
                if (restorePending) {
                    positions = checkpointRuntime.positions(
                            sourceOperatorUid, binding, activeSplit, 0L,
                            new byte[] {0});
                    checkpointRuntime.restorePosition(positions);
                    request = new StreamSourceOpenRequest(
                            sourceOperatorUid, binding, activeSplit,
                            openRequest.outputChannelIdentity(),
                            positions.currentPositionFrame(),
                            positions.logicalPosition());
                    restorePending = false;
                }
                current = provider.open(request);
                if (current == null) {
                    throw new IllegalStateException(
                            "Stream source provider returned no reader");
                }
                reader = current;
                if (checkpointRuntime != null && positions == null) {
                    positions = checkpointRuntime.positions(
                            sourceOperatorUid, binding, activeSplit, 0L,
                            current.checkpointPositionFrame());
                }
                readiness.complete(null);
                boolean splitCompleted = false;
                boolean splitRevoked = false;
                long checkpointWakeCredits = 0L;
                while (!cancellationRequested.get()) {
                    if (coordinatorManaged && isRevoked(activeSplit)) {
                        splitRevoked = true;
                        break;
                    }
                    long afterCheckpoint = emitPendingCheckpoint(
                            sequence, positions);
                    if (afterCheckpoint != sequence) {
                        sequence = afterCheckpoint;
                        // The wakeUp can arrive after this barrier was sent.
                        checkpointWakeCredits = Math.addExact(
                                checkpointWakeCredits, 1L);
                        continue;
                    }
                    StreamSourceEvent event;
                    try {
                        event = current.nextEvent();
                    } catch (Exception failure) {
                        if (coordinatorManaged && isRevoked(activeSplit)) {
                            splitRevoked = true;
                            break;
                        }
                        throw failure;
                    }
                    if (coordinatorManaged && isRevoked(activeSplit)) {
                        splitRevoked = true;
                        break;
                    }
                    if (watchdogFailure != null) throw watchdogFailure;
                    if (event == null) {
                        if (cancellationRequested.get()) return;
                        long afterWakeCheckpoint = emitPendingCheckpoint(
                                sequence, positions);
                        if (afterWakeCheckpoint != sequence) {
                            sequence = afterWakeCheckpoint;
                            if (checkpointWakeCredits > 0L) {
                                checkpointWakeCredits--;
                            }
                            continue;
                        }
                        if (checkpointWakeCredits > 0L) {
                            checkpointWakeCredits--;
                            continue;
                        }
                        // A null poll is not an EOF. Only END_OF_INPUT may
                        // complete a declared bounded split; yield so a
                        // non-blocking reader cannot spin at full CPU.
                        Thread.sleep(1L);
                        continue;
                    }
                    if (event.kind() == StreamSourceEvent.Kind.END_OF_INPUT
                            && !declaredBoundedTermination
                            && !coordinatorManaged) {
                        throw new IllegalStateException(
                                "Source emitted undeclared END_OF_INPUT");
                    }
                    validateAndRecordProgress(event);
                    if (event.kind() == StreamSourceEvent.Kind.END_OF_INPUT) {
                        if (positions != null) {
                            terminalCheckpointMember = positions.snapshot();
                        }
                        CheckpointRequest terminal =
                                pendingCheckpoint.getAndSet(null);
                        if (terminal != null) {
                            completeTerminalCheckpoint(terminal);
                        }
                        completedDynamicSplit.set(activeSplit);
                        if (coordinatorManaged) {
                            splitCompleted = true;
                            break;
                        }
                        sequence++;
                        output.send(envelope(event, sequence));
                        return;
                    }
                    sequence++;
                    output.send(envelope(event, sequence));
                    if (positions != null
                            && event.kind() == StreamSourceEvent.Kind.RECORD) {
                        positions.recordEmitted(
                                event.record().positionFrame());
                    }
                }
                closeReader(current);
                current = null;
                reader = null;
                activeDynamicSplit = null;
                if (splitRevoked || (coordinatorManaged
                        && isRevoked(activeSplit))) {
                    completedDynamicSplit.set(null);
                    terminalCheckpointMember = null;
                    SplitRevocation revocation = splitRevocation.get();
                    if (revocation != null) {
                        if (failure == null) finishRevocation(revocation);
                        else revocation.done.completeExceptionally(failure);
                    }
                    activeSplit = null;
                    continue;
                }
                if (failure != null) return;
                if (!splitCompleted) return;
                activeSplit = null;
            }
        } catch (StreamChannelCancelledException cancelled) {
            if (!cancellationRequested.get()) {
                failure = cancelled;
            }
        } catch (StreamChannelFailedException channelFailure) {
            failure = channelFailure.getCause();
        } catch (InterruptedException interrupted) {
            if (!cancellationRequested.get()) {
                fail(watchdogFailure == null
                        ? interrupted : watchdogFailure);
            } else {
                Thread.currentThread().interrupt();
            }
        } catch (Throwable thrown) {
            readiness.completeExceptionally(thrown);
            if (!cancellationRequested.get()) fail(thrown);
        } finally {
            if (!readiness.isDone()) {
                readiness.completeExceptionally(new IllegalStateException(
                        "source task ended before provider readiness"));
            }
            CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
            if (pending != null) {
                if (terminalCheckpointMember == null) {
                    pending.reject(failure(
                            StreamCheckpointException.Code.CALLBACK_FAILURE,
                            "source task ended before checkpoint emission"));
                } else {
                    completeTerminalCheckpoint(pending);
                }
            }
            reader = null;
            if (current != null) closeReader(current);
            SplitRevocation revocation = splitRevocation.getAndSet(null);
            if (revocation != null && !revocation.done.isDone()) {
                revocation.done.completeExceptionally(failure == null
                        ? new IllegalStateException(
                                "source task ended before split revocation")
                        : failure);
            }
            completed = true;
            if (idleWatchdog != null) idleWatchdog.interrupt();
        }
    }

    private void closeReader(StreamSourceReader closing) {
        try {
            closing.close();
        } catch (Throwable closeFailure) {
            if (!cancellationRequested.get() && failure == null) {
                fail(closeFailure);
            } else if (failure != null && failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private void validateAndRecordProgress(StreamSourceEvent event) {
        lastProgressNanos = System.nanoTime();
        if (event.kind() == StreamSourceEvent.Kind.WATERMARK_STATUS) {
            long generation = event.watermarkStatusGeneration();
            boolean nextIdle = event.watermarkStatus()
                    == StreamEnvelope.WatermarkStatus.IDLE;
            if (generation != watermarkStatusGeneration + 1L
                    || nextIdle == watermarkIdle) {
                throw new StreamTimeException(
                        StreamTimeException.Code.STATUS_TRANSITION,
                        "source watermark status transition is noncanonical");
            }
            watermarkStatusGeneration = generation;
            watermarkIdle = nextIdle;
            return;
        }
        if (event.kind() == StreamSourceEvent.Kind.END_OF_INPUT) return;
        if (idleTimeoutMillis == 0L) return;
        if (watermarkIdle) {
            throw new StreamTimeException(
                    StreamTimeException.Code.STATUS_TRANSITION,
                    "idle source emitted data before becoming ACTIVE");
        }
        if (event.kind() == StreamSourceEvent.Kind.WATERMARK) {
            if (event.watermark() < lastWatermark) {
                throw new StreamTimeException(
                        StreamTimeException.Code.INPUT_WATERMARK_REGRESSION,
                        "source watermark moved backwards");
            }
            lastWatermark = event.watermark();
        } else if (!event.record().hasEventTimestamp()
                || event.record().eventTimestamp() <= lastWatermark) {
            throw new StreamTimeException(
                    StreamTimeException.Code.INPUT_WATERMARK_REGRESSION,
                    "event-time source emitted a missing or late timestamp");
        }
    }

    private void runIdleWatchdog() {
        long timeoutNanos;
        try {
            timeoutNanos = Math.multiplyExact(idleTimeoutMillis, 1_000_000L);
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        try {
            while (!cancellationRequested.get() && !completed) {
                if (watermarkIdle) {
                    Thread.sleep(Math.min(idleTimeoutMillis, 100L));
                    continue;
                }
                long elapsed = System.nanoTime() - lastProgressNanos;
                long remaining = timeoutNanos - elapsed;
                if (remaining > 0L) {
                    long waitMillis = Math.max(1L, Math.min(100L,
                            (remaining + 999_999L) / 1_000_000L));
                    Thread.sleep(waitMillis);
                    continue;
                }
                StreamTimeException timeout = new StreamTimeException(
                        StreamTimeException.Code.IDLE_TIMEOUT,
                        "ACTIVE event-time source exceeded its idle timeout");
                watchdogFailure = timeout;
                StreamSourceReader current = reader;
                if (current != null) current.wakeUp();
                thread.interrupt();
                return;
            }
        } catch (InterruptedException stopped) {
            if (!cancellationRequested.get() && !completed
                    && watchdogFailure == null) {
                Thread.currentThread().interrupt();
            }
        } catch (Throwable wakeFailure) {
            watchdogFailure = wakeFailure;
            thread.interrupt();
        }
    }

    private void fail(Throwable thrown) {
        failure = thrown;
        CheckpointRequest pending = pendingCheckpoint.getAndSet(null);
        if (pending != null) pending.reject(thrown);
        output.fail(thrown);
    }

    private long emitPendingCheckpoint(
            long sequence, StreamSourcePositionCheckpoint positions) {
        CheckpointRequest request = pendingCheckpoint.getAndSet(null);
        if (request == null) return sequence;
        try {
            if (positions == null) {
                throw new UnsupportedOperationException(
                        "source position checkpoint runtime is unavailable");
            }
            if (request.checkpoint.checkpointSequence()
                    <= lastCheckpointSequence) {
                throw failure(
                        StreamCheckpointException.Code.CHECKPOINT_SEQUENCE_REPLAY,
                        "source checkpoint sequence was already emitted");
            }
            StreamCheckpointMember member = positions.snapshot();
            long barrierSequence = Math.addExact(sequence, 1L);
            output.send(StreamEnvelope.checkpointBarrier(
                    openRequest.outputChannelIdentity(), barrierSequence,
                    request.checkpoint));
            lastCheckpointSequence = request.checkpoint.checkpointSequence();
            request.result.complete(member);
            return barrierSequence;
        } catch (RuntimeException | Error checkpointFailure) {
            request.reject(checkpointFailure);
            throw checkpointFailure;
        }
    }

    private long emitPendingIdleCheckpoint(long sequence) {
        CheckpointRequest request = pendingCheckpoint.getAndSet(null);
        if (request == null) return sequence;
        try {
            if (request.checkpoint.checkpointSequence()
                    <= lastCheckpointSequence) {
                throw failure(
                        StreamCheckpointException.Code
                                .CHECKPOINT_SEQUENCE_REPLAY,
                        "source checkpoint sequence was already emitted");
            }
            long barrierSequence = Math.addExact(sequence, 1L);
            output.send(StreamEnvelope.checkpointBarrier(
                    openRequest.outputChannelIdentity(), barrierSequence,
                    request.checkpoint));
            lastCheckpointSequence = request.checkpoint.checkpointSequence();
            request.result.complete(terminalCheckpointMember == null
                    ? idleCheckpointMember() : terminalCheckpointMember);
            return barrierSequence;
        } catch (RuntimeException | Error checkpointFailure) {
            request.reject(checkpointFailure);
            throw checkpointFailure;
        }
    }

    private synchronized void completeTerminalCheckpoint(
            CheckpointRequest request) {
        StreamCheckpointMember member = terminalCheckpointMember;
        if (member == null) {
            request.reject(new IllegalStateException(
                    "source task is already complete"));
            return;
        }
        if (request.checkpoint.checkpointSequence()
                <= lastCheckpointSequence) {
            request.reject(failure(
                    StreamCheckpointException.Code.CHECKPOINT_SEQUENCE_REPLAY,
                    "source checkpoint sequence was already emitted"));
            return;
        }
        lastCheckpointSequence = request.checkpoint.checkpointSequence();
        request.result.complete(member);
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return checkpoint != null
                && openRequest.outputChannelIdentity().runId().equals(
                        checkpoint.runId())
                && openRequest.outputChannelIdentity().attemptId().equals(
                        checkpoint.attemptId())
                && openRequest.outputChannelIdentity().fenceToken()
                == checkpoint.fenceToken();
    }

    private StreamEnvelope envelope(StreamSourceEvent event, long sequence) {
        switch (event.kind()) {
            case RECORD:
                StreamSourceRecord record = event.record();
                return record.hasEventTimestamp()
                        ? StreamEnvelope.record(
                                openRequest.outputChannelIdentity(), sequence,
                                record.eventTimestamp(), record.recordFrame())
                        : StreamEnvelope.record(
                                openRequest.outputChannelIdentity(), sequence,
                                record.recordFrame());
            case WATERMARK:
                return StreamEnvelope.watermark(
                        openRequest.outputChannelIdentity(), sequence,
                        event.watermark());
            case WATERMARK_STATUS:
                return StreamEnvelope.watermarkStatus(
                        openRequest.outputChannelIdentity(), sequence,
                        event.watermarkStatus(),
                        event.watermarkStatusGeneration());
            case END_OF_INPUT:
                return StreamEnvelope.endOfInput(
                        openRequest.outputChannelIdentity(), sequence);
            default:
                throw new IllegalArgumentException(
                        "unsupported Stream source event kind");
        }
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static final class CheckpointRequest {
        private final CheckpointIdentity checkpoint;
        private final CompletableFuture<StreamCheckpointMember> result =
                new CompletableFuture<>();

        private CheckpointRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }

        private void reject(Throwable failure) {
            result.completeExceptionally(failure);
        }
    }

    private static final class SplitRevocation {
        private final String splitId;
        private final CompletableFuture<Void> done = new CompletableFuture<>();

        private SplitRevocation(String splitId) {
            this.splitId = splitId;
        }
    }
}
