package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Transport-neutral fair input gate for the portable UNION node. */
final class StreamUnionOperatorTask implements StreamOperatorTask {
    private static final String GATE_STATE = "__rowweave_gate__/inputs";
    private static final String GATE_CODEC =
            "rowweave.stream.multi-input-gate.v1";
    private final PortableStreamNode node;
    private final List<StreamChannelInput> inputs;
    private final StreamMultiInputGate gate;
    private final StreamChannelOutput output;
    private final FileCheckpointPayloadStore payloadStore;
    private final int subtaskIndex;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicLong sequence = new AtomicLong();
    private volatile Throwable failure;
    private volatile Thread thread;
    private volatile boolean completed;
    private CheckpointIdentity lastCheckpoint;
    private List<StreamCheckpointMember> lastCheckpointMembers =
            Collections.emptyList();
    private List<StreamCheckpointMember> terminalMembers =
            Collections.emptyList();
    private CheckpointRequest pendingCheckpoint;

    StreamUnionOperatorTask(PortableStreamNode node,
            List<? extends StreamChannelInput> inputs,
            StreamChannelOutput output) {
        this(node, inputs, output, null, Collections.emptyList(), 0, 0, 0);
    }

    StreamUnionOperatorTask(PortableStreamNode node,
            List<? extends StreamChannelInput> inputs,
            StreamChannelOutput output,
            FileCheckpointPayloadStore payloadStore,
            List<StreamCheckpointMember> restoreMembers,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        this(node, inputs, output, payloadStore, restoreMembers, false,
                subtaskIndex, keyGroupStart, keyGroupEnd);
    }

    StreamUnionOperatorTask(PortableStreamNode node,
            List<? extends StreamChannelInput> inputs,
            StreamChannelOutput output,
            FileCheckpointPayloadStore payloadStore,
            List<StreamCheckpointMember> restoreMembers,
            boolean restoring,
            int subtaskIndex, int keyGroupStart, int keyGroupEnd) {
        if (node == null || inputs == null || inputs.size() < 2
                || output == null || restoreMembers == null
                || subtaskIndex < 0 || keyGroupStart < 0
                || keyGroupEnd < keyGroupStart) {
            throw new IllegalArgumentException(
                    "Stream UNION task requires inputs and output");
        }
        if (restoring && restoreMembers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream UNION restore requires input gate state");
        }
        this.node = node;
        this.inputs = Collections.unmodifiableList(
                new ArrayList<>(inputs));
        this.gate = new StreamMultiInputGate(this.inputs);
        this.output = output;
        this.payloadStore = payloadStore;
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        if (!restoreMembers.isEmpty()) restoreGate(restoreMembers);
    }

    @Override public String operatorUid() { return node.operatorUid(); }

    @Override public StreamTaskResourceUsage resourceUsage() {
        return StreamTaskResourceUsage.zero();
    }

    @Override public void start() {
        if (thread != null) throw new IllegalStateException(
                "Stream UNION task already started");
        thread = new Thread(this::run,
                "rowweave-stream-union-" + node.operatorUid());
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        try {
            boolean sentEnd = false;
            while (!cancelled.get()) {
                StreamMultiInputGate.Event event = gate.take();
                if (event == null) break;
                StreamEnvelope envelope = event.envelope();
                List<StreamCheckpointMember> members =
                        envelope.kind() == StreamEnvelope.Kind.CHECKPOINT_BARRIER
                        || envelope.kind() == StreamEnvelope.Kind.END_OF_INPUT
                        ? snapshotGate() : Collections.emptyList();
                output.send(StreamParallelKeyedOutput.rebind(envelope,
                        output.channelIdentity(),
                        sequence.incrementAndGet()));
                if (envelope.kind()
                        == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
                    observeCheckpoint(envelope.checkpointIdentity(), members);
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.END_OF_INPUT) {
                    terminalMembers = members;
                    sentEnd = true;
                    break;
                }
            }
            if (!cancelled.get()) {
                if (!sentEnd && gate.finished()) {
                    terminalMembers = snapshotGate();
                    output.send(StreamEnvelope.endOfInput(
                            output.channelIdentity(),
                            sequence.incrementAndGet()));
                }
                output.close();
                completeNormally(terminalMembers);
            }
        } catch (Throwable thrown) {
            if (!cancelled.get()) {
                failure = thrown;
                output.fail(thrown);
                failCheckpoint(thrown);
            }
        }
    }

    private List<StreamCheckpointMember> snapshotGate() {
        if (payloadStore == null) return Collections.emptyList();
        Path staged = null;
        try {
            staged = Files.createTempFile("rowweave-stream-union-gate-",
                    ".snapshot");
            Files.write(staged, gate.snapshotState());
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(staged);
            return Collections.singletonList(
                    StreamCheckpointMember.stateHandle(
                            node.operatorUid(), subtaskIndex,
                            keyGroupStart, keyGroupEnd, GATE_STATE,
                            GATE_CODEC, 1, stored.digest(),
                            stored.contentBytes()));
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Stream UNION gate checkpoint failed", failure);
        } finally {
            if (staged != null) {
                try { Files.deleteIfExists(staged); }
                catch (IOException failure) {
                    throw new IllegalStateException(
                            "Stream UNION gate staging cleanup failed",
                            failure);
                }
            }
        }
    }

    private void restoreGate(List<StreamCheckpointMember> members) {
        if (payloadStore == null || members.size() != 1) {
            throw new IllegalArgumentException(
                    "Stream UNION restore gate member is incomplete");
        }
        StreamCheckpointMember member = members.get(0);
        if (member.kind() != StreamCheckpointMember.Kind.STATE
                || !node.operatorUid().equals(member.operatorUid())
                || member.subtaskIndex() != subtaskIndex
                || member.keyGroupStart() != keyGroupStart
                || member.keyGroupEnd() != keyGroupEnd
                || !GATE_STATE.equals(member.subjectId())
                || !GATE_CODEC.equals(member.codecId())
                || member.schemaVersion() != 1) {
            throw new IllegalArgumentException(
                    "Stream UNION restore gate member differs");
        }
        Path restored = null;
        try {
            restored = Files.createTempFile("rowweave-stream-union-gate-",
                    ".restore");
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), restored);
            gate.restoreState(Files.readAllBytes(restored));
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Stream UNION gate restore failed", failure);
        } finally {
            if (restored != null) {
                try { Files.deleteIfExists(restored); }
                catch (IOException failure) {
                    throw new IllegalStateException(
                            "Stream UNION gate restore cleanup failed",
                            failure);
                }
            }
        }
    }

    @Override public void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        for (StreamChannelInput input : inputs) input.cancel();
        output.cancel();
        failCheckpoint(new IllegalStateException("Stream UNION is canceled"));
    }

    @Override public synchronized CompletableFuture<
            List<StreamCheckpointMember>> checkpoint(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint is required");
        }
        if (!sameAttempt(checkpoint)) {
            return failedFuture(new IllegalArgumentException(
                    "Stream UNION checkpoint does not belong to the task"));
        }
        if (checkpoint.equals(lastCheckpoint)) {
            return CompletableFuture.completedFuture(lastCheckpointMembers);
        }
        if (lastCheckpoint != null && checkpoint.checkpointSequence()
                <= lastCheckpoint.checkpointSequence()) {
            return failedFuture(new IllegalArgumentException(
                    "Stream UNION checkpoint sequence was already observed"));
        }
        if (failure != null) return failedFuture(failure);
        if (cancelled.get()) return failedFuture(
                new IllegalStateException("Stream UNION is canceled"));
        if (completed) {
            lastCheckpoint = checkpoint;
            lastCheckpointMembers = terminalMembers;
            return CompletableFuture.completedFuture(terminalMembers);
        }
        if (pendingCheckpoint != null) {
            if (pendingCheckpoint.checkpoint.equals(checkpoint)) {
                return pendingCheckpoint.result;
            }
            return failedFuture(new IllegalStateException(
                    "Stream UNION already has a checkpoint in flight"));
        }
        pendingCheckpoint = new CheckpointRequest(checkpoint);
        return pendingCheckpoint.result;
    }

    private synchronized void observeCheckpoint(
            CheckpointIdentity checkpoint,
            List<StreamCheckpointMember> members) {
        if (!sameAttempt(checkpoint) || lastCheckpoint != null
                && checkpoint.checkpointSequence()
                <= lastCheckpoint.checkpointSequence()) {
            throw new IllegalArgumentException(
                    "Stream UNION checkpoint barrier is stale or foreign");
        }
        lastCheckpoint = checkpoint;
        lastCheckpointMembers = members;
        CheckpointRequest pending = pendingCheckpoint;
        pendingCheckpoint = null;
        if (pending == null) return;
        if (pending.checkpoint.equals(checkpoint)) {
            pending.result.complete(members);
        } else {
            pending.result.completeExceptionally(
                    new IllegalArgumentException(
                            "Stream UNION barrier differs from trigger"));
        }
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return inputs.get(0).channelIdentity().runId().equals(
                checkpoint.runId())
                && inputs.get(0).channelIdentity().attemptId().equals(
                checkpoint.attemptId())
                && inputs.get(0).channelIdentity().fenceToken()
                == checkpoint.fenceToken();
    }

    private synchronized void completeNormally(
            List<StreamCheckpointMember> members) {
        completed = true;
        CheckpointRequest pending = pendingCheckpoint;
        pendingCheckpoint = null;
        if (pending != null) {
            lastCheckpoint = pending.checkpoint;
            lastCheckpointMembers = members;
            pending.result.complete(members);
        }
    }

    private synchronized void failCheckpoint(Throwable thrown) {
        CheckpointRequest pending = pendingCheckpoint;
        pendingCheckpoint = null;
        if (pending != null) pending.result.completeExceptionally(thrown);
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable failure) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(failure);
        return result;
    }

    private static final class CheckpointRequest {
        final CheckpointIdentity checkpoint;
        final CompletableFuture<List<StreamCheckpointMember>> result =
                new CompletableFuture<>();

        private CheckpointRequest(CheckpointIdentity checkpoint) {
            this.checkpoint = checkpoint;
        }
    }

    @Override public Throwable failure() { return failure; }
    @Override public boolean completed() { return completed; }

    @Override public void awaitCompletion(long timeoutMillis)
            throws InterruptedException {
        Thread current = thread;
        if (current != null) current.join(timeoutMillis);
    }
}
