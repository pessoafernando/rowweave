package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamChannelStateCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamChannelInput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * One bounded unaligned checkpoint cut shared by every physical input of a
 * task. The first real barrier exposes one synthetic barrier on every lane,
 * allowing the existing operator lifecycle to snapshot immediately. Frames
 * preceding the remaining real barriers continue through the mailbox and are
 * copied into independently checksummed channel-state members.
 */
final class StreamUnalignedTaskGate {
    private final String operatorUid;
    private final int subtaskIndex;
    private final List<StreamChannelInput> rawInputs;
    private final List<StreamChannelInput> exposedInputs;
    private final FileCheckpointPayloadStore payloadStore;
    private final int maximumEventsPerLane;
    private final long maximumBytesPerLane;
    private final long maximumTotalBytes;
    private final int maximumFrameBytes;
    private final long[] observedPhysicalSequences;
    private final long[] deliveredSequences;
    private final long[] snapshotSequences;
    private final long[] barrierSequences;
    private final long[] capturedBytes;
    private final boolean[] realBarrierSeen;
    private final boolean[] syntheticBarrierPending;
    private final boolean[] ended;
    private final List<List<StreamEnvelope>> captured;
    private final List<ArrayDeque<StreamEnvelope>> replay;
    private final long[] highestReplayStatusGeneration;
    private final long[] physicalStatusGenerationOffset;
    private long remainingReplayFrames;

    private CheckpointIdentity activeCheckpoint;
    private CheckpointIdentity exposedCheckpoint;
    private CheckpointIdentity completedCheckpoint;
    private CompletableFuture<List<StreamCheckpointMember>> activeCut;
    private List<StreamCheckpointMember> completedMembers =
            Collections.emptyList();
    private boolean captureStarted;
    private long totalCapturedBytes;
    private RuntimeException terminalFailure;

    StreamUnalignedTaskGate(String operatorUid, int subtaskIndex,
            List<? extends StreamChannelInput> inputs,
            FileCheckpointPayloadStore payloadStore,
            List<StreamCheckpointMember> restoreMembers,
            boolean restoring, int maximumEventsPerLane,
            long maximumBytesPerLane,
            long maximumTotalBytes, int maximumFrameBytes) {
        if (operatorUid == null || operatorUid.isEmpty() || subtaskIndex < 0
                || inputs == null || inputs.isEmpty() || payloadStore == null
                || restoreMembers == null || maximumEventsPerLane < 1
                || maximumBytesPerLane < 1L || maximumTotalBytes < 1L
                || maximumFrameBytes < 1) {
            throw new IllegalArgumentException(
                    "unaligned task gate requires bounded inputs and storage");
        }
        this.operatorUid = operatorUid;
        this.subtaskIndex = subtaskIndex;
        this.payloadStore = payloadStore;
        this.maximumEventsPerLane = maximumEventsPerLane;
        this.maximumBytesPerLane = maximumBytesPerLane;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumFrameBytes = maximumFrameBytes;
        List<StreamChannelInput> copied = new ArrayList<>(inputs.size());
        List<StreamChannelInput> exposed = new ArrayList<>(inputs.size());
        Set<String> channelIds = new HashSet<>();
        String runId = null;
        String attemptId = null;
        long fenceToken = -1L;
        for (int index = 0; index < inputs.size(); index++) {
            StreamChannelInput input = inputs.get(index);
            if (input == null
                    || !channelIds.add(input.channelIdentity().channelId())) {
                throw new IllegalArgumentException(
                        "unaligned task input identities must be unique");
            }
            StreamChannelIdentity identity = input.channelIdentity();
            if (runId == null) {
                runId = identity.runId();
                attemptId = identity.attemptId();
                fenceToken = identity.fenceToken();
            } else if (!runId.equals(identity.runId())
                    || !attemptId.equals(identity.attemptId())
                    || fenceToken != identity.fenceToken()) {
                throw new IllegalArgumentException(
                        "unaligned task inputs belong to different attempts");
            }
            copied.add(input);
            exposed.add(new GatedInput(index, input));
        }
        this.rawInputs = Collections.unmodifiableList(copied);
        this.exposedInputs = Collections.unmodifiableList(exposed);
        this.observedPhysicalSequences = new long[inputs.size()];
        this.deliveredSequences = new long[inputs.size()];
        this.snapshotSequences = new long[inputs.size()];
        this.barrierSequences = new long[inputs.size()];
        this.capturedBytes = new long[inputs.size()];
        this.realBarrierSeen = new boolean[inputs.size()];
        this.syntheticBarrierPending = new boolean[inputs.size()];
        this.ended = new boolean[inputs.size()];
        this.captured = new ArrayList<>(inputs.size());
        this.replay = new ArrayList<>(inputs.size());
        this.highestReplayStatusGeneration = new long[inputs.size()];
        this.physicalStatusGenerationOffset = new long[inputs.size()];
        java.util.Arrays.fill(physicalStatusGenerationOffset, -1L);
        for (int index = 0; index < inputs.size(); index++) {
            captured.add(new ArrayList<StreamEnvelope>());
            replay.add(new ArrayDeque<StreamEnvelope>());
        }
        restore(restoreMembers, restoring);
    }

    List<StreamChannelInput> inputs() { return exposedInputs; }

    StreamOperatorTask wrap(StreamOperatorTask delegate) {
        if (delegate == null || !operatorUid.equals(delegate.operatorUid())) {
            throw new IllegalArgumentException(
                    "unaligned task delegate identity differs");
        }
        return new WrappedTask(delegate);
    }

    synchronized CompletableFuture<List<StreamCheckpointMember>> checkpoint(
            CheckpointIdentity checkpoint) {
        if (terminalFailure != null) return failed(terminalFailure);
        if (checkpoint == null || !sameAttempt(checkpoint)) {
            return failed(new IllegalArgumentException(
                    "unaligned checkpoint does not belong to the task"));
        }
        if (checkpoint.equals(completedCheckpoint)) {
            return CompletableFuture.completedFuture(completedMembers);
        }
        if (completedCheckpoint != null
                && checkpoint.checkpointSequence()
                <= completedCheckpoint.checkpointSequence()) {
            return failed(new IllegalArgumentException(
                    "unaligned checkpoint sequence was already completed"));
        }
        if (activeCheckpoint == null) {
            activeCheckpoint = checkpoint;
            activeCut = new CompletableFuture<>();
            CompletableFuture<List<StreamCheckpointMember>> cut = activeCut;
            if (allEnded()) beginCapture();
            return cut;
        } else if (!activeCheckpoint.equals(checkpoint)) {
            return failed(new IllegalStateException(
                    "unaligned task already has a checkpoint in flight"));
        }
        return activeCut;
    }

    private StreamEnvelope next(int index, long timeoutMillis,
            boolean blocking) throws InterruptedException {
        while (true) {
            StreamEnvelope pending;
            synchronized (this) {
                requireHealthy();
                pending = replay.get(index).pollFirst();
                if (pending != null) {
                    remainingReplayFrames--;
                    if (remainingReplayFrames == 0L) notifyAll();
                    return deliverReplay(index, pending);
                }
                if (remainingReplayFrames > 0L) {
                    if (!blocking) return null;
                    wait();
                    continue;
                }
                if (syntheticBarrierPending[index]) {
                    syntheticBarrierPending[index] = false;
                    CheckpointIdentity checkpoint = exposedCheckpoint;
                    if (checkpoint == null) {
                        throw poison(new IllegalStateException(
                                "unaligned logical barrier lost its checkpoint"));
                    }
                    long sequence = nextDeliveredSequence(index);
                    StreamEnvelope barrier = StreamEnvelope.checkpointBarrier(
                            rawInputs.get(index).channelIdentity(),
                            sequence, checkpoint);
                    deliveredSequences[index] = sequence;
                    if (!hasSyntheticBarrier()) exposedCheckpoint = null;
                    return barrier;
                }
            }
            StreamEnvelope envelope = blocking
                    ? rawInputs.get(index).receive()
                    : rawInputs.get(index).poll(timeoutMillis);
            if (envelope == null) return null;
            synchronized (this) {
                requireHealthy();
                if (!rawInputs.get(index).channelIdentity().equals(
                        envelope.channelIdentity())) {
                    throw poison(new IllegalArgumentException(
                            "unaligned input envelope belongs to another lane"));
                }
                if (envelope.kind()
                        == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
                    acceptBarrier(index, envelope);
                    continue;
                }
                return deliverPhysical(index, envelope);
            }
        }
    }

    private StreamEnvelope deliverPhysical(int index,
            StreamEnvelope envelope) {
        observePhysical(index, envelope);
        boolean terminal = envelope.kind()
                == StreamEnvelope.Kind.END_OF_INPUT;
        if (captureStarted && !realBarrierSeen[index] && !terminal) {
            capture(index, envelope);
        }
        StreamEnvelope rebound = deliverRebound(index,
                rebasePhysicalStatus(index, envelope));
        if (terminal) {
            ended[index] = true;
            if (captureStarted && !realBarrierSeen[index]) {
                // A terminal Source cannot emit a later physical barrier.
                // END is the lane's virtual cut marker, never replay data.
                realBarrierSeen[index] = true;
                barrierSequences[index] = envelope.sequence();
                if (allBarriered()) completeCapture();
            } else if (activeCheckpoint != null && !captureStarted
                    && allEnded()) {
                beginCapture();
            }
        }
        return rebound;
    }

    private StreamEnvelope deliverReplay(int index,
            StreamEnvelope envelope) {
        return deliverRebound(index, envelope);
    }

    private StreamEnvelope rebasePhysicalStatus(int index,
            StreamEnvelope envelope) {
        if (envelope.kind() != StreamEnvelope.Kind.WATERMARK_STATUS) {
            return envelope;
        }
        long generation = envelope.watermarkStatusGeneration();
        if (physicalStatusGenerationOffset[index] < 0L) {
            long previous = highestReplayStatusGeneration[index];
            physicalStatusGenerationOffset[index] = generation > previous
                    ? 0L : Math.subtractExact(Math.addExact(previous, 1L),
                    generation);
        }
        long adjusted = Math.addExact(generation,
                physicalStatusGenerationOffset[index]);
        return StreamEnvelope.watermarkStatus(
                envelope.channelIdentity(), envelope.sequence(),
                envelope.watermarkStatus(), adjusted);
    }

    private StreamEnvelope deliverRebound(int index,
            StreamEnvelope envelope) {
        long sequence = nextDeliveredSequence(index);
        StreamEnvelope rebound = rebind(envelope,
                rawInputs.get(index).channelIdentity(), sequence);
        deliveredSequences[index] = sequence;
        return rebound;
    }

    private long nextDeliveredSequence(int index) {
        try {
            return Math.addExact(deliveredSequences[index], 1L);
        } catch (ArithmeticException overflow) {
            throw poison(new IllegalStateException(
                    "unaligned logical input sequence overflowed", overflow));
        }
    }

    private void observePhysical(int index, StreamEnvelope envelope) {
        requireNextPhysical(index, envelope);
        observedPhysicalSequences[index] = envelope.sequence();
    }

    private void requireNextPhysical(int index, StreamEnvelope envelope) {
        long expected;
        try {
            expected = Math.addExact(
                    observedPhysicalSequences[index], 1L);
        } catch (ArithmeticException overflow) {
            throw poison(new IllegalStateException(
                    "unaligned physical input sequence overflowed", overflow));
        }
        if (envelope.sequence() != expected) {
            throw poison(new IllegalArgumentException(
                    "unaligned physical input sequence is not contiguous"));
        }
    }

    private void acceptBarrier(int index, StreamEnvelope envelope) {
        requireNextPhysical(index, envelope);
        CheckpointIdentity checkpoint = envelope.checkpointIdentity();
        if (!sameAttempt(checkpoint)) {
            throw poison(new IllegalArgumentException(
                    "unaligned barrier belongs to another task attempt"));
        }
        if (activeCheckpoint == null) {
            activeCheckpoint = checkpoint;
            activeCut = new CompletableFuture<>();
        } else if (!activeCheckpoint.equals(checkpoint)) {
            throw poison(new IllegalArgumentException(
                    "unaligned barrier differs from the active checkpoint"));
        }
        if (realBarrierSeen[index]) {
            throw poison(new IllegalArgumentException(
                    "unaligned input repeated its checkpoint barrier"));
        }
        if (!captureStarted) beginCapture();
        observedPhysicalSequences[index] = envelope.sequence();
        realBarrierSeen[index] = true;
        barrierSequences[index] = envelope.sequence();
        if (allBarriered()) completeCapture();
    }

    private void beginCapture() {
        if (captureStarted) return;
        captureStarted = true;
        exposedCheckpoint = activeCheckpoint;
        for (int index = 0; index < rawInputs.size(); index++) {
            snapshotSequences[index] = observedPhysicalSequences[index];
            if (ended[index]) {
                realBarrierSeen[index] = true;
                barrierSequences[index] = nextPhysicalSequence(index);
            } else {
                syntheticBarrierPending[index] = true;
            }
        }
        if (allBarriered()) completeCapture();
    }

    private long nextPhysicalSequence(int index) {
        try {
            return Math.addExact(observedPhysicalSequences[index], 1L);
        } catch (ArithmeticException overflow) {
            throw poison(new IllegalStateException(
                    "unaligned barrier sequence overflowed", overflow));
        }
    }

    private void capture(int index, StreamEnvelope envelope) {
        if (envelope.kind() == StreamEnvelope.Kind.CANCEL
                || envelope.kind() == StreamEnvelope.Kind.ERROR
                || envelope.kind() == StreamEnvelope.Kind.END_OF_INPUT) {
            throw poison(new IllegalStateException(
                    "terminal event cannot precede an unaligned barrier"));
        }
        byte[] frame = StreamEnvelopeCodec.encode(envelope);
        long laneBytes;
        long allBytes;
        try {
            laneBytes = Math.addExact(capturedBytes[index], frame.length);
            allBytes = Math.addExact(totalCapturedBytes, frame.length);
        } catch (ArithmeticException overflow) {
            throw poison(limit("unaligned channel-state bytes overflowed"));
        }
        if (frame.length > maximumFrameBytes
                || captured.get(index).size() >= maximumEventsPerLane
                || laneBytes > maximumBytesPerLane
                || allBytes > maximumTotalBytes) {
            throw poison(limit(
                    "unaligned channel state exceeds its resource lease"));
        }
        captured.get(index).add(envelope);
        capturedBytes[index] = laneBytes;
        totalCapturedBytes = allBytes;
    }

    private void completeCapture() {
        try {
            List<StreamCheckpointMember> members = new ArrayList<>(
                    rawInputs.size());
            long storedBytes = 0L;
            for (int index = 0; index < rawInputs.size(); index++) {
                StreamChannelState state = StreamChannelState.capture(
                        activeCheckpoint,
                        rawInputs.get(index).channelIdentity(),
                        snapshotSequences[index], barrierSequences[index],
                        captured.get(index), maximumEventsPerLane,
                        maximumBytesPerLane, maximumFrameBytes);
                byte[] frame = StreamChannelStateCodec.encode(state);
                storedBytes = Math.addExact(storedBytes, frame.length);
                if (storedBytes > maximumTotalBytes) {
                    throw limit(
                            "encoded channel state exceeds its resource lease");
                }
                FileCheckpointPayloadStore.StoredPayload stored =
                        store(frame);
                StreamChannelIdentity channel = state.channelIdentity();
                members.add(StreamCheckpointMember.channelState(
                        operatorUid, subtaskIndex, channel.channelId(),
                        channel.channelEpoch(), stored.digest(),
                        stored.contentBytes()));
            }
            completedCheckpoint = activeCheckpoint;
            completedMembers = Collections.unmodifiableList(members);
            CompletableFuture<List<StreamCheckpointMember>> completed =
                    activeCut;
            clearCapture();
            completed.complete(completedMembers);
        } catch (RuntimeException failure) {
            RuntimeException poisoned = poison(failure);
            if (activeCut != null) activeCut.completeExceptionally(poisoned);
            throw poisoned;
        }
    }

    private FileCheckpointPayloadStore.StoredPayload store(byte[] frame) {
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile("rowweave-stream-channel-", ".state");
            Files.write(staged, frame);
            return payloadStore.store(staged);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "unaligned channel state staging failed", failure);
        } finally {
            if (staged != null) {
                try {
                    Files.deleteIfExists(staged);
                } catch (IOException cleanup) {
                    if (primary != null) primary.addSuppressed(cleanup);
                }
            }
        }
    }

    private void restore(List<StreamCheckpointMember> members,
            boolean restoring) {
        Map<String, Integer> lanes = new HashMap<>();
        for (int index = 0; index < rawInputs.size(); index++) {
            lanes.put(rawInputs.get(index).channelIdentity().channelId(),
                    index);
        }
        Set<String> restored = new HashSet<>();
        long totalBytes = 0L;
        for (StreamCheckpointMember member : members) {
            if (member.kind() != StreamCheckpointMember.Kind.CHANNEL_STATE) {
                continue;
            }
            if (!operatorUid.equals(member.operatorUid())
                    || subtaskIndex != member.subtaskIndex()) {
                continue;
            }
            if (!restoring) {
                throw new IllegalArgumentException(
                        "channel state is unexpected without restore");
            }
            Integer index = lanes.get(member.subjectId());
            if (index == null || !restored.add(member.subjectId())) {
                throw new IllegalArgumentException(
                        "channel state lane is unknown or duplicated");
            }
            byte[] frame = load(member);
            try {
                totalBytes = Math.addExact(totalBytes, frame.length);
            } catch (ArithmeticException overflow) {
                throw limit("restored channel-state bytes overflowed");
            }
            if (totalBytes > maximumTotalBytes) {
                throw limit("restored channel state exceeds its lease");
            }
            StreamChannelState state = StreamChannelStateCodec.decode(
                    frame, maximumEventsPerLane, maximumBytesPerLane,
                    maximumFrameBytes);
            if (!member.subjectId().equals(
                    state.channelIdentity().channelId())
                    || member.logicalPosition()
                    != state.channelIdentity().channelEpoch()) {
                throw new IllegalArgumentException(
                        "channel state member identity differs from its payload");
            }
            replay.get(index).addAll(state.envelopes());
            remainingReplayFrames = Math.addExact(remainingReplayFrames,
                    state.envelopes().size());
            for (StreamEnvelope envelope : state.envelopes()) {
                if (envelope.kind() == StreamEnvelope.Kind.WATERMARK_STATUS) {
                    highestReplayStatusGeneration[index] = Math.max(
                            highestReplayStatusGeneration[index],
                            envelope.watermarkStatusGeneration());
                }
            }
        }
        if (restoring && restored.size() != rawInputs.size()) {
            throw new IllegalArgumentException(
                    "unaligned restore omits a physical input lane");
        }
    }

    private byte[] load(StreamCheckpointMember member) {
        if (member.contentBytes() < 1L
                || member.contentBytes() > maximumTotalBytes
                || member.contentBytes() > Integer.MAX_VALUE) {
            throw limit("channel state payload exceeds its restore lease");
        }
        Path restored = null;
        Throwable primary = null;
        try {
            restored = Files.createTempFile(
                    "rowweave-stream-channel-restore-", ".state");
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), restored);
            return Files.readAllBytes(restored);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "unaligned channel state restore failed", failure);
        } finally {
            if (restored != null) {
                try {
                    Files.deleteIfExists(restored);
                } catch (IOException cleanup) {
                    if (primary != null) primary.addSuppressed(cleanup);
                }
            }
        }
    }

    private void clearCapture() {
        activeCheckpoint = null;
        activeCut = null;
        captureStarted = false;
        totalCapturedBytes = 0L;
        for (int index = 0; index < rawInputs.size(); index++) {
            snapshotSequences[index] = 0L;
            barrierSequences[index] = 0L;
            capturedBytes[index] = 0L;
            realBarrierSeen[index] = false;
            captured.get(index).clear();
        }
    }

    private boolean hasSyntheticBarrier() {
        for (boolean value : syntheticBarrierPending) if (value) return true;
        return false;
    }

    private boolean allBarriered() {
        for (boolean value : realBarrierSeen) if (!value) return false;
        return true;
    }

    private boolean allEnded() {
        for (boolean value : ended) if (!value) return false;
        return true;
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        StreamChannelIdentity first = rawInputs.get(0).channelIdentity();
        return first.runId().equals(checkpoint.runId())
                && first.attemptId().equals(checkpoint.attemptId())
                && first.fenceToken() == checkpoint.fenceToken();
    }

    private void requireHealthy() {
        if (terminalFailure != null) throw terminalFailure;
    }

    private RuntimeException poison(RuntimeException failure) {
        terminalFailure = failure;
        if (activeCut != null && !activeCut.isDone()) {
            activeCut.completeExceptionally(failure);
        }
        return failure;
    }

    private static IllegalStateException limit(String message) {
        return new IllegalStateException(
                "CHANNEL_STATE_LIMIT: " + message);
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(failure);
        return result;
    }

    private static StreamEnvelope rebind(StreamEnvelope source,
            StreamChannelIdentity identity, long sequence) {
        switch (source.kind()) {
            case RECORD:
                return source.hasEventTimestamp()
                        ? StreamEnvelope.record(identity, sequence,
                        source.timestamp(), source.recordFrame())
                        : StreamEnvelope.record(identity, sequence,
                        source.recordFrame());
            case WATERMARK:
                return StreamEnvelope.watermark(
                        identity, sequence, source.timestamp());
            case WATERMARK_STATUS:
                return StreamEnvelope.watermarkStatus(identity, sequence,
                        source.watermarkStatus(),
                        source.watermarkStatusGeneration());
            case CHECKPOINT_BARRIER:
                return StreamEnvelope.checkpointBarrier(identity, sequence,
                        source.checkpointIdentity());
            case END_OF_INPUT:
                return StreamEnvelope.endOfInput(identity, sequence);
            case ASYNC_COMPLETION:
                return StreamEnvelope.asyncCompletion(identity, sequence,
                        source.operationIdentity());
            case CANCEL:
                return StreamEnvelope.cancel(identity, sequence);
            case ERROR:
                return StreamEnvelope.error(
                        identity, sequence, source.errorCode());
            default:
                throw new UnsupportedOperationException(
                        "unknown Stream envelope kind");
        }
    }

    private final class GatedInput implements StreamChannelInput {
        private final int index;
        private final StreamChannelInput raw;

        private GatedInput(int index, StreamChannelInput raw) {
            this.index = index;
            this.raw = raw;
        }

        @Override public StreamChannelIdentity channelIdentity() {
            return raw.channelIdentity();
        }

        @Override public StreamEnvelope receive() throws InterruptedException {
            return next(index, 0L, true);
        }

        @Override public StreamEnvelope poll(long timeoutMillis)
                throws InterruptedException {
            if (timeoutMillis < 0L) {
                throw new IllegalArgumentException(
                        "Stream channel poll timeout must not be negative");
            }
            return next(index, timeoutMillis, false);
        }

        @Override public void cancel() { raw.cancel(); }

        @Override public void fail(Throwable cause) { raw.fail(cause); }
    }

    private final class WrappedTask implements StreamOperatorTask {
        private final StreamOperatorTask delegate;

        private WrappedTask(StreamOperatorTask delegate) {
            this.delegate = delegate;
        }

        @Override public String operatorUid() { return delegate.operatorUid(); }

        @Override public void process(io.rowweave.DataRow input,
                Long eventTimestamp, Sink output) {
            delegate.process(input, eventTimestamp, output);
        }

        @Override public void start() { delegate.start(); }

        @Override public void start(long nowNanos) {
            delegate.start(nowNanos);
        }

        @Override public void cancel() { delegate.cancel(); }

        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            delegate.awaitCompletion(timeoutMillis);
        }

        @Override public Throwable failure() { return delegate.failure(); }

        @Override public boolean completed() { return delegate.completed(); }

        @Override public StreamTaskResourceUsage resourceUsage() {
            return delegate.resourceUsage();
        }

        @Override public CheckpointIdentity preparedCheckpoint() {
            return delegate.preparedCheckpoint();
        }

        @Override public boolean terminalInputSeen() {
            return delegate.terminalInputSeen();
        }

        @Override public void reconcile(
                io.rowweave.stream.contract.CheckpointDecision decision,
                io.rowweave.stream.runtime.StreamCheckpointAuthority authority) {
            delegate.reconcile(decision, authority);
        }

        @Override public CompletableFuture<List<StreamCheckpointMember>>
                checkpoint(CheckpointIdentity checkpoint) {
            CompletableFuture<List<StreamCheckpointMember>> channels =
                    StreamUnalignedTaskGate.this.checkpoint(checkpoint);
            final CompletableFuture<List<StreamCheckpointMember>> operator;
            try {
                operator = delegate.checkpoint(checkpoint);
            } catch (Throwable failure) {
                return failed(failure);
            }
            CompletableFuture<List<StreamCheckpointMember>> cut =
                    operator.thenCombine(channels, (operatorMembers,
                    channelMembers) -> {
                if (operatorMembers == null || channelMembers == null) {
                    throw new IllegalStateException(
                            "unaligned checkpoint returned a null cut");
                }
                List<StreamCheckpointMember> combined = new ArrayList<>(
                        operatorMembers.size() + channelMembers.size());
                for (StreamCheckpointMember member : operatorMembers) {
                    if (member == null || member.kind()
                            == StreamCheckpointMember.Kind.CHANNEL_STATE) {
                        throw new IllegalStateException(
                                "operator returned invalid channel state");
                    }
                    combined.add(member);
                }
                combined.addAll(channelMembers);
                long combinedBytes = 0L;
                try {
                    for (StreamCheckpointMember member : combined) {
                        combinedBytes = Math.addExact(
                                combinedBytes, member.contentBytes());
                    }
                } catch (ArithmeticException overflow) {
                    throw limit(
                            "unaligned checkpoint state bytes overflowed");
                }
                if (combinedBytes > maximumTotalBytes) {
                    throw limit(
                            "unaligned checkpoint state exceeds its resource lease");
                }
                return Collections.unmodifiableList(combined);
            });
            // thenCombine need not settle until both inputs finish. A failed
            // operator may never receive its physical barrier, so propagate
            // either failure without waiting for the other cut.
            operator.whenComplete((ignored, failure) -> {
                if (failure != null) cut.completeExceptionally(failure);
            });
            channels.whenComplete((ignored, failure) -> {
                if (failure != null) cut.completeExceptionally(failure);
            });
            return cut;
        }

        @Override public void assignSourceSplit(
                io.rowweave.stream.contract.StreamSourceSplit split) {
            delegate.assignSourceSplit(split);
        }

        @Override public CompletableFuture<Void> revokeSourceSplit(
                String splitId) {
            return delegate.revokeSourceSplit(splitId);
        }

        @Override public io.rowweave.stream.contract.StreamSourceSplit
                completedSourceSplit() {
            return delegate.completedSourceSplit();
        }

        @Override public boolean hasIdleTimer() {
            return delegate.hasIdleTimer();
        }

        @Override public long idleDelayMillis(long nowNanos) {
            return delegate.idleDelayMillis(nowNanos);
        }

        @Override public void onIdleTimer(long nowNanos, Sink output) {
            delegate.onIdleTimer(nowNanos, output);
        }

        @Override public boolean onWatermark(long timestamp, Sink output) {
            return delegate.onWatermark(timestamp, output);
        }

        @Override public boolean onWatermarkStatus(
                StreamEnvelope.WatermarkStatus status,
                long generation, Sink output) {
            return delegate.onWatermarkStatus(status, generation, output);
        }

        @Override public boolean onEnd(Sink output) {
            return delegate.onEnd(output);
        }

        @Override public boolean hasCheckpointState() {
            return delegate.hasCheckpointState();
        }

        @Override public StreamCheckpointMember snapshot(
                CheckpointIdentity checkpoint,
                FileCheckpointPayloadStore payloadStore) {
            return delegate.snapshot(checkpoint, payloadStore);
        }

        @Override public void restore(StreamCheckpointMember member,
                FileCheckpointPayloadStore payloadStore) {
            delegate.restore(member, payloadStore);
        }

        @Override public void close() { delegate.close(); }
    }
}
