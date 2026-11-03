package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelInput;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Single-mailbox bounded multi-input gate.
 *
 * <p>The gate preserves order within every input, polls ready inputs in round
 * robin order, merges watermark/idle/end signals, and blocks an input after an
 * aligned checkpoint barrier until every live input reaches the same cut.
 * It owns no thread and does not buffer records outside the bounded channels.
 */
final class StreamMultiInputGate {
    private static final int STATE_MAGIC = 0x414D4947;
    private static final int STATE_VERSION = 1;
    private static final long INPUT_POLL_SLICE_MILLIS = 10L;

    private final List<StreamChannelInput> inputs;
    private final boolean[] blocked;
    private final boolean[] ended;
    private final long[] watermarks;
    private final long[] statusGenerations;
    private final StreamEnvelope.WatermarkStatus[] statuses;
    private final ArrayDeque<Event> ready = new ArrayDeque<>();

    private int nextInput;
    private long emittedWatermark = Long.MIN_VALUE;
    private boolean emittedIdle;
    private long emittedStatusGeneration;
    private boolean recoveredImplicitActive;
    private CheckpointIdentity aligningCheckpoint;

    StreamMultiInputGate(List<? extends StreamChannelInput> inputs) {
        if (inputs == null || inputs.size() < 2) {
            throw new IllegalArgumentException(
                    "multi-input gate requires at least two inputs");
        }
        List<StreamChannelInput> copied = new ArrayList<>(inputs.size());
        String runId = null;
        String attemptId = null;
        long fenceToken = -1L;
        for (StreamChannelInput input : inputs) {
            if (input == null) {
                throw new IllegalArgumentException(
                        "multi-input gate contains a null input");
            }
            if (runId == null) {
                runId = input.channelIdentity().runId();
                attemptId = input.channelIdentity().attemptId();
                fenceToken = input.channelIdentity().fenceToken();
            } else if (!runId.equals(input.channelIdentity().runId())
                    || !attemptId.equals(
                    input.channelIdentity().attemptId())
                    || fenceToken != input.channelIdentity().fenceToken()) {
                throw new IllegalArgumentException(
                        "multi-input gate lanes belong to different attempts");
            }
            copied.add(input);
        }
        this.inputs = Collections.unmodifiableList(copied);
        this.blocked = new boolean[inputs.size()];
        this.ended = new boolean[inputs.size()];
        this.watermarks = new long[inputs.size()];
        this.statusGenerations = new long[inputs.size()];
        this.statuses = new StreamEnvelope.WatermarkStatus[inputs.size()];
        java.util.Arrays.fill(watermarks, Long.MIN_VALUE);
        java.util.Arrays.fill(statuses,
                StreamEnvelope.WatermarkStatus.ACTIVE);
    }

    Event take() throws InterruptedException {
        while (true) {
            Event event = poll(1_000L);
            if (event != null || finished()) return event;
        }
    }

    Event poll(long timeoutMillis) throws InterruptedException {
        if (timeoutMillis < 0L) {
            throw new IllegalArgumentException(
                    "multi-input gate timeout must not be negative");
        }
        Event pending = ready.pollFirst();
        if (pending != null) return pending;
        long deadline = deadline(timeoutMillis);
        while (!finished()) {
            boolean inspected = false;
            for (int offset = 0; offset < inputs.size(); offset++) {
                int index = (nextInput + offset) % inputs.size();
                if (blocked[index] || ended[index]) continue;
                inspected = true;
                StreamEnvelope envelope = inputs.get(index).poll(0L);
                if (envelope == null) continue;
                nextInput = (index + 1) % inputs.size();
                accept(index, envelope);
                pending = ready.pollFirst();
                if (pending != null) return pending;
                offset = -1;
            }
            if (finished()) return ready.pollFirst();
            if (timeoutMillis == 0L || System.nanoTime() >= deadline) {
                return null;
            }
            if (!inspected) {
                Thread.yield();
                continue;
            }
            int index = nextAvailableInput();
            if (index < 0) continue;
            long remaining = remainingMillis(deadline);
            if (remaining <= 0L) return null;
            StreamEnvelope envelope = inputs.get(index).poll(Math.min(
                    INPUT_POLL_SLICE_MILLIS, remaining));
            if (envelope != null) {
                nextInput = (index + 1) % inputs.size();
                accept(index, envelope);
                pending = ready.pollFirst();
                if (pending != null) return pending;
            }
        }
        return ready.pollFirst();
    }

    boolean finished() {
        for (boolean value : ended) if (!value) return false;
        return ready.isEmpty();
    }

    boolean inputEnded(int inputIndex) {
        if (inputIndex < 0 || inputIndex >= inputs.size()) {
            throw new IllegalArgumentException(
                    "multi-input gate lane is unavailable");
        }
        return ended[inputIndex];
    }

    byte[] snapshotState() {
        boolean pendingTerminalEnd = ready.size() == 1 && allEnded()
                && ready.peekFirst().envelope.kind()
                == StreamEnvelope.Kind.END_OF_INPUT;
        if (aligningCheckpoint != null
                || !ready.isEmpty() && !pendingTerminalEnd) {
            throw new IllegalStateException(
                    "multi-input gate snapshot is not at an aligned cut");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(STATE_MAGIC);
                output.writeInt(STATE_VERSION);
                output.writeInt(inputs.size());
                for (int index = 0; index < inputs.size(); index++) {
                    output.writeBoolean(ended[index]);
                    output.writeLong(watermarks[index]);
                    output.writeLong(statusGenerations[index]);
                    output.writeInt(statuses[index]
                            == StreamEnvelope.WatermarkStatus.ACTIVE ? 1 : 2);
                }
                output.writeInt(nextInput);
                output.writeLong(emittedWatermark);
                output.writeBoolean(emittedIdle);
                output.writeLong(emittedStatusGeneration);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "multi-input gate state encoding failed", impossible);
        }
    }

    static byte[] mergeStateForRescale(List<byte[]> frames) {
        if (frames == null || frames.isEmpty()) {
            throw new IllegalArgumentException(
                    "multi-input live rescale state is required");
        }
        GateState merged = null;
        for (byte[] frame : frames) {
            GateState current = decodeState(frame);
            if (merged == null) {
                merged = current;
            } else {
                merged.merge(current);
            }
        }
        // The migrated physical channel cut may contain a status emitted by
        // an old task whose generation is lower than another task's saved
        // generation. Preserve the aggregate status and its outgoing epoch,
        // but accept replay on each new logical input from generation zero.
        java.util.Arrays.fill(merged.statusGenerations, 0L);
        return merged.encode();
    }

    void restoreState(byte[] frame) {
        if (frame == null || frame.length == 0
                || aligningCheckpoint != null || !ready.isEmpty()) {
            throw new IllegalArgumentException(
                    "multi-input gate restore state is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            if (input.readInt() != STATE_MAGIC
                    || input.readInt() != STATE_VERSION
                    || input.readInt() != inputs.size()) {
                throw invalidState();
            }
            for (int index = 0; index < inputs.size(); index++) {
                ended[index] = input.readBoolean();
                input.readLong();
                long savedStatusGeneration = input.readLong();
                int status = input.readInt();
                if (status != 1 && status != 2) throw invalidState();
                // A newly opened source attempt starts ACTIVE without an
                // explicit status frame. The saved physical IDLE applies
                // only to the retired attempt.
                statuses[index] = ended[index]
                        ? StreamEnvelope.WatermarkStatus.IDLE
                        : StreamEnvelope.WatermarkStatus.ACTIVE;
                blocked[index] = false;
                if (savedStatusGeneration < 0L) throw invalidState();
                // The new source attempt may restart its physical watermark
                // and status epochs. Keep only the aggregate frontier as the
                // externally visible recovery floor; each input starts a new
                // epoch and must catch up before it advances that frontier.
                watermarks[index] = Long.MIN_VALUE;
                statusGenerations[index] = 0L;
            }
            nextInput = input.readInt();
            emittedWatermark = input.readLong();
            emittedIdle = input.readBoolean();
            emittedStatusGeneration = input.readLong();
            recoveredImplicitActive = emittedIdle && !allEnded();
            if (nextInput < 0 || nextInput >= inputs.size()
                    || emittedStatusGeneration < 0L
                    || input.read() != -1) throw invalidState();
        } catch (IOException | RuntimeException failure) {
            if (failure instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) failure;
            }
            throw new IllegalArgumentException(
                    "multi-input gate state is corrupt", failure);
        }
    }

    private static IllegalArgumentException invalidState() {
        return new IllegalArgumentException(
                "multi-input gate state is corrupt");
    }

    private static GateState decodeState(byte[] frame) {
        if (frame == null || frame.length == 0) throw invalidState();
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            if (input.readInt() != STATE_MAGIC
                    || input.readInt() != STATE_VERSION) {
                throw invalidState();
            }
            int count = input.readInt();
            if (count < 2 || count > 1_024) throw invalidState();
            GateState state = new GateState(count);
            for (int index = 0; index < count; index++) {
                state.ended[index] = input.readBoolean();
                state.watermarks[index] = input.readLong();
                state.statusGenerations[index] = input.readLong();
                state.statuses[index] = input.readInt();
                if (state.statusGenerations[index] < 0L
                        || (state.statuses[index] != 1
                        && state.statuses[index] != 2)) {
                    throw invalidState();
                }
            }
            int nextInput = input.readInt();
            state.emittedWatermark = input.readLong();
            state.emittedIdle = input.readBoolean();
            state.emittedStatusGeneration = input.readLong();
            if (nextInput < 0 || nextInput >= count
                    || state.emittedStatusGeneration < 0L
                    || input.read() != -1) throw invalidState();
            return state;
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "multi-input gate state is corrupt", failure);
        }
    }

    private static final class GateState {
        final boolean[] ended;
        final long[] watermarks;
        final long[] statusGenerations;
        final int[] statuses;
        long emittedWatermark;
        boolean emittedIdle;
        long emittedStatusGeneration;

        private GateState(int count) {
            ended = new boolean[count];
            watermarks = new long[count];
            statusGenerations = new long[count];
            statuses = new int[count];
        }

        private void merge(GateState other) {
            if (other.ended.length != ended.length) throw invalidState();
            for (int index = 0; index < ended.length; index++) {
                ended[index] &= other.ended[index];
                watermarks[index] = Math.min(
                        watermarks[index], other.watermarks[index]);
                statusGenerations[index] = Math.max(
                        statusGenerations[index],
                        other.statusGenerations[index]);
                if (other.statuses[index] == 1) statuses[index] = 1;
            }
            emittedWatermark = Math.min(
                    emittedWatermark, other.emittedWatermark);
            emittedIdle &= other.emittedIdle;
            emittedStatusGeneration = Math.max(
                    emittedStatusGeneration,
                    other.emittedStatusGeneration);
        }

        private byte[] encode() {
            try {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (DataOutputStream output = new DataOutputStream(bytes)) {
                    output.writeInt(STATE_MAGIC);
                    output.writeInt(STATE_VERSION);
                    output.writeInt(ended.length);
                    for (int index = 0; index < ended.length; index++) {
                        output.writeBoolean(ended[index]);
                        output.writeLong(watermarks[index]);
                        output.writeLong(statusGenerations[index]);
                        output.writeInt(statuses[index]);
                    }
                    output.writeInt(0);
                    output.writeLong(emittedWatermark);
                    output.writeBoolean(emittedIdle);
                    output.writeLong(emittedStatusGeneration);
                }
                return bytes.toByteArray();
            } catch (IOException impossible) {
                throw new IllegalStateException(
                        "multi-input gate state encoding failed", impossible);
            }
        }
    }

    void cancel() {
        for (StreamChannelInput input : inputs) input.cancel();
    }

    void fail(Throwable failure) {
        for (StreamChannelInput input : inputs) input.fail(failure);
    }

    void markEnded(int inputIndex, StreamEnvelope envelope) {
        if (inputIndex < 0 || inputIndex >= inputs.size()
                || envelope == null
                || envelope.kind() != StreamEnvelope.Kind.END_OF_INPUT) {
            throw new IllegalArgumentException(
                    "multi-input ended lane is invalid");
        }
        acceptEnd(inputIndex, envelope);
    }

    private void accept(int inputIndex, StreamEnvelope envelope) {
        if (recoveredImplicitActive
                && envelope.kind() != StreamEnvelope.Kind.END_OF_INPUT
                && envelope.kind() != StreamEnvelope.Kind.CANCEL
                && envelope.kind() != StreamEnvelope.Kind.ERROR) {
            recoveredImplicitActive = false;
            emittedIdle = false;
            emittedStatusGeneration = Math.addExact(
                    emittedStatusGeneration, 1L);
            ready.addLast(new Event(-1, StreamEnvelope.watermarkStatus(
                    envelope.channelIdentity(), envelope.sequence(),
                    StreamEnvelope.WatermarkStatus.ACTIVE,
                    emittedStatusGeneration)));
        }
        switch (envelope.kind()) {
            case RECORD:
            case ASYNC_COMPLETION:
            case CANCEL:
            case ERROR:
                ready.addLast(new Event(inputIndex, envelope));
                break;
            case WATERMARK:
                acceptWatermark(inputIndex, envelope);
                break;
            case WATERMARK_STATUS:
                acceptStatus(inputIndex, envelope);
                break;
            case CHECKPOINT_BARRIER:
                acceptBarrier(inputIndex, envelope);
                break;
            case END_OF_INPUT:
                acceptEnd(inputIndex, envelope);
                break;
            default:
                throw new UnsupportedOperationException(
                        "unsupported multi-input gate event");
        }
    }

    private void acceptWatermark(int inputIndex, StreamEnvelope envelope) {
        long timestamp = envelope.timestamp();
        if (timestamp < watermarks[inputIndex]) {
            throw new IllegalArgumentException(
                    "multi-input watermark regressed");
        }
        watermarks[inputIndex] = timestamp;
        emitMergedWatermark(envelope);
    }

    private void acceptStatus(int inputIndex, StreamEnvelope envelope) {
        long generation = envelope.watermarkStatusGeneration();
        if (generation <= statusGenerations[inputIndex]) {
            throw new IllegalArgumentException(
                    "multi-input watermark status generation did not advance");
        }
        statusGenerations[inputIndex] = generation;
        statuses[inputIndex] = envelope.watermarkStatus();
        boolean idle = allLiveInputsIdle();
        if (idle != emittedIdle) {
            emittedIdle = idle;
            emittedStatusGeneration = Math.addExact(
                    emittedStatusGeneration, 1L);
            ready.addLast(new Event(-1, StreamEnvelope.watermarkStatus(
                    envelope.channelIdentity(), envelope.sequence(),
                    idle ? StreamEnvelope.WatermarkStatus.IDLE
                            : StreamEnvelope.WatermarkStatus.ACTIVE,
                    emittedStatusGeneration)));
        }
        if (!idle) emitMergedWatermark(envelope);
    }

    private void acceptBarrier(int inputIndex, StreamEnvelope envelope) {
        CheckpointIdentity checkpoint = envelope.checkpointIdentity();
        if (aligningCheckpoint == null) aligningCheckpoint = checkpoint;
        else if (!aligningCheckpoint.equals(checkpoint)) {
            throw new IllegalArgumentException(
                    "multi-input barriers identify different checkpoints");
        }
        if (blocked[inputIndex]) {
            throw new IllegalArgumentException(
                    "multi-input received a duplicate barrier");
        }
        blocked[inputIndex] = true;
        if (!aligned()) return;
        java.util.Arrays.fill(blocked, false);
        aligningCheckpoint = null;
        ready.addLast(new Event(-1, envelope));
    }

    private void acceptEnd(int inputIndex, StreamEnvelope envelope) {
        if (ended[inputIndex]) return;
        ended[inputIndex] = true;
        blocked[inputIndex] = false;
        statuses[inputIndex] = StreamEnvelope.WatermarkStatus.IDLE;
        boolean terminal = allEnded();
        if (!terminal && allLiveInputsIdle() && !emittedIdle) {
            emittedIdle = true;
            emittedStatusGeneration = Math.addExact(
                    emittedStatusGeneration, 1L);
            ready.addLast(new Event(-1, StreamEnvelope.watermarkStatus(
                    envelope.channelIdentity(), envelope.sequence(),
                    StreamEnvelope.WatermarkStatus.IDLE,
                    emittedStatusGeneration)));
        }
        // END belongs to the current aligned cut. Any frontier it releases
        // (including a transition to all-live-idle) must be emitted before
        // the barrier so the snapshot cannot record unsent control frames.
        if (!terminal) emitMergedWatermark(envelope);
        if (aligningCheckpoint != null && aligned()) {
            ready.addLast(new Event(-1, StreamEnvelope.checkpointBarrier(
                    envelope.channelIdentity(), envelope.sequence(),
                    aligningCheckpoint)));
            java.util.Arrays.fill(blocked, false);
            aligningCheckpoint = null;
        }
        if (terminal) {
            ready.addLast(new Event(-1, envelope));
        }
    }

    private void emitMergedWatermark(StreamEnvelope source) {
        long merged = Long.MAX_VALUE;
        boolean active = false;
        for (int index = 0; index < inputs.size(); index++) {
            if (ended[index]
                    || statuses[index]
                    == StreamEnvelope.WatermarkStatus.IDLE) continue;
            active = true;
            merged = Math.min(merged, watermarks[index]);
        }
        if (active && merged > emittedWatermark) {
            emittedWatermark = merged;
            ready.addLast(new Event(-1, StreamEnvelope.watermark(
                    source.channelIdentity(), source.sequence(), merged)));
        }
    }

    private boolean aligned() {
        for (int index = 0; index < inputs.size(); index++) {
            if (!ended[index] && !blocked[index]) return false;
        }
        return true;
    }

    private boolean allLiveInputsIdle() {
        boolean live = false;
        for (int index = 0; index < inputs.size(); index++) {
            if (ended[index]) continue;
            live = true;
            if (statuses[index] != StreamEnvelope.WatermarkStatus.IDLE) {
                return false;
            }
        }
        return live;
    }

    private boolean allEnded() {
        for (boolean value : ended) if (!value) return false;
        return true;
    }

    private int nextAvailableInput() {
        for (int offset = 0; offset < inputs.size(); offset++) {
            int index = (nextInput + offset) % inputs.size();
            if (!blocked[index] && !ended[index]) return index;
        }
        return -1;
    }

    private static long deadline(long timeoutMillis) {
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long now = System.nanoTime();
        long candidate = now + timeoutNanos;
        return candidate < now ? Long.MAX_VALUE : candidate;
    }

    private static long remainingMillis(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0L) return 0L;
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    static final class Event {
        private final int inputIndex;
        private final StreamEnvelope envelope;

        private Event(int inputIndex, StreamEnvelope envelope) {
            this.inputIndex = inputIndex;
            this.envelope = envelope;
        }

        int inputIndex() { return inputIndex; }

        StreamEnvelope envelope() { return envelope; }
    }
}
