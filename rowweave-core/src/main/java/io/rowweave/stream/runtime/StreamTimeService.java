package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamEnvelope;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Single-mailbox-thread watermark, idle-status and bounded timer service.
 */
public final class StreamTimeService {
    public enum TimerKind { EVENT_TIME, PROCESSING_TIME }

    @FunctionalInterface
    public interface TimerHandler {
        void onTimer(Timer timer);
    }

    public static final class Timer {
        private final TimerKind kind;
        private final long timestamp;
        private final String timerId;

        private Timer(TimerKind kind, long timestamp, String timerId) {
            this.kind = kind;
            this.timestamp = timestamp;
            this.timerId = timerId;
        }

        public TimerKind kind() { return kind; }
        public long timestamp() { return timestamp; }
        public String timerId() { return timerId; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Timer)) return false;
            Timer that = (Timer) other;
            return timestamp == that.timestamp && kind == that.kind
                    && timerId.equals(that.timerId);
        }

        @Override
        public int hashCode() {
            int result = 31 * kind.hashCode() + Long.hashCode(timestamp);
            return 31 * result + timerId.hashCode();
        }
    }

    private static final Comparator<Timer> TIMER_ORDER = (left, right) -> {
        int timestamp = Long.compare(left.timestamp, right.timestamp);
        return timestamp != 0 ? timestamp
                : left.timerId.compareTo(right.timerId);
    };

    private final long[] inputWatermarks;
    private final long[] statusGenerations;
    private final StreamEnvelope.WatermarkStatus[] inputStatuses;
    private final int maximumTimers;
    private final LongSupplier processingClock;
    private final TimerHandler timerHandler;
    private final Thread ownerThread;
    private final PriorityQueue<Timer> eventTimers =
            new PriorityQueue<>(TIMER_ORDER);
    private final PriorityQueue<Timer> processingTimers =
            new PriorityQueue<>(TIMER_ORDER);
    private final Set<Timer> timerIdentities = new HashSet<>();

    private long operatorWatermark = Long.MIN_VALUE;
    private long processingTimeFloor = Long.MIN_VALUE;
    private boolean firingEventTimers;
    private boolean firingProcessingTimers;
    private int maximumObservedTimers;
    private StreamTimeException terminalFailure;

    public StreamTimeService(
            int inputCount, int maximumTimers,
            LongSupplier processingClock, TimerHandler timerHandler) {
        if (inputCount < 1 || inputCount > 2
                || maximumTimers < 1
                || processingClock == null || timerHandler == null) {
            throw failure(StreamTimeException.Code.INVALID_CONFIGURATION,
                    "input count, timer limit, clock and handler are required");
        }
        this.inputWatermarks = new long[inputCount];
        java.util.Arrays.fill(inputWatermarks, Long.MIN_VALUE);
        this.statusGenerations = new long[inputCount];
        this.inputStatuses = new StreamEnvelope.WatermarkStatus[inputCount];
        java.util.Arrays.fill(inputStatuses,
                StreamEnvelope.WatermarkStatus.ACTIVE);
        this.maximumTimers = maximumTimers;
        this.processingClock = processingClock;
        this.timerHandler = timerHandler;
        this.ownerThread = Thread.currentThread();
    }

    public void onWatermark(int inputIndex, long watermark) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        if (watermark < inputWatermarks[inputIndex]) {
            throw failure(StreamTimeException.Code.INPUT_WATERMARK_REGRESSION,
                    "input watermark must not move backwards");
        }
        inputWatermarks[inputIndex] = watermark;
        if (inputStatuses[inputIndex]
                == StreamEnvelope.WatermarkStatus.ACTIVE) {
            advanceOperatorWatermark();
        }
    }

    /** Complete a bounded input even when its last reported status was IDLE. */
    public void finishInput(int inputIndex) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        inputWatermarks[inputIndex] = Long.MAX_VALUE;
        inputStatuses[inputIndex] = StreamEnvelope.WatermarkStatus.ACTIVE;
        advanceOperatorWatermark();
    }

    /** Remove a completed lane from a still-running multi-input frontier. */
    public void excludeInput(int inputIndex) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        inputWatermarks[inputIndex] = Long.MAX_VALUE;
        inputStatuses[inputIndex] = StreamEnvelope.WatermarkStatus.IDLE;
        advanceOperatorWatermark();
    }

    public void onWatermarkStatus(
            int inputIndex, StreamEnvelope.WatermarkStatus status,
            long generation) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        if (status == null) {
            throw failure(StreamTimeException.Code.STATUS_TRANSITION,
                    "watermark status is required");
        }
        long previousGeneration = statusGenerations[inputIndex];
        if (previousGeneration == Long.MAX_VALUE
                || generation != previousGeneration + 1L) {
            throw failure(StreamTimeException.Code.STATUS_GENERATION,
                    "watermark status generation must increase exactly once");
        }
        StreamEnvelope.WatermarkStatus previous = inputStatuses[inputIndex];
        if (status == previous) {
            throw failure(StreamTimeException.Code.STATUS_TRANSITION,
                    "watermark status must alternate ACTIVE and IDLE");
        }
        if (status == StreamEnvelope.WatermarkStatus.ACTIVE
                && inputWatermarks[inputIndex] < operatorWatermark) {
            throw failure(StreamTimeException.Code.ACTIVATION_WATERMARK,
                    "reactivated input watermark is behind operator time");
        }
        inputStatuses[inputIndex] = status;
        statusGenerations[inputIndex] = generation;
        advanceOperatorWatermark();
    }

    public boolean register(TimerKind kind, long timestamp, String timerId) {
        checkOwnerAndHealthy();
        if (kind == null || timerId == null || timerId.isEmpty()
                || timerId.length() > 256) {
            throw failure(StreamTimeException.Code.INVALID_TIMER,
                    "timer kind and bounded identity are required");
        }
        Timer timer = new Timer(kind, timestamp, timerId);
        if (timerIdentities.contains(timer)) return false;
        if (timerIdentities.size() >= maximumTimers) {
            throw failure(StreamTimeException.Code.TIMER_LIMIT,
                    "task timer count exceeds the configured bound");
        }
        timerIdentities.add(timer);
        maximumObservedTimers = Math.max(
                maximumObservedTimers, timerIdentities.size());
        queue(kind).add(timer);
        if (kind == TimerKind.EVENT_TIME
                && timestamp <= operatorWatermark) {
            fireDue(eventTimers, operatorWatermark, true);
        } else if (kind == TimerKind.PROCESSING_TIME
                && timestamp <= processingTime()) {
            fireDue(processingTimers, processingTimeFloor, false);
        }
        return true;
    }

    public boolean cancel(TimerKind kind, long timestamp, String timerId) {
        checkOwnerAndHealthy();
        if (kind == null || timerId == null) return false;
        Timer timer = new Timer(kind, timestamp, timerId);
        if (!timerIdentities.remove(timer)) return false;
        return queue(kind).remove(timer);
    }

    public void advanceProcessingTime() {
        checkOwnerAndHealthy();
        fireDue(processingTimers, processingTime(), false);
    }

    public long operatorWatermark() {
        checkOwnerAndHealthy();
        return operatorWatermark;
    }

    public StreamEnvelope.WatermarkStatus inputStatus(int inputIndex) {
        checkOwnerAndHealthy();
        requireInput(inputIndex);
        return inputStatuses[inputIndex];
    }

    public int pendingTimerCount() {
        checkOwnerAndHealthy();
        return timerIdentities.size();
    }

    public int maximumTimerCount() {
        checkOwnerAndHealthy();
        return maximumTimers;
    }

    public int maximumObservedTimerCount() {
        checkOwnerAndHealthy();
        return maximumObservedTimers;
    }

    void writeCheckpoint(DataOutputStream output) throws IOException {
        checkOwnerAndHealthy();
        if (output == null) throw new IllegalArgumentException(
                "timer checkpoint output is required");
        output.writeInt(inputWatermarks.length);
        for (int index = 0; index < inputWatermarks.length; index++) {
            output.writeLong(inputWatermarks[index]);
            output.writeLong(statusGenerations[index]);
            output.writeInt(inputStatuses[index]
                    == StreamEnvelope.WatermarkStatus.ACTIVE ? 1 : 2);
        }
        output.writeLong(operatorWatermark);
        output.writeLong(processingTimeFloor);
        output.writeInt(maximumObservedTimers);
        writeTimers(output, eventTimers);
        writeTimers(output, processingTimers);
    }

    void restoreCheckpoint(DataInputStream input) throws IOException {
        checkOwnerAndHealthy();
        if (input == null || !timerIdentities.isEmpty()
                || operatorWatermark != Long.MIN_VALUE
                || processingTimeFloor != Long.MIN_VALUE) {
            throw failure(StreamTimeException.Code.INVALID_CONFIGURATION,
                    "timer restore requires a fresh service");
        }
        try {
            int inputs = input.readInt();
            if (inputs != inputWatermarks.length) {
                throw failure(StreamTimeException.Code.INVALID_CONFIGURATION,
                        "timer checkpoint input count differs");
            }
            for (int index = 0; index < inputs; index++) {
                inputWatermarks[index] = input.readLong();
                statusGenerations[index] = input.readLong();
                int status = input.readInt();
                if (statusGenerations[index] < 0L
                        || (status != 1 && status != 2)
                        || (status == 1
                        && (statusGenerations[index] & 1L) != 0L)
                        || (status == 2
                        && (statusGenerations[index] & 1L) == 0L)) {
                    throw failure(StreamTimeException.Code.INVALID_CONFIGURATION,
                            "timer checkpoint input progress is invalid");
                }
                inputStatuses[index] = status == 1
                        ? StreamEnvelope.WatermarkStatus.ACTIVE
                        : StreamEnvelope.WatermarkStatus.IDLE;
            }
            operatorWatermark = input.readLong();
            processingTimeFloor = input.readLong();
            maximumObservedTimers = input.readInt();
            readTimers(input, TimerKind.EVENT_TIME, eventTimers,
                    operatorWatermark);
            readTimers(input, TimerKind.PROCESSING_TIME, processingTimers,
                    processingTimeFloor);
            if (maximumObservedTimers < timerIdentities.size()
                    || maximumObservedTimers > maximumTimers
                    || operatorWatermark != derivedOperatorWatermark()) {
                throw failure(StreamTimeException.Code.INVALID_CONFIGURATION,
                        "timer checkpoint progress is inconsistent");
            }
        } catch (EOFException truncated) {
            throw failure(StreamTimeException.Code.INVALID_CONFIGURATION,
                    "timer checkpoint is truncated", truncated);
        }
    }

    private void writeTimers(DataOutputStream output,
            PriorityQueue<Timer> timers) throws IOException {
        output.writeInt(timers.size());
        PriorityQueue<Timer> ordered = new PriorityQueue<>(TIMER_ORDER);
        ordered.addAll(timers);
        while (!ordered.isEmpty()) {
            Timer timer = ordered.remove();
            output.writeLong(timer.timestamp);
            writeText(output, timer.timerId);
        }
    }

    private void readTimers(DataInputStream input, TimerKind kind,
            PriorityQueue<Timer> target, long completedTime)
            throws IOException {
        int count = input.readInt();
        if (count < 0 || count > maximumTimers
                || timerIdentities.size() + (long) count > maximumTimers) {
            throw failure(StreamTimeException.Code.TIMER_LIMIT,
                    "timer checkpoint exceeds the configured bound");
        }
        for (int index = 0; index < count; index++) {
            long timestamp = input.readLong();
            String timerId = readText(input);
            Timer timer = new Timer(kind, timestamp, timerId);
            if (timestamp <= completedTime
                    || !timerIdentities.add(timer)) {
                throw failure(StreamTimeException.Code.INVALID_TIMER,
                        "timer checkpoint contains a completed or duplicate timer");
            }
            target.add(timer);
        }
    }

    private long derivedOperatorWatermark() {
        long candidate = Long.MAX_VALUE;
        boolean active = false;
        for (int index = 0; index < inputStatuses.length; index++) {
            if (inputStatuses[index]
                    == StreamEnvelope.WatermarkStatus.ACTIVE) {
                active = true;
                candidate = Math.min(candidate, inputWatermarks[index]);
            }
        }
        return active ? candidate : operatorWatermark;
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 256) {
            throw failure(StreamTimeException.Code.INVALID_TIMER,
                    "timer identity is out of bounds");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > 256 || length > input.available()) {
            throw failure(StreamTimeException.Code.INVALID_TIMER,
                    "timer identity frame is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw failure(StreamTimeException.Code.INVALID_TIMER,
                    "timer identity is malformed", malformed);
        }
    }

    private void advanceOperatorWatermark() {
        long candidate = Long.MAX_VALUE;
        boolean hasActive = false;
        for (int input = 0; input < inputStatuses.length; input++) {
            if (inputStatuses[input]
                    == StreamEnvelope.WatermarkStatus.ACTIVE) {
                hasActive = true;
                candidate = Math.min(candidate, inputWatermarks[input]);
            }
        }
        if (!hasActive || candidate == operatorWatermark) return;
        if (candidate < operatorWatermark) {
            throw failure(
                    StreamTimeException.Code.OPERATOR_WATERMARK_REGRESSION,
                    "operator watermark must not move backwards");
        }
        operatorWatermark = candidate;
        fireDue(eventTimers, candidate, true);
    }

    private PriorityQueue<Timer> queue(TimerKind kind) {
        return kind == TimerKind.EVENT_TIME ? eventTimers : processingTimers;
    }

    private void fireDue(PriorityQueue<Timer> timers,
                         long currentTime, boolean eventTime) {
        if (eventTime ? firingEventTimers : firingProcessingTimers) return;
        if (eventTime) firingEventTimers = true;
        else firingProcessingTimers = true;
        try {
            while (!timers.isEmpty()
                    && timers.peek().timestamp <= currentTime) {
                Timer timer = timers.remove();
                timerIdentities.remove(timer);
                try {
                    timerHandler.onTimer(timer);
                } catch (Throwable callbackFailure) {
                    StreamTimeException poisoned = failure(
                            StreamTimeException.Code.CALLBACK_FAILURE,
                            "Stream timer callback failed", callbackFailure);
                    terminalFailure = poisoned;
                    throw poisoned;
                }
            }
        } finally {
            if (eventTime) firingEventTimers = false;
            else firingProcessingTimers = false;
        }
    }

    private long processingTime() {
        long observed = processingClock.getAsLong();
        if (observed > processingTimeFloor) processingTimeFloor = observed;
        return processingTimeFloor;
    }

    private void requireInput(int inputIndex) {
        if (inputIndex < 0 || inputIndex >= inputWatermarks.length) {
            throw failure(StreamTimeException.Code.INVALID_INPUT,
                    "input index is outside the task arity");
        }
    }

    private void checkOwnerAndHealthy() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamTimeException.Code.THREAD_OWNERSHIP,
                    "Stream time may only be accessed by its mailbox thread");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private static StreamTimeException failure(
            StreamTimeException.Code code, String message) {
        return new StreamTimeException(code, message);
    }

    private static StreamTimeException failure(
            StreamTimeException.Code code, String message, Throwable cause) {
        return new StreamTimeException(code, message, cause);
    }
}
