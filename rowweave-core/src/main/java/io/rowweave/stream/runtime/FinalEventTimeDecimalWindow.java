package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;
import io.rowweave.stream.contract.StreamStateDescriptor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.function.Consumer;
import io.rowweave.stream.api.StreamStateSpec;

/** Managed final-trigger event-time window for high-precision decimal sums. */
public final class FinalEventTimeDecimalWindow implements AutoCloseable {
    public static final String DECIMAL_STATE_SERIALIZER =
            StreamStateSpec.DECIMAL_WINDOW_SERIALIZER_CONTRACT;
    private static final int CHECKPOINT_MAGIC = 0x52575743; // RWWC
    private static final int CHECKPOINT_VERSION = 1;

    public enum WindowType { TUMBLING, SLIDING }

    public static final class Result {
        private final byte[] canonicalKey;
        private final long windowStart;
        private final long windowEnd;
        private final BigDecimal sum;

        private Result(byte[] canonicalKey, long windowStart,
                       long windowEnd, BigDecimal sum) {
            this.canonicalKey = canonicalKey.clone();
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.sum = sum;
        }

        public byte[] canonicalKey() { return canonicalKey.clone(); }
        public long windowStart() { return windowStart; }
        public long windowEnd() { return windowEnd; }
        public BigDecimal sum() { return sum; }
    }

    private final WindowType windowType;
    private final long sizeMillis;
    private final long slideMillis;
    private final int windowsPerRecord;
    private final int maximumValueBytes;
    private final int maxParallelism;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final long maximumSpillBytes;
    private final String operatorUid;
    private final StreamStateDescriptor stateDescriptor;
    private final ManagedKeyedValueState state;
    private final StreamTimeService time;
    private final Consumer<Result> output;
    private RuntimeException terminalFailure;
    private boolean closed;

    public FinalEventTimeDecimalWindow(
            String artifactId, String operatorUid,
            StreamStateDescriptor stateDescriptor,
            int maxParallelism, int keyGroupStart, int keyGroupEnd,
            WindowType windowType, long sizeMillis, long slideMillis,
            int maximumWindowsPerRecord, int maximumTimers,
            MemoryManager memory, Path spillRoot, long maximumSpillBytes,
            int maximumKeyBytes, int maximumValueBytes,
            Consumer<Result> output) {
        int requiredWindows = windowCount(sizeMillis, slideMillis);
        if (stateDescriptor == null
                || stateDescriptor.kind()
                != StreamStateDescriptor.Kind.KEYED_VALUE
                || stateDescriptor.ttlMode()
                != StreamStateDescriptor.TtlMode.NONE
                || !DECIMAL_STATE_SERIALIZER.equals(
                        stateDescriptor.serializerContractId())
                || windowType == null
                || sizeMillis <= 0L || slideMillis <= 0L
                || slideMillis > sizeMillis
                || (windowType == WindowType.TUMBLING
                    && sizeMillis != slideMillis)
                || maximumWindowsPerRecord < 1
                || maximumWindowsPerRecord > 1024
                || requiredWindows > maximumWindowsPerRecord
                || maximumTimers < 1 || output == null) {
            throw failure(StreamWindowException.Code.INVALID_CONFIGURATION,
                    "invalid final event-time window configuration");
        }
        this.windowType = windowType;
        this.sizeMillis = sizeMillis;
        this.slideMillis = slideMillis;
        this.windowsPerRecord = windowType == WindowType.TUMBLING
                ? 1 : requiredWindows;
        this.maximumValueBytes = maximumValueBytes;
        this.maxParallelism = maxParallelism;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.maximumSpillBytes = maximumSpillBytes;
        this.operatorUid = operatorUid;
        this.stateDescriptor = stateDescriptor;
        this.output = output;
        this.state = new ManagedKeyedValueState(
                artifactId, operatorUid, stateDescriptor,
                maxParallelism, keyGroupStart, keyGroupEnd,
                memory, spillRoot, maximumSpillBytes,
                maximumKeyBytes, maximumValueBytes, () -> 0L);
        this.time = new StreamTimeService(
                1, maximumTimers, () -> 0L, this::onTimer);
    }

    public void process(byte[] canonicalKey, BigDecimal amount,
                        long eventTimestamp) {
        checkOpen();
        long currentWatermark = time.operatorWatermark();
        if (eventTimestamp <= currentWatermark) {
            StreamWindowException late = failure(
                    StreamWindowException.Code.LATE_RECORD,
                    "record timestamp is not after the final operator watermark");
            terminalFailure = late;
            throw late;
        }
        if (amount == null) return;
        if (canonicalKey == null) {
            throw failure(StreamWindowException.Code.INVALID_CONFIGURATION,
                    "canonical group key is required");
        }
        ensureOutputScaleBounded(amount);

        WindowMembership membership = membership(eventTimestamp, canonicalKey);
        BigDecimal[] sums = new BigDecimal[membership.starts.length];
        byte[][] encodedSums = new byte[membership.starts.length][];
        boolean[] newTimers = new boolean[membership.starts.length];
        try {
            for (int i = 0; i < membership.starts.length; i++) {
                byte[] previous = state.get(
                        canonicalKey, namespace(
                                membership.starts[i], membership.ends[i]));
                sums[i] = previous == null ? amount
                        : decodeDecimal(previous).add(amount);
                ensureOutputScaleBounded(sums[i]);
                encodedSums[i] = encodeDecimal(sums[i]);
            }
            for (int i = 0; i < membership.starts.length; i++) {
                newTimers[i] = time.register(
                        StreamTimeService.TimerKind.EVENT_TIME,
                        membership.ends[i], membership.timerIds[i]);
            }
        } catch (RuntimeException preMutationFailure) {
            cancelNewTimers(membership, newTimers);
            throw preMutationFailure;
        }

        try {
            for (int i = 0; i < membership.starts.length; i++) {
                state.put(canonicalKey,
                        namespace(membership.starts[i], membership.ends[i]),
                        encodedSums[i], eventTimestamp);
            }
        } catch (RuntimeException mutationFailure) {
            cancelNewTimers(membership, newTimers);
            StreamWindowException poisoned = failure(
                    StreamWindowException.Code.TERMINAL_FAILURE,
                    "window state mutation failed", mutationFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    public void advanceWatermark(long watermark) {
        checkOpen();
        state.advanceWatermark(watermark);
        time.onWatermark(0, watermark);
    }

    public void finishInput() {
        checkOpen();
        state.advanceWatermark(Long.MAX_VALUE);
        time.finishInput(0);
    }

    public void onWatermarkStatus(
            io.rowweave.stream.contract.StreamEnvelope.WatermarkStatus status,
            long generation) {
        checkOpen();
        time.onWatermarkStatus(0, status, generation);
    }

    public long stateSpillBytes() {
        checkOpen();
        return state.spillBytes();
    }

    public int windowsPerRecord() {
        checkOpen();
        return windowsPerRecord;
    }

    public int activeWindowCount() {
        checkOpen();
        return time.pendingTimerCount();
    }

    public int maximumObservedWindowCount() {
        checkOpen();
        return time.maximumObservedTimerCount();
    }

    public int ownedKeyGroupCount() {
        checkOpen();
        return keyGroupEnd - keyGroupStart + 1;
    }

    public int maximumKeyGroupCount() {
        checkOpen();
        return maxParallelism;
    }

    public long maximumStateSpillBytes() {
        checkOpen();
        return maximumSpillBytes;
    }

    /** Freezes state and timer progress as one durable operator member. */
    public StreamCheckpointMember snapshot(
            int subtaskIndex, FileCheckpointPayloadStore payloadStore) {
        checkOpen();
        if (subtaskIndex < 0 || payloadStore == null) {
            throw failure(StreamWindowException.Code.INVALID_CONFIGURATION,
                    "window checkpoint runtime is invalid");
        }
        StreamCheckpointMember stateMember =
                state.snapshot(subtaskIndex, payloadStore);
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-window-checkpoint-", ".tmp");
            try (OutputStream file = Files.newOutputStream(staged,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
                 DataOutputStream output = new DataOutputStream(
                         new BufferedOutputStream(new LimitedOutputStream(
                                 file, payloadStore.maximumPayloadBytes())))) {
                output.writeInt(CHECKPOINT_MAGIC);
                output.writeInt(CHECKPOINT_VERSION);
                byte[] stateFrame =
                        StreamCheckpointMemberCodec.encode(stateMember);
                output.writeInt(stateFrame.length);
                output.write(stateFrame);
                time.writeCheckpoint(output);
            }
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(staged);
            return StreamCheckpointMember.stateHandle(
                    operatorUid, subtaskIndex, keyGroupStart, keyGroupEnd,
                    stateDescriptor.stateName(),
                    stateDescriptor.serializerContractId(),
                    stateDescriptor.stateSchemaVersion(), stored.digest(),
                    stored.contentBytes());
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException checkpointFailure) {
            primary = checkpointFailure;
            throw failure(StreamWindowException.Code.TERMINAL_FAILURE,
                    "window checkpoint staging failed", checkpointFailure);
        } finally {
            deleteCheckpointStaging(staged, primary);
        }
    }

    /** Restores a fresh window generation from its combined state/timer member. */
    public void restore(int subtaskIndex, StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        checkOpen();
        validateCheckpointMember(subtaskIndex, member, payloadStore);
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-window-restore-", ".tmp");
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), staged);
            try (DataInputStream input = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(staged)))) {
                if (input.readInt() != CHECKPOINT_MAGIC
                        || input.readInt() != CHECKPOINT_VERSION) {
                    throw failure(StreamWindowException.Code.CORRUPT_STATE,
                            "window checkpoint header is invalid");
                }
                int length = input.readInt();
                if (length <= 0 || length > 8 * 1024
                        || length > input.available()) {
                    throw failure(StreamWindowException.Code.CORRUPT_STATE,
                            "window state member frame is invalid");
                }
                byte[] stateFrame = new byte[length];
                input.readFully(stateFrame);
                StreamCheckpointMember stateMember =
                        StreamCheckpointMemberCodec.decode(stateFrame);
                state.restore(subtaskIndex, stateMember, payloadStore);
                time.restoreCheckpoint(input);
                if (input.read() != -1) {
                    throw failure(StreamWindowException.Code.CORRUPT_STATE,
                            "window checkpoint has trailing bytes");
                }
            }
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException restoreFailure) {
            primary = restoreFailure;
            throw failure(StreamWindowException.Code.CORRUPT_STATE,
                    "window checkpoint is unreadable", restoreFailure);
        } finally {
            deleteCheckpointStaging(staged, primary);
        }
    }

    /** Returns the managed-state payload nested in this composite handle. */
    public static StreamCheckpointMember nestedCheckpointMember(
            StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        if (member == null || payloadStore == null
                || member.kind() != StreamCheckpointMember.Kind.STATE
                || !DECIMAL_STATE_SERIALIZER.equals(member.codecId())
                || member.contentBytes() <= 0L) {
            return null;
        }
        FileCheckpointPayloadStore.StoredPayload stored =
                new FileCheckpointPayloadStore.StoredPayload(
                        member.contentDigest(), member.contentBytes());
        payloadStore.verifyStored(stored);
        Path path = payloadStore.payloadPath(member.contentDigest());
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            if (input.readInt() != CHECKPOINT_MAGIC) return null;
            if (input.readInt() != CHECKPOINT_VERSION) {
                throw corruptComposite(
                        "window checkpoint version is unsupported");
            }
            int length = input.readInt();
            if (length <= 0 || length > 8 * 1024
                    || length > input.available()) {
                throw corruptComposite(
                        "window state member frame is invalid");
            }
            byte[] frame = new byte[length];
            input.readFully(frame);
            StreamCheckpointMember nested =
                    StreamCheckpointMemberCodec.decode(frame);
            if (nested.kind() != StreamCheckpointMember.Kind.STATE
                    || !member.operatorUid().equals(nested.operatorUid())
                    || member.subtaskIndex() != nested.subtaskIndex()
                    || member.keyGroupStart() != nested.keyGroupStart()
                    || member.keyGroupEnd() != nested.keyGroupEnd()
                    || !member.subjectId().equals(nested.subjectId())
                    || !member.codecId().equals(nested.codecId())
                    || member.schemaVersion() != nested.schemaVersion()
                    || nested.contentBytes() <= 0L) {
                throw corruptComposite(
                        "window nested state member identity differs");
            }
            return nested;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException | IOException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                    "window checkpoint payload closure is corrupt", failure);
        }
    }

    private static StreamCheckpointException corruptComposite(String message) {
        return new StreamCheckpointException(
                StreamCheckpointException.Code.CORRUPT_PAYLOAD, message);
    }

    @Override
    public void close() {
        if (closed) return;
        state.close();
        closed = true;
    }

    private WindowMembership membership(long timestamp, byte[] key) {
        long lastStart;
        try {
            lastStart = Math.multiplyExact(
                    Math.floorDiv(timestamp, slideMillis), slideMillis);
        } catch (ArithmeticException overflow) {
            throw failure(StreamWindowException.Code.WINDOW_ARITHMETIC,
                    "window start is outside the supported time range",
                    overflow);
        }
        int count = windowType == WindowType.TUMBLING
                ? 1 : windowsPerRecord;
        long[] starts = new long[count];
        long[] ends = new long[count];
        String[] timerIds = new String[count];
        try {
            for (int i = 0; i < count; i++) {
                long start = Math.subtractExact(
                        lastStart, Math.multiplyExact((long) i, slideMillis));
                long end = Math.addExact(start, sizeMillis);
                if (timestamp < start || timestamp >= end) {
                    throw new ArithmeticException(
                            "timestamp is outside derived window");
                }
                starts[i] = start;
                ends[i] = end;
                timerIds[i] = timerId(key, start, end);
                if (timerIds[i].length() > 256) {
                    throw failure(StreamWindowException.Code.WINDOW_LIMIT,
                            "window timer identity exceeds its bound");
                }
            }
        } catch (StreamWindowException bounded) {
            throw bounded;
        } catch (ArithmeticException overflow) {
            throw failure(StreamWindowException.Code.WINDOW_ARITHMETIC,
                    "window interval is outside the supported time range",
                    overflow);
        }
        return new WindowMembership(starts, ends, timerIds);
    }

    private void onTimer(StreamTimeService.Timer timer) {
        TimerIdentity identity = decodeTimerId(timer.timerId());
        byte[] namespace = namespace(identity.windowStart, identity.windowEnd);
        byte[] encoded = state.get(identity.key, namespace);
        if (encoded == null) return;
        BigDecimal finalSum = decodeDecimal(encoded)
                .setScale(10, RoundingMode.HALF_UP);
        output.accept(new Result(identity.key, identity.windowStart,
                identity.windowEnd, finalSum));
        state.remove(identity.key, namespace);
    }

    private void cancelNewTimers(
            WindowMembership membership, boolean[] newTimers) {
        for (int i = 0; i < newTimers.length; i++) {
            if (newTimers[i]) {
                time.cancel(StreamTimeService.TimerKind.EVENT_TIME,
                        membership.ends[i], membership.timerIds[i]);
            }
        }
    }

    private static byte[] namespace(long start, long end) {
        return ByteBuffer.allocate(16).putLong(start).putLong(end).array();
    }

    private static String timerId(byte[] key, long start, long end) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(key)
                + '.' + start + '.' + end;
    }

    private static TimerIdentity decodeTimerId(String timerId) {
        int last = timerId.lastIndexOf('.');
        int middle = last <= 0 ? -1 : timerId.lastIndexOf('.', last - 1);
        if (middle < 0 || last == timerId.length() - 1) {
            throw failure(StreamWindowException.Code.CORRUPT_TIMER,
                    "window timer identity is malformed");
        }
        try {
            byte[] key = Base64.getUrlDecoder().decode(
                    timerId.substring(0, middle));
            long start = Long.parseLong(timerId.substring(middle + 1, last));
            long end = Long.parseLong(timerId.substring(last + 1));
            if (end <= start) {
                throw new IllegalArgumentException("invalid timer fields");
            }
            return new TimerIdentity(key, start, end);
        } catch (RuntimeException malformed) {
            throw failure(StreamWindowException.Code.CORRUPT_TIMER,
                    "window timer identity is malformed", malformed);
        }
    }

    private byte[] encodeDecimal(BigDecimal value) {
        BigInteger unscaled = value.unscaledValue();
        long magnitudeBytes = ((long) unscaled.bitLength() + 8L) / 8L;
        if (magnitudeBytes + 8L > maximumValueBytes) {
            throw failure(StreamWindowException.Code.WINDOW_LIMIT,
                    "window decimal state exceeds its encoded bound");
        }
        byte[] magnitude = unscaled.toByteArray();
        if (magnitude.length + 8 > maximumValueBytes) {
            throw failure(StreamWindowException.Code.WINDOW_LIMIT,
                    "window decimal state exceeds its encoded bound");
        }
        return ByteBuffer.allocate(8 + magnitude.length)
                .putInt(value.scale()).putInt(magnitude.length)
                .put(magnitude).array();
    }

    private static BigDecimal decodeDecimal(byte[] value) {
        try {
            if (value.length < 9) throw new IllegalArgumentException("short decimal");
            ByteBuffer frame = ByteBuffer.wrap(value);
            int scale = frame.getInt();
            int length = frame.getInt();
            if (length < 1 || length != frame.remaining()) {
                throw new IllegalArgumentException("invalid decimal length");
            }
            byte[] magnitude = new byte[length];
            frame.get(magnitude);
            BigInteger unscaled = new BigInteger(magnitude);
            if (!java.util.Arrays.equals(magnitude, unscaled.toByteArray())) {
                throw new IllegalArgumentException("non-canonical decimal");
            }
            return new BigDecimal(unscaled, scale);
        } catch (RuntimeException malformed) {
            throw failure(StreamWindowException.Code.CORRUPT_STATE,
                    "window decimal state is malformed", malformed);
        }
    }

    private void ensureOutputScaleBounded(BigDecimal value) {
        long expansion = 10L - value.scale();
        if (expansion > maximumValueBytes) {
            throw failure(StreamWindowException.Code.WINDOW_LIMIT,
                    "window decimal scale exceeds the final output bound");
        }
    }

    private void checkOpen() {
        if (closed) {
            throw failure(StreamWindowException.Code.TERMINAL_FAILURE,
                    "window is closed");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private void validateCheckpointMember(int subtaskIndex,
            StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        if (subtaskIndex < 0 || member == null || payloadStore == null
                || member.kind() != StreamCheckpointMember.Kind.STATE
                || !operatorUid.equals(member.operatorUid())
                || subtaskIndex != member.subtaskIndex()
                || keyGroupStart != member.keyGroupStart()
                || keyGroupEnd != member.keyGroupEnd()
                || !stateDescriptor.stateName().equals(member.subjectId())
                || !stateDescriptor.serializerContractId().equals(
                member.codecId())
                || stateDescriptor.stateSchemaVersion()
                != member.schemaVersion()) {
            throw failure(StreamWindowException.Code.CORRUPT_STATE,
                    "window checkpoint member differs from its descriptor");
        }
    }

    private static void deleteCheckpointStaging(
            Path staged, Throwable primary) {
        if (staged == null) return;
        try {
            Files.deleteIfExists(staged);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamWindowException.Code.TERMINAL_FAILURE,
                    "window checkpoint staging cleanup failed",
                    cleanupFailure);
        }
    }

    private static final class LimitedOutputStream extends FilterOutputStream {
        private final long maximumBytes;
        private long written;

        private LimitedOutputStream(OutputStream output, long maximumBytes) {
            super(output);
            this.maximumBytes = maximumBytes;
        }

        @Override public void write(int value) throws IOException {
            requireCapacity(1);
            out.write(value);
            written++;
        }

        @Override public void write(byte[] value, int offset, int length)
                throws IOException {
            requireCapacity(length);
            out.write(value, offset, length);
            written += length;
        }

        private void requireCapacity(int bytes) throws IOException {
            if (bytes < 0 || written > maximumBytes - bytes) {
                throw new IOException(
                        "window checkpoint exceeds its configured bound");
            }
        }
    }

    private static int windowCount(long size, long slide) {
        if (size <= 0L || slide <= 0L) return Integer.MAX_VALUE;
        long count = 1L + (size - 1L) / slide;
        return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
    }

    private static StreamWindowException failure(
            StreamWindowException.Code code, String message) {
        return new StreamWindowException(code, message);
    }

    private static StreamWindowException failure(
            StreamWindowException.Code code, String message,
            Throwable cause) {
        return new StreamWindowException(code, message, cause);
    }

    private static final class WindowMembership {
        private final long[] starts;
        private final long[] ends;
        private final String[] timerIds;

        private WindowMembership(
                long[] starts, long[] ends, String[] timerIds) {
            this.starts = starts;
            this.ends = ends;
            this.timerIds = timerIds;
        }
    }

    private static final class TimerIdentity {
        private final byte[] key;
        private final long windowStart;
        private final long windowEnd;

        private TimerIdentity(byte[] key, long windowStart, long windowEnd) {
            this.key = key;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }
    }
}
