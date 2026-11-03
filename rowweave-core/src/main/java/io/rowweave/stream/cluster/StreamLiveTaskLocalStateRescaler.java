package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.ManagedKeyedValueState;
import io.rowweave.stream.runtime.OfflineKeyedStateRescaler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Registry-selected bounded migration for non-key-group operator state. */
final class StreamLiveTaskLocalStateRescaler {
    private static final byte[] ASYNC_MAGIC =
            "RWXXAS01".getBytes(StandardCharsets.US_ASCII);
    private static final String ASYNC_NAME = "async-wait-inflight";
    private static final String ASYNC_CODEC =
            "rowweave.stream.async-wait-state.v3";
    private static final int ASYNC_VERSION = 3;
    private static final String LEGACY_ASYNC_CODEC =
            "rowweave.stream.async-wait-state.v2";
    private static final int LEGACY_ASYNC_VERSION = 2;

    private static final byte[] EVENT_TIME_MAGIC =
            "RWXXET01".getBytes(StandardCharsets.US_ASCII);
    private static final String EVENT_TIME_NAME =
            "event-time-assignment-generator";
    private static final String EVENT_TIME_CODEC =
            "rowweave.stream.event-time-assignment-state.v1";
    private static final int EVENT_TIME_VERSION = 1;

    private static final String GATE_NAME = "__rowweave_gate__/inputs";
    private static final String GATE_CODEC =
            "rowweave.stream.multi-input-gate.v1";
    private static final String BROADCAST_DIGEST_PREFIX =
            "__rowweave_broadcast_digest__/";
    private static final String BROADCAST_DIGEST_CODEC =
            "rowweave.stream.broadcast-digest.v1";

    private StreamLiveTaskLocalStateRescaler() { }

    static List<StreamCheckpointMember> select(
            StreamOperatorDefinitionRegistry.TaskLocalStateContract contract,
            PortableStreamNode node,
            List<StreamCheckpointMember> members) {
        String subject = subject(contract, node);
        String codec = codec(contract);
        List<StreamCheckpointMember> selected = new ArrayList<>();
        for (StreamCheckpointMember member : members) {
            if (member.kind() == StreamCheckpointMember.Kind.STATE
                    && node.operatorUid().equals(member.operatorUid())
                    && subject.equals(member.subjectId())
                    && (codec.equals(member.codecId())
                    || contract == StreamOperatorDefinitionRegistry
                    .TaskLocalStateContract.ASYNC_WAIT
                    && LEGACY_ASYNC_CODEC.equals(member.codecId()))) {
                selected.add(member);
            }
        }
        selected.sort(Comparator.comparingInt(
                StreamCheckpointMember::subtaskIndex));
        return selected;
    }

    static List<StreamCheckpointMember> rescale(
            StreamOperatorDefinitionRegistry.TaskLocalStateContract contract,
            PortableStreamNode node, int maximumParallelism,
            int targetParallelism,
            PortableRowSchema inputSchema,
            List<StreamCheckpointMember> source,
            FileCheckpointPayloadStore payloadStore) {
        if (contract == null || node == null || maximumParallelism < 1
                || targetParallelism < 1 || source == null
                || source.isEmpty() || payloadStore == null) {
            throw new IllegalArgumentException(
                    "live task-local state migration is incomplete");
        }
        List<byte[]> frames = new ArrayList<>(source.size());
        for (StreamCheckpointMember member : source) {
            frames.add(read(member, payloadStore));
        }
        List<byte[]> targetFrames;
        switch (contract) {
            case ASYNC_WAIT:
                targetFrames = rescaleAsync(node, inputSchema,
                        maximumParallelism, targetParallelism, frames);
                break;
            case EVENT_TIME_ASSIGNMENT:
                targetFrames = duplicate(targetParallelism,
                        mergeEventTime(frames));
                break;
            case MULTI_INPUT_GATE:
                targetFrames = duplicate(targetParallelism,
                        StreamMultiInputGate.mergeStateForRescale(frames));
                break;
            case BROADCAST_DIGEST:
                targetFrames = duplicate(targetParallelism,
                        requireIdentical(frames));
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown live task-local state contract");
        }
        List<StreamCheckpointMember> result = new ArrayList<>(
                targetParallelism);
        for (int subtask = 0; subtask < targetParallelism; subtask++) {
            FileCheckpointPayloadStore.StoredPayload stored = store(
                    targetFrames.get(subtask), payloadStore);
            result.add(StreamCheckpointMember.stateHandle(
                    node.operatorUid(), subtask,
                    OfflineKeyedStateRescaler.keyGroupStart(
                            subtask, targetParallelism, maximumParallelism),
                    OfflineKeyedStateRescaler.keyGroupEnd(
                            subtask, targetParallelism, maximumParallelism),
                    subject(contract, node), codec(contract), version(contract),
                    stored.digest(), stored.contentBytes()));
        }
        return Collections.unmodifiableList(result);
    }

    private static List<byte[]> rescaleAsync(PortableStreamNode node,
            PortableRowSchema inputSchema, int maximumParallelism,
            int targetParallelism, List<byte[]> frames) {
        if (!(node.binding() instanceof StreamAsyncBinding)
                || inputSchema == null) {
            throw invalid("async live rescale contract is incomplete");
        }
        StreamAsyncBinding binding = (StreamAsyncBinding) node.binding();
        List<List<AsyncPending>> targets = new ArrayList<>(
                targetParallelism);
        for (int target = 0; target < targetParallelism; target++) {
            targets.add(new ArrayList<>());
        }
        RowSerializer rowSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(inputSchema));
        int[] keyIndices = indices(inputSchema, node.stableKeyFields());
        RowSerializer keySerializer = keyIndices.length == 0 ? null
                : new RowSerializer(PortablePlanCompiler.toDataSchema(
                keySchema(inputSchema, keyIndices)));
        List<AsyncSnapshot> snapshots = new ArrayList<>(frames.size());
        for (byte[] frame : frames) {
            AsyncSnapshot snapshot = decodeAsync(frame,
                    binding.resources().maximumConcurrency(),
                    binding.resources().maximumPendingBytes());
            snapshots.add(snapshot);
            for (AsyncPending pending : snapshot.pending) {
                int target = asyncTarget(pending, rowSerializer,
                        keySerializer, keyIndices, maximumParallelism,
                        targetParallelism);
                targets.get(target).add(pending);
            }
        }
        List<byte[]> result = new ArrayList<>(targetParallelism);
        String sourceDigest = digest(frames);
        List<AsyncControl> controls = mergeAsyncControls(snapshots,
                binding.resources().maximumConcurrency());
        for (int subtask = 0; subtask < targetParallelism; subtask++) {
            List<AsyncPending> pending = targets.get(subtask);
            if (pending.size()
                    > binding.resources().maximumConcurrency()) {
                throw invalid(
                        "async live rescale target concurrency is exceeded");
            }
            long retainedBytes = 0L;
            for (AsyncPending operation : pending) {
                retainedBytes = Math.addExact(retainedBytes,
                        operation.retainedBytes());
            }
            if (retainedBytes
                    > binding.resources().maximumPendingBytes()) {
                throw invalid(
                        "async live rescale target bytes are exceeded");
            }
            result.add(encodeAsync(node.operatorUid(), sourceDigest,
                    subtask, pending, controls));
        }
        return result;
    }

    private static AsyncSnapshot decodeAsync(byte[] frame,
            int maximumPending, long maximumBytes) {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[ASYNC_MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            if (!Arrays.equals(magic, ASYNC_MAGIC)
                    || version != ASYNC_VERSION
                    && version != LEGACY_ASYNC_VERSION) {
                throw invalid("async live rescale state is incompatible");
            }
            String prefix = input.readUTF();
            long inputSequence = input.readLong();
            long emittedSequence = input.readLong();
            int count = input.readInt();
            if (prefix.isEmpty() || inputSequence < 0L
                    || emittedSequence < 0L
                    || emittedSequence > inputSequence || count < 0
                    || count > maximumPending) {
                throw invalid("async live rescale state is corrupt");
            }
            List<AsyncPending> pending = new ArrayList<>(count);
            long previous = emittedSequence;
            long retainedBytes = 0L;
            for (int index = 0; index < count; index++) {
                long sequence = input.readLong();
                String identity = input.readUTF();
                int attempt = input.readInt();
                Long timestamp = input.readBoolean()
                        ? Long.valueOf(input.readLong()) : null;
                byte[] inputFrame = readFrame(input, maximumBytes, false);
                byte[] requestFrame = readFrame(input, maximumBytes, false);
                int responseSize = input.readInt();
                byte[] response = responseSize == -1 ? null
                        : readFrame(input, responseSize, maximumBytes, true);
                if (sequence <= previous || sequence > inputSequence
                        || attempt < 1 || identity.isEmpty()
                        || version == LEGACY_ASYNC_VERSION
                        && !identity.equals(prefix + ':' + sequence)) {
                    throw invalid("async live rescale state is corrupt");
                }
                AsyncPending operation = new AsyncPending(identity, attempt,
                        timestamp, inputFrame, requestFrame, response);
                retainedBytes = Math.addExact(
                        retainedBytes, operation.retainedBytes());
                if (retainedBytes > maximumBytes) {
                    throw invalid("async live rescale bytes are exceeded");
                }
                pending.add(operation);
                previous = sequence;
            }
            int controlCount = input.readInt();
            if (controlCount < 0 || controlCount > maximumPending + 2) {
                throw invalid("async live rescale controls are invalid");
            }
            List<AsyncControl> controls = new ArrayList<>(controlCount);
            long previousFrontier = 0L;
            for (int index = 0; index < controlCount; index++) {
                AsyncControl control = new AsyncControl(input.readInt(),
                        input.readLong(), input.readLong(), input.readLong());
                if (!control.valid()
                        || control.frontier < previousFrontier
                        || control.frontier > inputSequence) {
                    throw invalid(
                            "async live rescale controls are invalid");
                }
                controls.add(control);
                previousFrontier = control.frontier;
            }
            if (input.read() != -1) {
                throw invalid("async live rescale state is corrupt");
            }
            return new AsyncSnapshot(pending, controls);
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "async live rescale state is corrupt", failure);
        }
    }

    private static byte[] encodeAsync(String operatorUid,
            String sourceDigest, int subtask, List<AsyncPending> pending,
            List<AsyncControl> controls) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(ASYNC_MAGIC);
                output.writeInt(ASYNC_VERSION);
                output.writeUTF("rescale-" + digest(operatorUid + ':'
                        + sourceDigest + ':' + subtask).substring(0, 32));
                output.writeLong(pending.size());
                output.writeLong(0L);
                output.writeInt(pending.size());
                long sequence = 0L;
                for (AsyncPending operation : pending) {
                    output.writeLong(++sequence);
                    output.writeUTF(operation.identity);
                    output.writeInt(operation.attempt);
                    output.writeBoolean(operation.timestamp != null);
                    if (operation.timestamp != null) {
                        output.writeLong(operation.timestamp);
                    }
                    writeFrame(output, operation.inputFrame);
                    writeFrame(output, operation.requestFrame);
                    if (operation.responseFrame == null) {
                        output.writeInt(-1);
                    } else {
                        writeFrame(output, operation.responseFrame);
                    }
                }
                output.writeInt(controls.size());
                for (AsyncControl control : controls) {
                    output.writeInt(control.kind);
                    output.writeLong(pending.size());
                    output.writeLong(control.value);
                    output.writeLong(control.generation);
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "async live rescale encoding failed", impossible);
        }
    }

    private static List<AsyncControl> mergeAsyncControls(
            List<AsyncSnapshot> snapshots, int maximumControls) {
        Long watermark = null;
        boolean anyStatus = false;
        boolean allIdle = !snapshots.isEmpty();
        long statusGeneration = 0L;
        for (AsyncSnapshot snapshot : snapshots) {
            AsyncControl lastWatermark = null;
            AsyncControl lastStatus = null;
            for (AsyncControl control : snapshot.controls) {
                if (control.kind == 1) lastWatermark = control;
                else lastStatus = control;
            }
            if (lastWatermark != null) {
                watermark = watermark == null ? lastWatermark.value
                        : Math.min(watermark, lastWatermark.value);
            }
            if (lastStatus == null) {
                allIdle = false;
            } else {
                anyStatus = true;
                allIdle &= lastStatus.kind == 2;
                statusGeneration = Math.max(
                        statusGeneration, lastStatus.generation);
            }
        }
        List<AsyncControl> merged = new ArrayList<>(2);
        if (watermark != null) {
            merged.add(new AsyncControl(1, 0L, watermark, 0L));
        }
        if (anyStatus) {
            merged.add(new AsyncControl(
                    allIdle ? 2 : 3, 0L, 0L, statusGeneration));
        }
        if (merged.size() > maximumControls) {
            AsyncControl status = merged.get(1);
            if (status.kind == 3) {
                return Collections.singletonList(status);
            }
            return Collections.singletonList(merged.get(0));
        }
        return Collections.unmodifiableList(merged);
    }

    private static int asyncTarget(AsyncPending pending,
            RowSerializer rowSerializer, RowSerializer keySerializer,
            int[] keyIndices, int maximumParallelism,
            int targetParallelism) {
        byte[] key;
        if (keySerializer == null) {
            key = pending.identity.getBytes(StandardCharsets.UTF_8);
        } else {
            DataRow row = rowSerializer.deserialize(pending.inputFrame);
            key = keySerializer.serializeProjected(row, keyIndices);
        }
        int keyGroup = ManagedKeyedValueState.keyGroup(
                key, maximumParallelism);
        return Math.min(targetParallelism - 1,
                (int) ((long) keyGroup * targetParallelism
                        / maximumParallelism));
    }

    private static byte[] readFrame(DataInputStream input,
            long maximumBytes, boolean emptyAllowed) throws IOException {
        return readFrame(input, input.readInt(), maximumBytes, emptyAllowed);
    }

    private static byte[] readFrame(DataInputStream input, int size,
            long maximumBytes, boolean emptyAllowed) throws IOException {
        if (size < (emptyAllowed ? 0 : 1) || size > maximumBytes
                || size > Integer.MAX_VALUE - 8) {
            throw invalid("async live rescale frame is invalid");
        }
        byte[] frame = new byte[size];
        input.readFully(frame);
        return frame;
    }

    private static void writeFrame(DataOutputStream output, byte[] frame)
            throws IOException {
        output.writeInt(frame.length);
        output.write(frame);
    }

    private static int[] indices(PortableRowSchema schema,
            List<String> fields) {
        int[] result = new int[fields.size()];
        for (int offset = 0; offset < fields.size(); offset++) {
            result[offset] = -1;
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(fields.get(offset))) {
                    result[offset] = index;
                    break;
                }
            }
            if (result[offset] < 0) {
                throw invalid("async live rescale key field is unavailable");
            }
        }
        return result;
    }

    private static PortableRowSchema keySchema(PortableRowSchema schema,
            int[] indices) {
        List<io.rowweave.portable.PortableField> fields =
                new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.field(index));
        return new PortableRowSchema(fields);
    }

    private static final class AsyncSnapshot {
        final List<AsyncPending> pending;
        final List<AsyncControl> controls;

        private AsyncSnapshot(List<AsyncPending> pending,
                List<AsyncControl> controls) {
            this.pending = pending;
            this.controls = controls;
        }
    }

    private static final class AsyncPending {
        final String identity;
        final int attempt;
        final Long timestamp;
        final byte[] inputFrame;
        final byte[] requestFrame;
        final byte[] responseFrame;

        private AsyncPending(String identity, int attempt, Long timestamp,
                byte[] inputFrame, byte[] requestFrame,
                byte[] responseFrame) {
            this.identity = identity;
            this.attempt = attempt;
            this.timestamp = timestamp;
            this.inputFrame = inputFrame;
            this.requestFrame = requestFrame;
            this.responseFrame = responseFrame;
        }

        private long retainedBytes() {
            long bytes = Math.addExact(
                    (long) inputFrame.length, requestFrame.length);
            return responseFrame == null ? bytes
                    : Math.addExact(bytes, responseFrame.length);
        }
    }

    private static final class AsyncControl {
        final int kind;
        final long frontier;
        final long value;
        final long generation;

        private AsyncControl(int kind, long frontier,
                long value, long generation) {
            this.kind = kind;
            this.frontier = frontier;
            this.value = value;
            this.generation = generation;
        }

        private boolean valid() {
            return frontier >= 0L
                    && (kind == 1 && generation == 0L
                    || (kind == 2 || kind == 3)
                    && value == 0L && generation > 0L);
        }
    }

    private static byte[] mergeEventTime(List<byte[]> frames) {
        long maximumTimestamp = Long.MIN_VALUE;
        long lastWatermark = Long.MAX_VALUE;
        long statusGeneration = 0L;
        boolean idle = true;
        long remainingMillis = 0L;
        for (byte[] frame : frames) {
            try (DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame))) {
                byte[] magic = new byte[EVENT_TIME_MAGIC.length];
                input.readFully(magic);
                if (!Arrays.equals(magic, EVENT_TIME_MAGIC)
                        || input.readInt() != EVENT_TIME_VERSION) {
                    throw invalid(
                            "event-time live rescale state is incompatible");
                }
                maximumTimestamp = Math.max(
                        maximumTimestamp, input.readLong());
                lastWatermark = Math.min(lastWatermark, input.readLong());
                statusGeneration = Math.max(
                        statusGeneration, input.readLong());
                boolean sourceIdle = input.readBoolean();
                idle &= sourceIdle;
                remainingMillis = Math.max(
                        remainingMillis, input.readLong());
                if (input.read() != -1) {
                    throw invalid("event-time live rescale state is corrupt");
                }
            } catch (IOException failure) {
                throw new IllegalArgumentException(
                        "event-time live rescale state is corrupt", failure);
            }
        }
        if (lastWatermark == Long.MAX_VALUE) lastWatermark = Long.MIN_VALUE;
        if (lastWatermark > maximumTimestamp) {
            throw invalid("event-time live rescale frontier is invalid");
        }
        if (idle) remainingMillis = 0L;
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(EVENT_TIME_MAGIC);
                output.writeInt(EVENT_TIME_VERSION);
                output.writeLong(maximumTimestamp);
                output.writeLong(lastWatermark);
                output.writeLong(statusGeneration);
                output.writeBoolean(idle);
                output.writeLong(remainingMillis);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "event-time live rescale encoding failed", impossible);
        }
    }

    private static List<byte[]> duplicate(int count, byte[] frame) {
        List<byte[]> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(frame.clone());
        }
        return result;
    }

    private static byte[] read(StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        Path staged = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-live-task-local-", ".restore");
            payloadStore.restore(
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes()),
                    staged);
            return Files.readAllBytes(staged);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "live task-local state restore failed", failure);
        } finally {
            delete(staged);
        }
    }

    private static FileCheckpointPayloadStore.StoredPayload store(
            byte[] frame, FileCheckpointPayloadStore payloadStore) {
        Path staged = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-live-task-local-", ".snapshot");
            Files.write(staged, frame);
            return payloadStore.store(staged);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "live task-local state publication failed", failure);
        } finally {
            delete(staged);
        }
    }

    private static String subject(
            StreamOperatorDefinitionRegistry.TaskLocalStateContract contract,
            PortableStreamNode node) {
        switch (contract) {
            case ASYNC_WAIT: return ASYNC_NAME;
            case EVENT_TIME_ASSIGNMENT: return EVENT_TIME_NAME;
            case MULTI_INPUT_GATE: return GATE_NAME;
            case BROADCAST_DIGEST:
                if (!(node.binding() instanceof StreamProcessBinding)
                        || ((StreamProcessBinding) node.binding())
                        .broadcastState() == null) {
                    throw invalid(
                            "broadcast digest contract is incomplete");
                }
                return BROADCAST_DIGEST_PREFIX
                        + ((StreamProcessBinding) node.binding())
                        .broadcastState().stateName();
            default: throw new IllegalArgumentException(
                    "unknown live task-local state contract");
        }
    }

    private static String codec(
            StreamOperatorDefinitionRegistry.TaskLocalStateContract contract) {
        switch (contract) {
            case ASYNC_WAIT: return ASYNC_CODEC;
            case EVENT_TIME_ASSIGNMENT: return EVENT_TIME_CODEC;
            case MULTI_INPUT_GATE: return GATE_CODEC;
            case BROADCAST_DIGEST: return BROADCAST_DIGEST_CODEC;
            default: throw new IllegalArgumentException(
                    "unknown live task-local state contract");
        }
    }

    private static int version(
            StreamOperatorDefinitionRegistry.TaskLocalStateContract contract) {
        return contract == StreamOperatorDefinitionRegistry
                .TaskLocalStateContract.ASYNC_WAIT ? ASYNC_VERSION : 1;
    }

    private static byte[] requireIdentical(List<byte[]> frames) {
        byte[] expected = frames.get(0);
        for (int index = 1; index < frames.size(); index++) {
            if (!Arrays.equals(expected, frames.get(index))) {
                throw invalid(
                        "broadcast replicas have different logical digests");
            }
        }
        return expected;
    }

    private static String digest(List<byte[]> frames) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (byte[] frame : frames) digest.update(frame);
            return hex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String digest(String value) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte current : value) result.append(String.format(
                java.util.Locale.ROOT, "%02x", current & 0xff));
        return result.toString();
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    private static void delete(Path staged) {
        if (staged == null) return;
        try {
            Files.deleteIfExists(staged);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "live task-local staging cleanup failed", failure);
        }
    }
}
