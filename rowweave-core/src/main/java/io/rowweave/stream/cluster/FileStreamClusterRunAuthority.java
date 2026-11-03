package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointDecision;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Checksummed, atomically replaced File authority for active Stream run fencing. */
public final class FileStreamClusterRunAuthority {
    private static final Object JVM_FILE_LOCK = new Object();
    private static final byte[] MAGIC =
            "RWXXSRV1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 2;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_STATE_BYTES = 256 * 1024;

    private final Path root;
    private final StreamClusterRunStateStore store;

    public FileStreamClusterRunAuthority(Path root) {
        if (root == null) {
            throw new IllegalArgumentException(
                    "Stream run authority root is required");
        }
        try {
            Files.createDirectories(root);
            if (!Files.isDirectory(root)) {
                throw new IOException("authority root is not a directory");
            }
            this.root = root.toRealPath();
            this.store = null;
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Stream run authority root is unavailable", failure);
        }
    }

    /** Uses a shared linearizable backend while preserving run-domain fencing. */
    public FileStreamClusterRunAuthority(StreamClusterRunStateStore store) {
        if (store == null) {
            throw new IllegalArgumentException(
                    "Stream run state store is required");
        }
        this.root = null;
        this.store = store;
    }

    public StreamClusterRunState create(StreamClusterRunState initial) {
        if (initial == null) {
            throw new IllegalArgumentException("initial Stream run is required");
        }
        if (store != null) {
            StreamClusterRunState existing = store.createIfAbsent(initial);
            if (!existing.equals(initial)) {
                throw failure(StreamClusterProtocolException.Code.RUN_CONFLICT,
                        "Stream run already has a different authority state");
            }
            return existing;
        }
        return locked(initial.runId(), () -> {
            Path state = statePath(initial.runId());
            if (Files.exists(state)) {
                StreamClusterRunState existing = read(state);
                if (!existing.equals(initial)) {
                    throw failure(StreamClusterProtocolException.Code.RUN_CONFLICT,
                            "Stream run already has a different authority state");
                }
                return existing;
            }
            write(state, initial);
            return initial;
        });
    }

    public StreamClusterRunState current(String runId) {
        if (store != null) {
            StreamClusterRunState current = store.current(runId);
            if (current == null) {
                throw failure(StreamClusterProtocolException.Code.RUN_CONFLICT,
                        "Stream run authority state does not exist");
            }
            return current;
        }
        return locked(runId, () -> {
            Path state = statePath(runId);
            if (!Files.exists(state)) {
                throw failure(StreamClusterProtocolException.Code.RUN_CONFLICT,
                        "Stream run authority state does not exist");
            }
            return read(state);
        });
    }

    /** Returns every durable latest-checkpoint recovery root by run ID. */
    public Map<String, CheckpointIdentity> completedCheckpointRoots() {
        Map<String, StreamClusterRunState> states = durableStates();
        Map<String, CheckpointIdentity> roots = new LinkedHashMap<>();
        for (StreamClusterRunState state : states.values()) {
            CheckpointIdentity checkpoint = state.latestCompletedCheckpoint();
            if (checkpoint != null) roots.put(state.runId(), checkpoint);
        }
        return Collections.unmodifiableMap(roots);
    }

    /** Returns every durable run ID, including runs without a completed cut. */
    public java.util.Set<String> runIds() {
        return Collections.unmodifiableSet(
                new java.util.LinkedHashSet<>(durableStates().keySet()));
    }

    /** Durably releases one terminal run from active recovery roots. */
    public void retire(String runId) {
        if (store != null) {
            store.delete(runId, current -> current);
            return;
        }
        locked(runId, () -> {
            Path state = statePath(runId);
            if (Files.deleteIfExists(state)) {
                try (FileChannel directory = FileChannel.open(
                        state.getParent(), StandardOpenOption.READ)) {
                    directory.force(true);
                }
            }
            return null;
        });
    }

    /** Retires a shared run only while the caller still owns its coordinator fence. */
    public void retire(String runId, long coordinatorTerm,
            String coordinatorIncarnation) {
        if (store == null) {
            StreamClusterRunState current = current(runId);
            requireCoordinator(current, coordinatorTerm,
                    coordinatorIncarnation);
            retire(runId);
            return;
        }
        store.delete(runId, current -> {
            requireCoordinator(current, coordinatorTerm,
                    coordinatorIncarnation);
            return current;
        });
    }

    private Map<String, StreamClusterRunState> durableStates() {
        if (store != null) {
            Map<String, StreamClusterRunState> states = new LinkedHashMap<>();
            for (StreamClusterRunState state : store.all()) {
                if (states.put(state.runId(), state) != null) {
                    throw failure(
                            StreamClusterProtocolException.Code.RUN_CONFLICT,
                            "Stream run authority catalog is duplicated");
                }
            }
            return states;
        }
        synchronized (JVM_FILE_LOCK) {
            Map<String, StreamClusterRunState> states = new LinkedHashMap<>();
            try (java.nio.file.DirectoryStream<Path> entries =
                         Files.newDirectoryStream(root, "run-*.bin")) {
                for (Path path : entries) {
                    StreamClusterRunState state = read(path);
                    if (states.put(state.runId(), state) != null) {
                        throw failure(
                                StreamClusterProtocolException.Code.RUN_CONFLICT,
                                "Stream run authority catalog is duplicated");
                    }
                }
            } catch (StreamClusterProtocolException failure) {
                throw failure;
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "Stream run authority catalog is unavailable", failure);
            }
            return states;
        }
    }

    public StreamClusterRunState recordCompletedCheckpoint(
            String runId, long coordinatorTerm,
            String coordinatorIncarnation, CheckpointDecision decision) {
        if (decision == null
                || decision.status() != CheckpointDecision.Status.COMPLETED) {
            throw new IllegalArgumentException(
                    "completed Stream checkpoint decision is required");
        }
        CheckpointIdentity checkpoint = decision.checkpointIdentity();
        return mutate(runId, current -> {
            requireCoordinator(
                    current, coordinatorTerm, coordinatorIncarnation);
            if (!current.runId().equals(checkpoint.runId())
                    || !current.attemptId().equals(checkpoint.attemptId())
                    || current.fenceToken() != checkpoint.fenceToken()
                    || checkpoint.coordinatorTerm()
                    > current.coordinatorTerm()
                    || (checkpoint.coordinatorTerm()
                    == current.coordinatorTerm()
                    && !current.coordinatorIncarnation().equals(
                    checkpoint.coordinatorIncarnation()))) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "completed checkpoint is stale for the active attempt");
            }
            CheckpointIdentity latest = current.latestCompletedCheckpoint();
            if (latest != null
                    && latest.attemptId().equals(checkpoint.attemptId())
                    && checkpoint.checkpointSequence()
                    <= latest.checkpointSequence()) {
                throw failure(StreamClusterProtocolException.Code.COMMAND_REPLAY,
                        "completed checkpoint sequence was already recorded");
            }
            return current.withCheckpoint(checkpoint);
        });
    }

    public StreamClusterRunState replaceCoordinator(
            String runId, long expectedTerm,
            String expectedIncarnation, long replacementTerm,
            String replacementIncarnation) {
        return mutate(runId, current -> {
            requireCoordinator(current, expectedTerm, expectedIncarnation);
            if (replacementTerm <= expectedTerm) {
                throw failure(
                        StreamClusterProtocolException.Code.STALE_COORDINATOR,
                        "replacement coordinator term must advance");
            }
            return current.withCoordinator(
                    replacementTerm, replacementIncarnation);
        });
    }

    public StreamClusterRunState restartAfterWorkerLoss(
            String runId, long coordinatorTerm,
            String coordinatorIncarnation,
            String lostWorkerId, String lostWorkerIncarnation,
            String newAttemptId, long newFenceToken,
            long newAssignmentGeneration, long newChannelEpoch,
            StreamWorkerPlacement newPlacement) {
        return mutate(runId, current -> {
            requireCoordinator(
                    current, coordinatorTerm, coordinatorIncarnation);
            if (!current.containsWorker(
                    lostWorkerId, lostWorkerIncarnation)) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "lost worker is not assigned to the active attempt");
            }
            if (current.attemptId().equals(newAttemptId)
                    || newFenceToken <= current.fenceToken()
                    || newAssignmentGeneration
                    <= current.assignmentGeneration()
                    || newChannelEpoch <= current.channelEpoch()) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "whole-graph restart must advance every attempt fence");
            }
            if (newPlacement == null || containsExact(
                    newPlacement, lostWorkerId, lostWorkerIncarnation)) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "whole-graph restart retains the lost worker incarnation");
            }
            return current.restart(
                    newAttemptId, newFenceToken, newAssignmentGeneration,
                    newChannelEpoch, newPlacement);
        });
    }

    public StreamClusterRunState restartAfterFailure(
            String runId, long coordinatorTerm,
            String coordinatorIncarnation,
            String newAttemptId, long newFenceToken,
            long newAssignmentGeneration, long newChannelEpoch,
            StreamWorkerPlacement newPlacement) {
        return mutate(runId, current -> {
            requireCoordinator(
                    current, coordinatorTerm, coordinatorIncarnation);
            if (current.attemptId().equals(newAttemptId)
                    || newFenceToken <= current.fenceToken()
                    || newAssignmentGeneration
                    <= current.assignmentGeneration()
                    || newChannelEpoch <= current.channelEpoch()
                    || newPlacement == null) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "whole-graph restart must advance every attempt fence");
            }
            return current.restart(
                    newAttemptId, newFenceToken, newAssignmentGeneration,
                    newChannelEpoch, newPlacement);
        });
    }

    /**
     * Atomically publishes the target generation of a checkpoint-driven
     * live rescale. The completed source cut remains the recovery root until
     * the target generation completes its first checkpoint.
     */
    public StreamClusterRunState liveRescaleCutover(
            String runId, long coordinatorTerm,
            String coordinatorIncarnation,
            String expectedAttemptId, long expectedFenceToken,
            long expectedAssignmentGeneration,
            CheckpointIdentity completedSourceCut,
            String targetAttemptId, long targetFenceToken,
            long targetAssignmentGeneration, long targetChannelEpoch,
            StreamWorkerPlacement targetPlacement) {
        return liveRescaleCutover(runId, coordinatorTerm,
                coordinatorIncarnation, expectedAttemptId,
                expectedFenceToken, expectedAssignmentGeneration,
                completedSourceCut, completedSourceCut, targetAttemptId,
                targetFenceToken, targetAssignmentGeneration,
                targetChannelEpoch, targetPlacement);
    }

    public StreamClusterRunState liveRescaleCutover(
            String runId, long coordinatorTerm,
            String coordinatorIncarnation,
            String expectedAttemptId, long expectedFenceToken,
            long expectedAssignmentGeneration,
            CheckpointIdentity completedSourceCut,
            CheckpointIdentity targetRestoreCut,
            String targetAttemptId, long targetFenceToken,
            long targetAssignmentGeneration, long targetChannelEpoch,
            StreamWorkerPlacement targetPlacement) {
        if (completedSourceCut == null || targetPlacement == null) {
            throw new IllegalArgumentException(
                    "live rescale cut and target placement are required");
        }
        return mutate(runId, current -> {
            requireCoordinator(current, coordinatorTerm,
                    coordinatorIncarnation);
            StreamClusterRunState target = new StreamClusterRunState(
                    current.coordinatorTerm(),
                    current.coordinatorIncarnation(), current.runId(),
                    targetAttemptId, targetFenceToken,
                    targetAssignmentGeneration, targetChannelEpoch,
                    targetPlacement, targetRestoreCut);
            if (current.equals(target)) return current;
            if (!current.attemptId().equals(expectedAttemptId)
                    || current.fenceToken() != expectedFenceToken
                    || current.assignmentGeneration()
                    != expectedAssignmentGeneration
                    || current.latestCompletedCheckpoint() == null
                    || !current.latestCompletedCheckpoint().equals(
                    completedSourceCut)
                    || !current.runId().equals(completedSourceCut.runId())
                    || !current.attemptId().equals(
                    completedSourceCut.attemptId())
                    || current.fenceToken()
                    != completedSourceCut.fenceToken()
                    || targetRestoreCut == null
                    || !current.runId().equals(targetRestoreCut.runId())
                    || (!completedSourceCut.equals(targetRestoreCut)
                    && (!targetAttemptId.equals(
                    targetRestoreCut.attemptId())
                    || targetFenceToken != targetRestoreCut.fenceToken()))
                    || targetAttemptId.equals(current.attemptId())
                    || targetFenceToken
                    != Math.addExact(current.fenceToken(), 1L)
                    || targetAssignmentGeneration != Math.addExact(
                    current.assignmentGeneration(), 1L)
                    || targetChannelEpoch != Math.addExact(
                    current.channelEpoch(), 1L)) {
                throw failure(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "live rescale source cut or target generation is stale");
            }
            return target;
        });
    }

    public Path statePath(String runId) {
        StreamControlMessage.text(runId, "run ID");
        if (root == null) {
            throw new IllegalStateException(
                    "shared Stream run authority has no local state path");
        }
        return root.resolve("run-" + sha256Hex(
                runId.getBytes(StandardCharsets.UTF_8)) + ".bin");
    }

    public boolean exists(String runId) {
        StreamControlMessage.text(runId, "run ID");
        if (store != null) return store.current(runId) != null;
        synchronized (JVM_FILE_LOCK) {
            return Files.exists(statePath(runId));
        }
    }

    private StreamClusterRunState mutate(
            String runId, Mutation mutation) {
        if (store != null) {
            StreamClusterRunState next = store.update(
                    runId, mutation::apply);
            if (next == null) {
                throw failure(StreamClusterProtocolException.Code.RUN_CONFLICT,
                        "Stream run authority state does not exist");
            }
            return next;
        }
        return locked(runId, () -> {
            Path state = statePath(runId);
            if (!Files.exists(state)) {
                throw failure(StreamClusterProtocolException.Code.RUN_CONFLICT,
                        "Stream run authority state does not exist");
            }
            StreamClusterRunState current = read(state);
            StreamClusterRunState next = mutation.apply(current);
            write(state, next);
            return next;
        });
    }

    private StreamClusterRunState locked(String runId, Action action) {
        StreamControlMessage.text(runId, "run ID");
        Path lockPath = root.resolve("run-" + sha256Hex(
                runId.getBytes(StandardCharsets.UTF_8)) + ".lock");
        synchronized (JVM_FILE_LOCK) {
            try (FileChannel channel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 FileLock ignored = channel.lock()) {
                return action.run();
            } catch (StreamClusterProtocolException failure) {
                throw failure;
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "Stream run authority operation failed", failure);
            }
        }
    }

    private static void requireCoordinator(
            StreamClusterRunState current, long term, String incarnation) {
        if (current.coordinatorTerm() != term
                || !current.coordinatorIncarnation().equals(incarnation)) {
            throw failure(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    "Stream coordinator fence is stale");
        }
    }

    private static boolean containsExact(
            StreamWorkerPlacement placement,
            String workerId, String workerIncarnation) {
        for (StreamWorkerPlacement.Assignment assignment
                : placement.assignments()) {
            if (assignment.workerId().equals(workerId)
                    && assignment.workerIncarnation().equals(
                            workerIncarnation)) {
                return true;
            }
        }
        return false;
    }

    private static void write(Path target, StreamClusterRunState state)
            throws IOException {
        byte[] frame = encode(state);
        Path temporary = Files.createTempFile(
                target.getParent(), ".stream-run-", ".tmp");
        boolean moved = false;
        try (FileChannel channel = FileChannel.open(temporary,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(frame);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(temporary, target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            moved = true;
            try (FileChannel directory = FileChannel.open(
                    target.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            if (!moved) Files.deleteIfExists(temporary);
        }
    }

    private static StreamClusterRunState read(Path path) throws IOException {
        long size = Files.size(path);
        if (size <= 0L || size > MAX_STATE_BYTES) {
            throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                    "Stream run authority frame exceeds its bound");
        }
        return decode(Files.readAllBytes(path));
    }

    static byte[] encode(StreamClusterRunState state) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream() {
                @Override public synchronized void write(int value) {
                    requireRoom(1);
                    super.write(value);
                }

                @Override public synchronized void write(byte[] value,
                        int offset, int length) {
                    requireRoom(length);
                    super.write(value, offset, length);
                }

                private void requireRoom(int length) {
                    if (length < 0 || count + (long) length
                            > MAX_STATE_BYTES - MAGIC.length - 8
                            - DIGEST_BYTES) {
                        throw failure(
                                StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                                "Stream run authority frame exceeds its bound");
                    }
                }
            };
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeLong(state.coordinatorTerm());
                writeText(body, state.coordinatorIncarnation());
                writeText(body, state.runId());
                writeText(body, state.attemptId());
                body.writeLong(state.fenceToken());
                body.writeLong(state.assignmentGeneration());
                body.writeLong(state.channelEpoch());
                body.writeInt(state.placement().assignments().size());
                for (StreamWorkerPlacement.Assignment assignment
                        : state.placement().assignments()) {
                    body.writeInt(assignment.taskOrdinal());
                    writeText(body, assignment.operatorUid());
                    body.writeInt(assignment.subtaskIndex());
                    writeText(body, assignment.workerId());
                    writeText(body, assignment.workerIncarnation());
                }
                CheckpointIdentity checkpoint =
                        state.latestCompletedCheckpoint();
                body.writeBoolean(checkpoint != null);
                if (checkpoint != null) writeCheckpoint(body, checkpoint);
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(VERSION);
                frame.writeInt(body.length);
                frame.write(body);
                frame.write(sha256(body));
            }
            byte[] frame = frameBytes.toByteArray();
            if (frame.length > MAX_STATE_BYTES) {
                throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                        "Stream run authority frame exceeds its bound");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static StreamClusterRunState decode(byte[] frame) {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            int bodyLength = input.readInt();
            int headerBytes = MAGIC.length + 8;
            if (!MessageDigest.isEqual(MAGIC, magic)
                    || (version != 1 && version != VERSION) || bodyLength < 0
                    || headerBytes + (long) bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw failure(
                        StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                        "Stream run authority frame is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw failure(StreamClusterProtocolException.Code.CHECKSUM_MISMATCH,
                        "Stream run authority checksum differs");
            }
            try (DataInputStream value = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                long term = value.readLong();
                String coordinator = readText(value);
                String runId = readText(value);
                String attemptId = readText(value);
                long fence = value.readLong();
                long generation = value.readLong();
                long channelEpoch = value.readLong();
                int count = value.readInt();
                // Legacy assignments need at least 14 wire bytes; the
                // existing state-frame bound limits physical task count.
                if (count <= 0 || count > MAX_STATE_BYTES / 14) {
                    throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                            "Stream run placement count is invalid");
                }
                List<StreamWorkerPlacement.Assignment> assignments =
                        new ArrayList<>(count);
                for (int index = 0; index < count; index++) {
                    int ordinal = value.readInt();
                    if (version == 1) {
                        assignments.add(new StreamWorkerPlacement.Assignment(
                                ordinal, readText(value), readText(value)));
                    } else {
                        String operatorUid = readText(value);
                        int subtaskIndex = value.readInt();
                        assignments.add(new StreamWorkerPlacement.Assignment(
                                ordinal, operatorUid, subtaskIndex,
                                readText(value), readText(value)));
                    }
                }
                CheckpointIdentity checkpoint = value.readBoolean()
                        ? readCheckpoint(value) : null;
                if (value.available() != 0) {
                    throw failure(
                            StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                            "Stream run authority frame has trailing bytes");
                }
                return new StreamClusterRunState(
                        term, coordinator, runId, attemptId, fence,
                        generation, channelEpoch,
                        new StreamWorkerPlacement(assignments), checkpoint);
            }
        } catch (StreamClusterProtocolException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream run authority frame is truncated");
        } catch (IOException malformed) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream run authority frame is malformed");
        }
    }

    private static void writeCheckpoint(
            DataOutputStream output, CheckpointIdentity checkpoint)
            throws IOException {
        writeText(output, checkpoint.definitionId());
        writeText(output, checkpoint.artifactId());
        writeText(output, checkpoint.runId());
        writeText(output, checkpoint.attemptId());
        output.writeLong(checkpoint.fenceToken());
        output.writeLong(checkpoint.coordinatorTerm());
        writeText(output, checkpoint.coordinatorIncarnation());
        output.writeLong(checkpoint.checkpointSequence());
        writeText(output, checkpoint.planDigest());
        writeText(output, checkpoint.deploymentDigest());
        output.writeInt(checkpoint.dataProtocolVersion());
        output.writeInt(checkpoint.stateFormatVersion());
    }

    private static CheckpointIdentity readCheckpoint(DataInputStream input)
            throws IOException {
        return new CheckpointIdentity(
                readText(input), readText(input), readText(input),
                readText(input), input.readLong(), input.readLong(),
                readText(input), input.readLong(), readText(input),
                readText(input), input.readInt(), input.readInt());
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        StreamControlMessage.text(value, "Stream run authority text");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > StreamControlMessage.MAX_TEXT_BYTES) {
            throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                    "Stream run authority text is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            return StreamControlMessage.text(
                    value, "Stream run authority text");
        } catch (CharacterCodingException | IllegalArgumentException invalid) {
            throw failure(
                    StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream run authority text is malformed");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value);
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[digest.length * 2];
        for (int index = 0; index < digest.length; index++) {
            int current = digest[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static StreamClusterProtocolException failure(
            StreamClusterProtocolException.Code code, String message) {
        return new StreamClusterProtocolException(code, message);
    }

    private interface Action {
        StreamClusterRunState run() throws IOException;
    }

    private interface Mutation {
        StreamClusterRunState apply(StreamClusterRunState current);
    }
}
