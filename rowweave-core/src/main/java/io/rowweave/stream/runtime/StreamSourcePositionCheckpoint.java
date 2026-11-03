package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** Mailbox-owned durable cut for one exact replayable source split. */
public final class StreamSourcePositionCheckpoint {
    private static final int MAGIC = 0x52575053; // RWPS
    private static final int VERSION = 1;
    private static final int MAX_TEXT_BYTES = 1024;
    private static final int MAX_POSITION_FRAME_BYTES = 4 * 1024 * 1024;

    private final String artifactId;
    private final String sourceOperatorUid;
    private final int subtaskIndex;
    private final StreamSourceBinding binding;
    private final StreamSourceSplit split;
    private final Path workingDirectory;
    private final FileCheckpointPayloadStore payloadStore;
    private final Thread ownerThread;

    private long logicalPosition;
    private byte[] positionFrame;
    private boolean emitted;
    private boolean restored;

    public StreamSourcePositionCheckpoint(
            String artifactId, String sourceOperatorUid, int subtaskIndex,
            StreamSourceBinding binding, StreamSourceSplit split,
            Path workingDirectory, FileCheckpointPayloadStore payloadStore,
            long initialLogicalPosition, byte[] initialPositionFrame) {
        if (!hasText(artifactId) || !hasText(sourceOperatorUid)
                || subtaskIndex < 0 || binding == null || split == null
                || workingDirectory == null || payloadStore == null
                || initialLogicalPosition < 0L
                || binding.replayGuarantee()
                != StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED
                || (!binding.dynamicDiscovery()
                && !binding.fixedSplits().contains(split))) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "exact source checkpoint configuration is invalid");
        }
        this.artifactId = artifactId;
        this.sourceOperatorUid = sourceOperatorUid;
        this.subtaskIndex = subtaskIndex;
        this.binding = binding;
        this.split = split;
        this.workingDirectory = workingDirectory.toAbsolutePath().normalize();
        this.payloadStore = payloadStore;
        this.logicalPosition = initialLogicalPosition;
        this.positionFrame = requireFrame(initialPositionFrame);
        this.ownerThread = Thread.currentThread();
        ensureDirectory(this.workingDirectory);
    }

    /** Records the position only after its corresponding record was emitted. */
    public void recordEmitted(byte[] nextPositionFrame) {
        checkOwner();
        byte[] nextFrame = requireFrame(nextPositionFrame);
        final long nextPosition;
        try {
            nextPosition = Math.addExact(logicalPosition, 1L);
        } catch (ArithmeticException overflow) {
            throw failure(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                    "source logical position overflow", overflow);
        }
        logicalPosition = nextPosition;
        positionFrame = nextFrame;
        emitted = true;
    }

    /** Advances engine-owned enumerator metadata at a no-record boundary. */
    public void updatePosition(byte[] nextPositionFrame) {
        checkOwner();
        positionFrame = requireFrame(nextPositionFrame);
        emitted = true;
    }

    /** Persists the last position observed before the ordered barrier cut. */
    public StreamCheckpointMember snapshot() {
        checkOwner();
        byte[] payload = encode();
        if (payload.length > payloadStore.maximumPayloadBytes()) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "source position checkpoint exceeds its staging bound");
        }
        Path staged = temporary("snapshot");
        Throwable primary = null;
        try {
            Files.write(staged, payload, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(staged);
            return StreamCheckpointMember.sourcePosition(
                    sourceOperatorUid, subtaskIndex, split.splitId(),
                    split.positionCodecId(), logicalPosition,
                    stored.digest(), stored.contentBytes());
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException writeFailure) {
            primary = writeFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "source position snapshot staging failed", writeFailure);
        } finally {
            cleanup(staged, primary);
        }
    }

    /** Restores only an exact member produced for this provider and split. */
    public void restore(StreamCheckpointMember member) {
        restore(member, false);
    }

    /** Rebinds one authority-proven split position to its new task owner. */
    void restoreReassigned(StreamCheckpointMember member) {
        restore(member, true);
    }

    private void restore(StreamCheckpointMember member,
            boolean reassigned) {
        checkOwner();
        if (emitted || restored) {
            throw failure(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                    "source position restore requires a fresh mailbox cut");
        }
        validateMember(member, reassigned);
        Path restored = temporary("restore");
        Throwable primary = null;
        try {
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), restored);
            RestoredPosition decoded = decode(restored, member,
                    reassigned);
            logicalPosition = decoded.logicalPosition;
            positionFrame = decoded.positionFrame;
            this.restored = true;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            cleanup(restored, primary);
        }
    }

    public long logicalPosition() {
        checkOwner();
        return logicalPosition;
    }

    public byte[] currentPositionFrame() {
        checkOwner();
        return positionFrame.clone();
    }

    private byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    Math.min(MAX_POSITION_FRAME_BYTES,
                            512 + positionFrame.length));
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeInt(VERSION);
                writeText(output, artifactId);
                writeText(output, sourceOperatorUid);
                output.writeInt(subtaskIndex);
                writeText(output, binding.providerKey());
                writeText(output, binding.providerContractDigest());
                writeText(output, binding.configurationDigest());
                writeText(output, split.splitId());
                writeText(output, split.positionCodecId());
                writeText(output, digest(split.descriptorFrame()));
                output.writeLong(logicalPosition);
                output.writeInt(positionFrame.length);
                output.write(positionFrame);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private RestoredPosition decode(
            Path restored, StreamCheckpointMember member,
            boolean reassigned) {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(restored)))) {
            int magic = input.readInt();
            int version = input.readInt();
            String storedArtifactId = readText(input);
            String storedOperatorUid = readText(input);
            int storedSubtaskIndex = input.readInt();
            String storedProviderKey = readText(input);
            String storedProviderContract = readText(input);
            String storedConfiguration = readText(input);
            String storedSplitId = readText(input);
            String storedPositionCodec = readText(input);
            String storedSplitDescriptorDigest = readText(input);
            long storedLogicalPosition = input.readLong();
            int frameLength = input.readInt();
            if (magic != MAGIC || version != VERSION
                    || !artifactId.equals(storedArtifactId)
                    || !sourceOperatorUid.equals(storedOperatorUid)
                    || (reassigned ? member.subtaskIndex() : subtaskIndex)
                    != storedSubtaskIndex
                    || !binding.providerKey().equals(storedProviderKey)
                    || !binding.providerContractDigest().equals(
                            storedProviderContract)
                    || !binding.configurationDigest().equals(
                            storedConfiguration)
                    || !split.splitId().equals(storedSplitId)
                    || !split.positionCodecId().equals(storedPositionCodec)
                    || !digest(split.descriptorFrame()).equals(
                            storedSplitDescriptorDigest)
                    || storedLogicalPosition != member.logicalPosition()
                    || frameLength < 1
                    || frameLength > MAX_POSITION_FRAME_BYTES) {
                throw failure(
                        StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                        "source position payload identity is invalid");
            }
            byte[] frame = new byte[frameLength];
            input.readFully(frame);
            if (input.read() != -1) {
                throw failure(
                        StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                        "source position payload has trailing bytes");
            }
            return new RestoredPosition(storedLogicalPosition, frame);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException readFailure) {
            throw failure(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                    "source position payload is truncated or unreadable",
                    readFailure);
        }
    }

    private void validateMember(StreamCheckpointMember member,
            boolean reassigned) {
        if (member == null
                || member.kind()
                != StreamCheckpointMember.Kind.SOURCE_POSITION
                || !sourceOperatorUid.equals(member.operatorUid())
                || !reassigned && subtaskIndex != member.subtaskIndex()
                || !split.splitId().equals(member.subjectId())
                || !split.positionCodecId().equals(member.codecId())) {
            throw failure(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                    "source position member identity does not match the task");
        }
    }

    private Path temporary(String action) {
        return workingDirectory.resolve(
                ".source-position-" + action + '-' + UUID.randomUUID() + ".tmp");
    }

    private void checkOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamCheckpointException.Code.THREAD_OWNERSHIP,
                    "source position may only be used by its mailbox thread");
        }
    }

    private static byte[] requireFrame(byte[] frame) {
        if (frame == null || frame.length < 1
                || frame.length > MAX_POSITION_FRAME_BYTES) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "source position frame exceeds its bound");
        }
        return frame.clone();
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(
                java.nio.charset.StandardCharsets.UTF_8);
        if (encoded.length < 1 || encoded.length > MAX_TEXT_BYTES) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "source position metadata exceeds its bound");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_TEXT_BYTES) {
            throw failure(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                    "source position metadata length is invalid");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        return new String(encoded, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "source position staging path is not a directory");
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException creationFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "source position staging directory creation failed",
                    creationFailure);
        }
    }

    private static void cleanup(Path temporary, Throwable primary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "source position staging cleanup failed", cleanupFailure);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isEmpty() && value.length() <= 256;
    }

    private static String digest(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder hex = new StringBuilder(64);
            for (byte current : digest) {
                hex.append(Character.forDigit((current >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(current & 0xf, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 is required by the JVM", impossible);
        }
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }

    private static final class RestoredPosition {
        private final long logicalPosition;
        private final byte[] positionFrame;

        private RestoredPosition(long logicalPosition, byte[] positionFrame) {
            this.logicalPosition = logicalPosition;
            this.positionFrame = positionFrame;
        }
    }
}
