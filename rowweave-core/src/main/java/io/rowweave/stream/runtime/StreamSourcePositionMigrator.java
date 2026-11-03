package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** Rebinds an exact source position payload to a rescaled target Artifact. */
public final class StreamSourcePositionMigrator {
    private static final int MAGIC = 0x52575053;
    private static final int VERSION = 1;
    private static final int MAX_TEXT_BYTES = 1024;
    private static final int MAX_POSITION_FRAME_BYTES = 4 * 1024 * 1024;

    private StreamSourcePositionMigrator() { }

    public static StreamCheckpointMember migrate(
            String sourceArtifactId, String targetArtifactId,
            String sourceOperatorUid, int targetSubtask,
            StreamSourceBinding targetBinding, StreamSourceSplit targetSplit,
            StreamCheckpointMember sourceMember,
            FileCheckpointPayloadStore payloadStore, Path workingRoot) {
        if (sourceArtifactId == null || targetArtifactId == null
                || sourceOperatorUid == null || targetSubtask < 0
                || targetBinding == null || targetSplit == null
                || sourceMember == null || payloadStore == null
                || workingRoot == null
                || !targetBinding.fixedSplits().contains(targetSplit)) {
            throw incompatible("source position migration is incomplete");
        }
        if (sourceMember.kind()
                != StreamCheckpointMember.Kind.SOURCE_POSITION
                || !sourceOperatorUid.equals(sourceMember.operatorUid())
                || !targetSplit.splitId().equals(sourceMember.subjectId())
                || !targetSplit.positionCodecId().equals(
                        sourceMember.codecId())) {
            throw incompatible("source position member contract differs");
        }
        ensureDirectory(workingRoot);
        Path source = temporary(workingRoot, "source");
        Path target = temporary(workingRoot, "target");
        Throwable primary = null;
        try {
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    sourceMember.contentDigest(),
                    sourceMember.contentBytes()), source);
            Position position = read(source, sourceArtifactId,
                    sourceOperatorUid, targetBinding, targetSplit,
                    sourceMember);
            write(target, targetArtifactId, sourceOperatorUid,
                    targetSubtask, targetBinding, targetSplit, position);
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(target);
            return StreamCheckpointMember.sourcePosition(
                    sourceOperatorUid, targetSubtask, targetSplit.splitId(),
                    targetSplit.positionCodecId(), position.logicalPosition,
                    stored.digest(), stored.contentBytes());
        } catch (StreamCheckpointException failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            StreamCheckpointException wrapped = new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "source position migration I/O failed", failure);
            primary = wrapped;
            throw wrapped;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            cleanup(source, primary);
            cleanup(target, primary);
        }
    }

    private static Position read(Path path, String sourceArtifactId,
            String operatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, StreamCheckpointMember member)
            throws IOException {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            int magic = input.readInt();
            int version = input.readInt();
            String artifact = readText(input);
            String storedOperator = readText(input);
            int storedSubtask = input.readInt();
            String provider = readText(input);
            String providerContract = readText(input);
            String configuration = readText(input);
            String splitId = readText(input);
            String positionCodec = readText(input);
            String descriptorDigest = readText(input);
            long logicalPosition = input.readLong();
            int length = input.readInt();
            if (magic != MAGIC || version != VERSION
                    || !sourceArtifactId.equals(artifact)
                    || !operatorUid.equals(storedOperator)
                    || storedSubtask != member.subtaskIndex()
                    || !binding.providerKey().equals(provider)
                    || !binding.providerContractDigest().equals(
                            providerContract)
                    || !binding.configurationDigest().equals(configuration)
                    || !split.splitId().equals(splitId)
                    || !split.positionCodecId().equals(positionCodec)
                    || !digest(split.descriptorFrame()).equals(
                            descriptorDigest)
                    || logicalPosition != member.logicalPosition()
                    || length < 1 || length > MAX_POSITION_FRAME_BYTES) {
                throw incompatible("source position payload contract differs");
            }
            byte[] frame = new byte[length];
            input.readFully(frame);
            if (input.read() != -1) {
                throw incompatible("source position payload has trailing bytes");
            }
            return new Position(logicalPosition, frame);
        } catch (EOFException malformed) {
            throw incompatible("source position payload is truncated");
        }
    }

    private static void write(Path path, String targetArtifactId,
            String operatorUid, int subtask, StreamSourceBinding binding,
            StreamSourceSplit split, Position position) throws IOException {
        try (DataOutputStream output = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(path,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE)))) {
            output.writeInt(MAGIC);
            output.writeInt(VERSION);
            writeText(output, targetArtifactId);
            writeText(output, operatorUid);
            output.writeInt(subtask);
            writeText(output, binding.providerKey());
            writeText(output, binding.providerContractDigest());
            writeText(output, binding.configurationDigest());
            writeText(output, split.splitId());
            writeText(output, split.positionCodecId());
            writeText(output, digest(split.descriptorFrame()));
            output.writeLong(position.logicalPosition);
            output.writeInt(position.frame.length);
            output.write(position.frame);
        }
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_TEXT_BYTES) {
            throw incompatible("source position text exceeds its bound");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(
                java.nio.charset.StandardCharsets.UTF_8);
        if (encoded.length < 1 || encoded.length > MAX_TEXT_BYTES) {
            throw incompatible("source position text exceeds its bound");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            char[] digits = "0123456789abcdef".toCharArray();
            char[] result = new char[bytes.length * 2];
            for (int index = 0; index < bytes.length; index++) {
                int current = bytes[index] & 0xff;
                result[index * 2] = digits[current >>> 4];
                result[index * 2 + 1] = digits[current & 0xf];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static Path temporary(Path root, String kind) {
        return root.resolve(".source-position-rescale-" + kind + '-'
                + UUID.randomUUID() + ".tmp");
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
        } catch (IOException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "source position migration directory failed", failure);
        }
    }

    private static void cleanup(Path path, Throwable primary) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException failure) {
            if (primary != null) primary.addSuppressed(failure);
            else throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "source position migration cleanup failed", failure);
        }
    }

    private static StreamCheckpointException incompatible(String message) {
        return new StreamCheckpointException(
                StreamCheckpointException.Code.RESCALE_INCOMPATIBLE, message);
    }

    private static final class Position {
        final long logicalPosition;
        final byte[] frame;
        private Position(long logicalPosition, byte[] frame) {
            this.logicalPosition = logicalPosition;
            this.frame = frame;
        }
    }
}
