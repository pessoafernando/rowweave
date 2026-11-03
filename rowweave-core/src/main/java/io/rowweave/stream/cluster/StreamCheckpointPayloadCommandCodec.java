package io.rowweave.stream.cluster;

import io.rowweave.stream.runtime.FileCheckpointPayloadStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Bounded metadata/chunk command nested in the authenticated Worker RPC. */
final class StreamCheckpointPayloadCommandCodec {
    static final int MAX_FRAME_BYTES =
            FileCheckpointPayloadStore.TRANSFER_CHUNK_BYTES + 128;

    static final class Command {
        private final String digest;
        private final long contentBytes;
        private final long offset;
        private final byte[] chunk;

        private Command(String digest, long contentBytes,
                long offset, byte[] chunk) {
            if (digest == null || !digest.matches("[0-9a-f]{64}")
                    || contentBytes < 1L || offset < 0L
                    || offset >= contentBytes || chunk == null
                    || chunk.length > FileCheckpointPayloadStore
                            .TRANSFER_CHUNK_BYTES
                    || offset > contentBytes - chunk.length) {
                throw new IllegalArgumentException(
                        "checkpoint payload command is invalid");
            }
            this.digest = digest;
            this.contentBytes = contentBytes;
            this.offset = offset;
            this.chunk = chunk.clone();
        }

        String digest() { return digest; }
        long contentBytes() { return contentBytes; }
        long offset() { return offset; }
        byte[] chunk() { return chunk.clone(); }
    }

    private StreamCheckpointPayloadCommandCodec() { }

    static byte[] fetch(String digest, long contentBytes, long offset) {
        return encode(new Command(digest, contentBytes, offset, new byte[0]));
    }

    static byte[] store(String digest, long contentBytes, long offset,
            byte[] chunk) {
        if (chunk == null || chunk.length == 0) {
            throw new IllegalArgumentException(
                    "checkpoint payload store chunk is required");
        }
        return encode(new Command(digest, contentBytes, offset, chunk));
    }

    static Command decode(byte[] frame) {
        if (frame == null || frame.length < 84
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "checkpoint payload command exceeds its bound");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            int digestLength = input.readInt();
            if (digestLength != 64) {
                throw new IllegalArgumentException(
                        "checkpoint payload digest is invalid");
            }
            byte[] digest = new byte[digestLength];
            input.readFully(digest);
            long contentBytes = input.readLong();
            long offset = input.readLong();
            int length = input.readInt();
            if (length < 0
                    || length > FileCheckpointPayloadStore
                            .TRANSFER_CHUNK_BYTES
                    || length > input.available()) {
                throw new IllegalArgumentException(
                        "checkpoint payload chunk exceeds its bound");
            }
            byte[] chunk = new byte[length];
            input.readFully(chunk);
            if (input.available() != 0) {
                throw new IllegalArgumentException(
                        "checkpoint payload command has trailing bytes");
            }
            return new Command(new String(digest, StandardCharsets.US_ASCII),
                    contentBytes, offset, chunk);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "checkpoint payload command is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "checkpoint payload command is malformed", malformed);
        }
    }

    private static byte[] encode(Command command) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                byte[] digest = command.digest.getBytes(
                        StandardCharsets.US_ASCII);
                output.writeInt(digest.length);
                output.write(digest);
                output.writeLong(command.contentBytes);
                output.writeLong(command.offset);
                output.writeInt(command.chunk.length);
                output.write(command.chunk);
            }
            byte[] frame = bytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "checkpoint payload command exceeds its bound");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
