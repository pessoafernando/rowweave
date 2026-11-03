package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical bounded command frame used for duplicate/conflict identity. */
public final class IncrementalAuthorityCommandCodec {
    public static final int MAX_FRAME_BYTES = 2048;

    private static final byte[] MAGIC = {'A', 'X', 'A', 'C'};
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;

    private IncrementalAuthorityCommandCodec() {}

    public static byte[] encode(IncrementalAuthorityCommand command) {
        if (command == null) throw new IllegalArgumentException("authority command is required");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bodyBytes);
            out.write(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(command.kind().wireId());
            IncrementalAuthorityCodecSupport.writeText(out, command.feedId());
            IncrementalAuthorityCodecSupport.writeText(out, command.commandId());
            out.writeLong(command.expectedRevision());
            out.writeLong(command.feedIncarnation());
            out.writeBoolean(command.intervalId() != null);
            if (command.intervalId() != null) {
                IncrementalAuthorityCodecSupport.writeText(out, command.intervalId());
                IncrementalAuthorityCodecSupport.writeText(out, command.fence().runId());
                IncrementalAuthorityCodecSupport.writeText(out, command.fence().attemptId());
                out.writeLong(command.fence().fenceToken());
            }
            out.write(command.transitionDigest());
            out.flush();
            byte[] body = bodyBytes.toByteArray();
            if (body.length + CHECKSUM_BYTES > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("authority command exceeds frame limit");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream(body.length + CHECKSUM_BYTES);
            frame.write(body);
            frame.write(IncrementalAuthorityValue.sha256(body));
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory authority command encoding failed", impossible);
        }
    }

    public static IncrementalAuthorityCommand decode(byte[] frame) {
        if (frame == null || frame.length < 4 + 1 + 1 + CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("authority command frame length is invalid");
        }
        int bodyLength = frame.length - CHECKSUM_BYTES;
        byte[] expected = Arrays.copyOfRange(frame, bodyLength, frame.length);
        byte[] body = Arrays.copyOf(frame, bodyLength);
        if (!MessageDigest.isEqual(expected, IncrementalAuthorityValue.sha256(body))) {
            throw new IllegalArgumentException("authority command checksum mismatch");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            byte[] magic = new byte[MAGIC.length];
            in.readFully(magic);
            if (!Arrays.equals(magic, MAGIC) || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("authority command header is invalid");
            }
            IncrementalAuthorityCommandKind kind =
                    IncrementalAuthorityCommandKind.fromWire(in.readUnsignedByte());
            String feedId = IncrementalAuthorityCodecSupport.readText(in, "command feed id");
            String commandId = IncrementalAuthorityCodecSupport.readText(in, "command id");
            long expectedRevision = in.readLong();
            long incarnation = in.readLong();
            String intervalId = null;
            RunAttemptFence fence = null;
            if (in.readBoolean()) {
                intervalId = IncrementalAuthorityCodecSupport.readText(in, "command interval id");
                fence = new RunAttemptFence(
                        IncrementalAuthorityCodecSupport.readText(in, "command run id"),
                        IncrementalAuthorityCodecSupport.readText(in, "command attempt id"),
                        in.readLong());
            }
            byte[] transition = IncrementalAuthorityCodecSupport.readDigest(
                    in, "command transition digest");
            if (in.read() != -1) throw new IllegalArgumentException("authority command trailing bytes");
            IncrementalAuthorityCommand decoded = new IncrementalAuthorityCommand(
                    kind, feedId, commandId, expectedRevision, incarnation,
                    intervalId, fence, transition);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("authority command is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("authority command is malformed", malformed);
        }
    }

    public static byte[] digest(IncrementalAuthorityCommand command) {
        return IncrementalAuthorityValue.sha256(encode(command));
    }
}
