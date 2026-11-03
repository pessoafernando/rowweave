package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Canonical wire for one typed source/state/Sink checkpoint member. */
public final class StreamCheckpointMemberCodec {
    private static final byte[] MAGIC =
            "RWXXSCB1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V2 =
            "RWXXSCB2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V3 =
            "RWXXSCB3".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_BODY_BYTES = 4096;

    private StreamCheckpointMemberCodec() { }

    public static byte[] magic() { return MAGIC.clone(); }

    public static byte[] encode(StreamCheckpointMember member) {
        if (member == null) throw new IllegalArgumentException("member is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(member.kind().wireId());
                CheckpointManifestCodec.writeText(output, member.operatorUid());
                output.writeInt(member.subtaskIndex());
                CheckpointManifestCodec.writeText(output, member.subjectId());
                writeNullableText(output, member.codecId());
                output.writeLong(member.logicalPosition());
                output.writeInt(member.keyGroupStart());
                output.writeInt(member.keyGroupEnd());
                output.writeInt(member.schemaVersion());
                writeNullableText(output, member.contractDigest());
                CheckpointManifestCodec.writeText(output, member.contentDigest());
                output.writeLong(member.contentBytes());
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw StreamContractSupport.failure("LIMIT_EXCEEDED",
                        "checkpoint.member.body", "member body exceeds its limit");
            }
            byte[] magic = member.kind()
                    == StreamCheckpointMember.Kind.CHANNEL_STATE
                    ? MAGIC_V3 : member.kind()
                    == StreamCheckpointMember.Kind.SOURCE_COORDINATOR
                    ? MAGIC_V2 : MAGIC;
            return CheckpointManifestCodec.frame(magic, body);
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory member encode failed", impossible);
        }
    }

    public static StreamCheckpointMember decode(byte[] frame) {
        byte[] body = CheckpointManifestCodec.bodyFromFrame(
                frame, v3(frame) ? MAGIC_V3
                        : v2(frame) ? MAGIC_V2 : MAGIC, MAX_BODY_BYTES);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            StreamCheckpointMember.Kind kind =
                    StreamCheckpointMember.Kind.fromWire(input.readInt());
            String operatorUid = CheckpointManifestCodec.readText(
                    input, 256, "checkpoint.member.operatorUid");
            int subtaskIndex = input.readInt();
            String subjectId = CheckpointManifestCodec.readText(
                    input, 512, "checkpoint.member.subjectId");
            String codecId = readNullableText(
                    input, 256, "checkpoint.member.codecId");
            long logicalPosition = input.readLong();
            int keyGroupStart = input.readInt();
            int keyGroupEnd = input.readInt();
            int schemaVersion = input.readInt();
            String contractDigest = readNullableText(
                    input, 64, "checkpoint.member.contractDigest");
            String contentDigest = CheckpointManifestCodec.readText(
                    input, 64, "checkpoint.member.contentDigest");
            long contentBytes = input.readLong();
            if (input.available() != 0) {
                throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                        "checkpoint.member.body", "member body has trailing bytes");
            }
            requireCanonicalShape(kind, subjectId, codecId, logicalPosition,
                    keyGroupStart, keyGroupEnd, schemaVersion,
                    contractDigest, contentBytes);
            switch (kind) {
                case SOURCE_POSITION:
                    return StreamCheckpointMember.sourcePosition(
                            operatorUid, subtaskIndex, subjectId, codecId, logicalPosition,
                            contentDigest, contentBytes);
                case STATE:
                    return StreamCheckpointMember.stateHandle(
                            operatorUid, subtaskIndex, keyGroupStart, keyGroupEnd,
                            subjectId, codecId, schemaVersion,
                            contentDigest, contentBytes);
                case TRANSACTIONAL_SINK:
                    return StreamCheckpointMember.transactionalSink(
                            operatorUid, subtaskIndex, subjectId,
                            contractDigest, contentDigest, contentBytes);
                case EAGER_SINK_CAPABILITY:
                    return StreamCheckpointMember.eagerSinkCapability(
                            operatorUid, subtaskIndex,
                            contractDigest, contentDigest);
                case SOURCE_COORDINATOR:
                    return StreamCheckpointMember.sourceCoordinator(
                            operatorUid, codecId, contentDigest, contentBytes);
                case CHANNEL_STATE:
                    return StreamCheckpointMember.channelState(
                            operatorUid, subtaskIndex, subjectId,
                            logicalPosition, contentDigest, contentBytes);
                default:
                    throw new IllegalStateException(
                            "unhandled checkpoint member kind");
            }
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "checkpoint.member.body", "member body is truncated");
        } catch (IOException malformed) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "checkpoint.member.body", "member body is malformed");
        }
    }

    private static boolean v2(byte[] frame) {
        if (frame == null || frame.length < MAGIC.length) return false;
        for (int index = 0; index < MAGIC_V2.length; index++) {
            if (frame[index] != MAGIC_V2[index]) return false;
        }
        return true;
    }

    private static boolean v3(byte[] frame) {
        if (frame == null || frame.length < MAGIC_V3.length) return false;
        for (int index = 0; index < MAGIC_V3.length; index++) {
            if (frame[index] != MAGIC_V3[index]) return false;
        }
        return true;
    }

    private static void requireCanonicalShape(
            StreamCheckpointMember.Kind kind, String subjectId, String codecId,
            long logicalPosition, int keyGroupStart, int keyGroupEnd,
            int schemaVersion, String contractDigest, long contentBytes) {
        boolean valid;
        switch (kind) {
            case SOURCE_POSITION:
                valid = codecId != null && logicalPosition >= 0L
                        && keyGroupStart == -1 && keyGroupEnd == -1
                        && schemaVersion == 0 && contractDigest == null;
                break;
            case STATE:
                valid = codecId != null && logicalPosition == -1L
                        && keyGroupStart >= 0 && keyGroupEnd >= keyGroupStart
                        && schemaVersion > 0 && contractDigest == null;
                break;
            case TRANSACTIONAL_SINK:
                valid = codecId == null && logicalPosition == -1L
                        && keyGroupStart == -1 && keyGroupEnd == -1
                        && schemaVersion == 0 && contractDigest != null;
                break;
            case EAGER_SINK_CAPABILITY:
                valid = "eager-capability".equals(subjectId)
                        && codecId == null && logicalPosition == -1L
                        && keyGroupStart == -1 && keyGroupEnd == -1
                        && schemaVersion == 0 && contractDigest != null
                        && contentBytes == 0L;
                break;
            case SOURCE_COORDINATOR:
                valid = "source-coordinator".equals(subjectId)
                        && codecId != null && logicalPosition == -1L
                        && keyGroupStart == -1 && keyGroupEnd == -1
                        && schemaVersion == 2 && contractDigest == null;
                break;
            case CHANNEL_STATE:
                valid = StreamChannelStateCodec.CODEC_ID.equals(codecId)
                        && logicalPosition > 0L
                        && keyGroupStart == -1 && keyGroupEnd == -1
                        && schemaVersion == 1 && contractDigest == null;
                break;
            default:
                valid = false;
        }
        if (!valid) {
            throw StreamContractSupport.failure("NON_CANONICAL_MEMBER",
                    "checkpoint.member", "member fields do not match its kind");
        }
    }

    private static String readNullableText(
            DataInputStream input, int maxBytes, String path) throws IOException {
        return input.readBoolean()
                ? CheckpointManifestCodec.readText(input, maxBytes, path) : null;
    }


    private static void writeNullableText(DataOutputStream output, String value)
            throws IOException {
        output.writeBoolean(value != null);
        if (value != null) CheckpointManifestCodec.writeText(output, value);
    }
}
