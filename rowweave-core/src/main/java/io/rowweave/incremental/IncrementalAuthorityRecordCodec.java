package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical checksum-protected wire for the bounded frontier authority projection. */
public final class IncrementalAuthorityRecordCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;

    private static final byte[] MAGIC = {'A', 'X', 'A', 'R'};
    private static final int CODEC_VERSION = 1;
    private static final int HEADER_BYTES = 4 + 1 + 4;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_PAYLOAD_BYTES = MAX_FRAME_BYTES - HEADER_BYTES - CHECKSUM_BYTES;

    private IncrementalAuthorityRecordCodec() {}

    public static byte[] encode(IncrementalAuthorityRecord record) {
        if (record == null) throw new IllegalArgumentException("authority record is required");
        try {
            byte[] payload = encodePayload(record);
            if (payload.length > MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("authority record exceeds 64 KiB");
            }
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(HEADER_BYTES + payload.length);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.write(MAGIC);
            body.writeByte(CODEC_VERSION);
            body.writeInt(payload.length);
            body.write(payload);
            body.flush();
            byte[] canonicalBody = bodyBytes.toByteArray();
            ByteArrayOutputStream frame = new ByteArrayOutputStream(canonicalBody.length + CHECKSUM_BYTES);
            frame.write(canonicalBody);
            frame.write(IncrementalAuthorityValue.sha256(canonicalBody));
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory authority record encoding failed", impossible);
        }
    }

    public static IncrementalAuthorityRecord decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("authority record frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("authority record magic is invalid");
            }
            if (input.readUnsignedByte() != CODEC_VERSION) {
                throw new IllegalArgumentException("unknown authority record codec version");
            }
            int payloadLength = input.readInt();
            if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                    || payloadLength != frame.length - HEADER_BYTES - CHECKSUM_BYTES) {
                throw new IllegalArgumentException("authority record payload length is invalid");
            }
            int bodyLength = HEADER_BYTES + payloadLength;
            byte[] expected = Arrays.copyOfRange(frame, bodyLength, frame.length);
            byte[] actual = IncrementalAuthorityValue.sha256(Arrays.copyOf(frame, bodyLength));
            if (!MessageDigest.isEqual(expected, actual)) {
                throw new IllegalArgumentException("authority record checksum mismatch");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            input.skipBytes(CHECKSUM_BYTES);
            if (input.read() != -1) throw new IllegalArgumentException("authority record trailing bytes");
            IncrementalAuthorityRecord decoded = decodePayload(payload);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("authority record is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("authority record frame is malformed", malformed);
        }
    }

    private static byte[] encodePayload(IncrementalAuthorityRecord record) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(record.schemaVersion());
        out.writeLong(record.revision());
        IncrementalAuthorityCodecSupport.writeText(out, record.feedId());
        out.writeLong(record.feedIncarnation());
        out.write(record.definitionDigest());
        out.write(record.sourceContractDigest());
        out.write(record.frontierContractDigest());
        out.writeByte(record.frontierKind().wireId());
        writeFrontier(out, record.committedFrontier());
        writeActive(out, record.activeInterval());
        writeReceipt(out, record.lastCommitReceipt());
        out.writeLong(record.lastCompletedBackfillOrdinal());
        writeReceipt(out, record.lastBackfillReceipt());
        out.writeByte(record.state().wireId());
        writeNullableDigest(out, record.conflictDigest());
        out.flush();
        return bytes.toByteArray();
    }

    private static IncrementalAuthorityRecord decodePayload(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        if (in.readUnsignedByte() != IncrementalAuthorityRecord.SCHEMA_VERSION) {
            throw new IllegalArgumentException("unknown authority record schema version");
        }
        long revision = in.readLong();
        String feedId = IncrementalAuthorityCodecSupport.readText(in, "feed id");
        long incarnation = in.readLong();
        byte[] definition = IncrementalAuthorityCodecSupport.readDigest(in, "definition digest");
        byte[] sourceContract = IncrementalAuthorityCodecSupport.readDigest(
                in, "source contract digest");
        byte[] contract = IncrementalAuthorityCodecSupport.readDigest(in, "frontier contract digest");
        IncrementalFrontier.Kind kind = IncrementalFrontier.Kind.fromWire(in.readUnsignedByte());
        IncrementalFrontier committed = readFrontier(in, "committed frontier");
        IncrementalActiveInterval active = readActive(in);
        IncrementalCommitReceipt normal = readReceipt(in);
        long backfillOrdinal = in.readLong();
        IncrementalCommitReceipt backfill = readReceipt(in);
        IncrementalFrontierAuthorityState state =
                IncrementalFrontierAuthorityState.fromWire(in.readUnsignedByte());
        byte[] conflict = readNullableDigest(in, "conflict digest");
        if (in.read() != -1) throw new IllegalArgumentException("authority payload trailing bytes");
        return new IncrementalAuthorityRecord(
                revision, feedId, incarnation, definition, sourceContract, contract,
                kind, committed,
                active, normal, backfillOrdinal, backfill, state, conflict);
    }

    private static void writeActive(DataOutputStream out, IncrementalActiveInterval active)
            throws IOException {
        out.writeBoolean(active != null);
        if (active == null) return;
        IncrementalAuthorityCodecSupport.writeBytes(
                out, CapturedTargetReceiptCodec.encode(active.capturedTarget()));
        IncrementalAuthorityCodecSupport.writeText(out, active.artifactId());
        out.writeLong(active.claimedRevision());
        writeFence(out, active.fence());
        out.writeByte(active.attemptOrdinal());
        out.writeByte(active.phase().wireId());
        writeNullableDigest(out, active.commitDecisionDigest());
    }

    private static IncrementalActiveInterval readActive(DataInputStream in) throws IOException {
        if (!in.readBoolean()) return null;
        byte[] captureFrame = IncrementalAuthorityCodecSupport.readBytes(
                in, CapturedTargetReceiptCodec.MAX_FRAME_BYTES, "captured target");
        CapturedTargetReceipt capture = CapturedTargetReceiptCodec.decode(captureFrame);
        String artifactId = IncrementalAuthorityCodecSupport.readText(in, "artifact id");
        long claimedRevision = in.readLong();
        RunAttemptFence fence = readFence(in);
        int attemptOrdinal = in.readUnsignedByte();
        IncrementalAttemptCommitPhase phase =
                IncrementalAttemptCommitPhase.fromWire(in.readUnsignedByte());
        byte[] decision = readNullableDigest(in, "commit decision digest");
        return new IncrementalActiveInterval(
                IncrementalIntervalIdentity.from(capture), capture, artifactId, claimedRevision,
                fence, attemptOrdinal, phase, decision);
    }

    private static void writeReceipt(DataOutputStream out, IncrementalCommitReceipt receipt)
            throws IOException {
        out.writeBoolean(receipt != null);
        if (receipt == null) return;
        IncrementalAuthorityCodecSupport.writeText(out, receipt.intervalId());
        IncrementalAuthorityCodecSupport.writeText(out, receipt.capturedTargetReceiptDigest());
        out.writeByte(receipt.mode().wireId());
        out.writeLong(receipt.expectedSequence());
        writeFrontier(out, receipt.from());
        writeFrontier(out, receipt.to());
        out.write(receipt.definitionFingerprint());
        IncrementalAuthorityCodecSupport.writeText(out, receipt.artifactId());
        out.writeLong(receipt.claimedRevision());
        writeFence(out, receipt.fence());
        IncrementalAuthorityCodecSupport.writeText(out, receipt.publicationId());
        out.writeLong(receipt.generation());
        out.write(receipt.manifestHash());
        out.write(receipt.commitDecisionDigest());
        IncrementalAuthorityCodecSupport.writeText(out, receipt.commandId());
        out.write(receipt.commandDigest());
    }

    private static IncrementalCommitReceipt readReceipt(DataInputStream in) throws IOException {
        if (!in.readBoolean()) return null;
        String intervalId = IncrementalAuthorityCodecSupport.readText(in, "receipt interval id");
        String capturedDigest = IncrementalAuthorityCodecSupport.readText(
                in, "receipt captured target digest");
        IncrementalRunMode mode = IncrementalRunMode.fromWire(in.readUnsignedByte());
        long expectedSequence = in.readLong();
        IncrementalFrontier from = readFrontier(in, "receipt from");
        IncrementalFrontier to = readFrontier(in, "receipt to");
        byte[] definition = IncrementalAuthorityCodecSupport.readDigest(in, "receipt definition");
        String artifactId = IncrementalAuthorityCodecSupport.readText(in, "receipt artifact id");
        long claimedRevision = in.readLong();
        RunAttemptFence fence = readFence(in);
        String publicationId = IncrementalAuthorityCodecSupport.readText(in, "receipt publication id");
        long generation = in.readLong();
        byte[] manifest = IncrementalAuthorityCodecSupport.readDigest(in, "receipt manifest hash");
        byte[] decision = IncrementalAuthorityCodecSupport.readDigest(in, "receipt decision digest");
        String commandId = IncrementalAuthorityCodecSupport.readText(in, "receipt command id");
        byte[] command = IncrementalAuthorityCodecSupport.readDigest(in, "receipt command digest");
        return IncrementalCommitReceipt.decoded(
                intervalId, capturedDigest, mode, expectedSequence, from, to,
                definition, artifactId, claimedRevision, fence, publicationId, generation,
                manifest, decision, commandId, command);
    }

    private static void writeFence(DataOutputStream out, RunAttemptFence fence) throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, fence.runId());
        IncrementalAuthorityCodecSupport.writeText(out, fence.attemptId());
        out.writeLong(fence.fenceToken());
    }

    private static RunAttemptFence readFence(DataInputStream in) throws IOException {
        return new RunAttemptFence(
                IncrementalAuthorityCodecSupport.readText(in, "fence run id"),
                IncrementalAuthorityCodecSupport.readText(in, "fence attempt id"),
                in.readLong());
    }

    private static void writeFrontier(DataOutputStream out, IncrementalFrontier frontier)
            throws IOException {
        IncrementalAuthorityCodecSupport.writeBytes(out, IncrementalFrontierCodec.encode(frontier));
    }

    private static IncrementalFrontier readFrontier(DataInputStream in, String field)
            throws IOException {
        return IncrementalFrontierCodec.decode(IncrementalAuthorityCodecSupport.readBytes(
                in, IncrementalFrontierCodec.MAX_FRAME_BYTES, field));
    }

    private static void writeNullableDigest(DataOutputStream out, byte[] digest) throws IOException {
        out.writeBoolean(digest != null);
        if (digest != null) out.write(digest);
    }

    private static byte[] readNullableDigest(DataInputStream in, String field) throws IOException {
        return in.readBoolean() ? IncrementalAuthorityCodecSupport.readDigest(in, field) : null;
    }
}
