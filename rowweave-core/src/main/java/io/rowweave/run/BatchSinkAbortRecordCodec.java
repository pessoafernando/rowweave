package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded codec for durable Sink abort authority records. */
public final class BatchSinkAbortRecordCodec {

    public static final int MAX_FRAME_BYTES = 8 * 1024;
    private static final int MAGIC = 0x41425331; // ABS1
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 10;
    private static final int CHECKSUM_BYTES = 32;

    private BatchSinkAbortRecordCodec() { }

    public static byte[] encode(BatchSinkAbortRecord record) {
        if (record == null) throw new IllegalArgumentException("abort record is required");
        ByteArrayOutputStream payload = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(payload)) {
            out.writeByte(record.state() == BatchSinkAbortRecord.State.ABORT_DECIDED ? 1 : 2);
            RunWire.writeText(out, record.fence().runId());
            RunWire.writeText(out, record.fence().attemptId());
            out.writeLong(record.fence().fenceToken());
            out.writeLong(record.assignmentGeneration());
            out.writeByte(SinkAttemptIdentityCodec.profileId(record.capabilityProfile()));
            RunWire.writeText(out, record.commitDomainId());
            out.writeLong(record.generation());
            RunWire.writeText(out, record.publicationId());
            out.write(record.identityDigest());
            RunWire.writeText(out, record.reasonCode());
            out.writeLong(record.decidedAtMillis());
            out.writeLong(record.completedAtMillis());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] body = payload.toByteArray();
        if (HEADER_BYTES + body.length + CHECKSUM_BYTES > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("abort record frame exceeds limit");
        }
        ByteArrayOutputStream framed = new ByteArrayOutputStream(
                HEADER_BYTES + body.length + CHECKSUM_BYTES);
        try (DataOutputStream out = new DataOutputStream(framed)) {
            out.writeInt(MAGIC);
            out.writeShort(VERSION);
            out.writeInt(body.length);
            out.write(body);
            out.write(sha256(framed.toByteArray()));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return framed.toByteArray();
    }

    public static BatchSinkAbortRecord decode(byte[] encoded) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("abort record frame length out of bounds");
        }
        RunWire.Reader frame = new RunWire.Reader(encoded);
        int magic = frame.readInt("abort.magic");
        int version = (frame.readUnsignedByte("abort.version.high") << 8)
                | frame.readUnsignedByte("abort.version.low");
        int length = frame.readInt("abort.length");
        if (magic != MAGIC || version != VERSION || length < 0
                || encoded.length != HEADER_BYTES + length + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("invalid abort record frame header");
        }
        byte[] body = frame.readFixed(length, "abort.body");
        byte[] checksum = frame.readFixed(CHECKSUM_BYTES, "abort.checksum");
        frame.finish("abort record frame");
        if (!MessageDigest.isEqual(
                sha256(Arrays.copyOf(encoded, HEADER_BYTES + length)), checksum)) {
            throw new IllegalArgumentException("abort record checksum mismatch");
        }
        RunWire.Reader in = new RunWire.Reader(body);
        int stateId = in.readUnsignedByte("abort.state");
        BatchSinkAbortRecord.State state;
        if (stateId == 1) state = BatchSinkAbortRecord.State.ABORT_DECIDED;
        else if (stateId == 2) state = BatchSinkAbortRecord.State.ABORTED;
        else throw new IllegalArgumentException("unknown abort record state");
        RunAttemptFence fence = new RunAttemptFence(
                in.readText("abort.runId", RunText.MAX_ID_BYTES),
                in.readText("abort.attemptId", RunText.MAX_ID_BYTES),
                in.readLong("abort.fenceToken"));
        long assignmentGeneration = in.readLong("abort.assignmentGeneration");
        BatchSinkDescriptor.CapabilityProfile profile =
                SinkAttemptIdentityCodec.profile(in.readUnsignedByte("abort.profile"));
        String domain = in.readText(
                "abort.commitDomainId", BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES);
        long generation = in.readLong("abort.generation");
        String publication = in.readText(
                "abort.publicationId", ManifestMemberReceipt.MAX_PUBLICATION_ID_BYTES);
        byte[] identity = in.readFixed(BatchSinkAbortRecord.DIGEST_BYTES, "abort.identity");
        String reason = in.readText(
                "abort.reasonCode", BatchSinkAbortRecord.MAX_REASON_CODE_CHARS);
        long decidedAt = in.readLong("abort.decidedAtMillis");
        long completedAt = in.readLong("abort.completedAtMillis");
        in.finish("abort record body");
        return BatchSinkAbortRecord.decoded(
                state, fence, assignmentGeneration, profile, domain,
                generation, publication, identity, reason, decidedAt, completedAt);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
