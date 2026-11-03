package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical bounded wire for a pending incremental Run binding. */
public final class IncrementalClaimRequestCodec {
    public static final int MAX_FRAME_BYTES = 96 * 1024;
    private static final int MAGIC = 0x52574951; // RWIQ
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;

    private IncrementalClaimRequestCodec() {}

    public static byte[] encode(IncrementalClaimRequest request) {
        if (request == null) throw new IllegalArgumentException("claim request is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            IncrementalAuthorityCodecSupport.writeText(out, request.feedId());
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, CapturedTargetReceiptCodec.encode(request.capturedTarget()));
            out.write(request.frontierContractDigest());
            IncrementalAuthorityCodecSupport.writeText(out, request.artifactId());
            IncrementalAuthorityCodecSupport.writeText(out, request.attemptId());
            IncrementalAuthorityCodecSupport.writeText(out, request.publicationId());
            out.writeInt(request.maxAttempts());
            out.writeLong(request.deadlineEpochMillis());
            out.writeLong(request.eventTimeMillis());
            out.flush();
            byte[] body = bytes.toByteArray();
            if (body.length + CHECKSUM_BYTES > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("claim request exceeds frame limit");
            }
            byte[] frame = Arrays.copyOf(body, body.length + CHECKSUM_BYTES);
            System.arraycopy(IncrementalAuthorityValue.sha256(body), 0,
                    frame, body.length, CHECKSUM_BYTES);
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException("encode claim request failed", impossible);
        }
    }

    public static IncrementalClaimRequest decode(byte[] frame) {
        if (frame == null || frame.length <= CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("claim request frame length is invalid");
        }
        byte[] body = Arrays.copyOf(frame, frame.length - CHECKSUM_BYTES);
        byte[] checksum = Arrays.copyOfRange(frame, body.length, frame.length);
        if (!MessageDigest.isEqual(
                IncrementalAuthorityValue.sha256(body), checksum)) {
            throw new IllegalArgumentException("claim request checksum mismatch");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("claim request header is invalid");
            }
            String feedId = IncrementalAuthorityCodecSupport.readText(in, "claim feed id");
            CapturedTargetReceipt capture = CapturedTargetReceiptCodec.decode(
                    IncrementalAuthorityCodecSupport.readBytes(
                            in, CapturedTargetReceiptCodec.MAX_FRAME_BYTES,
                            "claim captured target"));
            byte[] frontierContract = IncrementalAuthorityCodecSupport.readDigest(
                    in, "claim frontier contract");
            String artifactId = IncrementalAuthorityCodecSupport.readText(
                    in, "claim artifact id");
            String attemptId = IncrementalAuthorityCodecSupport.readText(
                    in, "claim attempt id");
            String publicationId = IncrementalAuthorityCodecSupport.readText(
                    in, "claim publication id");
            int maxAttempts = in.readInt();
            long deadline = in.readLong();
            long eventTime = in.readLong();
            if (in.read() != -1) {
                throw new IllegalArgumentException("claim request trailing bytes");
            }
            IncrementalClaimRequest decoded = new IncrementalClaimRequest(
                    feedId, capture, frontierContract, artifactId,
                    attemptId, publicationId, maxAttempts, deadline, eventTime);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("claim request is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("claim request is malformed", malformed);
        }
    }
}
