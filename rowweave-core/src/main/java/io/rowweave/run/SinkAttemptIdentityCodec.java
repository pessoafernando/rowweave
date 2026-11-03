package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire for {@link SinkAttemptIdentity}. */
public final class SinkAttemptIdentityCodec {

    public static final int MAX_FRAME_BYTES = 4 * 1024;
    private static final int MAGIC = 0x52575349; // RWSI
    private static final int CODEC_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MIN_FRAME_BYTES = 180;

    private SinkAttemptIdentityCodec() {
    }

    public static byte[] encode(SinkAttemptIdentity identity) {
        if (identity == null) {
            throw new IllegalArgumentException("sink attempt identity is required");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(CODEC_VERSION);
            out.writeInt(identity.identityVersion());
            RunWire.writeText(out, identity.artifactId());
            RunWire.writeText(out, identity.fence().runId());
            RunWire.writeText(out, identity.fence().attemptId());
            out.writeLong(identity.fence().fenceToken());
            out.writeLong(identity.assignmentGeneration());
            RunWire.writeText(out, identity.stageExecutionId());
            out.writeInt(identity.logicalStage());
            out.writeInt(identity.instanceIndex());
            RunWire.writeText(out, identity.sinkOperatorId());
            out.writeByte(profileId(identity.capabilityProfile()));
            RunWire.writeText(out, identity.commitDomainId());
            out.write(identity.descriptorDigest());
            out.write(identity.identityDigest());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] body = bytes.toByteArray();
        if (body.length > MAX_FRAME_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("sink attempt identity frame length exceeds limit");
        }
        byte[] checksum = sha256(body, 0, body.length);
        byte[] frame = Arrays.copyOf(body, body.length + checksum.length);
        System.arraycopy(checksum, 0, frame, body.length, checksum.length);
        return frame;
    }

    public static SinkAttemptIdentity decode(byte[] frame) {
        if (frame == null || frame.length < MIN_FRAME_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("sink attempt identity frame length out of bounds");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        byte[] expected = sha256(frame, 0, bodyLength);
        if (!MessageDigest.isEqual(expected,
                Arrays.copyOfRange(frame, bodyLength, frame.length))) {
            throw new IllegalArgumentException(
                    "sink attempt identity frame length or checksum mismatch");
        }
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(frame, 0, bodyLength))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("sink attempt identity magic mismatch");
            }
            int codecVersion = in.readInt();
            if (codecVersion != CODEC_VERSION) {
                throw new IllegalArgumentException(
                        "sink attempt identity codec version is unsupported: " + codecVersion);
            }
            int identityVersion = in.readInt();
            String artifactId = readText(in, "artifactId", RunText.MAX_ID_BYTES);
            String runId = readText(in, "runId", RunText.MAX_ID_BYTES);
            String attemptId = readText(in, "attemptId", RunText.MAX_ID_BYTES);
            long fenceToken = in.readLong();
            long assignmentGeneration = in.readLong();
            String stageExecutionId = readText(in, "stageExecutionId", 64);
            int logicalStage = in.readInt();
            int instanceIndex = in.readInt();
            String operatorId = readText(
                    in, "sinkOperatorId", SinkAttemptIdentity.MAX_OPERATOR_ID_BYTES);
            BatchSinkDescriptor.CapabilityProfile profile =
                    profile(in.readUnsignedByte());
            String domain = readText(in, "commitDomainId",
                    BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES);
            byte[] descriptorDigest = readFixed(in, DIGEST_BYTES, "descriptorDigest");
            byte[] identityDigest = readFixed(in, DIGEST_BYTES, "identityDigest");
            if (in.available() != 0) {
                throw new IllegalArgumentException(
                        "sink attempt identity has trailing bytes");
            }
            return SinkAttemptIdentity.decoded(
                    identityVersion, artifactId,
                    new RunAttemptFence(runId, attemptId, fenceToken),
                    assignmentGeneration, stageExecutionId, logicalStage,
                    instanceIndex, operatorId, profile, domain,
                    descriptorDigest, identityDigest);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "sink attempt identity field length is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "sink attempt identity is malformed", malformed);
        }
    }

    static int profileId(BatchSinkDescriptor.CapabilityProfile profile) {
        switch (profile) {
            case LEGACY_PROCESS_LOCAL: return 1;
            case SIDE_EFFECT_FREE_V1: return 2;
            case MANIFEST_ATOMIC_GROUP_V1: return 3;
            default: throw new IllegalArgumentException("unknown Sink capability profile");
        }
    }

    static BatchSinkDescriptor.CapabilityProfile profile(int id) {
        switch (id) {
            case 1: return BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL;
            case 2: return BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1;
            case 3: return BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1;
            default: throw new IllegalArgumentException(
                    "sink capability profile is unknown: " + id);
        }
    }

    static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    static byte[] readBytes(DataInputStream in, String field, int maxBytes)
            throws IOException {
        int length = in.readInt();
        if (length < 1 || length > maxBytes || length > in.available()) {
            throw new IllegalArgumentException(field + " length out of bounds");
        }
        return readFixed(in, length, field);
    }

    static String readText(DataInputStream in, String field, int maxBytes)
            throws IOException {
        return RunText.decode(readBytes(in, field, maxBytes), field, maxBytes);
    }

    static byte[] readFixed(DataInputStream in, int length, String field)
            throws IOException {
        if (length > in.available()) {
            throw new IllegalArgumentException(field + " length is truncated");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
