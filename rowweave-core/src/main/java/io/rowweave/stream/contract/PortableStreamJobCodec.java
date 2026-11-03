package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCheckpointMode;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical, checksummed and allocation-bounded PortableStreamJob v1 frame. */
public final class PortableStreamJobCodec {
    private static final byte[] MAGIC_V1 =
            "RWXXPSJ1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V2 =
            "RWXXPSJ2".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES = PortableStreamPlanCodec.MAX_FRAME_BYTES + 256;
    public static final int MAX_FRAME_BYTES = HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES;

    private PortableStreamJobCodec() { }

    public static byte[] encode(PortableStreamJob job) {
        if (job == null) throw new IllegalArgumentException("job is required");
        boolean v2 = job.checkpointConfig().mode()
                == StreamCheckpointMode.UNALIGNED
                || job.plan().planVersion()
                >= PortableStreamPlan.DYNAMIC_SOURCE_VERSION;
        byte[] planFrame = PortableStreamPlanCodec.encode(job.plan());
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(planFrame.length + 256);
            try (DataOutputStream output = new DataOutputStream(bodyBytes)) {
                output.writeInt(v2 ? 2 : PortableStreamJob.CURRENT_VERSION);
                output.writeInt(planFrame.length);
                output.write(planFrame);
                output.write(sha256(planFrame));
                StreamCheckpointConfig checkpoint = job.checkpointConfig();
                if (v2) output.writeInt(checkpoint.mode().wireId());
                else output.writeBoolean(true); // frozen v1 aligned flag
                output.writeLong(checkpoint.intervalMillis());
                output.writeLong(checkpoint.timeoutMillis());
                output.writeInt(checkpoint.retainedCompleted());
                StreamRestartPolicy restart = job.restartPolicy();
                output.writeInt(restart.maximumAttempts());
                output.writeLong(restart.backoffMillis());
                StreamResourceProfile resource = job.resourceProfile();
                output.writeInt(resource.mailboxEvents());
                output.writeInt(resource.maximumFrameBytes());
                output.writeInt(resource.channelCreditBytes());
                output.writeLong(resource.managedStateBytes());
                output.writeInt(resource.timerCount());
                output.writeInt(resource.windowCount());
                output.writeLong(resource.checkpointBytes());
                output.writeInt(resource.threadCount());
                output.writeInt(resource.fileDescriptorCount());
                output.writeLong(resource.spillDiskBytes());
            }
            byte[] body = bodyBytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw failure("LIMIT_EXCEEDED", "job.body", "job frame exceeds limit");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(frame)) {
                output.write(v2 ? MAGIC_V2 : MAGIC_V1);
                output.writeInt(v2 ? 2 : 1);
                output.writeInt(body.length);
                output.write(body);
                output.write(sha256(body));
            }
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory job encoding failed", impossible);
        }
    }

    public static PortableStreamJob decode(byte[] frame) {
        return decode(frame, false);
    }

    /** Frozen v1 reader used for N-1 compatibility and fail-closed tests. */
    public static PortableStreamJob decodeV1(byte[] frame) {
        return decode(frame, true);
    }

    private static PortableStreamJob decode(byte[] frame, boolean v1Only) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw failure("FRAME_LENGTH_MISMATCH", "job.envelope",
                    "invalid job frame length");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC_V1.length];
            input.readFully(magic);
            boolean v1 = MessageDigest.isEqual(magic, MAGIC_V1);
            boolean v2 = MessageDigest.isEqual(magic, MAGIC_V2);
            if (!v1 && !v2) {
                throw failure("INVALID_MAGIC", "job.envelope.magic",
                        "invalid Stream job magic");
            }
            if (v1Only && !v1) {
                throw failure("UNSUPPORTED_ENVELOPE_VERSION",
                        "job.envelope.version",
                        "Stream job v1 reader rejects v2 capabilities");
            }
            int envelopeVersion = input.readInt();
            if ((v1 && envelopeVersion != 1)
                    || (v2 && envelopeVersion != 2)) {
                throw failure("UNSUPPORTED_ENVELOPE_VERSION", "job.envelope.version",
                        "unsupported Stream job envelope version");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES != frame.length) {
                throw failure("FRAME_LENGTH_MISMATCH", "job.envelope.body",
                        "invalid Stream job body length");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(sha256(body), expected)) {
                throw failure("CHECKSUM_MISMATCH", "job.envelope.digest",
                        "Stream job checksum mismatch");
            }
            return decodeBody(body, envelopeVersion);
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure("FRAME_LENGTH_MISMATCH", "job.envelope",
                    "truncated Stream job frame");
        } catch (IOException malformed) {
            throw failure("FRAME_LENGTH_MISMATCH", "job.envelope",
                    "malformed Stream job frame");
        }
    }

    private static PortableStreamJob decodeBody(
            byte[] bodyBytes, int envelopeVersion) throws IOException {
        try (DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(bodyBytes))) {
            int jobVersion = body.readInt();
            if (jobVersion != envelopeVersion
                    || (jobVersion != PortableStreamJob.CURRENT_VERSION
                    && jobVersion != 2)) {
                throw failure("UNSUPPORTED_JOB_VERSION", "job.version",
                        "unsupported Stream product job version");
            }
            int planLength = body.readInt();
            if (planLength < 0 || planLength > PortableStreamPlanCodec.MAX_FRAME_BYTES
                    || planLength > body.available() - DIGEST_BYTES) {
                throw failure("FRAME_LENGTH_MISMATCH", "job.plan",
                        "invalid nested Stream plan length");
            }
            byte[] planFrame = new byte[planLength];
            body.readFully(planFrame);
            byte[] expectedPlanDigest = new byte[DIGEST_BYTES];
            body.readFully(expectedPlanDigest);
            if (!MessageDigest.isEqual(sha256(planFrame), expectedPlanDigest)) {
                throw failure("CHECKSUM_MISMATCH", "job.plan.digest",
                        "nested Stream plan checksum mismatch");
            }
            StreamCheckpointMode mode;
            if (jobVersion == 1) {
                if (!body.readBoolean()) {
                    throw failure("UNSUPPORTED_CHECKPOINT_MODE",
                            "job.checkpoint.aligned",
                            "Stream job v1 only supports aligned checkpoints");
                }
                mode = StreamCheckpointMode.ALIGNED;
            } else {
                try {
                    mode = StreamCheckpointMode.fromWireId(body.readInt());
                } catch (IllegalArgumentException unknown) {
                    throw failure("UNSUPPORTED_CHECKPOINT_MODE",
                            "job.checkpoint.mode",
                            "unknown Stream checkpoint mode");
                }
            }
            long interval = body.readLong();
            long timeout = body.readLong();
            int retained = body.readInt();
            StreamCheckpointConfig checkpoint;
            if (mode == StreamCheckpointMode.UNALIGNED) {
                checkpoint = interval == 0L
                        ? StreamCheckpointConfig.manualOnlyUnaligned(
                        timeout, retained)
                        : StreamCheckpointConfig.periodicUnaligned(
                        interval, timeout, retained);
            } else {
                checkpoint = interval == 0L
                        ? StreamCheckpointConfig.manualOnly(timeout, retained)
                        : StreamCheckpointConfig.periodic(
                        interval, timeout, retained);
            }
            StreamRestartPolicy restart = StreamRestartPolicy.fixedDelay(
                    body.readInt(), body.readLong());
            StreamResourceProfile resource = new StreamResourceProfile(
                    body.readInt(), body.readInt(), body.readInt(), body.readLong(),
                    body.readInt(), body.readInt(), body.readLong(), body.readInt(),
                    body.readInt(), body.readLong());
            if (body.available() != 0) {
                throw failure("FRAME_LENGTH_MISMATCH", "job.body",
                        "trailing Stream job body bytes");
            }
            return new PortableStreamJob(PortableStreamPlanCodec.decode(planFrame),
                    checkpoint, restart, resource);
        } catch (IllegalArgumentException invalid) {
            if (invalid instanceof StreamContractException) throw invalid;
            throw failure("INVALID_JOB_CONFIGURATION", "job.configuration",
                    "invalid Stream job configuration");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }
}
