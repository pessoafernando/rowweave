package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Canonical, bounded and checksummed Stream savepoint metadata wire. */
public final class StreamSavepointCodec {
    private static final byte[] MAGIC =
            "RWXXSSP1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_BODY_BYTES =
            CheckpointManifestCodec.MAX_FRAME_BYTES + 1024;
    public static final int MAX_FRAME_BYTES = MAX_BODY_BYTES + 48;

    private StreamSavepointCodec() { }

    public static byte[] encode(StreamSavepoint savepoint) {
        if (savepoint == null) {
            throw new IllegalArgumentException("savepoint is required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                CheckpointManifestCodec.writeText(
                        output, savepoint.savepointId());
                byte[] manifest = CheckpointManifestCodec.encode(
                        savepoint.checkpointManifest());
                output.writeInt(manifest.length);
                output.write(manifest);
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw StreamContractSupport.failure("LIMIT_EXCEEDED",
                        "savepoint.body", "savepoint metadata exceeds its limit");
            }
            return CheckpointManifestCodec.frame(MAGIC, body);
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory savepoint encode failed", impossible);
        }
    }

    public static StreamSavepoint decode(byte[] frame) {
        byte[] body = CheckpointManifestCodec.bodyFromFrame(
                frame, MAGIC, MAX_BODY_BYTES);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            String savepointId = CheckpointManifestCodec.readText(
                    input, 256, "savepoint.id");
            int manifestLength = input.readInt();
            if (manifestLength < 1
                    || manifestLength > CheckpointManifestCodec.MAX_FRAME_BYTES
                    || manifestLength > input.available()) {
                throw StreamContractSupport.failure("LIMIT_EXCEEDED",
                        "savepoint.manifest",
                        "checkpoint manifest exceeds its bound");
            }
            byte[] manifest = new byte[manifestLength];
            input.readFully(manifest);
            if (input.available() != 0) {
                throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                        "savepoint.body", "savepoint body has trailing bytes");
            }
            return new StreamSavepoint(savepointId,
                    CheckpointManifestCodec.decode(manifest));
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "savepoint.body", "savepoint body is truncated");
        } catch (IOException malformed) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "savepoint.body", "savepoint body is malformed");
        }
    }
}
