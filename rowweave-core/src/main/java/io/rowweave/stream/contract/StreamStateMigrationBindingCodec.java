package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Canonical bounded wire for one signed Stream state migration mapping. */
public final class StreamStateMigrationBindingCodec {
    private static final byte[] MAGIC =
            "RWXXSMB1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_BODY_BYTES = 4096;
    public static final int MAX_FRAME_BYTES = MAX_BODY_BYTES + 48;

    private StreamStateMigrationBindingCodec() { }

    public static byte[] encode(StreamStateMigrationBinding binding) {
        if (binding == null) {
            throw new IllegalArgumentException("state migration binding is required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                CheckpointManifestCodec.writeText(
                        output, binding.operatorUid());
                writeState(output, binding.sourceState());
                writeState(output, binding.targetState());
                CheckpointManifestCodec.writeText(
                        output, binding.deploymentSetId());
                CheckpointManifestCodec.writeText(
                        output, binding.deploymentId());
                CheckpointManifestCodec.writeText(
                        output, binding.deploymentJarDigest());
                CheckpointManifestCodec.writeText(
                        output, binding.adapterClassName());
                CheckpointManifestCodec.writeText(
                        output, binding.targetContentDigest());
                output.writeLong(binding.maximumOutputBytes());
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw StreamContractSupport.failure("LIMIT_EXCEEDED",
                        "stateMigration.body",
                        "state migration binding exceeds its bound");
            }
            return CheckpointManifestCodec.frame(MAGIC, body);
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory state migration encode failed", impossible);
        }
    }

    public static StreamStateMigrationBinding decode(byte[] frame) {
        byte[] body = CheckpointManifestCodec.bodyFromFrame(
                frame, MAGIC, MAX_BODY_BYTES);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            String operatorUid = CheckpointManifestCodec.readText(
                    input, 256, "stateMigration.operatorUid");
            StreamStateDescriptor source = readState(input, "source");
            StreamStateDescriptor target = readState(input, "target");
            String deploymentSetId = CheckpointManifestCodec.readText(
                    input, 64, "stateMigration.deploymentSetId");
            String deploymentId = CheckpointManifestCodec.readText(
                    input, 128, "stateMigration.deploymentId");
            String deploymentJarDigest = CheckpointManifestCodec.readText(
                    input, 64, "stateMigration.deploymentJarDigest");
            String adapterClassName = CheckpointManifestCodec.readText(
                    input, 512, "stateMigration.adapterClassName");
            String targetContentDigest = CheckpointManifestCodec.readText(
                    input, 64, "stateMigration.targetContentDigest");
            long maximumOutputBytes = input.readLong();
            if (input.available() != 0) {
                throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                        "stateMigration.body",
                        "state migration body has trailing bytes");
            }
            return new StreamStateMigrationBinding(
                    operatorUid, source, target, deploymentSetId,
                    deploymentId, deploymentJarDigest, adapterClassName,
                    targetContentDigest, maximumOutputBytes);
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "stateMigration.body",
                    "state migration body is truncated");
        } catch (IOException malformed) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "stateMigration.body",
                    "state migration body is malformed");
        }
    }

    private static void writeState(
            DataOutputStream output, StreamStateDescriptor state)
            throws IOException {
        CheckpointManifestCodec.writeText(output, state.stateName());
        output.writeInt(state.kind().wireId());
        CheckpointManifestCodec.writeText(
                output, state.serializerContractId());
        output.writeInt(state.stateSchemaVersion());
        output.writeInt(state.ttlMode().wireId());
        output.writeLong(state.ttlMillis());
    }

    private static StreamStateDescriptor readState(
            DataInputStream input, String side) throws IOException {
        return new StreamStateDescriptor(
                CheckpointManifestCodec.readText(
                        input, 256, "stateMigration." + side + ".name"),
                StreamStateDescriptor.Kind.fromWire(input.readInt()),
                CheckpointManifestCodec.readText(
                        input, 256,
                        "stateMigration." + side + ".serializer"),
                input.readInt(),
                StreamStateDescriptor.TtlMode.fromWire(input.readInt()),
                input.readLong());
    }
}
