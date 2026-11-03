package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Canonical decision observation wire; separate from Batch/CDC authority formats. */
public final class CheckpointDecisionCodec {
    private static final byte[] MAGIC =
            "RWXXSCD1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_BODY_BYTES = 4096;

    private CheckpointDecisionCodec() { }

    public static byte[] encode(CheckpointDecision decision) {
        if (decision == null) throw new IllegalArgumentException("decision is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                CheckpointManifestCodec.writeIdentity(
                        output, decision.checkpointIdentity());
                output.writeInt(decision.status().wireId());
                output.writeBoolean(decision.manifestDigest() != null);
                if (decision.manifestDigest() != null) {
                    CheckpointManifestCodec.writeText(
                            output, decision.manifestDigest());
                }
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw StreamContractSupport.failure("LIMIT_EXCEEDED",
                        "checkpoint.decision.body",
                        "decision body exceeds its limit");
            }
            return CheckpointManifestCodec.frame(MAGIC, body);
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory decision encode failed", impossible);
        }
    }

    public static CheckpointDecision decode(byte[] frame) {
        byte[] body = CheckpointManifestCodec.bodyFromFrame(
                frame, MAGIC, MAX_BODY_BYTES);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            CheckpointIdentity checkpoint =
                    CheckpointManifestCodec.readIdentity(input);
            CheckpointDecision.Status status =
                    CheckpointDecision.Status.fromWire(input.readInt());
            String manifestDigest = input.readBoolean()
                    ? CheckpointManifestCodec.readText(
                            input, 64, "checkpoint.decision.manifestDigest")
                    : null;
            if (input.available() != 0) {
                throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                        "checkpoint.decision.body",
                        "decision body has trailing bytes");
            }
            return new CheckpointDecision(checkpoint, status, manifestDigest);
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "checkpoint.decision.body", "decision body is truncated");
        } catch (IOException malformed) {
            throw StreamContractSupport.failure("FRAME_LENGTH_MISMATCH",
                    "checkpoint.decision.body", "decision body is malformed");
        }
    }
}
