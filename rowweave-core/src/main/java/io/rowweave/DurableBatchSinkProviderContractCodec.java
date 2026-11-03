package io.rowweave;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical bounded codec for signed durable provider contract resources. */
public final class DurableBatchSinkProviderContractCodec {

    public static final int MAX_FRAME_BYTES = 4 * 1024;
    private static final int MAGIC = 0x52575043; // RWPC
    private static final int FRAME_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 12;

    private DurableBatchSinkProviderContractCodec() {}

    public static byte[] encode(DurableBatchSinkProviderContract contract) {
        if (contract == null) {
            throw new IllegalArgumentException("provider contract is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(512);
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            payload.writeInt(contract.spiVersion());
            writeString(payload, contract.providerKey().connectorType());
            payload.writeInt(contract.providerKey().connectorVersion());
            payload.writeInt(contract.providerKey().capabilityProfile().wireId());
            writeCodec(payload, contract.configurationCodec());
            writeCodec(payload, contract.locatorCodec());
            writeCodec(payload, contract.outcomeCodec());
            payload.writeInt(contract.idempotencyScope().wireId());
            DurableSinkCleanupContract cleanup = contract.cleanupContract();
            payload.writeLong(cleanup.minimumRetentionMillis());
            payload.writeInt(cleanup.maxClaimBatch());
            payload.writeInt(cleanup.maxDeleteBatch());
            payload.writeBoolean(cleanup.leaseRequired());
            payload.writeInt(cleanup.orphanOwnership().wireId());
            payload.writeInt(cleanup.quarantinePolicy().wireId());
            payload.writeInt(cleanup.safeDeleteProof().wireId());
            DurableSinkAttemptBudget budget = contract.attemptBudget();
            payload.writeLong(budget.maxResidentBytes());
            payload.writeInt(budget.maxOpenHandles());
            payload.writeInt(budget.maxLocatorBytes());
            payload.writeInt(budget.maxControlBytes());
            payload.writeLong(budget.actionTimeoutMillis());
            payload.writeInt(contract.deploymentCapability().wireId());
            payload.flush();

            byte[] body = payloadBytes.toByteArray();
            int frameLength = HEADER_BYTES + body.length + DIGEST_BYTES;
            if (frameLength > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("provider contract frame exceeds bound");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(frameLength);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(FRAME_VERSION);
            frame.writeInt(body.length);
            frame.write(body);
            frame.write(sha256(body));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory provider contract encoding failed", impossible);
        }
    }

    public static DurableBatchSinkProviderContract decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("provider contract frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            if (input.readInt() != MAGIC || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException("provider contract frame header is invalid");
            }
            int payloadLength = input.readInt();
            int expectedLength = HEADER_BYTES + payloadLength + DIGEST_BYTES;
            if (payloadLength < 0 || expectedLength != frame.length) {
                throw new IllegalArgumentException("provider contract frame length is invalid");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] expectedDigest = new byte[DIGEST_BYTES];
            input.readFully(expectedDigest);
            if (input.read() != -1 || !MessageDigest.isEqual(
                    expectedDigest, sha256(payload))) {
                throw new IllegalArgumentException("provider contract checksum is invalid");
            }

            DataInputStream body = new DataInputStream(new ByteArrayInputStream(payload));
            int spiVersion = body.readInt();
            DurableSinkProviderKey key = new DurableSinkProviderKey(
                    readString(body, BatchSinkDescriptor.MAX_CONNECTOR_TYPE_BYTES),
                    body.readInt(),
                    BatchSinkDescriptor.CapabilityProfile.fromWire(body.readInt()));
            DurableSinkCodecDescriptor config = readCodec(body);
            DurableSinkCodecDescriptor locator = readCodec(body);
            DurableSinkCodecDescriptor outcome = readCodec(body);
            DurableBatchSinkProviderContract.IdempotencyScope idempotency =
                    DurableBatchSinkProviderContract.IdempotencyScope.fromWire(
                            body.readInt());
            long retention = body.readLong();
            int claims = body.readInt();
            int deletes = body.readInt();
            boolean lease = body.readBoolean();
            DurableBatchSinkProviderContract.OrphanOwnership ownership =
                    DurableBatchSinkProviderContract.OrphanOwnership.fromWire(
                            body.readInt());
            DurableBatchSinkProviderContract.QuarantinePolicy quarantine =
                    DurableBatchSinkProviderContract.QuarantinePolicy.fromWire(
                            body.readInt());
            DurableBatchSinkProviderContract.SafeDeleteProof safeDelete =
                    DurableBatchSinkProviderContract.SafeDeleteProof.fromWire(
                            body.readInt());
            DurableSinkCleanupContract cleanup = new DurableSinkCleanupContract(
                    retention, claims, deletes, lease, ownership, quarantine,
                    safeDelete);
            DurableSinkAttemptBudget budget = new DurableSinkAttemptBudget(
                    body.readLong(), body.readInt(), body.readInt(),
                    body.readInt(), body.readLong());
            DurableBatchSinkProviderContract.DeploymentCapability deployment =
                    DurableBatchSinkProviderContract.DeploymentCapability.fromWire(
                            body.readInt());
            if (body.read() != -1) {
                throw new IllegalArgumentException("provider contract payload has trailing bytes");
            }
            return new DurableBatchSinkProviderContract(spiVersion, key, config,
                    locator, outcome, idempotency, cleanup, budget, deployment);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException("provider contract frame is truncated", truncated);
        } catch (IOException invalid) {
            throw new IllegalArgumentException("provider contract frame is invalid", invalid);
        }
    }

    public static byte[] digest(DurableBatchSinkProviderContract contract) {
        return sha256(encode(contract));
    }

    private static void writeCodec(
            DataOutputStream output, DurableSinkCodecDescriptor codec)
            throws IOException {
        writeString(output, codec.codecId());
        output.writeInt(codec.codecVersion());
        output.writeInt(codec.maxPayloadBytes());
    }

    private static DurableSinkCodecDescriptor readCodec(DataInputStream input)
            throws IOException {
        return new DurableSinkCodecDescriptor(
                readString(input, DurableSinkCodecDescriptor.MAX_CODEC_ID_BYTES),
                input.readInt(), input.readInt());
    }

    private static void writeString(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInputStream input, int maximumBytes)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximumBytes) {
            throw new IllegalArgumentException(
                    "provider contract text length is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "provider contract text is not canonical UTF-8");
        }
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
