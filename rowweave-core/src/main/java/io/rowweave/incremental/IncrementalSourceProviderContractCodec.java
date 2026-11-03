package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical bounded codec for signed incremental source contracts. */
public final class IncrementalSourceProviderContractCodec {
    public static final int MAX_FRAME_BYTES = 4 * 1024;
    private static final int MAGIC = 0x52574953; // RWIS
    private static final int FRAME_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 12;

    private IncrementalSourceProviderContractCodec() { }

    public static byte[] encode(IncrementalSourceProviderContract contract) {
        if (contract == null) {
            throw new IllegalArgumentException(
                    "incremental source provider contract is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(512);
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            payload.writeInt(contract.spiVersion());
            writeText(payload, contract.providerKey().connectorType());
            payload.writeInt(contract.providerKey().connectorVersion());
            writeCodec(payload, contract.configurationCodec());
            writeCodec(payload, contract.captureResponseCodec());
            writeCodec(payload, contract.materializationResponseCodec());
            payload.writeInt(contract.maxTargetBytes());
            payload.writeInt(contract.frontierKind().wireId());
            payload.writeInt(contract.orderingGuarantee().wireId());
            payload.writeInt(contract.snapshotGuarantee().wireId());
            payload.writeBoolean(contract.tombstoneCapable());
            IncrementalSourceActionBudget budget = contract.actionBudget();
            payload.writeLong(budget.maxNormalResidentBytes());
            payload.writeLong(budget.maxLargeResidentBytes());
            payload.writeInt(budget.maxProviderHandles());
            payload.writeLong(budget.actionTimeoutMillis());
            payload.writeInt(contract.deploymentCapability().wireId());
            payload.flush();
            byte[] body = payloadBytes.toByteArray();
            int total = HEADER_BYTES + body.length + DIGEST_BYTES;
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental source provider contract frame exceeds bound");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(total);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(FRAME_VERSION);
            frame.writeInt(body.length);
            frame.write(body);
            frame.write(sha256(body));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory incremental source contract encoding failed",
                    impossible);
        }
    }

    public static IncrementalSourceProviderContract decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source provider contract frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame));
            if (input.readInt() != MAGIC
                    || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException(
                        "incremental source provider contract header is invalid");
            }
            int payloadLength = input.readInt();
            long expected = (long) HEADER_BYTES + payloadLength + DIGEST_BYTES;
            if (payloadLength < 0 || expected != frame.length) {
                throw new IllegalArgumentException(
                        "incremental source provider contract length is invalid");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] checksum = new byte[DIGEST_BYTES];
            input.readFully(checksum);
            if (input.read() != -1 || !MessageDigest.isEqual(
                    checksum, sha256(payload))) {
                throw new IllegalArgumentException(
                        "incremental source provider contract checksum is invalid");
            }
            DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(payload));
            int spiVersion = body.readInt();
            IncrementalSourceProviderKey key =
                    new IncrementalSourceProviderKey(
                            readText(body,
                                    IncrementalSourceProviderKey
                                            .MAX_CONNECTOR_TYPE_BYTES),
                            body.readInt());
            IncrementalSourceCodecDescriptor config = readCodec(body);
            IncrementalSourceCodecDescriptor capture = readCodec(body);
            IncrementalSourceCodecDescriptor materialization = readCodec(body);
            int maxTargetBytes = body.readInt();
            IncrementalFrontier.Kind frontierKind =
                    IncrementalFrontier.Kind.fromWire(body.readInt());
            IncrementalSourceProviderContract.OrderingGuarantee ordering =
                    IncrementalSourceProviderContract.OrderingGuarantee
                            .fromWire(body.readInt());
            IncrementalSourceProviderContract.SnapshotGuarantee snapshot =
                    IncrementalSourceProviderContract.SnapshotGuarantee
                            .fromWire(body.readInt());
            boolean tombstone = body.readBoolean();
            IncrementalSourceActionBudget budget =
                    new IncrementalSourceActionBudget(
                            body.readLong(), body.readLong(), body.readInt(),
                            body.readLong());
            IncrementalSourceProviderContract.DeploymentCapability deployment =
                    IncrementalSourceProviderContract.DeploymentCapability
                            .fromWire(body.readInt());
            if (body.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental source provider contract has trailing bytes");
            }
            return new IncrementalSourceProviderContract(
                    spiVersion, key, config, capture, materialization,
                    maxTargetBytes, frontierKind, ordering, snapshot,
                    tombstone, budget, deployment);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental source provider contract is truncated",
                    truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental source provider contract is malformed",
                    malformed);
        }
    }

    public static byte[] digest(IncrementalSourceProviderContract contract) {
        return sha256(encode(contract));
    }

    private static void writeCodec(
            DataOutputStream output, IncrementalSourceCodecDescriptor codec)
            throws IOException {
        writeText(output, codec.codecId());
        output.writeInt(codec.codecVersion());
        output.writeInt(codec.maxPayloadBytes());
    }

    private static IncrementalSourceCodecDescriptor readCodec(
            DataInputStream input) throws IOException {
        return new IncrementalSourceCodecDescriptor(
                readText(input,
                        IncrementalSourceCodecDescriptor.MAX_CODEC_ID_BYTES),
                input.readInt(), input.readInt());
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input, int maximumBytes)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximumBytes
                || length > input.available()) {
            throw new IllegalArgumentException(
                    "incremental source contract text length is invalid");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            CharBuffer value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return value.toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "incremental source contract text is invalid UTF-8",
                    invalid);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
