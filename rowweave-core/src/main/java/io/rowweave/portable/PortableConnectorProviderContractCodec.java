package io.rowweave.portable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical checksummed source/sink provider contract codec. */
public final class PortableConnectorProviderContractCodec {
    public static final int MAX_FRAME_BYTES = 2048;
    private static final byte[] MAGIC =
            "RWXXCPR1".getBytes(StandardCharsets.US_ASCII);
    private static final int FRAME_VERSION = 1;
    private static final int HEADER_BYTES = 16;
    private static final int CHECKSUM_BYTES = 32;

    private PortableConnectorProviderContractCodec() { }

    public static byte[] encode(PortableConnectorProviderContract contract) {
        if (contract == null) invalid("INVALID_CONNECTOR_CONTRACT");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(contract.contractVersion());
                body.writeInt(kindId(contract.bindingKind()));
                writeText(body, contract.providerKey());
                writeText(body, contract.configurationCodecId());
                body.writeInt(contract.maximumConfigurationBytes());
                body.writeInt(contract.maximumPayloadBytes());
                body.writeInt(contract.maximumInstances());
            }
            byte[] payload = bodyBytes.toByteArray();
            int size = Math.addExact(
                    Math.addExact(HEADER_BYTES, payload.length),
                    CHECKSUM_BYTES);
            if (size > MAX_FRAME_BYTES) limit();
            ByteArrayOutputStream result = new ByteArrayOutputStream(size);
            try (DataOutputStream output = new DataOutputStream(result)) {
                output.write(MAGIC);
                output.writeInt(FRAME_VERSION);
                output.writeInt(payload.length);
                output.write(payload);
                output.write(PortablePlanText.sha256(payload));
            }
            return result.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory connector contract encoding failed", impossible);
        }
    }

    public static PortableConnectorProviderContract decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) limit();
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                invalid("CONNECTOR_CONTRACT_MAGIC_MISMATCH");
            }
            if (input.readInt() != FRAME_VERSION) {
                invalid("UNSUPPORTED_CONNECTOR_CONTRACT_FRAME");
            }
            int bodyLength = input.readInt();
            if (bodyLength <= 0
                    || bodyLength != frame.length - HEADER_BYTES
                    - CHECKSUM_BYTES) limit();
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] checksum = new byte[CHECKSUM_BYTES];
            input.readFully(checksum);
            if (!MessageDigest.isEqual(
                    checksum, PortablePlanText.sha256(body))) {
                invalid("CONNECTOR_CONTRACT_CHECKSUM_MISMATCH");
            }
            try (DataInputStream fields = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                PortableConnectorProviderContract contract =
                        new PortableConnectorProviderContract(
                                fields.readInt(), kind(fields.readInt()),
                                readText(fields, 256),
                                readText(fields, 256), fields.readInt(),
                                fields.readInt(), fields.readInt());
                if (fields.read() != -1) {
                    invalid("CONNECTOR_CONTRACT_TRAILING_BYTES");
                }
                return contract;
            }
        } catch (EOFException truncated) {
            throw new PortablePlanException("CONNECTOR_CONTRACT_TRUNCATED",
                    "connectorContract.frame",
                    "connector contract frame is truncated", truncated);
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory connector contract decoding failed", impossible);
        }
    }

    public static String digest(PortableConnectorProviderContract contract) {
        return PortablePlanText.hex(PortablePlanText.sha256(encode(contract)));
    }

    private static int kindId(PortableOperatorCode.BindingKind kind) {
        if (kind == PortableOperatorCode.BindingKind.SOURCE) return 1;
        if (kind == PortableOperatorCode.BindingKind.SINK) return 2;
        invalid("INVALID_CONNECTOR_CONTRACT");
        return -1;
    }

    private static PortableOperatorCode.BindingKind kind(int id) {
        if (id == 1) return PortableOperatorCode.BindingKind.SOURCE;
        if (id == 2) return PortableOperatorCode.BindingKind.SINK;
        invalid("INVALID_CONNECTOR_CONTRACT");
        return null;
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum) limit();
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
        } catch (CharacterCodingException invalid) {
            throw new PortablePlanException("INVALID_CONNECTOR_CONTRACT",
                    "connectorContract.text",
                    "connector contract text is not canonical UTF-8", invalid);
        }
    }

    private static void limit() {
        throw new PortablePlanException("LIMIT_EXCEEDED",
                "connectorContract.frame",
                "connector contract frame exceeds its bounds");
    }

    private static void invalid(String code) {
        throw new PortablePlanException(code, "connectorContract.frame",
                "connector contract frame is invalid");
    }
}
