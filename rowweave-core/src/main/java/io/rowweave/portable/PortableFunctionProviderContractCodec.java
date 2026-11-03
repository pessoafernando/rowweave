package io.rowweave.portable;

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
import java.util.Arrays;

/** Canonical checksummed codec for signed portable function provider contracts. */
public final class PortableFunctionProviderContractCodec {
    public static final int MAX_FRAME_BYTES = 4096;
    private static final byte[] MAGIC =
            "RWXXFPC1".getBytes(StandardCharsets.US_ASCII);
    private static final int FRAME_VERSION = 1;
    private static final int HEADER_BYTES = 16;
    private static final int CHECKSUM_BYTES = 32;

    private PortableFunctionProviderContractCodec() { }

    public static byte[] encode(PortableFunctionProviderContract contract) {
        if (contract == null) {
            throw PortablePlanText.failure("INVALID_FUNCTION_CONTRACT",
                    "functionContract", "function contract is required");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(contract.contractVersion());
                writeText(body, contract.functionClassName());
                body.writeInt(contract.functionAbi().wireId());
                writeText(body, contract.configurationCodecId());
                body.writeInt(contract.maximumConfigurationBytes());
                body.writeLong(contract.maximumResources().maximumHeapBytes());
                body.writeLong(contract.maximumResources().maximumDirectBytes());
                body.writeInt(contract.maximumResources()
                        .maximumOutputRowsPerInput());
            }
            byte[] payload = bodyBytes.toByteArray();
            int frameSize = Math.addExact(
                    Math.addExact(HEADER_BYTES, payload.length), CHECKSUM_BYTES);
            if (frameSize > MAX_FRAME_BYTES) limit();
            ByteArrayOutputStream frameBytes =
                    new ByteArrayOutputStream(frameSize);
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(FRAME_VERSION);
                frame.writeInt(payload.length);
                frame.write(payload);
                frame.write(PortablePlanText.sha256(payload));
            }
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory function contract encoding failed", impossible);
        }
    }

    public static PortableFunctionProviderContract decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) limit();
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic)) {
                invalid("FUNCTION_CONTRACT_MAGIC_MISMATCH");
            }
            if (input.readInt() != FRAME_VERSION) {
                invalid("UNSUPPORTED_FUNCTION_CONTRACT_FRAME");
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
                invalid("FUNCTION_CONTRACT_CHECKSUM_MISMATCH");
            }
            try (DataInputStream fields = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                PortableFunctionProviderContract contract =
                        new PortableFunctionProviderContract(
                                fields.readInt(), readText(fields, 512),
                                FunctionAbi.fromWireId(fields.readInt()),
                                readText(fields, 256), fields.readInt(),
                                new FunctionResourceProfile(
                                        fields.readLong(), fields.readLong(),
                                        fields.readInt()));
                if (fields.read() != -1) {
                    invalid("FUNCTION_CONTRACT_TRAILING_BYTES");
                }
                return contract;
            }
        } catch (EOFException truncated) {
            throw PortablePlanText.failure("FUNCTION_CONTRACT_TRUNCATED",
                    "functionContract.frame",
                    "function contract frame is truncated");
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory function contract decoding failed", impossible);
        }
    }

    public static String digest(PortableFunctionProviderContract contract) {
        return PortablePlanText.hex(PortablePlanText.sha256(encode(contract)));
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
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return decoded.toString();
        } catch (CharacterCodingException invalid) {
            throw PortablePlanText.failure("INVALID_FUNCTION_CONTRACT",
                    "functionContract.text",
                    "function contract text is not canonical UTF-8");
        }
    }

    private static void limit() {
        throw PortablePlanText.failure("LIMIT_EXCEEDED",
                "functionContract.frame",
                "function contract frame exceeds its bounds");
    }

    private static void invalid(String code) {
        throw PortablePlanText.failure(code, "functionContract.frame",
                "function contract frame is invalid");
    }
}
