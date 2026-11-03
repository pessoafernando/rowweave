package io.rowweave.cluster.registry;

import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolHelloV1Codec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Bounded canonical legacy/secure codec shared by every durable RegistryWorker backend. */
public final class RegistryWorkerCodec {
    public static final int MAX_RECORD_BYTES = 4096;
    private static final int MAX_WORKER_ID_BYTES = 256;
    private static final int MAX_HOST_BYTES = 255;
    private static final int MAX_INCARNATION_BYTES = 256;
    private static final byte[] SECURE_MAGIC = "RWXXREGS1".getBytes(StandardCharsets.US_ASCII);
    private static final int SECURE_VERSION = 1;
    private static final int SECURE_SUFFIX_BYTES = SECURE_MAGIC.length + 4 + 64 + 64;
    private static final byte[] DATA_MAGIC = "RWXXDP01".getBytes(StandardCharsets.US_ASCII);
    private static final int DATA_VERSION = 1;
    private static final int DATA_PREFIX_BYTES = DATA_MAGIC.length + 4 + 4 + 4;
    private static final byte[] PROTOCOL_MAGIC =
            "RWXXPH01".getBytes(StandardCharsets.US_ASCII);
    private static final int PROTOCOL_VERSION = 1;
    private static final int PROTOCOL_PREFIX_BYTES =
            PROTOCOL_MAGIC.length + 2 * Integer.BYTES;

    private RegistryWorkerCodec() { }

    public static byte[] encode(RegistryWorker worker) {
        if (worker == null) throw invalid("registry worker is required");
        byte[] workerId = utf8(worker.workerId(), MAX_WORKER_ID_BYTES, "workerId");
        byte[] host = utf8(worker.host(), MAX_HOST_BYTES, "host");
        byte[] incarnation = worker.incarnationId() == null ? null
                : utf8(worker.incarnationId(), MAX_INCARNATION_BYTES, "incarnationId");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeBytes(out, workerId);
                writeBytes(out, host);
                out.writeInt(worker.port());
                out.writeInt(worker.slots());
                out.writeInt(worker.dataPort());
                if (incarnation == null) out.writeInt(-1);
                else writeBytes(out, incarnation);
                RpcSecurityMetadata security = worker.securityMetadata();
                if (security != null) {
                    out.write(SECURE_MAGIC);
                    out.writeInt(SECURE_VERSION);
                    out.write(security.deploymentSetId().getBytes(StandardCharsets.US_ASCII));
                    out.write(security.authzSetId().getBytes(StandardCharsets.US_ASCII));
                }
                DataPlaneRegistration data = worker.dataPlaneRegistration();
                if (data != null) {
                    if (security == null) {
                        throw invalid("data identity requires secure registry identity");
                    }
                    byte[] fingerprint = data.certificateFingerprint();
                    out.write(DATA_MAGIC);
                    out.writeInt(DATA_VERSION);
                    out.writeInt(data.protocol()
                            == DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0 ? 0 : 1);
                    out.writeInt(fingerprint == null ? 0 : fingerprint.length);
                    if (fingerprint != null) out.write(fingerprint);
                }
                ProtocolHelloV1 protocolHello = worker.protocolHello();
                if (protocolHello != null) {
                    byte[] helloFrame = ProtocolHelloV1Codec.encode(protocolHello);
                    out.write(PROTOCOL_MAGIC);
                    out.writeInt(PROTOCOL_VERSION);
                    out.writeInt(helloFrame.length);
                    out.write(helloFrame);
                }
            }
            byte[] encoded = bytes.toByteArray();
            if (encoded.length > MAX_RECORD_BYTES) throw invalid("registry record exceeds 4 KiB");
            return encoded;
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    public static RegistryWorker decode(byte[] encoded, boolean requireSecure) {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_RECORD_BYTES) {
            throw invalid("registry record length is out of bounds");
        }
        Cursor cursor = new Cursor(encoded);
        String workerId = cursor.text(MAX_WORKER_ID_BYTES, "workerId", false);
        String host = cursor.text(MAX_HOST_BYTES, "host", true);
        int port = cursor.integer("port");
        int slots = cursor.integer("slots");
        int dataPort = -1;
        String incarnation = null;
        RpcSecurityMetadata security = null;
        DataPlaneRegistration dataRegistration = null;
        ProtocolHelloV1 protocolHello = null;
        if (cursor.remaining() > 0) {
            dataPort = cursor.integer("dataPort");
            if (cursor.remaining() > 0) {
                incarnation = cursor.nullableText(MAX_INCARNATION_BYTES, "incarnationId");
                if (cursor.startsWith(SECURE_MAGIC)) {
                    if (cursor.remaining() < SECURE_SUFFIX_BYTES) {
                        throw invalid("secure registry suffix is truncated");
                    }
                    if (!Arrays.equals(SECURE_MAGIC, cursor.fixed(SECURE_MAGIC.length, "secureMagic"))) {
                        throw invalid("secure registry magic is invalid");
                    }
                    if (cursor.integer("secureVersion") != SECURE_VERSION) {
                        throw invalid("secure registry version is unsupported");
                    }
                    String deployment = cursor.fixedAscii(64, "deploymentSetId");
                    String authz = cursor.fixedAscii(64, "authzSetId");
                    security = RpcSecurityMetadata.of(deployment, authz);
                }
                if (cursor.startsWith(DATA_MAGIC)) {
                    if (security == null) {
                        throw invalid("data identity requires secure registry identity");
                    }
                    dataRegistration = decodeDataRegistration(cursor);
                }
                if (cursor.startsWith(PROTOCOL_MAGIC)) {
                    protocolHello = decodeProtocolHello(cursor);
                }
            }
        }
        if (cursor.remaining() != 0) throw invalid("registry record has trailing bytes");
        if (requireSecure && security == null) throw invalid("secure registry identity is required");
        return new RegistryWorker(workerId, host, port, slots, dataPort, incarnation,
                security, dataRegistration, protocolHello);
    }

    private static DataPlaneRegistration decodeDataRegistration(Cursor cursor) {
        if (cursor.remaining() < DATA_PREFIX_BYTES) {
            throw invalid("data identity extension is truncated");
        }
        if (!Arrays.equals(DATA_MAGIC, cursor.fixed(DATA_MAGIC.length, "dataMagic"))) {
            throw invalid("data identity magic is invalid");
        }
        if (cursor.integer("dataVersion") != DATA_VERSION) {
            throw invalid("data identity version is unsupported");
        }
        int protocol = cursor.integer("dataProtocol");
        int fingerprintLength = cursor.integer("dataFingerprintLength");
        if (fingerprintLength < 0
                || fingerprintLength > DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES
                || fingerprintLength > cursor.remaining()) {
            throw invalid("data identity fingerprint length is invalid");
        }
        byte[] fingerprint = cursor.fixed(fingerprintLength, "dataFingerprint");
        if (protocol == 0) {
            if (fingerprintLength != 0) {
                throw invalid("loopback data identity must not carry fingerprint");
            }
            return DataPlaneRegistration.loopbackPlainV0();
        }
        if (protocol == 1) {
            if (fingerprintLength != DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES) {
                throw invalid("MTLS data identity fingerprint length is invalid");
            }
            return DataPlaneRegistration.shuffleMtlsV1(fingerprint);
        }
        throw invalid("data identity protocol is unsupported");
    }

    private static ProtocolHelloV1 decodeProtocolHello(Cursor cursor) {
        if (cursor.remaining() < PROTOCOL_PREFIX_BYTES) {
            throw invalid("protocol hello extension is truncated");
        }
        if (!Arrays.equals(PROTOCOL_MAGIC,
                cursor.fixed(PROTOCOL_MAGIC.length, "protocolMagic"))) {
            throw invalid("protocol hello magic is invalid");
        }
        if (cursor.integer("protocolVersion") != PROTOCOL_VERSION) {
            throw invalid("protocol hello version is unsupported");
        }
        int length = cursor.integer("protocolHelloLength");
        if (length <= 0 || length > ProtocolHelloV1Codec.MAX_FRAME_BYTES
                || length > cursor.remaining()) {
            throw invalid("protocol hello length is invalid");
        }
        try {
            return ProtocolHelloV1Codec.decode(
                    cursor.fixed(length, "protocolHello"));
        } catch (IllegalArgumentException malformed) {
            throw invalid("protocol hello frame is invalid");
        }
    }

    private static byte[] utf8(String value, int max, String field) {
        if (value == null) throw invalid(field + " is required");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > max) throw invalid(field + " exceeds byte limit");
        return bytes;
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("INVALID_REGISTRY_WORKER: " + message);
    }

    private static final class Cursor {
        private final byte[] bytes;
        private int offset;

        Cursor(byte[] bytes) { this.bytes = bytes; }
        int remaining() { return bytes.length - offset; }

        boolean startsWith(byte[] prefix) {
            if (prefix == null || remaining() < prefix.length) return false;
            for (int index = 0; index < prefix.length; index++) {
                if (bytes[offset + index] != prefix[index]) return false;
            }
            return true;
        }

        int integer(String field) {
            if (remaining() < 4) throw invalid(field + " is truncated");
            int value = ByteBuffer.wrap(bytes, offset, 4).getInt();
            offset += 4;
            return value;
        }

        String text(int max, String field, boolean emptyAllowed) {
            int length = integer(field + "Length");
            if (length < 0 || length > max || (!emptyAllowed && length == 0)
                    || length > remaining()) {
                throw invalid(field + " length is invalid");
            }
            return decodeText(fixed(length, field), field);
        }

        String nullableText(int max, String field) {
            int length = integer(field + "Length");
            if (length == -1) return null;
            if (length < 0 || length > max || length > remaining()) {
                throw invalid(field + " length is invalid");
            }
            return decodeText(fixed(length, field), field);
        }

        byte[] fixed(int length, String field) {
            if (length < 0 || remaining() < length) throw invalid(field + " is truncated");
            byte[] result = Arrays.copyOfRange(bytes, offset, offset + length);
            offset += length;
            return result;
        }

        String fixedAscii(int length, String field) {
            byte[] value = fixed(length, field);
            for (byte current : value) {
                if (!((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f'))) {
                    throw invalid(field + " is not lowercase hex");
                }
            }
            return new String(value, StandardCharsets.US_ASCII);
        }

        private static String decodeText(byte[] value, String field) {
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(value)).toString();
            } catch (CharacterCodingException failure) {
                throw invalid(field + " is malformed UTF-8");
            }
        }
    }
}
