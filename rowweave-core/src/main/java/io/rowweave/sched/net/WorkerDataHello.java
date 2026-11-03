package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.DataPlaneRegistration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** bounded TLS-internal, pre-FrameCodec Worker hello. */
public final class WorkerDataHello {

    public static final int MAX_FRAME_BYTES = 1024;
    public static final int MAX_STRING_BYTES = 192;
    public static final int PREFIX_BYTES = 13;

    private static final byte[] MAGIC = new byte[] {'A', 'L', 'G', 'X', 'H', 'E', 'L', '1'};
    private static final int VERSION = 1;

    private WorkerDataHello() {
    }

    public static byte[] encode(WorkerDataIdentity identity) {
        if (identity == null) {
            throw new IllegalArgumentException("Worker data identity is required");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(MAX_FRAME_BYTES);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.write(MAGIC);
            output.writeInt(0); // patched after exact bounded encoding
            output.writeByte(protocolId(identity.protocol()));
            output.writeByte(VERSION);
            writeString(output, identity.workerId());
            writeString(output, identity.incarnationId());
            writeString(output, identity.deploymentSetId());
            writeString(output, identity.authzSetId());
            output.write(identity.certificateFingerprint());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] encoded = bytes.toByteArray();
        if (encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Worker hello exceeds 1 KiB limit");
        }
        ByteBuffer.wrap(encoded, MAGIC.length, Integer.BYTES).putInt(encoded.length);
        return encoded;
    }

    public static WorkerDataIdentity decode(byte[] encoded) {
        if (encoded == null || encoded.length < minimumBytes()
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Worker hello length out of bounds");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
            for (byte expected : MAGIC) {
                if (input.readByte() != expected) {
                    throw new IllegalArgumentException("Worker hello magic mismatch");
                }
            }
            int declaredLength = input.readInt();
            if (declaredLength != encoded.length
                    || declaredLength < minimumBytes()
                    || declaredLength > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("Worker hello declared length out of bounds");
            }
            DataPlaneRegistration.Protocol protocol = protocol(input.readUnsignedByte());
            int version = input.readUnsignedByte();
            if (version != VERSION) {
                throw new IllegalArgumentException("unsupported Worker hello version");
            }
            String workerId = readString(input, "worker id");
            String incarnation = readString(input, "worker incarnation");
            String deploymentSet = readString(input, "deployment set id");
            String authzSet = readString(input, "authorization set id");
            byte[] fingerprint = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
            input.readFully(fingerprint);
            if (input.read() != -1) {
                throw new IllegalArgumentException("Worker hello has trailing bytes");
            }
            return WorkerDataIdentity.authenticated(workerId, incarnation, protocol,
                    deploymentSet, authzSet, fingerprint);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException("truncated Worker hello", truncated);
        } catch (IOException failure) {
            throw new IllegalArgumentException("invalid Worker hello", failure);
        }
    }

    static int declaredLength(byte[] prefix) {
        if (prefix == null || prefix.length < PREFIX_BYTES) {
            throw new IllegalArgumentException("Worker hello prefix is incomplete");
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (prefix[i] != MAGIC[i]) {
                throw new IllegalArgumentException("Worker hello magic mismatch");
            }
        }
        int length = ByteBuffer.wrap(prefix, MAGIC.length, Integer.BYTES).getInt();
        if (length < minimumBytes() || length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Worker hello declared length out of bounds");
        }
        return length;
    }

    private static int minimumBytes() {
        return MAGIC.length + Integer.BYTES + 2 + 4 * Short.BYTES
                + 4 + DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES;
    }

    private static int protocolId(DataPlaneRegistration.Protocol protocol) {
        if (protocol != DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
            throw new IllegalArgumentException("Worker hello requires SHUFFLE_MTLS_V1");
        }
        return 1;
    }

    private static DataPlaneRegistration.Protocol protocol(int wireId) {
        if (wireId != 1) {
            throw new IllegalArgumentException("unknown Worker hello data protocol");
        }
        return DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1;
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        if (utf8.length == 0 || utf8.length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("Worker hello string length out of bounds");
        }
        output.writeShort(utf8.length);
        output.write(utf8);
    }

    private static String readString(DataInputStream input, String name) throws IOException {
        int length = input.readUnsignedShort();
        if (length == 0 || length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException(name + " length out of bounds");
        }
        byte[] utf8 = new byte[length];
        input.readFully(utf8);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(utf8)).toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(name + " is not valid UTF-8", invalid);
        }
    }
}
