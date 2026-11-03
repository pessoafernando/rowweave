package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Canonical, bounded ALSC/v1 resource frame carried inside a signed JAR. */
public final class StreamConnectorProviderContractCodec {
    public static final int MAX_FRAME_BYTES = 2048;
    private static final int MAGIC = 0x414c5343; // ALSC
    private static final int VERSION = 1;

    private StreamConnectorProviderContractCodec() { }

    public static byte[] encode(StreamConnectorProviderContract contract) {
        if (contract == null) throw new IllegalArgumentException("connector contract is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeByte(VERSION);
                out.writeByte(contract.kind() ==
                        StreamConnectorProviderContract.Kind.SOURCE ? 1 : 2);
                out.writeByte(contract.sourceReplay() == null ? 0 : 1);
                out.writeByte(contract.sinkVisibility() == null ? 0
                        : contract.sinkVisibility() ==
                        StreamConnectorProviderContract.SinkVisibility
                                .EAGER_AT_LEAST_ONCE ? 1 : 2);
                int flags = (contract.boundedEndOfInput() ? 1 : 0)
                        | (contract.dynamicDiscovery() ? 2 : 0)
                        | (contract.residentCdc() ? 4 : 0)
                        | (contract.externalCheckpointAcknowledgement()
                        ? 8 : 0);
                out.writeByte(flags);
                out.writeInt(contract.maximumConfigurationBytes());
                out.writeInt(contract.maximumConcurrentOpens());
                out.writeLong(contract.openTimeoutMillis());
                writeText(out, contract.deploymentId());
                writeText(out, contract.providerKey());
                writeText(out, contract.configurationCodecId());
            }
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("connector contract exceeds frame limit");
            }
            return result;
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    public static StreamConnectorProviderContract decode(byte[] frame) {
        if (frame == null || frame.length < 32
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("invalid connector contract frame size");
        }
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("unknown connector contract frame");
            }
            int kind = in.readUnsignedByte();
            int replay = in.readUnsignedByte();
            int visibility = in.readUnsignedByte();
            int flags = in.readUnsignedByte();
            if ((flags & ~15) != 0) {
                throw new IllegalArgumentException("unknown connector capability");
            }
            int maxConfig = in.readInt();
            int maxOpen = in.readInt();
            long timeout = in.readLong();
            String deploymentId = readText(in, 128);
            String providerKey = readText(in, 256);
            String codecId = readText(in, 256);
            StreamConnectorProviderContract result;
            if (kind == 1 && replay == 1 && visibility == 0) {
                result = StreamConnectorProviderContract.source(
                        deploymentId, providerKey, codecId, maxConfig,
                        maxOpen, timeout, (flags & 1) != 0,
                        (flags & 2) != 0, (flags & 4) != 0,
                        (flags & 8) != 0);
            } else if (kind == 2 && replay == 0 && visibility >= 1
                    && visibility <= 2 && flags == 0) {
                result = StreamConnectorProviderContract.sink(
                        deploymentId, providerKey, codecId, maxConfig,
                        maxOpen, timeout, visibility == 1
                                ? StreamConnectorProviderContract.SinkVisibility
                                .EAGER_AT_LEAST_ONCE
                                : StreamConnectorProviderContract.SinkVisibility
                                .CHECKPOINT_TRANSACTIONAL);
            } else {
                throw new IllegalArgumentException("invalid connector capability set");
            }
            if (in.available() != 0 || !Arrays.equals(frame, encode(result))) {
                throw new IllegalArgumentException("noncanonical connector contract frame");
            }
            return result;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("invalid connector contract frame", malformed);
        }
    }

    public static String digestHex(StreamConnectorProviderContract contract) {
        return StreamContractSupport.sha256Hex(encode(contract));
    }

    private static void writeText(DataOutputStream out, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream in, int maximumBytes)
            throws IOException {
        int size = in.readUnsignedShort();
        if (size < 1 || size > maximumBytes) {
            throw new IllegalArgumentException("invalid connector text size");
        }
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("invalid connector UTF-8", malformed);
        }
    }
}
