package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical bounded wire for {@link PreparedSinkLocator}. */
public final class PreparedSinkLocatorCodec {

    public static final int MAX_FRAME_BYTES = 16 * 1024;
    private static final int MAGIC = 0x5257504c; // RWPL
    private static final int LEGACY_CODEC_VERSION = 1;
    private static final int CURRENT_CODEC_VERSION = 2;
    private static final int DIGEST_BYTES = 32;
    private static final int MIN_FRAME_BYTES = 4 + 4 + 4 + 4 + 180 + 1
            + 4 + 1 + 4 + 1 + 8 + DIGEST_BYTES + 8 + DIGEST_BYTES;

    private PreparedSinkLocatorCodec() {
    }

    public static byte[] encode(PreparedSinkLocator locator) {
        if (locator == null) {
            throw new IllegalArgumentException("prepared sink locator is required");
        }
        byte[] identity = SinkAttemptIdentityCodec.encode(locator.attemptIdentity());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                identity.length + locator.payload().length + 256);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(locator.locatorVersion()
                    == PreparedSinkLocator.LEGACY_MANIFEST_VERSION
                    ? LEGACY_CODEC_VERSION : CURRENT_CODEC_VERSION);
            out.writeInt(locator.locatorVersion());
            SinkAttemptIdentityCodec.writeBytes(out, identity);
            out.writeByte(SinkAttemptIdentityCodec.profileId(
                    locator.capabilityProfile()));
            RunWire.writeText(out, locator.commitDomainId());
            if (locator.locatorVersion() == PreparedSinkLocator.CURRENT_VERSION) {
                writeProviderKey(out, locator.providerKey());
                writeCodec(out, locator.locatorCodec());
            }
            SinkAttemptIdentityCodec.writeBytes(out, locator.payload());
            if (locator.locatorVersion() == PreparedSinkLocator.CURRENT_VERSION) {
                out.write(locator.payloadDigest());
            }
            out.writeLong(locator.dataLength());
            out.write(locator.dataHash());
            out.writeLong(locator.preparedAtMillis());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] body = bytes.toByteArray();
        if (body.length > MAX_FRAME_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("prepared sink locator frame length exceeds limit");
        }
        byte[] checksum = SinkAttemptIdentityCodec.sha256(body, 0, body.length);
        byte[] frame = Arrays.copyOf(body, body.length + checksum.length);
        System.arraycopy(checksum, 0, frame, body.length, checksum.length);
        return frame;
    }

    public static PreparedSinkLocator decode(byte[] frame) {
        if (frame == null || frame.length < MIN_FRAME_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("prepared sink locator frame length out of bounds");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        byte[] expected = SinkAttemptIdentityCodec.sha256(frame, 0, bodyLength);
        if (!MessageDigest.isEqual(expected,
                Arrays.copyOfRange(frame, bodyLength, frame.length))) {
            throw new IllegalArgumentException(
                    "prepared sink locator frame length or checksum mismatch");
        }
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(frame, 0, bodyLength))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("prepared sink locator magic mismatch");
            }
            int codecVersion = in.readInt();
            if (codecVersion != LEGACY_CODEC_VERSION
                    && codecVersion != CURRENT_CODEC_VERSION) {
                throw new IllegalArgumentException(
                        "prepared sink locator codec version is unsupported: " + codecVersion);
            }
            int locatorVersion = in.readInt();
            if (codecVersion != locatorVersion) {
                throw new IllegalArgumentException(
                        "prepared sink locator codec/schema version mismatch");
            }
            SinkAttemptIdentity identity = SinkAttemptIdentityCodec.decode(
                    SinkAttemptIdentityCodec.readBytes(
                            in, "attemptIdentity",
                            SinkAttemptIdentityCodec.MAX_FRAME_BYTES));
            io.rowweave.BatchSinkDescriptor.CapabilityProfile profile =
                    SinkAttemptIdentityCodec.profile(in.readUnsignedByte());
            String domain = SinkAttemptIdentityCodec.readText(
                    in, "commitDomainId",
                    io.rowweave.BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES);
            DurableSinkProviderKey providerKey = codecVersion == LEGACY_CODEC_VERSION
                    ? new DurableSinkProviderKey(
                    "rowweave.manifest-file", 1,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1)
                    : readProviderKey(in);
            DurableSinkCodecDescriptor locatorCodec =
                    codecVersion == LEGACY_CODEC_VERSION
                    ? new DurableSinkCodecDescriptor(
                    "rowweave.manifest-member", 1,
                    PreparedSinkLocator.MAX_PAYLOAD_BYTES)
                    : readCodec(in);
            byte[] payload = SinkAttemptIdentityCodec.readBytes(
                    in, "payload", PreparedSinkLocator.MAX_PAYLOAD_BYTES);
            byte[] payloadDigest = codecVersion == LEGACY_CODEC_VERSION
                    ? SinkAttemptIdentityCodec.sha256(payload, 0, payload.length)
                    : SinkAttemptIdentityCodec.readFixed(
                    in, DIGEST_BYTES, "payloadDigest");
            long dataLength = in.readLong();
            byte[] dataHash = SinkAttemptIdentityCodec.readFixed(
                    in, DIGEST_BYTES, "dataHash");
            long preparedAtMillis = in.readLong();
            if (in.available() != 0) {
                throw new IllegalArgumentException(
                        "prepared sink locator has trailing bytes");
            }
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    locatorVersion, identity, profile, domain,
                    providerKey, locatorCodec, payload, payloadDigest,
                    dataLength, dataHash, preparedAtMillis);
            if (!MessageDigest.isEqual(frame, encode(locator))) {
                throw new IllegalArgumentException(
                        "prepared sink locator is not canonical");
            }
            return locator;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "prepared sink locator field length is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "prepared sink locator is malformed", malformed);
        }
    }

    private static void writeProviderKey(
            DataOutputStream out, DurableSinkProviderKey key) throws IOException {
        RunWire.writeText(out, key.connectorType());
        out.writeInt(key.connectorVersion());
        out.writeByte(SinkAttemptIdentityCodec.profileId(
                key.capabilityProfile()));
    }

    private static DurableSinkProviderKey readProviderKey(
            DataInputStream in) throws IOException {
        String connectorType = SinkAttemptIdentityCodec.readText(
                in, "provider.connectorType",
                BatchSinkDescriptor.MAX_CONNECTOR_TYPE_BYTES);
        int connectorVersion = in.readInt();
        BatchSinkDescriptor.CapabilityProfile profile =
                SinkAttemptIdentityCodec.profile(in.readUnsignedByte());
        return new DurableSinkProviderKey(
                connectorType, connectorVersion, profile);
    }

    private static void writeCodec(
            DataOutputStream out, DurableSinkCodecDescriptor codec)
            throws IOException {
        RunWire.writeText(out, codec.codecId());
        out.writeInt(codec.codecVersion());
        out.writeInt(codec.maxPayloadBytes());
    }

    private static DurableSinkCodecDescriptor readCodec(
            DataInputStream in) throws IOException {
        String codecId = SinkAttemptIdentityCodec.readText(
                in, "locatorCodec.codecId",
                DurableSinkCodecDescriptor.MAX_CODEC_ID_BYTES);
        return new DurableSinkCodecDescriptor(
                codecId, in.readInt(), in.readInt());
    }
}
