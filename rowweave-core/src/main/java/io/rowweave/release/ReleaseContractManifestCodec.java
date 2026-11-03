package io.rowweave.release;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Deterministic bounded release contract frame. */
public final class ReleaseContractManifestCodec {
    private static final byte[] MAGIC = "RWXXRLS1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = MAGIC.length + 8;
    public static final int MAX_COMPONENTS = 256;
    public static final int MAX_CAPABILITIES = 256;
    public static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private ReleaseContractManifestCodec() { }

    public static byte[] magic() { return MAGIC.clone(); }

    public static byte[] encode(ReleaseContractManifest manifest) {
        if (manifest == null) {
            throw ReleaseContractText.failure("INVALID_MANIFEST", "release",
                    "release manifest is required");
        }
        byte[] body = encodeBody(manifest);
        int frameLength = HEADER_BYTES + body.length + DIGEST_BYTES;
        if (frameLength > MAX_FRAME_BYTES) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", "release",
                    "release manifest frame exceeds limit");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(frameLength);
            DataOutputStream out = new DataOutputStream(bytes);
            out.write(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(body.length);
            out.write(body);
            out.write(sha256(body));
            out.close();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("release manifest encoding failed", impossible);
        }
    }

    static byte[] encodeBody(ReleaseContractManifest manifest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            writeText(out, manifest.releaseId());
            out.writeInt(manifest.protocolEpoch());
            out.writeInt(manifest.releaseRevision());
            out.writeInt(manifest.minimumReadableRevision());
            out.writeInt(manifest.minimumWritableRevision());
            out.writeInt(manifest.componentContracts().size());
            for (ReleaseComponentContract component : manifest.componentContracts()) {
                writeText(out, component.componentId());
                out.writeInt(component.formatVersion());
                out.writeInt(component.minimumReaderRevision());
                out.writeInt(component.maximumReaderRevision());
                out.writeInt(component.minimumWriterRevision());
                out.writeInt(component.maximumWriterRevision());
                out.writeInt(component.scope().wireId());
                writeText(out, component.contractDigest());
                out.writeBoolean(component.required());
            }
            out.writeInt(manifest.runtimeCapabilities().size());
            for (String capability : manifest.runtimeCapabilities()) {
                writeText(out, capability);
            }
            writeText(out, manifest.publicApiBaselineDigest());
            writeText(out, manifest.decimalSemanticsId());
            out.close();
            byte[] body = bytes.toByteArray();
            if (HEADER_BYTES + body.length + DIGEST_BYTES > MAX_FRAME_BYTES) {
                throw ReleaseContractText.failure("LIMIT_EXCEEDED", "release",
                        "release manifest frame exceeds limit");
            }
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException("release manifest body encoding failed", impossible);
        }
    }

    public static ReleaseContractManifest decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES) {
            throw ReleaseContractText.failure("FRAME_LENGTH_MISMATCH", "release",
                    "release manifest frame is truncated");
        }
        if (frame.length > MAX_FRAME_BYTES) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", "release",
                    "release manifest frame exceeds limit");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            in.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) {
                throw ReleaseContractText.failure("INVALID_MAGIC", "release.magic",
                        "release manifest magic is invalid");
            }
            if (in.readInt() != VERSION) {
                throw ReleaseContractText.failure("UNSUPPORTED_VERSION", "release.version",
                        "release manifest version is unsupported");
            }
            int bodyLength = in.readInt();
            long expected = HEADER_BYTES + (long) bodyLength + DIGEST_BYTES;
            if (bodyLength < 0 || expected != frame.length) {
                throw ReleaseContractText.failure("FRAME_LENGTH_MISMATCH", "release.length",
                        "release manifest length is invalid");
            }
            byte[] actual = sha256(frame, HEADER_BYTES, bodyLength);
            byte[] expectedDigest = Arrays.copyOfRange(
                    frame, HEADER_BYTES + bodyLength, frame.length);
            if (!MessageDigest.isEqual(actual, expectedDigest)) {
                throw ReleaseContractText.failure("CHECKSUM_MISMATCH", "release.checksum",
                        "release manifest checksum mismatch");
            }
            DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(frame, HEADER_BYTES, bodyLength));
            ReleaseContractManifest manifest = decodeBody(body);
            if (body.available() != 0) {
                throw ReleaseContractText.failure("FRAME_LENGTH_MISMATCH", "release.body",
                        "release manifest body has trailing bytes");
            }
            return manifest;
        } catch (ReleaseContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw new ReleaseContractException("FRAME_LENGTH_MISMATCH", "release",
                    "release manifest frame is truncated", truncated);
        } catch (IOException impossible) {
            throw new ReleaseContractException("FRAME_LENGTH_MISMATCH", "release",
                    "release manifest decode failed", impossible);
        }
    }

    private static ReleaseContractManifest decodeBody(DataInputStream in)
            throws IOException {
        String releaseId = readText(in, "releaseId");
        int epoch = in.readInt();
        int revision = in.readInt();
        int minimumReader = in.readInt();
        int minimumWriter = in.readInt();
        int componentCount = readCount(in, "components", MAX_COMPONENTS, false);
        List<ReleaseComponentContract> components = new ArrayList<>(componentCount);
        for (int index = 0; index < componentCount; index++) {
            components.add(new ReleaseComponentContract(
                    readText(in, "components.componentId"), in.readInt(),
                    in.readInt(), in.readInt(), in.readInt(), in.readInt(),
                    ReleaseComponentContract.Scope.fromWireId(in.readInt()),
                    readText(in, "components.contractDigest"), in.readBoolean()));
        }
        int capabilityCount = readCount(
                in, "runtimeCapabilities", MAX_CAPABILITIES, false);
        List<String> capabilities = new ArrayList<>(capabilityCount);
        for (int index = 0; index < capabilityCount; index++) {
            capabilities.add(readText(in, "runtimeCapabilities"));
        }
        return new ReleaseContractManifest(
                releaseId, epoch, revision, minimumReader, minimumWriter,
                components, capabilities,
                readText(in, "publicApiBaselineDigest"),
                readText(in, "decimalSemanticsId"));
    }

    private static void writeText(DataOutputStream out, String value)
            throws IOException {
        byte[] bytes = ReleaseContractText.encode(value, "release.text");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream in, String path)
            throws IOException {
        int length = readCount(in, path, ReleaseContractText.MAX_TEXT_BYTES, true);
        if (length > in.available()) {
            throw ReleaseContractText.failure("FRAME_LENGTH_MISMATCH", path,
                    "release contract text is truncated");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, ReleaseContractText.encode(value, path))) {
            throw ReleaseContractText.failure("MALFORMED_UTF8", path,
                    "release contract text is not canonical UTF-8");
        }
        return value;
    }

    private static int readCount(
            DataInputStream in, String path, int maximum, boolean allowZero)
            throws IOException {
        int count = in.readInt();
        if (count < (allowZero ? 0 : 1) || count > maximum) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", path,
                    "release contract count or length is invalid");
        }
        return count;
    }

    static byte[] sha256(byte[] bytes) {
        return sha256(bytes, 0, bytes.length);
    }

    static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }
}
