package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Deterministic, bounded checksum envelope for an owner and its token. */
public final class RecoveryOwnershipCodec {
    private static final int MAGIC = 0x5257524f; // RWRO
    private static final int VERSION = 1;
    private static final int OWNER = 1;
    private static final int TOKEN = 2;
    private static final int HEADER_BYTES = 12;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_BODY_BYTES = 1024;
    public static final int MAX_FRAME_BYTES = HEADER_BYTES + MAX_BODY_BYTES + CHECKSUM_BYTES;

    private RecoveryOwnershipCodec() {
    }

    public static byte[] encodeOwner(RecoveryOwner owner) {
        if (owner == null) throw new IllegalArgumentException("owner is required");
        return frame(OWNER, ownerBody(owner));
    }

    public static RecoveryOwner decodeOwner(byte[] encoded) {
        RunWire.Reader body = body(encoded, OWNER);
        RecoveryOwner owner = readOwner(body, "owner");
        body.finish("recovery owner body");
        return owner;
    }

    public static byte[] encodeToken(RecoveryToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(384);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RunWire.writeText(out, token.runId());
            writeOwner(out, token.owner());
            out.writeLong(token.ownershipEpoch());
            out.writeLong(token.claimedRevision());
            out.write(token.tokenDigest());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(TOKEN, bytes.toByteArray());
    }

    public static RecoveryToken decodeToken(byte[] encoded) {
        RunWire.Reader body = body(encoded, TOKEN);
        String runId = body.readText("token.runId", RunText.MAX_ID_BYTES);
        RecoveryOwner owner = readOwner(body, "token.owner");
        long epoch = body.readLong("token.ownershipEpoch");
        long revision = body.readLong("token.claimedRevision");
        byte[] digest = body.readFixed(CHECKSUM_BYTES, "token.digest");
        body.finish("recovery token body");
        return RecoveryToken.decoded(runId, owner, epoch, revision, digest);
    }

    static byte[] digestTokenFields(String runId, RecoveryOwner owner,
                                    long ownershipEpoch, long claimedRevision) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (ownershipEpoch <= 0L) {
            throw new IllegalArgumentException("ownershipEpoch must be > 0");
        }
        if (claimedRevision <= 0L) {
            throw new IllegalArgumentException("claimedRevision must be > 0");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(384);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RunWire.writeText(out, runId);
            writeOwner(out, owner);
            out.writeLong(ownershipEpoch);
            out.writeLong(claimedRevision);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    private static byte[] ownerBody(RecoveryOwner owner) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(280);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeOwner(out, owner);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static void writeOwner(DataOutputStream out, RecoveryOwner owner) throws IOException {
        RunWire.writeText(out, owner.ownerId());
        out.writeLong(owner.leaderTermToken());
    }

    private static RecoveryOwner readOwner(RunWire.Reader body, String path) {
        return new RecoveryOwner(body.readText(path + ".ownerId", RunText.MAX_ID_BYTES),
                body.readLong(path + ".leaderTermToken"));
    }

    private static byte[] frame(int kind, byte[] body) {
        if (body.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("recovery ownership body exceeds limit");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                HEADER_BYTES + body.length + CHECKSUM_BYTES);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeShort(VERSION);
            out.writeShort(kind);
            out.writeInt(body.length);
            out.write(body);
            out.flush();
            out.write(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static RunWire.Reader body(byte[] encoded, int expectedKind) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("recovery ownership frame length out of bounds");
        }
        RunWire.Reader frame = new RunWire.Reader(encoded);
        if (frame.readInt("frame.magic") != MAGIC) {
            throw new IllegalArgumentException("invalid recovery ownership magic");
        }
        int version = (frame.readUnsignedByte("frame.version.high") << 8)
                | frame.readUnsignedByte("frame.version.low");
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported recovery ownership version: " + version);
        }
        int kind = (frame.readUnsignedByte("frame.kind.high") << 8)
                | frame.readUnsignedByte("frame.kind.low");
        if (kind != expectedKind) {
            throw new IllegalArgumentException("unexpected recovery ownership record kind: " + kind);
        }
        int bodyLength = frame.readInt("frame.bodyLength");
        if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                || encoded.length != HEADER_BYTES + bodyLength + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("recovery ownership body length mismatch");
        }
        byte[] body = frame.readFixed(bodyLength, "frame.body");
        byte[] checksum = frame.readFixed(CHECKSUM_BYTES, "frame.checksum");
        frame.finish("recovery ownership frame");
        byte[] expected = sha256(Arrays.copyOf(encoded, encoded.length - CHECKSUM_BYTES));
        if (!MessageDigest.isEqual(expected, checksum)) {
            throw new IllegalArgumentException("recovery ownership checksum mismatch");
        }
        return new RunWire.Reader(body);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
