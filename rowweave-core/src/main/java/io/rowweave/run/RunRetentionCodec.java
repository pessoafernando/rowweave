package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Bounded canonical codec for a lifecycle reference and a retention token. */
public final class RunRetentionCodec {
    private static final int TOKEN_MAGIC = 0x52575254; // RWRT
    private static final int REF_MAGIC = 0x5257524c;   // RWRL
    private static final byte VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TOKEN_BYTES = 4 + 1 + 2 + RunText.MAX_ID_BYTES
            + 2 + RunText.MAX_ID_BYTES + 8 * 4 + DIGEST_BYTES * 2;
    private static final int MAX_REF_BYTES = 4 + 1 + 1 + 2 + RunText.MAX_ID_BYTES
            + 8 + 1 + 2 + RunText.MAX_ID_BYTES + 8 + 1 + 4
            + MAX_TOKEN_BYTES + DIGEST_BYTES;

    private RunRetentionCodec() { }

    public static byte[] encodeToken(RetentionToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        byte[] runId = utf8(token.runId());
        byte[] ownerId = utf8(token.owner().ownerId());
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(TOKEN_MAGIC);
            out.writeByte(VERSION);
            writeText(out, runId);
            writeText(out, ownerId);
            out.writeLong(token.owner().leaderTermToken());
            out.writeLong(token.ownershipEpoch());
            out.writeLong(token.terminalRevision());
            out.writeLong(token.terminalAtMillis());
            out.write(token.tokenDigest());
            out.flush();
            return appendChecksum(raw.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory token encoding failed", impossible);
        }
    }

    public static RetentionToken decodeToken(byte[] encoded) {
        ByteBuffer in = verifiedBody(encoded, MAX_TOKEN_BYTES, "retention token");
        if (in.getInt() != TOKEN_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported retention token envelope");
        }
        String runId = readText(in, "runId");
        String ownerId = readText(in, "ownerId");
        requireRemaining(in, 8 * 4 + DIGEST_BYTES, "retention token fields");
        long term = in.getLong();
        long epoch = in.getLong();
        long revision = in.getLong();
        long terminalAt = in.getLong();
        byte[] digest = new byte[DIGEST_BYTES];
        in.get(digest);
        requireConsumed(in, "retention token");
        return RetentionToken.decoded(runId, new RetentionOwner(ownerId, term),
                epoch, revision, terminalAt, digest);
    }

    public static byte[] encodeRef(RunLifecycleRef ref) {
        if (ref == null) throw new IllegalArgumentException("ref is required");
        byte[] runId = utf8(ref.runId());
        byte[] artifactId = utf8(ref.artifactId());
        byte[] token = ref.retentionToken() == null
                ? null : encodeToken(ref.retentionToken());
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(REF_MAGIC);
            out.writeByte(VERSION);
            out.writeByte(ref.state().ordinal());
            writeText(out, runId);
            out.writeLong(ref.revision());
            out.writeByte(ref.status().ordinal());
            writeText(out, artifactId);
            out.writeLong(ref.terminalAtMillis());
            out.writeByte(token == null ? 0 : 1);
            out.writeInt(token == null ? 0 : token.length);
            if (token != null) out.write(token);
            out.flush();
            return appendChecksum(raw.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory lifecycle ref encoding failed", impossible);
        }
    }

    public static RunLifecycleRef decodeRef(byte[] encoded) {
        ByteBuffer in = verifiedBody(encoded, MAX_REF_BYTES, "lifecycle ref");
        if (in.getInt() != REF_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported lifecycle ref envelope");
        }
        int stateOrdinal = in.get() & 0xff;
        RunLifecycleState[] states = RunLifecycleState.values();
        if (stateOrdinal >= states.length) {
            throw new IllegalArgumentException("invalid lifecycle state");
        }
        String runId = readText(in, "runId");
        requireRemaining(in, 8 + 1 + 2 + 8 + 1 + 4, "lifecycle ref fields");
        long revision = in.getLong();
        int statusOrdinal = in.get() & 0xff;
        RunStatus[] statuses = RunStatus.values();
        if (statusOrdinal >= statuses.length) {
            throw new IllegalArgumentException("invalid Run status");
        }
        String artifactId = readText(in, "artifactId");
        requireRemaining(in, 8 + 1 + 4, "lifecycle terminal fields");
        long terminalAt = in.getLong();
        int hasToken = in.get() & 0xff;
        int tokenLength = in.getInt();
        if ((hasToken != 0 && hasToken != 1)
                || tokenLength < 0 || tokenLength > MAX_TOKEN_BYTES
                || (hasToken == 0) != (tokenLength == 0)
                || in.remaining() != tokenLength) {
            throw new IllegalArgumentException("invalid lifecycle token length");
        }
        RetentionToken token = null;
        if (tokenLength > 0) {
            byte[] tokenBytes = new byte[tokenLength];
            in.get(tokenBytes);
            token = decodeToken(tokenBytes);
        }
        RunLifecycleState state = states[stateOrdinal];
        RunStatus status = statuses[statusOrdinal];
        if (state == RunLifecycleState.ACTIVE) {
            return RunLifecycleRef.active(runId, revision, status, artifactId);
        }
        if (state == RunLifecycleState.TERMINAL) {
            return RunLifecycleRef.terminal(
                    runId, revision, status, artifactId, terminalAt);
        }
        return RunLifecycleRef.deleting(
                runId, revision, status, artifactId, terminalAt, token);
    }

    static byte[] digestTokenFields(String runId, RetentionOwner owner,
                                    long epoch, long revision, long terminalAtMillis) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (owner == null) throw new IllegalArgumentException("owner is required");
        byte[] run = utf8(runId);
        byte[] ownerId = utf8(owner.ownerId());
        ByteBuffer fields = ByteBuffer.allocate(2 + run.length + 2 + ownerId.length + 8 * 4);
        fields.putShort((short) run.length).put(run)
                .putShort((short) ownerId.length).put(ownerId)
                .putLong(owner.leaderTermToken()).putLong(epoch)
                .putLong(revision).putLong(terminalAtMillis);
        return sha256(fields.array());
    }

    private static ByteBuffer verifiedBody(byte[] encoded, int maximum, String kind) {
        if (encoded == null || encoded.length < 4 + 1 + DIGEST_BYTES
                || encoded.length > maximum) {
            throw new IllegalArgumentException("invalid " + kind + " length");
        }
        int bodyLength = encoded.length - DIGEST_BYTES;
        byte[] body = Arrays.copyOf(encoded, bodyLength);
        byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
        if (!MessageDigest.isEqual(sha256(body), checksum)) {
            throw new IllegalArgumentException(kind + " checksum mismatch");
        }
        return ByteBuffer.wrap(body);
    }

    private static byte[] appendChecksum(byte[] body) {
        byte[] encoded = Arrays.copyOf(body, body.length + DIGEST_BYTES);
        System.arraycopy(sha256(body), 0, encoded, body.length, DIGEST_BYTES);
        return encoded;
    }

    private static void writeText(DataOutputStream out, byte[] text) throws IOException {
        if (text.length < 1 || text.length > RunText.MAX_ID_BYTES) {
            throw new IllegalArgumentException("text length is out of bounds");
        }
        out.writeShort(text.length);
        out.write(text);
    }

    private static String readText(ByteBuffer in, String field) {
        requireRemaining(in, 2, field + " length");
        int length = in.getShort() & 0xffff;
        if (length < 1 || length > RunText.MAX_ID_BYTES || in.remaining() < length) {
            throw new IllegalArgumentException("invalid " + field + " length");
        }
        byte[] bytes = new byte[length];
        in.get(bytes);
        return RunText.decode(bytes, field, RunText.MAX_ID_BYTES);
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void requireRemaining(ByteBuffer in, int minimum, String field) {
        if (in.remaining() < minimum) {
            throw new IllegalArgumentException("truncated " + field);
        }
    }

    private static void requireConsumed(ByteBuffer in, String kind) {
        if (in.hasRemaining()) throw new IllegalArgumentException("trailing " + kind + " bytes");
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
