package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Identity, cursor, entry and paging kernel reused across backends. */
public final class RunRecoveryCatalogKernel {
    public static final int SHARD_COUNT = 256;
    public static final int MAX_ENTRIES_PER_SHARD = 1024;
    public static final int MAX_PAGE_SIZE = 1000;

    private static final int ENTRY_MAGIC = 0x52575252; // RWRR
    private static final int CURSOR_MAGIC = 0x52575243; // RWRC
    private static final byte VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_CURSOR_CHARS = 128;

    private RunRecoveryCatalogKernel() { }

    public static String digestHex(String runId) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        return RunCommandCanonical.hex(sha256(runId.getBytes(StandardCharsets.UTF_8)));
    }

    public static int shard(String digestHex) {
        validateDigest(digestHex);
        return Integer.parseInt(digestHex.substring(0, 2), 16);
    }

    public static byte[] encodeRef(RecoverableRunRef ref) {
        if (ref == null) throw new IllegalArgumentException("ref is required");
        byte[] runId = ref.runId().getBytes(StandardCharsets.UTF_8);
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream(32 + runId.length);
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(ENTRY_MAGIC);
            out.writeByte(VERSION);
            out.writeShort(runId.length);
            out.write(runId);
            out.writeLong(ref.revision());
            out.writeByte(ref.status().ordinal());
            out.flush();
            byte[] body = raw.toByteArray();
            byte[] encoded = new byte[body.length + CHECKSUM_BYTES];
            System.arraycopy(body, 0, encoded, 0, body.length);
            System.arraycopy(sha256(body), 0, encoded, body.length, CHECKSUM_BYTES);
            return encoded;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory ref encoding failed", impossible);
        }
    }

    public static RecoverableRunRef decodeRef(byte[] encoded) {
        if (encoded == null || encoded.length < 4 + 1 + 2 + 1 + 8 + 1 + CHECKSUM_BYTES
                || encoded.length > 4 + 1 + 2 + RunText.MAX_ID_BYTES + 8 + 1 + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("invalid recoverable run ref length");
        }
        int bodyLength = encoded.length - CHECKSUM_BYTES;
        byte[] body = new byte[bodyLength];
        System.arraycopy(encoded, 0, body, 0, bodyLength);
        byte[] expected = sha256(body);
        byte[] actual = new byte[CHECKSUM_BYTES];
        System.arraycopy(encoded, bodyLength, actual, 0, CHECKSUM_BYTES);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new IllegalArgumentException("recoverable run ref checksum mismatch");
        }
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != ENTRY_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported recoverable run ref envelope");
        }
        int runIdLength = in.getShort() & 0xffff;
        if (runIdLength < 1 || runIdLength > RunText.MAX_ID_BYTES
                || in.remaining() != runIdLength + 8 + 1) {
            throw new IllegalArgumentException("invalid recoverable run ref body");
        }
        byte[] runId = new byte[runIdLength];
        in.get(runId);
        String identity = RunText.decode(runId, "runId", RunText.MAX_ID_BYTES);
        long revision = in.getLong();
        int ordinal = in.get() & 0xff;
        RunStatus[] statuses = RunStatus.values();
        if (ordinal >= statuses.length) throw new IllegalArgumentException("invalid run status");
        return new RecoverableRunRef(identity, revision, statuses[ordinal]);
    }

    public static Cursor decodeCursor(String token) {
        if (token == null) return new Cursor(0L, 0, null);
        if (token.isEmpty() || token.length() > MAX_CURSOR_CHARS) {
            throw new IllegalArgumentException("invalid recovery cursor length");
        }
        final byte[] encoded;
        try {
            encoded = Base64.getUrlDecoder().decode(token);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("invalid recovery cursor", malformed);
        }
        int withoutDigest = 4 + 1 + 8 + 2 + 1 + CHECKSUM_BYTES;
        int withDigest = withoutDigest + 32;
        if (encoded.length != withoutDigest && encoded.length != withDigest) {
            throw new IllegalArgumentException("invalid recovery cursor envelope length");
        }
        int bodyLength = encoded.length - CHECKSUM_BYTES;
        byte[] body = new byte[bodyLength];
        System.arraycopy(encoded, 0, body, 0, bodyLength);
        byte[] checksum = new byte[CHECKSUM_BYTES];
        System.arraycopy(encoded, bodyLength, checksum, 0, CHECKSUM_BYTES);
        if (!MessageDigest.isEqual(sha256(body), checksum)) {
            throw new IllegalArgumentException("recovery cursor checksum mismatch");
        }
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != CURSOR_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported recovery cursor");
        }
        long cycle = in.getLong();
        int shard = in.getShort() & 0xffff;
        int hasAfter = in.get() & 0xff;
        if (cycle < 0L || shard >= SHARD_COUNT || (hasAfter != 0 && hasAfter != 1)
                || in.remaining() != (hasAfter == 1 ? 32 : 0)) {
            throw new IllegalArgumentException("invalid recovery cursor fields");
        }
        String after = null;
        if (hasAfter == 1) {
            byte[] digest = new byte[32];
            in.get(digest);
            after = RunCommandCanonical.hex(digest);
            if (shard(after) != shard) {
                throw new IllegalArgumentException("recovery cursor shard mismatch");
            }
        }
        return new Cursor(cycle, shard, after);
    }

    public static String encodeCursor(long cycle, int shard, String afterDigest) {
        if (cycle < 0L || shard < 0 || shard >= SHARD_COUNT) {
            throw new IllegalArgumentException("invalid recovery cursor position");
        }
        byte[] digest = null;
        if (afterDigest != null) {
            validateDigest(afterDigest);
            if (shard(afterDigest) != shard) {
                throw new IllegalArgumentException("recovery cursor shard mismatch");
            }
            digest = hexBytes(afterDigest);
        }
        ByteBuffer body = ByteBuffer.allocate(4 + 1 + 8 + 2 + 1 + (digest == null ? 0 : 32));
        body.putInt(CURSOR_MAGIC).put(VERSION).putLong(cycle).putShort((short) shard)
                .put((byte) (digest == null ? 0 : 1));
        if (digest != null) body.put(digest);
        byte[] raw = body.array();
        byte[] encoded = new byte[raw.length + CHECKSUM_BYTES];
        System.arraycopy(raw, 0, encoded, 0, raw.length);
        System.arraycopy(sha256(raw), 0, encoded, raw.length, CHECKSUM_BYTES);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(encoded);
    }

    public static RecoverableRunPage page(String cursorToken, int limit,
                                           List<RecoverableRunRef> shardRefs) {
        validateLimit(limit);
        if (shardRefs == null) throw new IllegalArgumentException("shardRefs is required");
        if (shardRefs.size() > MAX_ENTRIES_PER_SHARD) {
            throw new IllegalStateException("recoverable run shard exceeds hard capacity");
        }
        Cursor cursor = decodeCursor(cursorToken);
        List<KeyedRef> sorted = new ArrayList<>(shardRefs.size());
        Set<String> identities = new HashSet<>();
        for (RecoverableRunRef ref : shardRefs) {
            if (ref == null) throw new IllegalStateException("null recoverable run ref");
            String digest = digestHex(ref.runId());
            if (shard(digest) != cursor.shard()) {
                throw new IllegalStateException("recoverable run ref in wrong shard");
            }
            if (!identities.add(digest)) {
                throw new IllegalStateException("duplicate recoverable run identity");
            }
            sorted.add(new KeyedRef(digest, ref));
        }
        sorted.sort(Comparator.comparing(keyed -> keyed.digest));
        List<RecoverableRunRef> page = new ArrayList<>(Math.min(limit, sorted.size()));
        String last = null;
        boolean more = false;
        for (KeyedRef keyed : sorted) {
            if (cursor.afterDigest() != null
                    && keyed.digest.compareTo(cursor.afterDigest()) <= 0) continue;
            if (page.size() == limit) {
                more = true;
                break;
            }
            page.add(keyed.ref);
            last = keyed.digest;
        }
        if (more) {
            return new RecoverableRunPage(page,
                    encodeCursor(cursor.cycle(), cursor.shard(), last), false);
        }
        boolean complete = cursor.shard() == SHARD_COUNT - 1;
        int nextShard = complete ? 0 : cursor.shard() + 1;
        long nextCycle = complete ? cursor.cycle() + 1L : cursor.cycle();
        if (complete && nextCycle < 0L) throw new IllegalStateException("recovery cursor overflow");
        return new RecoverableRunPage(page, encodeCursor(nextCycle, nextShard, null), complete);
    }

    public static void validateLimit(int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("limit must be in [1,1000]");
        }
    }

    private static void validateDigest(String digest) {
        if (digest == null || digest.length() != 64) {
            throw new IllegalArgumentException("invalid SHA-256 digest");
        }
        for (int i = 0; i < digest.length(); i++) {
            char c = digest.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException("invalid SHA-256 digest");
            }
        }
    }

    private static byte[] hexBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public static final class Cursor {
        private final long cycle;
        private final int shard;
        private final String afterDigest;

        private Cursor(long cycle, int shard, String afterDigest) {
            this.cycle = cycle;
            this.shard = shard;
            this.afterDigest = afterDigest;
        }

        public long cycle() { return cycle; }
        public int shard() { return shard; }
        public String afterDigest() { return afterDigest; }
    }

    private static final class KeyedRef {
        final String digest;
        final RecoverableRunRef ref;

        KeyedRef(String digest, RecoverableRunRef ref) {
            this.digest = digest;
            this.ref = ref;
        }
    }
}
