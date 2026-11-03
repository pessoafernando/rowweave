package io.rowweave.run;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Identity, cursor and bounded page kernel shared by the terminal lifecycle catalogs. */
public final class RunLifecycleCatalogKernel {
    public static final int SHARD_COUNT = 256;
    public static final int MAX_ENTRIES_PER_SHARD = 4096;
    public static final int MAX_PAGE_SIZE = 1000;

    private static final int CURSOR_MAGIC = 0x52574c43; // RWLC
    private static final byte VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_CURSOR_CHARS = 128;

    private RunLifecycleCatalogKernel() { }

    public static String digestHex(String runId) {
        return RunRecoveryCatalogKernel.digestHex(runId);
    }

    public static int shard(String digestHex) {
        return RunRecoveryCatalogKernel.shard(digestHex);
    }

    public static Cursor decodeCursor(String token) {
        if (token == null) return new Cursor(0L, 0, null);
        if (token.isEmpty() || token.length() > MAX_CURSOR_CHARS) {
            throw new IllegalArgumentException("invalid lifecycle cursor length");
        }
        final byte[] encoded;
        try {
            encoded = Base64.getUrlDecoder().decode(token);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("invalid lifecycle cursor", malformed);
        }
        int withoutDigest = 4 + 1 + 8 + 2 + 1 + CHECKSUM_BYTES;
        int withDigest = withoutDigest + 32;
        if (encoded.length != withoutDigest && encoded.length != withDigest) {
            throw new IllegalArgumentException("invalid lifecycle cursor envelope length");
        }
        int bodyLength = encoded.length - CHECKSUM_BYTES;
        byte[] body = new byte[bodyLength];
        System.arraycopy(encoded, 0, body, 0, bodyLength);
        byte[] checksum = new byte[CHECKSUM_BYTES];
        System.arraycopy(encoded, bodyLength, checksum, 0, CHECKSUM_BYTES);
        if (!MessageDigest.isEqual(sha256(body), checksum)) {
            throw new IllegalArgumentException("lifecycle cursor checksum mismatch");
        }
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != CURSOR_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported lifecycle cursor");
        }
        long cycle = in.getLong();
        int shard = in.getShort() & 0xffff;
        int hasAfter = in.get() & 0xff;
        if (cycle < 0L || shard >= SHARD_COUNT || (hasAfter != 0 && hasAfter != 1)
                || in.remaining() != (hasAfter == 1 ? 32 : 0)) {
            throw new IllegalArgumentException("invalid lifecycle cursor fields");
        }
        String after = null;
        if (hasAfter == 1) {
            byte[] digest = new byte[32];
            in.get(digest);
            after = RunCommandCanonical.hex(digest);
            if (shard(after) != shard) {
                throw new IllegalArgumentException("lifecycle cursor shard mismatch");
            }
        }
        return new Cursor(cycle, shard, after);
    }

    public static String encodeCursor(long cycle, int shard, String afterDigest) {
        if (cycle < 0L || shard < 0 || shard >= SHARD_COUNT) {
            throw new IllegalArgumentException("invalid lifecycle cursor position");
        }
        byte[] digest = null;
        if (afterDigest != null) {
            validateDigest(afterDigest);
            if (shard(afterDigest) != shard) {
                throw new IllegalArgumentException("lifecycle cursor shard mismatch");
            }
            digest = hexBytes(afterDigest);
        }
        ByteBuffer body = ByteBuffer.allocate(4 + 1 + 8 + 2 + 1
                + (digest == null ? 0 : digest.length));
        body.putInt(CURSOR_MAGIC).put(VERSION).putLong(cycle).putShort((short) shard)
                .put((byte) (digest == null ? 0 : 1));
        if (digest != null) body.put(digest);
        byte[] raw = body.array();
        byte[] encoded = new byte[raw.length + CHECKSUM_BYTES];
        System.arraycopy(raw, 0, encoded, 0, raw.length);
        System.arraycopy(sha256(raw), 0, encoded, raw.length, CHECKSUM_BYTES);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(encoded);
    }

    public static RunLifecyclePage page(String cursorToken, int limit,
                                        List<RunLifecycleRef> shardRefs) {
        validateLimit(limit);
        if (shardRefs == null) throw new IllegalArgumentException("shardRefs is required");
        if (shardRefs.size() > MAX_ENTRIES_PER_SHARD) {
            throw new IllegalStateException("lifecycle shard exceeds hard capacity");
        }
        Cursor cursor = decodeCursor(cursorToken);
        List<KeyedRef> sorted = new ArrayList<>(shardRefs.size());
        Set<String> identities = new HashSet<>();
        for (RunLifecycleRef ref : shardRefs) {
            if (ref == null) throw new IllegalStateException("null lifecycle ref");
            if (ref.state() == RunLifecycleState.ACTIVE) {
                throw new IllegalStateException("active ref returned by terminal lifecycle scan");
            }
            String digest = digestHex(ref.runId());
            if (shard(digest) != cursor.shard()) {
                throw new IllegalStateException("lifecycle ref in wrong shard");
            }
            if (!identities.add(digest)) {
                throw new IllegalStateException("duplicate lifecycle identity");
            }
            sorted.add(new KeyedRef(digest, ref));
        }
        sorted.sort(Comparator.comparing(value -> value.digest));
        List<RunLifecycleRef> page = new ArrayList<>(Math.min(limit, sorted.size()));
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
            return new RunLifecyclePage(page,
                    encodeCursor(cursor.cycle(), cursor.shard(), last), false);
        }
        boolean complete = cursor.shard() == SHARD_COUNT - 1;
        int nextShard = complete ? 0 : cursor.shard() + 1;
        long nextCycle = complete ? cursor.cycle() + 1L : cursor.cycle();
        if (complete && nextCycle < 0L) {
            throw new IllegalStateException("lifecycle cursor overflow");
        }
        return new RunLifecyclePage(
                page, encodeCursor(nextCycle, nextShard, null), complete);
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
            char value = digest.charAt(i);
            if (!((value >= '0' && value <= '9')
                    || (value >= 'a' && value <= 'f'))) {
                throw new IllegalArgumentException("invalid SHA-256 digest");
            }
        }
    }

    private static byte[] hexBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(
                    value.substring(i * 2, i * 2 + 2), 16);
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
        private final String digest;
        private final RunLifecycleRef ref;

        private KeyedRef(String digest, RunLifecycleRef ref) {
            this.digest = digest;
            this.ref = ref;
        }
    }
}
