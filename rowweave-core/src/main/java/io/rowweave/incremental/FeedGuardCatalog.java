package io.rowweave.incremental;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/** Fixed 256x16 persistent-incarnation guard model shared by authority backends. */
final class FeedGuardCatalog {
    static final int MAX_ENTRIES_PER_SHARD = 16;
    static final int MAX_SHARD_FRAME_BYTES = 1024;

    private final Shard[] shards = new Shard[FeedGuardShardObservation.SHARD_COUNT];

    FeedGuardCatalog() {
        for (int i = 0; i < shards.length; i++) shards[i] = new Shard();
    }

    static int shardIndex(String feedId) {
        byte[] digest = feedDigest(feedId);
        return digest[0] & 0xff;
    }

    long nextIncarnation(String feedId) {
        Long current = shard(feedId).entries.get(feedHash(feedId));
        if (current == null) return 1L;
        try {
            return Math.addExact(current, 1L);
        } catch (ArithmeticException exhausted) {
            throw new IllegalStateException("feed incarnation is exhausted", exhausted);
        }
    }

    boolean hasCapacity(String feedId) {
        Shard shard = shard(feedId);
        return shard.entries.containsKey(feedHash(feedId))
                || shard.entries.size() < MAX_ENTRIES_PER_SHARD;
    }

    FeedGuardShardObservation initialize(String feedId, long incarnation) {
        Shard shard = shard(feedId);
        String hash = feedHash(feedId);
        if (!hasCapacity(feedId) || incarnation != nextIncarnation(feedId)) {
            throw new IllegalArgumentException("feed guard initialization is invalid");
        }
        shard.entries.put(hash, incarnation);
        shard.revision++;
        return observation(feedId);
    }

    FeedGuardShardObservation observation(String feedId) {
        int index = shardIndex(feedId);
        Shard shard = shards[index];
        if (!shard.entries.containsKey(feedHash(feedId))) return null;
        byte[] frame = encode(index, shard);
        return new FeedGuardShardObservation(
                index, shard.revision, IncrementalAuthorityValue.sha256(frame));
    }

    int entryCount() {
        int count = 0;
        for (Shard shard : shards) count += shard.entries.size();
        return count;
    }

    int shardEntryCount(int index) {
        requireIndex(index);
        return shards[index].entries.size();
    }

    int shardFrameBytes(int index) {
        requireIndex(index);
        return encode(index, shards[index]).length;
    }

    private Shard shard(String feedId) {
        IncrementalAuthorityValue.text(feedId, "feed id");
        return shards[shardIndex(feedId)];
    }

    private static byte[] encode(int index, Shard shard) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bodyBytes);
            out.writeInt(0x41464731); // AFG1
            out.writeByte(1);
            out.writeShort(index);
            out.writeLong(shard.revision);
            out.writeByte(shard.entries.size());
            for (Map.Entry<String, Long> entry : shard.entries.entrySet()) {
                out.write(hexBytes(entry.getKey()));
                out.writeLong(entry.getValue());
            }
            out.flush();
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frame = new ByteArrayOutputStream(body.length + 32);
            frame.write(body);
            frame.write(IncrementalAuthorityValue.sha256(body));
            byte[] encoded = frame.toByteArray();
            if (encoded.length > MAX_SHARD_FRAME_BYTES) {
                throw new IllegalStateException("feed guard shard exceeds frame bound");
            }
            return encoded;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory feed guard encoding failed", impossible);
        }
    }

    private static byte[] feedDigest(String feedId) {
        return IncrementalAuthorityValue.sha256(
                IncrementalAuthorityValue.text(feedId, "feed id")
                        .getBytes(StandardCharsets.UTF_8));
    }

    private static String feedHash(String feedId) {
        byte[] digest = feedDigest(feedId);
        char[] chars = new char[digest.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < digest.length; i++) {
            int value = digest[i] & 0xff;
            chars[i * 2] = digits[value >>> 4];
            chars[i * 2 + 1] = digits[value & 0xf];
        }
        return new String(chars);
    }

    private static byte[] hexBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int high = Character.digit(value.charAt(i * 2), 16);
            int low = Character.digit(value.charAt(i * 2 + 1), 16);
            bytes[i] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static void requireIndex(int index) {
        if (index < 0 || index >= FeedGuardShardObservation.SHARD_COUNT) {
            throw new IllegalArgumentException("feed guard shard index is invalid");
        }
    }

    private static final class Shard {
        private long revision;
        private final TreeMap<String, Long> entries = new TreeMap<>();
    }
}
