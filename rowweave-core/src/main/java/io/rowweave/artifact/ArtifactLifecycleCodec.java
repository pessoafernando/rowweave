package io.rowweave.artifact;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Deterministic, bounded codec for inventory entries, references and GC tokens. */
public final class ArtifactLifecycleCodec {
    private static final int TOKEN_MAGIC = 0x52574754;     // RWGT
    private static final int INVENTORY_MAGIC = 0x5257494e; // RWIN
    private static final int REFERENCE_MAGIC = 0x52575246; // RWRF
    private static final byte VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TOKEN_BYTES = 4 + 1 + 32 + 2
            + ArtifactLifecycleSupport.MAX_TEXT_BYTES + 8 * 3 + DIGEST_BYTES * 2;
    private static final int MAX_REFERENCE_BYTES = 4 + 1 + 2
            + ArtifactLifecycleSupport.MAX_TEXT_BYTES + 32 + 8 + DIGEST_BYTES;
    static final int MAX_INVENTORY_BYTES = 4 + 1 + 1 + 8 + 4 + 8
            + ArtifactInventorySnapshot.MAX_ENTRIES
            * (1 + 32 + 8 * 3 + 4 + MAX_TOKEN_BYTES) + DIGEST_BYTES;

    private ArtifactLifecycleCodec() { }

    public static byte[] encodeGcToken(ArtifactGcToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        byte[] artifactId = ArtifactLifecycleSupport.hexBytes(token.artifactId());
        byte[] ownerId = ArtifactLifecycleSupport.textBytes(
                token.owner().ownerId(), "ownerId");
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(TOKEN_MAGIC);
            out.writeByte(VERSION);
            out.write(artifactId);
            writeText(out, ownerId);
            out.writeLong(token.owner().leaderTermToken());
            out.writeLong(token.ownershipEpoch());
            out.writeLong(token.inventoryGeneration());
            out.write(token.tokenDigest());
            out.flush();
            return appendChecksum(raw.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory GC token encoding failed", impossible);
        }
    }

    public static ArtifactGcToken decodeGcToken(byte[] encoded) {
        ByteBuffer in = verifiedBody(encoded, MAX_TOKEN_BYTES, "Artifact GC token");
        if (in.getInt() != TOKEN_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported Artifact GC token envelope");
        }
        requireRemaining(in, 32, "artifactId");
        byte[] artifact = new byte[32];
        in.get(artifact);
        String artifactId = ArtifactLifecycleSupport.hex(artifact);
        String ownerId = readText(in, "ownerId");
        requireRemaining(in, 8 * 3 + DIGEST_BYTES, "Artifact GC token fields");
        long term = in.getLong();
        long epoch = in.getLong();
        long generation = in.getLong();
        byte[] digest = new byte[DIGEST_BYTES];
        in.get(digest);
        requireConsumed(in, "Artifact GC token");
        return ArtifactGcToken.decoded(artifactId,
                new ArtifactGcOwner(ownerId, term), epoch, generation, digest);
    }

    public static byte[] encodeInventory(ArtifactInventorySnapshot inventory) {
        if (inventory == null) throw new IllegalArgumentException("inventory is required");
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(INVENTORY_MAGIC);
            out.writeByte(VERSION);
            out.writeByte(inventory.shard());
            out.writeLong(inventory.generation());
            out.writeInt(inventory.entries().size());
            out.writeLong(inventory.storedBytes());
            for (ArtifactInventoryEntry entry : inventory.entries()) {
                out.writeByte(entry.state().ordinal());
                out.write(ArtifactLifecycleSupport.hexBytes(entry.artifactId()));
                out.writeLong(entry.encodedBytes());
                out.writeLong(entry.createdAtMillis());
                out.writeLong(entry.generation());
                byte[] token = entry.gcToken() == null
                        ? null : encodeGcToken(entry.gcToken());
                out.writeInt(token == null ? 0 : token.length);
                if (token != null) out.write(token);
            }
            out.flush();
            return appendChecksum(raw.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory inventory encoding failed", impossible);
        }
    }

    public static ArtifactInventorySnapshot decodeInventory(byte[] encoded) {
        ByteBuffer in = verifiedBody(encoded, MAX_INVENTORY_BYTES, "Artifact inventory");
        if (in.getInt() != INVENTORY_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported Artifact inventory envelope");
        }
        requireRemaining(in, 1 + 8 + 4 + 8, "Artifact inventory header");
        int shard = in.get() & 0xff;
        long generation = in.getLong();
        int count = in.getInt();
        long declaredBytes = in.getLong();
        if (generation < 0L || count < 0
                || count > ArtifactInventorySnapshot.MAX_ENTRIES
                || declaredBytes < 0L) {
            throw new IllegalArgumentException("invalid Artifact inventory header");
        }
        List<ArtifactInventoryEntry> entries = new ArrayList<>(count);
        ArtifactInventoryState[] states = ArtifactInventoryState.values();
        for (int i = 0; i < count; i++) {
            requireRemaining(in, 1 + 32 + 8 * 3 + 4,
                    "Artifact inventory entry");
            int stateOrdinal = in.get() & 0xff;
            if (stateOrdinal >= states.length) {
                throw new IllegalArgumentException("invalid Artifact inventory state");
            }
            byte[] artifact = new byte[32];
            in.get(artifact);
            String artifactId = ArtifactLifecycleSupport.hex(artifact);
            long bytes = in.getLong();
            long createdAt = in.getLong();
            long entryGeneration = in.getLong();
            int tokenLength = in.getInt();
            if (tokenLength < 0 || tokenLength > MAX_TOKEN_BYTES
                    || in.remaining() < tokenLength) {
                throw new IllegalArgumentException("invalid inventory GC token length");
            }
            ArtifactGcToken token = null;
            if (tokenLength > 0) {
                byte[] tokenBytes = new byte[tokenLength];
                in.get(tokenBytes);
                token = decodeGcToken(tokenBytes);
            }
            ArtifactInventoryState state = states[stateOrdinal];
            if (state == ArtifactInventoryState.PUBLISHING) {
                if (token != null) throw new IllegalArgumentException("publishing token present");
                entries.add(ArtifactInventoryEntry.publishing(
                        artifactId, bytes, createdAt, entryGeneration));
            } else if (state == ArtifactInventoryState.LIVE) {
                if (token != null) throw new IllegalArgumentException("live token present");
                entries.add(ArtifactInventoryEntry.live(
                        artifactId, bytes, createdAt, entryGeneration));
            } else {
                entries.add(ArtifactInventoryEntry.deleting(
                        artifactId, bytes, createdAt, entryGeneration, token));
            }
        }
        requireConsumed(in, "Artifact inventory");
        ArtifactInventorySnapshot result = ArtifactInventorySnapshot.of(
                shard, generation, entries);
        if (result.storedBytes() != declaredBytes) {
            throw new IllegalArgumentException("Artifact inventory byte count mismatch");
        }
        return result;
    }

    public static byte[] encodeReference(ArtifactReferenceRef ref) {
        if (ref == null) throw new IllegalArgumentException("ref is required");
        byte[] runId = ArtifactLifecycleSupport.textBytes(ref.runId(), "runId");
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(REFERENCE_MAGIC);
            out.writeByte(VERSION);
            writeText(out, runId);
            out.write(ArtifactLifecycleSupport.hexBytes(ref.artifactId()));
            out.writeLong(ref.lifecycleRevision());
            out.flush();
            return appendChecksum(raw.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Artifact ref encoding failed", impossible);
        }
    }

    public static ArtifactReferenceRef decodeReference(byte[] encoded) {
        ByteBuffer in = verifiedBody(encoded, MAX_REFERENCE_BYTES, "Artifact reference");
        if (in.getInt() != REFERENCE_MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported Artifact reference envelope");
        }
        String runId = readText(in, "runId");
        requireRemaining(in, 32 + 8, "Artifact reference fields");
        byte[] artifact = new byte[32];
        in.get(artifact);
        long revision = in.getLong();
        requireConsumed(in, "Artifact reference");
        return new ArtifactReferenceRef(
                runId, ArtifactLifecycleSupport.hex(artifact), revision);
    }

    static byte[] digestGcTokenFields(String artifactId, ArtifactGcOwner owner,
                                      long epoch, long generation) {
        byte[] artifact = ArtifactLifecycleSupport.hexBytes(artifactId);
        if (owner == null) throw new IllegalArgumentException("owner is required");
        byte[] ownerId = ArtifactLifecycleSupport.textBytes(
                owner.ownerId(), "ownerId");
        ByteBuffer fields = ByteBuffer.allocate(
                artifact.length + 2 + ownerId.length + 8 * 3);
        fields.put(artifact).putShort((short) ownerId.length).put(ownerId)
                .putLong(owner.leaderTermToken()).putLong(epoch).putLong(generation);
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
        if (text.length < 1 || text.length > ArtifactLifecycleSupport.MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("text length is out of bounds");
        }
        out.writeShort(text.length);
        out.write(text);
    }

    private static String readText(ByteBuffer in, String field) {
        requireRemaining(in, 2, field + " length");
        int length = in.getShort() & 0xffff;
        if (length < 1 || length > ArtifactLifecycleSupport.MAX_TEXT_BYTES
                || in.remaining() < length) {
            throw new IllegalArgumentException("invalid " + field + " length");
        }
        byte[] bytes = new byte[length];
        in.get(bytes);
        return ArtifactLifecycleSupport.decodeText(bytes, field);
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
