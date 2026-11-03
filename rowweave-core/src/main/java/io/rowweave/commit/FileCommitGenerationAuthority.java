package io.rowweave.commit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.List;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * File generation authority for one host, or for a deployment whose file-lock semantics are proven.
 * The counter moves forward durably first, then the attempt claim is created durably; a crash in between leaves at most a harmless generation hole.
 */
public final class FileCommitGenerationAuthority implements CommitGenerationAuthority {

    private static final int COUNTER_MAGIC = 0x41474331; // AGC1
    private static final int CLAIM_MAGIC = 0x41474332;   // AGC2
    private static final int STRIPES = 64;
    private static final ReentrantLock[] JVM_LOCKS = new ReentrantLock[STRIPES];

    static {
        for (int i = 0; i < JVM_LOCKS.length; i++) {
            JVM_LOCKS[i] = new ReentrantLock();
        }
    }

    @Override
    public long reserve(Path domain, String attemptId) {
        if (domain == null) {
            throw new IllegalArgumentException("domain must not be null");
        }
        byte[] attemptBytes = validateAttempt(attemptId);
        Path root = ManifestDomainIdentity.canonicalPath(domain);
        Path authority = root.resolve("authority");
        Path claims = authority.resolve("claims");
        DurableFileOps.ensureDirectory(claims);
        String attemptHash = hex(CommitManifest.sha256(attemptBytes));
        Path shard = claims.resolve(attemptHash.substring(0, 2));
        DurableFileOps.ensureDirectory(shard);
        Path claim = shard.resolve(attemptHash + ".claim");
        Path lockPath = authority.resolve("authority.lock");
        Path counterPath = authority.resolve("counter");

        ReentrantLock jvmLock = JVM_LOCKS[(root.toString().hashCode() & Integer.MAX_VALUE) % STRIPES];
        jvmLock.lock();
        try (FileChannel lockChannel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = lockChannel.lock()) {
            if (Files.exists(claim)) {
                return decodeClaim(readBounded(claim, 8 * 1024), attemptId);
            }
            long current = Files.exists(counterPath)
                    ? decodeCounter(readBounded(counterPath, 128)) : 0L;
            if (current == Long.MAX_VALUE) {
                throw new IllegalStateException("manifest generation exhausted for domain: " + root);
            }
            long next = current + 1L;
            DurableFileOps.atomicReplace(counterPath, encodeCounter(next));
            try {
                DurableFileOps.atomicCreate(claim, encodeClaim(next, attemptId));
            } catch (java.nio.file.FileAlreadyExistsException raced) {
                return decodeClaim(readBounded(claim, 8 * 1024), attemptId);
            }
            return next;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to reserve manifest generation in: " + root, e);
        } finally {
            jvmLock.unlock();
        }
    }

    @Override
    public Long find(Path domain, String attemptId) {
        if (domain == null) {
            throw new IllegalArgumentException("domain must not be null");
        }
        byte[] attemptBytes = validateAttempt(attemptId);
        Path claim = claimPath(domain, attemptBytes);
        if (!Files.exists(claim, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        return decodeClaim(readBounded(claim, 8 * 1024), attemptId);
    }

    /** After a distributed authority has issued a token, idempotently write the audit claim for the shared file domain. */
    public void recordClaim(Path domain, String attemptId, long generation) {
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        byte[] attemptBytes = validateAttempt(attemptId);
        Path claim = claimPath(domain, attemptBytes);
        DurableFileOps.ensureDirectory(claim.getParent());
        byte[] encoded = encodeClaim(generation, attemptId);
        if (Files.exists(claim, LinkOption.NOFOLLOW_LINKS)) {
            long existing = decodeClaim(readBounded(claim, 8 * 1024), attemptId);
            if (existing != generation) {
                throw new IllegalStateException("attempt claim generation conflict");
            }
            return;
        }
        try {
            DurableFileOps.atomicCreate(claim, encoded);
        } catch (java.nio.file.FileAlreadyExistsException raced) {
            long existing = decodeClaim(readBounded(claim, 8 * 1024), attemptId);
            if (existing != generation) {
                throw new IllegalStateException("attempt claim generation conflict", raced);
            }
        }
    }

    Path claimPath(Path domain, String attemptId) {
        return claimPath(domain, validateAttempt(attemptId));
    }

    private static Path claimPath(Path domain, byte[] attemptBytes) {
        Path root = ManifestDomainIdentity.canonicalPath(domain);
        String attemptHash = hex(CommitManifest.sha256(attemptBytes));
        return root.resolve("authority").resolve("claims")
                .resolve(attemptHash.substring(0, 2)).resolve(attemptHash + ".claim");
    }

    ClaimScan scanClaims(Path domain, int maxClaims, int startShard, ClaimVisitor visitor) {
        if (domain == null || maxClaims <= 0 || startShard < 0 || startShard > 255
                || visitor == null) {
            throw new IllegalArgumentException("invalid bounded claim scan arguments");
        }
        Path claimsRoot = ManifestDomainIdentity.canonicalPath(domain)
                .resolve("authority").resolve("claims");
        if (!Files.isDirectory(claimsRoot, LinkOption.NOFOLLOW_LINKS)) {
            return new ClaimScan(0, false, startShard);
        }
        int examined = 0;
        int remainingBudget = maxClaims;
        int lastShard = startShard;
        for (int offset = 0; offset < 256; offset++) {
            int shardNumber = (startShard + offset) & 0xff;
            lastShard = shardNumber;
            Path shard = claimsRoot.resolve(String.format("%02x", shardNumber));
            if (!Files.exists(shard, LinkOption.NOFOLLOW_LINKS)) {
                continue;
            }
            if (!Files.isDirectory(shard, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("claim shard is not an owned directory: " + shard);
            }
            List<ClaimEntry> shardClaims = new ArrayList<>();
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(shard)) {
                for (Path entry : entries) {
                    String name = entry.getFileName().toString();
                    if (!name.endsWith(".claim")) {
                        throw new IllegalStateException("unexpected claim entry: " + entry);
                    }
                    Claim claim = decodeClaim(readBounded(entry, 8 * 1024));
                    shardClaims.add(new ClaimEntry(entry, claim));
                    if (shardClaims.size() > maxClaims) {
                        throw new IllegalStateException(
                                "claim shard exceeds bounded capacity " + maxClaims + ": " + shard);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed bounded claim scan: " + shard, e);
            }
            // Never truncate in the middle of a shard, otherwise a stable DirectoryStream order would starve the trailing claims forever.
            if (shardClaims.size() > remainingBudget) {
                return new ClaimScan(examined, true, shardNumber);
            }
            for (ClaimEntry entry : shardClaims) {
                visitor.visit(entry.path, entry.claim);
                examined++;
                remainingBudget--;
            }
            if (remainingBudget == 0) {
                return new ClaimScan(examined, true, (shardNumber + 1) & 0xff);
            }
        }
        return new ClaimScan(examined, false, (lastShard + 1) & 0xff);
    }

    private static byte[] validateAttempt(String attemptId) {
        if (attemptId == null || attemptId.isEmpty()) {
            throw new IllegalArgumentException("attemptId must not be empty");
        }
        byte[] bytes = attemptId.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 1024 || attemptId.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("invalid attemptId");
        }
        return bytes;
    }

    private static byte[] encodeCounter(long generation) {
        try {
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(payload)) {
                out.writeInt(COUNTER_MAGIC);
                out.writeLong(generation);
            }
            return withChecksum(payload.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static long decodeCounter(byte[] encoded) {
        byte[] payload = verifiedPayload(encoded, "generation counter");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (in.readInt() != COUNTER_MAGIC) {
                throw new IllegalStateException("generation counter magic mismatch");
            }
            long generation = in.readLong();
            if (generation < 0L || in.available() != 0) {
                throw new IllegalStateException("invalid generation counter");
            }
            return generation;
        } catch (IOException e) {
            throw new IllegalStateException("failed to decode generation counter", e);
        }
    }

    private static byte[] encodeClaim(long generation, String attemptId) {
        try {
            byte[] attempt = attemptId.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(payload)) {
                out.writeInt(CLAIM_MAGIC);
                out.writeLong(generation);
                out.writeInt(attempt.length);
                out.write(attempt);
            }
            return withChecksum(payload.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static long decodeClaim(byte[] encoded, String expectedAttempt) {
        Claim claim = decodeClaim(encoded);
        if (!claim.attemptId.equals(expectedAttempt)) {
            throw new IllegalStateException("attempt hash collision/conflict in generation claim");
        }
        return claim.generation;
    }

    private static Claim decodeClaim(byte[] encoded) {
        byte[] payload = verifiedPayload(encoded, "generation claim");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (in.readInt() != CLAIM_MAGIC) {
                throw new IllegalStateException("generation claim magic mismatch");
            }
            long generation = in.readLong();
            int length = in.readInt();
            if (generation <= 0L || length < 0 || length > 1024) {
                throw new IllegalStateException("invalid generation claim");
            }
            byte[] attempt = new byte[length];
            in.readFully(attempt);
            if (in.available() != 0) {
                throw new IllegalStateException("generation claim has trailing bytes");
            }
            String decoded = new String(attempt, StandardCharsets.UTF_8);
            return new Claim(generation, decoded);
        } catch (EOFException e) {
            throw new IllegalStateException("truncated generation claim", e);
        } catch (IOException e) {
            throw new IllegalStateException("failed to decode generation claim", e);
        }
    }

    interface ClaimVisitor {
        void visit(Path claimPath, Claim claim);
    }

    static final class Claim {
        private final long generation;
        private final String attemptId;

        private Claim(long generation, String attemptId) {
            this.generation = generation;
            this.attemptId = attemptId;
        }

        long generation() {
            return generation;
        }

        String attemptId() {
            return attemptId;
        }
    }

    static final class ClaimScan {
        private final int examined;
        private final boolean truncated;
        private final int nextShard;

        private ClaimScan(int examined, boolean truncated, int nextShard) {
            this.examined = examined;
            this.truncated = truncated;
            this.nextShard = nextShard;
        }

        int examined() { return examined; }
        boolean truncated() { return truncated; }
        int nextShard() { return nextShard; }
    }

    private static final class ClaimEntry {
        private final Path path;
        private final Claim claim;

        private ClaimEntry(Path path, Claim claim) {
            this.path = path;
            this.claim = claim;
        }
    }

    private static byte[] withChecksum(byte[] payload) {
        byte[] result = Arrays.copyOf(payload, payload.length + CommitManifest.SHA256_BYTES);
        System.arraycopy(CommitManifest.sha256(payload), 0, result,
                payload.length, CommitManifest.SHA256_BYTES);
        return result;
    }

    private static byte[] verifiedPayload(byte[] encoded, String type) {
        if (encoded.length <= CommitManifest.SHA256_BYTES) {
            throw new IllegalStateException(type + " is truncated");
        }
        int payloadLength = encoded.length - CommitManifest.SHA256_BYTES;
        byte[] payload = Arrays.copyOf(encoded, payloadLength);
        byte[] checksum = Arrays.copyOfRange(encoded, payloadLength, encoded.length);
        if (!MessageDigest.isEqual(CommitManifest.sha256(payload), checksum)) {
            throw new IllegalStateException(type + " checksum mismatch");
        }
        return payload;
    }

    private static byte[] readBounded(Path path, int maxBytes) {
        try {
            if (Files.isSymbolicLink(path)
                    || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("commit metadata is not an owned regular file: " + path);
            }
            long size = Files.size(path);
            if (size < 0L || size > maxBytes) {
                throw new IllegalStateException("metadata file exceeds limit: " + path + " size=" + size);
            }
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read commit metadata: " + path, e);
        }
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            result[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(result);
    }

}
