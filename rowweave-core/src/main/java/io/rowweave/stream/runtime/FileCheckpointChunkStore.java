package io.rowweave.stream.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32;

/** Fixed-memory content-addressed chunk store for durable Stream state. */
public final class FileCheckpointChunkStore {
    private static final byte[] MAGIC =
            "RWXXCHK1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_CHUNK_BYTES = 16 * 1024 * 1024;
    private static final int MAX_CHUNKS = 65_536;
    private static final int MAX_MANIFEST_BODY_BYTES = 16 * 1024 * 1024;

    public static final class ChunkHandle {
        private final String digest;
        private final int contentBytes;
        private final long crc32;
        private final String compressionId;
        private final String encryptionId;

        private ChunkHandle(String digest, int contentBytes, long crc32,
                            String compressionId, String encryptionId) {
            requireDigest(digest);
            if (contentBytes < 1 || crc32 < 0L || crc32 > 0xffff_ffffL
                    || !"NONE".equals(compressionId)
                    || !"NONE".equals(encryptionId)) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                        "invalid checkpoint chunk metadata");
            }
            this.digest = digest;
            this.contentBytes = contentBytes;
            this.crc32 = crc32;
            this.compressionId = compressionId;
            this.encryptionId = encryptionId;
        }

        public String digest() { return digest; }
        public int contentBytes() { return contentBytes; }
        public long crc32() { return crc32; }
        public String compressionId() { return compressionId; }
        public String encryptionId() { return encryptionId; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ChunkHandle)) return false;
            ChunkHandle that = (ChunkHandle) other;
            return contentBytes == that.contentBytes && crc32 == that.crc32
                    && digest.equals(that.digest)
                    && compressionId.equals(that.compressionId)
                    && encryptionId.equals(that.encryptionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(digest, contentBytes, crc32,
                    compressionId, encryptionId);
        }
    }

    public static final class StoredChunkedPayload {
        private final String digest;
        private final long contentBytes;
        private final int chunkBytes;
        private final List<ChunkHandle> chunks;

        private StoredChunkedPayload(String digest, long contentBytes,
                                     int chunkBytes, List<ChunkHandle> chunks) {
            requireDigest(digest);
            if (contentBytes < 1L || chunkBytes < 1 || chunks == null
                    || chunks.isEmpty() || chunks.size() > MAX_CHUNKS) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                        "invalid chunked payload metadata");
            }
            long total = 0L;
            ArrayList<ChunkHandle> copy = new ArrayList<>(chunks.size());
            for (ChunkHandle chunk : chunks) {
                if (chunk == null || chunk.contentBytes > chunkBytes) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                            "invalid chunk sequence");
                }
                total = Math.addExact(total, chunk.contentBytes);
                copy.add(chunk);
            }
            if (total != contentBytes) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                        "chunk lengths do not equal payload length");
            }
            this.digest = digest;
            this.contentBytes = contentBytes;
            this.chunkBytes = chunkBytes;
            this.chunks = Collections.unmodifiableList(copy);
        }

        public String digest() { return digest; }
        public long contentBytes() { return contentBytes; }
        public int chunkBytes() { return chunkBytes; }
        public List<ChunkHandle> chunks() { return chunks; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StoredChunkedPayload)) return false;
            StoredChunkedPayload that = (StoredChunkedPayload) other;
            return contentBytes == that.contentBytes
                    && chunkBytes == that.chunkBytes
                    && digest.equals(that.digest)
                    && chunks.equals(that.chunks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(digest, contentBytes, chunkBytes, chunks);
        }
    }

    private final Path chunkDirectory;
    private final Path manifestDirectory;
    private final int chunkBytes;
    private final long maximumPayloadBytes;
    private final int maximumChunks;

    public FileCheckpointChunkStore(Path root, int chunkBytes,
                                    long maximumPayloadBytes,
                                    int maximumChunks) {
        if (root == null || chunkBytes < 1 || chunkBytes > MAX_CHUNK_BYTES
                || maximumPayloadBytes < 1L || maximumChunks < 1
                || maximumChunks > MAX_CHUNKS) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "valid chunk store root and bounds are required");
        }
        Path normalized = root.toAbsolutePath().normalize();
        this.chunkDirectory = normalized.resolve("chunks");
        this.manifestDirectory = normalized.resolve("manifests");
        this.chunkBytes = chunkBytes;
        this.maximumPayloadBytes = maximumPayloadBytes;
        this.maximumChunks = maximumChunks;
        ensureDirectory(normalized);
        ensureDirectory(chunkDirectory);
        ensureDirectory(manifestDirectory);
    }

    public StoredChunkedPayload store(Path staged) {
        requireRegularFile(staged, StreamCheckpointException.Code.CORRUPT_PAYLOAD);
        try {
            long expectedBytes = Files.size(staged);
            long expectedChunks = expectedBytes == 0L ? 0L
                    : (expectedBytes - 1L) / chunkBytes + 1L;
            if (expectedBytes < 1L || expectedBytes > maximumPayloadBytes
                    || expectedChunks > maximumChunks) {
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "checkpoint payload exceeds chunk store bounds");
            }
            ArrayList<ChunkHandle> chunks = new ArrayList<>((int) expectedChunks);
            MessageDigest payloadDigest = sha256Digest();
            byte[] buffer = new byte[chunkBytes];
            long copied = 0L;
            try (InputStream input = Files.newInputStream(staged)) {
                while (copied < expectedBytes) {
                    int wanted = (int) Math.min(chunkBytes,
                            expectedBytes - copied);
                    int count = readFully(input, buffer, wanted);
                    if (count != wanted) {
                        throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                                "checkpoint payload changed while chunking");
                    }
                    payloadDigest.update(buffer, 0, count);
                    ChunkHandle handle = handle(buffer, count);
                    publishChunk(handle, buffer, count);
                    chunks.add(handle);
                    copied += count;
                }
                if (input.read() != -1) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                            "checkpoint payload grew while chunking");
                }
            }
            StoredChunkedPayload proposed = new StoredChunkedPayload(
                    hex(payloadDigest.digest()), copied, chunkBytes, chunks);
            Path path = manifestPath(proposed.digest);
            StoredChunkedPayload existing = readManifest(path,
                    proposed.digest);
            if (existing != null) return requireExact(existing, proposed);
            publishImmutable(path, encodeManifest(proposed),
                    ".rowweave-stream-chunk-manifest-");
            return requireExact(readManifest(path, proposed.digest), proposed);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint chunk publication failed", ioFailure);
        }
    }

    public StoredChunkedPayload get(String digest) {
        requireDigest(digest);
        return readManifest(manifestPath(digest), digest);
    }

    public void verifyStored(StoredChunkedPayload payload) {
        if (payload == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "chunked payload is required");
        }
        StoredChunkedPayload durable = readManifest(
                manifestPath(payload.digest), payload.digest);
        requireExact(durable, payload);
        if (payload.contentBytes > maximumPayloadBytes
                || payload.chunkBytes != chunkBytes
                || payload.chunks.size() > maximumChunks) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "chunked payload exceeds verification bounds");
        }
        try {
            MessageDigest payloadDigest = sha256Digest();
            long verified = 0L;
            byte[] buffer = new byte[Math.min(chunkBytes, 64 * 1024)];
            OutputStream discard = new OutputStream() {
                @Override public void write(int ignored) { }
                @Override public void write(
                        byte[] ignored, int offset, int length) { }
            };
            for (ChunkHandle chunk : payload.chunks) {
                verified += copyVerifiedChunk(
                        chunk, discard, payloadDigest, buffer);
                if (verified > maximumPayloadBytes) {
                    throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                            "verified checkpoint payload exceeds its bound");
                }
            }
            if (verified != payload.contentBytes
                    || !payload.digest.equals(hex(payloadDigest.digest()))) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                        "verified checkpoint payload digest drifted");
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint chunk verification failed", ioFailure);
        }
    }

    public void restore(StoredChunkedPayload payload, Path target) {
        if (payload == null || target == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "chunked payload and restore target are required");
        }
        StoredChunkedPayload durable = readManifest(
                manifestPath(payload.digest), payload.digest);
        requireExact(durable, payload);
        if (payload.contentBytes > maximumPayloadBytes
                || payload.chunkBytes != chunkBytes
                || payload.chunks.size() > maximumChunks) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "chunked payload exceeds restore bounds");
        }
        Path absoluteTarget = target.toAbsolutePath().normalize();
        Path parent = absoluteTarget.getParent();
        if (parent == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "restore target requires a parent directory");
        }
        ensureDirectory(parent);
        Path temporary = parent.resolve(
                ".rowweave-stream-chunk-restore-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            MessageDigest payloadDigest = sha256Digest();
            long restored = 0L;
            byte[] buffer = new byte[Math.min(chunkBytes, 64 * 1024)];
            try (OutputStream output = Files.newOutputStream(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                for (ChunkHandle chunk : payload.chunks) {
                    restored += copyVerifiedChunk(
                            chunk, output, payloadDigest, buffer);
                    if (restored > maximumPayloadBytes) {
                        throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                                "restored checkpoint payload exceeds its bound");
                    }
                }
            }
            if (restored != payload.contentBytes
                    || !payload.digest.equals(hex(payloadDigest.digest()))) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                        "restored checkpoint payload digest drifted");
            }
            forceFile(temporary);
            Files.move(temporary, absoluteTarget,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(parent);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "atomic chunked checkpoint restore failed", ioFailure);
        } finally {
            cleanup(temporary, primary);
        }
    }

    private void publishChunk(ChunkHandle handle, byte[] content, int length)
            throws IOException {
        Path target = chunkPath(handle);
        if (Files.exists(target)) {
            verifyChunkFile(target, handle);
            return;
        }
        Path temporary = chunkDirectory.resolve(
                ".rowweave-stream-chunk-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(content, 0, length);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try {
                Files.createLink(target, temporary);
                forceDirectory(chunkDirectory);
            } catch (FileAlreadyExistsException collision) {
                primary = collision;
            }
            verifyChunkFile(target, handle);
        } finally {
            cleanup(temporary, primary);
        }
    }

    private long copyVerifiedChunk(
            ChunkHandle chunk, OutputStream output,
            MessageDigest payloadDigest, byte[] buffer) throws IOException {
        Path path = chunkPath(chunk);
        requireRegularFile(path, StreamCheckpointException.Code.CORRUPT_CHUNK);
        if (Files.size(path) != chunk.contentBytes) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                    "checkpoint chunk length drifted");
        }
        MessageDigest digest = sha256Digest();
        CRC32 crc = new CRC32();
        int copied = 0;
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read == 0) continue;
                copied += read;
                if (copied > chunk.contentBytes) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                            "checkpoint chunk grew while restoring");
                }
                digest.update(buffer, 0, read);
                payloadDigest.update(buffer, 0, read);
                crc.update(buffer, 0, read);
                output.write(buffer, 0, read);
            }
        }
        if (copied != chunk.contentBytes
                || crc.getValue() != chunk.crc32
                || !chunk.digest.equals(hex(digest.digest()))) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                    "checkpoint chunk checksum or digest drifted");
        }
        return copied;
    }

    private static ChunkHandle handle(byte[] content, int length) {
        MessageDigest digest = sha256Digest();
        digest.update(content, 0, length);
        CRC32 crc = new CRC32();
        crc.update(content, 0, length);
        return new ChunkHandle(hex(digest.digest()), length,
                crc.getValue(), "NONE", "NONE");
    }

    private static int readFully(InputStream input, byte[] buffer, int wanted)
            throws IOException {
        int count = 0;
        while (count < wanted) {
            int read = input.read(buffer, count, wanted - count);
            if (read < 0) break;
            if (read == 0) continue;
            count += read;
        }
        return count;
    }

    private static void verifyChunkFile(Path path, ChunkHandle expected)
            throws IOException {
        requireRegularFile(path, StreamCheckpointException.Code.CORRUPT_CHUNK);
        if (Files.size(path) != expected.contentBytes) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                    "checkpoint chunk length drifted");
        }
        MessageDigest digest = sha256Digest();
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[Math.min(expected.contentBytes, 64 * 1024)];
        int count = 0;
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read == 0) continue;
                count += read;
                digest.update(buffer, 0, read);
                crc.update(buffer, 0, read);
            }
        }
        if (count != expected.contentBytes
                || crc.getValue() != expected.crc32
                || !expected.digest.equals(hex(digest.digest()))) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK,
                    "checkpoint chunk checksum or digest drifted");
        }
    }

    private static void publishImmutable(
            Path target, byte[] frame, String temporaryPrefix) {
        Path temporary = target.getParent().resolve(
                temporaryPrefix + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(frame);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try {
                Files.createLink(target, temporary);
                forceDirectory(target.getParent());
            } catch (FileAlreadyExistsException collision) {
                primary = collision;
            }
        } catch (IOException | UnsupportedOperationException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "durable checkpoint chunk metadata publication failed",
                    ioFailure);
        } catch (RuntimeException | Error runtimeFailure) {
            primary = runtimeFailure;
            throw runtimeFailure;
        } finally {
            cleanup(temporary, primary);
        }
    }

    private static void cleanup(Path temporary, Throwable primary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint chunk temporary cleanup failed", cleanupFailure);
        }
    }

    private static byte[] encodeManifest(StoredChunkedPayload payload) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, payload.digest);
                output.writeLong(payload.contentBytes);
                output.writeInt(payload.chunkBytes);
                output.writeInt(payload.chunks.size());
                for (ChunkHandle chunk : payload.chunks) {
                    writeText(output, chunk.digest);
                    output.writeInt(chunk.contentBytes);
                    output.writeLong(chunk.crc32);
                    writeText(output, chunk.compressionId);
                    writeText(output, chunk.encryptionId);
                }
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_MANIFEST_BODY_BYTES) {
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "checkpoint chunk manifest exceeds its bound");
            }
            return frame(body);
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory chunk manifest encode failed", impossible);
        }
    }

    private StoredChunkedPayload readManifest(Path path, String expectedDigest) {
        if (!Files.exists(path)) return null;
        requireRegularFile(path,
                StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST);
        try {
            long size = Files.size(path);
            if (size < HEADER_BYTES + DIGEST_BYTES
                    || size > HEADER_BYTES + MAX_MANIFEST_BODY_BYTES
                    + DIGEST_BYTES) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                        "checkpoint chunk manifest exceeds its bound");
            }
            byte[] body = bodyFromFrame(Files.readAllBytes(path));
            try (DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                String digest = readText(input, 64);
                long contentBytes = input.readLong();
                int encodedChunkBytes = input.readInt();
                int count = input.readInt();
                if (count < 1 || count > MAX_CHUNKS
                        || count > input.available() / 16 + 1) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                            "checkpoint chunk count exceeds its bound");
                }
                ArrayList<ChunkHandle> chunks = new ArrayList<>(count);
                for (int index = 0; index < count; index++) {
                    chunks.add(new ChunkHandle(
                            readText(input, 64), input.readInt(),
                            input.readLong(), readText(input, 32),
                            readText(input, 32)));
                }
                if (input.available() != 0 || !expectedDigest.equals(digest)) {
                    throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                            "checkpoint chunk manifest identity drifted");
                }
                return new StoredChunkedPayload(digest, contentBytes,
                        encodedChunkBytes, chunks);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "checkpoint chunk manifest is truncated", truncated);
        } catch (RuntimeException | IOException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "checkpoint chunk manifest is corrupt", malformed);
        }
    }

    private static byte[] frame(byte[] body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(MAGIC);
                output.writeInt(1);
                output.writeInt(body.length);
                output.write(body);
                output.write(sha256(body));
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory chunk manifest frame failed", impossible);
        }
    }

    private static byte[] bodyFromFrame(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > HEADER_BYTES + MAX_MANIFEST_BODY_BYTES
                + DIGEST_BYTES) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "invalid checkpoint chunk manifest frame length");
        }
        for (int index = 0; index < MAGIC.length; index++) {
            if (frame[index] != MAGIC[index]) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                        "invalid checkpoint chunk manifest magic");
            }
        }
        int version = ByteBuffer.wrap(frame, 8, 4).getInt();
        int bodyLength = ByteBuffer.wrap(frame, 12, 4).getInt();
        if (version != 1 || bodyLength < 0
                || bodyLength > MAX_MANIFEST_BODY_BYTES
                || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES
                != frame.length) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "invalid checkpoint chunk manifest envelope");
        }
        byte[] body = new byte[bodyLength];
        System.arraycopy(frame, HEADER_BYTES, body, 0, bodyLength);
        byte[] expected = sha256(body);
        int digestOffset = HEADER_BYTES + bodyLength;
        for (int index = 0; index < DIGEST_BYTES; index++) {
            if (frame[digestOffset + index] != expected[index]) {
                throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                        "checkpoint chunk manifest checksum drifted");
            }
        }
        return body;
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input, int maximumBytes)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximumBytes
                || length > input.available()) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "checkpoint chunk manifest text exceeds its bound");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(encoded,
                value.getBytes(StandardCharsets.UTF_8))) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "checkpoint chunk manifest text is malformed");
        }
        return value;
    }

    private static StoredChunkedPayload requireExact(
            StoredChunkedPayload existing, StoredChunkedPayload proposed) {
        if (existing == null || !existing.equals(proposed)) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "content-addressed chunk manifest drifted");
        }
        return existing;
    }

    private static void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(
                directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "checkpoint chunk path is not a directory");
            }
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent)) {
                forceDirectory(parent);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException | UnsupportedOperationException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint chunk directory is not durable", ioFailure);
        }
    }

    private static void requireRegularFile(
            Path path, StreamCheckpointException.Code code) {
        if (path == null || !Files.isRegularFile(
                path, LinkOption.NOFOLLOW_LINKS)) {
            throw failure(code, "checkpoint chunk data must be a regular file");
        }
    }

    private static void requireDigest(String digest) {
        if (digest == null || !digest.matches("[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                    "canonical checkpoint chunk digest is required");
        }
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static byte[] sha256(byte[] value) {
        return sha256Digest().digest(value);
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }

    Path chunkDirectory() { return chunkDirectory; }
    Path manifestDirectory() { return manifestDirectory; }
    Path chunkPath(ChunkHandle chunk) {
        if (chunk == null) throw failure(
                StreamCheckpointException.Code.INVALID_CONFIGURATION,
                "chunk handle is required");
        return chunkDirectory.resolve(chunk.digest + ".chunk");
    }
    Path manifestPath(String digest) {
        requireDigest(digest);
        return manifestDirectory.resolve(digest + ".chunks");
    }
}
