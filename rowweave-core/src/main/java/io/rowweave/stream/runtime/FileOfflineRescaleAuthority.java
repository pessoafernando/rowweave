package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/** Durable generation-fenced active pointer for verified offline rescale. */
public final class FileOfflineRescaleAuthority {
    private static final byte[] MAGIC =
            "RWXXORC1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES = 16 * 1024 * 1024;
    private static final int MAX_MEMBER_COUNT = MAX_BODY_BYTES / 32;
    private static final ReentrantLock[] JVM_LOCKS = new ReentrantLock[256];

    static {
        for (int index = 0; index < JVM_LOCKS.length; index++) {
            JVM_LOCKS[index] = new ReentrantLock();
        }
    }

    public static final class CompletedRescale {
        private final String rescaleId;
        private final long generation;
        private final String sourceSavepointDigest;
        private final String targetArtifactId;
        private final int maxParallelism;
        private final int targetParallelism;
        private final List<StreamCheckpointMember> targetMembers;
        private final List<OfflineKeyedStateRescaler.KeyGroupEvidence> keyGroups;

        private CompletedRescale(
                String rescaleId, long generation,
                String sourceSavepointDigest, String targetArtifactId,
                int maxParallelism, int targetParallelism,
                List<StreamCheckpointMember> targetMembers,
                List<OfflineKeyedStateRescaler.KeyGroupEvidence> keyGroups) {
            this.rescaleId = requireText(rescaleId);
            if (generation < 1L) incompatible("rescale generation is invalid");
            this.generation = generation;
            this.sourceSavepointDigest = requireDigest(
                    sourceSavepointDigest, "source savepoint digest");
            this.targetArtifactId = requireDigest(
                    targetArtifactId, "target artifact id");
            if (maxParallelism < 1
                    || maxParallelism > PortableStreamPlan.MAX_PARALLELISM
                    || targetParallelism < 1
                    || targetParallelism > maxParallelism) {
                incompatible("rescale parallelism is invalid");
            }
            this.maxParallelism = maxParallelism;
            this.targetParallelism = targetParallelism;
            this.targetMembers = canonicalMembers(
                    targetMembers, maxParallelism, targetParallelism);
            this.keyGroups = canonicalEvidence(keyGroups, maxParallelism);
        }

        public String rescaleId() { return rescaleId; }
        public long generation() { return generation; }
        public String sourceSavepointDigest() {
            return sourceSavepointDigest;
        }
        public String targetArtifactId() { return targetArtifactId; }
        public int maxParallelism() { return maxParallelism; }
        public int targetParallelism() { return targetParallelism; }
        public List<StreamCheckpointMember> targetMembers() {
            return targetMembers;
        }
        public List<OfflineKeyedStateRescaler.KeyGroupEvidence> keyGroups() {
            return keyGroups;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof CompletedRescale)) return false;
            CompletedRescale that = (CompletedRescale) other;
            return generation == that.generation
                    && maxParallelism == that.maxParallelism
                    && targetParallelism == that.targetParallelism
                    && rescaleId.equals(that.rescaleId)
                    && sourceSavepointDigest.equals(that.sourceSavepointDigest)
                    && targetArtifactId.equals(that.targetArtifactId)
                    && targetMembers.equals(that.targetMembers)
                    && keyGroups.equals(that.keyGroups);
        }

        @Override public int hashCode() {
            return Objects.hash(rescaleId, generation,
                    sourceSavepointDigest, targetArtifactId,
                    maxParallelism, targetParallelism,
                    targetMembers, keyGroups);
        }
    }

    private final Path root;
    private final Path activeFile;
    private final Path lockFile;
    private final FileCheckpointPayloadStore targetPayloadStore;
    private final ReentrantLock jvmLock;

    public FileOfflineRescaleAuthority(
            Path root, FileCheckpointPayloadStore targetPayloadStore) {
        if (root == null || targetPayloadStore == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "rescale authority root and payload store are required");
        }
        this.root = root.toAbsolutePath().normalize();
        this.activeFile = this.root.resolve("active-rescale.frame");
        this.lockFile = this.root.resolve("authority.lock");
        this.targetPayloadStore = targetPayloadStore;
        this.jvmLock = JVM_LOCKS[(this.root.hashCode() & Integer.MAX_VALUE)
                % JVM_LOCKS.length];
        ensureDirectory(this.root);
        ensureLockFile();
    }

    public CompletedRescale cutover(
            String rescaleId, long generation,
            String sourceSavepointDigest, String targetArtifactId,
            int maxParallelism, int targetParallelism,
            OfflineKeyedStateRescaler.Result result) {
        return cutover(rescaleId, generation, sourceSavepointDigest,
                targetArtifactId, maxParallelism, targetParallelism,
                result == null ? null : Collections.singletonList(result));
    }

    public CompletedRescale cutover(
            String rescaleId, long generation,
            String sourceSavepointDigest, String targetArtifactId,
            int maxParallelism, int targetParallelism,
            List<OfflineKeyedStateRescaler.Result> results) {
        if (results == null || results.isEmpty()) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "verified rescale result is required");
        }
        List<StreamCheckpointMember> targetMembers = new ArrayList<>();
        for (OfflineKeyedStateRescaler.Result result : results) {
            if (result == null
                    || !result.targetArtifactId().equals(targetArtifactId)
                    || maxParallelism != result.maxParallelism()
                    || targetParallelism != result.targetParallelism()) {
                incompatible("verified rescale result provenance differs");
            }
            targetMembers.addAll(result.targetMembers());
        }
        CompletedRescale proposed = new CompletedRescale(
                rescaleId, generation, sourceSavepointDigest,
                targetArtifactId, maxParallelism, targetParallelism,
                targetMembers, results.size() == 1
                        ? results.get(0).keyGroups()
                        : combinedEvidence(results, maxParallelism));
        return locked(() -> {
            CompletedRescale current = readActive();
            if (current != null && current.rescaleId.equals(rescaleId)) {
                if (!current.equals(proposed)) incompatible(
                        "rescale identity cannot be rebound");
                verifyPayloads(current);
                return current;
            }
            long expectedGeneration = current == null
                    ? 1L : Math.addExact(current.generation, 1L);
            if (generation != expectedGeneration) {
                incompatible("rescale generation cannot replace active state");
            }
            verifyPayloads(proposed);
            writeReplace(activeFile, frame(encode(proposed)));
            CompletedRescale durable = readActive();
            if (!proposed.equals(durable)) {
                throw corrupt(
                        "durable rescale pointer differs after publication");
            }
            verifyPayloads(durable);
            return durable;
        });
    }

    public CompletedRescale active() {
        return locked(() -> {
            CompletedRescale active = readActive();
            if (active != null) verifyPayloads(active);
            return active;
        });
    }

    private void verifyPayloads(CompletedRescale completed) {
        for (StreamCheckpointMember member : completed.targetMembers) {
            targetPayloadStore.verifyStored(
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes()));
        }
    }

    private CompletedRescale readActive() {
        if (!Files.exists(activeFile, LinkOption.NOFOLLOW_LINKS)) return null;
        byte[] body = readFrame(activeFile);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            String id = readText(input, 256);
            long generation = input.readLong();
            String source = readText(input, 64);
            String artifact = readText(input, 64);
            int maxParallelism = input.readInt();
            int targetParallelism = input.readInt();
            int memberCount = readCount(input, MAX_MEMBER_COUNT);
            List<StreamCheckpointMember> members =
                    new ArrayList<>(memberCount);
            for (int index = 0; index < memberCount; index++) {
                members.add(StreamCheckpointMemberCodec.decode(
                        readBytes(input, 8192)));
            }
            int evidenceCount = readCount(
                    input, PortableStreamPlan.MAX_PARALLELISM);
            List<OfflineKeyedStateRescaler.KeyGroupEvidence> evidence =
                    new ArrayList<>(evidenceCount);
            for (int index = 0; index < evidenceCount; index++) {
                evidence.add(new OfflineKeyedStateRescaler.KeyGroupEvidence(
                        input.readInt(), input.readLong(),
                        readText(input, 64)));
            }
            if (input.available() != 0) {
                throw corrupt("rescale pointer has trailing bytes");
            }
            return new CompletedRescale(id, generation, source, artifact,
                    maxParallelism, targetParallelism, members, evidence);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException | IOException malformed) {
            throw corrupt("rescale active pointer is malformed", malformed);
        }
    }

    private static byte[] encode(CompletedRescale completed) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, completed.rescaleId);
                output.writeLong(completed.generation);
                writeText(output, completed.sourceSavepointDigest);
                writeText(output, completed.targetArtifactId);
                output.writeInt(completed.maxParallelism);
                output.writeInt(completed.targetParallelism);
                output.writeInt(completed.targetMembers.size());
                for (StreamCheckpointMember member :
                        completed.targetMembers) {
                    writeBytes(output,
                            StreamCheckpointMemberCodec.encode(member));
                }
                output.writeInt(completed.keyGroups.size());
                for (OfflineKeyedStateRescaler.KeyGroupEvidence evidence
                        : completed.keyGroups) {
                    output.writeInt(evidence.keyGroup());
                    output.writeLong(evidence.recordCount());
                    writeText(output, evidence.contentDigest());
                }
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "rescale authority metadata exceeds its bound");
            }
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static List<StreamCheckpointMember> canonicalMembers(
            List<StreamCheckpointMember> members,
            int maxParallelism, int targetParallelism) {
        if (members == null || members.isEmpty()
                || members.size() % targetParallelism != 0
                || members.size() > MAX_MEMBER_COUNT) {
            incompatible("rescale target member closure is incomplete");
        }
        ArrayList<StreamCheckpointMember> copy =
                new ArrayList<>(members.size());
        java.util.Set<String> stateNames = new java.util.HashSet<>();
        int stateCount = members.size() / targetParallelism;
        for (int state = 0; state < stateCount; state++) {
            int base = state * targetParallelism;
            StreamCheckpointMember first = members.get(base);
            if (first == null || !stateNames.add(
                    first.operatorUid() + '\0' + first.subjectId())) {
                incompatible("rescale target state identity is duplicated");
            }
            String operator = first.operatorUid();
            String stateName = first.subjectId();
            String serializer = first.codecId();
            int schemaVersion = first.schemaVersion();
            for (int subtask = 0; subtask < targetParallelism; subtask++) {
                StreamCheckpointMember member = members.get(base + subtask);
                if (member == null
                        || member.kind()
                        != StreamCheckpointMember.Kind.STATE
                        || member.subtaskIndex() != subtask
                        || member.keyGroupStart()
                        != OfflineKeyedStateRescaler.keyGroupStart(
                                subtask, targetParallelism, maxParallelism)
                        || member.keyGroupEnd()
                        != OfflineKeyedStateRescaler.keyGroupEnd(
                                subtask, targetParallelism, maxParallelism)
                        || !stateName.equals(member.subjectId())
                        || !serializer.equals(member.codecId())
                        || schemaVersion != member.schemaVersion()) {
                    incompatible("rescale target ranges or contracts differ");
                }
                if (!operator.equals(member.operatorUid())) {
                    incompatible("rescale target operators differ");
                }
                copy.add(member);
            }
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<OfflineKeyedStateRescaler.KeyGroupEvidence>
    combinedEvidence(List<OfflineKeyedStateRescaler.Result> results,
            int maxParallelism) {
        List<OfflineKeyedStateRescaler.KeyGroupEvidence> combined =
                new ArrayList<>(maxParallelism);
        for (int group = 0; group < maxParallelism; group++) {
            long count = 0L;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                for (OfflineKeyedStateRescaler.Result result : results) {
                    OfflineKeyedStateRescaler.KeyGroupEvidence evidence =
                            result.keyGroups().get(group);
                    StreamCheckpointMember identity =
                            result.targetMembers().get(0);
                    count = Math.addExact(count, evidence.recordCount());
                    writeText(output, identity.subjectId());
                    writeText(output, identity.codecId());
                    output.writeInt(identity.schemaVersion());
                    output.writeLong(evidence.recordCount());
                    writeText(output, evidence.contentDigest());
                }
            } catch (IOException impossible) {
                throw new IllegalStateException(impossible);
            }
            combined.add(new OfflineKeyedStateRescaler.KeyGroupEvidence(
                    group, count, hex(sha256(bytes.toByteArray()))));
        }
        return Collections.unmodifiableList(combined);
    }

    private static List<OfflineKeyedStateRescaler.KeyGroupEvidence>
    canonicalEvidence(
            List<OfflineKeyedStateRescaler.KeyGroupEvidence> evidence,
            int maxParallelism) {
        if (evidence == null || evidence.size() != maxParallelism) {
            incompatible("rescale key-group evidence closure is incomplete");
        }
        ArrayList<OfflineKeyedStateRescaler.KeyGroupEvidence> copy =
                new ArrayList<>(evidence.size());
        for (int index = 0; index < evidence.size(); index++) {
            OfflineKeyedStateRescaler.KeyGroupEvidence item =
                    evidence.get(index);
            if (item == null || item.keyGroup() != index) {
                incompatible("rescale key-group evidence is not canonical");
            }
            copy.add(item);
        }
        return Collections.unmodifiableList(copy);
    }

    private <T> T locked(Action<T> action) {
        jvmLock.lock();
        try (FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            return action.run();
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "rescale authority lock failed", ioFailure);
        } finally {
            jvmLock.unlock();
        }
    }

    private interface Action<T> { T run(); }

    private void ensureLockFile() {
        try {
            if (!Files.exists(lockFile, LinkOption.NOFOLLOW_LINKS)) {
                try {
                    Files.createFile(lockFile);
                    forceDirectory(root);
                } catch (FileAlreadyExistsException raced) {
                    // Another process initialized the same lock.
                }
            }
            if (!Files.isRegularFile(lockFile, LinkOption.NOFOLLOW_LINKS)) {
                throw corrupt(
                        "rescale authority lock is not a regular file");
            }
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "rescale authority lock initialization failed",
                    ioFailure);
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "rescale authority path is not a directory");
            }
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "rescale authority directory failed", ioFailure);
        }
    }

    private static void writeReplace(Path target, byte[] content) {
        Path temporary = target.getParent().resolve(
                ".rowweave-rescale-active-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(target.getParent());
        } catch (IOException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "rescale active pointer publication failed", ioFailure);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException cleanupFailure) {
                if (primary != null) primary.addSuppressed(cleanupFailure);
                else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "rescale temporary cleanup failed", cleanupFailure);
            }
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
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] readFrame(Path path) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw corrupt("rescale active pointer is not a regular file");
        }
        try {
            long size = Files.size(path);
            if (size < HEADER_BYTES + DIGEST_BYTES
                    || size > HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES) {
                throw corrupt("rescale active pointer size is invalid");
            }
            byte[] frame = Files.readAllBytes(path);
            int version = ByteBuffer.wrap(frame, 8, 4).getInt();
            int length = ByteBuffer.wrap(frame, 12, 4).getInt();
            if (!MessageDigest.isEqual(MAGIC,
                    java.util.Arrays.copyOfRange(frame, 0, 8))
                    || version != 1 || length < 0
                    || length > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) length + DIGEST_BYTES
                    != frame.length) {
                throw corrupt("rescale active pointer frame is invalid");
            }
            byte[] body = java.util.Arrays.copyOfRange(
                    frame, HEADER_BYTES, HEADER_BYTES + length);
            if (!MessageDigest.isEqual(sha256(body),
                    java.util.Arrays.copyOfRange(
                            frame, HEADER_BYTES + length, frame.length))) {
                throw corrupt("rescale active pointer checksum differs");
            }
            return body;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw corrupt("rescale active pointer cannot be read", ioFailure);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum || length > input.available()) {
            throw corrupt("rescale metadata text exceeds its bound");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
        } catch (CharacterCodingException malformed) {
            throw corrupt("rescale metadata text is malformed", malformed);
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readBytes(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum || length > input.available()) {
            throw corrupt("rescale nested frame exceeds its bound");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static int readCount(DataInputStream input, int maximum)
            throws IOException {
        int count = input.readInt();
        if (count < 1 || count > maximum
                || count > input.available() / 8 + 1) {
            throw corrupt("rescale metadata count exceeds its bound");
        }
        return count;
    }

    private static String requireText(String value) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)
                || value.getBytes(StandardCharsets.UTF_8).length > 256) {
            incompatible("rescale id is invalid");
        }
        return value;
    }

    private static String requireDigest(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            incompatible(field + " is invalid");
        }
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte current : value) {
            result.append(String.format(java.util.Locale.ROOT,
                    "%02x", current & 0xff));
        }
        return result.toString();
    }

    private static void forceDirectory(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void incompatible(String message) {
        throw failure(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                message);
    }

    private static StreamCheckpointException corrupt(String message) {
        return failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                message);
    }

    private static StreamCheckpointException corrupt(
            String message, Throwable cause) {
        return new StreamCheckpointException(
                StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                message, cause);
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
}
