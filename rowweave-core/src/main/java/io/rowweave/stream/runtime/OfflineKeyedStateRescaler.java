package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamStateDescriptor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.CRC32;

/** Offline full-snapshot repartitioning over fixed canonical key groups. */
public final class OfflineKeyedStateRescaler {
    public enum RescaleMode { STOP_WITH_SAVEPOINT, LIVE }

    private static final int SNAPSHOT_MAGIC = 0x52575350;
    private static final int SNAPSHOT_VERSION = 1;
    private static final int RECORD_MAGIC = 0x52575331;
    private static final int RECORD_VERSION = 1;
    private static final byte PUT = 1;
    private static final int RECORD_FIXED_BYTES = 37;
    private static final int MAX_TEXT_BYTES = 1024;
    private static final int MAX_SOURCE_MEMBERS =
            PortableStreamPlan.MAX_PARALLELISM;

    public static final class KeyGroupEvidence {
        private final int keyGroup;
        private final long recordCount;
        private final String contentDigest;

        KeyGroupEvidence(
                int keyGroup, long recordCount, String contentDigest) {
            if (keyGroup < 0 || recordCount < 0L
                    || contentDigest == null
                    || !contentDigest.matches("[0-9a-f]{64}")) {
                throw incompatible("invalid key-group evidence");
            }
            this.keyGroup = keyGroup;
            this.recordCount = recordCount;
            this.contentDigest = contentDigest;
        }

        public int keyGroup() { return keyGroup; }
        public long recordCount() { return recordCount; }
        public String contentDigest() { return contentDigest; }

        @Override public boolean equals(Object other) {
            if (!(other instanceof KeyGroupEvidence)) return false;
            KeyGroupEvidence that = (KeyGroupEvidence) other;
            return keyGroup == that.keyGroup
                    && recordCount == that.recordCount
                    && contentDigest.equals(that.contentDigest);
        }

        @Override public int hashCode() {
            return Objects.hash(keyGroup, recordCount, contentDigest);
        }
    }

    public static final class Result {
        private final String sourceArtifactId;
        private final String targetArtifactId;
        private final String operatorUid;
        private final int maxParallelism;
        private final int targetParallelism;
        private final List<StreamCheckpointMember> targetMembers;
        private final List<KeyGroupEvidence> keyGroups;

        private Result(
                String sourceArtifactId, String targetArtifactId,
                String operatorUid, int maxParallelism,
                int targetParallelism,
                List<StreamCheckpointMember> targetMembers,
                List<KeyGroupEvidence> keyGroups) {
            this.sourceArtifactId = sourceArtifactId;
            this.targetArtifactId = targetArtifactId;
            this.operatorUid = operatorUid;
            this.maxParallelism = maxParallelism;
            this.targetParallelism = targetParallelism;
            this.targetMembers = Collections.unmodifiableList(
                    new ArrayList<>(targetMembers));
            this.keyGroups = Collections.unmodifiableList(
                    new ArrayList<>(keyGroups));
        }

        public String sourceArtifactId() { return sourceArtifactId; }
        public String targetArtifactId() { return targetArtifactId; }
        public String operatorUid() { return operatorUid; }
        public int maxParallelism() { return maxParallelism; }
        public int targetParallelism() { return targetParallelism; }
        public List<StreamCheckpointMember> targetMembers() {
            return targetMembers;
        }
        public List<KeyGroupEvidence> keyGroups() { return keyGroups; }
    }

    private static final class Header {
        private final String artifactId;
        private final String operatorUid;
        private final int subtask;
        private final int maxParallelism;
        private final int keyGroupStart;
        private final int keyGroupEnd;
        private final String stateName;
        private final String stateKind;
        private final String serializer;
        private final int schemaVersion;
        private final String ttlMode;
        private final long ttlMillis;
        private final int maximumKeyBytes;
        private final int maximumValueBytes;
        private final long watermark;
        private final long processingTimeFloor;

        private Header(
                String artifactId, String operatorUid, int subtask,
                int maxParallelism, int keyGroupStart, int keyGroupEnd,
                String stateName, String stateKind, String serializer,
                int schemaVersion, String ttlMode, long ttlMillis,
                int maximumKeyBytes, int maximumValueBytes,
                long watermark, long processingTimeFloor) {
            this.artifactId = artifactId;
            this.operatorUid = operatorUid;
            this.subtask = subtask;
            this.maxParallelism = maxParallelism;
            this.keyGroupStart = keyGroupStart;
            this.keyGroupEnd = keyGroupEnd;
            this.stateName = stateName;
            this.stateKind = stateKind;
            this.serializer = serializer;
            this.schemaVersion = schemaVersion;
            this.ttlMode = ttlMode;
            this.ttlMillis = ttlMillis;
            this.maximumKeyBytes = maximumKeyBytes;
            this.maximumValueBytes = maximumValueBytes;
            this.watermark = watermark;
            this.processingTimeFloor = processingTimeFloor;
        }
    }

    private static final class Frame {
        private final int keyGroup;
        private final byte[] encoded;
        private final byte[] key;
        private final byte[] namespace;
        private final byte[] value;
        private final long timestamp;

        private Frame(
                int keyGroup, byte[] encoded,
                byte[] key, byte[] namespace, byte[] value,
                long timestamp) {
            this.keyGroup = keyGroup;
            this.encoded = encoded;
            this.key = key;
            this.namespace = namespace;
            this.value = value;
            this.timestamp = timestamp;
        }

        private String stateIdentity() {
            MessageDigest digest = sha256Digest();
            updateLengthAndBytes(digest, key);
            updateLengthAndBytes(digest, namespace);
            return hex(digest.digest());
        }
    }

    private static final class EvidenceAccumulator {
        private long count;
        private final MessageDigest digest = sha256Digest();

        private void add(byte[] frame) {
            if (count == Long.MAX_VALUE) {
                throw incompatible("key-group record count overflow");
            }
            count++;
            digest.update((byte) (frame.length >>> 24));
            digest.update((byte) (frame.length >>> 16));
            digest.update((byte) (frame.length >>> 8));
            digest.update((byte) frame.length);
            digest.update(frame);
        }

        private KeyGroupEvidence finish(int keyGroup) {
            return new KeyGroupEvidence(
                    keyGroup, count, hex(digest.digest()));
        }
    }

    private final Path workingRoot;
    private final long maximumStagingBytes;

    public OfflineKeyedStateRescaler(
            Path workingRoot, long maximumStagingBytes) {
        if (workingRoot == null || maximumStagingBytes < 1L) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "rescale working root and staging bound are required");
        }
        this.workingRoot = workingRoot.toAbsolutePath().normalize();
        this.maximumStagingBytes = maximumStagingBytes;
        ensureDirectory(this.workingRoot);
    }

    public Result rescale(
            String sourceArtifactId, String targetArtifactId,
            String operatorUid, StreamStateDescriptor descriptor,
            int maxParallelism, int targetParallelism,
            List<StreamCheckpointMember> sourceMembers,
            FileCheckpointPayloadStore sourcePayloadStore,
            FileCheckpointPayloadStore targetPayloadStore) {
        return rescale(RescaleMode.STOP_WITH_SAVEPOINT,
                sourceArtifactId, targetArtifactId, operatorUid, descriptor,
                maxParallelism, targetParallelism, sourceMembers,
                sourcePayloadStore, targetPayloadStore);
    }

    public Result rescale(
            RescaleMode mode,
            String sourceArtifactId, String targetArtifactId,
            String operatorUid, StreamStateDescriptor descriptor,
            int maxParallelism, int targetParallelism,
            List<StreamCheckpointMember> sourceMembers,
            FileCheckpointPayloadStore sourcePayloadStore,
            FileCheckpointPayloadStore targetPayloadStore) {
        if (mode == null) {
            throw failure(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                    "Stream rescale mode is required");
        }
        requireIdentity(sourceArtifactId, "source artifact");
        requireIdentity(targetArtifactId, "target artifact");
        requireText(operatorUid, "operator UID");
        if (descriptor == null
                || (descriptor.kind()
                != StreamStateDescriptor.Kind.KEYED_VALUE
                && descriptor.kind()
                != StreamStateDescriptor.Kind.KEYED_AGGREGATING
                && descriptor.kind()
                != StreamStateDescriptor.Kind.KEYED_MAP
                && descriptor.kind()
                != StreamStateDescriptor.Kind.KEYED_LIST)
                || maxParallelism < 1
                || maxParallelism > PortableStreamPlan.MAX_PARALLELISM
                || targetParallelism < 1
                || targetParallelism > maxParallelism
                || sourcePayloadStore == null || targetPayloadStore == null) {
            throw incompatible("offline keyed rescale configuration is invalid");
        }
        List<StreamCheckpointMember> sources = canonicalSources(
                sourceMembers, operatorUid, descriptor, maxParallelism);
        long stagedBytes = 0L;
        for (StreamCheckpointMember source : sources) {
            stagedBytes = addBounded(stagedBytes, source.contentBytes());
        }
        Path operation = workingRoot.resolve(
                ".rowweave-offline-rescale-" + UUID.randomUUID());
        ensureDirectory(operation);
        Throwable primary = null;
        try {
            List<Path> archives = restoreSources(
                    sources, sourcePayloadStore, operation);
            EvidenceAccumulator[] sourceEvidence =
                    accumulators(maxParallelism);
            List<Header> headers = validateSources(
                    archives, sources, sourceArtifactId, operatorUid,
                    descriptor, maxParallelism, sourceEvidence,
                    operation.resolve("source-identities"));
            List<KeyGroupEvidence> expected =
                    finish(sourceEvidence, maxParallelism);
            List<StreamCheckpointMember> targets = new ArrayList<>(
                    targetParallelism);
            for (int subtask = 0; subtask < targetParallelism; subtask++) {
                int start = keyGroupStart(
                        subtask, targetParallelism, maxParallelism);
                int end = keyGroupEnd(
                        subtask, targetParallelism, maxParallelism);
                Header targetHeader = targetHeader(
                        targetArtifactId, operatorUid, subtask,
                        maxParallelism, start, end, descriptor, headers);
                Path targetArchive = operation.resolve(
                        "target-" + subtask + ".archive");
                EvidenceAccumulator[] targetEvidence =
                        accumulators(maxParallelism);
                writeTarget(targetArchive, targetHeader, archives,
                        sources, targetEvidence,
                        maximumStagingBytes - stagedBytes);
                requireEvidence(expected, targetEvidence, start, end);
                stagedBytes = addBounded(
                        stagedBytes, Files.size(targetArchive));
                FileCheckpointPayloadStore.StoredPayload stored =
                        targetPayloadStore.store(targetArchive);
                targets.add(StreamCheckpointMember.stateHandle(
                        operatorUid, subtask, start, end,
                        descriptor.stateName(),
                        descriptor.serializerContractId(),
                        descriptor.stateSchemaVersion(),
                        stored.digest(), stored.contentBytes()));
            }
            return new Result(sourceArtifactId, targetArtifactId,
                    operatorUid, maxParallelism, targetParallelism,
                    targets, expected);
        } catch (StreamCheckpointException rescaleFailure) {
            primary = rescaleFailure;
            throw rescaleFailure;
        } catch (IOException ioFailure) {
            StreamCheckpointException wrapped = failure(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "offline keyed rescale I/O failed", ioFailure);
            primary = wrapped;
            throw wrapped;
        } catch (RuntimeException | Error rescaleFailure) {
            primary = rescaleFailure;
            throw rescaleFailure;
        } finally {
            cleanupTree(operation, primary);
        }
    }

    /**
     * Rewrites one fully replicated keyed-map snapshot for every target task.
     * The internal canonical key is task-owned storage metadata; namespaces
     * and values are the logical broadcast map and must agree in every source
     * replica before any target payload is published.
     */
    public Result replicateKeyedMapState(
            String sourceArtifactId, String targetArtifactId,
            String operatorUid, StreamStateDescriptor descriptor,
            int maxParallelism, int targetParallelism,
            List<StreamCheckpointMember> sourceMembers,
            FileCheckpointPayloadStore sourcePayloadStore,
            FileCheckpointPayloadStore targetPayloadStore) {
        requireIdentity(sourceArtifactId, "source artifact");
        requireIdentity(targetArtifactId, "target artifact");
        requireText(operatorUid, "operator UID");
        if (descriptor == null
                || descriptor.kind() != StreamStateDescriptor.Kind.KEYED_MAP
                || maxParallelism < 1
                || maxParallelism > PortableStreamPlan.MAX_PARALLELISM
                || targetParallelism < 1
                || targetParallelism > maxParallelism
                || sourcePayloadStore == null || targetPayloadStore == null) {
            throw incompatible(
                    "replicated keyed-map rescale configuration is invalid");
        }
        List<StreamCheckpointMember> sources = canonicalSources(
                sourceMembers, operatorUid, descriptor, maxParallelism);
        long stagedBytes = 0L;
        for (StreamCheckpointMember source : sources) {
            stagedBytes = addBounded(stagedBytes, source.contentBytes());
        }
        Path operation = workingRoot.resolve(
                ".rowweave-replicated-rescale-" + UUID.randomUUID());
        ensureDirectory(operation);
        Throwable primary = null;
        try {
            List<Path> archives = restoreSources(
                    sources, sourcePayloadStore, operation);
            List<Header> headers = new ArrayList<>(sources.size());
            TreeMap<String, Frame> canonical = null;
            for (int index = 0; index < sources.size(); index++) {
                StreamCheckpointMember source = sources.get(index);
                TreeMap<String, Frame> replica = new TreeMap<>();
                byte[] sourceKey = canonicalKey(source.keyGroupStart(),
                        source.keyGroupEnd(), maxParallelism);
                Header header = scan(archives.get(index), frame -> {
                    if (!Arrays.equals(sourceKey, frame.key)
                            || replica.put(hex(frame.namespace), frame)
                            != null) {
                        throw corrupt(
                                "replicated state contains a non-canonical key");
                    }
                });
                validateHeader(header, source, sourceArtifactId,
                        operatorUid, descriptor, maxParallelism);
                if (!headers.isEmpty()
                        && (headers.get(0).maximumKeyBytes
                        != header.maximumKeyBytes
                        || headers.get(0).maximumValueBytes
                        != header.maximumValueBytes)) {
                    throw incompatible(
                            "replicated snapshot limits differ across tasks");
                }
                if (canonical == null) canonical = replica;
                else requireSameReplica(canonical, replica);
                headers.add(header);
            }
            List<StreamCheckpointMember> targets = new ArrayList<>(
                    targetParallelism);
            for (int subtask = 0; subtask < targetParallelism; subtask++) {
                int start = keyGroupStart(
                        subtask, targetParallelism, maxParallelism);
                int end = keyGroupEnd(
                        subtask, targetParallelism, maxParallelism);
                Header targetHeader = replicatedTargetHeader(
                        targetArtifactId, operatorUid, subtask,
                        maxParallelism, start, end, descriptor, headers);
                Path targetArchive = operation.resolve(
                        "target-" + subtask + ".archive");
                byte[] targetKey = canonicalKey(start, end, maxParallelism);
                writeReplica(targetArchive, targetHeader, targetKey,
                        canonical, maximumStagingBytes - stagedBytes);
                stagedBytes = addBounded(
                        stagedBytes, Files.size(targetArchive));
                FileCheckpointPayloadStore.StoredPayload stored =
                        targetPayloadStore.store(targetArchive);
                targets.add(StreamCheckpointMember.stateHandle(
                        operatorUid, subtask, start, end,
                        descriptor.stateName(),
                        descriptor.serializerContractId(),
                        descriptor.stateSchemaVersion(), stored.digest(),
                        stored.contentBytes()));
            }
            return new Result(sourceArtifactId, targetArtifactId,
                    operatorUid, maxParallelism, targetParallelism,
                    targets, Collections.emptyList());
        } catch (StreamCheckpointException failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            StreamCheckpointException wrapped = failure(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "replicated keyed-map rescale I/O failed", failure);
            primary = wrapped;
            throw wrapped;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            cleanupTree(operation, primary);
        }
    }

    private static void requireSameReplica(
            TreeMap<String, Frame> expected,
            TreeMap<String, Frame> actual) {
        if (!expected.keySet().equals(actual.keySet())) {
            throw incompatible("replicated state keys differ across tasks");
        }
        for (String key : expected.keySet()) {
            Frame left = expected.get(key);
            Frame right = actual.get(key);
            if (left.timestamp != right.timestamp
                    || !Arrays.equals(left.namespace, right.namespace)
                    || !Arrays.equals(left.value, right.value)) {
                throw incompatible(
                        "replicated state values differ across tasks");
            }
        }
    }

    private static Header replicatedTargetHeader(
            String artifactId, String operatorUid, int subtask,
            int maxParallelism, int start, int end,
            StreamStateDescriptor descriptor, List<Header> sources) {
        Header template = sources.get(0);
        long watermark = Long.MAX_VALUE;
        long processingFloor = Long.MIN_VALUE;
        for (Header source : sources) {
            watermark = Math.min(watermark, source.watermark);
            processingFloor = Math.max(
                    processingFloor, source.processingTimeFloor);
        }
        return new Header(artifactId, operatorUid, subtask,
                maxParallelism, start, end, descriptor.stateName(),
                descriptor.kind().name(), descriptor.serializerContractId(),
                descriptor.stateSchemaVersion(), descriptor.ttlMode().name(),
                descriptor.ttlMillis(), template.maximumKeyBytes,
                template.maximumValueBytes, watermark, processingFloor);
    }

    private static void writeReplica(
            Path target, Header header, byte[] canonicalKey,
            TreeMap<String, Frame> replica, long maximumArchiveBytes)
            throws IOException {
        try (DataOutputStream output = new DataOutputStream(
                new BufferedOutputStream(new BoundedOutputStream(
                        Files.newOutputStream(target,
                                StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE),
                        maximumArchiveBytes)))) {
            writeHeader(output, header);
            for (Frame frame : replica.values()) {
                byte[] encoded = encodeFrame(canonicalKey,
                        frame.namespace, frame.value, frame.timestamp,
                        header.maxParallelism);
                output.writeInt(encoded.length);
                output.write(encoded);
            }
            output.writeInt(0);
        }
    }

    private static byte[] canonicalKey(
            int start, int end, int maxParallelism) {
        for (int candidate = 0; candidate < Integer.MAX_VALUE; candidate++) {
            byte[] key = ByteBuffer.allocate(Integer.BYTES)
                    .putInt(candidate).array();
            int group = ManagedKeyedValueState.keyGroup(key, maxParallelism);
            if (group >= start && group <= end) return key;
        }
        throw incompatible("replicated state has no canonical key group");
    }

    private static byte[] encodeFrame(
            byte[] key, byte[] namespace, byte[] value, long timestamp,
            int maxParallelism) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    Math.addExact(RECORD_FIXED_BYTES,
                            Math.addExact(key.length,
                            Math.addExact(namespace.length, value.length))));
            CRC32 checksum = new CRC32();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(RECORD_MAGIC);
                output.writeInt(RECORD_VERSION);
                output.writeByte(PUT);
                output.writeInt(ManagedKeyedValueState.keyGroup(
                        key, maxParallelism));
                output.writeInt(key.length);
                output.writeInt(namespace.length);
                output.writeInt(value.length);
                output.writeLong(timestamp);
                output.write(key);
                output.write(namespace);
                output.write(value);
                output.flush();
                checksum.update(bytes.toByteArray());
                output.writeInt((int) checksum.getValue());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "replicated state encoding failed", impossible);
        }
    }

    private List<Path> restoreSources(
            List<StreamCheckpointMember> sources,
            FileCheckpointPayloadStore store, Path operation) {
        List<Path> archives = new ArrayList<>(sources.size());
        for (int index = 0; index < sources.size(); index++) {
            StreamCheckpointMember source = sources.get(index);
            Path archive = operation.resolve("source-" + index + ".archive");
            store.restore(new FileCheckpointPayloadStore.StoredPayload(
                    source.contentDigest(), source.contentBytes()), archive);
            archives.add(archive);
        }
        return archives;
    }

    private List<Header> validateSources(
            List<Path> archives, List<StreamCheckpointMember> sources,
            String artifactId, String operatorUid,
            StreamStateDescriptor descriptor, int maxParallelism,
            EvidenceAccumulator[] evidence, Path identities) {
        ensureDirectory(identities);
        List<Header> headers = new ArrayList<>(archives.size());
        for (int index = 0; index < archives.size(); index++) {
            StreamCheckpointMember source = sources.get(index);
            Header header = scan(archives.get(index), frame -> {
                requireUniqueIdentity(identities, frame.stateIdentity());
                evidence[frame.keyGroup].add(frame.encoded);
            });
            validateHeader(header, source, artifactId, operatorUid,
                    descriptor, maxParallelism);
            if (!headers.isEmpty()
                    && (headers.get(0).maximumKeyBytes
                    != header.maximumKeyBytes
                    || headers.get(0).maximumValueBytes
                    != header.maximumValueBytes)) {
                throw incompatible(
                        "source snapshot frame limits differ across subtasks");
            }
            headers.add(header);
        }
        return headers;
    }

    private void writeTarget(
            Path target, Header targetHeader, List<Path> sources,
            List<StreamCheckpointMember> sourceMembers,
            EvidenceAccumulator[] evidence,
            long maximumArchiveBytes) throws IOException {
        try (DataOutputStream output = new DataOutputStream(
                new BufferedOutputStream(new BoundedOutputStream(
                        Files.newOutputStream(target,
                                StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE),
                        maximumArchiveBytes)))) {
            writeHeader(output, targetHeader);
            for (int index = 0; index < sources.size(); index++) {
                StreamCheckpointMember source = sourceMembers.get(index);
                if (source.keyGroupEnd() < targetHeader.keyGroupStart
                        || source.keyGroupStart()
                        > targetHeader.keyGroupEnd) {
                    continue;
                }
                scan(sources.get(index), frame -> {
                    if (frame.keyGroup >= targetHeader.keyGroupStart
                            && frame.keyGroup <= targetHeader.keyGroupEnd) {
                        output.writeInt(frame.encoded.length);
                        output.write(frame.encoded);
                        evidence[frame.keyGroup].add(frame.encoded);
                    }
                });
            }
            output.writeInt(0);
        }
    }

    private static Header targetHeader(
            String targetArtifactId, String operatorUid, int subtask,
            int maxParallelism, int start, int end,
            StreamStateDescriptor descriptor, List<Header> sources) {
        Header template = sources.get(0);
        long watermark = Long.MAX_VALUE;
        long processingFloor = Long.MIN_VALUE;
        boolean overlap = false;
        for (Header source : sources) {
            if (source.keyGroupEnd < start || source.keyGroupStart > end) {
                continue;
            }
            overlap = true;
            watermark = Math.min(watermark, source.watermark);
            processingFloor = Math.max(
                    processingFloor, source.processingTimeFloor);
        }
        if (!overlap) throw incompatible("target key-group range has no source");
        return new Header(targetArtifactId, operatorUid, subtask,
                maxParallelism, start, end, descriptor.stateName(),
                descriptor.kind().name(), descriptor.serializerContractId(),
                descriptor.stateSchemaVersion(), descriptor.ttlMode().name(),
                descriptor.ttlMillis(), template.maximumKeyBytes,
                template.maximumValueBytes, watermark, processingFloor);
    }

    private static void validateHeader(
            Header header, StreamCheckpointMember member,
            String artifactId, String operatorUid,
            StreamStateDescriptor descriptor, int maxParallelism) {
        if (!artifactId.equals(header.artifactId)
                || !operatorUid.equals(header.operatorUid)
                || member.subtaskIndex() != header.subtask
                || maxParallelism != header.maxParallelism
                || member.keyGroupStart() != header.keyGroupStart
                || member.keyGroupEnd() != header.keyGroupEnd
                || !descriptor.stateName().equals(header.stateName)
                || !descriptor.kind().name().equals(header.stateKind)
                || !descriptor.serializerContractId().equals(
                        header.serializer)
                || descriptor.stateSchemaVersion() != header.schemaVersion
                || !descriptor.ttlMode().name().equals(header.ttlMode)
                || descriptor.ttlMillis() != header.ttlMillis
                || header.maximumKeyBytes < 1
                || header.maximumValueBytes < 1
                || header.maximumKeyBytes > StreamEdge.MAX_FRAME_BYTES
                || header.maximumValueBytes > StreamEdge.MAX_FRAME_BYTES) {
            throw incompatible("source snapshot header does not match rescale");
        }
    }

    private static List<StreamCheckpointMember> canonicalSources(
            List<StreamCheckpointMember> sourceMembers,
            String operatorUid, StreamStateDescriptor descriptor,
            int maxParallelism) {
        if (sourceMembers == null || sourceMembers.isEmpty()
                || sourceMembers.size() > MAX_SOURCE_MEMBERS) {
            throw incompatible("source state closure is invalid");
        }
        ArrayList<StreamCheckpointMember> copy =
                new ArrayList<>(sourceMembers.size());
        for (StreamCheckpointMember source : sourceMembers) {
            if (source == null
                    || source.kind() != StreamCheckpointMember.Kind.STATE
                    || !operatorUid.equals(source.operatorUid())
                    || !descriptor.stateName().equals(source.subjectId())
                    || !descriptor.serializerContractId().equals(
                            source.codecId())
                    || descriptor.stateSchemaVersion()
                    != source.schemaVersion()) {
                throw incompatible("source state member contract differs");
            }
            copy.add(source);
        }
        copy.sort(Comparator.comparingInt(
                StreamCheckpointMember::keyGroupStart));
        int next = 0;
        for (int index = 0; index < copy.size(); index++) {
            StreamCheckpointMember source = copy.get(index);
            if (source.subtaskIndex() != index
                    || source.keyGroupStart() != keyGroupStart(
                            index, copy.size(), maxParallelism)
                    || source.keyGroupEnd() != keyGroupEnd(
                            index, copy.size(), maxParallelism)
                    || source.keyGroupStart() != next) {
                throw incompatible(
                        "source subtask key-group ranges are not canonical");
            }
            next = source.keyGroupEnd() + 1;
        }
        if (next != maxParallelism) {
            throw incompatible("source key-group closure is incomplete");
        }
        return Collections.unmodifiableList(copy);
    }

    private static Header scan(Path archive, FrameConsumer consumer) {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(archive)))) {
            Header header = readHeader(input);
            while (true) {
                int length = input.readInt();
                if (length == 0) break;
                if (length < RECORD_FIXED_BYTES
                        || length > StreamEdge.MAX_FRAME_BYTES) {
                    throw corrupt("state frame length is invalid");
                }
                byte[] frame = new byte[length];
                input.readFully(frame);
                consumer.accept(decodeFrame(frame, header));
            }
            if (input.read() != -1) {
                throw corrupt("state snapshot has trailing bytes");
            }
            return header;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw corrupt("state snapshot is truncated", truncated);
        } catch (IOException ioFailure) {
            throw corrupt("state snapshot cannot be read", ioFailure);
        }
    }

    private interface FrameConsumer {
        void accept(Frame frame) throws IOException;
    }

    private static Header readHeader(DataInputStream input) throws IOException {
        int magic = input.readInt();
        int version = input.readInt();
        Header header = new Header(
                readText(input), readText(input), input.readInt(),
                input.readInt(), input.readInt(), input.readInt(),
                readText(input), readText(input), readText(input),
                input.readInt(), readText(input), input.readLong(),
                input.readInt(), input.readInt(), input.readLong(),
                input.readLong());
        if (magic != SNAPSHOT_MAGIC || version != SNAPSHOT_VERSION) {
            throw corrupt("state snapshot header version is invalid");
        }
        return header;
    }

    private static void writeHeader(
            DataOutputStream output, Header header) throws IOException {
        output.writeInt(SNAPSHOT_MAGIC);
        output.writeInt(SNAPSHOT_VERSION);
        writeText(output, header.artifactId);
        writeText(output, header.operatorUid);
        output.writeInt(header.subtask);
        output.writeInt(header.maxParallelism);
        output.writeInt(header.keyGroupStart);
        output.writeInt(header.keyGroupEnd);
        writeText(output, header.stateName);
        writeText(output, header.stateKind);
        writeText(output, header.serializer);
        output.writeInt(header.schemaVersion);
        writeText(output, header.ttlMode);
        output.writeLong(header.ttlMillis);
        output.writeInt(header.maximumKeyBytes);
        output.writeInt(header.maximumValueBytes);
        output.writeLong(header.watermark);
        output.writeLong(header.processingTimeFloor);
    }

    private static Frame decodeFrame(byte[] frame, Header header) {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            int magic = input.readInt();
            int version = input.readInt();
            byte operation = input.readByte();
            int keyGroup = input.readInt();
            int keyLength = input.readInt();
            int namespaceLength = input.readInt();
            int valueLength = input.readInt();
            long timestamp = input.readLong();
            long expectedLength = RECORD_FIXED_BYTES + (long) keyLength
                    + namespaceLength + valueLength;
            if (magic != RECORD_MAGIC || version != RECORD_VERSION
                    || operation != PUT
                    || keyGroup < header.keyGroupStart
                    || keyGroup > header.keyGroupEnd
                    || keyLength < 0 || keyLength > header.maximumKeyBytes
                    || namespaceLength < 0
                    || namespaceLength > header.maximumKeyBytes
                    || valueLength < 0
                    || valueLength > header.maximumValueBytes
                    || expectedLength != frame.length) {
                throw corrupt("state frame shape is invalid");
            }
            byte[] key = new byte[keyLength];
            byte[] namespace = new byte[namespaceLength];
            byte[] value = new byte[valueLength];
            input.readFully(key);
            input.readFully(namespace);
            input.readFully(value);
            int checksum = input.readInt();
            CRC32 crc = new CRC32();
            crc.update(frame, 0, frame.length - 4);
            if (checksum != (int) crc.getValue() || input.read() != -1
                    || ManagedKeyedValueState.keyGroup(
                    key, header.maxParallelism) != keyGroup) {
                throw corrupt("state frame checksum or key group differs");
            }
            return new Frame(keyGroup, frame, key, namespace, value,
                    timestamp);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException malformed) {
            throw corrupt("state frame is malformed", malformed);
        }
    }

    private static void requireUniqueIdentity(
            Path identities, String identity) {
        Path marker = identities.resolve(identity.substring(0, 2))
                .resolve(identity + ".state");
        ensureDirectory(marker.getParent());
        try {
            Files.createFile(marker);
        } catch (FileAlreadyExistsException duplicate) {
            throw corrupt("duplicate state identity in source closure",
                    duplicate);
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "state identity validation failed", ioFailure);
        }
    }

    private static void requireEvidence(
            List<KeyGroupEvidence> expected,
            EvidenceAccumulator[] actual, int start, int end) {
        for (int group = start; group <= end; group++) {
            KeyGroupEvidence found = actual[group].finish(group);
            if (!expected.get(group).equals(found)) {
                throw failure(StreamCheckpointException.Code
                                .RESCALE_EVIDENCE_MISMATCH,
                        "key-group count or digest changed during rescale");
            }
        }
    }

    private static EvidenceAccumulator[] accumulators(int count) {
        EvidenceAccumulator[] result = new EvidenceAccumulator[count];
        for (int index = 0; index < count; index++) {
            result[index] = new EvidenceAccumulator();
        }
        return result;
    }

    private static List<KeyGroupEvidence> finish(
            EvidenceAccumulator[] accumulators, int count) {
        List<KeyGroupEvidence> evidence = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            evidence.add(accumulators[index].finish(index));
        }
        return evidence;
    }

    private long addBounded(long current, long added) {
        final long total;
        try {
            total = Math.addExact(current, added);
        } catch (ArithmeticException overflow) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "rescale staging bytes overflow", overflow);
        }
        if (total > maximumStagingBytes) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "rescale staging exceeds its bound");
        }
        return total;
    }

    public static int keyGroupStart(
            int subtask, int parallelism, int maxParallelism) {
        validateAssignment(subtask, parallelism, maxParallelism);
        return (int) ((long) subtask * maxParallelism / parallelism);
    }

    public static int keyGroupEnd(
            int subtask, int parallelism, int maxParallelism) {
        validateAssignment(subtask, parallelism, maxParallelism);
        return (int) ((long) (subtask + 1) * maxParallelism
                / parallelism) - 1;
    }

    private static void validateAssignment(
            int subtask, int parallelism, int maxParallelism) {
        if (maxParallelism < 1
                || maxParallelism > PortableStreamPlan.MAX_PARALLELISM
                || parallelism < 1 || parallelism > maxParallelism
                || subtask < 0 || subtask >= parallelism) {
            throw incompatible("key-group assignment is invalid");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length < 1 || encoded.length > MAX_TEXT_BYTES) {
            throw incompatible("snapshot text exceeds its bound");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_TEXT_BYTES) {
            throw corrupt("snapshot text exceeds its bound");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
        } catch (CharacterCodingException malformed) {
            throw corrupt("snapshot text is malformed", malformed);
        }
    }

    private static void updateLengthAndBytes(
            MessageDigest digest, byte[] value) {
        digest.update((byte) (value.length >>> 24));
        digest.update((byte) (value.length >>> 16));
        digest.update((byte) (value.length >>> 8));
        digest.update((byte) value.length);
        digest.update(value);
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            result[index * 2] = digits[(value[index] >>> 4) & 0xf];
            result[index * 2 + 1] = digits[value[index] & 0xf];
        }
        return new String(result);
    }

    private static void requireIdentity(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw incompatible(field + " is invalid");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isEmpty()
                || value.getBytes(StandardCharsets.UTF_8).length
                > MAX_TEXT_BYTES) {
            throw incompatible(field + " is invalid");
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "rescale path is not a directory");
            }
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "rescale directory creation failed", ioFailure);
        }
    }

    private static void cleanupTree(Path root, Throwable primary) {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) return;
        final IOException[] cleanup = new IOException[1];
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(
                        Path file, BasicFileAttributes attributes)
                        throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult postVisitDirectory(
                        Path directory, IOException failure)
                        throws IOException {
                    if (failure != null) throw failure;
                    Files.delete(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException cleanupFailure) {
            cleanup[0] = cleanupFailure;
        }
        if (cleanup[0] != null) {
            if (primary != null) primary.addSuppressed(cleanup[0]);
            else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "rescale staging cleanup failed", cleanup[0]);
        }
    }

    private static final class BoundedOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final long maximumBytes;
        private long writtenBytes;

        private BoundedOutputStream(
                OutputStream delegate, long maximumBytes) {
            if (maximumBytes < 1L) {
                try {
                    delegate.close();
                } catch (IOException ignored) {
                    // The deterministic staging-limit failure remains primary.
                }
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "rescale target has no remaining staging budget");
            }
            this.delegate = delegate;
            this.maximumBytes = maximumBytes;
        }

        @Override public void write(int value) throws IOException {
            requireCapacity(1);
            delegate.write(value);
            writtenBytes++;
        }

        @Override public void write(byte[] value, int offset, int length)
                throws IOException {
            if (value == null) throw new NullPointerException("value");
            if (offset < 0 || length < 0
                    || offset > value.length - length) {
                throw new IndexOutOfBoundsException();
            }
            requireCapacity(length);
            delegate.write(value, offset, length);
            writtenBytes += length;
        }

        @Override public void flush() throws IOException { delegate.flush(); }
        @Override public void close() throws IOException { delegate.close(); }

        private void requireCapacity(int added) {
            if (added > maximumBytes - writtenBytes) {
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "rescale staging exceeds its bound");
            }
        }
    }

    private static StreamCheckpointException incompatible(String message) {
        return failure(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                message);
    }

    private static StreamCheckpointException corrupt(String message) {
        return corrupt(message, null);
    }

    private static StreamCheckpointException corrupt(
            String message, Throwable cause) {
        return cause == null
                ? failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                message)
                : failure(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
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
