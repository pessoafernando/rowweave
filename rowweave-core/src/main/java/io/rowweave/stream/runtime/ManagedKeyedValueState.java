package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.MemorySpillable;
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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.zip.CRC32;

/**
 * 3 task-local keyed replacement state used by value and aggregate APIs.
 *
 * <p>One bounded replacement frame per canonical state key is authoritative
 * for the local working generation. The access-order cache is only a managed
 * memory acceleration and can be dropped completely, so key cardinality never
 * creates an unbounded heap index and update churn cannot grow retained spill.
 */
public final class ManagedKeyedValueState
        implements AutoCloseable, MemorySpillable {
    private static final int RECORD_MAGIC = 0x52575331; // RWS1
    private static final int RECORD_VERSION = 1;
    private static final int RECORD_FIXED_BYTES = 37;
    private static final int CACHE_ENTRY_OVERHEAD = 128;
    private static final int SNAPSHOT_MAGIC = 0x52575350; // RWSP
    private static final int SNAPSHOT_VERSION = 1;
    private static final int MAX_SNAPSHOT_TEXT_BYTES = 1024;
    private static final byte PUT = 1;

    private final String artifactId;
    private final String operatorUid;
    private final StreamStateDescriptor descriptor;
    private final int maxParallelism;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final MemoryManager memory;
    private final Path workingDirectory;
    private final long maximumSpillBytes;
    private final int maximumKeyBytes;
    private final int maximumValueBytes;
    private final LongSupplier processingTimeMillis;
    private final Thread ownerThread;
    private final LinkedHashMap<StateKey, CachedValue> cache =
            new LinkedHashMap<>(16, 0.75f, true);

    private long residentBytes;
    private long spillBytes;
    private long watermark = Long.MIN_VALUE;
    private long processingTimeFloor = Long.MIN_VALUE;
    private boolean closed;
    private StreamStateException terminalFailure;

    public ManagedKeyedValueState(
            String artifactId, String operatorUid,
            StreamStateDescriptor descriptor,
            int maxParallelism, int keyGroupStart, int keyGroupEnd,
            MemoryManager memory, Path spillRoot, long maximumSpillBytes,
            int maximumKeyBytes, int maximumValueBytes,
            LongSupplier processingTimeMillis) {
        requireText(artifactId, "artifact identity");
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
                || keyGroupStart < 0 || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= maxParallelism
                || memory == null || spillRoot == null
                || maximumSpillBytes < 1L
                || maximumKeyBytes < 1 || maximumValueBytes < 1
                || maximumKeyBytes > StreamEdge.MAX_FRAME_BYTES
                || maximumValueBytes > StreamEdge.MAX_FRAME_BYTES
                || processingTimeMillis == null) {
            throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                    "invalid managed keyed state configuration");
        }
        this.artifactId = artifactId;
        this.operatorUid = operatorUid;
        this.descriptor = descriptor;
        this.maxParallelism = maxParallelism;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.memory = memory;
        this.maximumSpillBytes = maximumSpillBytes;
        this.maximumKeyBytes = maximumKeyBytes;
        this.maximumValueBytes = maximumValueBytes;
        this.processingTimeMillis = processingTimeMillis;
        this.ownerThread = Thread.currentThread();
        try {
            Files.createDirectories(spillRoot);
            String namespace = namespaceDigest(
                    artifactId, operatorUid, descriptor);
            this.workingDirectory = Files.createTempDirectory(
                    spillRoot, namespace + '-');
        } catch (IOException ioFailure) {
            throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot create managed state spill directory", ioFailure);
        }
        memory.registerSpillable(this);
    }

    /** SHA-256 based stable key-group mapping over canonical key bytes. */
    public static int keyGroup(byte[] canonicalKey, int maxParallelism) {
        if (canonicalKey == null || maxParallelism < 1
                || maxParallelism > PortableStreamPlan.MAX_PARALLELISM) {
            throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                    "canonical key and valid maxParallelism are required");
        }
        byte[] digest = sha256(canonicalKey);
        int hash = ((digest[0] & 0xff) << 24)
                | ((digest[1] & 0xff) << 16)
                | ((digest[2] & 0xff) << 8)
                | (digest[3] & 0xff);
        return Math.floorMod(hash, maxParallelism);
    }

    public void put(byte[] canonicalKey, byte[] namespace,
                    byte[] value, long eventTimeMillis) {
        checkOwnerAndOpen();
        validateKeyAndNamespace(canonicalKey, namespace);
        if (value == null || value.length > maximumValueBytes) {
            throw failure(StreamStateException.Code.FRAME_LIMIT,
                    "state value exceeds the configured frame bound");
        }
        if (RECORD_FIXED_BYTES + (long) canonicalKey.length
                + namespace.length + value.length
                > StreamEdge.MAX_FRAME_BYTES) {
            throw failure(StreamStateException.Code.FRAME_LIMIT,
                    "encoded state frame exceeds the hard frame bound");
        }
        int group = requireOwnedGroup(canonicalKey);
        long timestamp = descriptor.ttlMode()
                == StreamStateDescriptor.TtlMode.PROCESSING_TIME
                ? processingTime() : eventTimeMillis;
        try {
            replace(group, canonicalKey, namespace, value, timestamp);
        } catch (StreamStateException exhausted) {
            if (exhausted.code() != StreamStateException.Code.SPILL_LIMIT
                    || descriptor.ttlMode()
                    == StreamStateDescriptor.TtlMode.NONE) {
                throw exhausted;
            }
            cleanupExpired();
            replace(group, canonicalKey, namespace, value, timestamp);
        }
        cacheValue(canonicalKey, namespace, value, timestamp);
    }

    public byte[] get(byte[] canonicalKey, byte[] namespace) {
        checkOwnerAndOpen();
        validateKeyAndNamespace(canonicalKey, namespace);
        int group = requireOwnedGroup(canonicalKey);
        StateKey lookup = StateKey.lookup(canonicalKey, namespace);
        CachedValue cached = cache.get(lookup);
        if (cached != null) {
            if (expired(cached.timestamp)) {
                delete(group, canonicalKey, namespace);
                removeCached(lookup);
                return null;
            }
            return cached.value.clone();
        }
        FoundValue found = read(group, canonicalKey, namespace);
        if (!found.present) return null;
        if (expired(found.timestamp)) {
            delete(group, canonicalKey, namespace);
            return null;
        }
        cacheValue(canonicalKey, namespace, found.value, found.timestamp);
        return found.value.clone();
    }

    public void remove(byte[] canonicalKey, byte[] namespace) {
        checkOwnerAndOpen();
        validateKeyAndNamespace(canonicalKey, namespace);
        int group = requireOwnedGroup(canonicalKey);
        delete(group, canonicalKey, namespace);
        removeCached(StateKey.lookup(canonicalKey, namespace));
    }

    /** Streams every live entry for one canonical key without materializing it. */
    public EntryCursor entries(byte[] canonicalKey) {
        checkOwnerAndOpen();
        validateKeyAndNamespace(canonicalKey, new byte[0]);
        requireOwnedGroup(canonicalKey);
        cleanupExpired();
        return cursor(canonicalKey);
    }

    /** Streams every live entry in this task-local state shard. */
    public EntryCursor entries() {
        checkOwnerAndOpen();
        cleanupExpired();
        return cursor(null);
    }

    private EntryCursor cursor(byte[] canonicalKey) {
        try {
            return new FileEntryCursor(canonicalKey,
                    Files.newDirectoryStream(workingDirectory, "*.state"));
        } catch (IOException openFailure) {
            throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot open managed state cursor", openFailure);
        }
    }

    public void advanceWatermark(long nextWatermark) {
        checkOwnerAndOpen();
        if (nextWatermark < watermark) {
            throw failure(StreamStateException.Code.WATERMARK_REGRESSION,
                    "state watermark must not move backwards");
        }
        watermark = nextWatermark;
        cleanupExpired();
    }

    /** Current restored or locally advanced event-time progress. */
    public long currentWatermark() {
        checkOwnerAndOpen();
        return watermark;
    }

    /** Deterministically removes all state invisible at the current time. */
    public void cleanupExpired() {
        checkOwnerAndOpen();
        if (descriptor.ttlMode() == StreamStateDescriptor.TtlMode.NONE) return;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                workingDirectory, "*.state")) {
            for (Path file : files) {
                FoundValue value = readFile(file);
                if (expired(value.timestamp)) {
                    long fileBytes = Files.size(file);
                    Files.delete(file);
                    spillBytes = Math.subtractExact(spillBytes, fileBytes);
                    removeCached(StateKey.lookup(value.key, value.namespace));
                }
            }
        } catch (StreamStateException failure) {
            throw failure;
        } catch (IOException | ArithmeticException cleanupFailure) {
            StreamStateException poisoned = failure(
                    StreamStateException.Code.IO_FAILURE,
                    "managed state TTL cleanup failed", cleanupFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    @Override
    public long spill() {
        checkOwnerAndOpen();
        long released = residentBytes;
        cache.clear();
        residentBytes = 0L;
        memory.releaseExact(released);
        return released;
    }

    public long residentBytes() {
        checkOwnerAndOpen();
        return residentBytes;
    }

    public long spillBytes() {
        checkOwnerAndOpen();
        return spillBytes;
    }

    /** Freezes this mailbox cut into a bounded durable checkpoint payload. */
    public StreamCheckpointMember snapshot(
            int subtaskIndex, FileCheckpointPayloadStore payloadStore) {
        checkOwnerAndOpen();
        if (subtaskIndex < 0 || payloadStore == null) {
            throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                    "checkpoint subtask and payload store are required");
        }
        Path staged = workingDirectory.resolve(
                ".snapshot-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            try (OutputStream file = Files.newOutputStream(staged,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                 DataOutputStream output = new DataOutputStream(
                         new BufferedOutputStream(new BoundedOutputStream(
                                 file, payloadStore.maximumPayloadBytes())))) {
                writeSnapshotHeader(output, subtaskIndex);
                try (DirectoryStream<Path> files = Files.newDirectoryStream(
                        workingDirectory, "*.state")) {
                    for (Path stateFile : files) {
                        byte[] frame = readStateFrameBytes(stateFile);
                        FoundValue found = decodeStateFrame(frame);
                        if (!stateFile.equals(statePath(
                                found.keyGroup, found.key, found.namespace))) {
                            throw corrupt(
                                    "managed state spill identity mismatch");
                        }
                        output.writeInt(frame.length);
                        output.write(frame);
                    }
                }
                output.writeInt(0);
            }
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(staged);
            return StreamCheckpointMember.stateHandle(
                    operatorUid, subtaskIndex, keyGroupStart, keyGroupEnd,
                    descriptor.stateName(), descriptor.serializerContractId(),
                    descriptor.stateSchemaVersion(), stored.digest(),
                    stored.contentBytes());
        } catch (RuntimeException | Error failure) {
            primary = failure;
            if (failure instanceof StreamStateException
                    && ((StreamStateException) failure).code()
                    == StreamStateException.Code.CORRUPT_SPILL) {
                terminalFailure = (StreamStateException) failure;
            }
            throw failure;
        } catch (IOException snapshotFailure) {
            primary = snapshotFailure;
            throw checkpointFailure(StreamCheckpointException.Code.IO_FAILURE,
                    "managed state snapshot staging failed", snapshotFailure);
        } finally {
            deleteCheckpointStaging(staged, primary);
        }
    }

    /** Restores a fresh task-local generation from one exact state member. */
    public void restore(
            int subtaskIndex, StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        checkOwnerAndOpen();
        validateRestoreMember(subtaskIndex, member, payloadStore);
        requireFreshRestoreTarget();
        Path archive = workingDirectory.resolve(
                ".restore-archive-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        boolean installed = false;
        try {
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), archive);
            RestoredSnapshot restored = stageRestoredSnapshot(
                    archive, subtaskIndex);
            installRestoredFrames();
            spillBytes = restored.spillBytes;
            watermark = restored.watermark;
            processingTimeFloor = restored.processingTimeFloor;
            installed = true;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (!installed) cleanupRestoreArtifacts(primary);
            deleteCheckpointStaging(archive, primary);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        checkOwner();
        Throwable cleanupFailure = null;
        long released = residentBytes;
        cache.clear();
        residentBytes = 0L;
        memory.releaseExact(released);
        memory.unregisterSpillable(this);
        try (DirectoryStream<Path> files =
                     Files.newDirectoryStream(workingDirectory)) {
            for (Path file : files) Files.deleteIfExists(file);
        } catch (Throwable failure) {
            cleanupFailure = failure;
        }
        try {
            Files.deleteIfExists(workingDirectory);
        } catch (Throwable failure) {
            if (cleanupFailure == null) cleanupFailure = failure;
            else cleanupFailure.addSuppressed(failure);
        }
        closed = true;
        if (cleanupFailure != null) {
            throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot clean managed state spill directory",
                    cleanupFailure);
        }
    }

    private void writeSnapshotHeader(
            DataOutputStream output, int subtaskIndex) throws IOException {
        output.writeInt(SNAPSHOT_MAGIC);
        output.writeInt(SNAPSHOT_VERSION);
        writeText(output, artifactId);
        writeText(output, operatorUid);
        output.writeInt(subtaskIndex);
        output.writeInt(maxParallelism);
        output.writeInt(keyGroupStart);
        output.writeInt(keyGroupEnd);
        writeText(output, descriptor.stateName());
        writeText(output, descriptor.kind().name());
        writeText(output, descriptor.serializerContractId());
        output.writeInt(descriptor.stateSchemaVersion());
        writeText(output, descriptor.ttlMode().name());
        output.writeLong(descriptor.ttlMillis());
        output.writeInt(maximumKeyBytes);
        output.writeInt(maximumValueBytes);
        output.writeLong(watermark);
        output.writeLong(processingTimeFloor);
    }

    private void validateRestoreMember(
            int subtaskIndex, StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        if (subtaskIndex < 0 || payloadStore == null) {
            throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                    "checkpoint subtask and payload store are required");
        }
        if (member == null
                || member.kind() != StreamCheckpointMember.Kind.STATE
                || !operatorUid.equals(member.operatorUid())
                || subtaskIndex != member.subtaskIndex()
                || keyGroupStart != member.keyGroupStart()
                || keyGroupEnd != member.keyGroupEnd()
                || !descriptor.stateName().equals(member.subjectId())
                || !descriptor.serializerContractId().equals(member.codecId())
                || descriptor.stateSchemaVersion() != member.schemaVersion()) {
            throw snapshotCorrupt(
                    "checkpoint state member identity does not match the task");
        }
    }

    private void requireFreshRestoreTarget() {
        if (spillBytes != 0L || residentBytes != 0L || !cache.isEmpty()) {
            throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                    "managed state restore requires a fresh local generation");
        }
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                workingDirectory, "*.state")) {
            if (files.iterator().hasNext()) {
                throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                        "managed state restore target already contains state");
            }
        } catch (StreamStateException failure) {
            throw failure;
        } catch (IOException inspectionFailure) {
            throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot inspect managed state restore target",
                    inspectionFailure);
        }
    }

    private RestoredSnapshot stageRestoredSnapshot(
            Path archive, int subtaskIndex) {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(archive)))) {
            int magic = input.readInt();
            int version = input.readInt();
            String storedArtifactId = readText(input);
            String storedOperatorUid = readText(input);
            int storedSubtaskIndex = input.readInt();
            int storedMaxParallelism = input.readInt();
            int storedKeyGroupStart = input.readInt();
            int storedKeyGroupEnd = input.readInt();
            String storedStateName = readText(input);
            String storedStateKind = readText(input);
            String storedSerializer = readText(input);
            int storedSchemaVersion = input.readInt();
            String storedTtlMode = readText(input);
            long storedTtlMillis = input.readLong();
            int storedMaximumKeyBytes = input.readInt();
            int storedMaximumValueBytes = input.readInt();
            long storedWatermark = input.readLong();
            long storedProcessingTimeFloor = input.readLong();
            if (magic != SNAPSHOT_MAGIC || version != SNAPSHOT_VERSION
                    || !artifactId.equals(storedArtifactId)
                    || !operatorUid.equals(storedOperatorUid)
                    || subtaskIndex != storedSubtaskIndex
                    || maxParallelism != storedMaxParallelism
                    || keyGroupStart != storedKeyGroupStart
                    || keyGroupEnd != storedKeyGroupEnd
                    || !descriptor.stateName().equals(storedStateName)
                    || !descriptor.kind().name().equals(storedStateKind)
                    || !descriptor.serializerContractId().equals(
                            storedSerializer)
                    || descriptor.stateSchemaVersion() != storedSchemaVersion
                    || !descriptor.ttlMode().name().equals(storedTtlMode)
                    || descriptor.ttlMillis() != storedTtlMillis
                    || maximumKeyBytes != storedMaximumKeyBytes
                    || maximumValueBytes != storedMaximumValueBytes) {
                throw snapshotCorrupt(
                        "checkpoint state snapshot contract does not match");
            }
            long restoredSpillBytes = 0L;
            while (true) {
                int frameLength = input.readInt();
                if (frameLength == 0) break;
                if (frameLength < RECORD_FIXED_BYTES
                        || frameLength > StreamEdge.MAX_FRAME_BYTES) {
                    throw snapshotCorrupt(
                            "checkpoint state frame length is invalid");
                }
                restoredSpillBytes = Math.addExact(
                        restoredSpillBytes, frameLength);
                if (restoredSpillBytes > maximumSpillBytes) {
                    throw snapshotCorrupt(
                            "checkpoint state exceeds the local spill budget");
                }
                byte[] frame = new byte[frameLength];
                input.readFully(frame);
                FoundValue value;
                try {
                    value = decodeStateFrame(frame);
                } catch (StreamStateException corruptFrame) {
                    throw snapshotCorrupt(
                            "checkpoint contains a corrupt state frame",
                            corruptFrame);
                }
                Path target = statePath(
                        value.keyGroup, value.key, value.namespace);
                Path staged = target.resolveSibling(
                        ".restore-" + target.getFileName().toString());
                if (Files.exists(target) || Files.exists(staged)) {
                    throw snapshotCorrupt(
                            "checkpoint contains duplicate state identity");
                }
                writeRestoredFrame(staged, frame);
            }
            if (input.read() != -1) {
                throw snapshotCorrupt(
                        "checkpoint state snapshot has trailing bytes");
            }
            return new RestoredSnapshot(restoredSpillBytes,
                    storedWatermark, storedProcessingTimeFloor);
        } catch (StreamStateException failure) {
            throw failure;
        } catch (ArithmeticException overflow) {
            throw snapshotCorrupt(
                    "checkpoint state size overflow", overflow);
        } catch (IOException readFailure) {
            throw snapshotCorrupt(
                    "checkpoint state snapshot is truncated or unreadable",
                    readFailure);
        }
    }

    private void installRestoredFrames() {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                workingDirectory, ".restore-*.state")) {
            for (Path staged : files) {
                String name = staged.getFileName().toString();
                Path target = staged.resolveSibling(
                        name.substring(".restore-".length()));
                try {
                    Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException unsupported) {
                    Files.move(staged, target);
                }
            }
        } catch (IOException installFailure) {
            throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot install restored managed state", installFailure);
        }
    }

    private void writeRestoredFrame(Path staged, byte[] frame) {
        try {
            Files.write(staged, frame, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        } catch (IOException writeFailure) {
            throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot stage restored managed state", writeFailure);
        }
    }

    private void cleanupRestoreArtifacts(Throwable primary) {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                workingDirectory)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (name.endsWith(".state")) {
                    Files.deleteIfExists(file);
                }
            }
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot clean failed managed state restore",
                    cleanupFailure);
        }
        spillBytes = 0L;
    }

    private void replace(int keyGroup, byte[] key, byte[] namespace,
                         byte[] value, long timestamp) {
        long recordBytes = RECORD_FIXED_BYTES + (long) key.length
                + namespace.length + value.length;
        Path target = statePath(keyGroup, key, namespace);
        Path temporary = target.resolveSibling(target.getFileName().toString()
                + ".tmp-" + Long.toUnsignedString(System.nanoTime()));
        try {
            long previousBytes = 0L;
            if (Files.exists(target)) {
                read(keyGroup, key, namespace);
                previousBytes = Files.size(target);
            }
            if (previousBytes > spillBytes) {
                throw corrupt("managed state spill accounting drift");
            }
            long retainedBytes = Math.addExact(
                    Math.subtractExact(spillBytes, previousBytes), recordBytes);
            if (retainedBytes > maximumSpillBytes) {
                throw failure(StreamStateException.Code.SPILL_LIMIT,
                        "managed state spill budget exhausted");
            }
            byte[] payload = encode(
                    keyGroup, key, namespace, value, timestamp);
            CRC32 checksum = new CRC32();
            checksum.update(payload);
            try (OutputStream raw = Files.newOutputStream(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                 DataOutputStream output = new DataOutputStream(
                         new BufferedOutputStream(raw))) {
                output.write(payload);
                output.writeInt((int) checksum.getValue());
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, target,
                        StandardCopyOption.REPLACE_EXISTING);
            }
            spillBytes = retainedBytes;
        } catch (StreamStateException stateFailure) {
            deleteStaging(temporary, stateFailure);
            if (stateFailure.code() == StreamStateException.Code.CORRUPT_SPILL) {
                terminalFailure = stateFailure;
            }
            throw stateFailure;
        } catch (IOException | ArithmeticException writeFailure) {
            StreamStateException poisoned = failure(
                    StreamStateException.Code.IO_FAILURE,
                    "managed state spill replacement failed", writeFailure);
            deleteStaging(temporary, poisoned);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private void delete(int keyGroup, byte[] key, byte[] namespace) {
        Path target = statePath(keyGroup, key, namespace);
        try {
            if (!Files.exists(target)) return;
            read(keyGroup, key, namespace);
            long previousBytes = Files.size(target);
            if (previousBytes > spillBytes) {
                throw corrupt("managed state spill accounting drift");
            }
            Files.delete(target);
            spillBytes = Math.subtractExact(spillBytes, previousBytes);
        } catch (StreamStateException corrupt) {
            terminalFailure = corrupt;
            throw corrupt;
        } catch (IOException | ArithmeticException deleteFailure) {
            StreamStateException poisoned = failure(
                    StreamStateException.Code.IO_FAILURE,
                    "managed state spill delete failed", deleteFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private FoundValue read(int keyGroup, byte[] key, byte[] namespace) {
        Path stateFile = statePath(keyGroup, key, namespace);
        if (!Files.exists(stateFile)) return FoundValue.missing();
        FoundValue found = readFile(stateFile);
        if (found.keyGroup != keyGroup
                || !Arrays.equals(found.key, key)
                || !Arrays.equals(found.namespace, namespace)
                || !stateFile.equals(statePath(
                        found.keyGroup, found.key, found.namespace))) {
            StreamStateException collision = corrupt(
                    "managed state spill identity mismatch");
            terminalFailure = collision;
            throw collision;
        }
        return found;
    }

    private FoundValue readFile(Path stateFile) {
        try {
            byte[] frame = readStateFrameBytes(stateFile);
            FoundValue found = decodeStateFrame(frame);
            if (!stateFile.equals(statePath(
                    found.keyGroup, found.key, found.namespace))) {
                throw corrupt("managed state spill identity mismatch");
            }
            return found;
        } catch (StreamStateException corrupt) {
            terminalFailure = corrupt;
            throw corrupt;
        } catch (IOException readFailure) {
            StreamStateException poisoned = failure(
                    StreamStateException.Code.CORRUPT_SPILL,
                    "managed state spill is truncated or unreadable",
                    readFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private byte[] readStateFrameBytes(Path stateFile) throws IOException {
        long fileBytes = Files.size(stateFile);
        if (fileBytes < RECORD_FIXED_BYTES
                || fileBytes > StreamEdge.MAX_FRAME_BYTES) {
            throw corrupt("managed state spill frame length is invalid");
        }
        byte[] frame = new byte[Math.toIntExact(fileBytes)];
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(stateFile)))) {
            input.readFully(frame);
            if (input.read() != -1) {
                throw corrupt("managed state spill changed while being read");
            }
        }
        return frame;
    }

    private FoundValue decodeStateFrame(byte[] frame) {
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
            validateRecordHeader(magic, version, operation, keyGroup,
                    keyLength, namespaceLength, valueLength);
            byte[] key = new byte[keyLength];
            byte[] namespace = new byte[namespaceLength];
            byte[] value = new byte[valueLength];
            input.readFully(key);
            input.readFully(namespace);
            input.readFully(value);
            int expectedChecksum = input.readInt();
            if (expectedChecksum != checksum(
                    keyGroup, key, namespace, value, timestamp)
                    || input.read() != -1
                    || keyGroup(key, maxParallelism) != keyGroup) {
                throw corrupt("managed state spill checksum or identity mismatch");
            }
            return FoundValue.present(
                    keyGroup, key, namespace, value, timestamp);
        } catch (IOException readFailure) {
            throw failure(
                    StreamStateException.Code.CORRUPT_SPILL,
                    "managed state spill is truncated or unreadable",
                    readFailure);
        }
    }

    private void validateRecordHeader(
            int magic, int version, byte operation, int keyGroup,
            int keyLength, int namespaceLength, int valueLength) {
        if (magic != RECORD_MAGIC || version != RECORD_VERSION
                || operation != PUT
                || keyGroup < keyGroupStart || keyGroup > keyGroupEnd
                || keyLength < 0 || keyLength > maximumKeyBytes
                || namespaceLength < 0 || namespaceLength > maximumKeyBytes
                || valueLength < 0 || valueLength > maximumValueBytes
                || RECORD_FIXED_BYTES + (long) keyLength
                + namespaceLength + valueLength > StreamEdge.MAX_FRAME_BYTES) {
            throw corrupt("managed state spill frame is invalid");
        }
    }

    private byte[] encode(int keyGroup, byte[] key, byte[] namespace,
                          byte[] value, long timestamp) throws IOException {
        long payloadBytes = RECORD_FIXED_BYTES - 4L + key.length
                + (long) namespace.length + value.length;
        ByteArrayOutputStream frame = new ByteArrayOutputStream(
                Math.toIntExact(payloadBytes));
        try (DataOutputStream encoded = new DataOutputStream(frame)) {
            encoded.writeInt(RECORD_MAGIC);
            encoded.writeInt(RECORD_VERSION);
            encoded.writeByte(PUT);
            encoded.writeInt(keyGroup);
            encoded.writeInt(key.length);
            encoded.writeInt(namespace.length);
            encoded.writeInt(value.length);
            encoded.writeLong(timestamp);
            encoded.write(key);
            encoded.write(namespace);
            encoded.write(value);
        }
        return frame.toByteArray();
    }

    private int checksum(int keyGroup, byte[] key, byte[] namespace,
                         byte[] value, long timestamp) {
        try {
            CRC32 checksum = new CRC32();
            checksum.update(encode(keyGroup, key, namespace, value, timestamp));
            return (int) checksum.getValue();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private void cacheValue(byte[] key, byte[] namespace,
                            byte[] value, long timestamp) {
        StateKey lookup = StateKey.lookup(key, namespace);
        removeCached(lookup);
        long charge = CACHE_ENTRY_OVERHEAD + (long) key.length
                + namespace.length + value.length;
        if (charge > memory.budget()) return;
        while (!memory.tryReserve(charge)) {
            Iterator<Map.Entry<StateKey, CachedValue>> iterator =
                    cache.entrySet().iterator();
            if (!iterator.hasNext()) return;
            Map.Entry<StateKey, CachedValue> eldest = iterator.next();
            long eldestCharge = eldest.getValue().charge;
            iterator.remove();
            residentBytes -= eldestCharge;
            memory.releaseExact(eldestCharge);
        }
        cache.put(StateKey.copyOf(key, namespace),
                new CachedValue(value.clone(), timestamp, charge));
        residentBytes += charge;
    }

    private void removeCached(StateKey key) {
        CachedValue removed = cache.remove(key);
        if (removed != null) {
            residentBytes -= removed.charge;
            memory.releaseExact(removed.charge);
        }
    }

    private boolean expired(long timestamp) {
        if (descriptor.ttlMode() == StreamStateDescriptor.TtlMode.NONE) {
            return false;
        }
        long now = descriptor.ttlMode()
                == StreamStateDescriptor.TtlMode.EVENT_TIME
                ? watermark : processingTime();
        long ttl = descriptor.ttlMillis();
        if (timestamp > Long.MAX_VALUE - ttl) return false;
        long deadline = timestamp + ttl;
        return now >= deadline;
    }

    private long processingTime() {
        long observed = processingTimeMillis.getAsLong();
        if (observed > processingTimeFloor) processingTimeFloor = observed;
        return processingTimeFloor;
    }

    private int requireOwnedGroup(byte[] canonicalKey) {
        int group = keyGroup(canonicalKey, maxParallelism);
        if (group < keyGroupStart || group > keyGroupEnd) {
            throw failure(StreamStateException.Code.FOREIGN_KEY_GROUP,
                    "key group " + group + " does not belong to task range "
                            + keyGroupStart + ".." + keyGroupEnd);
        }
        return group;
    }

    private void validateKeyAndNamespace(byte[] key, byte[] namespace) {
        if (key == null || namespace == null
                || key.length > maximumKeyBytes
                || namespace.length > maximumKeyBytes) {
            throw failure(StreamStateException.Code.FRAME_LIMIT,
                    "state key or namespace exceeds the configured frame bound");
        }
    }

    private Path statePath(int keyGroup, byte[] key, byte[] namespace) {
        MessageDigest digest = newSha256();
        digest.update((byte) (key.length >>> 24));
        digest.update((byte) (key.length >>> 16));
        digest.update((byte) (key.length >>> 8));
        digest.update((byte) key.length);
        digest.update(key);
        digest.update(namespace);
        return workingDirectory.resolve("kg-" + keyGroup + '-'
                + hex(digest.digest(), 32) + ".state");
    }

    private void checkOwnerAndOpen() {
        checkOwner();
        if (closed) {
            throw failure(StreamStateException.Code.CLOSED,
                    "managed state is closed");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private void checkOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamStateException.Code.THREAD_OWNERSHIP,
                    "managed state may only be accessed by its mailbox thread");
        }
    }

    private static String namespaceDigest(
            String artifactId, String operatorUid,
            StreamStateDescriptor descriptor) {
        MessageDigest digest = newSha256();
        updateText(digest, artifactId);
        updateText(digest, operatorUid);
        updateText(digest, descriptor.stateName());
        updateText(digest, descriptor.kind().name());
        updateText(digest, descriptor.serializerContractId());
        updateText(digest, Integer.toString(descriptor.stateSchemaVersion()));
        return hex(digest.digest(), 16);
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(
                java.nio.charset.StandardCharsets.UTF_8);
        if (encoded.length > MAX_SNAPSHOT_TEXT_BYTES) {
            throw checkpointFailure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "managed state snapshot metadata exceeds its bound");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_SNAPSHOT_TEXT_BYTES) {
            throw snapshotCorrupt(
                    "checkpoint state snapshot text length is invalid");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        return new String(encoded, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void updateText(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static String hex(byte[] digest, int bytes) {
        StringBuilder hex = new StringBuilder(bytes * 2);
        for (int i = 0; i < bytes; i++) {
            hex.append(Character.forDigit((digest[i] >>> 4) & 0xf, 16));
            hex.append(Character.forDigit(digest[i] & 0xf, 16));
        }
        return hex.toString();
    }

    private static byte[] sha256(byte[] value) {
        return newSha256().digest(value);
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new AssertionError("SHA-256 is required by the JVM", unavailable);
        }
    }

    private static void deleteStaging(Path temporary, Throwable primary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamStateException.Code.IO_FAILURE,
                    "cannot clean managed state staging file",
                    cleanupFailure);
        }
    }

    private static void deleteCheckpointStaging(
            Path temporary, Throwable primary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw checkpointFailure(StreamCheckpointException.Code.IO_FAILURE,
                    "cannot clean checkpoint staging file", cleanupFailure);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isEmpty() || value.length() > 256) {
            throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                    field + " is required and bounded");
        }
    }

    private static StreamStateException corrupt(String message) {
        return failure(StreamStateException.Code.CORRUPT_SPILL, message);
    }

    private static StreamStateException snapshotCorrupt(String message) {
        return failure(StreamStateException.Code.CORRUPT_SNAPSHOT, message);
    }

    private static StreamStateException snapshotCorrupt(
            String message, Throwable cause) {
        return failure(StreamStateException.Code.CORRUPT_SNAPSHOT,
                message, cause);
    }

    private static StreamCheckpointException checkpointFailure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException checkpointFailure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }

    /** Task-thread-only cursor over canonical state frames. */
    public interface EntryCursor extends AutoCloseable {
        boolean advance();
        byte[] canonicalKey();
        byte[] namespace();
        byte[] value();
        @Override void close();
    }

    private final class FileEntryCursor implements EntryCursor {
        private final byte[] selectedKey;
        private final DirectoryStream<Path> files;
        private final Iterator<Path> iterator;
        private FoundValue current;
        private boolean cursorClosed;

        private FileEntryCursor(byte[] selectedKey,
                DirectoryStream<Path> files) {
            this.selectedKey = selectedKey == null
                    ? null : selectedKey.clone();
            this.files = files;
            this.iterator = files.iterator();
        }

        @Override public boolean advance() {
            checkCursor();
            current = null;
            while (iterator.hasNext()) {
                FoundValue found = readFile(iterator.next());
                if (selectedKey != null
                        && !Arrays.equals(selectedKey, found.key)) {
                    continue;
                }
                if (expired(found.timestamp)) continue;
                current = found;
                return true;
            }
            return false;
        }

        @Override public byte[] canonicalKey() {
            return current().key.clone();
        }

        @Override public byte[] namespace() {
            return current().namespace.clone();
        }

        @Override public byte[] value() {
            return current().value.clone();
        }

        @Override public void close() {
            if (cursorClosed) return;
            checkOwner();
            cursorClosed = true;
            current = null;
            try {
                files.close();
            } catch (IOException closeFailure) {
                throw failure(StreamStateException.Code.IO_FAILURE,
                        "cannot close managed state cursor", closeFailure);
            }
        }

        private FoundValue current() {
            checkCursor();
            if (current == null) {
                throw failure(StreamStateException.Code.INVALID_CONFIGURATION,
                        "managed state cursor has no current entry");
            }
            return current;
        }

        private void checkCursor() {
            checkOwnerAndOpen();
            if (cursorClosed) {
                throw failure(StreamStateException.Code.CLOSED,
                        "managed state cursor is closed");
            }
        }
    }

    private static StreamStateException failure(
            StreamStateException.Code code, String message) {
        return new StreamStateException(code, message);
    }

    private static StreamStateException failure(
            StreamStateException.Code code, String message, Throwable cause) {
        return new StreamStateException(code, message, cause);
    }

    private static final class StateKey {
        private final byte[] key;
        private final byte[] namespace;
        private final int hash;

        private StateKey(byte[] key, byte[] namespace, boolean copy) {
            this.key = copy ? key.clone() : key;
            this.namespace = copy ? namespace.clone() : namespace;
            this.hash = 31 * Arrays.hashCode(key) + Arrays.hashCode(namespace);
        }

        static StateKey lookup(byte[] key, byte[] namespace) {
            return new StateKey(key, namespace, false);
        }

        static StateKey copyOf(byte[] key, byte[] namespace) {
            return new StateKey(key, namespace, true);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StateKey)) return false;
            StateKey that = (StateKey) other;
            return Arrays.equals(key, that.key)
                    && Arrays.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static final class CachedValue {
        private final byte[] value;
        private final long timestamp;
        private final long charge;

        private CachedValue(byte[] value, long timestamp, long charge) {
            this.value = value;
            this.timestamp = timestamp;
            this.charge = charge;
        }
    }

    private static final class RestoredSnapshot {
        private final long spillBytes;
        private final long watermark;
        private final long processingTimeFloor;

        private RestoredSnapshot(
                long spillBytes, long watermark, long processingTimeFloor) {
            this.spillBytes = spillBytes;
            this.watermark = watermark;
            this.processingTimeFloor = processingTimeFloor;
        }
    }

    private static final class BoundedOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final long maximumBytes;
        private long writtenBytes;

        private BoundedOutputStream(
                OutputStream delegate, long maximumBytes) {
            this.delegate = delegate;
            this.maximumBytes = maximumBytes;
        }

        @Override
        public void write(int value) throws IOException {
            requireCapacity(1);
            delegate.write(value);
            writtenBytes++;
        }

        @Override
        public void write(byte[] value, int offset, int length)
                throws IOException {
            if (value == null) throw new NullPointerException("value");
            if (offset < 0 || length < 0 || offset > value.length - length) {
                throw new IndexOutOfBoundsException();
            }
            requireCapacity(length);
            delegate.write(value, offset, length);
            writtenBytes += length;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private void requireCapacity(int addedBytes) {
            if (addedBytes > maximumBytes - writtenBytes) {
                throw checkpointFailure(
                        StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "managed state snapshot exceeds its staging bound");
            }
        }
    }

    private static final class FoundValue {
        private final boolean present;
        private final int keyGroup;
        private final byte[] key;
        private final byte[] namespace;
        private final byte[] value;
        private final long timestamp;

        private FoundValue(boolean present, int keyGroup, byte[] key,
                           byte[] namespace, byte[] value, long timestamp) {
            this.present = present;
            this.keyGroup = keyGroup;
            this.key = key;
            this.namespace = namespace;
            this.value = value;
            this.timestamp = timestamp;
        }

        static FoundValue missing() {
            return new FoundValue(false, -1, null, null, null, 0L);
        }

        static FoundValue present(int keyGroup, byte[] key, byte[] namespace,
                                  byte[] value, long timestamp) {
            return new FoundValue(true, keyGroup, key, namespace,
                    value, timestamp);
        }
    }
}
