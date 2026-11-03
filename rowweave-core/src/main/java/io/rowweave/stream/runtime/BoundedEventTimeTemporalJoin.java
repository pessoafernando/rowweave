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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Single-mailbox-thread finite event-time interval join.
 *
 * <p>Record frames live in a quota-bound file working set. Heap state retains
 * only a quota-bound key/event index, and every match is pushed immediately to
 * {@link Sink}; the operator never materializes output rows.
 */
public final class BoundedEventTimeTemporalJoin implements AutoCloseable {
    private static final int RECORD_MAGIC = 0x5257544a; // RWTJ
    private static final int RECORD_VERSION = 1;
    private static final int RECORD_FIXED_BYTES = 41;
    private static final int SNAPSHOT_MAGIC = 0x52575453; // RWTS
    private static final int SNAPSHOT_VERSION = 1;

    @FunctionalInterface
    public interface JoinFunction {
        byte[] join(byte[] leftRecord, byte[] rightRecord) throws Exception;
    }

    @FunctionalInterface
    public interface Sink {
        void emit(byte[] joinedRecord) throws Exception;
    }

    private enum Side { LEFT, RIGHT }

    private final String operatorUid;
    private final long lowerBoundMillis;
    private final long upperBoundMillis;
    private final int maxParallelism;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final int maximumEntries;
    private final long maximumStateBytes;
    private final int maximumKeyBytes;
    private final int maximumRecordBytes;
    private final Path workingDirectory;
    private final JoinFunction function;
    private final Sink output;
    private final Thread ownerThread;
    private final Map<Key, NavigableMap<EventId, StoredRecord>> left =
            new LinkedHashMap<>();
    private final Map<Key, NavigableMap<EventId, StoredRecord>> right =
            new LinkedHashMap<>();

    private long leftSequence;
    private long rightSequence;
    private long leftWatermark = Long.MIN_VALUE;
    private long rightWatermark = Long.MIN_VALUE;
    private int stateEntries;
    private long stateBytes;
    private int maximumObservedEntries;
    private long maximumObservedBytes;
    private boolean closed;
    private StreamTemporalJoinException terminalFailure;

    public BoundedEventTimeTemporalJoin(
            String operatorUid, long lowerBoundMillis, long upperBoundMillis,
            int maxParallelism, int keyGroupStart, int keyGroupEnd,
            int maximumEntries, long maximumStateBytes,
            int maximumKeyBytes, int maximumRecordBytes,
            Path spillRoot, JoinFunction function, Sink output) {
        if (operatorUid == null || operatorUid.isEmpty()
                || lowerBoundMillis == Long.MIN_VALUE
                || lowerBoundMillis > upperBoundMillis
                || maxParallelism < 1
                || maxParallelism > PortableStreamPlan.MAX_PARALLELISM
                || keyGroupStart < 0 || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= maxParallelism
                || maximumEntries < 1 || maximumStateBytes < 1L
                || maximumKeyBytes < 1 || maximumRecordBytes < 1
                || maximumKeyBytes > StreamEdge.MAX_FRAME_BYTES
                || maximumRecordBytes > StreamEdge.MAX_FRAME_BYTES
                || spillRoot == null || function == null || output == null) {
            throw failure(StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                    "invalid bounded temporal join configuration");
        }
        this.operatorUid = operatorUid;
        this.lowerBoundMillis = lowerBoundMillis;
        this.upperBoundMillis = upperBoundMillis;
        this.maxParallelism = maxParallelism;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.maximumEntries = maximumEntries;
        this.maximumStateBytes = maximumStateBytes;
        this.maximumKeyBytes = maximumKeyBytes;
        this.maximumRecordBytes = maximumRecordBytes;
        this.function = function;
        this.output = output;
        this.ownerThread = Thread.currentThread();
        try {
            Files.createDirectories(spillRoot);
            this.workingDirectory = Files.createTempDirectory(
                    spillRoot, "temporal-join-");
        } catch (IOException ioFailure) {
            throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot create temporal join state directory", ioFailure);
        }
    }

    public void onLeft(byte[] canonicalKey, long eventTimestamp,
                       byte[] recordFrame) {
        onRecord(Side.LEFT, canonicalKey, eventTimestamp, recordFrame);
    }

    public void onRight(byte[] canonicalKey, long eventTimestamp,
                        byte[] recordFrame) {
        onRecord(Side.RIGHT, canonicalKey, eventTimestamp, recordFrame);
    }

    /** Removes one exact retained left record and compensates every match. */
    public void retractLeft(byte[] canonicalKey, long eventTimestamp,
            byte[] recordFrame, JoinFunction compensation) {
        retractRecord(Side.LEFT, canonicalKey, eventTimestamp,
                recordFrame, compensation);
    }

    /** Removes one exact retained right record and compensates every match. */
    public void retractRight(byte[] canonicalKey, long eventTimestamp,
            byte[] recordFrame, JoinFunction compensation) {
        retractRecord(Side.RIGHT, canonicalKey, eventTimestamp,
                recordFrame, compensation);
    }

    public void advanceLeftWatermark(long watermark) {
        checkOwnerAndHealthy();
        if (watermark < leftWatermark) watermarkRegression();
        leftWatermark = watermark;
        cleanup(right, watermark, false);
    }

    public void advanceRightWatermark(long watermark) {
        checkOwnerAndHealthy();
        if (watermark < rightWatermark) watermarkRegression();
        rightWatermark = watermark;
        cleanup(left, watermark, true);
    }

    public long leftWatermark() {
        checkOwnerAndHealthy();
        return leftWatermark;
    }

    public long rightWatermark() {
        checkOwnerAndHealthy();
        return rightWatermark;
    }

    /** A completed opposite side cannot produce new matches, but live-side
     * records remain until their own watermark rules out retractions. */
    public void reclaimAfterOppositeEnd(boolean leftEnded) {
        checkOwnerAndHealthy();
        Map<Key, NavigableMap<EventId, StoredRecord>> live =
                leftEnded ? right : left;
        long liveWatermark = leftEnded ? rightWatermark : leftWatermark;
        if (liveWatermark == Long.MIN_VALUE) return;
        Iterator<Map.Entry<Key, NavigableMap<EventId, StoredRecord>>> keys =
                live.entrySet().iterator();
        while (keys.hasNext()) {
            Map.Entry<Key, NavigableMap<EventId, StoredRecord>> keyed =
                    keys.next();
            Iterator<Map.Entry<EventId, StoredRecord>> events =
                    keyed.getValue().entrySet().iterator();
            while (events.hasNext()) {
                Map.Entry<EventId, StoredRecord> event = events.next();
                if (event.getKey().timestamp <= liveWatermark) {
                    delete(event.getValue());
                    events.remove();
                }
            }
            if (keyed.getValue().isEmpty()) keys.remove();
        }
    }

    public int stateEntryCount() {
        checkOwnerAndHealthy();
        return stateEntries;
    }

    public long stateBytes() {
        checkOwnerAndHealthy();
        return stateBytes;
    }

    public int maximumObservedEntries() {
        checkOwnerAndHealthy();
        return maximumObservedEntries;
    }

    public long maximumObservedBytes() {
        checkOwnerAndHealthy();
        return maximumObservedBytes;
    }

    public int ownedKeyGroupCount() {
        checkOwnerAndHealthy();
        return keyGroupEnd - keyGroupStart + 1;
    }

    /** Repartitions one checkpoint cut without replaying joined output. */
    public static List<StreamCheckpointMember> rescaleSnapshots(
            String operatorUid, long lowerBoundMillis,
            long upperBoundMillis, int maxParallelism,
            int targetParallelism, int maximumEntriesPerTarget,
            long maximumStateBytesPerTarget, int maximumKeyBytes,
            int maximumRecordBytes, List<StreamStateDescriptor> descriptors,
            List<StreamCheckpointMember> sourceMembers,
            FileCheckpointPayloadStore payloadStore, Path stagingRoot,
            long maximumStagingBytes) {
        if (operatorUid == null || operatorUid.isEmpty()
                || lowerBoundMillis == Long.MIN_VALUE
                || lowerBoundMillis > upperBoundMillis
                || maxParallelism < 1
                || maxParallelism > PortableStreamPlan.MAX_PARALLELISM
                || targetParallelism < 1
                || targetParallelism > maxParallelism
                || maximumEntriesPerTarget < 1
                || maximumStateBytesPerTarget < 1L
                || maximumKeyBytes < 1 || maximumRecordBytes < 1
                || descriptors == null || descriptors.size() != 2
                || sourceMembers == null || sourceMembers.isEmpty()
                || payloadStore == null || stagingRoot == null
                || maximumStagingBytes < 1L) {
            throw failure(StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                    "temporal live rescale configuration is invalid");
        }
        Map<String, StreamStateDescriptor> expectedDescriptors =
                new LinkedHashMap<>();
        for (StreamStateDescriptor descriptor : descriptors) {
            if (descriptor == null
                    || descriptor.kind()
                    != StreamStateDescriptor.Kind.KEYED_MAP
                    || expectedDescriptors.put(
                    descriptor.stateName(), descriptor) != null) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal live rescale descriptors are invalid");
            }
        }
        List<TemporalSnapshotSource> sources = rescaleSources(
                operatorUid, maxParallelism, expectedDescriptors,
                sourceMembers);
        Path operation = stagingRoot.resolve(
                ".rowweave-temporal-live-rescale-" + UUID.randomUUID());
        List<BoundedEventTimeTemporalJoin> targets = new ArrayList<>(
                targetParallelism);
        Throwable primary = null;
        try {
            Files.createDirectories(operation);
            for (int subtask = 0; subtask < targetParallelism; subtask++) {
                targets.add(new BoundedEventTimeTemporalJoin(
                        operatorUid, lowerBoundMillis, upperBoundMillis,
                        maxParallelism,
                        OfflineKeyedStateRescaler.keyGroupStart(
                                subtask, targetParallelism, maxParallelism),
                        OfflineKeyedStateRescaler.keyGroupEnd(
                                subtask, targetParallelism, maxParallelism),
                        maximumEntriesPerTarget,
                        maximumStateBytesPerTarget, maximumKeyBytes,
                        maximumRecordBytes, operation,
                        (left, right) -> new byte[] {1}, ignored -> { }));
            }
            long stagedBytes = 0L;
            long leftWatermark = Long.MAX_VALUE;
            long rightWatermark = Long.MAX_VALUE;
            for (TemporalSnapshotSource source : sources) {
                stagedBytes = addRescaleBytes(stagedBytes,
                        source.contentBytes, maximumStagingBytes);
                Path archive = operation.resolve(
                        "source-" + source.subtaskIndex + ".snapshot");
                payloadStore.restore(
                        new FileCheckpointPayloadStore.StoredPayload(
                                source.contentDigest, source.contentBytes),
                        archive);
                long[] watermarks = restoreForRescale(
                        archive, source, operatorUid, lowerBoundMillis,
                        upperBoundMillis, maxParallelism, descriptors,
                        maximumKeyBytes, maximumRecordBytes, targets);
                leftWatermark = Math.min(leftWatermark, watermarks[0]);
                rightWatermark = Math.min(rightWatermark, watermarks[1]);
                Files.deleteIfExists(archive);
            }
            List<StreamCheckpointMember> result = new ArrayList<>(
                    Math.multiplyExact(targetParallelism, 2));
            for (int subtask = 0; subtask < targets.size(); subtask++) {
                BoundedEventTimeTemporalJoin target = targets.get(subtask);
                target.advanceLeftWatermark(leftWatermark);
                target.advanceRightWatermark(rightWatermark);
                stagedBytes = addRescaleBytes(stagedBytes,
                        target.stateBytes(), maximumStagingBytes);
                List<StreamCheckpointMember> members = target.snapshot(
                        subtask, descriptors, payloadStore);
                stagedBytes = addRescaleBytes(stagedBytes,
                        members.get(0).contentBytes(), maximumStagingBytes);
                result.addAll(members);
            }
            return java.util.Collections.unmodifiableList(result);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot migrate temporal join checkpoint", failure);
        } finally {
            for (BoundedEventTimeTemporalJoin target : targets) {
                try {
                    target.close();
                } catch (Throwable cleanupFailure) {
                    if (primary != null) primary.addSuppressed(cleanupFailure);
                }
            }
            deleteTree(operation, primary);
        }
    }

    /** Freezes the current mailbox cut behind two logical state handles. */
    public List<StreamCheckpointMember> snapshot(int subtaskIndex,
            List<StreamStateDescriptor> descriptors,
            FileCheckpointPayloadStore payloadStore) {
        checkOwnerAndHealthy();
        if (subtaskIndex < 0 || descriptors == null
                || descriptors.size() != 2 || payloadStore == null) {
            throw failure(StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                    "temporal checkpoint configuration is invalid");
        }
        for (StreamStateDescriptor descriptor : descriptors) {
            if (descriptor == null
                    || descriptor.kind()
                    != StreamStateDescriptor.Kind.KEYED_MAP) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal checkpoint state is invalid");
            }
        }
        Path staged = workingDirectory.resolve(
                ".snapshot-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            List<Path> stateFiles = new ArrayList<>();
            try (DirectoryStream<Path> files = Files.newDirectoryStream(
                    workingDirectory, "*.state")) {
                for (Path file : files) stateFiles.add(file);
            }
            stateFiles.sort(Comparator.comparing(
                    path -> path.getFileName().toString()));
            if (stateFiles.size() != stateEntries) corrupt();
            try (OutputStream file = Files.newOutputStream(staged,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                 DataOutputStream output = new DataOutputStream(
                         new BufferedOutputStream(new BoundedOutputStream(
                                 file, payloadStore.maximumPayloadBytes())))) {
                output.writeInt(SNAPSHOT_MAGIC);
                output.writeInt(SNAPSHOT_VERSION);
                writeFrame(output, operatorUid.getBytes(
                        java.nio.charset.StandardCharsets.UTF_8));
                output.writeInt(subtaskIndex);
                output.writeInt(maxParallelism);
                output.writeInt(keyGroupStart);
                output.writeInt(keyGroupEnd);
                output.writeLong(lowerBoundMillis);
                output.writeLong(upperBoundMillis);
                output.writeLong(leftWatermark);
                output.writeLong(rightWatermark);
                output.writeLong(leftSequence);
                output.writeLong(rightSequence);
                output.writeInt(stateEntries);
                output.writeLong(stateBytes);
                output.writeInt(descriptors.size());
                for (StreamStateDescriptor descriptor : descriptors) {
                    writeFrame(output, descriptor.stateName().getBytes(
                            java.nio.charset.StandardCharsets.UTF_8));
                    writeFrame(output,
                            descriptor.serializerContractId().getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                    output.writeInt(descriptor.stateSchemaVersion());
                }
                for (Path stateFile : stateFiles) {
                    byte[] frame = readBoundedStateFile(stateFile);
                    writeFrame(output, frame);
                }
            }
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(staged);
            List<StreamCheckpointMember> members = new ArrayList<>(2);
            for (StreamStateDescriptor descriptor : descriptors) {
                members.add(StreamCheckpointMember.stateHandle(
                        operatorUid, subtaskIndex, keyGroupStart, keyGroupEnd,
                        descriptor.stateName(),
                        descriptor.serializerContractId(),
                        descriptor.stateSchemaVersion(), stored.digest(),
                        stored.contentBytes()));
            }
            return java.util.Collections.unmodifiableList(members);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot snapshot temporal join state", failure);
        } finally {
            try {
                Files.deleteIfExists(staged);
            } catch (IOException cleanupFailure) {
                if (primary != null) primary.addSuppressed(cleanupFailure);
                else throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                        "cannot clean temporal checkpoint staging",
                        cleanupFailure);
            }
        }
    }

    /** Restores a fresh mailbox owner from the exact paired state handles. */
    public void restore(int subtaskIndex,
            List<StreamStateDescriptor> descriptors,
            List<StreamCheckpointMember> members,
            FileCheckpointPayloadStore payloadStore) {
        checkOwnerAndHealthy();
        if (subtaskIndex < 0 || descriptors == null
                || descriptors.size() != 2 || members == null
                || members.size() != 2 || payloadStore == null
                || stateEntries != 0 || !left.isEmpty() || !right.isEmpty()
                || leftSequence != 0L || rightSequence != 0L
                || leftWatermark != Long.MIN_VALUE
                || rightWatermark != Long.MIN_VALUE) {
            throw failure(StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                    "temporal restore target or configuration is invalid");
        }
        Map<String, StreamStateDescriptor> expected = new HashMap<>();
        for (StreamStateDescriptor descriptor : descriptors) {
            if (descriptor == null
                    || descriptor.kind()
                    != StreamStateDescriptor.Kind.KEYED_MAP
                    || expected.put(descriptor.stateName(), descriptor) != null) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal restore state descriptors are invalid");
            }
        }
        String contentDigest = null;
        long contentBytes = -1L;
        Set<String> seenStates = new HashSet<>();
        for (StreamCheckpointMember member : members) {
            StreamStateDescriptor descriptor = member == null ? null
                    : expected.get(member.subjectId());
            if (descriptor == null
                    || member.kind() != StreamCheckpointMember.Kind.STATE
                    || !operatorUid.equals(member.operatorUid())
                    || member.subtaskIndex() != subtaskIndex
                    || member.keyGroupStart() != keyGroupStart
                    || member.keyGroupEnd() != keyGroupEnd
                    || !descriptor.serializerContractId().equals(
                    member.codecId())
                    || descriptor.stateSchemaVersion()
                    != member.schemaVersion()
                    || !seenStates.add(member.subjectId())) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal restore member identity differs");
            }
            if (contentDigest == null) {
                contentDigest = member.contentDigest();
                contentBytes = member.contentBytes();
            } else if (!contentDigest.equals(member.contentDigest())
                    || contentBytes != member.contentBytes()) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal restore handles do not share one payload");
            }
        }
        Path archive = workingDirectory.resolve(
                ".restore-" + UUID.randomUUID() + ".tmp");
        Path stagedDirectory = workingDirectory.resolve(
                ".restore-state-" + UUID.randomUUID());
        Throwable primary = null;
        boolean installed = false;
        try {
            payloadStore.restore(new FileCheckpointPayloadStore.StoredPayload(
                    contentDigest, contentBytes), archive);
            Files.createDirectory(stagedDirectory);
            RestoredSnapshot restored = readSnapshot(
                    archive, stagedDirectory, subtaskIndex, descriptors);
            for (Path staged : restored.stagedFiles) {
                Files.move(staged,
                        workingDirectory.resolve(staged.getFileName()),
                        StandardCopyOption.ATOMIC_MOVE);
            }
            left.putAll(restored.left);
            right.putAll(restored.right);
            leftWatermark = restored.leftWatermark;
            rightWatermark = restored.rightWatermark;
            leftSequence = restored.leftSequence;
            rightSequence = restored.rightSequence;
            stateEntries = restored.stateEntries;
            stateBytes = restored.stateBytes;
            maximumObservedEntries = stateEntries;
            maximumObservedBytes = stateBytes;
            installed = true;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot restore temporal join state", failure);
        } finally {
            if (!installed) cleanupRestoredState(primary);
            deleteTree(stagedDirectory, primary);
            try {
                Files.deleteIfExists(archive);
            } catch (IOException cleanupFailure) {
                if (primary != null) primary.addSuppressed(cleanupFailure);
                else if (installed) {
                    throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                            "cannot clean temporal restore archive",
                            cleanupFailure);
                }
            }
        }
    }

    private RestoredSnapshot readSnapshot(Path archive, Path stagedDirectory,
            int subtaskIndex, List<StreamStateDescriptor> descriptors)
            throws IOException {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(archive)))) {
            if (input.readInt() != SNAPSHOT_MAGIC
                    || input.readInt() != SNAPSHOT_VERSION
                    || !operatorUid.equals(text(readFrame(input, 256)))
                    || input.readInt() != subtaskIndex
                    || input.readInt() != maxParallelism
                    || input.readInt() != keyGroupStart
                    || input.readInt() != keyGroupEnd
                    || input.readLong() != lowerBoundMillis
                    || input.readLong() != upperBoundMillis) {
                corrupt();
            }
            long restoredLeftWatermark = input.readLong();
            long restoredRightWatermark = input.readLong();
            long restoredLeftSequence = input.readLong();
            long restoredRightSequence = input.readLong();
            int restoredEntries = input.readInt();
            long restoredBytes = input.readLong();
            if (restoredLeftSequence < 0L || restoredRightSequence < 0L
                    || restoredEntries < 0
                    || restoredEntries > maximumEntries
                    || restoredBytes < 0L
                    || restoredBytes > maximumStateBytes
                    || input.readInt() != descriptors.size()) {
                corrupt();
            }
            Set<String> descriptorNames = new HashSet<>();
            for (int index = 0; index < descriptors.size(); index++) {
                String name = text(readFrame(input, 256));
                String serializer = text(readFrame(input, 256));
                int schemaVersion = input.readInt();
                StreamStateDescriptor expected = descriptors.get(index);
                if (!expected.stateName().equals(name)
                        || !expected.serializerContractId().equals(serializer)
                        || expected.stateSchemaVersion() != schemaVersion
                        || !descriptorNames.add(name)) {
                    corrupt();
                }
            }
            Map<Key, NavigableMap<EventId, StoredRecord>> restoredLeft =
                    new LinkedHashMap<>();
            Map<Key, NavigableMap<EventId, StoredRecord>> restoredRight =
                    new LinkedHashMap<>();
            List<Path> stagedFiles = new ArrayList<>(restoredEntries);
            long observedBytes = 0L;
            int maximumRecordFileBytes = Math.toIntExact(Math.min(
                    Integer.MAX_VALUE, RECORD_FIXED_BYTES
                            + (long) maximumKeyBytes + maximumRecordBytes));
            for (int index = 0; index < restoredEntries; index++) {
                byte[] frame = readFrame(input, maximumRecordFileBytes);
                observedBytes = Math.addExact(observedBytes, frame.length);
                if (observedBytes > maximumStateBytes) corrupt();
                DecodedRecord decoded = decodeRecord(frame);
                validateRestoredIdentity(decoded, restoredLeftSequence,
                        restoredRightSequence);
                Path staged = stagedDirectory.resolve(stateFileName(
                        decoded.side, decoded.event));
                Files.write(staged, frame, StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
                stagedFiles.add(staged);
                Map<Key, NavigableMap<EventId, StoredRecord>> target =
                        decoded.side == Side.LEFT
                                ? restoredLeft : restoredRight;
                Key key = new Key(decoded.key);
                NavigableMap<EventId, StoredRecord> events = target.get(key);
                if (events == null) {
                    events = new TreeMap<>();
                    target.put(key, events);
                }
                Path installed = workingDirectory.resolve(
                        staged.getFileName());
                if (events.put(decoded.event,
                        new StoredRecord(installed, frame.length)) != null) {
                    corrupt();
                }
            }
            if (input.read() != -1 || observedBytes != restoredBytes) corrupt();
            return new RestoredSnapshot(restoredLeft, restoredRight,
                    stagedFiles, restoredLeftWatermark,
                    restoredRightWatermark, restoredLeftSequence,
                    restoredRightSequence, restoredEntries, restoredBytes);
        } catch (ArithmeticException overflow) {
            corrupt();
            throw new AssertionError(overflow);
        }
    }

    private static List<TemporalSnapshotSource> rescaleSources(
            String operatorUid, int maxParallelism,
            Map<String, StreamStateDescriptor> descriptors,
            List<StreamCheckpointMember> members) {
        Map<Integer, List<StreamCheckpointMember>> grouped = new TreeMap<>();
        for (StreamCheckpointMember member : members) {
            StreamStateDescriptor descriptor = member == null ? null
                    : descriptors.get(member.subjectId());
            if (descriptor == null
                    || member.kind() != StreamCheckpointMember.Kind.STATE
                    || !operatorUid.equals(member.operatorUid())
                    || !descriptor.serializerContractId().equals(
                    member.codecId())
                    || descriptor.stateSchemaVersion()
                    != member.schemaVersion()) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal live rescale member is incompatible");
            }
            grouped.computeIfAbsent(member.subtaskIndex(),
                    ignored -> new ArrayList<>()).add(member);
        }
        List<TemporalSnapshotSource> sources = new ArrayList<>(
                grouped.size());
        int nextGroup = 0;
        int nextSubtask = 0;
        for (Map.Entry<Integer, List<StreamCheckpointMember>> entry
                : grouped.entrySet()) {
            List<StreamCheckpointMember> pair = entry.getValue();
            pair.sort(Comparator.comparing(
                    StreamCheckpointMember::subjectId));
            if (entry.getKey() != nextSubtask || pair.size() != 2
                    || pair.get(0).subjectId().equals(
                    pair.get(1).subjectId())
                    || pair.get(0).keyGroupStart() != nextGroup
                    || pair.get(0).keyGroupStart()
                    != pair.get(1).keyGroupStart()
                    || pair.get(0).keyGroupEnd()
                    != pair.get(1).keyGroupEnd()
                    || !pair.get(0).contentDigest().equals(
                    pair.get(1).contentDigest())
                    || pair.get(0).contentBytes()
                    != pair.get(1).contentBytes()) {
                throw failure(
                        StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                        "temporal live rescale source closure is invalid");
            }
            sources.add(new TemporalSnapshotSource(entry.getKey(),
                    pair.get(0).keyGroupStart(),
                    pair.get(0).keyGroupEnd(),
                    pair.get(0).contentDigest(),
                    pair.get(0).contentBytes()));
            nextGroup = Math.addExact(pair.get(0).keyGroupEnd(), 1);
            nextSubtask++;
        }
        if (sources.isEmpty() || nextGroup != maxParallelism) {
            throw failure(
                    StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                    "temporal live rescale key-group closure is incomplete");
        }
        return sources;
    }

    private static long[] restoreForRescale(Path archive,
            TemporalSnapshotSource source, String operatorUid,
            long lowerBoundMillis, long upperBoundMillis,
            int maxParallelism, List<StreamStateDescriptor> descriptors,
            int maximumKeyBytes, int maximumRecordBytes,
            List<BoundedEventTimeTemporalJoin> targets) throws IOException {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(archive)))) {
            if (input.readInt() != SNAPSHOT_MAGIC
                    || input.readInt() != SNAPSHOT_VERSION
                    || !operatorUid.equals(text(readFrame(input, 256)))
                    || input.readInt() != source.subtaskIndex
                    || input.readInt() != maxParallelism
                    || input.readInt() != source.keyGroupStart
                    || input.readInt() != source.keyGroupEnd
                    || input.readLong() != lowerBoundMillis
                    || input.readLong() != upperBoundMillis) {
                corrupt();
            }
            long leftWatermark = input.readLong();
            long rightWatermark = input.readLong();
            long leftSequence = input.readLong();
            long rightSequence = input.readLong();
            int entries = input.readInt();
            long stateBytes = input.readLong();
            if (leftSequence < 0L || rightSequence < 0L
                    || entries < 0 || stateBytes < 0L
                    || input.readInt() != descriptors.size()) {
                corrupt();
            }
            Set<String> names = new HashSet<>();
            for (int index = 0; index < descriptors.size(); index++) {
                String name = text(readFrame(input, 256));
                String serializer = text(readFrame(input, 256));
                int schemaVersion = input.readInt();
                StreamStateDescriptor descriptor = descriptors.get(index);
                if (!descriptor.stateName().equals(name)
                        || !descriptor.serializerContractId().equals(
                        serializer)
                        || descriptor.stateSchemaVersion() != schemaVersion
                        || !names.add(name)) {
                    corrupt();
                }
            }
            long observedBytes = 0L;
            int maximumRecordFileBytes = Math.toIntExact(Math.min(
                    Integer.MAX_VALUE, RECORD_FIXED_BYTES
                            + (long) maximumKeyBytes + maximumRecordBytes));
            for (int index = 0; index < entries; index++) {
                byte[] frame = readFrame(input, maximumRecordFileBytes);
                observedBytes = Math.addExact(observedBytes, frame.length);
                DecodedRecord record = decodeRescaleRecord(
                        frame, maximumKeyBytes, maximumRecordBytes);
                long sequenceLimit = record.side == Side.LEFT
                        ? leftSequence : rightSequence;
                int keyGroup = ManagedKeyedValueState.keyGroup(
                        record.key, maxParallelism);
                if (record.event.sequence < 1L
                        || record.event.sequence > sequenceLimit
                        || keyGroup < source.keyGroupStart
                        || keyGroup > source.keyGroupEnd) {
                    corrupt();
                }
                int target = Math.min(targets.size() - 1,
                        (int) ((long) keyGroup * targets.size()
                                / maxParallelism));
                if (record.side == Side.LEFT) {
                    targets.get(target).onLeft(record.key,
                            record.event.timestamp, record.record);
                } else {
                    targets.get(target).onRight(record.key,
                            record.event.timestamp, record.record);
                }
            }
            if (input.read() != -1 || observedBytes != stateBytes) corrupt();
            return new long[] {leftWatermark, rightWatermark};
        } catch (ArithmeticException overflow) {
            corrupt();
            throw new AssertionError(overflow);
        }
    }

    private static DecodedRecord decodeRescaleRecord(byte[] encoded,
            int maximumKeyBytes, int maximumRecordBytes) throws IOException {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(encoded))) {
            if (input.readInt() != RECORD_MAGIC
                    || input.readInt() != RECORD_VERSION) corrupt();
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > maximumKeyBytes
                    + maximumRecordBytes + RECORD_FIXED_BYTES
                    || bodyLength != encoded.length - 16) corrupt();
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            int expectedCrc = input.readInt();
            CRC32 crc = new CRC32();
            crc.update(body);
            if ((int) crc.getValue() != expectedCrc || input.read() != -1) {
                corrupt();
            }
            try (DataInputStream fields = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                Side side = fields.readBoolean() ? Side.RIGHT : Side.LEFT;
                EventId event = new EventId(
                        fields.readLong(), fields.readLong());
                byte[] key = readFrame(fields, maximumKeyBytes);
                byte[] record = readFrame(fields, maximumRecordBytes);
                if (fields.read() != -1) corrupt();
                return new DecodedRecord(side, event, key, record);
            }
        }
    }

    private static long addRescaleBytes(
            long current, long added, long maximum) {
        final long total;
        try {
            total = Math.addExact(current, added);
        } catch (ArithmeticException overflow) {
            throw failure(StreamTemporalJoinException.Code.STATE_BYTE_LIMIT,
                    "temporal live rescale staging bytes overflow", overflow);
        }
        if (added < 0L || total > maximum) {
            throw failure(StreamTemporalJoinException.Code.STATE_BYTE_LIMIT,
                    "temporal live rescale staging exceeds its bound");
        }
        return total;
    }

    private void validateRestoredIdentity(DecodedRecord decoded,
            long restoredLeftSequence, long restoredRightSequence) {
        long sequenceLimit = decoded.side == Side.LEFT
                ? restoredLeftSequence : restoredRightSequence;
        if (decoded.event.sequence < 1L
                || decoded.event.sequence > sequenceLimit
                || ManagedKeyedValueState.keyGroup(
                decoded.key, maxParallelism) < keyGroupStart
                || ManagedKeyedValueState.keyGroup(
                decoded.key, maxParallelism) > keyGroupEnd) {
            corrupt();
        }
    }

    private void cleanupRestoredState(Throwable primary) {
        left.clear();
        right.clear();
        stateEntries = 0;
        stateBytes = 0L;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                workingDirectory, "*.state")) {
            for (Path file : files) Files.deleteIfExists(file);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
        }
    }

    private static void deleteTree(Path directory, Throwable primary) {
        if (!Files.exists(directory)) return;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (Path file : files) Files.deleteIfExists(file);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
        }
        try {
            Files.deleteIfExists(directory);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
        }
    }

    private static String stateFileName(Side side, EventId event) {
        return (side == Side.LEFT ? "left-" : "right-")
                + event.sequence + ".state";
    }

    private static String text(byte[] value) {
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private void onRecord(
            Side side, byte[] canonicalKey, long eventTimestamp,
            byte[] recordFrame) {
        checkOwnerAndHealthy();
        validateRecord(recordFrame);
        if (canonicalKey == null) return;
        validateKey(canonicalKey);
        requireOwned(canonicalKey);
        requireOnTime(side, eventTimestamp);
        long sequence = nextSequence(side);
        long encodedBytes = encodedSize(canonicalKey, recordFrame);
        reserve(encodedBytes);

        Key key = new Key(canonicalKey);
        NavigableMap<EventId, StoredRecord> opposite =
                index(side == Side.LEFT ? right : left, key);
        if (opposite != null) {
            for (Map.Entry<EventId, StoredRecord> candidate :
                    opposite.entrySet()) {
                if (matches(side, eventTimestamp,
                        candidate.getKey().timestamp)) {
                    emit(side, recordFrame,
                            read(candidate.getValue(), key,
                                    side == Side.LEFT ? Side.RIGHT : Side.LEFT,
                                    candidate.getKey()));
                }
            }
        }
        EventId event = new EventId(eventTimestamp, sequence);
        StoredRecord stored = write(side, event, key, recordFrame);
        Map<Key, NavigableMap<EventId, StoredRecord>> own =
                side == Side.LEFT ? left : right;
        NavigableMap<EventId, StoredRecord> events = own.get(key);
        if (events == null) {
            events = new TreeMap<>();
            own.put(key, events);
        }
        events.put(event, stored);
        stateEntries++;
        stateBytes += stored.bytes;
        maximumObservedEntries = Math.max(maximumObservedEntries, stateEntries);
        maximumObservedBytes = Math.max(maximumObservedBytes, stateBytes);
    }

    private void retractRecord(Side side, byte[] canonicalKey,
            long eventTimestamp, byte[] recordFrame,
            JoinFunction compensation) {
        checkOwnerAndHealthy();
        validateRecord(recordFrame);
        if (canonicalKey == null || compensation == null) {
            throw failure(
                    StreamTemporalJoinException.Code.INVALID_CONFIGURATION,
                    "temporal retraction identity is incomplete");
        }
        validateKey(canonicalKey);
        requireOwned(canonicalKey);
        requireOnTime(side, eventTimestamp);
        Key key = new Key(canonicalKey);
        Map<Key, NavigableMap<EventId, StoredRecord>> own =
                side == Side.LEFT ? left : right;
        NavigableMap<EventId, StoredRecord> events = own.get(key);
        Map.Entry<EventId, StoredRecord> removed = null;
        if (events != null) {
            for (Map.Entry<EventId, StoredRecord> candidate :
                    events.entrySet()) {
                if (candidate.getKey().timestamp == eventTimestamp
                        && Arrays.equals(recordFrame, read(
                        candidate.getValue(), key, side,
                        candidate.getKey()))) {
                    removed = candidate;
                    break;
                }
            }
        }
        if (removed == null) {
            throw failure(StreamTemporalJoinException.Code.MISSING_RETRACTION,
                    "temporal retraction does not identify retained state");
        }
        NavigableMap<EventId, StoredRecord> opposite =
                index(side == Side.LEFT ? right : left, key);
        if (opposite != null) {
            for (Map.Entry<EventId, StoredRecord> candidate :
                    opposite.entrySet()) {
                if (matches(side, eventTimestamp,
                        candidate.getKey().timestamp)) {
                    emit(side, recordFrame,
                            read(candidate.getValue(), key,
                                    side == Side.LEFT
                                            ? Side.RIGHT : Side.LEFT,
                                    candidate.getKey()), compensation);
                }
            }
        }
        delete(removed.getValue());
        events.remove(removed.getKey());
        if (events.isEmpty()) own.remove(key);
    }

    private void emit(Side incomingSide, byte[] incoming, byte[] retained) {
        emit(incomingSide, incoming, retained, function);
    }

    private void emit(Side incomingSide, byte[] incoming, byte[] retained,
            JoinFunction emitter) {
        try {
            byte[] joined = incomingSide == Side.LEFT
                    ? emitter.join(incoming.clone(), retained)
                    : emitter.join(retained, incoming.clone());
            validateRecord(joined);
            output.emit(joined.clone());
        } catch (StreamTemporalJoinException failure) {
            throw failure;
        } catch (Throwable userFailure) {
            StreamTemporalJoinException poisoned = failure(
                    StreamTemporalJoinException.Code.USER_FUNCTION_FAILURE,
                    "temporal join function or output failed", userFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private void requireOnTime(Side side, long timestamp) {
        if (side == Side.LEFT && rightWatermark != Long.MIN_VALUE
                && rightWatermark >= saturatedAdd(timestamp, upperBoundMillis)) {
            lateRecord();
        }
        if (side == Side.RIGHT && leftWatermark != Long.MIN_VALUE
                && leftWatermark >= saturatedAdd(timestamp, -lowerBoundMillis)) {
            lateRecord();
        }
    }

    private boolean matches(Side incomingSide, long incoming, long retained) {
        if (incomingSide == Side.LEFT) {
            return retained >= saturatedAdd(incoming, lowerBoundMillis)
                    && retained <= saturatedAdd(incoming, upperBoundMillis);
        }
        return retained >= saturatedAdd(incoming, -upperBoundMillis)
                && retained <= saturatedAdd(incoming, -lowerBoundMillis);
    }

    private void cleanup(
            Map<Key, NavigableMap<EventId, StoredRecord>> index,
            long watermark, boolean leftRecords) {
        if (watermark == Long.MIN_VALUE) return;
        Iterator<Map.Entry<Key, NavigableMap<EventId, StoredRecord>>> keys =
                index.entrySet().iterator();
        while (keys.hasNext()) {
            Map.Entry<Key, NavigableMap<EventId, StoredRecord>> keyed = keys.next();
            Iterator<Map.Entry<EventId, StoredRecord>> events =
                    keyed.getValue().entrySet().iterator();
            while (events.hasNext()) {
                Map.Entry<EventId, StoredRecord> event = events.next();
                long cleanupAt = leftRecords
                        ? saturatedAdd(event.getKey().timestamp, upperBoundMillis)
                        : saturatedAdd(event.getKey().timestamp, -lowerBoundMillis);
                if (watermark >= cleanupAt) {
                    delete(event.getValue());
                    events.remove();
                }
            }
            if (keyed.getValue().isEmpty()) keys.remove();
        }
    }

    private StoredRecord write(
            Side side, EventId event, Key key, byte[] recordFrame) {
        Path target = workingDirectory.resolve(
                stateFileName(side, event));
        Path staged = workingDirectory.resolve(
                ".stage-" + UUID.randomUUID() + ".tmp");
        try {
            byte[] encoded = encode(side, event, key.bytes, recordFrame);
            Files.write(staged, encoded, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            try {
                Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(staged, target);
            }
            return new StoredRecord(target, encoded.length);
        } catch (IOException | RuntimeException storeFailure) {
            try {
                Files.deleteIfExists(staged);
            } catch (IOException cleanupFailure) {
                storeFailure.addSuppressed(cleanupFailure);
            }
            StreamTemporalJoinException poisoned = failure(
                    StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot persist temporal join record", storeFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private byte[] read(
            StoredRecord stored, Key expectedKey,
            Side expectedSide, EventId expectedEvent) {
        try {
            byte[] encoded = readBoundedStateFile(stored.path);
            DecodedRecord decoded = decodeRecord(encoded);
            if (decoded.side != expectedSide
                    || !decoded.event.equals(expectedEvent)
                    || !Arrays.equals(decoded.key, expectedKey.bytes)) {
                corrupt();
            }
            return decoded.record;
        } catch (StreamTemporalJoinException failure) {
            terminalFailure = failure;
            throw failure;
        } catch (IOException readFailure) {
            StreamTemporalJoinException poisoned = failure(
                    StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot read temporal join state", readFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private DecodedRecord decodeRecord(byte[] encoded) throws IOException {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(encoded))) {
            if (input.readInt() != RECORD_MAGIC
                    || input.readInt() != RECORD_VERSION) corrupt();
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > maximumKeyBytes
                    + maximumRecordBytes + RECORD_FIXED_BYTES
                    || bodyLength != encoded.length - 16) corrupt();
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            int expectedCrc = input.readInt();
            CRC32 crc = new CRC32();
            crc.update(body);
            if ((int) crc.getValue() != expectedCrc || input.read() != -1) {
                corrupt();
            }
            try (DataInputStream fields = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                Side side = fields.readBoolean() ? Side.RIGHT : Side.LEFT;
                EventId event = new EventId(
                        fields.readLong(), fields.readLong());
                byte[] key = readFrame(fields, maximumKeyBytes);
                byte[] record = readFrame(fields, maximumRecordBytes);
                if (fields.read() != -1) corrupt();
                return new DecodedRecord(side, event, key, record);
            }
        }
    }

    private byte[] readBoundedStateFile(Path stateFile) throws IOException {
        long maximum = RECORD_FIXED_BYTES + (long) maximumKeyBytes
                + maximumRecordBytes;
        long size = Files.size(stateFile);
        if (size < 1L || size > maximum || size > Integer.MAX_VALUE) corrupt();
        byte[] frame = Files.readAllBytes(stateFile);
        if (frame.length != size) corrupt();
        return frame;
    }

    private byte[] encode(
            Side side, EventId event, byte[] key, byte[] record) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeBoolean(side == Side.RIGHT);
                body.writeLong(event.timestamp);
                body.writeLong(event.sequence);
                writeFrame(body, key);
                writeFrame(body, record);
            }
            byte[] payload = bodyBytes.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(payload);
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(encoded)) {
                output.writeInt(RECORD_MAGIC);
                output.writeInt(RECORD_VERSION);
                output.writeInt(payload.length);
                output.write(payload);
                output.writeInt((int) crc.getValue());
            }
            return encoded.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory temporal state encoding failed",
                    impossible);
        }
    }

    private void reserve(long encodedBytes) {
        if (stateEntries >= maximumEntries) {
            throw failure(StreamTemporalJoinException.Code.STATE_ENTRY_LIMIT,
                    "temporal join state entry count exceeds its bound");
        }
        if (encodedBytes > maximumStateBytes - stateBytes) {
            throw failure(StreamTemporalJoinException.Code.STATE_BYTE_LIMIT,
                    "temporal join state bytes exceed their bound");
        }
    }

    private long encodedSize(byte[] key, byte[] record) {
        return RECORD_FIXED_BYTES + (long) key.length + record.length;
    }

    private void delete(StoredRecord stored) {
        try {
            Files.delete(stored.path);
            stateEntries--;
            stateBytes -= stored.bytes;
        } catch (IOException deleteFailure) {
            StreamTemporalJoinException poisoned = failure(
                    StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot clean temporal join state", deleteFailure);
            terminalFailure = poisoned;
            throw poisoned;
        }
    }

    private long nextSequence(Side side) {
        long current = side == Side.LEFT ? leftSequence : rightSequence;
        if (current == Long.MAX_VALUE) {
            throw failure(StreamTemporalJoinException.Code.STATE_ENTRY_LIMIT,
                    "temporal join event sequence exhausted");
        }
        current++;
        if (side == Side.LEFT) leftSequence = current;
        else rightSequence = current;
        return current;
    }

    private void validateKey(byte[] key) {
        if (key.length == 0 || key.length > maximumKeyBytes) {
            throw failure(StreamTemporalJoinException.Code.FRAME_LIMIT,
                    "temporal join key exceeds its frame bound");
        }
    }

    private void validateRecord(byte[] record) {
        if (record == null || record.length == 0
                || record.length > maximumRecordBytes) {
            throw failure(StreamTemporalJoinException.Code.FRAME_LIMIT,
                    "temporal join record exceeds its frame bound");
        }
    }

    private void requireOwned(byte[] key) {
        int group = ManagedKeyedValueState.keyGroup(key, maxParallelism);
        if (group < keyGroupStart || group > keyGroupEnd) {
            throw failure(StreamTemporalJoinException.Code.UNOWNED_KEY_GROUP,
                    "temporal join key-group is not owned by this subtask");
        }
    }

    private void checkOwnerAndHealthy() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamTemporalJoinException.Code.WRONG_THREAD,
                    "temporal join state is owned by one mailbox thread");
        }
        if (closed) {
            throw failure(StreamTemporalJoinException.Code.CLOSED,
                    "temporal join is closed");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private static NavigableMap<EventId, StoredRecord> index(
            Map<Key, NavigableMap<EventId, StoredRecord>> values, Key key) {
        return values.get(key);
    }

    private static long saturatedAdd(long value, long delta) {
        if (delta > 0L && value > Long.MAX_VALUE - delta) {
            return Long.MAX_VALUE;
        }
        if (delta < 0L && value < Long.MIN_VALUE - delta) {
            return Long.MIN_VALUE;
        }
        return value + delta;
    }

    private static void writeFrame(DataOutputStream output, byte[] frame)
            throws IOException {
        output.writeInt(frame.length);
        output.write(frame);
    }

    private static byte[] readFrame(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum) corrupt();
        byte[] frame = new byte[length];
        input.readFully(frame);
        return frame;
    }

    private static void lateRecord() {
        throw failure(StreamTemporalJoinException.Code.LATE_RECORD,
                "temporal join record is behind the opposite cleanup horizon");
    }

    private static void watermarkRegression() {
        throw failure(StreamTemporalJoinException.Code.WATERMARK_REGRESSION,
                "temporal join watermark must not move backwards");
    }

    private static void corrupt() {
        throw failure(StreamTemporalJoinException.Code.IO_FAILURE,
                "temporal join state frame is corrupt");
    }

    @Override
    public void close() {
        if (closed) return;
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamTemporalJoinException.Code.WRONG_THREAD,
                    "temporal join state is owned by one mailbox thread");
        }
        closed = true;
        StreamTemporalJoinException cleanupFailure = null;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                workingDirectory)) {
            for (Path file : files) Files.deleteIfExists(file);
        } catch (IOException failure) {
            cleanupFailure = failure(StreamTemporalJoinException.Code.IO_FAILURE,
                    "cannot remove temporal join state directory", failure);
        }
        try {
            Files.deleteIfExists(workingDirectory);
        } catch (IOException failure) {
            if (cleanupFailure == null) {
                cleanupFailure = failure(
                        StreamTemporalJoinException.Code.IO_FAILURE,
                        "cannot remove temporal join state directory", failure);
            } else {
                cleanupFailure.addSuppressed(failure);
            }
        } finally {
            left.clear();
            right.clear();
            stateEntries = 0;
            stateBytes = 0L;
        }
        if (cleanupFailure != null) throw cleanupFailure;
    }

    private static StreamTemporalJoinException failure(
            StreamTemporalJoinException.Code code, String message) {
        return new StreamTemporalJoinException(code, message);
    }

    private static StreamTemporalJoinException failure(
            StreamTemporalJoinException.Code code, String message,
            Throwable cause) {
        return new StreamTemporalJoinException(code, message, cause);
    }

    private static final class Key {
        private final byte[] bytes;
        private final int hash;

        private Key(byte[] bytes) {
            this.bytes = bytes.clone();
            this.hash = Arrays.hashCode(this.bytes);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Key
                    && Arrays.equals(bytes, ((Key) other).bytes);
        }

        @Override
        public int hashCode() { return hash; }
    }

    private static final class EventId implements Comparable<EventId> {
        private final long timestamp;
        private final long sequence;

        private EventId(long timestamp, long sequence) {
            this.timestamp = timestamp;
            this.sequence = sequence;
        }

        @Override
        public int compareTo(EventId other) {
            int time = Long.compare(timestamp, other.timestamp);
            return time != 0 ? time : Long.compare(sequence, other.sequence);
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof EventId)) return false;
            EventId that = (EventId) other;
            return timestamp == that.timestamp && sequence == that.sequence;
        }

        @Override public int hashCode() {
            return 31 * Long.hashCode(timestamp) + Long.hashCode(sequence);
        }
    }

    private static final class DecodedRecord {
        private final Side side;
        private final EventId event;
        private final byte[] key;
        private final byte[] record;

        private DecodedRecord(
                Side side, EventId event, byte[] key, byte[] record) {
            this.side = side;
            this.event = event;
            this.key = key;
            this.record = record;
        }
    }

    private static final class TemporalSnapshotSource {
        private final int subtaskIndex;
        private final int keyGroupStart;
        private final int keyGroupEnd;
        private final String contentDigest;
        private final long contentBytes;

        private TemporalSnapshotSource(int subtaskIndex,
                int keyGroupStart, int keyGroupEnd,
                String contentDigest, long contentBytes) {
            this.subtaskIndex = subtaskIndex;
            this.keyGroupStart = keyGroupStart;
            this.keyGroupEnd = keyGroupEnd;
            this.contentDigest = contentDigest;
            this.contentBytes = contentBytes;
        }
    }

    private static final class RestoredSnapshot {
        private final Map<Key, NavigableMap<EventId, StoredRecord>> left;
        private final Map<Key, NavigableMap<EventId, StoredRecord>> right;
        private final List<Path> stagedFiles;
        private final long leftWatermark;
        private final long rightWatermark;
        private final long leftSequence;
        private final long rightSequence;
        private final int stateEntries;
        private final long stateBytes;

        private RestoredSnapshot(
                Map<Key, NavigableMap<EventId, StoredRecord>> left,
                Map<Key, NavigableMap<EventId, StoredRecord>> right,
                List<Path> stagedFiles, long leftWatermark,
                long rightWatermark, long leftSequence,
                long rightSequence, int stateEntries, long stateBytes) {
            this.left = left;
            this.right = right;
            this.stagedFiles = stagedFiles;
            this.leftWatermark = leftWatermark;
            this.rightWatermark = rightWatermark;
            this.leftSequence = leftSequence;
            this.rightSequence = rightSequence;
            this.stateEntries = stateEntries;
            this.stateBytes = stateBytes;
        }
    }

    private static final class StoredRecord {
        private final Path path;
        private final long bytes;

        private StoredRecord(Path path, long bytes) {
            this.path = path;
            this.bytes = bytes;
        }
    }

    private static final class BoundedOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final long maximumBytes;
        private long written;

        private BoundedOutputStream(OutputStream delegate, long maximumBytes) {
            this.delegate = delegate;
            this.maximumBytes = maximumBytes;
        }

        @Override public void write(int value) throws IOException {
            reserveWrite(1);
            delegate.write(value);
        }

        @Override public void write(byte[] bytes, int offset, int length)
                throws IOException {
            if (bytes == null || offset < 0 || length < 0
                    || offset > bytes.length - length) {
                throw new IndexOutOfBoundsException();
            }
            reserveWrite(length);
            delegate.write(bytes, offset, length);
        }

        @Override public void flush() throws IOException { delegate.flush(); }
        @Override public void close() throws IOException { delegate.close(); }

        private void reserveWrite(int bytes) {
            if (bytes > maximumBytes - written) {
                throw new StreamTemporalJoinException(
                        StreamTemporalJoinException.Code.STATE_BYTE_LIMIT,
                        "temporal checkpoint exceeds its byte bound");
            }
            written += bytes;
        }
    }
}
