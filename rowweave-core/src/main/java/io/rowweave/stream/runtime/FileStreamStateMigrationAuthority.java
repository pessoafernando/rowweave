package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamStateMigrationBinding;
import io.rowweave.stream.contract.StreamStateMigrationBindingCodec;

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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/** Durable expected-closure and single active pointer for state migration. */
public final class FileStreamStateMigrationAuthority {
    private static final byte[] PLAN_MAGIC =
            "RWXXSMG1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ACK_MAGIC =
            "RWXXSAK1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ACTIVE_MAGIC =
            "RWXXSAC1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_STATES = 65_536;
    private static final int MAX_BODY_BYTES = 16 * 1024 * 1024;
    private static final ReentrantLock[] JVM_LOCKS = new ReentrantLock[256];

    static {
        for (int index = 0; index < JVM_LOCKS.length; index++) {
            JVM_LOCKS[index] = new ReentrantLock();
        }
    }

    public static final class ExpectedState {
        private final StreamStateMigrationBinding binding;
        private final StreamCheckpointMember sourceMember;

        private ExpectedState(
                StreamStateMigrationBinding binding,
                StreamCheckpointMember sourceMember) {
            validateSource(binding, sourceMember);
            this.binding = binding;
            this.sourceMember = sourceMember;
        }

        public static ExpectedState of(
                StreamStateMigrationBinding binding,
                StreamCheckpointMember sourceMember) {
            return new ExpectedState(binding, sourceMember);
        }

        public StreamStateMigrationBinding binding() { return binding; }
        public StreamCheckpointMember sourceMember() { return sourceMember; }

        private String identity() {
            return sha256Hex(concat(
                    StreamStateMigrationBindingCodec.encode(binding),
                    StreamCheckpointMemberCodec.encode(sourceMember)));
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof ExpectedState)) return false;
            ExpectedState that = (ExpectedState) other;
            return binding.equals(that.binding)
                    && sourceMember.equals(that.sourceMember);
        }

        @Override public int hashCode() {
            return Objects.hash(binding, sourceMember);
        }
    }

    public static final class CompletedMigration {
        private final String migrationId;
        private final long generation;
        private final String sourceSavepointDigest;
        private final String targetArtifactId;
        private final List<StreamCheckpointMember> targetMembers;

        private CompletedMigration(
                String migrationId, long generation,
                String sourceSavepointDigest,
                String targetArtifactId,
                List<StreamCheckpointMember> targetMembers) {
            this.migrationId = requireText(migrationId, "migration id", 256);
            if (generation < 1L) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_INCOMPATIBLE,
                        "migration generation is invalid");
            }
            this.generation = generation;
            this.sourceSavepointDigest = requireDigest(
                    sourceSavepointDigest, "source savepoint digest");
            this.targetArtifactId = requireDigest(
                    targetArtifactId, "target artifact id");
            if (targetMembers == null || targetMembers.isEmpty()
                    || targetMembers.size() > MAX_STATES) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_INCOMPATIBLE,
                        "completed migration target closure is invalid");
            }
            this.targetMembers = immutableMembers(targetMembers);
        }

        public String migrationId() { return migrationId; }
        public long generation() { return generation; }
        public String sourceSavepointDigest() {
            return sourceSavepointDigest;
        }
        public String targetArtifactId() { return targetArtifactId; }
        public List<StreamCheckpointMember> targetMembers() {
            return targetMembers;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof CompletedMigration)) return false;
            CompletedMigration that = (CompletedMigration) other;
            return generation == that.generation
                    && migrationId.equals(that.migrationId)
                    && sourceSavepointDigest.equals(that.sourceSavepointDigest)
                    && targetArtifactId.equals(that.targetArtifactId)
                    && targetMembers.equals(that.targetMembers);
        }

        @Override public int hashCode() {
            return Objects.hash(migrationId, generation, sourceSavepointDigest,
                    targetArtifactId, targetMembers);
        }
    }

    private static final class Plan {
        private final String migrationId;
        private final long generation;
        private final String sourceSavepointDigest;
        private final String targetArtifactId;
        private final List<ExpectedState> expected;

        private Plan(String migrationId, long generation,
                     String sourceSavepointDigest,
                     String targetArtifactId, List<ExpectedState> expected) {
            this.migrationId = migrationId;
            this.generation = generation;
            this.sourceSavepointDigest = sourceSavepointDigest;
            this.targetArtifactId = targetArtifactId;
            this.expected = expected;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof Plan)) return false;
            Plan that = (Plan) other;
            return generation == that.generation
                    && migrationId.equals(that.migrationId)
                    && sourceSavepointDigest.equals(that.sourceSavepointDigest)
                    && targetArtifactId.equals(that.targetArtifactId)
                    && expected.equals(that.expected);
        }

        @Override public int hashCode() {
            return Objects.hash(migrationId, generation, sourceSavepointDigest,
                    targetArtifactId, expected);
        }
    }

    private static final class Acknowledgement {
        private final ExpectedState expected;
        private final StreamCheckpointMember targetMember;

        private Acknowledgement(
                ExpectedState expected,
                StreamCheckpointMember targetMember) {
            validateTarget(expected, targetMember);
            this.expected = expected;
            this.targetMember = targetMember;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof Acknowledgement)) return false;
            Acknowledgement that = (Acknowledgement) other;
            return expected.equals(that.expected)
                    && targetMember.equals(that.targetMember);
        }

        @Override public int hashCode() {
            return Objects.hash(expected, targetMember);
        }
    }

    private final Path root;
    private final Path migrations;
    private final Path activeFile;
    private final Path lockFile;
    private final FileCheckpointChunkStore shadowStore;
    private final ReentrantLock jvmLock;

    public FileStreamStateMigrationAuthority(
            Path root, FileCheckpointChunkStore shadowStore) {
        if (root == null || shadowStore == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "migration authority root and shadow store are required");
        }
        this.root = root.toAbsolutePath().normalize();
        this.migrations = this.root.resolve("migrations");
        this.activeFile = this.root.resolve("active.frame");
        this.lockFile = this.root.resolve("authority.lock");
        this.shadowStore = shadowStore;
        this.jvmLock = JVM_LOCKS[(this.root.hashCode() & Integer.MAX_VALUE)
                % JVM_LOCKS.length];
        ensureDirectory(this.root);
        ensureDirectory(migrations);
        ensureLockFile();
    }

    public void begin(
            String migrationId, long generation,
            String sourceSavepointDigest,
            String targetArtifactId, List<ExpectedState> expectedStates) {
        String id = requireText(migrationId, "migration id", 256);
        String sourceDigest = requireDigest(
                sourceSavepointDigest, "source savepoint digest");
        String artifact = requireDigest(
                targetArtifactId, "target artifact id");
        if (generation < 1L) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_INCOMPATIBLE,
                    "migration generation is invalid");
        }
        List<ExpectedState> expected = canonicalExpected(expectedStates);
        Plan proposed = new Plan(
                id, generation, sourceDigest, artifact, expected);
        locked(() -> {
            Path directory = migrationDirectory(id);
            ensureDirectory(directory);
            ensureDirectory(directory.resolve("acks"));
            Path planFile = directory.resolve("plan.frame");
            Plan existing = readPlan(planFile);
            if (existing != null) {
                if (!existing.equals(proposed)) conflict();
                return null;
            }
            publishImmutable(planFile, frame(PLAN_MAGIC, encodePlan(proposed)));
            if (!proposed.equals(readPlan(planFile))) conflict();
            return null;
        });
    }

    public void acknowledge(
            String migrationId, StreamStateMigrationBinding binding,
            StreamCheckpointMember sourceMember,
            StreamCheckpointMember targetMember) {
        String id = requireText(migrationId, "migration id", 256);
        ExpectedState expected = ExpectedState.of(binding, sourceMember);
        Acknowledgement proposed = new Acknowledgement(expected, targetMember);
        locked(() -> {
            Plan plan = requirePlan(id);
            ExpectedState canonical = findExpected(plan, expected);
            Acknowledgement exact = new Acknowledgement(
                    canonical, targetMember);
            Path ackFile = acknowledgementPath(id, canonical);
            Acknowledgement existing = readAcknowledgement(ackFile);
            if (existing != null) {
                if (!existing.equals(exact)) conflict();
                return null;
            }
            requireTargetPayload(exact.targetMember, false);
            publishImmutable(ackFile,
                    frame(ACK_MAGIC, encodeAcknowledgement(exact)));
            if (!exact.equals(readAcknowledgement(ackFile))) conflict();
            return null;
        });
    }

    public CompletedMigration cutover(String migrationId) {
        String id = requireText(migrationId, "migration id", 256);
        return locked(() -> {
            CompletedMigration active = readActive();
            if (active != null && active.migrationId.equals(id)) {
                verifyCompleted(active);
                return active;
            }
            Plan plan = requirePlan(id);
            long expectedGeneration = active == null
                    ? 1L : Math.addExact(active.generation, 1L);
            if (plan.generation != expectedGeneration) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_INCOMPATIBLE,
                        "migration generation cannot replace active state");
            }
            List<StreamCheckpointMember> targets =
                    new ArrayList<>(plan.expected.size());
            for (ExpectedState expected : plan.expected) {
                Acknowledgement acknowledgement = readAcknowledgement(
                        acknowledgementPath(id, expected));
                if (acknowledgement == null
                        || !acknowledgement.expected.equals(expected)) {
                    throw failure(StreamCheckpointException.Code
                                    .STATE_MIGRATION_INCOMPLETE,
                            "migration expected closure is incomplete");
                }
                requireTargetPayload(acknowledgement.targetMember, true);
                targets.add(acknowledgement.targetMember);
            }
            CompletedMigration proposed = new CompletedMigration(
                    plan.migrationId, plan.generation,
                    plan.sourceSavepointDigest,
                    plan.targetArtifactId, targets);
            writeReplace(activeFile,
                    frame(ACTIVE_MAGIC, encodeCompleted(proposed)));
            CompletedMigration durable = readActive();
            if (!proposed.equals(durable)) conflict();
            return durable;
        });
    }

    public CompletedMigration active() {
        return locked(this::readActive);
    }

    private void verifyCompleted(CompletedMigration completed) {
        Plan plan = requirePlan(completed.migrationId);
        if (!plan.sourceSavepointDigest.equals(
                completed.sourceSavepointDigest)
                || plan.generation != completed.generation
                || !plan.targetArtifactId.equals(completed.targetArtifactId)
                || plan.expected.size() != completed.targetMembers.size()) {
            corrupt("active migration does not match its plan");
        }
        for (int index = 0; index < plan.expected.size(); index++) {
            validateTarget(plan.expected.get(index),
                    completed.targetMembers.get(index));
            requireTargetPayload(completed.targetMembers.get(index), true);
        }
    }

    private void requireTargetPayload(
            StreamCheckpointMember member, boolean verifyBytes) {
        FileCheckpointChunkStore.StoredChunkedPayload payload =
                shadowStore.get(member.contentDigest());
        if (payload == null || payload.contentBytes() != member.contentBytes()) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_OUTPUT_MISMATCH,
                    "shadow target payload is missing or differs");
        }
        if (verifyBytes) shadowStore.verifyStored(payload);
    }

    private Plan requirePlan(String migrationId) {
        Plan plan = readPlan(migrationDirectory(migrationId)
                .resolve("plan.frame"));
        if (plan == null || !migrationId.equals(plan.migrationId)) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_INCOMPATIBLE,
                    "migration plan does not exist");
        }
        return plan;
    }

    private static ExpectedState findExpected(
            Plan plan, ExpectedState proposed) {
        for (ExpectedState expected : plan.expected) {
            if (expected.equals(proposed)) return expected;
        }
        throw failure(StreamCheckpointException.Code
                        .STATE_MIGRATION_INCOMPATIBLE,
                "state is not in the expected migration closure");
    }

    private Path migrationDirectory(String migrationId) {
        return migrations.resolve(sha256Hex(
                migrationId.getBytes(StandardCharsets.UTF_8)));
    }

    private Path acknowledgementPath(
            String migrationId, ExpectedState expected) {
        return migrationDirectory(migrationId).resolve("acks")
                .resolve(expected.identity() + ".frame");
    }

    private Plan readPlan(Path path) {
        if (!Files.exists(path)) return null;
        byte[] body = readFrame(path, PLAN_MAGIC);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            String id = requireText(
                    readText(input, 256), "migration id", 256);
            long generation = input.readLong();
            if (generation < 1L) {
                throw corrupt("migration generation is invalid");
            }
            String source = requireDigest(readText(input, 64),
                    "source savepoint digest");
            String artifact = requireDigest(readText(input, 64),
                    "target artifact id");
            int count = readCount(input);
            List<ExpectedState> expected = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                expected.add(readExpected(input));
            }
            requireEnd(input);
            return new Plan(id, generation, source, artifact,
                    canonicalExpected(expected));
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException | IOException malformed) {
            throw corrupt("migration plan is malformed", malformed);
        }
    }

    private Acknowledgement readAcknowledgement(Path path) {
        if (!Files.exists(path)) return null;
        byte[] body = readFrame(path, ACK_MAGIC);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            ExpectedState expected = readExpected(input);
            StreamCheckpointMember target = StreamCheckpointMemberCodec.decode(
                    readBytes(input, 8192));
            requireEnd(input);
            return new Acknowledgement(expected, target);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException | IOException malformed) {
            throw corrupt("migration acknowledgement is malformed",
                    malformed);
        }
    }

    private CompletedMigration readActive() {
        if (!Files.exists(activeFile)) return null;
        byte[] body = readFrame(activeFile, ACTIVE_MAGIC);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            String id = readText(input, 256);
            long generation = input.readLong();
            String source = readText(input, 64);
            String artifact = readText(input, 64);
            int count = readCount(input);
            List<StreamCheckpointMember> targets = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                targets.add(StreamCheckpointMemberCodec.decode(
                        readBytes(input, 8192)));
            }
            requireEnd(input);
            return new CompletedMigration(
                    id, generation, source, artifact, targets);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException | IOException malformed) {
            throw corrupt("active migration pointer is malformed", malformed);
        }
    }

    private static byte[] encodePlan(Plan plan) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, plan.migrationId);
                output.writeLong(plan.generation);
                writeText(output, plan.sourceSavepointDigest);
                writeText(output, plan.targetArtifactId);
                output.writeInt(plan.expected.size());
                for (ExpectedState expected : plan.expected) {
                    writeExpected(output, expected);
                }
            }
            return requireBody(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] encodeAcknowledgement(
            Acknowledgement acknowledgement) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeExpected(output, acknowledgement.expected);
                writeBytes(output, StreamCheckpointMemberCodec.encode(
                        acknowledgement.targetMember));
            }
            return requireBody(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] encodeCompleted(CompletedMigration completed) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, completed.migrationId);
                output.writeLong(completed.generation);
                writeText(output, completed.sourceSavepointDigest);
                writeText(output, completed.targetArtifactId);
                output.writeInt(completed.targetMembers.size());
                for (StreamCheckpointMember member : completed.targetMembers) {
                    writeBytes(output, StreamCheckpointMemberCodec.encode(member));
                }
            }
            return requireBody(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void writeExpected(
            DataOutputStream output, ExpectedState expected)
            throws IOException {
        writeBytes(output, StreamStateMigrationBindingCodec.encode(
                expected.binding));
        writeBytes(output, StreamCheckpointMemberCodec.encode(
                expected.sourceMember));
    }

    private static ExpectedState readExpected(DataInputStream input)
            throws IOException {
        StreamStateMigrationBinding binding =
                StreamStateMigrationBindingCodec.decode(
                        readBytes(input,
                                StreamStateMigrationBindingCodec.MAX_FRAME_BYTES));
        StreamCheckpointMember source = StreamCheckpointMemberCodec.decode(
                readBytes(input, 8192));
        return ExpectedState.of(binding, source);
    }

    private static List<ExpectedState> canonicalExpected(
            List<ExpectedState> states) {
        if (states == null || states.isEmpty() || states.size() > MAX_STATES) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_INCOMPATIBLE,
                    "migration expected closure is invalid");
        }
        ArrayList<ExpectedState> copy = new ArrayList<>(states.size());
        for (ExpectedState state : states) {
            if (state == null) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_INCOMPATIBLE,
                        "migration expected closure contains null");
            }
            copy.add(state);
        }
        copy.sort(Comparator
                .comparing((ExpectedState state) ->
                        state.sourceMember.operatorUid())
                .thenComparing(state -> state.sourceMember.subjectId())
                .thenComparingInt(state -> state.sourceMember.subtaskIndex())
                .thenComparingInt(state -> state.sourceMember.keyGroupStart())
                .thenComparingInt(state -> state.sourceMember.keyGroupEnd())
                .thenComparing(ExpectedState::identity));
        for (int index = 1; index < copy.size(); index++) {
            ExpectedState previous = copy.get(index - 1);
            ExpectedState current = copy.get(index);
            if (previous.identity().equals(current.identity())
                    || sameState(previous, current)
                    && previous.sourceMember.keyGroupEnd()
                    >= current.sourceMember.keyGroupStart()) {
                conflict();
            }
        }
        return Collections.unmodifiableList(copy);
    }

    private static boolean sameState(
            ExpectedState left, ExpectedState right) {
        return left.sourceMember.operatorUid().equals(
                right.sourceMember.operatorUid())
                && left.sourceMember.subjectId().equals(
                        right.sourceMember.subjectId());
    }

    private static List<StreamCheckpointMember> immutableMembers(
            List<StreamCheckpointMember> members) {
        ArrayList<StreamCheckpointMember> copy = new ArrayList<>(members.size());
        for (StreamCheckpointMember member : members) {
            if (member == null
                    || member.kind() != StreamCheckpointMember.Kind.STATE) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_INCOMPATIBLE,
                        "completed migration contains invalid target state");
            }
            copy.add(member);
        }
        return Collections.unmodifiableList(copy);
    }

    private static void validateSource(
            StreamStateMigrationBinding binding,
            StreamCheckpointMember source) {
        if (binding == null || source == null
                || source.kind() != StreamCheckpointMember.Kind.STATE) {
            incompatible();
        }
        StreamStateDescriptor descriptor = binding.sourceState();
        if (!binding.operatorUid().equals(source.operatorUid())
                || !descriptor.stateName().equals(source.subjectId())
                || !descriptor.serializerContractId().equals(source.codecId())
                || descriptor.stateSchemaVersion() != source.schemaVersion()) {
            incompatible();
        }
    }

    private static void validateTarget(
            ExpectedState expected, StreamCheckpointMember target) {
        StreamStateMigrationBinding binding = expected.binding;
        StreamCheckpointMember source = expected.sourceMember;
        StreamStateDescriptor descriptor = binding.targetState();
        if (target == null
                || target.kind() != StreamCheckpointMember.Kind.STATE
                || !binding.operatorUid().equals(target.operatorUid())
                || source.subtaskIndex() != target.subtaskIndex()
                || source.keyGroupStart() != target.keyGroupStart()
                || source.keyGroupEnd() != target.keyGroupEnd()
                || !descriptor.stateName().equals(target.subjectId())
                || !descriptor.serializerContractId().equals(target.codecId())
                || descriptor.stateSchemaVersion() != target.schemaVersion()
                || !binding.targetContentDigest().equals(
                        target.contentDigest())) {
            incompatible();
        }
    }

    private static void incompatible() {
        throw failure(StreamCheckpointException.Code
                        .STATE_MIGRATION_INCOMPATIBLE,
                "state migration member does not match its binding");
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
                    "migration authority lock failed", ioFailure);
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
            requireRegularFile(lockFile);
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "migration authority lock initialization failed",
                    ioFailure);
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "migration authority path is not a directory");
            }
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "migration authority directory failed", ioFailure);
        }
    }

    private static void publishImmutable(Path target, byte[] content) {
        Path temporary = target.getParent().resolve(
                ".rowweave-stream-migration-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            writeNewForced(temporary, content);
            try {
                Files.createLink(target, temporary);
                forceDirectory(target.getParent());
            } catch (FileAlreadyExistsException collision) {
                primary = collision;
            }
        } catch (IOException | UnsupportedOperationException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "immutable migration metadata publication failed",
                    ioFailure);
        } finally {
            deleteTemporary(temporary, primary);
        }
    }

    private static void writeReplace(Path target, byte[] content) {
        Path temporary = target.getParent().resolve(
                ".rowweave-stream-migration-active-"
                        + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            writeNewForced(temporary, content);
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(target.getParent());
        } catch (IOException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "migration active pointer publication failed", ioFailure);
        } finally {
            deleteTemporary(temporary, primary);
        }
    }

    private static void writeNewForced(Path path, byte[] content)
            throws IOException {
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
    }

    private static void deleteTemporary(Path path, Throwable primary) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "migration temporary cleanup failed", cleanupFailure);
        }
    }

    private static byte[] readFrame(Path path, byte[] magic) {
        requireRegularFile(path);
        try {
            long size = Files.size(path);
            if (size < HEADER_BYTES + DIGEST_BYTES
                    || size > HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES) {
                throw corrupt("migration metadata size is invalid");
            }
            byte[] frame = Files.readAllBytes(path);
            if (!MessageDigest.isEqual(magic,
                    java.util.Arrays.copyOfRange(frame, 0, 8))) {
                throw corrupt("migration metadata magic is invalid");
            }
            int version = ByteBuffer.wrap(frame, 8, 4).getInt();
            int length = ByteBuffer.wrap(frame, 12, 4).getInt();
            if (version != 1 || length < 0 || length > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) length + DIGEST_BYTES
                    != frame.length) {
                throw corrupt("migration metadata frame is invalid");
            }
            byte[] body = java.util.Arrays.copyOfRange(
                    frame, HEADER_BYTES, HEADER_BYTES + length);
            byte[] digest = java.util.Arrays.copyOfRange(
                    frame, HEADER_BYTES + length, frame.length);
            if (!MessageDigest.isEqual(sha256(body), digest)) {
                throw corrupt("migration metadata checksum differs");
            }
            return body;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException ioFailure) {
            throw corrupt("migration metadata cannot be read", ioFailure);
        }
    }

    private static byte[] frame(byte[] magic, byte[] body) {
        requireBody(body);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(magic);
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

    private static byte[] requireBody(byte[] body) {
        if (body.length > MAX_BODY_BYTES) {
            throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    "migration metadata exceeds its bound");
        }
        return body;
    }

    private static void requireRegularFile(Path path) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw corrupt("migration metadata is not a regular file");
        }
    }

    private static void forceDirectory(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum || length > input.available()) {
            throw corrupt("migration metadata text exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw corrupt("migration metadata text is malformed", malformed);
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] bytes)
            throws IOException {
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static byte[] readBytes(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum || length > input.available()) {
            throw corrupt("migration nested frame exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    private static int readCount(DataInputStream input) throws IOException {
        int count = input.readInt();
        if (count < 1 || count > MAX_STATES
                || count > input.available() / 8 + 1) {
            throw corrupt("migration state count exceeds its bound");
        }
        return count;
    }

    private static void requireEnd(DataInputStream input) throws IOException {
        if (input.available() != 0) {
            throw corrupt("migration metadata has trailing bytes");
        }
    }

    private static String requireText(
            String value, String field, int maximumBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)
                || value.getBytes(StandardCharsets.UTF_8).length
                > maximumBytes) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_INCOMPATIBLE,
                    field + " is invalid");
        }
        return value;
    }

    private static String requireDigest(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_INCOMPATIBLE,
                    field + " is invalid");
        }
        return value;
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = new byte[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value);
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[digest.length * 2];
        for (int index = 0; index < digest.length; index++) {
            result[index * 2] = digits[(digest[index] >>> 4) & 0xf];
            result[index * 2 + 1] = digits[digest[index] & 0xf];
        }
        return new String(result);
    }

    private static void conflict() {
        throw failure(StreamCheckpointException.Code
                        .STATE_MIGRATION_INCOMPATIBLE,
                "migration durable identity conflicts");
    }

    private static StreamCheckpointException corrupt(String message) {
        return corrupt(message, null);
    }

    private static StreamCheckpointException corrupt(
            String message, Throwable cause) {
        return cause == null
                ? failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                message)
                : failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
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
