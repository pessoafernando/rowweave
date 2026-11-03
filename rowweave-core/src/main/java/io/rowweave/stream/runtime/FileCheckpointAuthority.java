package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointDecisionCodec;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointManifestCodec;
import io.rowweave.stream.contract.StreamEdge;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

/** Durable immutable first-decision-wins authority for Stream checkpoints. */
public final class FileCheckpointAuthority implements StreamCheckpointAuthority {
    enum PublicationStep {
        MANIFEST_DURABLE, DECISION_DURABLE, COMPLETION_DURABLE
    }

    interface PublicationObserver {
        void after(PublicationStep step, Path path);
    }

    private static final PublicationObserver NOOP = (step, path) -> { };

    private final Path manifestDirectory;
    private final Path checkpointDirectory;
    private final PublicationObserver observer;

    public FileCheckpointAuthority(Path root) {
        this(root, NOOP);
    }

    FileCheckpointAuthority(Path root, PublicationObserver observer) {
        if (root == null || observer == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint authority root and observer are required");
        }
        Path normalized = root.toAbsolutePath().normalize();
        manifestDirectory = normalized.resolve("manifests");
        checkpointDirectory = normalized.resolve("checkpoints");
        this.observer = observer;
        ensureDirectory(normalized);
        ensureDirectory(manifestDirectory);
        ensureDirectory(checkpointDirectory);
    }

    /** Publishes only ABORTED or COMMIT_DECIDED as the immutable first decision. */
    public CheckpointDecision publishDecision(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        validateFirstDecision(proposed, manifest);
        CheckpointIdentity identity = proposed.checkpointIdentity();
        Path decisionPath = decisionPath(identity);
        CheckpointDecision existing = readFrameDecision(decisionPath, identity);
        if (existing != null) {
            CheckpointDecision exact = requireExact(existing, proposed);
            if (exact.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
                publishManifest(exact, manifest);
            }
            return exact;
        }

        if (proposed.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
            publishManifest(proposed, manifest);
        }
        publishImmutable(decisionPath, CheckpointDecisionCodec.encode(proposed));
        observer.after(PublicationStep.DECISION_DURABLE, decisionPath);
        return requireExact(readFrameDecision(decisionPath, identity), proposed);
    }

    /** Adds an immutable completion marker without replacing the commit decision. */
    public CheckpointDecision complete(CheckpointDecision completed) {
        if (completed == null
                || completed.status() != CheckpointDecision.Status.COMPLETED) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "completion marker must carry COMPLETED status");
        }
        CheckpointIdentity identity = completed.checkpointIdentity();
        CheckpointDecision committed = readRawDecision(identity);
        if (committed == null
                || committed.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                || !committed.manifestDigest().equals(
                        completed.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "completion requires the exact durable commit decision");
        }
        requireManifest(committed);
        Path path = completionPath(identity);
        CheckpointDecision existing = readFrameDecision(path, identity);
        if (existing != null) return requireExact(existing, completed);
        publishImmutable(path, CheckpointDecisionCodec.encode(completed));
        observer.after(PublicationStep.COMPLETION_DURABLE, path);
        return requireExact(readFrameDecision(path, identity), completed);
    }

    /** Returns COMPLETED when its marker exists, otherwise the first decision. */
    public CheckpointDecision decision(CheckpointIdentity identity) {
        requireIdentity(identity);
        CheckpointDecision first = firstDecision(identity);
        CheckpointDecision completion = readFrameDecision(
                completionPath(identity), identity);
        if (completion == null) return first;
        if (first == null
                || first.status() != CheckpointDecision.Status.COMMIT_DECIDED
                || completion.status() != CheckpointDecision.Status.COMPLETED
                || !first.manifestDigest().equals(
                        completion.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "checkpoint completion is detached from its commit decision");
        }
        return completion;
    }

    /** Returns the immutable ABORTED/COMMIT_DECIDED linearization point. */
    public CheckpointDecision firstDecision(CheckpointIdentity identity) {
        requireIdentity(identity);
        CheckpointDecision first = readRawDecision(identity);
        if (first != null
                && first.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
            requireManifest(first);
        }
        return first;
    }

    /** Lists durable decisions for one run, including completion markers. */
    public List<CheckpointDecision> decisionsForRun(String runId) {
        if (runId == null || runId.isEmpty()) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint run identity is required");
        }
        List<CheckpointDecision> result = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> entries =
                     Files.newDirectoryStream(
                             checkpointDirectory, "*.decision")) {
            for (Path path : entries) {
                CheckpointDecision first = CheckpointDecisionCodec.decode(
                        readBounded(path));
                CheckpointIdentity identity = first.checkpointIdentity();
                if (!path.equals(decisionPath(identity))) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                            "checkpoint decision identity does not match its name");
                }
                if (runId.equals(identity.runId())) {
                    result.add(decision(identity));
                }
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint decision catalog is unavailable", failure);
        }
        result.sort(Comparator.comparingLong(value ->
                value.checkpointIdentity().checkpointSequence()));
        return Collections.unmodifiableList(result);
    }

    private CheckpointManifest requireManifest(CheckpointDecision decision) {
        CheckpointManifest durable = manifest(decision.manifestDigest());
        if (durable == null
                || !decision.checkpointIdentity().equals(
                        durable.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "commit decision has no exact durable manifest");
        }
        return durable;
    }

    public CheckpointManifest manifest(String manifestDigest) {
        requireDigest(manifestDigest);
        Path path = manifestPath(manifestDigest);
        if (!Files.exists(path)) return null;
        try {
            CheckpointManifest decoded = CheckpointManifestCodec.decode(
                    readBounded(path));
            if (!manifestDigest.equals(decoded.manifestDigest())) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint manifest digest does not match its name");
            }
            return decoded;
        } catch (StreamCheckpointException corruption) {
            throw corruption;
        } catch (RuntimeException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "checkpoint manifest frame is corrupt", malformed);
        }
    }

    Path decisionPath(CheckpointIdentity identity) {
        requireIdentity(identity);
        return checkpointDirectory.resolve(identityDigest(identity) + ".decision");
    }

    Path manifestPath(String digest) {
        requireDigest(digest);
        return manifestDirectory.resolve(digest + ".manifest");
    }

    private Path completionPath(CheckpointIdentity identity) {
        requireIdentity(identity);
        return checkpointDirectory.resolve(identityDigest(identity) + ".completed");
    }

    private void publishManifest(
            CheckpointDecision decision, CheckpointManifest manifest) {
        String digest = decision.manifestDigest();
        Path path = manifestPath(digest);
        CheckpointManifest existing = manifest(digest);
        if (existing != null) {
            if (!existing.equals(manifest)) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "content-addressed checkpoint manifest drifted");
            }
            return;
        }
        publishImmutable(path, CheckpointManifestCodec.encode(manifest));
        observer.after(PublicationStep.MANIFEST_DURABLE, path);
        existing = manifest(digest);
        if (existing == null || !existing.equals(manifest)) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "durable checkpoint manifest does not match proposal");
        }
    }

    private void validateFirstDecision(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        if (proposed == null
                || (proposed.status() != CheckpointDecision.Status.ABORTED
                && proposed.status()
                != CheckpointDecision.Status.COMMIT_DECIDED)) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "first decision must be ABORTED or COMMIT_DECIDED");
        }
        if (proposed.status() == CheckpointDecision.Status.ABORTED) {
            if (manifest != null) {
                throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                        "aborted checkpoint cannot publish a manifest");
            }
            return;
        }
        if (manifest == null
                || !proposed.checkpointIdentity().equals(
                        manifest.checkpointIdentity())
                || !proposed.manifestDigest().equals(
                        manifest.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "commit decision requires its exact canonical manifest");
        }
    }

    private CheckpointDecision readRawDecision(CheckpointIdentity identity) {
        return readFrameDecision(decisionPath(identity), identity);
    }

    @Override public void retainOnly(Set<CheckpointIdentity> identities,
            Set<String> manifestDigests) {
        if (identities == null || manifestDigests == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "protected checkpoint manifests are required");
        }
        Set<Path> protectedCheckpoints = new HashSet<>();
        for (CheckpointIdentity identity : identities) {
            requireIdentity(identity);
            protectedCheckpoints.add(decisionPath(identity));
            protectedCheckpoints.add(completionPath(identity));
        }
        Set<Path> protectedManifests = new HashSet<>();
        for (String digest : manifestDigests) {
            protectedManifests.add(manifestPath(digest));
        }
        try {
            boolean checkpointChanged = removeUnprotected(
                    checkpointDirectory, protectedCheckpoints,
                    ".decision", ".completed");
            if (checkpointChanged) forceDirectory(checkpointDirectory);
            if (removeUnprotected(manifestDirectory, protectedManifests,
                    ".manifest")) {
                forceDirectory(manifestDirectory);
            }
        } catch (IOException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "completed checkpoint removal failed", failure);
        }
    }

    private static boolean removeUnprotected(Path directory,
            Set<Path> protectedPaths, String... suffixes) throws IOException {
        boolean changed = false;
        try (java.nio.file.DirectoryStream<Path> entries =
                     Files.newDirectoryStream(directory)) {
            for (Path path : entries) {
                boolean managed = false;
                for (String suffix : suffixes) {
                    if (path.getFileName().toString().endsWith(suffix)) {
                        managed = true;
                        break;
                    }
                }
                if (managed && !protectedPaths.contains(path)) {
                    changed |= Files.deleteIfExists(path);
                }
            }
        }
        return changed;
    }

    private CheckpointDecision readFrameDecision(
            Path path, CheckpointIdentity expectedIdentity) {
        if (!Files.exists(path)) return null;
        try {
            CheckpointDecision decoded = CheckpointDecisionCodec.decode(
                    readBounded(path));
            if (!expectedIdentity.equals(decoded.checkpointIdentity())) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint decision identity does not match its name");
            }
            return decoded;
        } catch (StreamCheckpointException corruption) {
            throw corruption;
        } catch (RuntimeException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "checkpoint decision frame is corrupt", malformed);
        }
    }

    private static CheckpointDecision requireExact(
            CheckpointDecision existing, CheckpointDecision proposed) {
        if (existing == null || !existing.equals(proposed)) {
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "checkpoint first decision cannot be rewritten");
        }
        return existing;
    }

    private static void publishImmutable(Path target, byte[] frame) {
        Path temporary = target.getParent().resolve(
                ".rowweave-stream-checkpoint-" + UUID.randomUUID() + ".tmp");
        Throwable primary = null;
        try {
            writeAndForce(temporary, frame);
            try {
                Files.createLink(target, temporary);
                forceDirectory(target.getParent());
            } catch (FileAlreadyExistsException collision) {
                primary = collision;
            }
        } catch (IOException | UnsupportedOperationException ioFailure) {
            primary = ioFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "durable immutable checkpoint publication failed", ioFailure);
        } catch (RuntimeException | Error runtimeFailure) {
            primary = runtimeFailure;
            throw runtimeFailure;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException cleanupFailure) {
                if (primary != null) {
                    primary.addSuppressed(cleanupFailure);
                } else {
                    throw failure(StreamCheckpointException.Code.IO_FAILURE,
                            "checkpoint temporary cleanup failed", cleanupFailure);
                }
            }
        }
    }

    private static byte[] readBounded(Path path) {
        try {
            long size = Files.size(path);
            if (size < 1L || size > StreamEdge.MAX_FRAME_BYTES) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint authority frame exceeds its bound");
            }
            return Files.readAllBytes(path);
        } catch (StreamCheckpointException corruption) {
            throw corruption;
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint authority read failed", ioFailure);
        }
    }

    private static void writeAndForce(Path path, byte[] frame)
            throws IOException {
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(frame);
            while (buffer.hasRemaining()) channel.write(buffer);
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
            if (!Files.isDirectory(directory)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "checkpoint authority path is not a directory");
            }
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent)) {
                forceDirectory(parent);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException | UnsupportedOperationException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint authority directory is not durable", ioFailure);
        }
    }

    private static String identityDigest(CheckpointIdentity identity) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, identity.definitionId());
                writeText(output, identity.artifactId());
                writeText(output, identity.runId());
                writeText(output, identity.attemptId());
                output.writeLong(identity.fenceToken());
                output.writeLong(identity.coordinatorTerm());
                writeText(output, identity.coordinatorIncarnation());
                output.writeLong(identity.checkpointSequence());
                writeText(output, identity.planDigest());
                writeText(output, identity.deploymentDigest());
                output.writeInt(identity.dataProtocolVersion());
                output.writeInt(identity.stateFormatVersion());
            }
            return hex(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory checkpoint identity encoding failed", impossible);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(
                java.nio.charset.StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
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

    private static void requireIdentity(CheckpointIdentity identity) {
        if (identity == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint identity is required");
        }
    }

    private static void requireDigest(String digest) {
        if (digest == null || !digest.matches("[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "canonical checkpoint digest is required");
        }
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
