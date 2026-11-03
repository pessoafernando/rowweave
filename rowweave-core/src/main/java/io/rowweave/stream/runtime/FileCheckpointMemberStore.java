package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** Durable immutable typed-handle catalog referenced by checkpoint evidence. */
public final class FileCheckpointMemberStore {
    private static final int MAX_MEMBER_FRAME_BYTES = 8 * 1024;
    private static final String MEMBER_PREFIX = "stream-member:";

    private final Path memberDirectory;

    public FileCheckpointMemberStore(Path root) {
        if (root == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint member catalog root is required");
        }
        Path normalized = root.toAbsolutePath().normalize();
        ensureDirectory(normalized);
        memberDirectory = normalized.resolve("members");
        ensureDirectory(memberDirectory);
    }

    public void publish(StreamCheckpointMember member) {
        if (member == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint member is required");
        }
        byte[] frame = StreamCheckpointMemberCodec.encode(member);
        if (frame.length > MAX_MEMBER_FRAME_BYTES) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member frame exceeds its bound");
        }
        Path temporary = temporary();
        Throwable primary = null;
        try {
            Files.write(temporary, frame, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            forceFile(temporary);
            Path target = memberPath(member);
            try {
                Files.createLink(target, temporary);
                forceDirectory(memberDirectory);
            } catch (FileAlreadyExistsException retry) {
                if (!Arrays.equals(frame, readFrame(target))) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                            "checkpoint member identity was reused");
                }
            }
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException publicationFailure) {
            primary = publicationFailure;
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint member publication failed",
                    publicationFailure);
        } finally {
            cleanup(temporary, primary);
        }
    }

    public List<StreamCheckpointMember> resolve(
            CheckpointDecision decision, StreamCheckpointAuthority authority) {
        if (decision == null || authority == null
                || (decision.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                && decision.status()
                != CheckpointDecision.Status.COMPLETED)) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "committed checkpoint decision is required for restore");
        }
        CheckpointDecision first = authority.firstDecision(
                decision.checkpointIdentity());
        if (first == null
                || first.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                || !first.manifestDigest().equals(decision.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "checkpoint restore decision is not authoritative");
        }
        if (decision.status() == CheckpointDecision.Status.COMPLETED
                && !decision.equals(authority.decision(
                        decision.checkpointIdentity()))) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "completed checkpoint is not authoritative");
        }
        CheckpointManifest manifest = authority.manifest(
                decision.manifestDigest());
        List<StreamCheckpointMember> resolved = new ArrayList<>();
        for (CheckpointTaskReceipt receipt : manifest.taskReceipts()) {
            for (CheckpointEvidence evidence : receipt.evidence()) {
                StreamCheckpointMember member = read(evidence);
                if (!evidence.equals(member.toEvidence())) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                            "checkpoint member does not match manifest evidence");
                }
                resolved.add(member);
            }
        }
        return Collections.unmodifiableList(resolved);
    }

    public List<StreamCheckpointMember> resolve(
            CheckpointDecision decision, FileCheckpointAuthority authority) {
        return resolve(decision, (StreamCheckpointAuthority) authority);
    }

    void verify(CheckpointTaskReceipt receipt) {
        if (receipt == null) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint receipt is required");
        }
        for (CheckpointEvidence evidence : receipt.evidence()) {
            StreamCheckpointMember member = read(evidence);
            if (!evidence.equals(member.toEvidence())) {
                throw failure(
                        StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                        "checkpoint member does not match receipt evidence");
            }
        }
    }

    StreamCheckpointMember read(CheckpointEvidence evidence) {
        if (evidence == null) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint evidence is required");
        }
        Path path = memberPath(evidence);
        try {
            StreamCheckpointMember member = StreamCheckpointMemberCodec.decode(
                    readFrame(path));
            if (!evidence.equals(member.toEvidence())) {
                throw failure(
                        StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                        "checkpoint member evidence does not match its name");
            }
            return member;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member frame is corrupt", malformed);
        }
    }

    Path memberPath(StreamCheckpointMember member) {
        if (member == null) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member is required");
        }
        return memberPath(member.toEvidence());
    }

    void retainOnly(Collection<StreamCheckpointMember> retained) {
        if (retained == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "retained checkpoint members are required");
        }
        Set<Path> protectedPaths = new HashSet<>();
        for (StreamCheckpointMember member : retained) {
            protectedPaths.add(memberPath(member));
        }
        try {
            boolean changed = false;
            try (java.nio.file.DirectoryStream<Path> entries =
                         Files.newDirectoryStream(
                                 memberDirectory, "*.member")) {
                for (Path path : entries) {
                    if (!protectedPaths.contains(path)) {
                        changed |= Files.deleteIfExists(path);
                    }
                }
            }
            if (changed) forceDirectory(memberDirectory);
        } catch (IOException failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint member removal failed", failure);
        }
    }

    private Path memberPath(CheckpointEvidence evidence) {
        requireMemberId(evidence.memberId());
        return memberDirectory.resolve(
                evidence.memberId().substring(MEMBER_PREFIX.length()) + '-'
                + evidence.contentDigest() + '-'
                + Long.toUnsignedString(evidence.contentBytes()) + ".member");
    }

    private byte[] readFrame(Path path) {
        try {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(
                        StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                        "checkpoint member frame is missing");
            }
            long size = Files.size(path);
            if (size < 1L || size > MAX_MEMBER_FRAME_BYTES) {
                throw failure(
                        StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                        "checkpoint member frame length is invalid");
            }
            byte[] frame = new byte[(int) size];
            try (DataInputStream input = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(path)))) {
                input.readFully(frame);
                if (input.read() != -1) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                            "checkpoint member changed while being read");
                }
            }
            return frame;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException readFailure) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member frame is unreadable", readFailure);
        }
    }

    private Path temporary() {
        return memberDirectory.resolve(
                ".rowweave-stream-member-" + UUID.randomUUID() + ".tmp");
    }

    private static void requireMemberId(String memberId) {
        if (memberId == null
                || !memberId.matches("stream-member:[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "canonical checkpoint member identity is required");
        }
    }

    private static void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "checkpoint member path is not a directory");
            }
            Path parent = directory.getParent();
            if (parent != null && Files.isDirectory(parent)) {
                forceDirectory(parent);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (IOException creationFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint member directory creation failed",
                    creationFailure);
        }
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

    private static void cleanup(Path temporary, Throwable primary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException cleanupFailure) {
            if (primary != null) primary.addSuppressed(cleanupFailure);
            else throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "checkpoint member staging cleanup failed", cleanupFailure);
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
