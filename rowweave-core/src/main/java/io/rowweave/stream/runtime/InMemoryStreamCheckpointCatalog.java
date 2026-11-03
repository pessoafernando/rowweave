package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSavepoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Shared in-process checkpoint metadata catalog with immutable identities. */
public final class InMemoryStreamCheckpointCatalog
        implements StreamCheckpointCatalog {
    private final ConcurrentHashMap<String, StreamCheckpointMember> members =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StreamSavepoint> savepoints =
            new ConcurrentHashMap<>();

    @Override public void publish(StreamCheckpointMember member) {
        if (member == null) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint member is required");
        }
        StreamCheckpointMember existing = members.putIfAbsent(
                key(member.toEvidence()), member);
        if (existing != null && !existing.equals(member)) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member identity was reused");
        }
    }

    @Override public StreamCheckpointMember member(
            CheckpointEvidence evidence) {
        if (evidence == null) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint evidence is required");
        }
        StreamCheckpointMember value = members.get(key(evidence));
        if (value == null || !evidence.equals(value.toEvidence())) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member frame is missing or corrupt");
        }
        return value;
    }

    @Override public StreamSavepoint publishSavepoint(
            StreamSavepoint savepoint) {
        if (savepoint == null) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "savepoint is required");
        }
        StreamSavepoint existing = savepoints.putIfAbsent(
                savepoint.savepointId(), savepoint);
        if (existing != null && !existing.equals(savepoint)) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                    "savepoint name cannot be rebound to another checkpoint");
        }
        return existing == null ? savepoint : existing;
    }

    @Override public StreamSavepoint get(String savepointId) {
        return savepoints.get(StreamSavepoint.canonicalId(savepointId));
    }

    @Override public List<StreamSavepoint> savepoints() {
        List<StreamSavepoint> result = new ArrayList<>(savepoints.values());
        result.sort((left, right) -> left.savepointId().compareTo(
                right.savepointId()));
        return Collections.unmodifiableList(result);
    }

    @Override public void removeCheckpointReference(String checkpointId) {
        String canonical = StreamSavepoint.canonicalId(checkpointId);
        if (!canonical.startsWith("stream-checkpoint:")) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "only retained checkpoint references may be removed");
        }
        savepoints.remove(canonical);
    }

    @Override public void retainOnlyMembers(
            Collection<StreamCheckpointMember> retained) {
        if (retained == null) {
            throw StreamCheckpointCatalog.failure(
                    StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "retained checkpoint members are required");
        }
        Set<String> protectedKeys = new HashSet<>();
        for (StreamCheckpointMember member : retained) {
            protectedKeys.add(key(member.toEvidence()));
        }
        members.keySet().retainAll(protectedKeys);
    }

    private static String key(CheckpointEvidence evidence) {
        return evidence.memberId() + ':' + evidence.contentDigest() + ':'
                + Long.toUnsignedString(evidence.contentBytes());
    }
}
