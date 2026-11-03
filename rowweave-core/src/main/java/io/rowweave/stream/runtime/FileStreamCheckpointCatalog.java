package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSavepoint;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rowweave.stream.contract.CheckpointIdentity;

/** File-backed checkpoint metadata catalog for Local and single-Master use. */
public final class FileStreamCheckpointCatalog
        implements StreamCheckpointCatalog {
    private final FileCheckpointMemberStore members;
    private final FileStreamSavepointStore savepoints;

    public FileStreamCheckpointCatalog(Path root) {
        this(new FileCheckpointMemberStore(root),
                new FileStreamSavepointStore(root));
    }

    FileStreamCheckpointCatalog(FileCheckpointMemberStore members,
            FileStreamSavepointStore savepoints) {
        if (members == null || savepoints == null) {
            throw new IllegalArgumentException(
                    "checkpoint member and savepoint stores are required");
        }
        this.members = members;
        this.savepoints = savepoints;
    }

    @Override public void publish(StreamCheckpointMember member) {
        members.publish(member);
    }

    @Override public StreamCheckpointMember member(
            CheckpointEvidence evidence) {
        return members.read(evidence);
    }

    @Override public StreamSavepoint publishSavepoint(
            StreamSavepoint savepoint) {
        return savepoints.publish(savepoint);
    }

    @Override public StreamSavepoint get(String savepointId) {
        return savepoints.get(savepointId);
    }

    @Override public List<StreamSavepoint> savepoints() {
        return savepoints.all();
    }

    @Override public void removeCheckpointReference(String checkpointId) {
        savepoints.removeCheckpointReference(checkpointId);
    }

    @Override public void retainOnlyMembers(
            Collection<StreamCheckpointMember> retained) {
        members.retainOnly(retained);
    }

    public int retainLatest(FileCheckpointAuthority authority,
            FileCheckpointPayloadStore payloads,
            Map<String, CheckpointIdentity> recoveryRoots,
            Set<String> activeRuns, String runId, int retainedCompleted) {
        return new FileCheckpointRetention(authority, this, payloads,
                recoveryRoots, activeRuns)
                .retainLatest(runId, retainedCompleted);
    }
}
