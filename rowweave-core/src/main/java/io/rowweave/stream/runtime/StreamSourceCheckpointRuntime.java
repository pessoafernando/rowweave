package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;

import java.nio.file.Path;

/** Immutable task-local resources for source checkpoint and exact reopen. */
public final class StreamSourceCheckpointRuntime {
    private final String artifactId;
    private final int subtaskIndex;
    private final Path workingDirectory;
    private final FileCheckpointPayloadStore payloadStore;
    private final StreamCheckpointMember restoreMember;

    public StreamSourceCheckpointRuntime(
            String artifactId, int subtaskIndex, Path workingDirectory,
            FileCheckpointPayloadStore payloadStore,
            StreamCheckpointMember restoreMember) {
        if (artifactId == null || artifactId.isEmpty()
                || artifactId.length() > 256 || subtaskIndex < 0
                || workingDirectory == null || payloadStore == null) {
            throw new IllegalArgumentException(
                    "source checkpoint runtime configuration is invalid");
        }
        this.artifactId = artifactId;
        this.subtaskIndex = subtaskIndex;
        this.workingDirectory = workingDirectory;
        this.payloadStore = payloadStore;
        this.restoreMember = restoreMember;
    }

    StreamSourcePositionCheckpoint positions(
            String sourceOperatorUid, StreamSourceBinding binding,
            StreamSourceSplit split, long logicalPosition,
            byte[] positionFrame) {
        return new StreamSourcePositionCheckpoint(
                artifactId, sourceOperatorUid, subtaskIndex, binding, split,
                workingDirectory, payloadStore, logicalPosition, positionFrame);
    }

    String artifactId() { return artifactId; }
    int subtaskIndex() { return subtaskIndex; }
    StreamCheckpointMember restoreMember() { return restoreMember; }

    void restorePosition(StreamSourcePositionCheckpoint positions) {
        if (restoreMember == null) return;
        if (restoreMember.subtaskIndex() == subtaskIndex) {
            positions.restore(restoreMember);
        } else {
            positions.restoreReassigned(restoreMember);
        }
    }
}
