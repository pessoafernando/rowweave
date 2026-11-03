package io.rowweave.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** One Attempt's complete, immutable physical assignment manifest. */
public final class StageAssignmentManifest {
    public static final int MAX_ASSIGNMENTS = 4096;
    public static final int MAX_RECORDS_PER_SHARD = 64;
    public static final int MAX_SHARDS = MAX_ASSIGNMENTS / MAX_RECORDS_PER_SHARD;

    private final RunAttemptFence fence;
    private final String artifactId;
    private final long generation;
    private final AttemptOutputProfile outputProfile;
    private final QualityGateAssignmentBinding qualityGateBinding;
    private final List<StageAssignmentRecord> assignments;

    public StageAssignmentManifest(RunAttemptFence fence, String artifactId, long generation,
                                   List<StageAssignmentRecord> assignments) {
        this(fence, artifactId, generation,
                AttemptOutputProfile.UNKNOWN_LEGACY, assignments, null);
    }

    public StageAssignmentManifest(RunAttemptFence fence, String artifactId, long generation,
                                   AttemptOutputProfile outputProfile,
                                   List<StageAssignmentRecord> assignments) {
        this(fence, artifactId, generation, outputProfile, assignments, null);
    }

    public StageAssignmentManifest(
            RunAttemptFence fence, String artifactId, long generation,
            AttemptOutputProfile outputProfile,
            List<StageAssignmentRecord> assignments,
            QualityGateAssignmentBinding qualityGateBinding) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (outputProfile == null) {
            throw new IllegalArgumentException("outputProfile is required");
        }
        this.artifactId = RunText.required(artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (generation <= 0L) throw new IllegalArgumentException("generation must be positive");
        if (assignments == null || assignments.isEmpty()
                || assignments.size() > MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException("assignment count must be in [1," + MAX_ASSIGNMENTS + "]");
        }
        this.fence = fence;
        this.generation = generation;
        this.outputProfile = outputProfile;
        List<StageAssignmentRecord> copy = new ArrayList<>(assignments.size());
        Set<String> executionIds = new HashSet<>();
        Set<Long> runtimeIds = new HashSet<>();
        Set<String> logicalInstances = new HashSet<>();
        for (int i = 0; i < assignments.size(); i++) {
            StageAssignmentRecord record = assignments.get(i);
            if (record == null) throw new IllegalArgumentException("assignment is required at " + i);
            if (record.physicalOrdinal() != i) {
                throw new IllegalArgumentException("assignments must have contiguous canonical ordinals");
            }
            String expectedId = StageAssignmentRecord.deriveExecutionId(fence, generation, i);
            if (!expectedId.equals(record.stageExecutionId())) {
                throw new IllegalArgumentException("assignment identity does not match manifest");
            }
            String logicalInstance = record.logicalStage() + ":" + record.instanceIndex();
            if (!executionIds.add(record.stageExecutionId())
                    || !runtimeIds.add(record.runtimeStageId())
                    || !logicalInstances.add(logicalInstance)) {
                throw new IllegalArgumentException("duplicate assignment identity");
            }
            copy.add(record);
        }
        this.assignments = Collections.unmodifiableList(copy);
        if (qualityGateBinding != null) {
            if (outputProfile != AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1) {
                throw new IllegalArgumentException(
                        "quality binding requires manifest output profile");
            }
            for (QualityGateAssignmentBinding.MemberBinding member
                    : qualityGateBinding.members()) {
                int ordinal = member.physicalOrdinal();
                if (ordinal >= copy.size()
                        || !ManifestCommitJournalSnapshot.outputRole(
                        copy.get(ordinal).role())) {
                    throw new IllegalArgumentException(
                            "quality binding references a non-output assignment");
                }
            }
        }
        this.qualityGateBinding = qualityGateBinding;
    }

    public RunAttemptFence fence() { return fence; }
    public String artifactId() { return artifactId; }
    public long generation() { return generation; }
    public AttemptOutputProfile outputProfile() { return outputProfile; }
    public boolean hasQualityGateBinding() { return qualityGateBinding != null; }
    public QualityGateAssignmentBinding qualityGateBinding() {
        return qualityGateBinding;
    }
    public List<StageAssignmentRecord> assignments() { return assignments; }
    public int assignmentCount() { return assignments.size(); }
    public StageAssignmentRecord assignment(int ordinal) { return assignments.get(ordinal); }
    public int shardCount() {
        return (assignmentCount() + MAX_RECORDS_PER_SHARD - 1) / MAX_RECORDS_PER_SHARD;
    }

    List<StageAssignmentRecord> shardAssignments(int shardIndex) {
        if (shardIndex < 0 || shardIndex >= shardCount()) {
            throw new IllegalArgumentException("shardIndex out of bounds: " + shardIndex);
        }
        int from = shardIndex * MAX_RECORDS_PER_SHARD;
        int to = Math.min(from + MAX_RECORDS_PER_SHARD, assignmentCount());
        return assignments.subList(from, to);
    }
}
