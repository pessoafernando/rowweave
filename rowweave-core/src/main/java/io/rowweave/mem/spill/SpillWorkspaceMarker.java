package io.rowweave.mem.spill;

import java.util.Objects;

/** non-sensitive ownership marker model. */
final class SpillWorkspaceMarker {

    enum Kind { BASE, INSTANCE, JOB }

    private final Kind kind;
    private final String instanceId;
    private final String jobId;
    private final long createdAtMillis;

    private SpillWorkspaceMarker(Kind kind, String instanceId, String jobId,
                                 long createdAtMillis) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.instanceId = instanceId;
        this.jobId = jobId;
        this.createdAtMillis = createdAtMillis;
    }

    static SpillWorkspaceMarker base() {
        return new SpillWorkspaceMarker(Kind.BASE, null, null, -1);
    }

    static SpillWorkspaceMarker instance(String instanceId, long createdAtMillis) {
        requireIdentity(instanceId, "instanceId");
        if (createdAtMillis < 0) {
            throw new IllegalArgumentException("createdAtMillis must be >= 0");
        }
        return new SpillWorkspaceMarker(Kind.INSTANCE, instanceId, null, createdAtMillis);
    }

    static SpillWorkspaceMarker job(String jobId, String instanceId) {
        requireIdentity(jobId, "jobId");
        requireIdentity(instanceId, "instanceId");
        if (jobId.equals(instanceId)) {
            throw new IllegalArgumentException("jobId must differ from instanceId");
        }
        return new SpillWorkspaceMarker(Kind.JOB, instanceId, jobId, -1);
    }

    Kind kind() {
        return kind;
    }

    String instanceId() {
        return instanceId;
    }

    String jobId() {
        return jobId;
    }

    long createdAtMillis() {
        return createdAtMillis;
    }

    private static void requireIdentity(String value, String name) {
        if (value == null || !value.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException(name + " must be 32 lowercase hex characters");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SpillWorkspaceMarker)) return false;
        SpillWorkspaceMarker that = (SpillWorkspaceMarker) other;
        return createdAtMillis == that.createdAtMillis
                && kind == that.kind
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, instanceId, jobId, createdAtMillis);
    }
}
