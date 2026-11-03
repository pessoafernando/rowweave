package io.rowweave.artifact;

import java.util.Objects;

/** An exact runId to artifactId reference held inside the Run authority. */
public final class ArtifactReferenceRef {
    private final String runId;
    private final String artifactId;
    private final long lifecycleRevision;

    public ArtifactReferenceRef(String runId, String artifactId,
                                long lifecycleRevision) {
        ArtifactLifecycleSupport.requireText(runId, "runId");
        this.artifactId = ArtifactRepositorySupport.validateId(artifactId);
        if (lifecycleRevision <= 0L) {
            throw new IllegalArgumentException("lifecycleRevision must be > 0");
        }
        this.runId = runId;
        this.lifecycleRevision = lifecycleRevision;
    }

    public String runId() { return runId; }
    public String artifactId() { return artifactId; }
    public long lifecycleRevision() { return lifecycleRevision; }

    public static boolean isContentAddressed(String artifactId) {
        try {
            ArtifactRepositorySupport.validateId(artifactId);
            return true;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }

    public static String requireContentAddressed(String artifactId) {
        return ArtifactRepositorySupport.validateId(artifactId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ArtifactReferenceRef)) return false;
        ArtifactReferenceRef that = (ArtifactReferenceRef) other;
        return lifecycleRevision == that.lifecycleRevision
                && runId.equals(that.runId) && artifactId.equals(that.artifactId);
    }

    @Override
    public int hashCode() { return Objects.hash(runId, artifactId, lifecycleRevision); }
}
