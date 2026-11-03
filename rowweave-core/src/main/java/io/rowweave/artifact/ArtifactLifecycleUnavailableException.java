package io.rowweave.artifact;

/** The Artifact inventory and reference authority is temporarily unavailable; a janitor must keep the work and retry. */
public final class ArtifactLifecycleUnavailableException
        extends IllegalStateException {
    public ArtifactLifecycleUnavailableException(String message) {
        super(message);
    }

    public ArtifactLifecycleUnavailableException(
            String message, Throwable cause) {
        super(message, cause);
    }
}
