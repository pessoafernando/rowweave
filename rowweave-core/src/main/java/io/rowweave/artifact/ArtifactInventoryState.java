package io.rowweave.artifact;

/** Crash-recoverable Artifact publication and deletion state held in the file inventory. */
public enum ArtifactInventoryState {
    PUBLISHING,
    LIVE,
    DELETING
}
