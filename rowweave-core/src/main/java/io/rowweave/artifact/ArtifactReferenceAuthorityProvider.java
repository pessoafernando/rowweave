package io.rowweave.artifact;

/** Optional registry capability: exposes an Artifact reference authority on the same backend as the RunStateStore. */
public interface ArtifactReferenceAuthorityProvider {
    ArtifactReferenceAuthority artifactReferenceAuthority();
}
