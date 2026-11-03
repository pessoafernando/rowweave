package io.rowweave.artifact;

/** Optional infrastructure capability: hands the Master an ArtifactRepository shared across its generations. */
public interface ArtifactRepositoryProvider {
    ArtifactRepository artifactRepository();
}
