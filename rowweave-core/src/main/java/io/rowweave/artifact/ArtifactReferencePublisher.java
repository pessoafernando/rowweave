package io.rowweave.artifact;

/** Callback that establishes the Run authority reference while the repository publication guard is held. */
@FunctionalInterface
public interface ArtifactReferencePublisher<T> {
    T publish(String artifactId);
}
