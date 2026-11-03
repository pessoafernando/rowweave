package io.rowweave.stream.cluster;

import io.rowweave.artifact.ArtifactRepository;

/** Registry capability for a shared Stream product-job authority. */
public interface StreamProductJobStateStoreProvider {
    StreamProductJobStateStore streamProductJobStateStore(
            ArtifactRepository artifactRepository);
}
