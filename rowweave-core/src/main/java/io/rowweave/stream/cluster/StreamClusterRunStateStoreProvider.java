package io.rowweave.stream.cluster;

/** Registry capability exposing shared Stream run/placement authority. */
public interface StreamClusterRunStateStoreProvider {
    StreamClusterRunStateStore streamClusterRunStateStore();
}
