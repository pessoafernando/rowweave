package io.rowweave.stream.runtime;

/** Registry capability exposing shared Stream checkpoint metadata. */
public interface StreamCheckpointCatalogProvider {
    StreamCheckpointCatalog streamCheckpointCatalog();
}
