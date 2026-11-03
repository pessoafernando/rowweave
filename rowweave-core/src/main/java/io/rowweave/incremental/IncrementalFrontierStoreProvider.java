package io.rowweave.incremental;

/** Deployment capability exposing the shared durable incremental frontier authority. */
public interface IncrementalFrontierStoreProvider {
    IncrementalFrontierStore incrementalFrontierStore();
}
