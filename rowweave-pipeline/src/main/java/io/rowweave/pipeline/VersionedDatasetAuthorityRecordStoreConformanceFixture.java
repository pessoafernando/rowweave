package io.rowweave.pipeline;

/**
 * Supplies independent handles over one disposable authority namespace.
 * Implementations used by conformance must not share a process-local cache as
 * their durability mechanism.
 */
@FunctionalInterface
public interface VersionedDatasetAuthorityRecordStoreConformanceFixture {
    VersionedDatasetAuthorityRecordStore openStore() throws Exception;

    default void closeStore(VersionedDatasetAuthorityRecordStore store)
            throws Exception {
        if (store instanceof AutoCloseable) {
            ((AutoCloseable) store).close();
        }
    }
}
