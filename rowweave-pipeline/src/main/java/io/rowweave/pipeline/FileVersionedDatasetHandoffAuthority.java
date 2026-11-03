package io.rowweave.pipeline;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Durable VERSIONED_DATASET authority for a shared filesystem root. */
public final class FileVersionedDatasetHandoffAuthority
        implements VersionedDatasetHandoffAuthority {
    private final StoreBackedVersionedDatasetHandoffAuthority delegate;

    public FileVersionedDatasetHandoffAuthority(Path root) {
        this.delegate = new StoreBackedVersionedDatasetHandoffAuthority(
                new FileVersionedDatasetAuthorityRecordStore(root));
    }

    FileVersionedDatasetHandoffAuthority(
            Path root, LongSupplier clock, Runnable afterDurableWrite) {
        this.delegate = new StoreBackedVersionedDatasetHandoffAuthority(
                new FileVersionedDatasetAuthorityRecordStore(
                        root, afterDurableWrite), clock);
    }

    @Override
    public VersionedDatasetProducerAttempt claim(
            VersionedDatasetProducerAttempt attempt) {
        return delegate.claim(attempt);
    }

    @Override
    public Optional<VersionedDatasetProducerAttempt> queryClaim(
            VersionedDatasetHandoffKey key) {
        return delegate.queryClaim(key);
    }

    @Override
    public VersionedDatasetVisibleWinner publish(
            VersionedDatasetCommitEvidence evidence) {
        return delegate.publish(evidence);
    }

    @Override
    public Optional<VersionedDatasetVisibleWinner> query(
            VersionedDatasetHandoffKey key) {
        return delegate.query(key);
    }

    @Override
    public VersionedDatasetConsumerBinding bind(
            VersionedDatasetConsumerRequest request) {
        return delegate.bind(request);
    }

    @Override
    public Optional<VersionedDatasetConsumerBinding> queryBinding(
            VersionedDatasetConsumerRequest request) {
        return delegate.queryBinding(request);
    }
}
