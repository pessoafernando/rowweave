package io.rowweave.pipeline;

import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Thread-safe conformance authority. State is intentionally process-local; use a
 * durable implementation when bindings must survive restart.
 */
public final class InMemoryVersionedDatasetHandoffAuthority
        implements VersionedDatasetHandoffAuthority {
    private final StoreBackedVersionedDatasetHandoffAuthority delegate;

    public InMemoryVersionedDatasetHandoffAuthority() {
        this(System::currentTimeMillis);
    }

    InMemoryVersionedDatasetHandoffAuthority(LongSupplier clock) {
        this.delegate = new StoreBackedVersionedDatasetHandoffAuthority(
                new InMemoryVersionedDatasetAuthorityRecordStore(), clock);
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
