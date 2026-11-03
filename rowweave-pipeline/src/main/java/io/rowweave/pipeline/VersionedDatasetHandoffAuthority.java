package io.rowweave.pipeline;

import java.util.Optional;

/** Product-neutral first-winner authority for VERSIONED_DATASET handoffs. */
public interface VersionedDatasetHandoffAuthority {
    VersionedDatasetProducerAttempt claim(VersionedDatasetProducerAttempt attempt);

    Optional<VersionedDatasetProducerAttempt> queryClaim(
            VersionedDatasetHandoffKey key);

    VersionedDatasetVisibleWinner publish(VersionedDatasetCommitEvidence evidence);

    Optional<VersionedDatasetVisibleWinner> query(VersionedDatasetHandoffKey key);

    VersionedDatasetConsumerBinding bind(VersionedDatasetConsumerRequest request);

    Optional<VersionedDatasetConsumerBinding> queryBinding(
            VersionedDatasetConsumerRequest request);
}
