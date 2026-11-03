package io.rowweave.pipeline;

import java.util.Optional;

/**
 * Opaque durable CAS substrate. Implementations provide storage only; the
 * first-winner and binding state machine remains in RowWeave. Provider adapters
 * must pass {@link VersionedDatasetAuthorityRecordStoreConformance} before use.
 */
public interface VersionedDatasetAuthorityRecordStore {
    enum RecordKind {
        HANDOFF,
        CONSUMER_BINDING
    }

    Optional<VersionedDatasetAuthorityRecord> read(
            RecordKind kind, String recordId);

    /** expectedVersionToken is null only when the record must be absent. */
    boolean compareAndSet(
            RecordKind kind, String recordId, String expectedVersionToken,
            byte[] replacement);
}
