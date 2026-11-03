package io.rowweave.pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

final class InMemoryVersionedDatasetAuthorityRecordStore
        implements VersionedDatasetAuthorityRecordStore {
    private final Map<String, VersionedDatasetAuthorityRecord> records = new HashMap<>();

    @Override
    public synchronized Optional<VersionedDatasetAuthorityRecord> read(
            RecordKind kind, String recordId) {
        return Optional.ofNullable(records.get(key(kind, recordId)));
    }

    @Override
    public synchronized boolean compareAndSet(
            RecordKind kind, String recordId, String expectedVersionToken,
            byte[] replacement) {
        String key = key(kind, recordId);
        VersionedDatasetAuthorityRecord current = records.get(key);
        if (expectedVersionToken == null) {
            if (current != null) return false;
        } else if (current == null
                || !expectedVersionToken.equals(current.versionToken())) {
            return false;
        }
        records.put(key, new VersionedDatasetAuthorityRecord(replacement));
        return true;
    }

    private static String key(RecordKind kind, String recordId) {
        if (kind == null || recordId == null) {
            throw new IllegalArgumentException("record kind and ID are required");
        }
        return kind.name() + ':' + recordId;
    }
}
