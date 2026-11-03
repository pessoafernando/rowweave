package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionedDatasetAuthorityRecordStoreConformanceTest {
    @TempDir
    Path temporary;

    @Test
    void publicConformanceVerifiesDurableCasAdapterContract() throws Exception {
        VersionedDatasetAuthorityRecordStoreConformanceReport report =
                VersionedDatasetAuthorityRecordStoreConformance.verify(
                        () -> new FileVersionedDatasetAuthorityRecordStore(temporary));

        assertEquals(new LinkedHashSet<>(Arrays.asList(
                "empty-read", "create-and-reopen", "cas-conflict",
                "record-kind-isolation", "defensive-ownership",
                "concurrent-single-winner", "record-limit")),
                report.passedChecks());
    }

    @Test
    void publicConformanceRejectsAdapterThatAcknowledgesWithoutStorage() {
        VersionedDatasetAuthorityRecordStoreConformanceFixture broken = () ->
                new VersionedDatasetAuthorityRecordStore() {
                    @Override
                    public Optional<VersionedDatasetAuthorityRecord> read(
                            RecordKind kind, String recordId) {
                        return Optional.empty();
                    }

                    @Override
                    public boolean compareAndSet(
                            RecordKind kind, String recordId,
                            String expectedVersionToken, byte[] replacement) {
                        return true;
                    }
                };

        VersionedDatasetHandoffException failure = assertThrows(
                VersionedDatasetHandoffException.class,
                () -> VersionedDatasetAuthorityRecordStoreConformance.verify(broken));
        assertEquals("CONFORMANCE_FAILED", failure.code());
    }
}
