package io.rowweave.pipeline;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Successful checks from the public authority record-store conformance. */
public final class VersionedDatasetAuthorityRecordStoreConformanceReport {
    private final Set<String> passedChecks;

    VersionedDatasetAuthorityRecordStoreConformanceReport(Set<String> passedChecks) {
        this.passedChecks = Collections.unmodifiableSet(
                new LinkedHashSet<>(passedChecks));
    }

    public Set<String> passedChecks() {
        return passedChecks;
    }
}
