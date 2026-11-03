package io.rowweave;

/** non-forgeable Object Storage quality result with atomic publication only. */
public final class ObjectStorageRowClassification {

    private final DataSet validRows;
    private final DataSet rejectedRows;

    ObjectStorageRowClassification(DataSet validRows, DataSet rejectedRows) {
        if (validRows == null || rejectedRows == null) {
            throw ObjectStorageQualitySource.invalid(
                    "classification branches are required");
        }
        this.validRows = validRows;
        this.rejectedRows = rejectedRows;
    }

    /** Build the only admitted three-member Manifest publication; execution starts at commit. */
    public void writeToManifest(TypedQualityManifest target) {
        if (target == null) {
            throw publicationInvalid("target is required");
        }
        BatchSession session = validRows.owningSession();
        if (session == null || session != rejectedRows.owningSession()) {
            throw publicationInvalid("classification must belong to one BatchSession");
        }
        try {
            DataSet acceptedSummary = validRows.typedQualityAcceptedSummarySkeleton();
            DataSet rejectedSummary = rejectedRows.typedQualityRejectedSummarySkeleton(
                    ObjectStorageQualitySource.MAX_REASON_BUCKETS);
            DataSet summary = acceptedSummary.union(rejectedSummary).orderBy(
                    "quality_metric_kind", "quality_error_code", "quality_rule_id",
                    "quality_field_index", "quality_detail", "row_count");
            ManifestFileSink validOutput = ManifestFileSink.qualityGate(
                    validRows.getSchema(), target.domainPath(), target.validMemberName(),
                    TypedQualityManifest.VALID_SINK_UID, target.qualityGatePolicy());
            ManifestFileSink rejectedOutput = ManifestFileSink.qualityGate(
                    rejectedRows.getSchema(), target.domainPath(), target.rejectedMemberName(),
                    TypedQualityManifest.REJECTED_SINK_UID, target.qualityGatePolicy());
            ManifestFileSink summaryOutput = ManifestFileSink.qualityGate(
                    TypedQualityManifest.summarySchema(), target.domainPath(),
                    target.summaryMemberName(), TypedQualityManifest.SUMMARY_SINK_UID,
                    target.qualityGatePolicy());
            session.registerObjectStorageQualityPublication(
                    validRows, validOutput, rejectedRows, rejectedOutput,
                    summary, summaryOutput);
        } catch (RuntimeException invalid) {
            if (invalid instanceof IllegalArgumentException
                    && invalid.getMessage() != null
                    && invalid.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:")) {
                throw invalid;
            }
            throw new IllegalArgumentException(
                    "QUALITY_PUBLICATION_INVALID: compound Object Storage quality plan is invalid",
                    invalid);
        }
    }

    private static IllegalArgumentException publicationInvalid(String detail) {
        return new IllegalArgumentException("QUALITY_PUBLICATION_INVALID: " + detail);
    }
}
