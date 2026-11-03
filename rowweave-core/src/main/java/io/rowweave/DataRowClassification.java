package io.rowweave;

/** engine-created pair of exhaustive, mutually exclusive typed-row results. */
public final class DataRowClassification {
    private final DataSet validRows;
    private final DataSet rejectedRows;
    private final int maxReasonBuckets;

    DataRowClassification(DataSet validRows, DataSet rejectedRows, int maxReasonBuckets) {
        if (validRows == null || rejectedRows == null) {
            throw new IllegalArgumentException("classification branches are required");
        }
        if (maxReasonBuckets <= 0) {
            throw new IllegalArgumentException("classification reason bound is required");
        }
        this.validRows = validRows;
        this.rejectedRows = rejectedRows;
        this.maxReasonBuckets = maxReasonBuckets;
    }

    public DataSet validRows() {
        return validRows;
    }

    public DataSet rejectedRows() {
        return rejectedRows;
    }

    /**
     * register valid, rejected and canonical summary members as one
     * compound Manifest publication. This only builds the Plan; execution remains
     * the owning {@link ExecutionEnvironment}'s responsibility.
     */
    public void writeToManifest(TypedQualityManifest target) {
        if (target == null) {
            throw publicationInvalid("target is required");
        }
        ExecutionEnvironment session = validRows.owningSession();
        if (session == null || session != rejectedRows.owningSession()) {
            throw publicationInvalid("classification must belong to one ExecutionEnvironment");
        }

        try {
            DataSet acceptedSummary = validRows.typedQualityAcceptedSummarySkeleton();
            DataSet rejectedSummary = rejectedRows.typedQualityRejectedSummarySkeleton(
                    maxReasonBuckets);
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

            session.registerTypedQualityPublication(
                    validRows, validOutput, rejectedRows, rejectedOutput, summary, summaryOutput);
        } catch (RuntimeException invalid) {
            if (invalid instanceof IllegalArgumentException
                    && invalid.getMessage() != null
                    && invalid.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:")) {
                throw invalid;
            }
            throw new IllegalArgumentException(
                    "QUALITY_PUBLICATION_INVALID: compound Manifest plan is invalid", invalid);
        }
    }

    private static IllegalArgumentException publicationInvalid(String detail) {
        return new IllegalArgumentException("QUALITY_PUBLICATION_INVALID: " + detail);
    }
}
