package io.rowweave;

import io.rowweave.commit.CommitManifest;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * one atomic Manifest target for valid rows, rejected rows and their
 * bounded typed-quality summary.
 */
public final class TypedQualityManifest {

    static final String SUMMARY_VERSION = "1";
    static final String VALID_SINK_UID = "typed-quality.valid";
    static final String REJECTED_SINK_UID = "typed-quality.rejected";
    static final String SUMMARY_SINK_UID = "typed-quality.summary";

    private static final DataSchema SUMMARY_SCHEMA = new DataSchema(
            new Field("quality_metric_kind", DataType.StringType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType),
            new Field("row_count", DataType.LongType));

    private final String domainPath;
    private final String validMemberName;
    private final String rejectedMemberName;
    private final String summaryMemberName;
    private final QualityGatePolicy qualityGatePolicy;

    private TypedQualityManifest(String domainPath, String validMemberName,
                                 String rejectedMemberName, String summaryMemberName,
                                 QualityGatePolicy qualityGatePolicy) {
        this.domainPath = domainPath;
        this.validMemberName = validMemberName;
        this.rejectedMemberName = rejectedMemberName;
        this.summaryMemberName = summaryMemberName;
        this.qualityGatePolicy = qualityGatePolicy;
    }

    public static TypedQualityManifest of(String domainPath, String validMemberName,
                                          String rejectedMemberName, String summaryMemberName) {
        try {
            if (domainPath == null || domainPath.isEmpty()) {
                throw new IllegalArgumentException("domain path is required");
            }
            String canonicalDomain = Paths.get(domainPath).toAbsolutePath().normalize().toString();
            validateMember(validMemberName);
            validateMember(rejectedMemberName);
            validateMember(summaryMemberName);
            Set<String> members = new HashSet<>();
            members.add(validMemberName);
            members.add(rejectedMemberName);
            members.add(summaryMemberName);
            if (members.size() != 3) {
                throw new IllegalArgumentException("member names must be distinct");
            }
            return new TypedQualityManifest(canonicalDomain, validMemberName,
                    rejectedMemberName, summaryMemberName, QualityGatePolicy.allowAll());
        } catch (RuntimeException invalid) {
            throw invalid(invalid);
        }
    }

    /** Freeze explicit durable publication thresholds into all three Sink nodes. */
    public TypedQualityManifest withQualityGate(QualityGatePolicy policy) {
        try {
            if (policy == null) throw new IllegalArgumentException("quality gate policy is required");
            // Force canonical validation before this target can enter a PlanGraph.
            QualityGatePolicy canonical = QualityGatePolicyCodec.decode(
                    QualityGatePolicyCodec.encode(policy));
            return new TypedQualityManifest(domainPath, validMemberName,
                    rejectedMemberName, summaryMemberName, canonical);
        } catch (RuntimeException invalid) {
            throw invalid(invalid);
        }
    }

    public static DataSchema summarySchema() {
        return SUMMARY_SCHEMA;
    }

    String domainPath() {
        return domainPath;
    }

    String validMemberName() {
        return validMemberName;
    }

    String rejectedMemberName() {
        return rejectedMemberName;
    }

    String summaryMemberName() {
        return summaryMemberName;
    }

    QualityGatePolicy qualityGatePolicy() {
        return qualityGatePolicy;
    }

    private static void validateMember(String memberName) {
        new CommitManifest.Member(memberName, "data/1/probe.data", "probe", 0L,
                new byte[CommitManifest.SHA256_BYTES]);
    }

    private static IllegalArgumentException invalid(RuntimeException cause) {
        if (cause instanceof IllegalArgumentException
                && cause.getMessage() != null
                && cause.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:")) {
            return (IllegalArgumentException) cause;
        }
        return new IllegalArgumentException(
                "QUALITY_PUBLICATION_INVALID: compound Manifest target is invalid", cause);
    }
}
