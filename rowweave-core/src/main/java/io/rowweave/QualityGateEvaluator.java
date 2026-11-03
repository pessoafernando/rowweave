package io.rowweave;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeSet;

/** Overflow-safe exact threshold evaluation over already aggregated metadata. */
final class QualityGateEvaluator {

    private static final BigInteger MILLION = BigInteger.valueOf(1_000_000L);

    private QualityGateEvaluator() {
    }

    static QualityGateEvaluation evaluate(
            QualityGatePolicy policy, long acceptedRows, long rejectedRows,
            Map<QualityReasonKey, Long> observedReasons) {
        if (policy == null || observedReasons == null) {
            throw new IllegalArgumentException("quality gate evaluation inputs are required");
        }
        if (acceptedRows < 0L || rejectedRows < 0L) {
            throw new IllegalArgumentException("quality row counts must be nonnegative");
        }
        for (Map.Entry<QualityReasonKey, Long> entry : observedReasons.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue() < 0L) {
                throw new IllegalArgumentException("quality reason observations are invalid");
            }
        }

        TreeSet<String> failures = new TreeSet<>();
        if (policy.maxRejectedRows() != null
                && rejectedRows > policy.maxRejectedRows()) {
            failures.add("REJECTED_ROWS_EXCEEDED");
        }
        if (policy.maxRejectedPartsPerMillion() != null && rejectedRows != 0L) {
            BigInteger rejectedScaled = BigInteger.valueOf(rejectedRows).multiply(MILLION);
            BigInteger total = BigInteger.valueOf(acceptedRows)
                    .add(BigInteger.valueOf(rejectedRows));
            BigInteger allowedScaled = total.multiply(
                    BigInteger.valueOf(policy.maxRejectedPartsPerMillion()));
            if (rejectedScaled.compareTo(allowedScaled) > 0) {
                failures.add("REJECTED_RATIO_EXCEEDED");
            }
        }
        for (QualityGatePolicy.ReasonThreshold threshold : policy.reasonThresholds()) {
            Long observed = observedReasons.get(threshold.reason());
            if (observed != null && observed > threshold.maximum()) {
                failures.add("REASON_THRESHOLD_EXCEEDED");
            }
        }
        return new QualityGateEvaluation(new java.util.ArrayList<>(failures));
    }
}
