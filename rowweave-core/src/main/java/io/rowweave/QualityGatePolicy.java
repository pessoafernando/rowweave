package io.rowweave;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Immutable, bounded quality thresholds frozen into a compound publication Plan. */
public final class QualityGatePolicy {
    public static final int CURRENT_VERSION = 1;
    public static final int MAX_REASON_THRESHOLDS = 256;
    public static final int MAX_EXEMPLARS = 64;

    private static final QualityGatePolicy ALLOW_ALL = new Builder().build();

    private final int version;
    private final Long maxRejectedRows;
    private final Integer maxRejectedPartsPerMillion;
    private final List<ReasonThreshold> reasonThresholds;
    private final int exemplarLimit;

    private QualityGatePolicy(Builder builder) {
        this.version = CURRENT_VERSION;
        this.maxRejectedRows = builder.maxRejectedRows;
        this.maxRejectedPartsPerMillion = builder.maxRejectedPartsPerMillion;
        List<ReasonThreshold> copy = new ArrayList<>(builder.reasonThresholds.size());
        for (Map.Entry<QualityReasonKey, Long> entry : builder.reasonThresholds.entrySet()) {
            copy.add(new ReasonThreshold(entry.getKey(), entry.getValue()));
        }
        this.reasonThresholds = Collections.unmodifiableList(copy);
        this.exemplarLimit = builder.exemplarLimit;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static QualityGatePolicy allowAll() {
        return ALLOW_ALL;
    }

    public int version() {
        return version;
    }

    public Long maxRejectedRows() {
        return maxRejectedRows;
    }

    public Integer maxRejectedPartsPerMillion() {
        return maxRejectedPartsPerMillion;
    }

    public List<ReasonThreshold> reasonThresholds() {
        return reasonThresholds;
    }

    public int exemplarLimit() {
        return exemplarLimit;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof QualityGatePolicy)) return false;
        QualityGatePolicy that = (QualityGatePolicy) other;
        return version == that.version && exemplarLimit == that.exemplarLimit
                && Objects.equals(maxRejectedRows, that.maxRejectedRows)
                && Objects.equals(maxRejectedPartsPerMillion,
                that.maxRejectedPartsPerMillion)
                && reasonThresholds.equals(that.reasonThresholds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, maxRejectedRows, maxRejectedPartsPerMillion,
                reasonThresholds, exemplarLimit);
    }

    public static final class ReasonThreshold {
        private final QualityReasonKey reason;
        private final long maximum;

        private ReasonThreshold(QualityReasonKey reason, long maximum) {
            this.reason = reason;
            this.maximum = maximum;
        }

        public QualityReasonKey reason() {
            return reason;
        }

        public long maximum() {
            return maximum;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof ReasonThreshold)) return false;
            ReasonThreshold that = (ReasonThreshold) other;
            return maximum == that.maximum && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, maximum);
        }
    }

    public static final class Builder {
        private Long maxRejectedRows;
        private Integer maxRejectedPartsPerMillion;
        private final TreeMap<QualityReasonKey, Long> reasonThresholds = new TreeMap<>();
        private int exemplarLimit;

        private Builder() {
        }

        public Builder maxRejectedRows(long maximum) {
            if (maximum < 0L) throw new IllegalArgumentException("maximum rejected rows is negative");
            this.maxRejectedRows = maximum;
            return this;
        }

        public Builder maxRejectedPartsPerMillion(int maximum) {
            if (maximum < 0 || maximum > 1_000_000) {
                throw new IllegalArgumentException("maximum rejected ratio is outside [0, 1000000]");
            }
            this.maxRejectedPartsPerMillion = maximum;
            return this;
        }

        public Builder reasonThreshold(QualityReasonKey reason, long maximum) {
            if (reason == null) throw new IllegalArgumentException("quality reason is required");
            if (maximum < 0L) throw new IllegalArgumentException("quality reason maximum is negative");
            if (reasonThresholds.containsKey(reason)) {
                throw new IllegalArgumentException("duplicate quality reason threshold");
            }
            if (reasonThresholds.size() >= MAX_REASON_THRESHOLDS) {
                throw new IllegalArgumentException("too many quality reason thresholds");
            }
            reasonThresholds.put(reason, maximum);
            return this;
        }

        public Builder exemplarLimit(int limit) {
            if (limit < 0 || limit > MAX_EXEMPLARS) {
                throw new IllegalArgumentException("quality exemplar limit is outside [0, 64]");
            }
            this.exemplarLimit = limit;
            return this;
        }

        public QualityGatePolicy build() {
            return new QualityGatePolicy(this);
        }
    }
}
