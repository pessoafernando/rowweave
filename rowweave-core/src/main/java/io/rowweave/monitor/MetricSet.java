package io.rowweave.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable set of typed metric observations sharing one scope and tag set.
 * Tags identify the concrete entity (e.g. jobId, stageIndex, workerId).
 */
public final class MetricSet {

    private final String scope;
    private final Map<String, String> tags;
    private final List<MetricValue> values;

    private MetricSet(Builder builder) {
        this.scope = builder.scope;
        this.tags = Collections.unmodifiableMap(new LinkedHashMap<>(builder.tags));
        this.values = Collections.unmodifiableList(new ArrayList<>(builder.values));
    }

    public String scope() { return scope; }
    public Map<String, String> tags() { return tags; }
    public List<MetricValue> values() { return values; }

    public static Builder builder(String scope) { return new Builder(scope); }

    public static final class Builder {
        private final String scope;
        private final Map<String, String> tags = new LinkedHashMap<>();
        private final List<MetricValue> values = new ArrayList<>();

        private Builder(String scope) {
            if (scope == null || scope.codePoints().allMatch(Character::isWhitespace)) {
                throw new IllegalArgumentException("metric scope is required");
            }
            this.scope = scope;
        }

        public Builder tag(String key, String value) {
            this.tags.put(Objects.requireNonNull(key, "tag key"), value);
            return this;
        }

        public Builder add(MetricValue value) {
            this.values.add(Objects.requireNonNull(value, "metric value"));
            return this;
        }

        public Builder gauge(String name, long value, String unit) {
            return add(new MetricValue(name, MetricKind.GAUGE, value, unit, System.currentTimeMillis()));
        }

        public Builder gauge(String name, double value, String unit) {
            return add(new MetricValue(name, MetricKind.GAUGE, value, unit, System.currentTimeMillis()));
        }

        public Builder counter(String name, long value, String unit) {
            return add(new MetricValue(name, MetricKind.COUNTER, value, unit, System.currentTimeMillis()));
        }

        public MetricSet build() {
            if (values.isEmpty()) {
                throw new IllegalStateException("metric set must contain at least one value");
            }
            return new MetricSet(this);
        }
    }
}
