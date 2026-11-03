package io.rowweave.monitor.http;

import io.rowweave.monitor.MetricKind;
import io.rowweave.monitor.MetricSet;
import io.rowweave.monitor.MetricValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Renders bounded monitor metric snapshots as Prometheus text version 0.0.4. */
final class MonitorPrometheus {
    private static final String PREFIX = "rowweave_";

    private MonitorPrometheus() { }

    static String render(List<MetricSet> metricSets) {
        Map<String, StringBuilder> families = new LinkedHashMap<>();
        Map<String, MetricKind> kinds = new LinkedHashMap<>();
        for (MetricSet metricSet : metricSets) {
            for (MetricValue value : metricSet.values()) {
                String rawName = value.name().toLowerCase(java.util.Locale.ROOT);
                String scope = sanitize(metricSet.scope()).toLowerCase(java.util.Locale.ROOT);
                if (!rawName.startsWith(scope + "_") && !rawName.startsWith(scope + ".")) {
                    rawName = scope + "." + rawName;
                }
                String name = PREFIX + sanitize(rawName);
                StringBuilder family = families.computeIfAbsent(name, ignored -> new StringBuilder());
                MetricKind previous = kinds.put(name, value.kind());
                if (previous != null && previous != value.kind()) {
                    throw new IllegalStateException("metric kind changed for " + name);
                }
                family.append(name);
                appendLabels(family, metricSet);
                family.append(' ').append(Double.toString(value.value())).append('\n');
            }
        }
        StringBuilder output = new StringBuilder();
        for (Map.Entry<String, StringBuilder> entry : families.entrySet()) {
            output.append("# TYPE ").append(entry.getKey()).append(' ')
                    .append(prometheusKind(kinds.get(entry.getKey()))).append('\n');
            output.append(entry.getValue());
        }
        return output.toString();
    }

    private static void appendLabels(StringBuilder target, MetricSet metricSet) {
        if (metricSet.tags().isEmpty()) return;
        target.append('{');
        boolean first = true;
        for (Map.Entry<String, String> tag : metricSet.tags().entrySet()) {
            if (!first) target.append(',');
            first = false;
            target.append(sanitizeLabel(tag.getKey())).append("=\"")
                    .append(escapeLabel(tag.getValue())).append('"');
        }
        target.append('}');
    }

    private static String prometheusKind(MetricKind kind) {
        return kind == MetricKind.COUNTER ? "counter" : "gauge";
    }

    private static String sanitizeLabel(String value) {
        StringBuilder result = new StringBuilder(value.length());
        boolean letterStart = value.isEmpty() || !Character.isLetter(value.charAt(0));
        if (letterStart) result.append('x');
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (Character.isLetterOrDigit(current) || current == '_') {
                result.append(current);
            } else {
                result.append('_');
            }
        }
        return result.toString();
    }

    private static String sanitize(String value) {
        StringBuilder result = new StringBuilder(value.length());
        boolean letterStart = value.isEmpty() || !Character.isLetter(value.charAt(0));
        if (letterStart) result.append('x');
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (Character.isLetterOrDigit(current)) {
                result.append(Character.toLowerCase(current));
            } else {
                result.append('_');
            }
        }
        return result.toString();
    }

    private static String escapeLabel(String value) {
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
    }
}
