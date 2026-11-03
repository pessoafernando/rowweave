package io.rowweave.stream.spi.conformance;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Immutable bounded scenario report with no connector data or configuration. */
public final class StreamConformanceReport {
    private final Set<String> passed;

    StreamConformanceReport(Set<String> passed) {
        this.passed = Collections.unmodifiableSet(
                new LinkedHashSet<>(passed));
    }

    public int passedScenarioCount() { return passed.size(); }
    public boolean passed(String scenario) { return passed.contains(scenario); }
    public Set<String> passedScenarios() { return passed; }
}
