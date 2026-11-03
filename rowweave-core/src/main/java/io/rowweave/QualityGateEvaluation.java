package io.rowweave;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Exact threshold outcome; durable decision construction is a separate product contract. */
final class QualityGateEvaluation {
    private final List<String> failureCodes;

    QualityGateEvaluation(List<String> failureCodes) {
        this.failureCodes = Collections.unmodifiableList(new ArrayList<>(failureCodes));
    }

    boolean passed() {
        return failureCodes.isEmpty();
    }

    List<String> failureCodes() {
        return failureCodes;
    }
}
