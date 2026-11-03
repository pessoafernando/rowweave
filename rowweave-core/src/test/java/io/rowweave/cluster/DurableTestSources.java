package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanNode;

import java.util.Collections;
import java.util.List;

/** Exact embedded source fixture for durable protocol tests whose subject is not source admission. */
final class DurableTestSources {
    private DurableTestSources() {
    }

    static PlanNode source(DataSchema meta, List<DataRow> rows, String splitId) {
        return PlanNode.sourceFromInputs(meta, Collections.singletonList(
                new CollectionSource(meta, splitId, rows)));
    }
}
