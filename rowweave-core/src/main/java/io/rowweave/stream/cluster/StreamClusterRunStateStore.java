package io.rowweave.stream.cluster;

import java.util.List;
import java.util.function.UnaryOperator;

/** Linearizable storage seam for Stream placement, attempt and coordinator fences. */
public interface StreamClusterRunStateStore {
    StreamClusterRunState createIfAbsent(StreamClusterRunState initial);

    StreamClusterRunState current(String runId);

    List<StreamClusterRunState> all();

    StreamClusterRunState update(String runId,
            UnaryOperator<StreamClusterRunState> mutation);

    StreamClusterRunState delete(String runId,
            UnaryOperator<StreamClusterRunState> validation);

    /** Reserves one globally monotonic positive coordinator term. */
    long nextCoordinatorTerm();
}
