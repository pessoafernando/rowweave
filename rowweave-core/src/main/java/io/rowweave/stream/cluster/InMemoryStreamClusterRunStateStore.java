package io.rowweave.stream.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/** Process-local linearizable run authority used by combined and HA tests. */
public final class InMemoryStreamClusterRunStateStore
        implements StreamClusterRunStateStore {
    private final Map<String, StreamClusterRunState> runs =
            new LinkedHashMap<>();
    private long coordinatorTerm;

    @Override
    public synchronized StreamClusterRunState createIfAbsent(
            StreamClusterRunState initial) {
        if (initial == null) {
            throw new IllegalArgumentException("initial Stream run is required");
        }
        StreamClusterRunState existing = runs.get(initial.runId());
        if (existing != null) return existing;
        runs.put(initial.runId(), initial);
        return initial;
    }

    @Override
    public synchronized StreamClusterRunState current(String runId) {
        requireRunId(runId);
        return runs.get(runId);
    }

    @Override
    public synchronized List<StreamClusterRunState> all() {
        return new ArrayList<>(runs.values());
    }

    @Override
    public synchronized StreamClusterRunState update(String runId,
            UnaryOperator<StreamClusterRunState> mutation) {
        requireRunId(runId);
        if (mutation == null) {
            throw new IllegalArgumentException("Stream run mutation is required");
        }
        StreamClusterRunState current = runs.get(runId);
        if (current == null) return null;
        StreamClusterRunState next = mutation.apply(current);
        requireIdentity(runId, next);
        runs.put(runId, next);
        return next;
    }

    @Override
    public synchronized StreamClusterRunState delete(String runId,
            UnaryOperator<StreamClusterRunState> validation) {
        requireRunId(runId);
        if (validation == null) {
            throw new IllegalArgumentException(
                    "Stream run deletion validation is required");
        }
        StreamClusterRunState current = runs.get(runId);
        if (current == null) return null;
        StreamClusterRunState validated = validation.apply(current);
        requireIdentity(runId, validated);
        runs.remove(runId);
        return validated;
    }

    @Override
    public synchronized long nextCoordinatorTerm() {
        coordinatorTerm = Math.addExact(coordinatorTerm, 1L);
        return coordinatorTerm;
    }

    private static void requireRunId(String runId) {
        StreamControlMessage.text(runId, "run ID");
    }

    private static void requireIdentity(String runId,
            StreamClusterRunState state) {
        if (state == null || !runId.equals(state.runId())) {
            throw new IllegalStateException(
                    "Stream run mutation changed its identity");
        }
    }
}
