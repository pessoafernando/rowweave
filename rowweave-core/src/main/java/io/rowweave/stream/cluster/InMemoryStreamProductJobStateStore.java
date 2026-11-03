package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamJobId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/** Process-local linearizable store used by combined and HA contract tests. */
public final class InMemoryStreamProductJobStateStore
        implements StreamProductJobStateStore {
    private final Map<StreamJobId, StreamProductJobSnapshot> jobs =
            new LinkedHashMap<>();

    @Override
    public synchronized StreamProductJobSnapshot createIfAbsent(
            StreamProductJobSnapshot initial) {
        if (initial == null) {
            throw new IllegalArgumentException(
                    "initial Stream product job is required");
        }
        StreamProductJobSnapshot existing = jobs.get(initial.jobId());
        if (existing != null) return existing;
        jobs.put(initial.jobId(), initial);
        return initial;
    }

    @Override
    public synchronized StreamProductJobSnapshot current(StreamJobId jobId) {
        if (jobId == null) {
            throw new IllegalArgumentException("Stream job ID is required");
        }
        return jobs.get(jobId);
    }

    @Override
    public synchronized List<StreamProductJobSnapshot> all() {
        return new ArrayList<>(jobs.values());
    }

    @Override
    public synchronized StreamProductJobSnapshot update(StreamJobId jobId,
            UnaryOperator<StreamProductJobSnapshot> mutation) {
        if (jobId == null || mutation == null) {
            throw new IllegalArgumentException(
                    "Stream job ID and mutation are required");
        }
        StreamProductJobSnapshot current = jobs.get(jobId);
        if (current == null) return null;
        StreamProductJobSnapshot next = mutation.apply(current);
        if (next == null || !jobId.equals(next.jobId())) {
            throw new IllegalStateException(
                    "Stream product mutation changed its identity");
        }
        jobs.put(jobId, next);
        return next;
    }
}
