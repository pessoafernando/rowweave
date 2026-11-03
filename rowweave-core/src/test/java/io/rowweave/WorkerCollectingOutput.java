package io.rowweave;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-only worker-writable collector for same-JVM cluster execution.
 * Rows live in a static fixture registry so serialized Worker copies publish back to the assertion owner.
 */
public final class WorkerCollectingOutput implements Sink {

    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, State> STATES = new ConcurrentHashMap<>();

    private final DataSchema dataSchema;
    private final String collectorId;
    private transient int coordinatedStartSize = -1;

    public WorkerCollectingOutput(DataSchema dataSchema) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("dataSchema must not be null");
        }
        this.dataSchema = dataSchema;
        this.collectorId = UUID.randomUUID().toString();
        STATES.put(collectorId, new State());
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public boolean isWorkerWritable() {
        return true;
    }

    @Override
    public void collect(DataRow row) {
        State state = state();
        synchronized (state) {
            if (coordinatedStartSize < 0) {
                coordinatedStartSize = state.rows.size();
            }
            state.rows.add(row.copy());
        }
    }

    @Override
    public boolean supportsCoordinatedCommit() {
        return true;
    }

    @Override
    public void prepareCommit() {
        State state = state();
        synchronized (state) {
            if (coordinatedStartSize < 0) {
                coordinatedStartSize = state.rows.size();
            }
        }
    }

    @Override
    public void rollbackCommit() {
        State state = state();
        synchronized (state) {
            if (coordinatedStartSize >= 0) {
                state.rows.subList(coordinatedStartSize, state.rows.size()).clear();
                coordinatedStartSize = -1;
            }
        }
    }

    @Override
    public void abort() {
        rollbackCommit();
    }

    @Override
    public void commitPrepared() {
        // Visibility is synchronous because the fixture registry is process-local.
    }

    @Override
    public void finalizeCommit() {
        coordinatedStartSize = -1;
    }

    @Override
    public void close() {
        // Worker copies must leave collected rows visible to the assertion owner.
    }

    public List<DataRow> getRows() {
        State state = state();
        synchronized (state) {
            return Collections.unmodifiableList(new ArrayList<>(state.rows));
        }
    }

    private State state() {
        State state = STATES.get(collectorId);
        if (state == null) {
            throw new IllegalStateException("test collector state is unavailable");
        }
        return state;
    }

    private static final class State {
        private final List<DataRow> rows = new ArrayList<>();
    }
}
