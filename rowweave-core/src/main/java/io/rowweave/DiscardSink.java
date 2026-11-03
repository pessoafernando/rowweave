package io.rowweave;

/**
 * A Sink that discards every result row. For jobs and tests that only care about completion status or rowCount: it buffers no rows and produces no external side effect.
 * It travels with the PlanGraph and executes at the tail of a Worker.
 */
public final class DiscardSink implements BatchSink {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;

    public DiscardSink(DataSchema dataSchema) {
        this.dataSchema = dataSchema;
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public BatchSinkDescriptor batchSinkDescriptor() {
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "rowweave.discard", 1,
                io.rowweave.commit.SchemaFingerprint.of(dataSchema),
                "none:discard", "none:side-effect-free",
                BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1);
    }

    @Override
    public void collect(DataRow row) {
        // Intentionally discard each pushed row without materialization.
    }

    @Override
    public boolean isWorkerWritable() {
        return true;
    }

    @Override
    public boolean supportsCoordinatedCommit() {
        return true;
    }

    @Override
    public void prepareCommit() {
        // no-op: there is no external state.
    }

    @Override
    public void commitPrepared() {
        // no-op
    }

    @Override
    public void rollbackCommit() {
        // no-op
    }

    @Override
    public void finalizeCommit() {
        // no-op
    }
}
