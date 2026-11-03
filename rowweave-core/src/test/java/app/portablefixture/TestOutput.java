package app.portablefixture;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;

public final class TestOutput implements BatchSink {
    private final DataSchema schema;

    public TestOutput(DataSchema schema) {
        this.schema = schema;
    }

    @Override public DataSchema getSchema() {
        record("meta");
        return schema;
    }
    @Override public void collect(DataRow row) { }
    @Override public boolean isWorkerWritable() {
        record("workerWritable");
        return true;
    }
    @Override public String exclusiveDestinationId() {
        record("destination");
        return null;
    }
    @Override public boolean supportsCoordinatedCommit() {
        record("coordinated");
        return false;
    }

    @Override
    public BatchSinkDescriptor batchSinkDescriptor() {
        record("descriptor");
        return new BatchSinkDescriptor(
                1, "test-sink", 1, SchemaFingerprint.of(schema),
                "destination", "domain",
                BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1);
    }

    private static void record(String action) {
        System.setProperty(
                "rowweave.test.portable.sink." + action + ".thread",
                Thread.currentThread().getName());
    }
}
