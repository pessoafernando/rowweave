package app.mapfixture;

import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.MapPartitionFunction;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.io.IOException;

/** Emits one partition-sensitive sum so row-level repartitioning is observable. */
public final class PrefixingMapPartition implements MapPartitionFunction {
    private static final long serialVersionUID = 1L;
    public static final String INVOCATIONS_PROPERTY =
            "rowweave.test.map-partition.invocations";
    private static final String OBSERVE_PREFIX = "observe:";
    private static final DataSchema SCHEMA =
            new DataSchema(new Field("id", DataType.LongType));
    private final String observationRoot;

    public PrefixingMapPartition() {
        this.observationRoot = null;
    }

    public PrefixingMapPartition(byte[] configuration) {
        String decoded = configuration == null ? ""
                : new String(configuration, StandardCharsets.UTF_8);
        this.observationRoot = decoded.startsWith(OBSERVE_PREFIX)
                ? decoded.substring(OBSERVE_PREFIX.length()) : null;
    }

    @Override
    public void mapPartition(PartitionDataRowCursor rows, Collector collector) {
        increment(INVOCATIONS_PROPERTY);
        recordInvocation();
        long sum = 0L;
        DataRow row;
        while ((row = rows.next()) != null) {
            sum = Math.addExact(sum, row.getLong(0));
        }
        DataRow result = new DataRow(1);
        result.set(0, sum);
        collector.collect(result);
    }

    @Override public DataSchema getResultSchema() { return SCHEMA; }

    private static void increment(String property) {
        synchronized (System.getProperties()) {
            String current = System.getProperty(property, "0");
            System.setProperty(property,
                    Integer.toString(Integer.parseInt(current) + 1));
        }
    }

    private void recordInvocation() {
        if (observationRoot == null) return;
        try {
            Path marker = Files.createTempFile(
                    Paths.get(observationRoot), "invocation-", ".marker");
            String worker = System.getProperty(
                    "rowweave.test.map-partition.worker.label", "unknown");
            Files.write(marker, worker.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "failed to record map-partition invocation", failure);
        }
    }
}
