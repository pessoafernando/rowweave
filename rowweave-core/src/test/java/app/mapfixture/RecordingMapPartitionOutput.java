package app.mapfixture;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Process-global recording is confined to the signed test deployment. */
public final class RecordingMapPartitionOutput implements BatchSink {
    private static final long serialVersionUID = 1L;
    public static final String ROWS_PROPERTY =
            "rowweave.test.map-partition.rows";

    private final DataSchema schema;
    private final BatchSinkDescriptor descriptor;
    private final Path observationFile;

    public RecordingMapPartitionOutput(
            DataSchema schema, BatchSinkDescriptor descriptor) {
        this(schema, descriptor, null);
    }

    public RecordingMapPartitionOutput(
            DataSchema schema, BatchSinkDescriptor descriptor,
            byte[] configuration) {
        this.schema = schema;
        this.descriptor = descriptor;
        this.observationFile = observationFile(configuration);
    }

    @Override public DataSchema getSchema() { return schema; }

    @Override
    public void collect(DataRow row) {
        synchronized (System.getProperties()) {
            String current = System.getProperty(ROWS_PROPERTY, "");
            String separator = current.isEmpty() ? "" : ",";
            System.setProperty(ROWS_PROPERTY,
                    current + separator + row.getLong(0));
        }
        if (observationFile != null) {
            try {
                Files.write(observationFile,
                        (Long.toString(row.getLong(0)) + "\n")
                                .getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.APPEND);
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "failed to record map-partition output", failure);
            }
        }
    }

    @Override public boolean isWorkerWritable() { return true; }

    @Override
    public BatchSinkDescriptor batchSinkDescriptor() {
        return descriptor;
    }

    private static Path observationFile(byte[] configuration) {
        String decoded = configuration == null ? ""
                : new String(configuration, StandardCharsets.UTF_8);
        if (!decoded.startsWith("observe:")) return null;
        try {
            return Files.createTempFile(
                    Paths.get(decoded.substring("observe:".length())),
                    "rows-", ".txt");
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "failed to create map-partition output observation",
                    failure);
        }
    }
}
