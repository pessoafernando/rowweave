package io.rowweave.cluster;

import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanNode;

import java.nio.file.Path;
import java.util.Collections;

final class ClusterTestFileInput {

    private ClusterTestFileInput() {
    }

    static FileSource write(Path path, DataSchema meta, Iterable<DataRow> rows) {
        FileSink writer = new FileSink(meta, path.toString());
        try {
            for (DataRow row : rows) {
                writer.collect(row);
            }
            writer.close();
        } catch (RuntimeException | Error failure) {
            try {
                writer.abort();
            } catch (Throwable abortFailure) {
                failure.addSuppressed(abortFailure);
            }
            throw failure;
        }
        return new FileSource(meta, path.toString());
    }

    static PlanNode source(Path path, DataSchema meta, Iterable<DataRow> rows) {
        return PlanNode.sourceFromInputs(meta,
                Collections.singletonList(write(path, meta, rows)));
    }
}

