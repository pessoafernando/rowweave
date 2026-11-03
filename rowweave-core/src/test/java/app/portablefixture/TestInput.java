package app.portablefixture;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;

import java.util.Collections;
import java.util.Iterator;

public final class TestInput implements BatchSource {
    private final DataSchema schema =
            new DataSchema(new Field("id", DataType.LongType));

    @Override public DataSchema getSchema() {
        System.setProperty(
                "rowweave.test.portable.source.meta.thread",
                Thread.currentThread().getName());
        return schema;
    }

    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() {
        System.setProperty(
                "rowweave.test.portable.source.descriptor.thread",
                Thread.currentThread().getName());
        return new BatchSourceSplitDescriptor(
                1, "test-source", 1, "split",
                SchemaFingerprint.of(schema), "snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee
                        .EXACT_ORDERED_FROM_START);
    }

    @Override
    public Iterator<DataRow> iterator() {
        return Collections.<DataRow>emptyList().iterator();
    }
}
