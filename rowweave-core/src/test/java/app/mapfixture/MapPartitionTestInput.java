package app.mapfixture;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.util.Arrays;
import java.util.Iterator;

/** One deterministic row for one frozen source split. */
public final class MapPartitionTestInput implements BatchSource {
    private static final long serialVersionUID = 1L;
    private static final DataSchema SCHEMA =
            new DataSchema(new Field("id", DataType.LongType));

    private final BatchSourceSplitDescriptor descriptor;

    public MapPartitionTestInput(BatchSourceSplitDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override public DataSchema getSchema() { return SCHEMA; }

    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() {
        return descriptor;
    }

    @Override
    public Iterator<DataRow> iterator() {
        long first = "split-a".equals(descriptor.splitId()) ? 1L : 3L;
        DataRow left = new DataRow(1);
        left.set(0, first);
        DataRow right = new DataRow(1);
        right.set(0, first + 1L);
        return Arrays.asList(left, right).iterator();
    }
}
