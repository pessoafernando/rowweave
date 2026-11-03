package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.SourceInputSchemas;
import io.rowweave.internal.BatchSourceSplitBindings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Worker command payload containing one ordered, frozen SOURCE binding bucket. */
public final class AssignedSourceBatch implements Source {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;
    private final List<BatchSourceSplitBinding> bindings;
    private final List<Source> inputs;

    public AssignedSourceBatch(List<BatchSourceSplitBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException(
                    "assigned source input batch requires at least one binding");
        }
        List<BatchSourceSplitBinding> bindingCopy = new ArrayList<>(bindings.size());
        List<Source> inputCopy = new ArrayList<>(bindings.size());
        Set<String> splitIds = new HashSet<>();
        for (int i = 0; i < bindings.size(); i++) {
            BatchSourceSplitBinding binding = bindings.get(i);
            if (binding == null) {
                throw new IllegalArgumentException(
                        "assigned source binding[" + i + "] must not be null");
            }
            if (!splitIds.add(binding.descriptor().splitId())) {
                throw new IllegalArgumentException(
                        "assigned source duplicate splitId: " + binding.descriptor().splitId());
            }
            bindingCopy.add(binding);
            inputCopy.add(binding.input());
        }
        this.dataSchema = inputCopy.get(0).getSchema();
        this.inputs = new ArrayList<>(SourceInputSchemas.validateAndCopy(
                dataSchema, inputCopy, "AssignedSourceBatch", true));
        String schema = SchemaFingerprint.of(dataSchema);
        for (int i = 0; i < bindingCopy.size(); i++) {
            if (!schema.equals(bindingCopy.get(i).descriptor().schemaFingerprint())) {
                throw new IllegalArgumentException(
                        "assigned source schemaFingerprint mismatch at binding " + i);
            }
        }
        BatchSourceSplitBindings.validateCurrent(bindingCopy);
        this.bindings = bindingCopy;
    }

    @Override public DataSchema getSchema() { return dataSchema; }
    public List<BatchSourceSplitBinding> bindings() {
        return Collections.unmodifiableList(bindings);
    }
    public List<Source> sources() { return Collections.unmodifiableList(inputs); }

    private Object readResolve() {
        return new AssignedSourceBatch(bindings);
    }

    @Override
    public Iterator<DataRow> iterator() {
        throw new UnsupportedOperationException(
                "AssignedSourceBatch requires AssignedSourceInputBatchExecutor");
    }
}
