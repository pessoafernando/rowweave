package io.rowweave.internal;

import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** side-effect-free v1 Source[] schema validation. */
public final class SourceInputSchemas {

    private SourceInputSchemas() {
    }

    /** Validate a public Source[] entry point and return an immutable descriptor snapshot. */
    public static List<Source> validateAgainstFirst(
            List<? extends Source> inputs, String owner) {
        if (inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException(owner + " requires at least one Source");
        }
        Source first = requireInput(inputs.get(0), 0, owner);
        DataSchema expected = requireMeta(first.getSchema(), "input[0]", owner);
        return validateAndCopy(expected, inputs, owner, false);
    }

    /** Validate PlanNode SOURCE descriptors against its declared metadata. Empty is a placeholder source. */
    public static List<Source> validateAndCopy(
            DataSchema expected, List<? extends Source> inputs, String owner,
            boolean requireNonEmpty) {
        DataSchema declared = requireMeta(expected, "dataSchema", owner);
        if (inputs == null) {
            throw new IllegalArgumentException(owner + " inputs must not be null");
        }
        if (requireNonEmpty && inputs.isEmpty()) {
            throw new IllegalArgumentException(owner + " requires at least one Source");
        }
        List<Source> copy = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            Source input = requireInput(inputs.get(i), i, owner);
            DataSchema actual = requireMeta(input.getSchema(), "input[" + i + "]", owner);
            requireV1Compatible(declared, actual, i, owner);
            copy.add(input);
        }
        return Collections.unmodifiableList(copy);
    }

    private static Source requireInput(Source input, int index, String owner) {
        if (input == null) {
            throw new IllegalArgumentException(
                    owner + " input[" + index + "] must not be null");
        }
        return input;
    }

    private static DataSchema requireMeta(DataSchema meta, String path, String owner) {
        if (meta == null) {
            throw new IllegalArgumentException(owner + " " + path + ".dataSchema must not be null");
        }
        for (int i = 0; i < meta.getFieldCount(); i++) {
            Field field = meta.getField(i);
            if (field == null) {
                throw new IllegalArgumentException(
                        owner + " " + path + ".dataSchema.field[" + i + "] must not be null");
            }
            if (field.getName() == null) {
                throw new IllegalArgumentException(
                        owner + " " + path + ".dataSchema.field[" + i + "].name must not be null");
            }
            if (field.getDataType() == null) {
                throw new IllegalArgumentException(
                        owner + " " + path + ".dataSchema.field[" + i + "].dataType must not be null");
            }
        }
        return meta;
    }

    /** v1 compatibility intentionally compares ordered field name + type, not alias. */
    private static void requireV1Compatible(
            DataSchema expected, DataSchema actual, int inputIndex, String owner) {
        if (expected.getFieldCount() != actual.getFieldCount()) {
            throw new IllegalArgumentException(owner + " input[" + inputIndex
                    + "] field count differs from input[0]: expected="
                    + expected.getFieldCount() + " actual=" + actual.getFieldCount());
        }
        for (int i = 0; i < expected.getFieldCount(); i++) {
            Field left = expected.getField(i);
            Field right = actual.getField(i);
            if (!left.getName().equals(right.getName())) {
                throw new IllegalArgumentException(owner + " input[" + inputIndex
                        + "] field[" + i + "] name differs from input[0]");
            }
            if (left.getDataType() != right.getDataType()) {
                throw new IllegalArgumentException(owner + " input[" + inputIndex
                        + "] field[" + i + "] dataType differs from input[0]");
            }
        }
    }
}
