package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * bounded, describable and one-row-at-a-time schema conversion.
 */
public final class SchemaConversion {

    public static final int MAX_FIELDS = 4_096;
    public static final int MAX_UTF8_BYTES = 64 * 1024;

    public enum Cast {
        IDENTITY,
        INTEGER_TO_LONG,
        INTEGER_TO_DECIMAL,
        LONG_TO_DECIMAL
    }

    private final String inputSchemaFingerprint;
    private final DataSchema outputDataSchema;
    private final List<String> sourceIndices;
    private final List<String> casts;
    private final List<String> nullPolicies;
    private final List<String> defaultKinds;
    private final List<String> defaultValues;
    private final int inputFieldCount;
    private final int[] runtimeSourceIndices;
    private final DataType[] runtimeSourceTypes;
    private final Cast[] runtimeCasts;
    private final boolean[] runtimeNullable;
    private final Object[] runtimeDefaults;

    private SchemaConversion(DataSchema inputDataSchema, String inputSchemaFingerprint, DataSchema outputDataSchema,
                             List<String> sourceIndices, List<String> casts,
                             List<String> nullPolicies, List<String> defaultKinds,
                             List<String> defaultValues) {
        this.inputSchemaFingerprint = inputSchemaFingerprint;
        this.outputDataSchema = outputDataSchema;
        this.sourceIndices = immutableCopy(sourceIndices);
        this.casts = immutableCopy(casts);
        this.nullPolicies = immutableCopy(nullPolicies);
        this.defaultKinds = immutableCopy(defaultKinds);
        this.defaultValues = immutableCopy(defaultValues);
        this.inputFieldCount = inputDataSchema.getFieldCount();
        int fields = outputDataSchema.getFieldCount();
        this.runtimeSourceIndices = new int[fields];
        this.runtimeSourceTypes = new DataType[fields];
        this.runtimeCasts = new Cast[fields];
        this.runtimeNullable = new boolean[fields];
        this.runtimeDefaults = new Object[fields];
        for (int i = 0; i < fields; i++) {
            int sourceIndex = Integer.parseInt(sourceIndices.get(i));
            runtimeSourceIndices[i] = sourceIndex;
            runtimeSourceTypes[i] = sourceIndex < 0
                    ? null : inputDataSchema.getField(sourceIndex).getDataType();
            runtimeCasts[i] = Cast.valueOf(casts.get(i));
            runtimeNullable[i] = "NULLABLE".equals(nullPolicies.get(i));
            if ("VALUE".equals(defaultKinds.get(i))) {
                runtimeDefaults[i] = AddFields.decodeValue(
                        outputDataSchema.getField(i).getDataType(), defaultValues.get(i));
            }
        }
    }

    public static Builder builder(DataSchema inputDataSchema) {
        return new Builder(inputDataSchema);
    }

    /**
     * Rebuild a conversion from the primitive/string PlanNode description. This method is deliberately pure so
     * callers can run the same validation before any source, worker or output side effect is admitted.
     */
    public static SchemaConversion fromDescribable(
            DataSchema inputDataSchema, DataSchema outputDataSchema,
            List<String> sourceIndices, List<String> casts,
            List<String> nullPolicies, List<String> defaultKinds,
            List<String> defaultValues) {
        try {
            return rebuild(inputDataSchema, outputDataSchema, sourceIndices, casts,
                    nullPolicies, defaultKinds, defaultValues);
        } catch (IllegalArgumentException failure) {
            String message = failure.getMessage();
            if (message != null && message.startsWith("SCHEMA_MAPPING_INVALID:")) {
                throw failure;
            }
            throw invalid("describable parameters are invalid");
        } catch (RuntimeException failure) {
            throw invalid("describable parameters are invalid");
        }
    }

    private static SchemaConversion rebuild(
            DataSchema inputDataSchema, DataSchema outputDataSchema,
            List<String> sourceIndices, List<String> casts,
            List<String> nullPolicies, List<String> defaultKinds,
            List<String> defaultValues) {
        if (outputDataSchema == null) {
            throw invalid("output DataSchema is required");
        }
        int fieldCount = outputDataSchema.getFieldCount();
        if (fieldCount == 0 || fieldCount > MAX_FIELDS) {
            throw invalid("output field count is out of bounds");
        }
        requireDescriptorSize(sourceIndices, fieldCount);
        requireDescriptorSize(casts, fieldCount);
        requireDescriptorSize(nullPolicies, fieldCount);
        requireDescriptorSize(defaultKinds, fieldCount);
        requireDescriptorSize(defaultValues, fieldCount);

        Builder builder = builder(inputDataSchema);
        for (int i = 0; i < fieldCount; i++) {
            String sourceToken = sourceIndices.get(i);
            int sourceIndex = Integer.parseInt(sourceToken);
            if (!Integer.toString(sourceIndex).equals(sourceToken)) {
                throw invalid("source index is not canonical at index " + i);
            }
            Cast cast = Cast.valueOf(casts.get(i));
            String nullPolicy = nullPolicies.get(i);
            String defaultKind = defaultKinds.get(i);
            String defaultValue = defaultValues.get(i);
            Field target = outputDataSchema.getField(i);

            if (sourceIndex >= 0) {
                if (!"NONE".equals(defaultKind) || !"".equals(defaultValue)) {
                    throw invalid("mapped field has a default at index " + i);
                }
                if ("REQUIRED".equals(nullPolicy)) {
                    builder.required(sourceIndex, target, cast);
                } else if ("NULLABLE".equals(nullPolicy)) {
                    builder.nullable(sourceIndex, target, cast);
                } else {
                    throw invalid("null policy is invalid at index " + i);
                }
                continue;
            }

            if (sourceIndex != -1 || cast != Cast.IDENTITY) {
                throw invalid("default source or cast is invalid at index " + i);
            }
            if ("NULL".equals(defaultKind)) {
                if (!"NULLABLE".equals(nullPolicy) || !"".equals(defaultValue)) {
                    throw invalid("null default is invalid at index " + i);
                }
                builder.defaulted(target, null);
            } else if ("VALUE".equals(defaultKind)) {
                if (!"REQUIRED".equals(nullPolicy)) {
                    throw invalid("value default is invalid at index " + i);
                }
                Object decoded = AddFields.decodeValue(target.getDataType(), defaultValue);
                if (decoded == null || !defaultValue.equals(AddFields.encodeValue(
                        target.getDataType(), decoded))) {
                    throw invalid("default value is not canonical at index " + i);
                }
                builder.defaulted(target, decoded);
            } else {
                throw invalid("default kind is invalid at index " + i);
            }
        }

        SchemaConversion conversion = builder.build();
        if (!conversion.sourceIndices().equals(sourceIndices)
                || !conversion.casts().equals(casts)
                || !conversion.nullPolicies().equals(nullPolicies)
                || !conversion.defaultKinds().equals(defaultKinds)
                || !conversion.defaultValues().equals(defaultValues)) {
            throw invalid("describable parameters are not canonical");
        }
        return conversion;
    }

    private static void requireDescriptorSize(List<String> values, int fieldCount) {
        if (values == null || values.size() != fieldCount) {
            throw invalid("descriptor list length mismatch");
        }
    }

    public DataSchema getOutputSchema() {
        return outputDataSchema;
    }

    public String inputSchemaFingerprint() {
        return inputSchemaFingerprint;
    }

    public List<String> sourceIndices() {
        return sourceIndices;
    }

    public List<String> casts() {
        return casts;
    }

    public List<String> nullPolicies() {
        return nullPolicies;
    }

    public List<String> defaultKinds() {
        return defaultKinds;
    }

    public List<String> defaultValues() {
        return defaultValues;
    }

    /** Convert and push one record at a time; this operator never owns an input-sized work set. */
    public void apply(RowStream in, RowSink out) {
        StreamEvent event;
        while ((event = in.next()) != null) {
            if (event.isRecord()) {
                out.emitRow(project(event.row()));
                continue;
            }
            out.emit(event);
            if (event.isEndOfStream() || event.isError() || event.isCancel()) {
                return;
            }
        }
        out.endOfStream();
    }

    /** Convert one row without retaining it. A failing field never exposes a partially built output row. */
    public DataRow project(DataRow input) {
        int actualWidth = input == null ? -1 : input.size();
        if (actualWidth != inputFieldCount) {
            throw new IllegalStateException("SCHEMA_ROW_WIDTH_MISMATCH expected="
                    + inputFieldCount + " actual=" + actualWidth);
        }
        DataRow output = new DataRow(runtimeSourceIndices.length);
        for (int i = 0; i < runtimeSourceIndices.length; i++) {
            Object value;
            int sourceIndex = runtimeSourceIndices[i];
            if (sourceIndex < 0) {
                value = copyDefault(runtimeDefaults[i]);
            } else {
                value = input.get(sourceIndex);
                if (value == null) {
                    if (!runtimeNullable[i]) {
                        throw new IllegalStateException("SCHEMA_NULL_VIOLATION field=" + i
                                + " type=" + outputDataSchema.getField(i).getDataType());
                    }
                } else {
                    requireRuntimeType(value, runtimeSourceTypes[i], i,
                            outputDataSchema.getField(i).getDataType());
                    value = convert(value, runtimeCasts[i]);
                }
            }
            output.set(i, value);
        }
        return output;
    }

    private static Object convert(Object value, Cast cast) {
        switch (cast) {
            case IDENTITY:
                return value;
            case INTEGER_TO_LONG:
                return ((Integer) value).longValue();
            case INTEGER_TO_DECIMAL:
                return BigDecimal.valueOf(((Integer) value).longValue());
            case LONG_TO_DECIMAL:
                return BigDecimal.valueOf((Long) value);
            default:
                throw new IllegalStateException("SCHEMA_MAPPING_INVALID: unsupported cast");
        }
    }

    private static void requireRuntimeType(
            Object value, DataType sourceType, int outputIndex, DataType targetType) {
        boolean matches;
        switch (sourceType) {
            case BooleanType: matches = value instanceof Boolean; break;
            case StringType: matches = value instanceof String; break;
            case IntegerType: matches = value instanceof Integer; break;
            case LongType: matches = value instanceof Long; break;
            case BigDecimalType: matches = value instanceof BigDecimal; break;
            case DateType: matches = value instanceof Date && !(value instanceof Timestamp); break;
            case TimestampType: matches = value instanceof Timestamp; break;
            default: matches = false;
        }
        if (!matches) {
            throw new IllegalStateException("SCHEMA_VALUE_TYPE_MISMATCH field="
                    + outputIndex + " type=" + targetType);
        }
    }

    private static Object copyDefault(Object value) {
        if (value instanceof Timestamp) {
            Timestamp source = (Timestamp) value;
            Timestamp copy = new Timestamp(source.getTime());
            copy.setNanos(source.getNanos());
            return copy;
        }
        if (value instanceof Date) {
            return new Date(((Date) value).getTime());
        }
        return value;
    }

    private static List<String> immutableCopy(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("SCHEMA_MAPPING_INVALID: " + detail);
    }

    public static final class Builder {
        private final DataSchema inputDataSchema;
        private final String inputSchemaFingerprint;
        private final List<Field> targetFields = new ArrayList<>();
        private final List<String> sourceIndices = new ArrayList<>();
        private final List<String> casts = new ArrayList<>();
        private final List<String> nullPolicies = new ArrayList<>();
        private final List<String> defaultKinds = new ArrayList<>();
        private final List<String> defaultValues = new ArrayList<>();
        private final Set<String> targetNames = new HashSet<>();

        private Builder(DataSchema inputDataSchema) {
            if (inputDataSchema == null) {
                throw invalid("input DataSchema is required");
            }
            if (inputDataSchema.getFieldCount() > MAX_FIELDS) {
                throw invalid("input field count exceeds " + MAX_FIELDS);
            }
            for (int i = 0; i < inputDataSchema.getFieldCount(); i++) {
                Field field = inputDataSchema.getField(i);
                if (field == null || field.getName() == null || field.getAlias() == null
                        || field.getDataType() == null) {
                    throw invalid("input field declaration is invalid at index " + i);
                }
                boundedUtf8(field.getName(), "input field name", i);
                boundedUtf8(field.getAlias(), "input field alias", i);
            }
            this.inputDataSchema = inputDataSchema;
            try {
                this.inputSchemaFingerprint = SchemaFingerprint.of(inputDataSchema);
            } catch (RuntimeException failure) {
                throw invalid("input schema cannot be fingerprinted");
            }
        }

        public Builder required(String sourceName, Field targetField, Cast cast) {
            return mapped(resolveUniqueSourceName(sourceName), targetField, cast, "REQUIRED");
        }

        public Builder required(int sourceIndex, Field targetField, Cast cast) {
            return mapped(sourceIndex, targetField, cast, "REQUIRED");
        }

        public Builder nullable(String sourceName, Field targetField, Cast cast) {
            return mapped(resolveUniqueSourceName(sourceName), targetField, cast, "NULLABLE");
        }

        public Builder nullable(int sourceIndex, Field targetField, Cast cast) {
            return mapped(sourceIndex, targetField, cast, "NULLABLE");
        }

        public Builder defaulted(Field targetField, Object value) {
            Field target = requireTarget(targetField);
            validateDefault(target.getDataType(), value);
            targetFields.add(target);
            sourceIndices.add("-1");
            casts.add(Cast.IDENTITY.name());
            nullPolicies.add(value == null ? "NULLABLE" : "REQUIRED");
            defaultKinds.add(value == null ? "NULL" : "VALUE");
            String encoded = value == null ? "" : AddFields.encodeValue(target.getDataType(), value);
            boundedUtf8(encoded, "default value", targetFields.size() - 1);
            defaultValues.add(encoded);
            return this;
        }

        public SchemaConversion build() {
            if (targetFields.isEmpty()) {
                throw invalid("at least one target field is required");
            }
            if (targetFields.size() > MAX_FIELDS) {
                throw invalid("target field count exceeds " + MAX_FIELDS);
            }
            Field[] frozenFields = new Field[targetFields.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                Field field = targetFields.get(i);
                frozenFields[i] = new Field(field.getName(), field.getAlias(), field.getDataType());
            }
            return new SchemaConversion(inputDataSchema, inputSchemaFingerprint, new DataSchema(frozenFields),
                    sourceIndices, casts, nullPolicies, defaultKinds, defaultValues);
        }

        private Builder mapped(int sourceIndex, Field targetField, Cast cast, String nullPolicy) {
            if (sourceIndex < 0 || sourceIndex >= inputDataSchema.getFieldCount()) {
                throw invalid("source index is out of bounds");
            }
            if (cast == null) {
                throw invalid("cast is required");
            }
            Field source = inputDataSchema.getField(sourceIndex);
            Field target = requireTarget(targetField);
            validateCast(source.getDataType(), target.getDataType(), cast);
            targetFields.add(target);
            sourceIndices.add(Integer.toString(sourceIndex));
            casts.add(cast.name());
            nullPolicies.add(nullPolicy);
            defaultKinds.add("NONE");
            defaultValues.add("");
            return this;
        }

        private int resolveUniqueSourceName(String sourceName) {
            if (sourceName == null || sourceName.isEmpty()) {
                throw invalid("source name is required");
            }
            int found = -1;
            for (int i = 0; i < inputDataSchema.getFieldCount(); i++) {
                if (sourceName.equals(inputDataSchema.getField(i).getName())) {
                    if (found >= 0) {
                        throw invalid("source name is ambiguous");
                    }
                    found = i;
                }
            }
            if (found < 0) {
                throw invalid("source name does not exist");
            }
            return found;
        }

        private Field requireTarget(Field field) {
            int index = targetFields.size();
            if (field == null || field.getName() == null || field.getName().isEmpty()
                    || field.getAlias() == null || field.getDataType() == null
                    || field.getDataType() == DataType.NullType) {
                throw invalid("target field declaration is invalid at index " + index);
            }
            boundedUtf8(field.getName(), "target field name", index);
            boundedUtf8(field.getAlias(), "target field alias", index);
            if (!targetNames.add(field.getName())) {
                throw invalid("target field name is duplicated");
            }
            return new Field(field.getName(), field.getAlias(), field.getDataType());
        }

        private static void validateCast(DataType source, DataType target, Cast cast) {
            if (source == null || source == DataType.NullType || target == null || target == DataType.NullType) {
                throw invalid("NullType is not supported by schema conversion");
            }
            boolean allowed;
            switch (cast) {
                case IDENTITY:
                    allowed = source == target;
                    break;
                case INTEGER_TO_LONG:
                    allowed = source == DataType.IntegerType && target == DataType.LongType;
                    break;
                case INTEGER_TO_DECIMAL:
                    allowed = source == DataType.IntegerType && target == DataType.BigDecimalType;
                    break;
                case LONG_TO_DECIMAL:
                    allowed = source == DataType.LongType && target == DataType.BigDecimalType;
                    break;
                default:
                    allowed = false;
            }
            if (!allowed) {
                throw invalid("source, target and cast are incompatible");
            }
        }

        private static void validateDefault(DataType type, Object value) {
            if (type == null || type == DataType.NullType) {
                throw invalid("NullType default is not supported");
            }
            if (value == null) {
                return;
            }
            boolean exact;
            switch (type) {
                case BooleanType: exact = value.getClass() == Boolean.class; break;
                case StringType: exact = value.getClass() == String.class; break;
                case IntegerType: exact = value.getClass() == Integer.class; break;
                case LongType: exact = value.getClass() == Long.class; break;
                case BigDecimalType: exact = value.getClass() == BigDecimal.class; break;
                case DateType: exact = value.getClass() == Date.class; break;
                case TimestampType: exact = value.getClass() == Timestamp.class; break;
                default: exact = false;
            }
            if (!exact) {
                throw invalid("default value type does not match target type");
            }
        }

        private static void boundedUtf8(String value, String owner, int index) {
            if (value.getBytes(StandardCharsets.UTF_8).length > MAX_UTF8_BYTES) {
                throw invalid(owner + " exceeds " + MAX_UTF8_BYTES + " bytes at index " + index);
            }
        }
    }
}
