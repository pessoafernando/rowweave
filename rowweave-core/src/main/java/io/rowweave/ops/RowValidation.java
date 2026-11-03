package io.rowweave.ops;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.SourceRecordPosition;
import io.rowweave.sched.stream.StreamEvent;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** bounded and describable validation for already-decoded typed rows. */
public final class RowValidation {

    public static final int MAX_RULES = 1_024;
    public static final int MAX_FIELDS = 4_096;
    public static final int MAX_RULE_ID_BYTES = 64;
    public static final int MAX_BOUND_BYTES = 1_024;
    public static final int MAX_SCHEMA_TEXT_BYTES = 64 * 1024;
    public static final int MAX_STRING_UTF8_BYTES = 16 * 1024 * 1024;

    public enum Kind {
        NOT_NULL,
        INTEGER_RANGE_CLOSED,
        LONG_RANGE_CLOSED,
        DECIMAL_RANGE_CLOSED,
        STRING_UTF8_BYTES_RANGE_CLOSED
    }

    /** package-private structured result consumed by the built-in classifier. */
    static final class Evaluation {
        enum Status {
            VALID,
            ROW_WIDTH_MISMATCH,
            VALUE_TYPE_MISMATCH,
            RULE_VIOLATION
        }

        private static final Evaluation VALID = new Evaluation(
                Status.VALID, "", "", -1, "", "", -1);

        private final Status status;
        private final String errorCode;
        private final String ruleId;
        private final int fieldIndex;
        private final String detail;
        private final String failFastRuleId;
        private final int actualWidth;

        private Evaluation(Status status, String errorCode, String ruleId,
                           int fieldIndex, String detail, String failFastRuleId,
                           int actualWidth) {
            this.status = status;
            this.errorCode = errorCode;
            this.ruleId = ruleId;
            this.fieldIndex = fieldIndex;
            this.detail = detail;
            this.failFastRuleId = failFastRuleId;
            this.actualWidth = actualWidth;
        }

        static Evaluation width(int actualWidth) {
            return new Evaluation(Status.ROW_WIDTH_MISMATCH,
                    "QUALITY_ROW_WIDTH_MISMATCH", "", -1, "ROW_WIDTH", "", actualWidth);
        }

        static Evaluation type(String failFastRuleId, int fieldIndex, DataType expected) {
            return new Evaluation(Status.VALUE_TYPE_MISMATCH,
                    "QUALITY_VALUE_TYPE_MISMATCH", "", fieldIndex,
                    expected.name(), failFastRuleId, -1);
        }

        static Evaluation rule(String ruleId, int fieldIndex, Kind kind) {
            return new Evaluation(Status.RULE_VIOLATION,
                    "QUALITY_RULE_VIOLATION", ruleId, fieldIndex,
                    kind.name(), ruleId, -1);
        }

        boolean isValid() { return status == Status.VALID; }
        Status status() { return status; }
        String errorCode() { return errorCode; }
        String ruleId() { return ruleId; }
        int fieldIndex() { return fieldIndex; }
        String detail() { return detail; }

        @Override
        public String toString() {
            return "Evaluation{status=" + status + ", errorCode=" + errorCode
                    + ", ruleId=" + ruleId + ", fieldIndex=" + fieldIndex
                    + ", detail=" + detail + '}';
        }
    }

    private final DataSchema inputDataSchema;
    private final String inputSchemaFingerprint;
    private final List<String> ruleIds;
    private final List<String> ruleKinds;
    private final List<String> fieldIndices;
    private final List<String> lowerBounds;
    private final List<String> upperBounds;
    private final int inputFieldCount;
    private final String[] runtimeRuleIds;
    private final Kind[] runtimeKinds;
    private final int[] runtimeFieldIndices;
    private final DataType[] runtimeFieldTypes;
    private final Object[] runtimeLowerBounds;
    private final Object[] runtimeUpperBounds;

    private RowValidation(DataSchema inputDataSchema, String inputSchemaFingerprint,
                          List<String> ruleIds, List<String> ruleKinds,
                          List<String> fieldIndices, List<String> lowerBounds,
                          List<String> upperBounds) {
        this.inputDataSchema = inputDataSchema;
        this.inputSchemaFingerprint = inputSchemaFingerprint;
        this.ruleIds = immutableCopy(ruleIds);
        this.ruleKinds = immutableCopy(ruleKinds);
        this.fieldIndices = immutableCopy(fieldIndices);
        this.lowerBounds = immutableCopy(lowerBounds);
        this.upperBounds = immutableCopy(upperBounds);
        this.inputFieldCount = inputDataSchema.getFieldCount();
        int count = ruleIds.size();
        this.runtimeRuleIds = new String[count];
        this.runtimeKinds = new Kind[count];
        this.runtimeFieldIndices = new int[count];
        this.runtimeFieldTypes = new DataType[count];
        this.runtimeLowerBounds = new Object[count];
        this.runtimeUpperBounds = new Object[count];
        for (int i = 0; i < count; i++) {
            Kind kind = Kind.valueOf(ruleKinds.get(i));
            int fieldIndex = Integer.parseInt(fieldIndices.get(i));
            runtimeRuleIds[i] = ruleIds.get(i);
            runtimeKinds[i] = kind;
            runtimeFieldIndices[i] = fieldIndex;
            runtimeFieldTypes[i] = inputDataSchema.getField(fieldIndex).getDataType();
            switch (kind) {
                case INTEGER_RANGE_CLOSED:
                    runtimeLowerBounds[i] = Integer.valueOf(lowerBounds.get(i));
                    runtimeUpperBounds[i] = Integer.valueOf(upperBounds.get(i));
                    break;
                case LONG_RANGE_CLOSED:
                    runtimeLowerBounds[i] = Long.valueOf(lowerBounds.get(i));
                    runtimeUpperBounds[i] = Long.valueOf(upperBounds.get(i));
                    break;
                case DECIMAL_RANGE_CLOSED:
                    runtimeLowerBounds[i] = new BigDecimal(lowerBounds.get(i));
                    runtimeUpperBounds[i] = new BigDecimal(upperBounds.get(i));
                    break;
                case STRING_UTF8_BYTES_RANGE_CLOSED:
                    runtimeLowerBounds[i] = Integer.valueOf(lowerBounds.get(i));
                    runtimeUpperBounds[i] = Integer.valueOf(upperBounds.get(i));
                    break;
                case NOT_NULL:
                    break;
                default:
                    throw invalid("unsupported runtime rule kind");
            }
        }
    }

    public static Builder builder(DataSchema inputDataSchema) {
        return new Builder(inputDataSchema);
    }

    /** Pure canonical rebuild shared by admission and, after QV2/QV3, worker execution. */
    public static RowValidation fromDescribable(
            DataSchema inputDataSchema, DataSchema outputDataSchema,
            List<String> ruleIds, List<String> ruleKinds,
            List<String> fieldIndices, List<String> lowerBounds,
            List<String> upperBounds) {
        try {
            return rebuild(inputDataSchema, outputDataSchema, ruleIds, ruleKinds,
                    fieldIndices, lowerBounds, upperBounds);
        } catch (IllegalArgumentException failure) {
            if (failure.getMessage() != null
                    && failure.getMessage().startsWith("QUALITY_VALIDATION_INVALID:")) {
                throw failure;
            }
            throw invalid("describable parameters are invalid");
        } catch (RuntimeException failure) {
            throw invalid("describable parameters are invalid");
        }
    }

    private static RowValidation rebuild(
            DataSchema inputDataSchema, DataSchema outputDataSchema,
            List<String> ruleIds, List<String> ruleKinds,
            List<String> fieldIndices, List<String> lowerBounds,
            List<String> upperBounds) {
        Builder builder = builder(inputDataSchema);
        if (outputDataSchema == null || !SchemaFingerprint.of(inputDataSchema)
                .equals(SchemaFingerprint.of(outputDataSchema))) {
            throw invalid("output schema must exactly match input schema");
        }
        int count = requireDescriptorSizes(
                ruleIds, ruleKinds, fieldIndices, lowerBounds, upperBounds);
        for (int i = 0; i < count; i++) {
            String indexToken = fieldIndices.get(i);
            int fieldIndex = Integer.parseInt(indexToken);
            if (!Integer.toString(fieldIndex).equals(indexToken)) {
                throw invalid("field index is not canonical at rule " + i);
            }
            Kind kind = Kind.valueOf(ruleKinds.get(i));
            String ruleId = ruleIds.get(i);
            String lower = lowerBounds.get(i);
            String upper = upperBounds.get(i);
            switch (kind) {
                case NOT_NULL:
                    if (!lower.isEmpty() || !upper.isEmpty()) {
                        throw invalid("NOT_NULL bounds must be empty at rule " + i);
                    }
                    builder.notNull(ruleId, fieldIndex);
                    break;
                case INTEGER_RANGE_CLOSED:
                    builder.integerRange(ruleId, fieldIndex,
                            canonicalInteger(lower), canonicalInteger(upper));
                    break;
                case LONG_RANGE_CLOSED:
                    builder.longRange(ruleId, fieldIndex,
                            canonicalLong(lower), canonicalLong(upper));
                    break;
                case DECIMAL_RANGE_CLOSED:
                    builder.decimalRange(ruleId, fieldIndex,
                            canonicalDecimal(lower), canonicalDecimal(upper));
                    break;
                case STRING_UTF8_BYTES_RANGE_CLOSED:
                    builder.stringUtf8BytesRange(ruleId, fieldIndex,
                            canonicalStringBound(lower), canonicalStringBound(upper));
                    break;
                default:
                    throw invalid("unknown rule kind at rule " + i);
            }
        }
        RowValidation rebuilt = builder.build();
        if (!rebuilt.ruleIds.equals(ruleIds)
                || !rebuilt.ruleKinds.equals(ruleKinds)
                || !rebuilt.fieldIndices.equals(fieldIndices)
                || !rebuilt.lowerBounds.equals(lowerBounds)
                || !rebuilt.upperBounds.equals(upperBounds)) {
            throw invalid("describable parameters are not canonical");
        }
        return rebuilt;
    }

    @SafeVarargs
    private static int requireDescriptorSizes(List<String>... lists) {
        if (lists[0] == null) throw invalid("descriptor list is required");
        int count = lists[0].size();
        if (count == 0 || count > MAX_RULES) {
            throw invalid("rule count is out of bounds");
        }
        for (List<String> list : lists) {
            if (list == null || list.size() != count) {
                throw invalid("descriptor list length mismatch");
            }
        }
        return count;
    }

    public DataSchema getInputSchema() {
        return inputDataSchema;
    }

    public String inputSchemaFingerprint() {
        return inputSchemaFingerprint;
    }

    public List<String> ruleIds() {
        return ruleIds;
    }

    public List<String> ruleKinds() {
        return ruleKinds;
    }

    public List<String> fieldIndices() {
        return fieldIndices;
    }

    public List<String> lowerBounds() {
        return lowerBounds;
    }

    public List<String> upperBounds() {
        return upperBounds;
    }

    /** Validate and push exactly one record at a time without copying or retaining input rows. */
    public void apply(RowStream in, RowSink out) {
        StreamEvent event;
        while ((event = in.next()) != null) {
            if (event.isRecord()) {
                validate(event.row());
                out.emit(event);
                continue;
            }
            out.emit(event);
            if (event.isEndOfStream() || event.isError() || event.isCancel()) {
                return;
            }
        }
        out.endOfStream();
    }

    /**
     * opt-in runtime: require exact source positions for every RECORD, append the
     * bounded diagnostic to quality failures, and strip the position before successful emit.
     */
    public void applyWithSourcePosition(
            RowStream in, RowSink out, String expectedSourceOperatorUid,
            List<BatchSourceSplitDescriptor> exactSourceSplits) {
        SourcePositionSequence sequence = new SourcePositionSequence(
                expectedSourceOperatorUid, exactSourceSplits);
        StreamEvent event;
        while ((event = in.next()) != null) {
            if (event.isRecord()) {
                SourceRecordPosition position = sequence.verify(event);
                try {
                    validate(event.row());
                } catch (IllegalStateException qualityFailure) {
                    throw located(qualityFailure, position);
                }
                out.emit(StreamEvent.record(event.row()));
                continue;
            }
            out.emit(event);
            if (event.isEndOfStream() || event.isError() || event.isCancel()) {
                return;
            }
        }
        out.endOfStream();
    }

    /** Validate one typed row. A failure never invokes DataRow.toString or retains the row. */
    public void validate(DataRow row) {
        Evaluation result = evaluate(row);
        if (result.isValid()) return;
        switch (result.status) {
            case ROW_WIDTH_MISMATCH:
                throw new IllegalStateException("QUALITY_ROW_WIDTH_MISMATCH expected="
                        + inputFieldCount + " actual=" + result.actualWidth);
            case VALUE_TYPE_MISMATCH:
                throw new IllegalStateException("QUALITY_VALUE_TYPE_MISMATCH rule="
                        + result.failFastRuleId + " field=" + result.fieldIndex
                        + " type=" + result.detail);
            case RULE_VIOLATION:
                throw new IllegalStateException("QUALITY_RULE_VIOLATION rule="
                        + result.failFastRuleId + " field=" + result.fieldIndex
                        + " kind=" + result.detail);
            default:
                throw new IllegalStateException("QUALITY_VALIDATION_RUNTIME_INVALID");
        }
    }

    /** One deterministic structured evaluation; VALID reuses a singleton and retains no row. */
    Evaluation evaluate(DataRow row) {
        int actualWidth = row == null ? -1 : row.size();
        if (actualWidth != inputFieldCount) {
            return Evaluation.width(actualWidth);
        }
        return evaluateRules(row, true);
    }

    /** Classification additionally protects every schema field before a carrier can be serialized. */
    Evaluation evaluateForClassification(DataRow row) {
        int actualWidth = row == null ? -1 : row.size();
        if (actualWidth != inputFieldCount) {
            return Evaluation.width(actualWidth);
        }
        for (int i = 0; i < inputFieldCount; i++) {
            Object value = row.get(i);
            DataType expected = inputDataSchema.getField(i).getDataType();
            if (value != null && !matchesRuntimeType(value, expected)) {
                return Evaluation.type("", i, expected);
            }
        }
        return evaluateRules(row, false);
    }

    private Evaluation evaluateRules(DataRow row, boolean checkRuleTypes) {
        for (int i = 0; i < runtimeKinds.length; i++) {
            int fieldIndex = runtimeFieldIndices[i];
            Object value = row.get(fieldIndex);
            if (value == null) {
                if (runtimeKinds[i] == Kind.NOT_NULL) return evaluationViolation(i);
                continue;
            }
            if (checkRuleTypes && !matchesRuntimeType(value, runtimeFieldTypes[i])) {
                return Evaluation.type(runtimeRuleIds[i], fieldIndex, runtimeFieldTypes[i]);
            }
            switch (runtimeKinds[i]) {
                case NOT_NULL:
                    break;
                case INTEGER_RANGE_CLOSED: {
                    int actual = (Integer) value;
                    if (actual < (Integer) runtimeLowerBounds[i]
                            || actual > (Integer) runtimeUpperBounds[i]) {
                        return evaluationViolation(i);
                    }
                    break;
                }
                case LONG_RANGE_CLOSED: {
                    long actual = (Long) value;
                    if (actual < (Long) runtimeLowerBounds[i]
                            || actual > (Long) runtimeUpperBounds[i]) {
                        return evaluationViolation(i);
                    }
                    break;
                }
                case DECIMAL_RANGE_CLOSED: {
                    BigDecimal actual = (BigDecimal) value;
                    if (actual.compareTo((BigDecimal) runtimeLowerBounds[i]) < 0
                            || actual.compareTo((BigDecimal) runtimeUpperBounds[i]) > 0) {
                        return evaluationViolation(i);
                    }
                    break;
                }
                case STRING_UTF8_BYTES_RANGE_CLOSED: {
                    int min = (Integer) runtimeLowerBounds[i];
                    int max = (Integer) runtimeUpperBounds[i];
                    long bytes = strictUtf8Length((String) value, max);
                    if (bytes < min || bytes > max) return evaluationViolation(i);
                    break;
                }
                default:
                    throw new IllegalStateException("QUALITY_VALIDATION_RUNTIME_INVALID");
            }
        }
        return Evaluation.VALID;
    }

    static boolean matchesRuntimeType(Object value, DataType type) {
        switch (type) {
            case BooleanType: return value.getClass() == Boolean.class;
            case StringType: return value.getClass() == String.class;
            case IntegerType: return value.getClass() == Integer.class;
            case LongType: return value.getClass() == Long.class;
            case BigDecimalType: return value.getClass() == BigDecimal.class;
            case DateType: return value instanceof Date && !(value instanceof Timestamp);
            case TimestampType: return value.getClass() == Timestamp.class;
            case NullType:
                return value instanceof Boolean || value instanceof String
                        || value instanceof Integer || value instanceof Long
                        || value instanceof BigDecimal || value instanceof Timestamp
                        || value instanceof Date;
            default: return false;
        }
    }

    /** Returns max+1 as soon as the closed upper bound is exceeded; malformed UTF-16 always fails. */
    private static long strictUtf8Length(String value, int maxBytes) {
        long bytes = 0L;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current <= 0x7f) {
                bytes++;
            } else if (current <= 0x7ff) {
                bytes += 2L;
            } else if (Character.isHighSurrogate(current)) {
                if (i + 1 >= value.length()
                        || !Character.isLowSurrogate(value.charAt(i + 1))) {
                    return maxBytes + 1L;
                }
                i++;
                bytes += 4L;
            } else if (Character.isLowSurrogate(current)) {
                return maxBytes + 1L;
            } else {
                bytes += 3L;
            }
            if (bytes > maxBytes) return bytes;
        }
        return bytes;
    }

    private Evaluation evaluationViolation(int ruleIndex) {
        return Evaluation.rule(runtimeRuleIds[ruleIndex], runtimeFieldIndices[ruleIndex],
                runtimeKinds[ruleIndex]);
    }

    private static IllegalStateException located(
            IllegalStateException qualityFailure, SourceRecordPosition position) {
        return new IllegalStateException(qualityFailure.getMessage()
                + " source=" + position.sourceOperatorUid()
                + " split=" + position.splitIdentitySha256()
                + " ordinal=" + position.splitLocalOrdinal(), qualityFailure);
    }

    private static IllegalStateException invalidSourcePosition(String detail) {
        return new IllegalStateException("QUALITY_SOURCE_POSITION_INVALID: " + detail);
    }

    /** Per-operator bounded verifier; state is one next ordinal per frozen split, never per row. */
    static final class SourcePositionSequence {
        private final String expectedSourceOperatorUid;
        private final Map<String, Integer> splitIndices;
        private final long[] nextOrdinals;

        SourcePositionSequence(
                String expectedSourceOperatorUid,
                List<BatchSourceSplitDescriptor> exactSourceSplits) {
            if (exactSourceSplits == null || exactSourceSplits.isEmpty()) {
                throw invalidSourcePosition("exact source splits are required");
            }
            this.splitIndices = new HashMap<>(exactSourceSplits.size());
            this.nextOrdinals = new long[exactSourceSplits.size()];
            String validatedUid = null;
            for (int i = 0; i < exactSourceSplits.size(); i++) {
                BatchSourceSplitDescriptor descriptor = exactSourceSplits.get(i);
                final SourceRecordPosition initial;
                try {
                    initial = SourceRecordPosition.of(
                            expectedSourceOperatorUid, descriptor, 0L);
                } catch (IllegalArgumentException invalid) {
                    throw invalidSourcePosition("source expectation is invalid");
                }
                if (validatedUid == null) validatedUid = initial.sourceOperatorUid();
                if (splitIndices.put(initial.splitIdentitySha256(), i) != null) {
                    throw invalidSourcePosition("source split identity is duplicated");
                }
            }
            this.expectedSourceOperatorUid = validatedUid;
        }

        SourceRecordPosition verify(StreamEvent event) {
            if (!event.hasSourcePosition()) {
                throw invalidSourcePosition("RECORD has no source position");
            }
            SourceRecordPosition position = event.sourcePosition();
            if (!expectedSourceOperatorUid.equals(position.sourceOperatorUid())) {
                throw invalidSourcePosition("source operator identity mismatch");
            }
            Integer splitIndex = splitIndices.get(position.splitIdentitySha256());
            if (splitIndex == null) {
                throw invalidSourcePosition("source split identity mismatch");
            }
            long expectedOrdinal = nextOrdinals[splitIndex];
            if (position.splitLocalOrdinal() != expectedOrdinal) {
                throw invalidSourcePosition("split-local ordinal is not contiguous");
            }
            if (expectedOrdinal == Long.MAX_VALUE) {
                throw invalidSourcePosition("split-local ordinal overflow");
            }
            nextOrdinals[splitIndex] = expectedOrdinal + 1L;
            return position;
        }
    }

    private static List<String> immutableCopy(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static int canonicalInteger(String token) {
        if (token == null || token.isEmpty() || token.length() > 11) {
            throw invalid("integer bound is invalid");
        }
        int value = Integer.parseInt(token);
        if (!Integer.toString(value).equals(token)) {
            throw invalid("integer bound is not canonical");
        }
        return value;
    }

    private static long canonicalLong(String token) {
        if (token == null || token.isEmpty() || token.length() > 20) {
            throw invalid("long bound is invalid");
        }
        long value = Long.parseLong(token);
        if (!Long.toString(value).equals(token)) {
            throw invalid("long bound is not canonical");
        }
        return value;
    }

    private static BigDecimal canonicalDecimal(String token) {
        if (token == null || token.isEmpty()
                || token.getBytes(StandardCharsets.UTF_8).length > MAX_BOUND_BYTES) {
            throw invalid("decimal bound is invalid or exceeds its byte limit");
        }
        BigDecimal value = new BigDecimal(token);
        if (!value.toPlainString().equals(token)) {
            throw invalid("decimal bound is not canonical plain notation");
        }
        return value;
    }

    private static String canonicalDecimal(BigDecimal value) {
        if (value == null || estimatedPlainLength(value) > MAX_BOUND_BYTES) {
            throw invalid("decimal bound is invalid or exceeds its byte limit");
        }
        String token = value.toPlainString();
        if (token.getBytes(StandardCharsets.UTF_8).length > MAX_BOUND_BYTES) {
            throw invalid("decimal bound exceeds its byte limit");
        }
        return token;
    }

    private static long estimatedPlainLength(BigDecimal value) {
        long sign = value.signum() < 0 ? 1L : 0L;
        long precision = value.precision();
        long scale = value.scale();
        if (scale <= 0L) return sign + precision - scale;
        if (precision > scale) return sign + precision + 1L;
        return sign + scale + 2L;
    }

    private static int canonicalStringBound(String token) {
        int value = canonicalInteger(token);
        if (value < 0 || value > MAX_STRING_UTF8_BYTES) {
            throw invalid("string UTF-8 bound is out of bounds");
        }
        return value;
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("QUALITY_VALIDATION_INVALID: " + detail);
    }

    public static final class Builder {
        private final DataSchema inputDataSchema;
        private final String inputSchemaFingerprint;
        private final List<String> ruleIds = new ArrayList<>();
        private final List<String> ruleKinds = new ArrayList<>();
        private final List<String> fieldIndices = new ArrayList<>();
        private final List<String> lowerBounds = new ArrayList<>();
        private final List<String> upperBounds = new ArrayList<>();
        private final Set<String> uniqueRuleIds = new HashSet<>();

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
                if (field.getName().getBytes(StandardCharsets.UTF_8).length > MAX_SCHEMA_TEXT_BYTES
                        || field.getAlias().getBytes(StandardCharsets.UTF_8).length
                        > MAX_SCHEMA_TEXT_BYTES) {
                    throw invalid("input field text exceeds its byte limit at index " + i);
                }
            }
            this.inputDataSchema = inputDataSchema;
            try {
                this.inputSchemaFingerprint = SchemaFingerprint.of(inputDataSchema);
            } catch (RuntimeException failure) {
                throw invalid("input schema cannot be fingerprinted");
            }
        }

        public Builder notNull(String ruleId, String fieldName) {
            return notNull(ruleId, resolveUniqueFieldName(fieldName));
        }

        public Builder notNull(String ruleId, int fieldIndex) {
            return add(ruleId, Kind.NOT_NULL, fieldIndex, "", "");
        }

        public Builder integerRange(String ruleId, String fieldName, int min, int max) {
            return integerRange(ruleId, resolveUniqueFieldName(fieldName), min, max);
        }

        public Builder integerRange(String ruleId, int fieldIndex, int min, int max) {
            if (min > max) throw invalid("integer range is reversed");
            requireType(fieldIndex, DataType.IntegerType);
            return add(ruleId, Kind.INTEGER_RANGE_CLOSED, fieldIndex,
                    Integer.toString(min), Integer.toString(max));
        }

        public Builder longRange(String ruleId, String fieldName, long min, long max) {
            return longRange(ruleId, resolveUniqueFieldName(fieldName), min, max);
        }

        public Builder longRange(String ruleId, int fieldIndex, long min, long max) {
            if (min > max) throw invalid("long range is reversed");
            requireType(fieldIndex, DataType.LongType);
            return add(ruleId, Kind.LONG_RANGE_CLOSED, fieldIndex,
                    Long.toString(min), Long.toString(max));
        }

        public Builder decimalRange(
                String ruleId, String fieldName, BigDecimal min, BigDecimal max) {
            return decimalRange(ruleId, resolveUniqueFieldName(fieldName), min, max);
        }

        public Builder decimalRange(
                String ruleId, int fieldIndex, BigDecimal min, BigDecimal max) {
            requireType(fieldIndex, DataType.BigDecimalType);
            String lower = canonicalDecimal(min);
            String upper = canonicalDecimal(max);
            if (min.compareTo(max) > 0) throw invalid("decimal range is reversed");
            return add(ruleId, Kind.DECIMAL_RANGE_CLOSED, fieldIndex, lower, upper);
        }

        public Builder stringUtf8BytesRange(
                String ruleId, String fieldName, int minBytes, int maxBytes) {
            return stringUtf8BytesRange(
                    ruleId, resolveUniqueFieldName(fieldName), minBytes, maxBytes);
        }

        public Builder stringUtf8BytesRange(
                String ruleId, int fieldIndex, int minBytes, int maxBytes) {
            if (minBytes < 0 || maxBytes < minBytes || maxBytes > MAX_STRING_UTF8_BYTES) {
                throw invalid("string UTF-8 range is out of bounds");
            }
            requireType(fieldIndex, DataType.StringType);
            return add(ruleId, Kind.STRING_UTF8_BYTES_RANGE_CLOSED, fieldIndex,
                    Integer.toString(minBytes), Integer.toString(maxBytes));
        }

        public RowValidation build() {
            if (ruleIds.isEmpty() || ruleIds.size() > MAX_RULES) {
                throw invalid("rule count is out of bounds");
            }
            return new RowValidation(inputDataSchema, inputSchemaFingerprint,
                    ruleIds, ruleKinds, fieldIndices, lowerBounds, upperBounds);
        }

        private Builder add(String ruleId, Kind kind, int fieldIndex,
                            String lowerBound, String upperBound) {
            if (ruleIds.size() >= MAX_RULES) {
                throw invalid("rule count exceeds " + MAX_RULES);
            }
            requireRuleId(ruleId);
            Field field = field(fieldIndex);
            if (field.getDataType() == DataType.NullType) {
                throw invalid("NullType fields are not supported");
            }
            ruleIds.add(ruleId);
            ruleKinds.add(kind.name());
            fieldIndices.add(Integer.toString(fieldIndex));
            lowerBounds.add(lowerBound);
            upperBounds.add(upperBound);
            return this;
        }

        private void requireRuleId(String ruleId) {
            if (ruleId == null || !ruleId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")
                    || ruleId.getBytes(StandardCharsets.UTF_8).length > MAX_RULE_ID_BYTES) {
                throw invalid("rule ID is invalid");
            }
            if (!uniqueRuleIds.add(ruleId)) {
                throw invalid("rule ID is duplicated");
            }
        }

        private void requireType(int fieldIndex, DataType expected) {
            DataType actual = field(fieldIndex).getDataType();
            if (actual != expected) {
                throw invalid("rule kind is incompatible with field type");
            }
        }

        private Field field(int fieldIndex) {
            if (fieldIndex < 0 || fieldIndex >= inputDataSchema.getFieldCount()) {
                throw invalid("field index is out of bounds");
            }
            return inputDataSchema.getField(fieldIndex);
        }

        private int resolveUniqueFieldName(String fieldName) {
            if (fieldName == null || fieldName.isEmpty()) {
                throw invalid("field name is required");
            }
            int found = -1;
            for (int i = 0; i < inputDataSchema.getFieldCount(); i++) {
                if (fieldName.equals(inputDataSchema.getField(i).getName())) {
                    if (found >= 0) throw invalid("field name is ambiguous");
                    found = i;
                }
            }
            if (found < 0) throw invalid("field name does not exist");
            return found;
        }
    }
}
