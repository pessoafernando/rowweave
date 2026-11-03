package io.rowweave.ops;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.RowSnapshot;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.SourceRecordPosition;
import io.rowweave.sched.stream.StreamEvent;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * internal built-in: one positioned typed-row evaluation followed by two canonical routes.
 * The class is public only so the root API and plan compiler can share the same implementation.
 */
public final class TypedRowClassification {

    public static final String CARRIER_VERSION = "1";
    public static final String ROUTE_CONTRACT = "BOTH_V1";
    public static final String VALID_ROUTE = "VALID";
    public static final String REJECTED_ROUTE = "REJECTED";

    private static final int VALID_TAG = 0;
    private static final int REJECTED_TAG = 1;
    private static final String WIDTH = "QUALITY_ROW_WIDTH_MISMATCH";
    private static final String TYPE = "QUALITY_VALUE_TYPE_MISMATCH";
    private static final String RULE = "QUALITY_RULE_VIOLATION";

    private final DataSchema inputDataSchema;
    private final DataSchema carrierSchema;
    private final DataSchema rejectedSchema;
    private final RowValidation validation;
    private final String sourceOperatorUid;
    private final List<BatchSourceSplitDescriptor> exactSourceSplits;

    private TypedRowClassification(
            DataSchema inputDataSchema, RowValidation validation,
            String sourceOperatorUid, List<BatchSourceSplitDescriptor> exactSourceSplits) {
        if (inputDataSchema == null || validation == null) {
            throw invalidConfiguration("input schema and validation are required");
        }
        if (!SchemaFingerprint.of(inputDataSchema).equals(validation.inputSchemaFingerprint())) {
            throw invalidConfiguration("validation input schema differs from classifier input");
        }
        if (inputDataSchema.getFieldCount() + 8 > RowValidation.MAX_FIELDS) {
            throw invalidConfiguration("carrier field count exceeds the bounded field limit");
        }
        // Constructor validates UID, exact descriptors, duplicates and replay contract without opening input.
        new RowValidation.SourcePositionSequence(sourceOperatorUid, exactSourceSplits);
        this.inputDataSchema = inputDataSchema;
        this.validation = validation;
        this.sourceOperatorUid = sourceOperatorUid;
        this.exactSourceSplits = Collections.unmodifiableList(
                new ArrayList<>(exactSourceSplits));
        this.rejectedSchema = rejectedMeta();
        this.carrierSchema = carrierMeta(inputDataSchema, rejectedSchema);
    }

    public static TypedRowClassification of(
            DataSchema inputDataSchema, RowValidation validation,
            String sourceOperatorUid, List<BatchSourceSplitDescriptor> exactSourceSplits) {
        try {
            return new TypedRowClassification(
                    inputDataSchema, validation, sourceOperatorUid, exactSourceSplits);
        } catch (RuntimeException failure) {
            if (failure.getMessage() != null
                    && failure.getMessage().startsWith("QUALITY_CLASSIFICATION_INVALID:")) {
                throw failure;
            }
            throw invalidConfiguration("source position contract is invalid");
        }
    }

    public DataSchema carrierSchema() {
        return carrierSchema;
    }

    public DataSchema rejectedSchema() {
        return rejectedSchema;
    }

    public PipelineExecutor.Stage classifierStage() {
        return this::classify;
    }

    public PipelineExecutor.Stage validRouteStage() {
        return routeStage(carrierSchema, inputDataSchema, VALID_ROUTE);
    }

    public PipelineExecutor.Stage rejectedRouteStage() {
        return routeStage(carrierSchema, rejectedSchema, REJECTED_ROUTE);
    }

    /** Rebuild a fragment-local route from its input/output schemas and closed route token. */
    public static PipelineExecutor.Stage routeStage(
            DataSchema carrierMeta, DataSchema outputMeta, String route) {
        RouteProjection projection = RouteProjection.of(carrierMeta, outputMeta, route);
        return projection::route;
    }

    private void classify(RowStream in, RowSink out) {
        RowValidation.SourcePositionSequence sequence =
                new RowValidation.SourcePositionSequence(
                        sourceOperatorUid, exactSourceSplits);
        StreamEvent event;
        while ((event = in.next()) != null) {
            if (event.isRecord()) {
                SourceRecordPosition position = sequence.verify(event);
                RowValidation.Evaluation result =
                        validation.evaluateForClassification(event.row());
                out.emitRow(carrier(event.row(), position, result));
                continue;
            }
            out.emit(event);
            if (terminal(event)) return;
        }
        out.endOfStream();
    }

    private DataRow carrier(
            DataRow input, SourceRecordPosition position,
            RowValidation.Evaluation evaluation) {
        int offset = inputDataSchema.getFieldCount();
        DataRow carrier = new DataRow(offset + 8);
        if (evaluation.isValid()) {
            for (int i = 0; i < offset; i++) {
                carrier.set(i, RowSnapshot.copyValue(input.get(i)));
            }
            carrier.set(offset, VALID_TAG);
            carrier.set(offset + 1, "");
            carrier.set(offset + 2, "");
            carrier.set(offset + 3, 0L);
            carrier.set(offset + 4, "");
            carrier.set(offset + 5, "");
            carrier.set(offset + 6, -1);
            carrier.set(offset + 7, "");
        } else {
            carrier.set(offset, REJECTED_TAG);
            carrier.set(offset + 1, position.sourceOperatorUid());
            carrier.set(offset + 2, position.splitIdentitySha256());
            carrier.set(offset + 3, position.splitLocalOrdinal());
            carrier.set(offset + 4, evaluation.errorCode());
            carrier.set(offset + 5, evaluation.ruleId());
            carrier.set(offset + 6, evaluation.fieldIndex());
            carrier.set(offset + 7, evaluation.detail());
        }
        return carrier;
    }

    private static final class RouteProjection {
        private final DataSchema inputDataSchema;
        private final int acceptedTag;

        private RouteProjection(DataSchema inputDataSchema, int acceptedTag) {
            this.inputDataSchema = inputDataSchema;
            this.acceptedTag = acceptedTag;
        }

        private static RouteProjection of(
                DataSchema carrierMeta, DataSchema outputMeta, String route) {
            if (carrierMeta == null || outputMeta == null
                    || carrierMeta.getFieldCount() < 8) {
                throw invalidConfiguration("route schema is invalid");
            }
            int inputCount = carrierMeta.getFieldCount() - 8;
            DataSchema inputMeta = prefixMeta(carrierMeta, inputCount);
            DataSchema expectedCarrier = carrierMeta(inputMeta, rejectedMeta());
            if (!sameMeta(expectedCarrier, carrierMeta)) {
                throw invalidConfiguration("carrier schema is noncanonical");
            }
            int tag;
            DataSchema expectedOutput;
            if (VALID_ROUTE.equals(route)) {
                tag = VALID_TAG;
                expectedOutput = inputMeta;
            } else if (REJECTED_ROUTE.equals(route)) {
                tag = REJECTED_TAG;
                expectedOutput = rejectedMeta();
            } else {
                throw invalidConfiguration("route is unknown");
            }
            if (!sameMeta(expectedOutput, outputMeta)) {
                throw invalidConfiguration("route output schema is invalid");
            }
            return new RouteProjection(inputMeta, tag);
        }

        private void route(RowStream in, RowSink out) {
            StreamEvent event;
            while ((event = in.next()) != null) {
                if (event.isRecord()) {
                    DataRow carrier = event.row();
                    int tag = verifyCarrier(carrier);
                    if (tag == acceptedTag) {
                        out.emitRow(tag == VALID_TAG
                                ? projectValid(carrier) : projectRejected(carrier));
                    }
                    continue;
                }
                out.emit(event);
                if (terminal(event)) return;
            }
            out.endOfStream();
        }

        private int verifyCarrier(DataRow carrier) {
            int offset = inputDataSchema.getFieldCount();
            if (carrier == null || carrier.size() != offset + 8) {
                throw invalidRuntime("carrier width is invalid");
            }
            Object tagValue = carrier.get(offset);
            if (tagValue == null || tagValue.getClass() != Integer.class) {
                throw invalidRuntime("route tag type is invalid");
            }
            int tag = (Integer) tagValue;
            if (tag == VALID_TAG) {
                verifyValid(carrier, offset);
                return tag;
            }
            if (tag == REJECTED_TAG) {
                verifyRejected(carrier, offset);
                return tag;
            }
            throw invalidRuntime("route tag is unknown");
        }

        private void verifyValid(DataRow carrier, int offset) {
            for (int i = 0; i < offset; i++) {
                Object value = carrier.get(i);
                DataType type = inputDataSchema.getField(i).getDataType();
                if (value != null && !RowValidation.matchesRuntimeType(value, type)) {
                    throw invalidRuntime("valid field type is invalid");
                }
            }
            if (!"".equals(carrier.get(offset + 1))
                    || !"".equals(carrier.get(offset + 2))
                    || !Long.valueOf(0L).equals(carrier.get(offset + 3))
                    || !"".equals(carrier.get(offset + 4))
                    || !"".equals(carrier.get(offset + 5))
                    || !Integer.valueOf(-1).equals(carrier.get(offset + 6))
                    || !"".equals(carrier.get(offset + 7))) {
                throw invalidRuntime("valid diagnostic sentinel is noncanonical");
            }
        }

        private void verifyRejected(DataRow carrier, int offset) {
            for (int i = 0; i < offset; i++) {
                if (carrier.get(i) != null) {
                    throw invalidRuntime("rejected carrier retains an input value");
                }
            }
            Object uidValue = carrier.get(offset + 1);
            Object digestValue = carrier.get(offset + 2);
            Object ordinalValue = carrier.get(offset + 3);
            Object codeValue = carrier.get(offset + 4);
            Object ruleValue = carrier.get(offset + 5);
            Object indexValue = carrier.get(offset + 6);
            Object detailValue = carrier.get(offset + 7);
            if (!(uidValue instanceof String) || !canonicalUid((String) uidValue)
                    || !(digestValue instanceof String)
                    || !((String) digestValue).matches("[0-9a-f]{64}")
                    || ordinalValue == null || ordinalValue.getClass() != Long.class
                    || (Long) ordinalValue < 0L
                    || !(codeValue instanceof String) || !(ruleValue instanceof String)
                    || indexValue == null || indexValue.getClass() != Integer.class
                    || !(detailValue instanceof String)) {
                throw invalidRuntime("rejected diagnostic shape is invalid");
            }
            String code = (String) codeValue;
            String ruleId = (String) ruleValue;
            int fieldIndex = (Integer) indexValue;
            String detail = (String) detailValue;
            if (WIDTH.equals(code)) {
                if (!ruleId.isEmpty() || fieldIndex != -1 || !"ROW_WIDTH".equals(detail)) {
                    throw invalidRuntime("width diagnostic is noncanonical");
                }
                return;
            }
            if (TYPE.equals(code)) {
                if (!ruleId.isEmpty() || fieldIndex < 0
                        || fieldIndex >= inputDataSchema.getFieldCount()
                        || !inputDataSchema.getField(fieldIndex).getDataType().name().equals(detail)) {
                    throw invalidRuntime("type diagnostic is noncanonical");
                }
                return;
            }
            if (RULE.equals(code)) {
                if (!ruleId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")
                        || fieldIndex < 0 || fieldIndex >= inputDataSchema.getFieldCount()
                        || !isRuleKind(detail)) {
                    throw invalidRuntime("rule diagnostic is noncanonical");
                }
                return;
            }
            throw invalidRuntime("quality error code is unknown");
        }

        private DataRow projectValid(DataRow carrier) {
            int count = inputDataSchema.getFieldCount();
            DataRow result = new DataRow(count);
            for (int i = 0; i < count; i++) result.set(i, carrier.get(i));
            return result;
        }

        private DataRow projectRejected(DataRow carrier) {
            int offset = inputDataSchema.getFieldCount();
            DataRow result = new DataRow(7);
            for (int i = 0; i < 7; i++) result.set(i, carrier.get(offset + 1 + i));
            return result;
        }
    }

    private static boolean terminal(StreamEvent event) {
        return event.isEndOfStream() || event.isError() || event.isCancel();
    }

    private static boolean isRuleKind(String detail) {
        try {
            RowValidation.Kind.valueOf(detail);
            return true;
        } catch (IllegalArgumentException failure) {
            return false;
        }
    }

    private static boolean canonicalUid(String uid) {
        if (uid.isEmpty() || uid.indexOf('\0') >= 0
                || !Normalizer.isNormalized(uid, Normalizer.Form.NFC)) {
            return false;
        }
        try {
            return StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(uid)).remaining()
                    <= SourceRecordPosition.MAX_SOURCE_UID_BYTES;
        } catch (CharacterCodingException malformed) {
            return false;
        }
    }

    private static DataSchema prefixMeta(DataSchema carrier, int count) {
        Field[] fields = new Field[count];
        for (int i = 0; i < count; i++) fields[i] = carrier.getField(i);
        return new DataSchema(fields);
    }

    private static boolean sameMeta(DataSchema left, DataSchema right) {
        return SchemaFingerprint.of(left).equals(SchemaFingerprint.of(right));
    }

    private static DataSchema carrierMeta(DataSchema input, DataSchema rejected) {
        Field[] fields = new Field[input.getFieldCount() + 8];
        System.arraycopy(input.getFields(), 0, fields, 0, input.getFieldCount());
        int offset = input.getFieldCount();
        fields[offset] = new Field("__rowweave_quality_route_v1", DataType.IntegerType);
        System.arraycopy(rejected.getFields(), 0, fields, offset + 1, 7);
        return new DataSchema(fields);
    }

    private static DataSchema rejectedMeta() {
        return new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
    }

    private static IllegalArgumentException invalidConfiguration(String detail) {
        return new IllegalArgumentException("QUALITY_CLASSIFICATION_INVALID: " + detail);
    }

    private static IllegalStateException invalidRuntime(String detail) {
        return new IllegalStateException(
                "QUALITY_CLASSIFICATION_RUNTIME_INVALID: " + detail);
    }
}
