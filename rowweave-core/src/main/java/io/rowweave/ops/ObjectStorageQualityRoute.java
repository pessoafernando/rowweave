package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.SourceRecordPosition;
import io.rowweave.sched.stream.StreamEvent;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

/**
 * internal built-in: validates and projects the two Object quality routes.
 * Public visibility only lets the root API and the Plan compiler share one implementation.
 */
public final class ObjectStorageQualityRoute {

    public static final String CARRIER_VERSION = "1";
    public static final String VALID_ROUTE = "VALID";
    public static final String REJECTED_ROUTE = "REJECTED";

    private static final int VALID_TAG = 0;
    private static final int REJECTED_TAG = 1;
    private static final String RULE_ID = "OBJECT_ROW_DECODE_V1";

    private ObjectStorageQualityRoute() {
    }

    public static PipelineExecutor.Stage routeStage(
            DataSchema carrierMeta, DataSchema outputMeta, String route) {
        RouteProjection projection = RouteProjection.of(carrierMeta, outputMeta, route);
        return projection::route;
    }

    public static DataSchema rejectedMeta() {
        return new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
    }

    private static final class RouteProjection {
        private final DataSchema inputMeta;
        private final int acceptedTag;

        private RouteProjection(DataSchema inputMeta, int acceptedTag) {
            this.inputMeta = inputMeta;
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
            if (!sameMeta(carrierMeta(inputMeta), carrierMeta)) {
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

        private void route(io.rowweave.sched.stream.RowStream in,
                           io.rowweave.sched.stream.RowSink out) {
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
            int offset = inputMeta.getFieldCount();
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
                if (value != null
                        && !RowValidation.matchesRuntimeType(
                        value, inputMeta.getField(i).getDataType())) {
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
                    || !(codeValue instanceof String)
                    || !RULE_ID.equals(ruleValue)
                    || !Integer.valueOf(-1).equals(indexValue)
                    || !(detailValue instanceof String)) {
                throw invalidRuntime("rejected diagnostic shape is invalid");
            }
            String code = (String) codeValue;
            String detail = (String) detailValue;
            if (!("QUALITY_RAW_UTF8_INVALID".equals(code) && "UTF8".equals(detail))
                    && !("QUALITY_RAW_CR_INVALID".equals(code) && "CR".equals(detail))
                    && !("QUALITY_RAW_ROW_LIMIT_EXCEEDED".equals(code)
                    && "ROW_BYTES".equals(detail))
                    && !("QUALITY_RAW_FORMAT_INVALID".equals(code)
                    && "ROWWEAVE_ROW_TEXT_V1".equals(detail))) {
                throw invalidRuntime("rejected diagnostic tuple is noncanonical");
            }
        }

        private DataRow projectValid(DataRow carrier) {
            DataRow result = new DataRow(inputMeta.getFieldCount());
            for (int i = 0; i < result.size(); i++) result.set(i, carrier.get(i));
            return result;
        }

        private DataRow projectRejected(DataRow carrier) {
            int offset = inputMeta.getFieldCount();
            DataRow result = new DataRow(7);
            for (int i = 0; i < result.size(); i++) {
                result.set(i, carrier.get(offset + 1 + i));
            }
            return result;
        }
    }

    private static DataSchema carrierMeta(DataSchema inputMeta) {
        Field[] fields = new Field[inputMeta.getFieldCount() + 8];
        System.arraycopy(inputMeta.getFields(), 0, fields, 0, inputMeta.getFieldCount());
        int offset = inputMeta.getFieldCount();
        fields[offset] = new Field("quality_route_tag", DataType.IntegerType);
        System.arraycopy(rejectedMeta().getFields(), 0, fields, offset + 1, 7);
        return new DataSchema(fields);
    }

    private static DataSchema prefixMeta(DataSchema carrier, int count) {
        Field[] fields = new Field[count];
        for (int i = 0; i < count; i++) fields[i] = carrier.getField(i);
        return new DataSchema(fields);
    }

    private static boolean sameMeta(DataSchema left, DataSchema right) {
        return SchemaFingerprint.of(left).equals(SchemaFingerprint.of(right));
    }

    private static boolean canonicalUid(String uid) {
        if (uid.isEmpty() || uid.indexOf('\0') >= 0
                || !Normalizer.isNormalized(uid, Normalizer.Form.NFC)) return false;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(uid));
            return encoded.remaining() <= SourceRecordPosition.MAX_SOURCE_UID_BYTES;
        } catch (CharacterCodingException malformed) {
            return false;
        }
    }

    private static boolean terminal(StreamEvent event) {
        return event.isEndOfStream() || event.isError() || event.isCancel();
    }

    private static IllegalArgumentException invalidConfiguration(String detail) {
        return new IllegalArgumentException("OBJECT_QUALITY_INVALID: " + detail);
    }

    private static IllegalStateException invalidRuntime(String detail) {
        return new IllegalStateException("OBJECT_QUALITY_RUNTIME_INVALID: " + detail);
    }
}
