package io.rowweave;

import io.rowweave.internal.RowSnapshot;
import io.rowweave.sched.stream.SourceRecordPosition;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** strict, payload-free classification of one already framed record. */
final class ObjectStorageQualityRecordDecoder {

    private static final int VALID_TAG = 0;
    private static final int REJECTED_TAG = 1;
    private static final String RULE_ID = "OBJECT_ROW_DECODE_V1";

    private final DataSchema inputMeta;
    private final int inputFieldCount;
    private final SourceRecordPosition.SplitFactory positions;

    ObjectStorageQualityRecordDecoder(ObjectStorageQualitySource input) {
        if (input == null) throw runtimeInvalid("quality input is required");
        this.inputMeta = input.originalSource().getSchema();
        this.inputFieldCount = inputMeta.getFieldCount();
        this.positions = SourceRecordPosition.forSplit(
                input.sourceOperatorUid(), input.originalSource().batchSourceSplit());
    }

    DataRow classify(byte[] record, int length, long ordinal, boolean rowLimitExceeded) {
        if (record == null || length < 0 || length > record.length) {
            throw runtimeInvalid("record bounds are invalid");
        }
        SourceRecordPosition position = positions.at(ordinal);
        if (rowLimitExceeded) {
            return rejected(position,
                    "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES");
        }
        for (int i = 0; i < length; i++) {
            if (record[i] == '\r') {
                return rejected(position, "QUALITY_RAW_CR_INVALID", "CR");
            }
        }
        final String text;
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(record, 0, length));
            text = decoded.toString();
        } catch (CharacterCodingException invalidUtf8) {
            return rejected(position, "QUALITY_RAW_UTF8_INVALID", "UTF8");
        }
        final DataRow decoded;
        try {
            decoded = FileSink.decodeRow(inputMeta, text);
            if (!text.equals(FileSink.encodeRowText(inputMeta, decoded))) {
                return rejected(position,
                        "QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1");
            }
        } catch (RuntimeException invalidFormat) {
            return rejected(position,
                    "QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1");
        }
        return valid(decoded);
    }

    private DataRow valid(DataRow decoded) {
        DataRow carrier = new DataRow(inputFieldCount + 8);
        for (int i = 0; i < inputFieldCount; i++) {
            carrier.set(i, RowSnapshot.copyValue(decoded.get(i)));
        }
        carrier.set(inputFieldCount, VALID_TAG);
        carrier.set(inputFieldCount + 1, "");
        carrier.set(inputFieldCount + 2, "");
        carrier.set(inputFieldCount + 3, 0L);
        carrier.set(inputFieldCount + 4, "");
        carrier.set(inputFieldCount + 5, "");
        carrier.set(inputFieldCount + 6, -1);
        carrier.set(inputFieldCount + 7, "");
        return carrier;
    }

    private DataRow rejected(
            SourceRecordPosition position, String errorCode, String detail) {
        DataRow carrier = new DataRow(inputFieldCount + 8);
        carrier.set(inputFieldCount, REJECTED_TAG);
        carrier.set(inputFieldCount + 1, position.sourceOperatorUid());
        carrier.set(inputFieldCount + 2, position.splitIdentitySha256());
        carrier.set(inputFieldCount + 3, position.splitLocalOrdinal());
        carrier.set(inputFieldCount + 4, errorCode);
        carrier.set(inputFieldCount + 5, RULE_ID);
        carrier.set(inputFieldCount + 6, -1);
        carrier.set(inputFieldCount + 7, detail);
        return carrier;
    }

    private static IllegalStateException runtimeInvalid(String detail) {
        return new IllegalStateException(
                "OBJECT_QUALITY_RUNTIME_INVALID: " + detail);
    }
}
