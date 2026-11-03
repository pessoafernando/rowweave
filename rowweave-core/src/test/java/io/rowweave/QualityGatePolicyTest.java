package io.rowweave;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical bounded policy and exact threshold evaluation. */
class QualityGatePolicyTest {

    private static final QualityReasonKey NULL_FIELD = QualityReasonKey.of(
            "QUALITY_RULE_VIOLATION", "adaptor.required", 0, "NOT_NULL");
    private static final QualityReasonKey BAD_PERIOD = QualityReasonKey.of(
            "QUALITY_RULE_VIOLATION", "period.range", 1, "INTEGER_RANGE_CLOSED");

    @Test
    void publicModelsCannotBypassInvariantsThroughJavaSerialization() {
        assertFalse(Serializable.class.isAssignableFrom(QualityGatePolicy.class));
        assertFalse(Serializable.class.isAssignableFrom(QualityReasonKey.class));
    }

    @Test
    void canonicalCodecDigestAndOrderingIgnoreBuilderInsertionOrder() {
        QualityGatePolicy first = QualityGatePolicy.builder()
                .maxRejectedRows(8)
                .maxRejectedPartsPerMillion(250_000)
                .reasonThreshold(BAD_PERIOD, 4)
                .reasonThreshold(NULL_FIELD, 2)
                .exemplarLimit(16)
                .build();
        QualityGatePolicy second = QualityGatePolicy.builder()
                .reasonThreshold(NULL_FIELD, 2)
                .exemplarLimit(16)
                .maxRejectedPartsPerMillion(250_000)
                .reasonThreshold(BAD_PERIOD, 4)
                .maxRejectedRows(8)
                .build();

        byte[] encoded = QualityGatePolicyCodec.encode(first);
        assertEquals(first, second);
        assertArrayEquals(encoded, QualityGatePolicyCodec.encode(second));
        assertArrayEquals(QualityGatePolicyCodec.digest(first),
                QualityGatePolicyCodec.digest(second));
        assertEquals(first, QualityGatePolicyCodec.decode(encoded));

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(
                        Arrays.copyOf(encoded, encoded.length - 1)));
    }

    @Test
    void codecRejectsUnknownVersionCountsLengthsTrailingAndInvalidUtf8() {
        byte[] allowAll = QualityGatePolicyCodec.encode(QualityGatePolicy.allowAll());
        byte[] unknownVersion = allowAll.clone();
        unknownVersion[5] = 2;
        resign(unknownVersion);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(unknownVersion));

        byte[] oversizedCount = allowAll.clone();
        oversizedCount[11] = 1;
        oversizedCount[12] = 1;
        resign(oversizedCount);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(oversizedCount));

        byte[] badLength = allowAll.clone();
        badLength[9]++;
        resign(badLength);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(badLength));

        byte[] trailing = Arrays.copyOf(allowAll, allowAll.length + 1);
        System.arraycopy(allowAll, allowAll.length - 32,
                trailing, allowAll.length - 31, 32);
        trailing[9]++;
        resign(trailing);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(trailing));

        QualityGatePolicy withRule = QualityGatePolicy.builder()
                .reasonThreshold(NULL_FIELD, 1).build();
        byte[] invalidUtf8 = QualityGatePolicyCodec.encode(withRule);
        byte[] marker = "QUALITY_RULE_VIOLATION".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int offset = indexOf(invalidUtf8, marker);
        assertTrue(offset >= 0);
        invalidUtf8[offset] = (byte) 0xc3;
        resign(invalidUtf8);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicyCodec.decode(invalidUtf8));
    }

    @Test
    void validatesClosedKeysThresholdBoundsDuplicatesAndWorstCaseFrame() {
        assertThrows(IllegalArgumentException.class,
                () -> QualityReasonKey.of("bad", "", -1, "FORMAT"));
        assertThrows(IllegalArgumentException.class,
                () -> QualityReasonKey.of("QUALITY_RULE_VIOLATION", "bad rule", 0,
                        "NOT_NULL"));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicy.builder().maxRejectedRows(-1));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicy.builder().maxRejectedPartsPerMillion(1_000_001));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicy.builder().exemplarLimit(65));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGatePolicy.builder()
                        .reasonThreshold(NULL_FIELD, 1)
                        .reasonThreshold(NULL_FIELD, 2));

        QualityGatePolicy.Builder maximum = QualityGatePolicy.builder()
                .maxRejectedRows(Long.MAX_VALUE)
                .maxRejectedPartsPerMillion(1_000_000)
                .exemplarLimit(64);
        for (int i = 0; i < 256; i++) {
            maximum.reasonThreshold(QualityReasonKey.of(
                    "QUALITY_RULE_VIOLATION", String.format("r%063d", i),
                    i, "INTEGER_RANGE_CLOSED"), Long.MAX_VALUE);
        }
        byte[] encoded = QualityGatePolicyCodec.encode(maximum.build());
        assertTrue(encoded.length <= QualityGatePolicyCodec.MAX_FRAME_BYTES);
        assertEquals(maximum.build(), QualityGatePolicyCodec.decode(encoded));
        assertThrows(IllegalArgumentException.class,
                () -> maximum.reasonThreshold(QualityReasonKey.of(
                        "QUALITY_RULE_VIOLATION", "overflow", 256,
                        "INTEGER_RANGE_CLOSED"), 1));
    }

    @Test
    void acceptsExactMixedCaseTypeDiagnosticProducedByClassifier() {
        for (String detail : Arrays.asList(
                "ROW_WIDTH",
                "BooleanType", "StringType", "IntegerType", "LongType",
                "BigDecimalType", "DateType", "TimestampType",
                "NOT_NULL", "INTEGER_RANGE_CLOSED", "LONG_RANGE_CLOSED",
                "DECIMAL_RANGE_CLOSED", "STRING_UTF8_BYTES_RANGE_CLOSED",
                "UTF8", "CR", "ROW_BYTES", "ROWWEAVE_ROW_TEXT_V1")) {
            QualityReasonKey reason = QualityReasonKey.of(
                    "QUALITY_VALUE_TYPE_MISMATCH", "", 1, detail);
            QualityGatePolicy policy = QualityGatePolicy.builder()
                    .reasonThreshold(reason, 0L).build();
            assertEquals(policy, QualityGatePolicyCodec.decode(
                    QualityGatePolicyCodec.encode(policy)));
        }

        assertThrows(IllegalArgumentException.class, () -> QualityReasonKey.of(
                "QUALITY_VALUE_TYPE_MISMATCH", "", 1, "BigdecimalType"));
        assertThrows(IllegalArgumentException.class, () -> QualityReasonKey.of(
                "QUALITY_VALUE_TYPE_MISMATCH", "", 1, "MixedCase"));
        assertThrows(IllegalArgumentException.class, () -> QualityReasonKey.of(
                "QUALITY_VALUE_TYPE_MISMATCH", "", 1, "NullType"));
    }

    @Test
    void evaluatesInclusiveRowsRatioAndConfiguredReasonsWithoutOverflow() {
        QualityGatePolicy policy = QualityGatePolicy.builder()
                .maxRejectedRows(2)
                .maxRejectedPartsPerMillion(200_000)
                .reasonThreshold(NULL_FIELD, 1)
                .exemplarLimit(3)
                .build();
        Map<QualityReasonKey, Long> atBoundary = new LinkedHashMap<>();
        atBoundary.put(NULL_FIELD, 1L);
        atBoundary.put(BAD_PERIOD, 1L);

        QualityGateEvaluation pass = QualityGateEvaluator.evaluate(
                policy, 8L, 2L, atBoundary);
        assertTrue(pass.passed());
        assertTrue(pass.failureCodes().isEmpty());

        Map<QualityReasonKey, Long> tooManyForRule = new LinkedHashMap<>();
        tooManyForRule.put(NULL_FIELD, 2L);
        QualityGateEvaluation fail = QualityGateEvaluator.evaluate(
                policy, 7L, 3L, tooManyForRule);
        assertFalse(fail.passed());
        assertEquals(Arrays.asList(
                "REASON_THRESHOLD_EXCEEDED",
                "REJECTED_RATIO_EXCEEDED",
                "REJECTED_ROWS_EXCEEDED"), fail.failureCodes());

        QualityGatePolicy zeroRatio = QualityGatePolicy.builder()
                .maxRejectedPartsPerMillion(0).build();
        assertTrue(QualityGateEvaluator.evaluate(
                zeroRatio, Long.MAX_VALUE, 0L, new LinkedHashMap<>()).passed());
        assertFalse(QualityGateEvaluator.evaluate(
                zeroRatio, Long.MAX_VALUE - 1, 1L,
                new LinkedHashMap<>()).passed());
        assertTrue(QualityGateEvaluator.evaluate(
                zeroRatio, 0L, 0L, new LinkedHashMap<>()).passed());
        assertTrue(QualityGateEvaluator.evaluate(
                QualityGatePolicy.allowAll(), 0L, Long.MAX_VALUE,
                new LinkedHashMap<>()).passed());
    }

    private static void resign(byte[] frame) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(Arrays.copyOf(frame, frame.length - 32));
            System.arraycopy(digest, 0, frame, frame.length - 32, 32);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    }
}
