package io.rowweave;

import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded, canonical and metadata-only quality decision. */
class QualityGateDecisionCodecTest {

    private static final QualityReasonKey REQUIRED_FIELD = QualityReasonKey.of(
            "QUALITY_RULE_VIOLATION", "adaptor.required", 0, "NOT_NULL");
    private static final QualityReasonKey PERIOD_RANGE = QualityReasonKey.of(
            "QUALITY_RULE_VIOLATION", "period.range", 1, "INTEGER_RANGE_CLOSED");

    @Test
    void decisionBindsIdentityPolicyTotalsMembersObservationsAndCanonicalFailureSet() {
        QualityGatePolicy policy = QualityGatePolicy.builder()
                .maxRejectedRows(2L)
                .maxRejectedPartsPerMillion(200_000)
                .reasonThreshold(PERIOD_RANGE, 4L)
                .reasonThreshold(REQUIRED_FIELD, 1L)
                .exemplarLimit(2)
                .build();
        Map<QualityReasonKey, Long> observations = new LinkedHashMap<>();
        observations.put(PERIOD_RANGE, 5L);
        observations.put(REQUIRED_FIELD, 2L);
        QualityPreparedEvidence evidence = evidence(
                7L, 3L, observations, twoExemplars(), members());

        QualityGateDecision decision = QualityGateDecision.evaluate(
                "artifact-quality", fence(), "publication-quality", policy, evidence);

        assertEquals("artifact-quality", decision.artifactId());
        assertEquals(fence(), decision.fence());
        assertEquals("publication-quality", decision.publicationId());
        assertArrayEquals(QualityGatePolicyCodec.digest(policy), decision.policyDigest());
        assertEquals(7L, decision.acceptedTotal());
        assertEquals(3L, decision.rejectedTotal());
        assertEquals(members(), decision.members());
        assertEquals(Arrays.asList(
                new QualityGateDecision.ReasonObservation(REQUIRED_FIELD, 1L, 2L),
                new QualityGateDecision.ReasonObservation(PERIOD_RANGE, 4L, 5L)),
                decision.reasonObservations());
        assertEquals(QualityGateDecision.Outcome.FAIL, decision.outcome());
        assertEquals(Arrays.asList(
                QualityGateDecision.FailureCode.REASON_THRESHOLD_EXCEEDED,
                QualityGateDecision.FailureCode.REJECTED_RATIO_EXCEEDED,
                QualityGateDecision.FailureCode.REJECTED_ROWS_EXCEEDED),
                decision.failureCodes());
        assertEquals(twoExemplars(), decision.exemplars());
    }

    @Test
    void passAndFailDecisionsRoundTripToCanonicalBytesAndStableDigest() {
        QualityGatePolicy passPolicy = QualityGatePolicy.builder()
                .maxRejectedRows(2L)
                .reasonThreshold(REQUIRED_FIELD, 1L)
                .exemplarLimit(1)
                .build();
        Map<QualityReasonKey, Long> observed = new LinkedHashMap<>();
        observed.put(REQUIRED_FIELD, 1L);
        QualityGateDecision pass = QualityGateDecision.evaluate(
                "artifact-quality", fence(), "publication-quality", passPolicy,
                evidence(8L, 2L, observed,
                        Collections.singletonList(twoExemplars().get(0)), members()));

        byte[] encoded = QualityGateDecisionCodec.encode(pass);
        QualityGateDecision decoded = QualityGateDecisionCodec.decode(encoded);
        assertEquals(QualityGateDecision.Outcome.PASS, pass.outcome());
        assertTrue(pass.failureCodes().isEmpty());
        assertEquals(pass, decoded);
        assertArrayEquals(encoded, QualityGateDecisionCodec.encode(decoded));
        assertArrayEquals(QualityGateDecisionCodec.digest(pass),
                QualityGateDecisionCodec.digest(decoded));

        QualityGateDecision fail = QualityGateDecision.evaluate(
                "artifact-quality", fence(), "publication-quality",
                QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                evidence(8L, 2L, Collections.emptyMap(),
                        Collections.emptyList(), members()));
        assertEquals(fail, QualityGateDecisionCodec.decode(
                QualityGateDecisionCodec.encode(fail)));
        assertFalse(Arrays.equals(QualityGateDecisionCodec.digest(pass),
                QualityGateDecisionCodec.digest(fail)));
    }

    @Test
    void codecRejectsCorruptTruncatedUnknownVersionTrailingAndOversizedFrames() {
        QualityGateDecision decision = QualityGateDecision.evaluate(
                "artifact-quality", fence(), "publication-quality",
                QualityGatePolicy.allowAll(),
                evidence(1L, 0L, Collections.emptyMap(),
                        Collections.emptyList(), members()));
        byte[] encoded = QualityGateDecisionCodec.encode(decision);

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> QualityGateDecisionCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGateDecisionCodec.decode(
                        Arrays.copyOf(encoded, encoded.length - 1)));

        byte[] unknownVersion = encoded.clone();
        unknownVersion[5]++;
        resign(unknownVersion);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGateDecisionCodec.decode(unknownVersion));

        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        System.arraycopy(encoded, encoded.length - 32,
                trailing, encoded.length - 31, 32);
        trailing[9]++;
        resign(trailing);
        assertThrows(IllegalArgumentException.class,
                () -> QualityGateDecisionCodec.decode(trailing));
        assertThrows(IllegalArgumentException.class,
                () -> QualityGateDecisionCodec.decode(new byte[
                        QualityGateDecisionCodec.MAX_FRAME_BYTES + 1]));
    }

    @Test
    void exactWorstCaseRemainsBelowDecisionAndExemplarHardLimits() {
        QualityGatePolicy.Builder policy = QualityGatePolicy.builder()
                .maxRejectedRows(Long.MAX_VALUE)
                .maxRejectedPartsPerMillion(1_000_000)
                .exemplarLimit(QualityGatePolicy.MAX_EXEMPLARS);
        Map<QualityReasonKey, Long> observed = new LinkedHashMap<>();
        for (int i = 0; i < QualityGatePolicy.MAX_REASON_THRESHOLDS; i++) {
            QualityReasonKey reason = QualityReasonKey.of(
                    "QUALITY_RULE_VIOLATION", String.format("r%063d", i),
                    i, "STRING_UTF8_BYTES_RANGE_CLOSED");
            policy.reasonThreshold(reason, Long.MAX_VALUE);
            observed.put(reason, Long.MAX_VALUE);
        }
        List<QualityExemplar> exemplars = new ArrayList<>();
        String maximumUid = repeat('u', 256);
        String maximumCode = repeat('E', 64);
        for (int i = 0; i < QualityGatePolicy.MAX_EXEMPLARS; i++) {
            exemplars.add(QualityExemplar.of(maximumUid,
                    String.format("%064x", i), i, maximumCode));
        }
        QualityGateDecision maximum = QualityGateDecision.evaluate(
                repeat('a', 256),
                new RunAttemptFence(repeat('r', 256), repeat('t', 256), Long.MAX_VALUE),
                repeat('p', 256), policy.build(),
                evidence(Long.MAX_VALUE, Long.MAX_VALUE, observed, exemplars,
                        maximumMembers()));

        byte[] encoded = QualityGateDecisionCodec.encode(maximum);
        assertTrue(encoded.length <= QualityGateDecisionCodec.MAX_FRAME_BYTES);
        assertTrue(QualityGateDecisionCodec.encodedExemplarSectionBytes(maximum)
                <= QualityGateDecisionCodec.MAX_EXEMPLAR_SECTION_BYTES);
        assertEquals(maximum, QualityGateDecisionCodec.decode(encoded));
    }

    @Test
    void decisionIsNonSerializableAndItsWireContainsNoPathPayloadOrSecret() {
        assertFalse(Serializable.class.isAssignableFrom(QualityGateDecision.class));
        QualityGateDecision decision = QualityGateDecision.evaluate(
                "artifact-quality", fence(), "publication-quality",
                QualityGatePolicy.allowAll(),
                evidence(1L, 0L, Collections.emptyMap(),
                        Collections.emptyList(), members()));
        String wire = new String(QualityGateDecisionCodec.encode(decision),
                StandardCharsets.ISO_8859_1);
        for (String forbidden : Arrays.asList(
                "/domain/ledger", "data/71/valid.data", "raw-payload",
                "credential-secret", "worker.example:7001", "java.lang.Exception")) {
            assertFalse(wire.contains(forbidden), forbidden);
        }
    }

    private static RunAttemptFence fence() {
        return new RunAttemptFence("run-quality", "attempt-1", 7L);
    }

    private static QualityPreparedEvidence evidence(
            long accepted, long rejected, Map<QualityReasonKey, Long> observations,
            List<QualityExemplar> exemplars,
            List<QualityPreparedMemberEvidence> members) {
        return new QualityPreparedEvidence(
                accepted, rejected, observations, exemplars, members);
    }

    private static List<QualityPreparedMemberEvidence> members() {
        return Arrays.asList(
                member(TypedQualityManifest.VALID_SINK_UID, "valid", 11L, 1),
                member(TypedQualityManifest.REJECTED_SINK_UID, "rejected", 22L, 2),
                member(TypedQualityManifest.SUMMARY_SINK_UID, "summary", 33L, 3));
    }

    private static List<QualityPreparedMemberEvidence> maximumMembers() {
        return Arrays.asList(
                member(TypedQualityManifest.VALID_SINK_UID, repeat('v', 256),
                        Long.MAX_VALUE, 1),
                member(TypedQualityManifest.REJECTED_SINK_UID, repeat('r', 256),
                        Long.MAX_VALUE, 2),
                member(TypedQualityManifest.SUMMARY_SINK_UID, repeat('s', 256),
                        Long.MAX_VALUE, 3));
    }

    private static QualityPreparedMemberEvidence member(
            String role, String name, long length, int seed) {
        return new QualityPreparedMemberEvidence(
                role, name, length, digest(seed), digest(seed + 10));
    }

    private static List<QualityExemplar> twoExemplars() {
        return Arrays.asList(
                QualityExemplar.of("quality.source", repeat('0', 64), 1L,
                        "QUALITY_RULE_VIOLATION"),
                QualityExemplar.of("quality.source", repeat('1', 64), 2L,
                        "QUALITY_VALUE_TYPE_MISMATCH"));
    }

    private static byte[] digest(int value) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
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
}
