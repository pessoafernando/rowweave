package io.rowweave;

import io.rowweave.run.RunAttemptFence;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable metadata-only quality decision bound to one prepared publication. */
public final class QualityGateDecision {
    static final int MAX_ID_BYTES = 256;
    static final int DIGEST_BYTES = 32;

    public enum Outcome { PASS, FAIL }

    public enum FailureCode {
        REASON_THRESHOLD_EXCEEDED,
        REJECTED_RATIO_EXCEEDED,
        REJECTED_ROWS_EXCEEDED
    }

    private final String artifactId;
    private final RunAttemptFence fence;
    private final String publicationId;
    private final byte[] policyDigest;
    private final long acceptedTotal;
    private final long rejectedTotal;
    private final List<QualityPreparedMemberEvidence> members;
    private final List<ReasonObservation> reasonObservations;
    private final Outcome outcome;
    private final List<FailureCode> failureCodes;
    private final List<QualityExemplar> exemplars;

    QualityGateDecision(
            String artifactId, RunAttemptFence fence, String publicationId,
            byte[] policyDigest, long acceptedTotal, long rejectedTotal,
            List<QualityPreparedMemberEvidence> members,
            List<ReasonObservation> reasonObservations, Outcome outcome,
            List<FailureCode> failureCodes, List<QualityExemplar> exemplars) {
        this.artifactId = boundedText(artifactId, "artifactId", MAX_ID_BYTES);
        if (fence == null) throw invalid("fence is required");
        boundedText(fence.runId(), "fence.runId", MAX_ID_BYTES);
        boundedText(fence.attemptId(), "fence.attemptId", MAX_ID_BYTES);
        this.fence = fence;
        this.publicationId = boundedText(
                publicationId, "publicationId", MAX_ID_BYTES);
        requireDigest(policyDigest, "policyDigest");
        if (acceptedTotal < 0L || rejectedTotal < 0L) {
            throw invalid("quality totals are negative");
        }
        this.members = canonicalMembers(members);
        this.reasonObservations = canonicalObservations(reasonObservations);
        if (outcome == null) throw invalid("outcome is required");
        this.failureCodes = canonicalFailures(failureCodes);
        if ((outcome == Outcome.PASS) != this.failureCodes.isEmpty()) {
            throw invalid("outcome and failure codes disagree");
        }
        this.exemplars = canonicalExemplars(exemplars);
        this.policyDigest = policyDigest.clone();
        this.acceptedTotal = acceptedTotal;
        this.rejectedTotal = rejectedTotal;
        this.outcome = outcome;
    }

    public static QualityGateDecision evaluate(
            String artifactId, RunAttemptFence fence, String publicationId,
            QualityGatePolicy policy, QualityPreparedEvidence evidence) {
        if (policy == null) throw invalid("policy is required");
        if (evidence == null) throw invalid("prepared evidence is required");
        List<ReasonObservation> observations = new ArrayList<>(
                policy.reasonThresholds().size());
        for (QualityGatePolicy.ReasonThreshold threshold : policy.reasonThresholds()) {
            observations.add(new ReasonObservation(
                    threshold.reason(), threshold.maximum(),
                    evidence.observedReason(threshold.reason())));
        }
        QualityGateEvaluation evaluation = QualityGateEvaluator.evaluate(
                policy, evidence.acceptedTotal(), evidence.rejectedTotal(),
                evidence.reasonObservations());
        List<FailureCode> failures = new ArrayList<>(
                evaluation.failureCodes().size());
        for (String failure : evaluation.failureCodes()) {
            failures.add(FailureCode.valueOf(failure));
        }
        if (evidence.exemplars().size() > policy.exemplarLimit()) {
            throw invalid("prepared exemplars exceed the policy limit");
        }
        return new QualityGateDecision(artifactId, fence, publicationId,
                QualityGatePolicyCodec.digest(policy), evidence.acceptedTotal(),
                evidence.rejectedTotal(), evidence.members(), observations,
                evaluation.passed() ? Outcome.PASS : Outcome.FAIL,
                failures, evidence.exemplars());
    }

    public String artifactId() { return artifactId; }
    public RunAttemptFence fence() { return fence; }
    public String publicationId() { return publicationId; }
    public byte[] policyDigest() { return policyDigest.clone(); }
    public long acceptedTotal() { return acceptedTotal; }
    public long rejectedTotal() { return rejectedTotal; }
    public List<QualityPreparedMemberEvidence> members() { return members; }
    public List<ReasonObservation> reasonObservations() { return reasonObservations; }
    public Outcome outcome() { return outcome; }
    public List<FailureCode> failureCodes() { return failureCodes; }
    public List<QualityExemplar> exemplars() { return exemplars; }

    private static List<QualityPreparedMemberEvidence> canonicalMembers(
            List<QualityPreparedMemberEvidence> values) {
        if (values == null || values.size() != 3) {
            throw invalid("exactly three prepared members are required");
        }
        String[] roles = {
                TypedQualityManifest.VALID_SINK_UID,
                TypedQualityManifest.REJECTED_SINK_UID,
                TypedQualityManifest.SUMMARY_SINK_UID
        };
        List<QualityPreparedMemberEvidence> copy = new ArrayList<>(3);
        for (int i = 0; i < roles.length; i++) {
            QualityPreparedMemberEvidence member = values.get(i);
            if (member == null || !roles[i].equals(member.role())) {
                throw invalid("prepared members are not in canonical role order");
            }
            boundedText(member.memberName(), "member.name", MAX_ID_BYTES);
            copy.add(member);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<ReasonObservation> canonicalObservations(
            List<ReasonObservation> values) {
        if (values == null
                || values.size() > QualityGatePolicy.MAX_REASON_THRESHOLDS) {
            throw invalid("reason observations exceed their bound");
        }
        List<ReasonObservation> copy = new ArrayList<>(values.size());
        ReasonObservation previous = null;
        for (ReasonObservation value : values) {
            if (value == null || (previous != null
                    && previous.reason().compareTo(value.reason()) >= 0)) {
                throw invalid("reason observations are not canonical");
            }
            copy.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<FailureCode> canonicalFailures(List<FailureCode> values) {
        if (values == null || values.size() > FailureCode.values().length) {
            throw invalid("failure codes exceed their closed set");
        }
        List<FailureCode> copy = new ArrayList<>(values.size());
        FailureCode previous = null;
        for (FailureCode value : values) {
            if (value == null || (previous != null
                    && previous.ordinal() >= value.ordinal())) {
                throw invalid("failure codes are not canonical");
            }
            copy.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<QualityExemplar> canonicalExemplars(
            List<QualityExemplar> values) {
        if (values == null || values.size() > QualityGatePolicy.MAX_EXEMPLARS) {
            throw invalid("quality exemplars exceed their bound");
        }
        List<QualityExemplar> copy = new ArrayList<>(values.size());
        QualityExemplar previous = null;
        for (QualityExemplar value : values) {
            if (value == null || (previous != null && previous.compareTo(value) >= 0)) {
                throw invalid("quality exemplars are not canonical");
            }
            copy.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(copy);
    }

    static String boundedText(String value, String field, int maxBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw invalid(field + " is not non-empty NUL-free NFC text");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > maxBytes) {
                throw invalid(field + " exceeds its UTF-8 bound");
            }
        } catch (CharacterCodingException malformed) {
            throw invalid(field + " is not strict UTF-8");
        }
        return value;
    }

    private static void requireDigest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw invalid(field + " must be SHA-256");
        }
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("quality gate decision is invalid: " + detail);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof QualityGateDecision)) return false;
        QualityGateDecision that = (QualityGateDecision) other;
        return acceptedTotal == that.acceptedTotal
                && rejectedTotal == that.rejectedTotal
                && artifactId.equals(that.artifactId)
                && fence.equals(that.fence)
                && publicationId.equals(that.publicationId)
                && MessageDigest.isEqual(policyDigest, that.policyDigest)
                && members.equals(that.members)
                && reasonObservations.equals(that.reasonObservations)
                && outcome == that.outcome
                && failureCodes.equals(that.failureCodes)
                && exemplars.equals(that.exemplars);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(artifactId, fence, publicationId,
                acceptedTotal, rejectedTotal, members, reasonObservations,
                outcome, failureCodes, exemplars);
        return 31 * result + Arrays.hashCode(policyDigest);
    }

    public static final class ReasonObservation {
        private final QualityReasonKey reason;
        private final long maximum;
        private final long observed;

        public ReasonObservation(QualityReasonKey reason, long maximum, long observed) {
            if (reason == null) throw invalid("observation reason is required");
            if (maximum < 0L || observed < 0L) {
                throw invalid("observation counts are negative");
            }
            this.reason = reason;
            this.maximum = maximum;
            this.observed = observed;
        }

        public QualityReasonKey reason() { return reason; }
        public long maximum() { return maximum; }
        public long observed() { return observed; }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof ReasonObservation)) return false;
            ReasonObservation that = (ReasonObservation) other;
            return maximum == that.maximum && observed == that.observed
                    && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, maximum, observed);
        }
    }
}
