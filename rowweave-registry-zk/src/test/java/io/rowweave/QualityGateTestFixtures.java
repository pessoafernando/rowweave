package io.rowweave;

import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocatorCodec;
import io.rowweave.run.RunAttemptFence;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Test-only bridge for package-private prepared-evidence construction. */
public final class QualityGateTestFixtures {
    private static final List<String> ROLES = Collections.unmodifiableList(
            Arrays.asList(TypedQualityManifest.VALID_SINK_UID,
                    TypedQualityManifest.REJECTED_SINK_UID,
                    TypedQualityManifest.SUMMARY_SINK_UID));

    private QualityGateTestFixtures() {
    }

    public static List<String> roles() {
        return ROLES;
    }

    public static QualityGateDecision decision(
            String artifactId, RunAttemptFence fence, String publicationId,
            QualityGatePolicy policy, long accepted, long rejected,
            List<ManifestMemberReceipt> receipts) {
        List<QualityPreparedMemberEvidence> members = new ArrayList<>();
        for (int i = 0; i < receipts.size(); i++) {
            ManifestMemberReceipt receipt = receipts.get(i);
            members.add(new QualityPreparedMemberEvidence(
                    ROLES.get(i), receipt.member().name(),
                    receipt.member().length(), receipt.member().sha256(),
                    sha256(PreparedSinkLocatorCodec.encode(
                            receipt.preparedLocator()))));
        }
        return QualityGateDecision.evaluate(
                artifactId, fence, publicationId, policy,
                new QualityPreparedEvidence(
                        accepted, rejected, Collections.emptyMap(),
                        Collections.emptyList(), members));
    }

    public static QualityGateDecision decision(
            String artifactId, RunAttemptFence fence, String publicationId,
            QualityGatePolicy policy, long accepted, long rejected,
            List<ManifestMemberReceipt> receipts,
            Map<QualityReasonKey, Long> reasonObservations,
            List<QualityExemplar> exemplars) {
        List<QualityPreparedMemberEvidence> members = new ArrayList<>();
        for (int i = 0; i < receipts.size(); i++) {
            ManifestMemberReceipt receipt = receipts.get(i);
            members.add(new QualityPreparedMemberEvidence(
                    ROLES.get(i), receipt.member().name(),
                    receipt.member().length(), receipt.member().sha256(),
                    sha256(PreparedSinkLocatorCodec.encode(
                            receipt.preparedLocator()))));
        }
        return QualityGateDecision.evaluate(
                artifactId, fence, publicationId, policy,
                new QualityPreparedEvidence(
                        accepted, rejected, reasonObservations,
                        exemplars, members));
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
