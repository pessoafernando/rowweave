package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.QualityPreparedMemberEvidence;
import io.rowweave.commit.CommitManifest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Immutable authority view of sealed output members plus an optional irreversible intent. */
public final class ManifestCommitJournalSnapshot {
    private final StageAssignmentSnapshot assignments;
    private final List<ManifestMemberReceipt> members;
    private final QualityGateDecision qualityDecision;
    private final ManifestCommitIntent intent;

    ManifestCommitJournalSnapshot(
            StageAssignmentSnapshot assignments,
            List<ManifestMemberReceipt> members,
            ManifestCommitIntent intent) {
        this(assignments, members,
                intent == null ? null : intent.qualityDecision(), intent);
    }

    ManifestCommitJournalSnapshot(
            StageAssignmentSnapshot assignments,
            List<ManifestMemberReceipt> members,
            QualityGateDecision qualityDecision,
            ManifestCommitIntent intent) {
        if (assignments == null) throw new IllegalArgumentException("assignments are required");
        if (members == null
                || members.size() != assignments.manifest().assignmentCount()) {
            throw new IllegalArgumentException("member receipt count must match assignments");
        }
        List<ManifestMemberReceipt> copy = new ArrayList<>(members.size());
        Set<String> names = new HashSet<>();
        String domain = null;
        String publication = null;
        long generation = 0L;
        for (int i = 0; i < members.size(); i++) {
            ManifestMemberReceipt receipt = members.get(i);
            if (receipt != null) {
                requireMemberEvidence(assignments, i, receipt);
                if (!names.add(receipt.member().name())) {
                    throw new IllegalArgumentException("duplicate manifest member name");
                }
                if (domain == null) {
                    domain = receipt.domainId();
                    generation = receipt.generation();
                    publication = receipt.publicationId();
                } else if (!domain.equals(receipt.domainId())
                        || generation != receipt.generation()
                        || !publication.equals(receipt.publicationId())) {
                    throw new IllegalArgumentException(
                            "manifest member commit identity mismatch");
                }
            }
            copy.add(receipt);
        }
        this.assignments = assignments;
        this.members = Collections.unmodifiableList(copy);
        this.qualityDecision = qualityDecision;
        this.intent = intent;
        if (qualityDecision != null) requireQualityDecision(qualityDecision);
        if (intent != null && (qualityDecision == null) != !intent.hasQualityDecision()) {
            throw new IllegalArgumentException(
                    "manifest intent quality state does not match journal decision");
        }
        if (intent != null && qualityDecision != null
                && !qualityDecision.equals(intent.qualityDecision())) {
            throw new IllegalArgumentException(
                    "manifest intent embeds a different quality decision");
        }
        if (intent != null && !buildIntentInternal().equals(intent)) {
            throw new IllegalArgumentException(
                    "manifest intent does not match sealed member closure");
        }
    }

    public static ManifestCommitJournalSnapshot initial(
            StageAssignmentSnapshot assignments) {
        if (assignments == null) throw new IllegalArgumentException("assignments are required");
        return new ManifestCommitJournalSnapshot(assignments,
                Collections.nCopies(assignments.manifest().assignmentCount(),
                        (ManifestMemberReceipt) null), null);
    }

    public StageAssignmentSnapshot assignments() { return assignments; }
    public ManifestMemberReceipt member(int ordinal) { return members.get(ordinal); }
    public QualityGateDecision qualityDecision() { return qualityDecision; }
    public ManifestCommitIntent intent() { return intent; }
    public int memberCount() {
        int count = 0;
        for (ManifestMemberReceipt member : members) if (member != null) count++;
        return count;
    }

    public int expectedMemberCount() {
        int count = 0;
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            if (outputRole(assignments.manifest().assignment(i).role())) count++;
        }
        return count;
    }

    public boolean allExpectedMembersSealed() {
        return expectedMemberCount() > 0 && memberCount() == expectedMemberCount();
    }

    public ManifestCommitJournalSnapshot withMember(ManifestMemberReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (qualityDecision != null || intent != null) {
            throw new IllegalStateException(
                    "manifest quality decision or intent freezes member receipts");
        }
        int ordinal = receipt.token().physicalOrdinal();
        if (ordinal < 0 || ordinal >= members.size() || members.get(ordinal) != null) {
            throw new IllegalArgumentException("member receipt is already present or out of bounds");
        }
        List<ManifestMemberReceipt> copy = new ArrayList<>(members);
        copy.set(ordinal, receipt);
        return new ManifestCommitJournalSnapshot(assignments, copy, null, null);
    }

    public ManifestCommitJournalSnapshot withQualityDecision(
            QualityGateDecision decision) {
        if (decision == null) {
            throw new IllegalArgumentException("quality decision is required");
        }
        if (qualityDecision != null || intent != null) {
            throw new IllegalStateException("manifest journal slot is already decided");
        }
        return new ManifestCommitJournalSnapshot(
                assignments, members, decision, null);
    }

    public ManifestCommitIntent buildIntent() {
        return buildIntentInternal();
    }

    public ManifestCommitJournalSnapshot withIntent(ManifestCommitIntent intended) {
        if (intended == null) throw new IllegalArgumentException("intent is required");
        if (intent != null) throw new IllegalStateException("manifest intent is already present");
        return new ManifestCommitJournalSnapshot(
                assignments, members, qualityDecision, intended);
    }

    public CommitManifest intendedManifest() {
        if (!allExpectedMembersSealed()) {
            throw new IllegalStateException("manifest member closure is incomplete");
        }
        ManifestMemberReceipt identity = firstMember();
        List<CommitManifest.Member> descriptors = new ArrayList<>(memberCount());
        for (ManifestMemberReceipt member : members) {
            if (member != null) descriptors.add(member.member());
        }
        return new CommitManifest(identity.domainId(), identity.generation(),
                identity.publicationId(), descriptors);
    }

    private ManifestCommitIntent buildIntentInternal() {
        if (!allExpectedMembersSealed()) {
            throw new IllegalStateException("manifest member closure is incomplete");
        }
        ManifestMemberReceipt identity = firstMember();
        int[] selectedShards = qualityDecision == null
                ? allShardIndices(assignments.manifest().shardCount())
                : qualityMemberShardIndices(
                assignments.manifest().qualityGateBinding());
        List<byte[]> shardDigests = new ArrayList<>(selectedShards.length);
        for (int shard : selectedShards) {
            shardDigests.add(sha256(
                    ManifestCommitJournalCodec.encodeMemberShard(this, shard)));
        }
        if (qualityDecision == null) {
            return new ManifestCommitIntent(
                assignments.manifest().fence(),
                assignments.manifest().generation(), identity.domainId(),
                identity.generation(), identity.publicationId(), memberCount(),
                shardDigests, sha256(intendedManifest().encode()));
        }
        if (qualityDecision.outcome() != QualityGateDecision.Outcome.PASS) {
            throw new IllegalStateException(
                    "failed quality decision cannot become manifest intent");
        }
        return ManifestCommitIntent.quality(
                assignments.manifest().fence(),
                assignments.manifest().generation(), identity.domainId(),
                identity.generation(), identity.publicationId(), memberCount(),
                shardDigests, sha256(intendedManifest().encode()),
                QualityGateDecisionCodec.encode(qualityDecision));
    }

    private static int[] allShardIndices(int shardCount) {
        int[] indices = new int[shardCount];
        for (int i = 0; i < shardCount; i++) indices[i] = i;
        return indices;
    }

    private static int[] qualityMemberShardIndices(
            QualityGateAssignmentBinding binding) {
        int[] selected = new int[QualityGateAssignmentBinding.MEMBER_COUNT];
        int count = 0;
        for (QualityGateAssignmentBinding.MemberBinding member
                : binding.members()) {
            int shard = member.physicalOrdinal()
                    / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
            boolean present = false;
            for (int i = 0; i < count; i++) {
                if (selected[i] == shard) {
                    present = true;
                    break;
                }
            }
            if (!present) selected[count++] = shard;
        }
        java.util.Arrays.sort(selected, 0, count);
        return java.util.Arrays.copyOf(selected, count);
    }

    private void requireQualityDecision(QualityGateDecision decision) {
        QualityGateAssignmentBinding binding =
                assignments.manifest().qualityGateBinding();
        if (binding == null || !MessageDigest.isEqual(
                binding.policyDigest(), decision.policyDigest())
                || !allExpectedMembersSealed() || memberCount() != 3
                || !decision.fence().equals(assignments.manifest().fence())
                || !decision.artifactId().equals(assignments.manifest().artifactId())) {
            throw new IllegalArgumentException(
                    "quality decision identity, policy or member closure mismatch");
        }
        ManifestMemberReceipt identity = firstMember();
        if (!decision.publicationId().equals(identity.publicationId())) {
            throw new IllegalArgumentException(
                    "quality decision publication identity mismatch");
        }
        for (int i = 0; i < binding.members().size(); i++) {
            QualityGateAssignmentBinding.MemberBinding expected =
                    binding.members().get(i);
            ManifestMemberReceipt receipt = members.get(expected.physicalOrdinal());
            if (receipt == null || !receipt.hasPreparedLocator()
                    || !binding.matches(expected.physicalOrdinal(),
                    receipt.preparedLocator().attemptIdentity())) {
                throw new IllegalArgumentException(
                        "quality prepared member binding mismatch");
            }
            QualityPreparedMemberEvidence evidence = decision.members().get(i);
            if (!expected.role().equals(evidence.role())
                    || !receipt.member().name().equals(evidence.memberName())
                    || receipt.member().length() != evidence.dataLength()
                    || !MessageDigest.isEqual(
                    receipt.member().sha256(), evidence.dataHash())
                    || !MessageDigest.isEqual(
                    sha256(PreparedSinkLocatorCodec.encode(
                            receipt.preparedLocator())),
                    evidence.preparedLocatorDigest())) {
                throw new IllegalArgumentException(
                        "quality decision prepared evidence mismatch");
            }
        }
    }

    private ManifestMemberReceipt firstMember() {
        for (ManifestMemberReceipt member : members) {
            if (member != null) return member;
        }
        throw new IllegalStateException("manifest has no member receipts");
    }

    private static void requireMemberEvidence(
            StageAssignmentSnapshot assignments, int ordinal,
            ManifestMemberReceipt receipt) {
        StageAssignmentRecord record = assignments.manifest().assignment(ordinal);
        if (!outputRole(record.role())) {
            throw new IllegalArgumentException(
                    "manifest member requires an output assignment");
        }
        StageAssignmentAcceptance acceptance = assignments.acceptance(ordinal);
        StageTerminalReceipt terminal = assignments.terminal(ordinal);
        if (acceptance == null || terminal == null || !terminal.succeeded()) {
            throw new IllegalArgumentException(
                    "manifest member requires a successful accepted assignment");
        }
        if (!receipt.token().equals(acceptance.token())
                || !receipt.token().equals(terminal.token())
                || !MessageDigest.isEqual(
                receipt.commandDigest(), acceptance.commandDigest())
                || !MessageDigest.isEqual(
                receipt.commandDigest(), terminal.commandDigest())) {
            throw new IllegalArgumentException(
                    "manifest member assignment evidence mismatch");
        }
    }

    static boolean outputRole(StageAssignmentRole role) {
        return role == StageAssignmentRole.SINK
                || role == StageAssignmentRole.WHOLE_GRAPH
                || role == StageAssignmentRole.SYSTEM_CONTROL;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
