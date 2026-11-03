package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.QualityPreparedMemberEvidence;
import io.rowweave.commit.CommitManifest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Bounded three-member projection of the durable quality commit journal. */
public final class QualityGateCommitJournalSnapshot {
    private final RunAttemptFence fence;
    private final String artifactId;
    private final long assignmentGeneration;
    private final long acceptanceGeneration;
    private final QualityGateAssignmentBinding binding;
    private final List<StageAssignmentAcceptance> acceptances;
    private final List<StageTerminalReceipt> terminals;
    private final List<ManifestMemberReceipt> members;
    private final List<byte[]> memberShardDigests;
    private final QualityGateDecision decision;
    private final ManifestCommitIntent intent;

    public QualityGateCommitJournalSnapshot(
            RunAttemptFence fence, String artifactId, long assignmentGeneration,
            long acceptanceGeneration,
            QualityGateAssignmentBinding binding,
            List<StageAssignmentAcceptance> acceptances,
            List<StageTerminalReceipt> terminals,
            List<ManifestMemberReceipt> members,
            List<byte[]> memberShardDigests,
            QualityGateDecision decision, ManifestCommitIntent intent) {
        if (fence == null || artifactId == null || artifactId.isEmpty()
                || assignmentGeneration <= 0L || acceptanceGeneration <= 0L
                || binding == null) {
            throw new IllegalArgumentException(
                    "quality journal identity and binding are required");
        }
        if (acceptances == null || terminals == null || members == null
                || acceptances.size() != QualityGateAssignmentBinding.MEMBER_COUNT
                || terminals.size() != QualityGateAssignmentBinding.MEMBER_COUNT
                || members.size() != QualityGateAssignmentBinding.MEMBER_COUNT) {
            throw new IllegalArgumentException(
                    "quality journal requires exactly three member slots");
        }
        List<ManifestMemberReceipt> copy = new ArrayList<>(members.size());
        Set<String> memberNames = new HashSet<>();
        String memberDomain = null;
        String memberPublication = null;
        long memberGeneration = 0L;
        for (int i = 0; i < members.size(); i++) {
            StageAssignmentAcceptance acceptance = acceptances.get(i);
            StageTerminalReceipt terminal = terminals.get(i);
            ManifestMemberReceipt member = members.get(i);
            int ordinal = binding.members().get(i).physicalOrdinal();
            if ((acceptance != null
                    && acceptance.token().physicalOrdinal() != ordinal)
                    || (terminal != null
                    && terminal.token().physicalOrdinal() != ordinal)
                    || (member != null
                    && member.token().physicalOrdinal() != ordinal)) {
                throw new IllegalArgumentException(
                        "quality journal member ordinal does not match binding");
            }
            if (member != null) {
                if (acceptance == null || terminal == null
                        || !terminal.succeeded()
                        || !member.token().equals(acceptance.token())
                        || !member.token().equals(terminal.token())
                        || !MessageDigest.isEqual(member.commandDigest(),
                        acceptance.commandDigest())
                        || !MessageDigest.isEqual(member.commandDigest(),
                        terminal.commandDigest())
                        || !member.hasPreparedLocator()
                        || !binding.matches(ordinal,
                        member.preparedLocator().attemptIdentity())) {
                    throw new IllegalArgumentException(
                            "quality member assignment evidence mismatch");
                }
                if (!memberNames.add(member.member().name())) {
                    throw new IllegalArgumentException(
                            "duplicate quality manifest member name");
                }
                if (memberDomain == null) {
                    memberDomain = member.domainId();
                    memberGeneration = member.generation();
                    memberPublication = member.publicationId();
                } else if (!memberDomain.equals(member.domainId())
                        || memberGeneration != member.generation()
                        || !memberPublication.equals(member.publicationId())) {
                    throw new IllegalArgumentException(
                            "quality member commit identity mismatch");
                }
            }
            copy.add(member);
        }
        if (memberShardDigests == null
                || memberShardDigests.size()
                != selectedShardIndices(binding).length) {
            throw new IllegalArgumentException(
                    "quality journal member shard digest count mismatch");
        }
        List<byte[]> digestCopy = new ArrayList<>(memberShardDigests.size());
        for (byte[] digest : memberShardDigests) {
            if (digest == null || digest.length != StageTerminalReceipt.DIGEST_BYTES) {
                throw new IllegalArgumentException(
                        "quality member shard digest must be SHA-256");
            }
            digestCopy.add(digest.clone());
        }
        if (decision != null && !decision.fence().equals(fence)) {
            throw new IllegalArgumentException(
                    "quality journal decision fence mismatch");
        }
        if (intent != null && (!intent.hasQualityDecision()
                || !intent.fence().equals(fence)
                || decision == null
                || !decision.equals(intent.qualityDecision()))) {
            throw new IllegalArgumentException(
                    "quality journal intent does not match decision");
        }
        this.fence = fence;
        this.artifactId = artifactId;
        this.assignmentGeneration = assignmentGeneration;
        this.acceptanceGeneration = acceptanceGeneration;
        this.binding = binding;
        this.acceptances = Collections.unmodifiableList(
                new ArrayList<>(acceptances));
        this.terminals = Collections.unmodifiableList(new ArrayList<>(terminals));
        this.members = Collections.unmodifiableList(copy);
        this.memberShardDigests = Collections.unmodifiableList(digestCopy);
        this.decision = decision;
        this.intent = intent;
        if (decision != null) requireDecisionEvidence(decision);
        if (intent != null && !buildIntentInternal().equals(intent)) {
            throw new IllegalArgumentException(
                    "quality journal intent does not match member closure");
        }
    }

    public static QualityGateCommitJournalSnapshot from(
            ManifestCommitJournalSnapshot journal) {
        if (journal == null) return null;
        StageAssignmentManifest manifest = journal.assignments().manifest();
        QualityGateAssignmentBinding binding = manifest.qualityGateBinding();
        if (binding == null) return null;
        List<ManifestMemberReceipt> members = new ArrayList<>(
                QualityGateAssignmentBinding.MEMBER_COUNT);
        List<StageAssignmentAcceptance> acceptances = new ArrayList<>(
                QualityGateAssignmentBinding.MEMBER_COUNT);
        List<StageTerminalReceipt> terminals = new ArrayList<>(
                QualityGateAssignmentBinding.MEMBER_COUNT);
        List<byte[]> memberShardDigests = new ArrayList<>();
        for (QualityGateAssignmentBinding.MemberBinding member
                : binding.members()) {
            acceptances.add(journal.assignments().acceptance(
                    member.physicalOrdinal()));
            terminals.add(journal.assignments().terminal(
                    member.physicalOrdinal()));
            members.add(journal.member(member.physicalOrdinal()));
        }
        int[] shards = selectedShardIndices(binding);
        for (int shard : shards) {
            memberShardDigests.add(sha256(
                    ManifestCommitJournalCodec.encodeMemberShard(
                            journal, shard)));
        }
        return new QualityGateCommitJournalSnapshot(
                manifest.fence(), manifest.artifactId(), manifest.generation(),
                journal.assignments().acceptanceGeneration(), binding,
                acceptances, terminals, members, memberShardDigests,
                journal.qualityDecision(), journal.intent());
    }

    public RunAttemptFence fence() { return fence; }
    public String artifactId() { return artifactId; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public long acceptanceGeneration() { return acceptanceGeneration; }
    public QualityGateAssignmentBinding binding() { return binding; }
    public List<ManifestMemberReceipt> members() { return members; }
    public QualityGateDecision decision() { return decision; }
    public ManifestCommitIntent intent() { return intent; }

    public int memberCount() {
        int count = 0;
        for (ManifestMemberReceipt member : members) {
            if (member != null) count++;
        }
        return count;
    }

    public boolean allMembersSealed() {
        for (int i = 0; i < members.size(); i++) {
            StageAssignmentAcceptance acceptance = acceptances.get(i);
            StageTerminalReceipt terminal = terminals.get(i);
            ManifestMemberReceipt member = members.get(i);
            if (acceptance == null || terminal == null || member == null
                    || !terminal.succeeded()
                    || !member.token().equals(acceptance.token())
                    || !member.token().equals(terminal.token())
                    || !MessageDigest.isEqual(member.commandDigest(),
                    acceptance.commandDigest())
                    || !MessageDigest.isEqual(member.commandDigest(),
                    terminal.commandDigest())) {
                return false;
            }
        }
        return true;
    }

    public ManifestMemberReceipt memberForOrdinal(int physicalOrdinal) {
        for (int i = 0; i < binding.members().size(); i++) {
            if (binding.members().get(i).physicalOrdinal() == physicalOrdinal) {
                return members.get(i);
            }
        }
        return null;
    }

    public QualityGateCommitJournalSnapshot withMember(
            ManifestMemberReceipt receipt, byte[] selectedShardDigest) {
        if (receipt == null || selectedShardDigest == null) {
            throw new IllegalArgumentException(
                    "quality receipt and selected shard digest are required");
        }
        if (decision != null || intent != null) {
            throw new IllegalStateException(
                    "quality decision or intent freezes member receipts");
        }
        int selected = -1;
        for (int i = 0; i < binding.members().size(); i++) {
            if (binding.members().get(i).physicalOrdinal()
                    == receipt.token().physicalOrdinal()) selected = i;
        }
        if (selected < 0 || members.get(selected) != null) {
            throw new IllegalArgumentException(
                    "quality member is already present or outside binding");
        }
        List<ManifestMemberReceipt> updated = new ArrayList<>(members);
        updated.set(selected, receipt);
        List<byte[]> digests = new ArrayList<>(memberShardDigests);
        int shard = receipt.token().physicalOrdinal()
                / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        int[] shards = selectedShardIndices(binding);
        int digestIndex = -1;
        for (int i = 0; i < shards.length; i++) {
            if (shards[i] == shard) digestIndex = i;
        }
        if (digestIndex < 0) {
            throw new IllegalArgumentException("quality member shard is not selected");
        }
        digests.set(digestIndex, selectedShardDigest);
        return copy(updated, digests, null, null);
    }

    public QualityGateCommitJournalSnapshot withQualityDecision(
            QualityGateDecision qualityDecision) {
        if (qualityDecision == null || decision != null || intent != null) {
            throw new IllegalStateException("quality journal slot is already decided");
        }
        return copy(members, memberShardDigests, qualityDecision, null);
    }

    public QualityGateCommitJournalSnapshot withIntent(
            ManifestCommitIntent commitIntent) {
        if (commitIntent == null || intent != null) {
            throw new IllegalStateException("quality manifest intent is already present");
        }
        return copy(members, memberShardDigests, decision, commitIntent);
    }

    public CommitManifest intendedManifest() {
        if (!allMembersSealed()) {
            throw new IllegalStateException("quality member closure is incomplete");
        }
        List<Integer> order = new ArrayList<>(
                QualityGateAssignmentBinding.MEMBER_COUNT);
        for (int i = 0; i < members.size(); i++) order.add(i);
        order.sort(Comparator.comparingInt(
                index -> binding.members().get(index).physicalOrdinal()));
        ManifestMemberReceipt identity = members.get(order.get(0));
        List<CommitManifest.Member> descriptors = new ArrayList<>(members.size());
        for (Integer index : order) {
            ManifestMemberReceipt member = members.get(index);
            if (!identity.domainId().equals(member.domainId())
                    || identity.generation() != member.generation()
                    || !identity.publicationId().equals(member.publicationId())) {
                throw new IllegalStateException(
                        "quality member commit identity mismatch");
            }
            descriptors.add(member.member());
        }
        return new CommitManifest(identity.domainId(), identity.generation(),
                identity.publicationId(), descriptors);
    }

    public ManifestCommitIntent buildIntent() {
        return buildIntentInternal();
    }

    private ManifestCommitIntent buildIntentInternal() {
        if (decision == null
                || decision.outcome() != QualityGateDecision.Outcome.PASS) {
            throw new IllegalStateException(
                    "quality PASS decision is required for manifest intent");
        }
        CommitManifest manifest = intendedManifest();
        return ManifestCommitIntent.quality(
                fence, assignmentGeneration, manifest.domainId(),
                manifest.generation(), manifest.attemptId(),
                members.size(), memberShardDigests, sha256(manifest.encode()),
                QualityGateDecisionCodec.encode(decision));
    }

    private QualityGateCommitJournalSnapshot copy(
            List<ManifestMemberReceipt> updatedMembers,
            List<byte[]> updatedDigests, QualityGateDecision updatedDecision,
            ManifestCommitIntent updatedIntent) {
        return new QualityGateCommitJournalSnapshot(
                fence, artifactId, assignmentGeneration, acceptanceGeneration,
                binding, acceptances, terminals, updatedMembers,
                updatedDigests, updatedDecision, updatedIntent);
    }

    private void requireDecisionEvidence(QualityGateDecision qualityDecision) {
        if (!allMembersSealed() || !artifactId.equals(qualityDecision.artifactId())
                || !MessageDigest.isEqual(
                binding.policyDigest(), qualityDecision.policyDigest())
                || qualityDecision.members().size()
                != QualityGateAssignmentBinding.MEMBER_COUNT) {
            throw new IllegalArgumentException(
                    "quality decision identity, policy or member closure mismatch");
        }
        for (int i = 0; i < members.size(); i++) {
            ManifestMemberReceipt member = members.get(i);
            QualityPreparedMemberEvidence evidence =
                    qualityDecision.members().get(i);
            QualityGateAssignmentBinding.MemberBinding expected =
                    binding.members().get(i);
            if (!qualityDecision.publicationId().equals(member.publicationId())
                    || !member.hasPreparedLocator()
                    || !binding.matches(expected.physicalOrdinal(),
                    member.preparedLocator().attemptIdentity())
                    || !expected.role().equals(evidence.role())
                    || !member.member().name().equals(evidence.memberName())
                    || member.member().length() != evidence.dataLength()
                    || !MessageDigest.isEqual(
                    member.member().sha256(), evidence.dataHash())
                    || !MessageDigest.isEqual(sha256(
                    PreparedSinkLocatorCodec.encode(member.preparedLocator())),
                    evidence.preparedLocatorDigest())) {
                throw new IllegalArgumentException(
                        "quality decision prepared evidence mismatch");
            }
        }
    }

    private static int[] selectedShardIndices(
            QualityGateAssignmentBinding selectedBinding) {
        int[] selected = new int[QualityGateAssignmentBinding.MEMBER_COUNT];
        int count = 0;
        for (QualityGateAssignmentBinding.MemberBinding member
                : selectedBinding.members()) {
            int shard = member.physicalOrdinal()
                    / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
            boolean present = false;
            for (int i = 0; i < count; i++) {
                if (selected[i] == shard) present = true;
            }
            if (!present) selected[count++] = shard;
        }
        Arrays.sort(selected, 0, count);
        return Arrays.copyOf(selected, count);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

}
