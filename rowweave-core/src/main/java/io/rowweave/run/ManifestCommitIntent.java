package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable commit decision: the exact journal shards and manifest bytes to publish. */
public final class ManifestCommitIntent {
    private final RunAttemptFence fence;
    private final long assignmentGeneration;
    private final String domainId;
    private final long generation;
    private final String publicationId;
    private final int memberCount;
    private final List<byte[]> memberShardDigests;
    private final byte[] manifestHash;
    private final byte[] qualityDecisionBytes;
    private final QualityGateDecision qualityDecision;
    private final byte[] policyDigest;
    private final byte[] qualityDecisionDigest;

    ManifestCommitIntent(
            RunAttemptFence fence, long assignmentGeneration,
            String domainId, long generation, String publicationId,
            int memberCount, List<byte[]> memberShardDigests,
            byte[] manifestHash) {
        this(fence, assignmentGeneration, domainId, generation, publicationId,
                memberCount, memberShardDigests, manifestHash, null);
    }

    private ManifestCommitIntent(
            RunAttemptFence fence, long assignmentGeneration,
            String domainId, long generation, String publicationId,
            int memberCount, List<byte[]> memberShardDigests,
            byte[] manifestHash, byte[] qualityDecisionBytes) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (assignmentGeneration <= 0L || generation <= 0L) {
            throw new IllegalArgumentException("intent generations must be positive");
        }
        RunText.required(domainId, "domainId", ManifestMemberReceipt.MAX_DOMAIN_ID_BYTES);
        RunText.required(publicationId, "publicationId",
                ManifestMemberReceipt.MAX_PUBLICATION_ID_BYTES);
        if (memberCount < 1 || memberCount > StageAssignmentManifest.MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException("intent member count out of bounds");
        }
        if (memberShardDigests == null || memberShardDigests.isEmpty()
                || memberShardDigests.size() > StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException("intent shard count out of bounds");
        }
        List<byte[]> digests = new ArrayList<>(memberShardDigests.size());
        for (byte[] digest : memberShardDigests) {
            requireDigest(digest, "member shard digest");
            digests.add(digest.clone());
        }
        requireDigest(manifestHash, "manifest hash");
        this.fence = fence;
        this.assignmentGeneration = assignmentGeneration;
        this.domainId = domainId;
        this.generation = generation;
        this.publicationId = publicationId;
        this.memberCount = memberCount;
        this.memberShardDigests = Collections.unmodifiableList(digests);
        this.manifestHash = manifestHash.clone();
        if (qualityDecisionBytes == null) {
            this.qualityDecisionBytes = null;
            this.qualityDecision = null;
            this.policyDigest = null;
            this.qualityDecisionDigest = null;
        } else {
            QualityGateDecision decoded = QualityGateDecisionCodec.decode(
                    qualityDecisionBytes);
            if (decoded.outcome() != QualityGateDecision.Outcome.PASS
                    || !decoded.fence().equals(fence)
                    || !decoded.publicationId().equals(publicationId)) {
                throw new IllegalArgumentException(
                        "quality intent requires a matching PASS decision");
            }
            this.qualityDecisionBytes = qualityDecisionBytes.clone();
            this.qualityDecision = decoded;
            this.policyDigest = decoded.policyDigest();
            this.qualityDecisionDigest = sha256(qualityDecisionBytes);
        }
    }

    static ManifestCommitIntent quality(
            ManifestCommitIntent legacyFields, byte[] qualityDecisionBytes) {
        if (legacyFields == null || qualityDecisionBytes == null) {
            throw new IllegalArgumentException(
                    "quality intent fields and decision bytes are required");
        }
        List<byte[]> shardDigests = new ArrayList<>(
                legacyFields.memberShardCount());
        for (int i = 0; i < legacyFields.memberShardCount(); i++) {
            shardDigests.add(legacyFields.memberShardDigest(i));
        }
        return new ManifestCommitIntent(legacyFields.fence,
                legacyFields.assignmentGeneration, legacyFields.domainId,
                legacyFields.generation, legacyFields.publicationId,
                legacyFields.memberCount, shardDigests,
                legacyFields.manifestHash, qualityDecisionBytes);
    }

    /**
     * Builds the bounded quality-gate intent projection. The shard digests are
     * the unique member shards selected by the quality binding, in ascending
     * shard-index order; ordinary Manifest intents retain their legacy all-shard
     * representation.
     */
    public static ManifestCommitIntent quality(
            RunAttemptFence fence, long assignmentGeneration,
            String domainId, long generation, String publicationId,
            int memberCount, List<byte[]> selectedMemberShardDigests,
            byte[] manifestHash, byte[] qualityDecisionBytes) {
        if (memberCount != QualityGateAssignmentBinding.MEMBER_COUNT) {
            throw new IllegalArgumentException(
                    "quality intent requires exactly three members");
        }
        if (selectedMemberShardDigests == null
                || selectedMemberShardDigests.isEmpty()
                || selectedMemberShardDigests.size()
                > QualityGateAssignmentBinding.MEMBER_COUNT) {
            throw new IllegalArgumentException(
                    "quality intent requires one to three selected member shards");
        }
        return new ManifestCommitIntent(
                fence, assignmentGeneration, domainId, generation,
                publicationId, memberCount, selectedMemberShardDigests,
                manifestHash, qualityDecisionBytes);
    }

    public RunAttemptFence fence() { return fence; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String domainId() { return domainId; }
    public long generation() { return generation; }
    public String publicationId() { return publicationId; }
    public int memberCount() { return memberCount; }
    public int memberShardCount() { return memberShardDigests.size(); }
    public byte[] memberShardDigest(int index) {
        return memberShardDigests.get(index).clone();
    }
    public byte[] manifestHash() { return manifestHash.clone(); }
    public boolean hasQualityDecision() { return qualityDecision != null; }
    public QualityGateDecision qualityDecision() { return qualityDecision; }
    public byte[] qualityDecisionBytes() {
        return qualityDecisionBytes == null ? null : qualityDecisionBytes.clone();
    }
    public byte[] policyDigest() {
        return policyDigest == null ? null : policyDigest.clone();
    }
    public byte[] qualityDecisionDigest() {
        return qualityDecisionDigest == null ? null : qualityDecisionDigest.clone();
    }

    private static void requireDigest(byte[] value, String field) {
        if (value == null || value.length != StageTerminalReceipt.DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ManifestCommitIntent)) return false;
        ManifestCommitIntent that = (ManifestCommitIntent) other;
        if (assignmentGeneration != that.assignmentGeneration
                || generation != that.generation
                || memberCount != that.memberCount
                || !fence.equals(that.fence)
                || !domainId.equals(that.domainId)
                || !publicationId.equals(that.publicationId)
                || !MessageDigest.isEqual(manifestHash, that.manifestHash)
                || !equalNullable(qualityDecisionBytes, that.qualityDecisionBytes)
                || memberShardDigests.size() != that.memberShardDigests.size()) {
            return false;
        }
        for (int i = 0; i < memberShardDigests.size(); i++) {
            if (!MessageDigest.isEqual(
                    memberShardDigests.get(i), that.memberShardDigests.get(i))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fence, assignmentGeneration, domainId,
                generation, publicationId, memberCount);
        for (byte[] digest : memberShardDigests) {
            result = 31 * result + Arrays.hashCode(digest);
        }
        result = 31 * result + Arrays.hashCode(manifestHash);
        return 31 * result + Arrays.hashCode(qualityDecisionBytes);
    }

    private static boolean equalNullable(byte[] left, byte[] right) {
        return left == null ? right == null
                : right != null && MessageDigest.isEqual(left, right);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
