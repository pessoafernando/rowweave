package io.rowweave.run;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/** Immutable proof that one successful output assignment handed off an installed member. */
public final class ManifestMemberReceipt {
    public static final int MAX_DOMAIN_ID_BYTES = 256;
    public static final int MAX_PUBLICATION_ID_BYTES = 256;
    public static final int MAX_MEMBER_NAME_BYTES = 256;
    public static final int MAX_RELATIVE_PATH_BYTES = 1024;
    public static final int MAX_SCHEMA_FINGERPRINT_BYTES = 256;

    private final StageAssignmentToken token;
    private final byte[] commandDigest;
    private final String domainId;
    private final long generation;
    private final String publicationId;
    private final CommitManifest.Member member;
    private final long sealedAtMillis;
    private final PreparedSinkLocator preparedLocator;

    private ManifestMemberReceipt(
            StageAssignmentToken token, byte[] commandDigest,
            String domainId, long generation, String publicationId,
            CommitManifest.Member member, long sealedAtMillis,
            PreparedSinkLocator preparedLocator) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (commandDigest == null
                || commandDigest.length != StageTerminalReceipt.DIGEST_BYTES) {
            throw new IllegalArgumentException("commandDigest must be SHA-256");
        }
        this.domainId = bounded(domainId, "domainId", MAX_DOMAIN_ID_BYTES);
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be positive");
        }
        this.publicationId = bounded(
                publicationId, "publicationId", MAX_PUBLICATION_ID_BYTES);
        if (member == null) throw new IllegalArgumentException("member is required");
        bounded(member.name(), "member.name", MAX_MEMBER_NAME_BYTES);
        bounded(member.relativeDataPath(), "member.relativeDataPath",
                MAX_RELATIVE_PATH_BYTES);
        bounded(member.schemaFingerprint(), "member.schemaFingerprint",
                MAX_SCHEMA_FINGERPRINT_BYTES);
        String expectedPath = "data/" + generation + "/" + member.name() + ".data";
        if (!expectedPath.equals(member.relativeDataPath())) {
            throw new IllegalArgumentException(
                    "member path does not match immutable generation identity");
        }
        if (sealedAtMillis < 0L) {
            throw new IllegalArgumentException("sealedAtMillis must be >= 0");
        }
        this.token = token;
        this.commandDigest = commandDigest.clone();
        this.generation = generation;
        this.member = member;
        this.sealedAtMillis = sealedAtMillis;
        validateLocator(preparedLocator);
        this.preparedLocator = preparedLocator;
    }

    public static ManifestMemberReceipt sealed(
            StageAssignmentToken token, byte[] commandDigest,
            String domainId, long generation, String publicationId,
            CommitManifest.Member member, long sealedAtMillis) {
        return new ManifestMemberReceipt(token, commandDigest, domainId,
                generation, publicationId, member, sealedAtMillis, null);
    }

    public static ManifestMemberReceipt sealed(
            StageAssignmentToken token, byte[] commandDigest,
            String domainId, long generation, String publicationId,
            CommitManifest.Member member, long sealedAtMillis,
            PreparedSinkLocator preparedLocator) {
        if (preparedLocator == null) {
            throw new IllegalArgumentException("prepared sink locator is required");
        }
        return new ManifestMemberReceipt(token, commandDigest, domainId,
                generation, publicationId, member, sealedAtMillis, preparedLocator);
    }

    public StageAssignmentToken token() { return token; }
    public byte[] commandDigest() { return commandDigest.clone(); }
    public String domainId() { return domainId; }
    public long generation() { return generation; }
    public String publicationId() { return publicationId; }
    public CommitManifest.Member member() { return member; }
    public long sealedAtMillis() { return sealedAtMillis; }
    public boolean hasPreparedLocator() { return preparedLocator != null; }
    public PreparedSinkLocator preparedLocator() { return preparedLocator; }

    private void validateLocator(PreparedSinkLocator locator) {
        if (locator == null) return;
        SinkAttemptIdentity identity = locator.attemptIdentity();
        if (!domainId.equals(locator.commitDomainId())
                || !token.fence().equals(identity.fence())
                || token.manifestGeneration() != identity.assignmentGeneration()
                || !token.stageExecutionId().equals(identity.stageExecutionId())
                || token.logicalStage() != identity.logicalStage()
                || token.instanceIndex() != identity.instanceIndex()
                || locator.preparedAtMillis() != sealedAtMillis
                || locator.dataLength() != member.length()
                || !MessageDigest.isEqual(locator.dataHash(), member.sha256())
                || !member.equals(ManifestCommitWire.decodeMember(locator.payload()))) {
            throw new IllegalArgumentException(
                    "prepared sink locator does not match manifest member receipt");
        }
    }

    private static String bounded(String value, String field, int maxBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new IllegalArgumentException(field + " is invalid or exceeds limit");
        }
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ManifestMemberReceipt)) return false;
        ManifestMemberReceipt that = (ManifestMemberReceipt) other;
        return generation == that.generation
                && sealedAtMillis == that.sealedAtMillis
                && token.equals(that.token)
                && domainId.equals(that.domainId)
                && publicationId.equals(that.publicationId)
                && member.equals(that.member)
                && Objects.equals(preparedLocator, that.preparedLocator)
                && MessageDigest.isEqual(commandDigest, that.commandDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(token, domainId, generation,
                publicationId, member, sealedAtMillis);
        result = 31 * result + Objects.hashCode(preparedLocator);
        return 31 * result + java.util.Arrays.hashCode(commandDigest);
    }
}
