package io.rowweave.pipeline;

import java.util.Objects;

/** Immutable external-version evidence proposed by a claimed producer attempt. */
public final class VersionedDatasetCommitEvidence {
    private final VersionedDatasetProducerAttempt attempt;
    private final String storageProviderId;
    private final int storageContractVersion;
    private final String versionId;
    private final String readerBindingCodecId;
    private final int readerBindingCodecVersion;
    private final byte[] readerBinding;
    private final String contentDigest;
    private final long rowCount;
    private final long byteCount;
    private final long committedAtEpochMillis;

    public VersionedDatasetCommitEvidence(
            VersionedDatasetProducerAttempt attempt,
            String storageProviderId, int storageContractVersion,
            String versionId, String readerBindingCodecId,
            int readerBindingCodecVersion, byte[] readerBinding,
            String contentDigest, long rowCount, long byteCount,
            long committedAtEpochMillis) {
        if (attempt == null) {
            throw PipelineText.failure("INVALID_ATTEMPT", "evidence.attempt",
                    "producer attempt is required");
        }
        this.attempt = attempt;
        this.storageProviderId = VersionedDatasetHandoffSupport.id(
                storageProviderId, "evidence.storageProviderId");
        this.storageContractVersion = VersionedDatasetHandoffSupport.positiveVersion(
                storageContractVersion, "evidence.storageContractVersion");
        this.versionId = VersionedDatasetHandoffSupport.id(
                versionId, "evidence.versionId");
        this.readerBindingCodecId = VersionedDatasetHandoffSupport.id(
                readerBindingCodecId, "evidence.readerBindingCodecId");
        this.readerBindingCodecVersion = VersionedDatasetHandoffSupport.positiveVersion(
                readerBindingCodecVersion, "evidence.readerBindingCodecVersion");
        this.readerBinding = VersionedDatasetHandoffSupport.bytes(
                readerBinding, "evidence.readerBinding",
                VersionedDatasetHandoffLimits.MAX_READER_BINDING_BYTES);
        this.contentDigest = VersionedDatasetHandoffSupport.digest(
                contentDigest, "evidence.contentDigest");
        this.rowCount = VersionedDatasetHandoffSupport.nonNegative(
                rowCount, "evidence.rowCount");
        this.byteCount = VersionedDatasetHandoffSupport.nonNegative(
                byteCount, "evidence.byteCount");
        this.committedAtEpochMillis = VersionedDatasetHandoffSupport.positive(
                committedAtEpochMillis, "evidence.committedAtEpochMillis",
                "INVALID_TIMESTAMP");
    }

    public VersionedDatasetProducerAttempt attempt() { return attempt; }
    public String storageProviderId() { return storageProviderId; }
    public int storageContractVersion() { return storageContractVersion; }
    public String versionId() { return versionId; }
    public String readerBindingCodecId() { return readerBindingCodecId; }
    public int readerBindingCodecVersion() { return readerBindingCodecVersion; }
    public byte[] readerBinding() { return readerBinding.clone(); }
    public String contentDigest() { return contentDigest; }
    public long rowCount() { return rowCount; }
    public long byteCount() { return byteCount; }
    public long committedAtEpochMillis() { return committedAtEpochMillis; }
    public String evidenceDigest() {
        return VersionedDatasetHandoffCodec.sha256Hex(
                VersionedDatasetHandoffCodec.encodeEvidence(this));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VersionedDatasetCommitEvidence)) return false;
        VersionedDatasetCommitEvidence that = (VersionedDatasetCommitEvidence) other;
        return storageContractVersion == that.storageContractVersion
                && readerBindingCodecVersion == that.readerBindingCodecVersion
                && rowCount == that.rowCount && byteCount == that.byteCount
                && committedAtEpochMillis == that.committedAtEpochMillis
                && attempt.equals(that.attempt)
                && storageProviderId.equals(that.storageProviderId)
                && versionId.equals(that.versionId)
                && readerBindingCodecId.equals(that.readerBindingCodecId)
                && VersionedDatasetHandoffSupport.bytesEqual(
                        readerBinding, that.readerBinding)
                && contentDigest.equals(that.contentDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(attempt, storageProviderId, storageContractVersion,
                versionId, readerBindingCodecId, readerBindingCodecVersion,
                contentDigest, rowCount, byteCount, committedAtEpochMillis)
                + VersionedDatasetHandoffSupport.bytesHash(readerBinding);
    }
}
