package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

final class IncrementalProofDigest {
    private IncrementalProofDigest() {}

    static byte[] publication(
            String feedId, long feedIncarnation,
            byte[] frontierRecoveryTokenDigest, byte[] runRecoveryTokenDigest,
            String domainId, RunAttemptFence fence, String publicationId,
            long generation, byte[] manifestHash, byte[] decisionDigest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(out, "rowweave-cdc-publication-proof-v2");
            IncrementalAuthorityCodecSupport.writeText(out, feedId);
            out.writeLong(feedIncarnation);
            out.write(frontierRecoveryTokenDigest);
            out.write(runRecoveryTokenDigest);
            writeIdentity(out, domainId, fence, publicationId, generation,
                    manifestHash, decisionDigest);
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory publication proof failed", impossible);
        }
    }

    static byte[] joint(
            IncrementalActiveInterval active, VerifiedIncrementalPublicationProof publication) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(out, "rowweave-cdc-joint-proof-v1");
            IncrementalAuthorityCodecSupport.writeText(out, active.identity().intervalId());
            IncrementalAuthorityCodecSupport.writeText(
                    out, active.identity().capturedTargetReceiptDigest());
            out.writeByte(active.capturedTarget().mode().wireId());
            IncrementalAuthorityCodecSupport.writeText(out, active.artifactId());
            out.write(active.capturedTarget().definitionDigest());
            writeIdentity(out, publication.domainId(), publication.fence(),
                    publication.publicationId(), publication.generation(),
                    publication.manifestHash(), publication.commitDecisionDigest());
            out.write(publication.proofDigest());
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory joint proof failed", impossible);
        }
    }

    static byte[] noSinkIntent(
            IncrementalActiveInterval active, byte[] assignmentRoot) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(
                    out, "rowweave-cdc-no-sink-intent-v1");
            IncrementalAuthorityCodecSupport.writeText(
                    out, active.identity().intervalId());
            IncrementalAuthorityCodecSupport.writeText(
                    out, active.identity().capturedTargetReceiptDigest());
            out.writeByte(active.capturedTarget().mode().wireId());
            IncrementalAuthorityCodecSupport.writeText(out, active.artifactId());
            writeFence(out, active.fence());
            IncrementalAuthorityCodecSupport.writeBytes(out, assignmentRoot);
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory no-intent proof failed", impossible);
        }
    }

    static byte[] conflict(
            String feedId, long feedIncarnation, String intervalId,
            String artifactId, RunAttemptFence fence, String domainId,
            String publicationId, long generation, byte[] manifestHash,
            byte[] decisionDigest, byte[] frontierTokenDigest,
            byte[] runTokenDigest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(
                    out, "rowweave-cdc-conflict-proof-v1");
            IncrementalAuthorityCodecSupport.writeText(out, feedId);
            out.writeLong(feedIncarnation);
            IncrementalAuthorityCodecSupport.writeText(out, intervalId);
            IncrementalAuthorityCodecSupport.writeText(out, artifactId);
            writeIdentity(out, domainId, fence, publicationId, generation,
                    manifestHash, decisionDigest);
            out.write(frontierTokenDigest);
            out.write(runTokenDigest);
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory conflict proof failed", impossible);
        }
    }

    private static void writeIdentity(
            DataOutputStream out, String domainId, RunAttemptFence fence,
            String publicationId, long generation, byte[] manifestHash,
            byte[] decisionDigest) throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, domainId);
        writeFence(out, fence);
        IncrementalAuthorityCodecSupport.writeText(out, publicationId);
        out.writeLong(generation);
        out.write(manifestHash);
        out.write(decisionDigest);
    }

    private static void writeFence(DataOutputStream out, RunAttemptFence fence)
            throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, fence.runId());
        IncrementalAuthorityCodecSupport.writeText(out, fence.attemptId());
        out.writeLong(fence.fenceToken());
    }
}
