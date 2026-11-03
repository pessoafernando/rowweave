package io.rowweave.incremental;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Canonical exact-binding digests for authority transition payloads. */
final class IncrementalTransitionDigest {
    private IncrementalTransitionDigest() {}

    static byte[] initialize(IncrementalInitializeRequest request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(out, "rowweave-cdc-initialize-v1");
            IncrementalAuthorityCodecSupport.writeText(out, request.feedId());
            out.writeLong(request.feedIncarnation());
            out.write(request.definitionDigest());
            out.write(request.sourceContractDigest());
            out.write(request.frontierContractDigest());
            out.writeByte(request.frontierKind().wireId());
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory initialize digest failed", impossible);
        }
    }

    static byte[] claim(IncrementalClaimRequest request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(out, "rowweave-cdc-claim-v1");
            IncrementalAuthorityCodecSupport.writeText(out, request.feedId());
            byte[] capture = CapturedTargetReceiptCodec.encode(request.capturedTarget());
            IncrementalAuthorityCodecSupport.writeBytes(out, capture);
            out.write(request.frontierContractDigest());
            IncrementalAuthorityCodecSupport.writeText(out, request.artifactId());
            IncrementalAuthorityCodecSupport.writeText(out, request.attemptId());
            IncrementalAuthorityCodecSupport.writeText(out, request.publicationId());
            out.writeInt(request.maxAttempts());
            out.writeLong(request.deadlineEpochMillis());
            out.writeLong(request.eventTimeMillis());
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory claim digest failed", impossible);
        }
    }
}
