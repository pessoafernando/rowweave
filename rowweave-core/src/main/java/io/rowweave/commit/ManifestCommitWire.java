package io.rowweave.commit;

import java.util.Collections;

/** Deterministic, bounded, checksummed frame for the member descriptor on the control plane. */
public final class ManifestCommitWire {

    private static final String FRAME_ID = "manifest-member-wire-v1";

    private ManifestCommitWire() {
    }

    public static byte[] encodeMember(CommitManifest.Member member) {
        if (member == null) {
            throw new IllegalArgumentException("manifest member must not be null");
        }
        return new CommitManifest(FRAME_ID, 1L, FRAME_ID, Collections.singletonList(member)).encode();
    }

    public static CommitManifest.Member decodeMember(byte[] encoded) {
        CommitManifest frame = CommitManifest.decode(encoded);
        if (!FRAME_ID.equals(frame.domainId()) || !FRAME_ID.equals(frame.attemptId())
                || frame.generation() != 1L || frame.members().size() != 1) {
            throw new IllegalArgumentException("invalid manifest member control frame");
        }
        return frame.members().get(0);
    }
}
