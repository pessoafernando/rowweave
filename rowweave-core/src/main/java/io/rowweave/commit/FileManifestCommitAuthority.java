package io.rowweave.commit;

import java.nio.file.Path;

/** Manifest cluster authority for one host or a deployment with proven shared file-lock semantics. */
public final class FileManifestCommitAuthority implements ManifestCommitAuthority {

    private final FileCommitGenerationAuthority generations = new FileCommitGenerationAuthority();

    @Override
    public Lease tryAcquire(Path domain, String attemptId) {
        if (attemptId == null || attemptId.isEmpty()) {
            throw new IllegalArgumentException("attemptId must not be empty");
        }
        FileCommitDomainLease lease = FileCommitDomainLease.tryAcquire(domain);
        return lease == null ? null : lease::close;
    }

    @Override
    public long reserve(Path domain, String attemptId) {
        return generations.reserve(domain, attemptId);
    }

    @Override
    public Long find(Path domain, String attemptId) {
        return generations.find(domain, attemptId);
    }
}
