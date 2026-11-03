package io.rowweave.commit;

/** Manifest cluster authority capability that a registry or deployment provider may expose. */
public interface ManifestCommitAuthorityProvider {
    ManifestCommitAuthority manifestCommitAuthority();
}
