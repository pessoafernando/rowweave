package io.rowweave.commit;

/** Explicit outcome for the case where the manifest directory entry is already visible but the following directory force or ACK was never confirmed. */
public final class ManifestCommitUncertainException extends IllegalStateException {

    public ManifestCommitUncertainException(String message, Throwable cause) {
        super(message, cause);
    }
}
