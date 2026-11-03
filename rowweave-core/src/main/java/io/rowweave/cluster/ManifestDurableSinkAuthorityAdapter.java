package io.rowweave.cluster;

import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.commit.CommitManifest;
import io.rowweave.run.ManifestCommitIntent;

import java.security.MessageDigest;

/** Strict read-through projection of the existing Manifest journal; owns no state. */
final class ManifestDurableSinkAuthorityAdapter
        implements DurableSinkAuthorityAdapter {
    private final ManifestCommitIntent intent;
    private final CommitManifest intended;
    private final java.util.function.Supplier<BatchSinkOutcome> outcome;

    ManifestDurableSinkAuthorityAdapter(
            ManifestCommitIntent intent, CommitManifest intended,
            java.util.function.Supplier<BatchSinkOutcome> outcome) {
        if (intent == null || intended == null || outcome == null) {
            throw new IllegalArgumentException(
                    "Manifest authority projection fields are required");
        }
        this.intent = intent;
        this.intended = intended;
        this.outcome = outcome;
    }

    @Override
    public byte[] requireCommitDecision() {
        if (intent.generation() != intended.generation()
                || !intent.domainId().equals(intended.domainId())
                || !intent.publicationId().equals(intended.attemptId())
                || !MessageDigest.isEqual(
                intent.manifestHash(), sha256(intended.encode()))) {
            throw new IllegalStateException(
                    "Manifest authority decision conflicts with prepared publication");
        }
        return intent.manifestHash();
    }

    @Override
    public BatchSinkOutcome authorityOutcome() {
        return outcome.get();
    }

    private static byte[] sha256(byte[] frame) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(frame);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
