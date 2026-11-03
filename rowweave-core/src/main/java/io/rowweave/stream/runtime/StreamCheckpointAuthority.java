package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;

import java.util.List;
import java.util.Set;

/** Durable bounded first-decision-wins metadata authority for Stream checkpoints. */
public interface StreamCheckpointAuthority {
    CheckpointDecision publishDecision(
            CheckpointDecision proposed, CheckpointManifest manifest);

    CheckpointDecision complete(CheckpointDecision completed);

    CheckpointDecision decision(CheckpointIdentity identity);

    CheckpointDecision firstDecision(CheckpointIdentity identity);

    CheckpointManifest manifest(String manifestDigest);

    /** Lists durable first decisions/completions for one run in sequence order. */
    List<CheckpointDecision> decisionsForRun(String runId);

    /** Removes metadata that is unreachable from the supplied durable roots. */
    void retainOnly(Set<CheckpointIdentity> identities,
            Set<String> manifestDigests);
}
