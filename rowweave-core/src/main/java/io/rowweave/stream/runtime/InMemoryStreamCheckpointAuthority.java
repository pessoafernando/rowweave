package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Process-local linearizable implementation of checkpoint first-decision semantics. */
public final class InMemoryStreamCheckpointAuthority
        implements StreamCheckpointAuthority {
    private final Map<CheckpointIdentity, CheckpointDecision> first =
            new LinkedHashMap<>();
    private final Map<CheckpointIdentity, CheckpointDecision> completed =
            new LinkedHashMap<>();
    private final Map<String, CheckpointManifest> manifests =
            new LinkedHashMap<>();

    @Override
    public synchronized CheckpointDecision publishDecision(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        validateFirstDecision(proposed, manifest);
        CheckpointIdentity identity = proposed.checkpointIdentity();
        CheckpointDecision existing = first.get(identity);
        if (existing != null) return requireExact(existing, proposed);
        if (manifest != null) {
            CheckpointManifest prior = manifests.get(manifest.manifestDigest());
            if (prior != null && !prior.equals(manifest)) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "content-addressed checkpoint manifest drifted");
            }
            manifests.put(manifest.manifestDigest(), manifest);
        }
        first.put(identity, proposed);
        return proposed;
    }

    @Override
    public synchronized CheckpointDecision complete(
            CheckpointDecision marker) {
        if (marker == null
                || marker.status() != CheckpointDecision.Status.COMPLETED) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "completion marker must carry COMPLETED status");
        }
        CheckpointDecision committed = first.get(marker.checkpointIdentity());
        if (committed == null
                || committed.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                || !committed.manifestDigest().equals(marker.manifestDigest())) {
            throw failure(
                    StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "completion requires the exact durable commit decision");
        }
        requireManifest(committed);
        CheckpointDecision existing = completed.get(marker.checkpointIdentity());
        if (existing != null) return requireExact(existing, marker);
        completed.put(marker.checkpointIdentity(), marker);
        return marker;
    }

    @Override
    public synchronized CheckpointDecision decision(
            CheckpointIdentity identity) {
        requireIdentity(identity);
        CheckpointDecision completion = completed.get(identity);
        return completion == null ? firstDecision(identity) : completion;
    }

    @Override
    public synchronized CheckpointDecision firstDecision(
            CheckpointIdentity identity) {
        requireIdentity(identity);
        CheckpointDecision decision = first.get(identity);
        if (decision != null
                && decision.status()
                == CheckpointDecision.Status.COMMIT_DECIDED) {
            requireManifest(decision);
        }
        return decision;
    }

    @Override
    public synchronized CheckpointManifest manifest(String digest) {
        if (digest == null || !digest.matches("[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "canonical checkpoint digest is required");
        }
        return manifests.get(digest);
    }

    @Override
    public synchronized List<CheckpointDecision> decisionsForRun(
            String runId) {
        if (runId == null || runId.isEmpty()) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint run identity is required");
        }
        List<CheckpointDecision> result = new ArrayList<>();
        for (CheckpointDecision value : first.values()) {
            if (runId.equals(value.checkpointIdentity().runId())) {
                CheckpointDecision completion = completed.get(
                        value.checkpointIdentity());
                result.add(completion == null ? value : completion);
            }
        }
        result.sort(Comparator.comparingLong(value ->
                value.checkpointIdentity().checkpointSequence()));
        return Collections.unmodifiableList(result);
    }

    @Override public synchronized void retainOnly(
            Set<CheckpointIdentity> identities,
            Set<String> manifestDigests) {
        if (identities == null || manifestDigests == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "protected checkpoint manifests are required");
        }
        first.keySet().retainAll(identities);
        completed.keySet().retainAll(identities);
        manifests.keySet().retainAll(manifestDigests);
    }

    private CheckpointManifest requireManifest(CheckpointDecision decision) {
        CheckpointManifest manifest = manifests.get(decision.manifestDigest());
        if (manifest == null || !decision.checkpointIdentity().equals(
                manifest.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "commit decision has no exact durable manifest");
        }
        return manifest;
    }

    private static void validateFirstDecision(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        if (proposed == null
                || (proposed.status() != CheckpointDecision.Status.ABORTED
                && proposed.status()
                != CheckpointDecision.Status.COMMIT_DECIDED)) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "first decision must be ABORTED or COMMIT_DECIDED");
        }
        if (proposed.status() == CheckpointDecision.Status.ABORTED) {
            if (manifest != null) {
                throw failure(
                        StreamCheckpointException.Code.INVALID_CONFIGURATION,
                        "aborted checkpoint cannot publish a manifest");
            }
            return;
        }
        if (manifest == null
                || !proposed.checkpointIdentity().equals(
                manifest.checkpointIdentity())
                || !proposed.manifestDigest().equals(
                manifest.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "commit decision requires its exact canonical manifest");
        }
    }

    private static CheckpointDecision requireExact(
            CheckpointDecision existing, CheckpointDecision proposed) {
        if (!existing.equals(proposed)) {
            throw failure(
                    StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "checkpoint first decision cannot be rewritten");
        }
        return existing;
    }

    private static void requireIdentity(CheckpointIdentity identity) {
        if (identity == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint identity is required");
        }
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }
}
