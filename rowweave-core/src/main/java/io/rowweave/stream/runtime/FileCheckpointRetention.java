package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSavepoint;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Crash-safe mark-and-sweep retention across checkpoint metadata and payloads. */
public final class FileCheckpointRetention {
    private final StreamCheckpointAuthority authority;
    private final StreamCheckpointCatalog catalog;
    private final FileCheckpointPayloadStore payloads;
    private final Map<String, CheckpointIdentity> recoveryRoots;
    private final Set<String> activeRuns;

    public FileCheckpointRetention(
            FileCheckpointAuthority authority,
            FileCheckpointMemberStore members,
            FileCheckpointPayloadStore payloads,
            FileStreamSavepointStore savepoints) {
        this(authority, members, payloads, savepoints,
                java.util.Collections.emptyMap(),
                java.util.Collections.emptySet());
    }

    public FileCheckpointRetention(
            FileCheckpointAuthority authority,
            FileCheckpointMemberStore members,
            FileCheckpointPayloadStore payloads,
            FileStreamSavepointStore savepoints,
            Map<String, CheckpointIdentity> recoveryRoots) {
        this(authority, members, payloads, savepoints, recoveryRoots,
                recoveryRoots.keySet());
    }

    public FileCheckpointRetention(
            FileCheckpointAuthority authority,
            FileCheckpointMemberStore members,
            FileCheckpointPayloadStore payloads,
            FileStreamSavepointStore savepoints,
            Map<String, CheckpointIdentity> recoveryRoots,
            Set<String> activeRuns) {
        this(authority,
                new FileStreamCheckpointCatalog(members, savepoints),
                payloads, recoveryRoots, activeRuns);
    }

    public FileCheckpointRetention(
            StreamCheckpointAuthority authority,
            StreamCheckpointCatalog catalog,
            FileCheckpointPayloadStore payloads,
            Map<String, CheckpointIdentity> recoveryRoots,
            Set<String> activeRuns) {
        if (authority == null || catalog == null || payloads == null
                || recoveryRoots == null
                || activeRuns == null) {
            throw new IllegalArgumentException(
                    "checkpoint retention stores are required");
        }
        this.authority = authority;
        this.catalog = catalog;
        this.payloads = payloads;
        LinkedHashMap<String, CheckpointIdentity> roots =
                new LinkedHashMap<>();
        for (Map.Entry<String, CheckpointIdentity> entry
                : recoveryRoots.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isEmpty()
                    || entry.getValue() == null
                    || !entry.getKey().equals(entry.getValue().runId())) {
                throw new IllegalArgumentException(
                        "checkpoint recovery root is invalid");
            }
            roots.put(entry.getKey(), entry.getValue());
        }
        this.recoveryRoots = java.util.Collections.unmodifiableMap(roots);
        HashSet<String> runs = new HashSet<>();
        for (String runId : activeRuns) {
            if (runId == null || runId.isEmpty()) {
                throw new IllegalArgumentException(
                        "active checkpoint run identity is invalid");
            }
            runs.add(runId);
        }
        if (!runs.containsAll(roots.keySet())) {
            throw new IllegalArgumentException(
                    "checkpoint recovery roots require active runs");
        }
        this.activeRuns = java.util.Collections.unmodifiableSet(runs);
    }

    /** Retains the newest regular checkpoints for one run and every savepoint. */
    public int retainLatest(String runId, int retainedCompleted) {
        if (runId == null || runId.isEmpty()
                || retainedCompleted < 1 || retainedCompleted > 128) {
            throw new IllegalArgumentException(
                    "run identity and retained checkpoint count are required");
        }
        List<StreamSavepoint> savepointRoots = catalog.savepoints();
        List<StreamSavepoint> regular = new ArrayList<>();
        for (StreamSavepoint point : savepointRoots) {
            CheckpointIdentity identity = point.checkpointManifest()
                    .checkpointIdentity();
            if (point.savepointId().startsWith("stream-checkpoint:")
                    && runId.equals(identity.runId())) {
                regular.add(point);
            }
        }
        regular.sort(Comparator.comparingLong((StreamSavepoint point) ->
                point.checkpointManifest().checkpointIdentity()
                        .checkpointSequence()).reversed());
        if (regular.size() <= retainedCompleted) {
            sweepFromRemainingReferences();
            return 0;
        }

        Set<String> evictedIds = new HashSet<>();
        for (int index = retainedCompleted; index < regular.size(); index++) {
            StreamSavepoint point = regular.get(index);
            evictedIds.add(point.savepointId());
        }
        for (String checkpointId : evictedIds) {
            catalog.removeCheckpointReference(checkpointId);
        }
        sweepFromRemainingReferences();
        return evictedIds.size();
    }

    private void sweepFromRemainingReferences() {
        Set<CheckpointIdentity> retainedIdentities = new HashSet<>();
        Set<String> retainedManifests = new HashSet<>();
        List<StreamCheckpointMember> retainedMembers = new ArrayList<>();
        Set<String> retainedPayloads = new HashSet<>();
        for (StreamSavepoint point : catalog.savepoints()) {
            CheckpointIdentity identity = point.checkpointManifest()
                    .checkpointIdentity();
            retainCompleted(identity, retainedIdentities, retainedManifests,
                    retainedMembers, retainedPayloads);
        }
        for (CheckpointIdentity identity : recoveryRoots.values()) {
            retainDecision(identity, true, retainedIdentities,
                    retainedManifests,
                    retainedMembers, retainedPayloads);
        }
        for (String runId : activeRuns) {
            CheckpointIdentity root = recoveryRoots.get(runId);
            long floor = root == null ? 0L : root.checkpointSequence();
            for (CheckpointDecision decision
                    : authority.decisionsForRun(runId)) {
                if (decision.checkpointIdentity().checkpointSequence() > floor
                        && (decision.status()
                        == CheckpointDecision.Status.COMMIT_DECIDED
                        || decision.status()
                        == CheckpointDecision.Status.COMPLETED)) {
                    retainDecision(decision.checkpointIdentity(), false,
                            retainedIdentities, retainedManifests,
                            retainedMembers, retainedPayloads);
                }
            }
        }
        payloads.retainOnly(retainedPayloads);
        catalog.retainOnlyMembers(retainedMembers);
        authority.retainOnly(retainedIdentities, retainedManifests);
    }

    private void retainCompleted(
            CheckpointIdentity identity,
            Set<CheckpointIdentity> retainedIdentities,
            Set<String> retainedManifests,
            List<StreamCheckpointMember> retainedMembers,
            Set<String> retainedPayloads) {
        retainDecision(identity, true, retainedIdentities,
                retainedManifests, retainedMembers, retainedPayloads);
    }

    private void retainDecision(
            CheckpointIdentity identity, boolean requireCompleted,
            Set<CheckpointIdentity> retainedIdentities,
            Set<String> retainedManifests,
            List<StreamCheckpointMember> retainedMembers,
            Set<String> retainedPayloads) {
        if (!retainedIdentities.add(identity)) return;
        CheckpointDecision decision = authority.decision(identity);
        if (decision == null
                || (requireCompleted
                && decision.status() != CheckpointDecision.Status.COMPLETED)
                || (!requireCompleted
                && decision.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                && decision.status()
                != CheckpointDecision.Status.COMPLETED)) {
            throw new IllegalStateException(
                    "retained checkpoint reference is not completed");
        }
        retainedManifests.add(decision.manifestDigest());
        for (StreamCheckpointMember member
                : catalog.resolve(decision, authority)) {
            retainedMembers.add(member);
            retainPayloadClosure(member, retainedPayloads);
        }
    }

    private void retainPayloadClosure(StreamCheckpointMember root,
            Set<String> retainedPayloads) {
        StreamCheckpointMember current = root;
        Set<String> visitedPayloads = new HashSet<>();
        int depth = 0;
        while (current != null && current.contentBytes() > 0L) {
            if (!visitedPayloads.add(current.contentDigest())) return;
            retainedPayloads.add(current.contentDigest());
            if (++depth > 8) {
                throw new StreamCheckpointException(
                        StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                        "checkpoint payload dependency depth exceeds its bound");
            }
            current = FinalEventTimeDecimalWindow.nestedCheckpointMember(
                    current, payloads);
        }
    }
}
