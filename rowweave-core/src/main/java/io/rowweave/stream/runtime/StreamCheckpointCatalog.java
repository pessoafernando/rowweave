package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSavepoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Durable bounded member proof and named savepoint metadata catalog. */
public interface StreamCheckpointCatalog {
    void publish(StreamCheckpointMember member);

    StreamCheckpointMember member(CheckpointEvidence evidence);

    StreamSavepoint publishSavepoint(StreamSavepoint savepoint);

    StreamSavepoint get(String savepointId);

    /** Returns every durable automatic checkpoint and named savepoint root. */
    List<StreamSavepoint> savepoints();

    /** Removes one automatic checkpoint root after its retention window closes. */
    void removeCheckpointReference(String checkpointId);

    /** Removes member metadata unreachable from the supplied durable roots. */
    void retainOnlyMembers(Collection<StreamCheckpointMember> retained);

    default void verify(CheckpointTaskReceipt receipt) {
        if (receipt == null) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint receipt is required");
        }
        for (CheckpointEvidence evidence : receipt.evidence()) {
            StreamCheckpointMember resolved = member(evidence);
            if (!evidence.equals(resolved.toEvidence())) {
                throw failure(
                        StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                        "checkpoint member does not match receipt evidence");
            }
        }
    }

    default List<StreamCheckpointMember> resolve(
            CheckpointDecision decision, StreamCheckpointAuthority authority) {
        if (decision == null || authority == null
                || (decision.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                && decision.status()
                != CheckpointDecision.Status.COMPLETED)) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "committed checkpoint decision is required for restore");
        }
        CheckpointIdentity identity = decision.checkpointIdentity();
        CheckpointDecision first = authority.firstDecision(identity);
        if (first == null
                || first.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                || !first.manifestDigest().equals(
                decision.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "checkpoint restore decision is not authoritative");
        }
        if (decision.status() == CheckpointDecision.Status.COMPLETED
                && !decision.equals(authority.decision(identity))) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "completed checkpoint is not authoritative");
        }
        CheckpointManifest manifest = authority.manifest(
                decision.manifestDigest());
        if (manifest == null || !identity.equals(
                manifest.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "checkpoint manifest is not authoritative");
        }
        List<StreamCheckpointMember> resolved = new ArrayList<>();
        for (CheckpointTaskReceipt receipt : manifest.taskReceipts()) {
            for (CheckpointEvidence evidence : receipt.evidence()) {
                StreamCheckpointMember value = member(evidence);
                if (!evidence.equals(value.toEvidence())) {
                    throw failure(StreamCheckpointException.Code
                                    .CORRUPT_MEMBER_CATALOG,
                            "checkpoint member does not match manifest evidence");
                }
                resolved.add(value);
            }
        }
        return Collections.unmodifiableList(resolved);
    }

    default StreamSavepoint create(String savepointId,
            CheckpointIdentity checkpointIdentity,
            StreamCheckpointAuthority authority) {
        if (checkpointIdentity == null || authority == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint identity and authority are required");
        }
        String canonicalId = StreamSavepoint.canonicalId(savepointId);
        StreamSavepoint existing = get(canonicalId);
        if (existing != null) {
            if (!checkpointIdentity.equals(
                    existing.checkpointManifest().checkpointIdentity())) {
                throw failure(StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                        "savepoint name cannot be rebound to another checkpoint");
            }
            return existing;
        }
        CheckpointDecision decision = authority.decision(checkpointIdentity);
        if (decision == null
                || decision.status() != CheckpointDecision.Status.COMPLETED) {
            throw failure(
                    StreamCheckpointException.Code.CHECKPOINT_NOT_COMPLETED,
                    "savepoint requires an exact completed checkpoint");
        }
        CheckpointManifest manifest = authority.manifest(
                decision.manifestDigest());
        if (manifest == null || !checkpointIdentity.equals(
                manifest.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                    "completed checkpoint has no exact manifest");
        }
        StreamSavepoint proposed = new StreamSavepoint(canonicalId, manifest);
        StreamSavepoint durable = publishSavepoint(proposed);
        if (!proposed.equals(durable)) {
            throw failure(StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                    "savepoint name cannot be rebound to another checkpoint");
        }
        return durable;
    }

    static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }
}
