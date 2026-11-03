package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointAuthorityReference;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;

/** Production coordinator joining exact task evidence with durable File authority. */
public final class StreamCheckpointCoordinator {
    private final CheckpointExpectation expectation;
    private final CheckpointAuthorityReference collecting;
    private final StreamCheckpointAuthority authority;
    private final FileCheckpointMemberStore memberStore;
    private final StreamCheckpointCatalog catalog;

    public StreamCheckpointCoordinator(
            CheckpointExpectation expectation,
            FileCheckpointAuthority authority) {
        this(expectation, (StreamCheckpointAuthority) authority,
                (FileCheckpointMemberStore) null);
    }

    public StreamCheckpointCoordinator(
            CheckpointExpectation expectation,
            FileCheckpointAuthority authority,
            FileCheckpointMemberStore memberStore) {
        this(expectation, (StreamCheckpointAuthority) authority, memberStore);
    }

    public StreamCheckpointCoordinator(
            CheckpointExpectation expectation,
            StreamCheckpointAuthority authority) {
        this(expectation, authority, (StreamCheckpointCatalog) null);
    }

    public StreamCheckpointCoordinator(
            CheckpointExpectation expectation,
            StreamCheckpointAuthority authority,
            FileCheckpointMemberStore memberStore) {
        if (expectation == null || authority == null) {
            throw new IllegalArgumentException(
                    "checkpoint expectation and authority are required");
        }
        this.expectation = expectation;
        this.collecting = new CheckpointAuthorityReference(expectation);
        this.authority = authority;
        this.memberStore = memberStore;
        this.catalog = null;
        authority.decision(expectation.checkpointIdentity());
    }

    public StreamCheckpointCoordinator(
            CheckpointExpectation expectation,
            StreamCheckpointAuthority authority,
            StreamCheckpointCatalog catalog) {
        if (expectation == null || authority == null) {
            throw new IllegalArgumentException(
                    "checkpoint expectation and authority are required");
        }
        this.expectation = expectation;
        this.collecting = new CheckpointAuthorityReference(expectation);
        this.authority = authority;
        this.memberStore = null;
        this.catalog = catalog;
        authority.decision(expectation.checkpointIdentity());
    }

    public void acknowledge(CheckpointTaskReceipt receipt) {
        if (authority.firstDecision(expectation.checkpointIdentity()) != null) {
            throw failure(StreamCheckpointException.Code.CHECKPOINT_ALREADY_DECIDED,
                    "durable checkpoint decision no longer accepts ACKs");
        }
        if (catalog != null) catalog.verify(receipt);
        else if (memberStore != null) memberStore.verify(receipt);
        collecting.acknowledge(receipt);
    }

    public CheckpointDecision decideCommit() {
        CheckpointDecision durable = authority.firstDecision(
                expectation.checkpointIdentity());
        if (durable != null) {
            if (durable.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
                return durable;
            }
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "aborted checkpoint cannot commit");
        }
        CheckpointDecision proposed = collecting.decideCommit();
        return authority.publishDecision(proposed, collecting.manifest());
    }

    public CheckpointDecision abort() {
        CheckpointDecision durable = authority.firstDecision(
                expectation.checkpointIdentity());
        if (durable != null) {
            if (durable.status() == CheckpointDecision.Status.ABORTED) {
                return durable;
            }
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "committed checkpoint cannot abort");
        }
        return authority.publishDecision(
                CheckpointDecision.aborted(expectation.checkpointIdentity()),
                null);
    }

    /** Marks completion only after external Sink/source reconcile has succeeded. */
    public CheckpointDecision complete(CheckpointDecision commitDecision) {
        CheckpointDecision durable = authority.firstDecision(
                expectation.checkpointIdentity());
        if (durable == null || !durable.equals(commitDecision)
                || durable.status()
                != CheckpointDecision.Status.COMMIT_DECIDED) {
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "completion requires the exact durable commit decision");
        }
        return authority.complete(CheckpointDecision.completed(durable));
    }

    public CheckpointDecision currentDecision() {
        return authority.decision(expectation.checkpointIdentity());
    }

    public CheckpointIdentity checkpointIdentity() {
        return expectation.checkpointIdentity();
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }
}
