package io.rowweave.cluster;

import io.rowweave.commit.BatchSinkOutcome;

import java.security.MessageDigest;

/** Provider-independent decision-before-visibility and retry/probe state machine. */
final class DurableSinkCommitCoordinator {
    interface WorkerControl {
        BatchSinkOutcome execute(
                ProviderPreparedGroupCommandCodec.Action action,
                byte[] authorityDecisionDigest);
    }

    private final DurableSinkAuthorityAdapter authority;
    private final WorkerControl worker;

    DurableSinkCommitCoordinator(
            DurableSinkAuthorityAdapter authority, WorkerControl worker) {
        if (authority == null || worker == null) {
            throw new IllegalArgumentException(
                    "durable Sink coordinator fields are required");
        }
        this.authority = authority;
        this.worker = worker;
    }

    BatchSinkOutcome publishAndFinalize() {
        return publishAndFinalize(() -> { }, () -> { });
    }

    BatchSinkOutcome publishAndFinalize(
            Runnable beforeDecision, Runnable afterDecision) {
        if (beforeDecision == null || afterDecision == null) {
            throw new IllegalArgumentException(
                    "durable decision hooks are required");
        }
        beforeDecision.run();
        byte[] decision = authority.requireCommitDecision();
        afterDecision.run();
        BatchSinkOutcome published;
        try {
            published = worker.execute(
                    ProviderPreparedGroupCommandCodec.Action.PUBLISH, decision);
        } catch (RuntimeException uncertain) {
            published = worker.execute(
                    ProviderPreparedGroupCommandCodec.Action.QUERY, decision);
            if (published.state() != BatchSinkOutcome.State.COMMITTED) {
                throw uncertain;
            }
        }
        if (published.state() != BatchSinkOutcome.State.COMMITTED
                || published.identityDigest() != null
                && !MessageDigest.isEqual(
                decision, published.identityDigest())) {
            throw new IllegalStateException(
                    "provider publication does not match durable decision");
        }
        try {
            worker.execute(ProviderPreparedGroupCommandCodec.Action.FINALIZE,
                    decision);
        } catch (RuntimeException cleanupFailure) {
            try {
                worker.execute(
                        ProviderPreparedGroupCommandCodec.Action.FINALIZE,
                        decision);
            } catch (RuntimeException replayFailure) {
                // A visible publication cannot be downgraded by bounded cleanup.
            }
        }
        return published;
    }

    BatchSinkOutcome query() {
        BatchSinkOutcome projected = authority.authorityOutcome();
        if (projected.state() != BatchSinkOutcome.State.COMMIT_DECIDED
                && projected.state() != BatchSinkOutcome.State.COMMITTED) {
            return projected;
        }
        byte[] decision = authority.requireCommitDecision();
        BatchSinkOutcome provider;
        try {
            provider = worker.execute(
                    ProviderPreparedGroupCommandCodec.Action.QUERY, decision);
        } catch (RuntimeException unavailable) {
            return projected;
        }
        if (provider.state()
                == BatchSinkOutcome.State.CORRUPT_OR_CONFLICT) {
            return provider;
        }
        if (provider.state() == BatchSinkOutcome.State.COMMITTED) {
            if (provider.identityDigest() == null
                    || !MessageDigest.isEqual(
                    decision, provider.identityDigest())) {
                return BatchSinkOutcome.of(
                        BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                        provider.generation(), null,
                        "PROVIDER_QUERY_IDENTITY_CONFLICT");
            }
            return provider;
        }
        return projected;
    }

    BatchSinkOutcome abort(String reasonCode) {
        byte[] decision = authority.requireAbortDecision(reasonCode);
        BatchSinkOutcome aborted;
        try {
            aborted = worker.execute(
                    ProviderPreparedGroupCommandCodec.Action.ABORT, decision);
        } catch (RuntimeException uncertain) {
            aborted = worker.execute(
                    ProviderPreparedGroupCommandCodec.Action.ABORT, decision);
        }
        authority.completeAbort();
        return aborted;
    }
}
