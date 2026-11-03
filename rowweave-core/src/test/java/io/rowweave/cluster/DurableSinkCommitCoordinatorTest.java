package io.rowweave.cluster;

import io.rowweave.commit.BatchSinkOutcome;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DurableSinkCommitCoordinatorTest {
    private static final byte[] DECISION = digest((byte) 7);

    @Test
    void queryPreservesProviderConflictOverAuthorityProjection() {
        BatchSinkOutcome conflict = BatchSinkOutcome.of(
                BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                3L, null, "PROVIDER_CONFLICT");

        BatchSinkOutcome observed = coordinator(conflict).query();

        assertEquals(BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                observed.state());
        assertEquals("PROVIDER_CONFLICT", observed.detailCode());
    }

    @Test
    void queryRejectsProviderIdentityThatDiffersFromAuthorityDecision() {
        BatchSinkOutcome wrongIdentity = BatchSinkOutcome.of(
                BatchSinkOutcome.State.COMMITTED,
                3L, digest((byte) 9), "PROVIDER_COMMITTED");

        BatchSinkOutcome observed = coordinator(wrongIdentity).query();

        assertEquals(BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                observed.state());
        assertEquals("PROVIDER_QUERY_IDENTITY_CONFLICT",
                observed.detailCode());
    }

    @Test
    void queryUsesProviderCommittedOnlyForExactAuthorityIdentity() {
        BatchSinkOutcome committed = BatchSinkOutcome.of(
                BatchSinkOutcome.State.COMMITTED,
                3L, DECISION, "PROVIDER_COMMITTED");

        BatchSinkOutcome observed = coordinator(committed).query();

        assertEquals(BatchSinkOutcome.State.COMMITTED, observed.state());
        assertArrayEquals(DECISION, observed.identityDigest());
    }

    @Test
    void queryKeepsDurableProjectionWhenWorkerIsUnavailable() {
        DurableSinkCommitCoordinator coordinator = new DurableSinkCommitCoordinator(
                authority(), (action, decision) -> {
                    throw new IllegalStateException("worker unavailable");
                });

        assertEquals(BatchSinkOutcome.State.COMMIT_DECIDED,
                coordinator.query().state());
    }

    private static DurableSinkCommitCoordinator coordinator(
            BatchSinkOutcome workerOutcome) {
        return new DurableSinkCommitCoordinator(
                authority(), (action, decision) -> workerOutcome);
    }

    private static DurableSinkAuthorityAdapter authority() {
        return new DurableSinkAuthorityAdapter() {
            @Override
            public byte[] requireCommitDecision() {
                return DECISION.clone();
            }

            @Override
            public BatchSinkOutcome authorityOutcome() {
                return BatchSinkOutcome.of(
                        BatchSinkOutcome.State.COMMIT_DECIDED,
                        3L, DECISION, "AUTHORITY_COMMIT_DECIDED");
            }
        };
    }

    private static byte[] digest(byte value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, value);
        return digest;
    }
}
