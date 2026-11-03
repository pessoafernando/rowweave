package io.rowweave.incremental;

import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Artifact visibility and zero-attempt pending Run share one publication guard. */
class IncrementalArtifactPendingPublicationContractTest {

    @Test
    void publishesArtifactAndExactPendingRunInsideOneNamedBoundary()
            throws Exception {
        InMemoryArtifactRepository repository = repository();
        byte[] artifact = encoded("incremental-pending", bytes("payload"));
        String artifactId = PlanArtifactCodec.artifactId(artifact);
        Authority authority = authority();
        IncrementalClaimRequest request = request(artifactId);

        IncrementalPendingRunDecision published =
                repository.publishReferencedIncrementalPending(
                        artifact, ignored -> { }, request,
                        value -> authority.store.publishPending(
                                authority.token, value));

        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                published.code());
        assertArrayEquals(artifact, repository.get(artifactId));
        assertEquals(request, authority.store.pending("feed"));
        assertEquals(RunStatus.READY, published.runSnapshot().status());
        assertEquals(0, published.runSnapshot().attemptCount());
        assertNull(published.runSnapshot().currentAttempt());

        IncrementalPendingRunDecision replay =
                repository.publishReferencedIncrementalPending(
                        artifact, ignored -> { }, request,
                        value -> authority.store.publishPending(
                                authority.token, value));
        assertEquals(IncrementalPendingRunDecision.Code.ALREADY_PUBLISHED,
                replay.code());
        assertEquals(0, replay.runSnapshot().attemptCount());
    }

    @Test
    void artifactIdentityMismatchOrNullDecisionCannotMasqueradeAsReference()
            throws Exception {
        InMemoryArtifactRepository repository = repository();
        byte[] artifact = encoded("incremental-mismatch", bytes("payload"));
        AtomicInteger callbacks = new AtomicInteger();
        IncrementalClaimRequest mismatch = request(
                repeat('a', 64));

        assertThrows(IllegalArgumentException.class,
                () -> repository.publishReferencedIncrementalPending(
                        artifact, ignored -> { }, mismatch, value -> {
                            callbacks.incrementAndGet();
                            return null;
                        }));
        assertEquals(0, callbacks.get());

        IncrementalClaimRequest exact = request(
                PlanArtifactCodec.artifactId(artifact));
        assertThrows(IllegalStateException.class,
                () -> repository.publishReferencedIncrementalPending(
                        artifact, ignored -> { }, exact, value -> null));
    }

    private static Authority authority() throws Exception {
        InMemoryIncrementalAuthorityStore store =
                new InMemoryIncrementalAuthorityStore();
        IncrementalInitializeRequest initialize =
                new IncrementalInitializeRequest(
                        "feed", 1L, digest("definition"),
                        digest("source-contract"), digest("frontier-contract"),
                        IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, "feed", "initialize",
                0L, 1L, null, null, initialize.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        command, initialize)).code());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                store.record("feed").state());
        FrontierOwnershipClaim ownership = store.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master", 1L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, ownership.code());
        return new Authority(store, ownership.token());
    }

    private static InMemoryArtifactRepository repository() {
        return new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
    }

    private static byte[] encoded(String definitionId, byte[] payload) {
        OperatorDescriptor source = new OperatorDescriptor(
                "source-1", OperatorDescriptor.Kind.SOURCE, "source",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                Collections.<String>emptyList());
        PlanArtifact artifact = new PlanArtifact(
                definitionId, PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                Arrays.asList(
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
                        PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS),
                Collections.<String>emptyList(),
                Collections.singletonList(source),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                payload);
        return PlanArtifactCodec.encode(artifact);
    }

    private static IncrementalClaimRequest request(String artifactId)
            throws Exception {
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        return new IncrementalClaimRequest(
                "feed", capture, digest("frontier-contract"), artifactId,
                "attempt-1", "publication-1", 3, 0L, 10L);
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int index = 0; index < count; index++) result.append(value);
        return result.toString();
    }

    private static final class Authority {
        final InMemoryIncrementalAuthorityStore store;
        final FrontierRecoveryToken token;

        Authority(InMemoryIncrementalAuthorityStore store,
                  FrontierRecoveryToken token) {
            this.store = store;
            this.token = token;
        }
    }
}
