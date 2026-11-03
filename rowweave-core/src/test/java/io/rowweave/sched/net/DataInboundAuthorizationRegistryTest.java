package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** grant OPEN is one-shot and bound to exact peer + connection generation. */
class DataInboundAuthorizationRegistryTest {
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-auth", "attempt-auth", 97L);
    private static final UUID TARGET = new UUID(101L, 103L);

    @Test
    void wrongGrantDoesNotConsumeOrdinalAndReplayOrCrossConnectionIsRejected() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(1 << 20);
        DataInboundAuthorizationRegistry registry =
                new DataInboundAuthorizationRegistry(budget);
        WorkerDataIdentity source = identity("source", (byte) 1);
        ShuffleProducerGrant first = grant((byte) 2);
        ShuffleProducerGrant second = grant((byte) 3);
        ShuffleInboundAuthorization authorization = ShuffleInboundAuthorization.of(
                TARGET, 1, Arrays.asList(
                        ShuffleAuthorizationRecord.authorize(FENCE, TARGET, 1, source, first),
                        ShuffleAuthorizationRecord.authorize(FENCE, TARGET, 2, source, second)));

        DataInboundAuthorizationRegistry.PreparedHandle prepared = registry.prepare(
                "assignment-a", FENCE, Collections.singletonList(authorization));
        assertThrows(DataInboundAuthorizationRegistry.AuthorizationRejectedException.class,
                () -> registry.open(TARGET, 1, grant((byte) 9), 11L, source));
        assertEquals(0, registry.openStreamCount());

        DataInboundAuthorizationRegistry.AuthorizedStream one =
                registry.open(TARGET, 1, first, 11L, source);
        assertTrue(one.matchesOwner(11L, source));
        assertThrows(DataInboundAuthorizationRegistry.AuthorizationRejectedException.class,
                () -> registry.open(TARGET, 1, first, 11L, source));
        assertThrows(DataInboundAuthorizationRegistry.AuthorizationRejectedException.class,
                () -> registry.open(TARGET, 2, second, 12L, source));

        DataInboundAuthorizationRegistry.AuthorizedStream two =
                registry.open(TARGET, 2, second, 11L, source);
        assertTrue(two.markEos());
        assertFalse(two.markEos());
        one.fail();
        prepared.close();
        assertEquals(0, registry.preparedTargetCount());
        assertEquals(0, registry.openStreamCount());
        assertEquals(0L, budget.snapshot().usedBytes());
        registry.close();
    }

    @Test
    void exactPeerMismatchAndConflictingPrepareFailClosedWithoutSecretDiagnostics() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(1 << 20);
        DataInboundAuthorizationRegistry registry =
                new DataInboundAuthorizationRegistry(budget);
        WorkerDataIdentity source = identity("source", (byte) 4);
        ShuffleProducerGrant grant = grant((byte) 5);
        ShuffleInboundAuthorization authorization = ShuffleInboundAuthorization.of(
                TARGET, 1, Collections.singletonList(
                        ShuffleAuthorizationRecord.authorize(
                                FENCE, TARGET, 1, source, grant)));
        DataInboundAuthorizationRegistry.PreparedHandle prepared = registry.prepare(
                "assignment-a", FENCE, Collections.singletonList(authorization));

        RuntimeException mismatch = assertThrows(
                DataInboundAuthorizationRegistry.AuthorizationRejectedException.class,
                () -> registry.open(TARGET, 1, grant, 21L,
                        identity("other", (byte) 6)));
        assertFalse(mismatch.getMessage().contains("5, 5"));
        assertThrows(IllegalStateException.class, () -> registry.prepare(
                "assignment-b", FENCE, Collections.singletonList(authorization)));

        prepared.close();
        assertEquals(0L, budget.snapshot().usedBytes());
        registry.close();
    }

    @Test
    void aggregateLeaseOutlivesPreparedHandleUntilLastAuthorizedStreamReleases() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(1 << 20);
        DataInboundAuthorizationRegistry registry =
                new DataInboundAuthorizationRegistry(budget);
        WorkerDataIdentity source = identity("source", (byte) 7);
        ShuffleProducerGrant grant = grant((byte) 8);
        ShuffleInboundAuthorization authorization = ShuffleInboundAuthorization.of(
                TARGET, 1, Collections.singletonList(
                        ShuffleAuthorizationRecord.authorize(
                                FENCE, TARGET, 1, source, grant)));
        DataInboundAuthorizationRegistry.PreparedHandle prepared = registry.prepare(
                "assignment-a", FENCE, Collections.singletonList(authorization));
        DataInboundAuthorizationRegistry.AuthorizedStream stream =
                registry.open(TARGET, 1, grant, 31L, source);
        long charged = budget.snapshot().usedBytes();
        assertTrue(charged > 0L);

        prepared.close();
        assertEquals(charged, budget.snapshot().usedBytes(),
                "handler/connection child reference must retain aggregate lease");
        stream.fail();
        assertEquals(0L, budget.snapshot().usedBytes());
        registry.close();
    }

    private static ShuffleProducerGrant grant(byte fill) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, fill);
        return ShuffleProducerGrant.fromBytes(bytes);
    }

    private static WorkerDataIdentity identity(String worker, byte fill) {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, fill);
        return WorkerDataIdentity.authenticated(worker, "inc-" + worker,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", fingerprint);
    }
}
