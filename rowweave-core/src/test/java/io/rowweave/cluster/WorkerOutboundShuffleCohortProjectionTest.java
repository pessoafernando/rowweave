package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** every effective producer endpoint is prepared before assignment ACK. */
class WorkerOutboundShuffleCohortProjectionTest {

    @Test
    void projectsAuthorizedEndpointsAcrossLegacyEffectiveAndBroadcastAliasesOnce() {
        DataSchema meta = new DataSchema(new Field("id", DataType.IntegerType));
        ShuffleEndpoint first = endpoint(1, (byte) 1);
        ShuffleEndpoint second = endpoint(2, (byte) 2);
        ShuffleEndpoint third = endpoint(3, (byte) 3);
        ShuffleEndpoint safeIdentityOnly = ShuffleEndpoint.muxAuthenticated(
                "127.0.0.1", 7401, UUID.randomUUID(), identity());
        StageCommand command = StageCommand.router(meta, 1, null,
                        new String[]{"id"}, Arrays.asList(first, safeIdentityOnly))
                .withDownTargets(Arrays.asList(
                        ShuffleTarget.socket(first), ShuffleTarget.local("local-a"),
                        ShuffleTarget.socket(second)))
                .withBroadcastGroups(Arrays.asList(
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(second)),
                                new String[]{"id"}, false),
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(third)),
                                new String[]{"id"}, false)));

        List<ShuffleEndpoint> projected =
                WorkerServer.authorizedOutboundEndpoints(command);

        assertEquals(3, projected.size());
        assertSame(first, projected.get(0));
        assertSame(second, projected.get(1));
        assertSame(third, projected.get(2));
    }

    private static ShuffleEndpoint endpoint(int ordinal, byte fill) {
        byte[] secret = new byte[32];
        byte[] cohort = new byte[32];
        Arrays.fill(secret, fill);
        Arrays.fill(cohort, (byte) (fill + 10));
        return ShuffleEndpoint.muxAuthorized(
                "127.0.0.1", 7401, new UUID(101L, ordinal), identity(),
                ShuffleProducerGrant.fromBytes(secret), ordinal, cohort, 1);
    }

    private static WorkerDataIdentity identity() {
        return WorkerDataIdentity.authenticated(
                "destination", "inc-destination",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", new byte[32]);
    }
}
