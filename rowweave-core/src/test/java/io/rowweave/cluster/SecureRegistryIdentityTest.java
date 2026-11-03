package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** leader discovery never crosses deployment/authz set identity. */
class SecureRegistryIdentityTest {
    private static final RpcSecurityMetadata LOCAL = RpcSecurityMetadata.of(
            repeat('a', 64), repeat('b', 64));

    @Test
    void electionPublishesIdentityAndMatchingLocatorReturnsLeader() {
        InProcessRegistry registry = new InProcessRegistry();
        MasterElection election = new MasterElection(
                registry, new Endpoint("127.0.0.1", 9000), LOCAL);
        election.start();
        try {
            RegistryWorker record = registry.worker(MasterServer.MASTER_LEADER_ROLE_ID);
            assertEquals(LOCAL, record.securityMetadata());
            Endpoint endpoint = new RegistryMasterLocator(registry, LOCAL).locateLeader();
            assertEquals(9000, endpoint.port());
        } finally {
            election.close();
        }
    }

    @Test
    void missingOrMismatchedLeaderIdentityFailsBeforeEndpointUse() {
        InProcessRegistry registry = new InProcessRegistry();
        registry.register(new RegistryWorker(MasterServer.MASTER_LEADER_ROLE_ID,
                "127.0.0.1", 9000, 1));
        assertThrows(SecurityException.class,
                () -> new RegistryMasterLocator(registry, LOCAL).locateLeader());

        registry.report(new RegistryWorker(MasterServer.MASTER_LEADER_ROLE_ID,
                "127.0.0.1", 9000, 1, -1, null,
                RpcSecurityMetadata.of(repeat('c', 64), repeat('b', 64))));
        assertThrows(SecurityException.class,
                () -> new RegistryMasterLocator(registry, LOCAL).locateLeader());
    }

    @Test
    void standbyCannotJoinAnActiveLeaderFromAnotherSecuritySet() {
        InProcessRegistry registry = new InProcessRegistry();
        registry.register(new RegistryWorker(MasterServer.MASTER_LEADER_ROLE_ID,
                "127.0.0.1", 9000, 1, -1, null,
                RpcSecurityMetadata.of(repeat('c', 64), repeat('b', 64))));
        MasterElection standby = new MasterElection(
                registry, new Endpoint("127.0.0.1", 9001), LOCAL);
        assertThrows(SecurityException.class, standby::start);
        standby.close();
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
