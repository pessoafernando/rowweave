package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stream and Batch use isolated named HA roles. */
class RegistryRoleElectionTest {

    @Test
    void oneNamedRoleFailsOverWithoutCollidingWithAnotherRole() {
        InProcessRegistry registry = new InProcessRegistry();
        String streamRole = "__rowweave_stream_master_leader__";
        RegistryRoleElection first = new RegistryRoleElection(registry,
                streamRole, new Endpoint("127.0.0.1", 62001), null);
        RegistryRoleElection second = new RegistryRoleElection(registry,
                streamRole, new Endpoint("127.0.0.1", 62002), null);
        RegistryRoleElection batch = new RegistryRoleElection(registry,
                MasterServer.MASTER_LEADER_ROLE_ID,
                new Endpoint("127.0.0.1", 62003), null);
        AtomicInteger secondGains = new AtomicInteger();
        second.addLeadershipListener(active -> {
            if (active) secondGains.incrementAndGet();
        });

        first.start();
        second.start();
        batch.start();
        assertTrue(first.isLeader());
        assertFalse(second.isLeader());
        assertTrue(batch.isLeader());
        assertEquals(new Endpoint("127.0.0.1", 62001),
                new RegistryRoleLocator(registry, streamRole, null)
                        .locateLeader());

        first.close();
        assertTrue(second.isLeader());
        assertEquals(1, secondGains.get());
        assertEquals(new Endpoint("127.0.0.1", 62002),
                new RegistryRoleLocator(registry, streamRole, null)
                        .locateLeader());
        second.close();
        batch.close();
    }

    @Test
    void locatorRejectsDeploymentOrAuthzIdentityMismatch() {
        InProcessRegistry registry = new InProcessRegistry();
        String role = "__rowweave_stream_master_leader__";
        RpcSecurityMetadata actual = RpcSecurityMetadata.of(
                repeat('a'), repeat('b'));
        RegistryRoleElection election = new RegistryRoleElection(registry,
                role, new Endpoint("127.0.0.1", 62101), actual);
        election.start();
        try {
            assertThrows(SecurityException.class,
                    () -> new RegistryRoleLocator(registry, role,
                            RpcSecurityMetadata.of(repeat('c'), repeat('b')))
                            .locateLeader());
        } finally {
            election.close();
        }
    }

    private static String repeat(char value) {
        char[] text = new char[64];
        java.util.Arrays.fill(text, value);
        return new String(text);
    }
}
