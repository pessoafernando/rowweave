package io.rowweave.stream.cluster;

import io.rowweave.StreamJobClient;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.RegistryRoleElection;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** every HA client call resolves the active Stream leader. */
class StreamRegistryHaClientTest {

    @Test
    void clientFollowsNamedStreamLeaderAfterFailover(@TempDir Path root) {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        InProcessRegistry registry = new InProcessRegistry();
        RegistryRoleElection[] elections = new RegistryRoleElection[2];
        try (StreamProductJobMasterServer first =
                     new StreamProductJobMasterServer(root.resolve("first"),
                             snapshot -> { throw new AssertionError(); },
                             security, 0,
                             () -> elections[0] != null
                                     && elections[0].isLeader());
             StreamProductJobMasterServer second =
                     new StreamProductJobMasterServer(root.resolve("second"),
                             snapshot -> { throw new AssertionError(); },
                             security, 0,
                             () -> elections[1] != null
                                     && elections[1].isLeader())) {
            elections[0] = election(registry, security, first.port());
            elections[1] = election(registry, security, second.port());
            elections[0].start();
            elections[1].start();
            try (StreamJobClient client = new ClusterStreamJobClient(
                    registry, security)) {
                assertMissing(client, "before-failover");
                elections[0].close();
                assertTrue(elections[1].isLeader());
                assertMissing(client, "after-failover");
            }
        } finally {
            close(elections[1]);
            close(elections[0]);
        }
    }

    private static RegistryRoleElection election(InProcessRegistry registry,
            ControlSecurityConfig security, int port) {
        return new RegistryRoleElection(registry,
                StreamClusterRoles.MASTER_LEADER_ROLE_ID,
                new Endpoint(security.advertisedHost(), port),
                security.securityMetadata());
    }

    private static void assertMissing(StreamJobClient client, String jobId) {
        StreamControlException missing = assertThrows(
                StreamControlException.class,
                () -> client.attach(StreamJobId.of(jobId)));
        assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                missing.code());
    }

    private static void close(RegistryRoleElection election) {
        if (election != null) election.close();
    }
}
