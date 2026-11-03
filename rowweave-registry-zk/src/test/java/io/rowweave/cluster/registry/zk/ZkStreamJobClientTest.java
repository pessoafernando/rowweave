package io.rowweave.cluster.registry.zk;

import io.rowweave.StreamJobClient;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.RegistryRoleElection;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.cluster.StreamClusterRoles;
import io.rowweave.stream.cluster.StreamProductJobMasterServer;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** public ZK Stream client owns leader discovery session. */
class ZkStreamJobClientTest {

    @Test
    void clientDiscoversNamedStreamLeader(@TempDir Path root)
            throws Exception {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry electionRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root,
                             snapshot -> { throw new AssertionError(); },
                             security, 0);
             RegistryRoleElection election = new RegistryRoleElection(
                     electionRegistry,
                     StreamClusterRoles.MASTER_LEADER_ROLE_ID,
                     new Endpoint(security.advertisedHost(), master.port()),
                     security.securityMetadata());
             StreamJobClient client = new ZkStreamJobClient(
                     zk.getConnectString(), security)) {
            election.start();
            StreamControlException missing = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("zk-missing")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    missing.code());
        }
    }
}
