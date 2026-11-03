package io.rowweave;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.cluster.RegistryStreamWorkerDirectory;
import io.rowweave.stream.cluster.InMemoryStreamProductJobStateStore;
import io.rowweave.stream.cluster.StreamClusterRunStateStore;
import io.rowweave.stream.cluster.StreamClusterRunStateStoreProvider;
import io.rowweave.stream.cluster.StreamClusterRoles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public Stream Masters bind admission to named election. */
class StreamMasterRuntimeHaTest {

    @Test
    void standbyTakesLeadershipWhenActiveMasterCloses(@TempDir Path root) {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        InProcessRegistry registry = new InProcessRegistry();
        RegistryStreamWorkerDirectory workers =
                new RegistryStreamWorkerDirectory(registry, security);
        InMemoryStreamProductJobStateStore productJobs =
                new InMemoryStreamProductJobStateStore();
        try (StreamMasterRuntime first = RowWeave.startHaStreamMaster(
                     root.resolve("first"), workers, security, 0, registry,
                     productJobs);
             StreamMasterRuntime second = RowWeave.startHaStreamMaster(
                     root.resolve("second"), workers, security, 0, registry,
                     productJobs);
             StreamJobClient client = RowWeave.createRegistryStreamJobClient(
                     registry, security)) {
            StreamClusterRunStateStore runAuthority =
                    ((StreamClusterRunStateStoreProvider) registry)
                            .streamClusterRunStateStore();
            assertTrue(first.isLeader());
            assertFalse(second.isLeader());
            assertEquals(3L, runAuthority.nextCoordinatorTerm(),
                    "both Masters must reserve terms from shared authority");
            assertMissing(client, "first-leader");

            first.close();

            assertTrue(second.isLeader());
            assertMissing(client, "second-leader");

            registry.deregister(StreamClusterRoles.MASTER_LEADER_ROLE_ID);
            assertTrue(second.isLeader());
            assertEquals(5L, runAuthority.nextCoordinatorTerm(),
                    "a process that regains leadership must advance its term");
        }
    }

    private static void assertMissing(StreamJobClient client, String jobId) {
        StreamControlException missing = assertThrows(
                StreamControlException.class,
                () -> client.attach(StreamJobId.of(jobId)));
        assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                missing.code());
    }
}
