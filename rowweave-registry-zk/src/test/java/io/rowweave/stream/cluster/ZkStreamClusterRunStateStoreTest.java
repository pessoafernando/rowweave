package io.rowweave.stream.cluster;

import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** independent ZK sessions share run and term authority. */
class ZkStreamClusterRunStateStoreTest {

    @Test
    void independentSessionsShareRunAndFenceOldMaster() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry replacementRegistry =
                     new ZkServiceRegistry(zk.getConnectString())) {
            StreamClusterRunStateStore firstStore =
                    ((StreamClusterRunStateStoreProvider) firstRegistry)
                            .streamClusterRunStateStore();
            StreamClusterRunStateStore replacementStore =
                    ((StreamClusterRunStateStoreProvider) replacementRegistry)
                            .streamClusterRunStateStore();
            FileStreamClusterRunAuthority first =
                    new FileStreamClusterRunAuthority(firstStore);
            FileStreamClusterRunAuthority replacement =
                    new FileStreamClusterRunAuthority(replacementStore);

            long firstTerm = firstStore.nextCoordinatorTerm();
            long replacementTerm = replacementStore.nextCoordinatorTerm();
            assertTrue(replacementTerm > firstTerm);
            first.create(StreamClusterRunState.initial(
                    firstTerm, "master-first", "zk-run", "attempt-1",
                    1L, 1L, 1L, placement()));

            StreamClusterRunState takenOver = replacement.replaceCoordinator(
                    "zk-run", firstTerm, "master-first",
                    replacementTerm, "master-replacement");
            assertEquals(takenOver, first.current("zk-run"));
            StreamClusterProtocolException stale = assertThrows(
                    StreamClusterProtocolException.class,
                    () -> first.replaceCoordinator(
                            "zk-run", firstTerm, "master-first",
                            replacementTerm + 1L, "stale-master"));
            assertEquals(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    stale.code());
        }
    }

    private static StreamWorkerPlacement placement() {
        return new StreamWorkerPlacement(Arrays.asList(
                new StreamWorkerPlacement.Assignment(
                        0, "source", 0, "worker-a", "inc-a"),
                new StreamWorkerPlacement.Assignment(
                        1, "sink", 0, "worker-b", "inc-b")));
    }
}
