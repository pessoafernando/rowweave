package io.rowweave.stream.api;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** pure role/deployment parsing precedes all resources. */
class StreamClusterConfigTest {

    @Test
    void combinedRoleParsesWithoutOpeningRuntimeResources() {
        Properties properties = base();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "combined-worker");
        properties.setProperty("rowweave.stream.master.port", "62001");
        properties.setProperty("rowweave.stream.worker.port", "62002");

        StreamClusterConfig config =
                StreamClusterConfig.fromProperties(properties);

        assertTrue(config.masterEnabled());
        assertTrue(config.workerEnabled());
        assertEquals("combined-worker", config.workerId());
        assertEquals(62001, config.masterPort());
        assertEquals(62002, config.workerPort());
        assertEquals(Paths.get(properties.getProperty(
                "rowweave.stream.state.dir")).toAbsolutePath().normalize(),
                config.stateRoot());
        assertFalse(config.zkEnabled());
    }

    @Test
    void masterOnlyDirectModeRequiresAtLeastOneWorkerEndpoint() {
        Properties properties = base();
        properties.setProperty("rowweave.stream.master.enable", "true");
        assertThrows(IllegalArgumentException.class,
                () -> StreamClusterConfig.fromProperties(properties));

        properties.setProperty("rowweave.stream.master.workers",
                "127.0.0.1:62011,localhost:62012");
        StreamClusterConfig config =
                StreamClusterConfig.fromProperties(properties);
        assertEquals(2, config.fixedWorkers().size());
        assertEquals(62012, config.fixedWorkers().get(1).port());
        assertEquals(7L, config.javaThreadUpperBound(),
                "Master listener, recovery, checkpoint and four control "
                        + "request threads must be in the process envelope");
    }

    @Test
    void zkModeAllowsIndependentRolesAndRequiresConnectString() {
        Properties properties = base();
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "zk-worker");
        properties.setProperty("rowweave.registry", "zk");
        assertThrows(IllegalArgumentException.class,
                () -> StreamClusterConfig.fromProperties(properties));

        properties.setProperty("rowweave.zk.connectString", "zk-a:2181");
        StreamClusterConfig config =
                StreamClusterConfig.fromProperties(properties);
        assertTrue(config.zkEnabled());
        assertEquals("zk-a:2181", config.zkConnectString());
    }

    @Test
    void zkMasterRequiresSharedArtifactDirectory() {
        Properties properties = base();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.registry", "zk");
        properties.setProperty("rowweave.zk.connectString", "zk-a:2181");
        assertThrows(IllegalArgumentException.class,
                () -> StreamClusterConfig.fromProperties(properties));

        properties.setProperty("rowweave.stream.master.artifact.dir",
                Paths.get("target", "stream-shared-artifacts").toString());
        StreamClusterConfig config =
                StreamClusterConfig.fromProperties(properties);
        assertEquals(Paths.get(properties.getProperty(
                "rowweave.stream.master.artifact.dir"))
                .toAbsolutePath().normalize(), config.artifactRoot());
    }

    @Test
    void missingRoleAndWorkerIdentityFailPurely() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamClusterConfig.fromProperties(base()));
        Properties worker = base();
        worker.setProperty("rowweave.stream.worker.enable", "true");
        assertThrows(IllegalArgumentException.class,
                () -> StreamClusterConfig.fromProperties(worker));
    }

    @Test
    void workerRoleParsesTheCompleteFiniteResourceEnvelope() {
        Properties properties = base();
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "bounded-worker");
        properties.setProperty("rowweave.stream.worker.taskSlots", "7");
        properties.setProperty("rowweave.stream.worker.channelCapacity", "19");
        properties.setProperty("rowweave.stream.worker.maximumFrameBytes", "4096");
        properties.setProperty("rowweave.stream.worker.maximumResidentBytes", "8192");
        properties.setProperty("rowweave.stream.worker.maximumThreads", "23");
        properties.setProperty("rowweave.stream.worker.maximumTimerCount", "29");
        properties.setProperty("rowweave.stream.worker.maximumWindowCount", "31");
        properties.setProperty("rowweave.stream.worker.maximumCheckpointBytes", "32768");
        properties.setProperty("rowweave.stream.worker.maximumSpillDiskBytes", "65536");
        properties.setProperty("rowweave.stream.worker.maximumFileDescriptorCount", "37");

        StreamClusterConfig config = StreamClusterConfig
                .fromProperties(properties);
        StreamWorkerConfig worker = config.workerConfig();

        assertEquals(7, worker.taskSlots());
        assertEquals(19, worker.channelCapacity());
        assertEquals(4096, worker.maximumFrameBytes());
        assertEquals(8192L, worker.maximumResidentBytes());
        assertEquals(23, worker.maximumThreads());
        assertEquals(29, worker.maximumTimerCount());
        assertEquals(31, worker.maximumWindowCount());
        assertEquals(32768L, worker.maximumCheckpointBytes());
        assertEquals(65536L, worker.maximumSpillDiskBytes());
        assertEquals(37, worker.maximumFileDescriptorCount());
        assertEquals(29L, config.javaThreadUpperBound(),
                "task/lane, bounded control pool and source discovery threads must be bounded");

        properties.setProperty("rowweave.deployment.count", "1");
        assertEquals(30L, StreamClusterConfig.fromProperties(properties)
                        .javaThreadUpperBound(),
                "the signed deployment source probe must be in the process envelope");

        properties.setProperty("rowweave.stream.worker.maximumCheckpointBytes",
                "-1");
        assertThrows(IllegalArgumentException.class,
                () -> StreamClusterConfig.fromProperties(properties));
    }

    private static Properties base() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.state.dir",
                Paths.get("target", "stream-cluster-config").toString());
        return properties;
    }
}
