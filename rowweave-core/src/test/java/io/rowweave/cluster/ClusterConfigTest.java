package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.cluster.rpc.ControlSecurityProfile;
import org.junit.jupiter.api.Test;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Properties;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ClusterConfigTest {

    @Test
    void rpcServerFixedPort() throws Exception {
        int freePort = freeNonEphemeralPort();
        try (RpcServer s = new RpcServer(req -> RpcMessage.ack(), freePort)) {
            assertEquals(freePort, s.port(), "a fixed port is bound");
        }
        try (RpcServer s0 = new RpcServer(req -> RpcMessage.ack())) {
            assertTrue(s0.port() > 0, "a random port resolves above 0");
        }
    }

    private static int freeNonEphemeralPort() throws Exception {
        for (int port = 20_000; port < 30_000; port++) {
            try (ServerSocket probe = new ServerSocket()) {
                probe.bind(new InetSocketAddress(
                        InetAddress.getLoopbackAddress(), port));
                return port;
            } catch (BindException occupied) {
                // Try the next fixed service port outside the Darwin ephemeral range.
            }
        }
        throw new IllegalStateException(
                "no free non-ephemeral loopback port for fixed-port test");
    }

    @Test
    void configDefaults() {
        ClusterConfig c = ClusterConfig.fromProperties(new Properties());
        assertFalse(c.masterEnabled());
        assertFalse(c.workerEnabled());
        assertEquals(128, c.workerSlots(), "default slots=128 (production heap at least 2G; matches WorkerConfig.DEFAULT_SLOTS)");
        assertEquals(0, c.masterPort(), "default 0 means a random port");
        assertEquals(0L, c.memoryBytes(), "default 0 means unset (the pool default fraction applies)");
        assertEquals(5000L, c.heartbeatStalenessMs());
        assertNull(c.masterArtifactDirectory());
        assertEquals(ControlSecurityProfile.LOOPBACK, c.controlProfile());
        assertEquals("127.0.0.1", c.controlEndpoint().advertisedHost());
        assertEquals(256, c.rpcAdmission().maxConnections());
    }

    @Test
    void configFromProperties() {
        Properties p = new Properties();
        p.setProperty("rowweave.master.enable", "true");
        p.setProperty("rowweave.worker.enable", "true");
        p.setProperty("rowweave.master.port", "19000");
        p.setProperty("rowweave.worker.master.host", "10.0.0.1");
        p.setProperty("rowweave.worker.master.port", "19000");
        p.setProperty("rowweave.worker.slots", "8");
        p.setProperty("rowweave.memory.bytes", "536870912"); // 512MB
        p.setProperty("rowweave.heartbeat.staleness.ms", "3000");
        p.setProperty("rowweave.master.artifact.dir", " /shared/rowweave-artifacts ");

        ClusterConfig c = ClusterConfig.fromProperties(p);
        assertTrue(c.masterEnabled());
        assertTrue(c.workerEnabled());
        assertEquals(19000, c.masterPort());
        assertEquals("10.0.0.1", c.masterHost());
        assertEquals(19000, c.workerMasterPort());
        assertEquals(8, c.workerSlots(), "threads/parallelism");
        assertEquals(536870912L, c.memoryBytes(), "managed memory total");
        assertEquals(3000L, c.heartbeatStalenessMs());
        assertEquals("/shared/rowweave-artifacts", c.masterArtifactDirectory());
    }

    @Test
    void invalidControlEndpointFailsBeforeClusterRolesCanStart() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.master.enable", "true");
        properties.setProperty("rowweave.control.securityProfile", "LOOPBACK");
        properties.setProperty("rowweave.control.host", "0.0.0.0");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusterConfig.fromProperties(properties));
        assertTrue(failure.getMessage().contains("LOOPBACK"), failure.getMessage());
    }

    @Test
    void clusterMainBuildsConfiguredFileRepositoryAndLeavesMissingConfigExplicit(
            @org.junit.jupiter.api.io.TempDir Path temporary) {
        assertNull(ClusterMain.masterArtifactRepository(
                ClusterConfig.fromProperties(new Properties())));

        Properties configured = new Properties();
        Path root = temporary.resolve("configured-artifacts");
        configured.setProperty("rowweave.master.artifact.dir", root.toString());
        io.rowweave.artifact.ArtifactRepository repository =
                ClusterMain.masterArtifactRepository(ClusterConfig.fromProperties(configured));
        assertTrue(repository instanceof io.rowweave.artifact.FileArtifactRepository);
    }
}

