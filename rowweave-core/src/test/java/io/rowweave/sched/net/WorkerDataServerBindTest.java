package io.rowweave.sched.net;

import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The data server must bind the exact address the control plane already resolved, rather than a wildcard. */
class WorkerDataServerBindTest {

    @Test
    void loopbackDataServerBindsExactControlAddress() {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY, "127.0.0.1");
        ControlSecurityConfig control = ControlSecurityConfig.fromProperties(properties);
        DataPlaneConfig dataPlane = DataPlaneConfig.fromProperties(
                properties, control, Runtime.getRuntime().maxMemory());

        try (WorkerDataServer server = new WorkerDataServer(
                new DataInboundRegistry(), 1_000L, 0, dataPlane)) {
            InetSocketAddress bound = server.boundAddress();
            assertEquals(dataPlane.bindAddress(), bound.getAddress());
            assertTrue(bound.getAddress().isLoopbackAddress());
        }
    }
}
