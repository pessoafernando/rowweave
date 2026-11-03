package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlEndpointConfig;
import io.rowweave.cluster.rpc.RpcAdmissionConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** role facilities and assignment concurrency have one exact thread ledger. */
class ProcessThreadCapacityModelTest {

    @Test
    void masterWorkerAndCombinedFacilitiesAreAdditive() {
        Properties properties = baseProperties();
        DataPlaneConfig dataPlane = DataPlaneConfig.fromProperties(
                properties, ControlEndpointConfig.fromProperties(properties),
                4L * 1024L * 1024L * 1024L, 4);
        RpcAdmissionConfig rpc = RpcAdmissionConfig.fromProperties(properties);

        assertEquals(19L, ProcessThreadCapacityModel.estimate(
                properties, true, false, false, rpc, null,
                0, 0, 4).totalThreads());
        assertEquals(43L, ProcessThreadCapacityModel.estimate(
                properties, false, true, false, rpc, dataPlane,
                2, 10, 4).totalThreads());
        assertEquals(59L, ProcessThreadCapacityModel.estimate(
                properties, true, true, false, rpc, dataPlane,
                2, 10, 4).totalThreads());
    }

    @Test
    void mtlsZkSessionsAndDeploymentProbeAreExplicitReserves() {
        Properties properties = baseProperties();
        properties.setProperty("rowweave.control.securityProfile", "MTLS");
        properties.setProperty("rowweave.control.host", "127.0.0.1");
        properties.setProperty("rowweave.deployment.count", "1");
        DataPlaneConfig dataPlane = DataPlaneConfig.fromProperties(
                properties, ControlEndpointConfig.fromProperties(properties),
                4L * 1024L * 1024L * 1024L, 4);

        ProcessThreadCapacityModel.Snapshot snapshot =
                ProcessThreadCapacityModel.estimate(
                        properties, true, true, true,
                        RpcAdmissionConfig.fromProperties(properties), dataPlane,
                        2, 10, 4);

        assertEquals(6L, snapshot.workerHandshakeThreads());
        assertEquals(32L, snapshot.zkVendorThreads());
        assertEquals(1L, snapshot.deploymentProbeThreads());
        assertEquals(98L, snapshot.totalThreads());
    }

    private static Properties baseProperties() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.control.maxConnections", "2");
        properties.setProperty("rowweave.cluster.assign.parallelism", "3");
        properties.setProperty("rowweave.monitor.enable", "true");
        properties.setProperty("rowweave.monitor.ioThreads", "2");
        properties.setProperty("rowweave.shuffle.ioThreads", "2");
        properties.setProperty("rowweave.shuffle.maxIngressHandlers", "3");
        properties.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "4");
        return properties;
    }
}
