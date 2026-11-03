package io.rowweave;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamWorkerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** secured cluster deployment is available from public SDKs. */
class StreamPublicSecureClusterConfigTest {

    @Test
    void explicitEndpointSecurityFlowsThroughPublicWorkerMasterAndClient(
            @TempDir Path root) {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        StreamWorkerConfig firstConfig = StreamWorkerConfig.defaults(
                "worker-a", root.resolve("worker-a"))
                .withControlEndpoint(security, 0);
        StreamWorkerConfig secondConfig = StreamWorkerConfig.defaults(
                "worker-b", root.resolve("worker-b"))
                .withControlEndpoint(security, 0);

        try (StreamWorkerRuntime first = RowWeave.startStreamWorker(firstConfig,
                     Collections.emptyList(), Collections.emptyList(),
                     Collections.emptyList(), Collections.emptyList());
             StreamWorkerRuntime second = RowWeave.startStreamWorker(secondConfig,
                     Collections.emptyList(), Collections.emptyList(),
                     Collections.emptyList(), Collections.emptyList());
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     root.resolve("master"),
                     java.util.Arrays.asList(
                             first.endpoint(), second.endpoint()),
                     security, 0);
             StreamJobClient client =
                     RowWeave.createRealClusterStreamJobClient(
                             master.host(), master.port(), security)) {
            assertEquals(security.advertisedHost(), first.endpoint().host());
            assertEquals(security.advertisedHost(), master.host());
            StreamControlException missing = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("missing-public-job")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    missing.code());
        }
    }
}
