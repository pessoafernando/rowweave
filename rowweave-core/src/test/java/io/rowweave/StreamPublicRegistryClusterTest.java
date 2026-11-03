package io.rowweave;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.cluster.RegistryStreamWorkerDirectory;
import io.rowweave.stream.cluster.RemoteStreamWorkerEndpoint;
import io.rowweave.stream.cluster.StreamWorkerCapabilityProfile;
import io.rowweave.stream.contract.PortableStreamPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** public Workers self-register; Master consumes a directory. */
class StreamPublicRegistryClusterTest {

    @Test
    void publicMasterDiscoversSelfRegisteredWorkerIncarnations(
            @TempDir Path root) {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        InProcessRegistry registry = new InProcessRegistry();
        try (StreamWorkerRuntime first = worker(
                     "registry-a", root.resolve("worker-a"), security);
             StreamWorkerRuntime second = worker(
                     "registry-b", root.resolve("worker-b"), security);
             StreamWorkerRegistryLease firstLease = first.register(registry);
             StreamWorkerRegistryLease secondLease = second.register(registry);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     root.resolve("master"),
                     new RegistryStreamWorkerDirectory(registry, security),
                     security, 0);
             StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                     master.host(), master.port(), security)) {
            List<RemoteStreamWorkerEndpoint> discovered =
                    new RegistryStreamWorkerDirectory(
                            registry, security).snapshot();
            assertEquals(2, discovered.size());
            assertEquals(3,
                    discovered.get(0).profile().controlProtocolVersion());
            assertEquals(true, discovered.get(0).profile().capabilities()
                    .contains(StreamWorkerCapabilityProfile
                            .PROGRESS_CAPABILITY));
            assertEquals(true, discovered.get(0).profile().capabilities()
                    .contains(StreamWorkerCapabilityProfile
                            .CHECKPOINT_TRANSFER_CAPABILITY));
            assertEquals(true, discovered.get(0).profile().capabilities()
                    .contains(PortableStreamPlan
                            .DYNAMIC_SPLIT_DISCOVERY_CAPABILITY));
            assertEquals(true, discovered.get(0).profile().capabilities()
                    .contains(PortableStreamPlan
                            .RETRACT_CHANGELOG_CAPABILITY));
            StreamControlException missing = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("registry-missing")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    missing.code());
        }
        assertEquals(0, new RegistryStreamWorkerDirectory(
                registry, security).snapshot().size());
    }

    private static StreamWorkerRuntime worker(String id, Path root,
            ControlSecurityConfig security) {
        return RowWeave.startStreamWorker(StreamWorkerConfig.defaults(id, root)
                        .withControlEndpoint(security, 0),
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
    }
}
