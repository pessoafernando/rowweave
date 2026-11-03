package io.rowweave.stream.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Worker placement resolves current registry incarnations. */
class RegistryStreamWorkerDirectoryTest {

    @Test
    void snapshotDiscoversWorkersAndTracksReplacement(@TempDir Path root) {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        InProcessRegistry registry = new InProcessRegistry();
        StreamWorkerCapabilityProfile firstProfile = profile(
                "worker-a", "inc-a-1");
        StreamWorkerCapabilityProfile secondProfile = profile(
                "worker-b", "inc-b-1");
        try (StreamRemoteWorkerServer first = worker(
                     firstProfile, root.resolve("first"), security);
             StreamRemoteWorkerServer second = worker(
                     secondProfile, root.resolve("second"), security)) {
            RegistryStreamWorkerDirectory.register(registry,
                    security.advertisedHost(), first.port(), firstProfile,
                    security.securityMetadata());
            RegistryStreamWorkerDirectory.register(registry,
                    security.advertisedHost(), second.port(), secondProfile,
                    security.securityMetadata());
            RegistryStreamWorkerDirectory directory =
                    new RegistryStreamWorkerDirectory(registry, security);

            assertDoesNotThrow(() -> new RemoteStreamSourceSinkRunFactory(
                    root.resolve("single-worker-run"), 1L,
                    "single-worker-master",
                    Collections.singletonList(
                            RemoteStreamWorkerEndpoint.connect(
                                    security.advertisedHost(), first.port(),
                                    security)),
                    root.resolve("single-worker-checkpoint")),
                    "one capable Worker is a legal cluster topology");

            List<RemoteStreamWorkerEndpoint> initial = directory.snapshot();
            assertEquals(Arrays.asList("worker-a", "worker-b"),
                    workerIds(initial));

            StreamWorkerCapabilityProfile replacementProfile = profile(
                    "worker-a", "inc-a-2");
            try (StreamRemoteWorkerServer replacement = worker(
                         replacementProfile, root.resolve("replacement"),
                         security)) {
                RegistryStreamWorkerDirectory.register(registry,
                        security.advertisedHost(), replacement.port(),
                        replacementProfile, security.securityMetadata());
                List<RemoteStreamWorkerEndpoint> replaced =
                        directory.snapshot();
                assertEquals("inc-a-2",
                        replaced.get(0).profile().workerIncarnation());
                assertEquals(Arrays.asList("worker-a", "worker-b"),
                        workerIds(replaced));
            }
        }
    }

    private static StreamRemoteWorkerServer worker(
            StreamWorkerCapabilityProfile profile, Path root,
            ControlSecurityConfig security) {
        return new StreamRemoteWorkerServer(profile,
                Collections.emptyMap(), Collections.emptyMap(),
                reference -> { throw new AssertionError(); },
                root, security, 0);
    }

    private static StreamWorkerCapabilityProfile profile(
            String workerId, String incarnation) {
        return new StreamWorkerCapabilityProfile(workerId, incarnation,
                1, 1, Arrays.asList(
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY),
                2, 8, 4096, 65_536L, 8);
    }

    private static List<String> workerIds(
            List<RemoteStreamWorkerEndpoint> endpoints) {
        java.util.ArrayList<String> result = new java.util.ArrayList<>();
        for (RemoteStreamWorkerEndpoint endpoint : endpoints) {
            result.add(endpoint.profile().workerId());
        }
        return result;
    }
}
