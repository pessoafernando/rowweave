package io.rowweave.stream.cluster;

import io.rowweave.StreamJobClient;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Master, Worker and Client share one secured transport path. */
class StreamSecureControlTransportIntegrationTest {

    @Test
    void masterAndClientUseExplicitSecuritySnapshot(@TempDir Path root) {
        ControlSecurityConfig security = loopback();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root,
                             artifact -> { throw new AssertionError(); },
                             security, 0);
             StreamJobClient client = new ClusterStreamJobClient(
                     security.advertisedHost(), master.port(), security)) {
            StreamControlException missing = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("missing-job")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    missing.code());
        }
    }

    @Test
    void masterAndWorkerUseExplicitSecuritySnapshot(@TempDir Path root) {
        ControlSecurityConfig security = loopback();
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "worker-secure", "inc-secure", 1, 1,
                        Arrays.asList(
                                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile.DATA_CAPABILITY),
                        1, 4, 4096, 65_536L, 8);
        try (StreamRemoteWorkerServer worker =
                     new StreamRemoteWorkerServer(profile,
                             Collections.emptyMap(), Collections.emptyMap(),
                             reference -> { throw new AssertionError(); },
                             root, security, 0)) {
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            security.advertisedHost(), worker.port(), security);
            assertEquals(profile.workerId(), endpoint.profile().workerId());
            assertEquals(profile.workerIncarnation(),
                    endpoint.profile().workerIncarnation());
        }
    }

    @Test
    void standbyMasterRejectsBeforeAuthorityOrRunFactoryMutation(
            @TempDir Path root) {
        ControlSecurityConfig security = loopback();
        AtomicBoolean leader = new AtomicBoolean();
        java.util.concurrent.atomic.AtomicInteger creates =
                new java.util.concurrent.atomic.AtomicInteger();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root,
                             snapshot -> {
                                 creates.incrementAndGet();
                                 throw new AssertionError();
                             }, security, 0, leader::get);
             StreamJobClient client = new ClusterStreamJobClient(
                     security.advertisedHost(), master.port(), security)) {
            StreamControlException standby = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("standby-job")));
            assertEquals(StreamControlException.Code.TRANSPORT_FAILURE,
                    standby.code());
            assertEquals(0, creates.get());

            leader.set(true);
            StreamControlException active = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("active-missing")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    active.code());
            assertEquals(0, creates.get());
        }
    }

    @Test
    void masterCanUseInjectedSharedProductAuthorityStore(
            @TempDir Path root) {
        ControlSecurityConfig security = loopback();
        InMemoryStreamProductJobStateStore store =
                new InMemoryStreamProductJobStateStore();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(store,
                             snapshot -> { throw new AssertionError(); },
                             security, 0, () -> true);
             StreamJobClient client = new ClusterStreamJobClient(
                     security.advertisedHost(), master.port(), security)) {
            StreamControlException missing = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("shared-missing")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    missing.code());
        }
    }

    private static ControlSecurityConfig loopback() {
        return ControlSecurityConfig.fromProperties(new Properties());
    }
}
