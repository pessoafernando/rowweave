package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.MasterServer;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierStore;
import io.rowweave.incremental.IncrementalFrontierStoreProvider;
import io.rowweave.incremental.IncrementalFrontierView;
import io.rowweave.incremental.IncrementalFrontierViewCodec;
import io.rowweave.incremental.IncrementalInitializeRequest;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkIncrementalFrontierQueryTest {

    @Test
    void remoteClientReturnsOnlyTheBoundedRedactedFrontierView() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(zk.getConnectString());
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            IncrementalFrontierStore authority =
                    ((IncrementalFrontierStoreProvider) registry).incrementalFrontierStore();
            IncrementalInitializeRequest request = new IncrementalInitializeRequest(
                    "public-feed", 1L, digest("definition-secret"),
                    digest("source-secret"), digest("frontier-contract"),
                    IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    authority.initialize("initialize-public-feed", request).code());
            master.bindLeadership(election);
            election.start();

            try (ZkRemoteJobClient client = new ZkRemoteJobClient(zk.getConnectString())) {
                IncrementalFrontierView view =
                        client.queryIncrementalFrontier("public-feed");
                assertEquals("public-feed", view.feedId());
                assertEquals(1L, view.feedIncarnation());
                assertEquals(0L, view.revision());
                assertEquals(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                        view.frontierKind());
                assertEquals("READY", view.state());
                assertNull(view.activeRunId());
                assertEquals(16, view.definitionDigestPrefix().length());
                assertEquals(16, view.committedFrontierDigestPrefix().length());
                assertTrue(IncrementalFrontierViewCodec.encode(view).length <= 4096);
                assertNull(client.queryIncrementalFrontier("absent-feed"));
            }
        }
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
    }
}
