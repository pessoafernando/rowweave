package io.rowweave.cluster.registry.zk;

import io.rowweave.commit.ManifestCommitAuthority;
import io.rowweave.commit.ManifestCommitAuthorityProvider;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ZooKeeper keeps a persistent generation number and a session-scoped domain writer lease. */
class ZkManifestCommitAuthorityTest {

    @Test
    void crossSessionGenerationIsPersistentAndLeaseFencesActiveWriter(@TempDir Path domain)
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(zk.getConnectString())) {
            ManifestCommitAuthority first =
                    ((ManifestCommitAuthorityProvider) firstRegistry).manifestCommitAuthority();
            ManifestCommitAuthority second =
                    ((ManifestCommitAuthorityProvider) secondRegistry).manifestCommitAuthority();

            ManifestCommitAuthority.Lease lease = first.tryAcquire(domain, "attempt-a");
            assertNotNull(lease);
            assertNull(second.tryAcquire(domain, "attempt-b"));
            long generationA = first.reserve(domain, "attempt-a");
            assertEquals(generationA, second.find(domain, "attempt-a"));
            assertEquals(generationA, second.reserve(domain, "attempt-a"));

            lease.close();
            ManifestCommitAuthority.Lease nextLease = second.tryAcquire(domain, "attempt-b");
            assertNotNull(nextLease);
            long generationB = second.reserve(domain, "attempt-b");
            assertTrue(generationB > generationA);
            nextLease.close();
        }
    }

    @Test
    void registrySessionDeathReleasesWriterLease(@TempDir Path domain) throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry observer = new ZkServiceRegistry(zk.getConnectString())) {
            ZkServiceRegistry owner = new ZkServiceRegistry(zk.getConnectString());
            ManifestCommitAuthority owned = owner.manifestCommitAuthority();
            assertNotNull(owned.tryAcquire(domain, "attempt-dead"));
            owner.close();

            ManifestCommitAuthority observerAuthority = observer.manifestCommitAuthority();
            ManifestCommitAuthority.Lease recovered = null;
            long deadline = System.currentTimeMillis() + 5_000L;
            while (recovered == null && System.currentTimeMillis() < deadline) {
                recovered = observerAuthority.tryAcquire(domain, "attempt-recovery");
                if (recovered == null) {
                    Thread.sleep(20L);
                }
            }
            assertNotNull(recovered, "ZK session close must release EPHEMERAL domain lease");
            recovered.close();
        }
    }
}
