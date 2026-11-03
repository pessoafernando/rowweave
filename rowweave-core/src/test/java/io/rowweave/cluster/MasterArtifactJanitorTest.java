package io.rowweave.cluster;

import io.rowweave.artifact.ArtifactGcOwner;
import io.rowweave.artifact.ArtifactJanitor;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.cluster.registry.InProcessRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterArtifactJanitorTest {

    @Test
    void matchingRepositoryAndRunAuthorityEnableExplicitPassAndFixedLifecycle() {
        InProcessRegistry registry = new InProcessRegistry();
        io.rowweave.monitor.InMemoryJobMonitor monitor =
                new io.rowweave.monitor.InMemoryJobMonitor(10);
        MasterServer master = new MasterServer(
                5_000L, 0, registry, monitor,
                registry.artifactRepository());
        assertFalse(master.artifactJanitorActive());

        ArtifactJanitor.PassResult pass = master.runArtifactRetentionPassForTest(
                new ArtifactGcOwner("fixed-test", 1L));
        assertNotNull(pass);
        assertNotNull(monitor.artifactRetention());
        assertTrue("READY".equals(
                monitor.artifactRetention().referenceStatus()));
        assertTrue("READY".equals(
                monitor.artifactRetention().inventoryStatus()));
        master.startFixedLeadershipServices();
        assertTrue(master.artifactJanitorActive());

        master.close();
        assertFalse(master.artifactJanitorActive());
    }

    @Test
    void electionAcquireAndLossFenceArtifactJanitorLifecycle() {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer master = new MasterServer(
                5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP,
                registry.artifactRepository());
        MasterElection election = new MasterElection(
                registry, new Endpoint("127.0.0.1", master.port()));
        master.bindLeadership(election);
        try {
            assertFalse(master.artifactJanitorActive());
            election.start();
            assertTrue(master.artifactJanitorActive());
            election.close();
            assertFalse(master.artifactJanitorActive());
        } finally {
            election.close();
            master.close();
        }
    }

    @Test
    void mismatchedRepositoryCapabilityNeverStartsDestructiveMaintenance() {
        InProcessRegistry registry = new InProcessRegistry();
        InMemoryArtifactRepository delegate =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        ArtifactRepository missingLifecycleCapability = new ArtifactRepository() {
            @Override public String put(byte[] encodedArtifact) {
                return delegate.put(encodedArtifact);
            }
            @Override public byte[] get(String artifactId) {
                return delegate.get(artifactId);
            }
        };
        try (MasterServer master = new MasterServer(
                5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP,
                missingLifecycleCapability)) {
            master.startFixedLeadershipServices();
            assertFalse(master.artifactJanitorActive());
        } finally {
            delegate.close();
        }
    }
}

