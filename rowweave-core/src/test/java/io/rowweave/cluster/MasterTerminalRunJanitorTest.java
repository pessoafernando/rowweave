package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleCatalogKernel;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.TerminalRunJanitor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterTerminalRunJanitorTest {

    @Test
    void explicitPassUsesRegistryCatalogAndRespectsAuthorityTime() {
        InProcessRegistry registry = new InProcessRegistry();
        io.rowweave.monitor.InMemoryJobMonitor monitor =
                new io.rowweave.monitor.InMemoryJobMonitor(10);
        RunStateStore store = registry.runStateStore();
        String runId = shardZeroRunId();
        RunDecision created = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.cancelReadyRun(
                        "cancel", runId, created.snapshot().revision(), 2L,
                        "CANCELED", "terminal")).code());

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry, monitor)) {
            TerminalRunJanitor.PassResult pass =
                    master.runTerminalRetentionPassForTest(
                            new RetentionOwner("test-master", 1L));

            assertEquals(1, pass.scannedRefs());
            assertEquals(1, pass.notDueRuns());
            assertEquals(0, pass.purgedRuns());
            assertNotNull(store.get(runId));
            assertNotNull(monitor.runRetention());
            assertEquals(1, monitor.runRetention().backlog());
        }
    }

    @Test
    void fixedLeadershipServicesStartExplicitlyAndCloseWithMaster() {
        MasterServer master = new MasterServer(
                5_000L, 0, new InProcessRegistry());
        assertFalse(master.terminalRunJanitorActive());

        master.startFixedLeadershipServices();

        assertTrue(master.terminalRunJanitorActive());
        master.close();
        assertFalse(master.terminalRunJanitorActive());
    }

    @Test
    void electionAcquireAndLossOwnTheJanitorLifecycle() {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer master = new MasterServer(5_000L, 0, registry);
        MasterElection election = new MasterElection(
                registry, new Endpoint("127.0.0.1", master.port()));
        master.bindLeadership(election);
        try {
            assertFalse(master.terminalRunJanitorActive());

            election.start();
            assertTrue(master.terminalRunJanitorActive());

            election.close();
            assertFalse(master.terminalRunJanitorActive());
        } finally {
            election.close();
            master.close();
        }
    }

    private static String shardZeroRunId() {
        for (int value = 0; ; value++) {
            String runId = "master-janitor-" + value;
            if (RunLifecycleCatalogKernel.shard(
                    RunLifecycleCatalogKernel.digestHex(runId)) == 0) {
                return runId;
            }
        }
    }
}

