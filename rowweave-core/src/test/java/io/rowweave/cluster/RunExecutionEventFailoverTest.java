package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunStateStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("multiprocess")
class RunExecutionEventFailoverTest {

    @Test
    void replacementLeaderContinuesAfterCursorWithoutReplayDuplicates() {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        RunDecision created = store.apply(RunCommand.createRun(
                "create", "run-ha-events", "definition", "artifact", 1, 0L, 1L));
        RunCommand attempt = RunCommand.createAttempt(
                "attempt", "run-ha-events", created.snapshot().revision(),
                "attempt-1", "publication-1", 2L);
        assertEquals(RunDecision.Code.ACCEPTED, store.apply(attempt).code());
        assertEquals(RunDecision.Code.ALREADY_APPLIED, store.apply(attempt).code());

        MasterServer masterA = new MasterServer(60_000L, 0, registry);
        MasterServer masterB = new MasterServer(60_000L, 0, registry);
        MasterElection electionA = new MasterElection(
                registry, new Endpoint("127.0.0.1", masterA.port()));
        MasterElection electionB = new MasterElection(
                registry, new Endpoint("127.0.0.1", masterB.port()));
        masterA.bindLeadership(electionA);
        masterB.bindLeadership(electionB);
        electionA.start();
        electionB.start();
        MasterServer oldMaster = electionA.isLeader() ? masterA : masterB;
        MasterElection oldElection = electionA.isLeader() ? electionA : electionB;
        MasterServer replacementMaster = electionA.isLeader() ? masterB : masterA;
        MasterElection replacementElection = electionA.isLeader() ? electionB : electionA;

        try (RealClusterJobClient client = new RealClusterJobClient(
                new RegistryMasterLocator(registry))) {
            RunExecutionEventPage before = client.queryRunEvents(
                    "run-ha-events", 0L, 1);
            assertEquals(1L, before.nextAfterSequence());
            assertTrue(before.hasMore());

            oldElection.close();
            oldMaster.close();
            assertTrue(replacementElection.isLeader());

            RunExecutionEventPage after = client.queryRunEvents(
                    "run-ha-events", before.nextAfterSequence(), 256);
            assertEquals(1, after.events().size());
            assertEquals(2L, after.events().get(0).sequence());
            assertFalse(after.hasMore());
            assertEquals(0, client.queryRunEvents(
                    "run-ha-events", after.nextAfterSequence(), 256).events().size());
        } finally {
            replacementMaster.close();
            replacementElection.close();
        }
    }
}

