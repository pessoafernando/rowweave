package io.rowweave.cluster.registry;

import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.RecoverableRunCatalogProvider;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunLifecycleCatalogProvider;
import io.rowweave.run.RunRetentionAuthorityProvider;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InProcessRegistryRunStateStoreTest {

    @Test
    void oneRegistryExposesOneSharedRunAuthority() {
        InProcessRegistry registry = new InProcessRegistry();

        assertTrue(registry instanceof RunStateStoreProvider);
        RunStateStore first = ((RunStateStoreProvider) registry).runStateStore();
        RunStateStore second = ((RunStateStoreProvider) registry).runStateStore();
        assertSame(first, second);
        assertTrue(registry instanceof RecoverableRunCatalogProvider);
        assertSame(first, ((RecoverableRunCatalogProvider) registry).recoverableRunCatalog());
        assertTrue(registry instanceof RunLifecycleCatalogProvider);
        assertSame(first, ((RunLifecycleCatalogProvider) registry).runLifecycleCatalog());
        assertTrue(registry instanceof RunRetentionAuthorityProvider);
        assertSame(first, ((RunRetentionAuthorityProvider) registry).runRetentionAuthority());
        assertTrue(registry instanceof RunRecoveryOwnershipProvider);
        assertSame(first, ((RunRecoveryOwnershipProvider) registry).runRecoveryOwnership());
        assertTrue(registry instanceof DurableStageAssignmentAuthorityProvider);
        assertSame(first, ((DurableStageAssignmentAuthorityProvider) registry)
                .durableStageAssignmentAuthority());

        RunDecision created = first.apply(RunCommand.createRun(
                "create-run", "run-shared", "definition", "artifact", 2, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        assertEquals("artifact", second.get("run-shared").artifactId());
    }
}
