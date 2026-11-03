package io.rowweave.run;

import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.cluster.registry.InProcessRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class InMemoryArtifactReferenceAuthorityTest
        extends ArtifactReferenceAuthorityContract {

    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                () -> 100L, 4096);
    }

    @Test
    void perArtifactReferenceShardCapacityRejectsOnlyNewRun() {
        InMemoryRunStateStore store = new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                () -> 100L, 1);
        List<String> sameShard = sameReferenceShardRunIds(2);
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "first", sameShard.get(0), "definition", ARTIFACT_A,
                        1, 0L, 1L)).code());
        assertEquals(RunDecision.Code.LIMIT_EXCEEDED,
                store.apply(RunCommand.createRun(
                        "second", sameShard.get(1), "definition", ARTIFACT_A,
                        1, 0L, 1L)).code());
        assertEquals(1L, ((ArtifactReferenceAuthority) store)
                .inspect(ARTIFACT_A).referenceCount());
    }

    @Test
    void inProcessRegistryExposesSameRunAuthorityInstance() {
        InProcessRegistry registry = new InProcessRegistry();
        ArtifactReferenceAuthority authority =
                ((ArtifactReferenceAuthorityProvider) registry)
                        .artifactReferenceAuthority();
        assertSame(registry.runStateStore(), authority);
    }

    private static List<String> sameReferenceShardRunIds(int count) {
        List<String> ids = new ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String runId = "artifact-ref-capacity-" + value;
            int shard = RunRecoveryCatalogKernel.shard(
                    RunRecoveryCatalogKernel.digestHex(runId));
            if (target < 0) target = shard;
            if (shard == target) ids.add(runId);
        }
        return ids;
    }
}
