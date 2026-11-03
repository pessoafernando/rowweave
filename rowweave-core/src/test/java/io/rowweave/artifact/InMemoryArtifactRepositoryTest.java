package io.rowweave.artifact;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryArtifactRepositoryTest extends ArtifactRepositoryContract {

    @Override
    protected ArtifactRepository newRepository() {
        return new InMemoryArtifactRepository(32, 8L * 1024L * 1024L);
    }

    @Test
    void entryAndByteLimitsAreAtomicAndReplayDoesNotDoubleCharge() {
        byte[] first = encoded("first", new byte[256]);
        byte[] second = encoded("second", new byte[256]);
        InMemoryArtifactRepository entries =
                new InMemoryArtifactRepository(1, first.length + second.length + 1L);

        String firstId = entries.put(first);
        assertEquals(firstId, entries.put(first));
        assertThrows(IllegalStateException.class, () -> entries.put(second));
        assertEquals(firstId, PlanArtifactCodec.artifactId(entries.get(firstId)));

        InMemoryArtifactRepository bytes =
                new InMemoryArtifactRepository(2, first.length);
        bytes.put(first);
        assertThrows(IllegalStateException.class, () -> bytes.put(second));
        assertEquals(first.length, bytes.storedBytes());
        assertEquals(1, bytes.size());
    }

    @Test
    void inventoryUsageIsExactGlobalAndConstantTime() {
        byte[] first = encoded("usage-first", new byte[32]);
        byte[] second = encoded("usage-second", new byte[48]);
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(7, 1_000_000L);
        repository.put(first);
        repository.put(second);

        ArtifactInventoryUsage usage = repository.inventoryUsage(0);

        assertEquals(1, repository.inventoryUsageShardCount());
        assertEquals(ArtifactInventoryUsage.Scope.GLOBAL, usage.scope());
        assertEquals(-1, usage.shard());
        assertEquals(2, usage.entries());
        assertEquals(first.length + second.length, usage.bytes());
        assertEquals(7, usage.entryCapacity());
        assertEquals(1_000_000L, usage.byteCapacity());
        assertThrows(IllegalArgumentException.class,
                () -> repository.inventoryUsage(1));
        repository.close();
    }

    @Test
    void constructorBoundsAreRequired() {
        assertThrows(IllegalArgumentException.class,
                () -> new InMemoryArtifactRepository(0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new InMemoryArtifactRepository(1, 0));
    }

    @Test
    void storedCloneHasRepositoryOwnedChargeAfterRequestLeaseReleases() {
        byte[] encoded = encoded("resident", new byte[256]);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(8L * 1024L * 1024L);
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(2, encoded.length * 2L, budget);
        ControlResidentLease request = budget.openLease();
        ControlResidentLease.OwnerToken requestOwner = request.ownerToken();
        try {
            repository.put(encoded, bytes -> request.reserve(requestOwner, bytes));
        } finally {
            request.release(requestOwner);
        }

        assertEquals(encoded.length, budget.snapshot().usedBytes(),
                "persistent clone must not disappear with the request lease");
        repository.close();
        assertEquals(0L, budget.snapshot().usedBytes());
    }
}
