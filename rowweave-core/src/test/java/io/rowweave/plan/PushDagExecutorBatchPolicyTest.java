package io.rowweave.plan;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Router staging stays bounded, and several producers fall back to exactly the older row-at-a-time path. */
class PushDagExecutorBatchPolicyTest {

    @Test
    void batchesOnlySingleRouterAndBoundsTotalStagingReferences() {
        String key = "rowweave.local.shuffle.batch";
        String previous = System.getProperty(key);
        try {
            System.setProperty(key, "true");
            assertEquals(32, PushDagExecutor.localShuffleBatchSize(4, 1));
            assertEquals(15, PushDagExecutor.localShuffleBatchSize(17, 1),
                    "17 partitions × 15 refs <= 256");
            assertEquals(1, PushDagExecutor.localShuffleBatchSize(257, 1),
                    "a parallelism above 256 allocates no per-partition staging");
            assertEquals(1, PushDagExecutor.localShuffleBatchSize(4, 4),
                    "several routers must not hold their locks together and form a producer convoy");

            System.setProperty(key, "false");
            assertEquals(1, PushDagExecutor.localShuffleBatchSize(4, 1),
                    "with the batch switches off the row-at-a-time path is taken exactly");
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }
}
