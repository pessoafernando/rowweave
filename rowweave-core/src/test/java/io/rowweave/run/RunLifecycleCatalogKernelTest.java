package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunLifecycleCatalogKernelTest {

    @Test
    void pageIsDeterministicBoundedAndCyclesAcrossAllShards() {
        RunLifecycleRef terminal = RunLifecycleRef.terminal(
                "run-terminal", 2L, RunStatus.FAILED, "artifact", 10L);
        int shard = RunLifecycleCatalogKernel.shard(
                RunLifecycleCatalogKernel.digestHex(terminal.runId()));
        String cursor = RunLifecycleCatalogKernel.encodeCursor(7L, shard, null);
        RunLifecyclePage page = RunLifecycleCatalogKernel.page(
                cursor, 1, Arrays.asList(terminal));
        assertEquals(1, page.runs().size());
        assertEquals(terminal.runId(), page.runs().get(0).runId());
        assertFalse(page.nextCursor().isEmpty());
        assertEquals(shard == RunLifecycleCatalogKernel.SHARD_COUNT - 1,
                page.cycleComplete());

        String last = RunLifecycleCatalogKernel.encodeCursor(
                8L, RunLifecycleCatalogKernel.SHARD_COUNT - 1, null);
        RunLifecyclePage completed = RunLifecycleCatalogKernel.page(
                last, 10, java.util.Collections.<RunLifecycleRef>emptyList());
        assertTrue(completed.cycleComplete());
    }

    @Test
    void pageRejectsActiveWrongShardDuplicateOversizeAndBadCursor() {
        RunLifecycleRef terminal = RunLifecycleRef.terminal(
                "run", 2L, RunStatus.CANCELED, "artifact", 10L);
        int actual = RunLifecycleCatalogKernel.shard(
                RunLifecycleCatalogKernel.digestHex(terminal.runId()));
        int wrong = (actual + 1) % RunLifecycleCatalogKernel.SHARD_COUNT;
        assertThrows(IllegalStateException.class,
                () -> RunLifecycleCatalogKernel.page(
                        RunLifecycleCatalogKernel.encodeCursor(0L, wrong, null),
                        1, Arrays.asList(terminal)));
        assertThrows(IllegalStateException.class,
                () -> RunLifecycleCatalogKernel.page(
                        RunLifecycleCatalogKernel.encodeCursor(0L, actual, null),
                        2, Arrays.asList(terminal, terminal)));
        assertThrows(IllegalStateException.class,
                () -> RunLifecycleCatalogKernel.page(
                        RunLifecycleCatalogKernel.encodeCursor(0L, actual, null),
                        1, Arrays.asList(RunLifecycleRef.active(
                                terminal.runId(), 1L, RunStatus.READY, "artifact"))));

        List<RunLifecycleRef> oversized = new ArrayList<>();
        for (int i = 0; i <= RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD; i++) {
            oversized.add(RunLifecycleRef.terminal(
                    "run-" + i, 2L, RunStatus.FAILED, "artifact", 10L));
        }
        assertThrows(IllegalStateException.class,
                () -> RunLifecycleCatalogKernel.page(
                        RunLifecycleCatalogKernel.encodeCursor(0L, actual, null),
                        1, oversized));
        assertThrows(IllegalArgumentException.class,
                () -> RunLifecycleCatalogKernel.decodeCursor("not-a-cursor"));
        assertThrows(IllegalArgumentException.class,
                () -> RunLifecycleCatalogKernel.decodeCursor(
                        RunRecoveryCatalogKernel.encodeCursor(0L, 0, null)));
    }
}
