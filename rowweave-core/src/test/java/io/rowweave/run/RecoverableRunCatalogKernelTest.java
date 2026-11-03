package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoverableRunCatalogKernelTest {

    @Test
    void refCodecIsDeterministicStrictAndRejectsTerminalState() {
        RecoverableRunRef ref = new RecoverableRunRef("run-Ω", 7L, RunStatus.RECONCILING);
        byte[] encoded = RunRecoveryCatalogKernel.encodeRef(ref);
        assertArrayEquals(encoded, RunRecoveryCatalogKernel.encodeRef(ref));
        RecoverableRunRef decoded = RunRecoveryCatalogKernel.decodeRef(encoded);
        assertEquals(ref.runId(), decoded.runId());
        assertEquals(ref.revision(), decoded.revision());
        assertEquals(ref.status(), decoded.status());

        byte[] corrupt = encoded.clone();
        corrupt[8] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> RunRecoveryCatalogKernel.decodeRef(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> new RecoverableRunRef("run", 1L, RunStatus.SUCCEEDED));
    }

    @Test
    void pageRejectsWrongShardDuplicateAndOversizedBackendResponse() {
        RecoverableRunRef ref = new RecoverableRunRef("run", 1L, RunStatus.READY);
        int actualShard = RunRecoveryCatalogKernel.shard(
                RunRecoveryCatalogKernel.digestHex(ref.runId()));
        int wrongShard = (actualShard + 1) % RunRecoveryCatalogKernel.SHARD_COUNT;
        String wrongCursor = RunRecoveryCatalogKernel.encodeCursor(0L, wrongShard, null);
        assertThrows(IllegalStateException.class,
                () -> RunRecoveryCatalogKernel.page(wrongCursor, 1, Arrays.asList(ref)));
        String cursor = RunRecoveryCatalogKernel.encodeCursor(0L, actualShard, null);
        assertThrows(IllegalStateException.class,
                () -> RunRecoveryCatalogKernel.page(cursor, 2, Arrays.asList(ref, ref)));

        java.util.List<RecoverableRunRef> oversized = new java.util.ArrayList<>();
        for (int i = 0; i <= RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD; i++) {
            oversized.add(ref);
        }
        assertThrows(IllegalStateException.class,
                () -> RunRecoveryCatalogKernel.page(cursor, 1, oversized));
    }
}
