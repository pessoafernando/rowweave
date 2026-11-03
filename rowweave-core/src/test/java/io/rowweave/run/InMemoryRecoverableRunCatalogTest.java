package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryRecoverableRunCatalogTest {

    @Test
    void fullShardRejectsNewRunWithoutPublishingSnapshotOrEvent() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore(2);
        List<String> ids = sameShardIds(3);
        assertEquals(RunDecision.Code.ACCEPTED, create(store, ids.get(0), "create-0").code());
        assertEquals(RunDecision.Code.ACCEPTED, create(store, ids.get(1), "create-1").code());
        assertEquals(RunDecision.Code.LIMIT_EXCEEDED, create(store, ids.get(2), "create-2").code());
        assertEquals(null, store.get(ids.get(2)));
        assertEquals(0, store.events(ids.get(2), 0L, 10).size());
    }

    private static RunDecision create(InMemoryRunStateStore store, String runId, String commandId) {
        return store.apply(RunCommand.createRun(
                commandId, runId, "definition", "artifact", 1, 0L, 1L));
    }

    private static List<String> sameShardIds(int count) throws Exception {
        @SuppressWarnings("unchecked")
        List<String>[] byShard = new List[256];
        for (int i = 0; ; i++) {
            String id = "capacity-run-" + i;
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(id.getBytes(StandardCharsets.UTF_8));
            int shard = digest[0] & 0xff;
            if (byShard[shard] == null) byShard[shard] = new ArrayList<>();
            byShard[shard].add(id);
            if (byShard[shard].size() == count) return byShard[shard];
        }
    }
}
