package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Black-box contract that every backend advertising recovery catalog capability must pass. */
public abstract class RecoverableRunCatalogContract {

    protected abstract RunStateStore newStore();

    @Test
    void scansOneBoundedShardWithStableContinuationAndCycle() throws Exception {
        RunStateStore store = newStore();
        RecoverableRunCatalog catalog = (RecoverableRunCatalog) store;
        List<String> runIds = runIdsInShard(0, 3);
        for (int i = runIds.size() - 1; i >= 0; i--) {
            create(store, runIds.get(i), "create-" + i);
        }
        runIds.sort(Comparator.comparing(RecoverableRunCatalogContract::sha256HexUnchecked));

        RecoverableRunPage first = catalog.scan(null, 2);
        assertEquals(runIds.subList(0, 2), ids(first));
        assertFalse(first.cycleComplete());
        assertThrows(UnsupportedOperationException.class,
                () -> first.runs().add(first.runs().get(0)));

        RecoverableRunPage second = catalog.scan(first.nextCursor(), 2);
        assertEquals(runIds.subList(2, 3), ids(second));
        assertFalse(second.cycleComplete());

        RecoverableRunPage page = second;
        int calls = 0;
        while (!page.cycleComplete() && calls++ < 256) {
            page = catalog.scan(page.nextCursor(), 2);
        }
        assertTrue(page.cycleComplete());
        assertTrue(calls <= 255);
        assertEquals(runIds.subList(0, 2), ids(catalog.scan(page.nextCursor(), 2)));
    }

    @Test
    void catalogTracksAcceptedNonTerminalRevisionAndRemovesTerminalAtomically() throws Exception {
        RunStateStore store = newStore();
        RecoverableRunCatalog catalog = (RecoverableRunCatalog) store;
        String runId = runIdsInShard(0, 1).get(0);
        RunCommand create = RunCommand.createRun(
                "create", runId, "definition", "artifact", 2, 0L, 1L);
        RunSnapshot created = accepted(store, create);
        RecoverableRunRef initial = only(catalog.scan(null, 10));
        assertEquals(created.runId(), initial.runId());
        assertEquals(created.revision(), initial.revision());
        assertEquals(RunStatus.READY, initial.status());

        assertEquals(RunDecision.Code.ALREADY_APPLIED, store.apply(create).code());
        assertEquals(initial.revision(), only(catalog.scan(null, 10)).revision());
        assertEquals(RunDecision.Code.STALE_REVISION, store.apply(RunCommand.createAttempt(
                "stale", runId, 99L, "attempt-stale", "publication-stale", 2L)).code());
        assertEquals(initial.revision(), only(catalog.scan(null, 10)).revision());

        RunSnapshot running = accepted(store, RunCommand.createAttempt(
                "attempt", runId, created.revision(), "attempt-1", "publication-1", 2L));
        RecoverableRunRef updated = only(catalog.scan(null, 10));
        assertEquals(running.revision(), updated.revision());
        assertEquals(RunStatus.RUNNING, updated.status());

        RunSnapshot terminalReady = accepted(store, RunCommand.advanceAttempt(
                "fail", runId, running.revision(), "attempt-1", 1L,
                AttemptStatus.FAILED, false, 3L, "FAILED", "terminal"));
        assertEquals(RunStatus.FAILED, terminalReady.status());
        assertTrue(catalog.scan(null, 10).runs().isEmpty());
    }

    @Test
    void rejectsMalformedCursorAndUnboundedLimitBeforeReading() {
        RecoverableRunCatalog catalog = (RecoverableRunCatalog) newStore();
        assertThrows(IllegalArgumentException.class, () -> catalog.scan("", 1));
        assertThrows(IllegalArgumentException.class, () -> catalog.scan("not-a-cursor", 1));
        assertThrows(IllegalArgumentException.class, () -> catalog.scan(null, 0));
        assertThrows(IllegalArgumentException.class, () -> catalog.scan(null, 1001));
    }

    @Test
    void providerReturnsTheExactAuthorityCatalogAndFileStoreDoesNotClaimCapability() {
        RunStateStore store = newStore();
        assertTrue(store instanceof RecoverableRunCatalog);
        assertFalse(RecoverableRunCatalog.class.isAssignableFrom(FileRunStateStore.class));
    }

    private static RunSnapshot create(RunStateStore store, String runId, String commandId) {
        return accepted(store, RunCommand.createRun(
                commandId, runId, "definition", "artifact", 2, 0L, 1L));
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RecoverableRunRef only(RecoverableRunPage page) {
        assertEquals(1, page.runs().size());
        return page.runs().get(0);
    }

    private static List<String> ids(RecoverableRunPage page) {
        List<String> ids = new ArrayList<>();
        for (RecoverableRunRef ref : page.runs()) ids.add(ref.runId());
        return ids;
    }

    private static List<String> runIdsInShard(int shard, int count) throws Exception {
        List<String> ids = new ArrayList<>();
        for (int i = 0; ids.size() < count; i++) {
            String id = "catalog-run-" + i;
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(id.getBytes(StandardCharsets.UTF_8));
            if ((digest[0] & 0xff) == shard) ids.add(id);
        }
        return ids;
    }

    private static String sha256HexUnchecked(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(64);
            for (byte b : digest) out.append(String.format(java.util.Locale.ROOT, "%02x", b & 0xff));
            return out.toString();
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
