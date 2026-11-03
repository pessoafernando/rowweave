package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunRetentionAuthorityKernelTest {

    @Test
    void replacementCannotOverflowTheDurableOwnershipEpoch() {
        InMemoryRunStateStore store = new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD, () -> 100L);
        RunDecision created = store.apply(RunCommand.createRun(
                "create", "run", "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision canceled = store.apply(RunCommand.cancelReadyRun(
                "cancel", "run", created.snapshot().revision(), 2L,
                "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
        RunLifecycleRef terminal = store.lifecycle("run");
        RetentionToken exhausted = RetentionToken.create(
                "run", new RetentionOwner("old", 1L), Long.MAX_VALUE,
                canceled.snapshot().revision(), terminal.terminalAtMillis());
        RunLifecycleRef deleting = RunLifecycleRef.deleting(
                "run", canceled.snapshot().revision(), canceled.snapshot().status(),
                canceled.snapshot().artifactId(), terminal.terminalAtMillis(), exhausted);

        assertThrows(IllegalStateException.class,
                () -> RunRetentionAuthorityKernel.evaluate(
                        "run", canceled.snapshot(), deleting, false,
                        canceled.snapshot().revision(), terminal.terminalAtMillis(),
                        new RetentionOwner("replacement", 2L), Long.MAX_VALUE));
    }
}
