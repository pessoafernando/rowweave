package io.rowweave.incremental;

import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** hard process loss after force exposes the joint after-state. */
class IncrementalFileAuthorityProcessCrashContractTest {
    @Test
    void processHaltAfterJointForceRecoversBothProjections(
            @TempDir Path root) throws Exception {
        Process process = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                IncrementalFileAuthorityCrashProbe.class.getName(),
                root.toString())).redirectErrorStream(true).start();
        assertTrue(process.waitFor(20L, TimeUnit.SECONDS));
        assertEquals(96, process.exitValue());

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        IncrementalAuthorityRecord record = reopened.record("feed");
        assertEquals(IncrementalFrontierAuthorityState.READY, record.state());
        assertNull(record.activeInterval());
        assertNotNull(record.lastCommitReceipt());
        assertEquals(RunStatus.SUCCEEDED, reopened.runSnapshot(
                record.lastCommitReceipt().fence().runId()).status());
    }
}
