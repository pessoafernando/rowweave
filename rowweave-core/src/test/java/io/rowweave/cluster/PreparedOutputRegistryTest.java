package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreparedOutputRegistryTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final RunAttemptFence TEST_FENCE =
            new RunAttemptFence("test-run", "test-attempt", 1L);

    @Test
    void staleAttemptFenceCannotMutatePreparedOutput() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        RunAttemptFence owner = new RunAttemptFence("run", "attempt-owner", 1L);
        RunAttemptFence stale = new RunAttemptFence("run", "attempt-stale", 2L);
        TrackingOutput output = new TrackingOutput();
        assertTrue(registry.register("g", 901L, output, owner));

        assertThrows(IllegalArgumentException.class, () -> registry.commit("g", 901L, stale));
        assertThrows(IllegalArgumentException.class, () -> registry.rollback("g", 901L, stale));
        assertThrows(IllegalArgumentException.class,
                () -> registry.finalizeCommit("g", 901L, stale));
        assertEquals(0, output.commitCalls);
        assertEquals(0, output.rollbackCalls);
        assertEquals(0, output.finalizeCalls);

        registry.commit("g", 901L, owner);
        registry.finalizeCommit("g", 901L, owner);
        assertEquals(1, output.commitCalls);
        assertEquals(1, output.finalizeCalls);
        registry.close();
    }

    @Test
    void rollbackTombstoneOnlySuppressesSameAttempt() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        RunAttemptFence first = new RunAttemptFence("run", "attempt-1", 1L);
        RunAttemptFence second = new RunAttemptFence("run", "attempt-2", 2L);
        registry.rollback("g", 902L, first);

        TrackingOutput replacement = new TrackingOutput();
        assertTrue(registry.register("g", 902L, replacement, second));
        registry.rollback("g", 902L, second);
        assertEquals(1, replacement.rollbackCalls);

        registry.rollback("g", 903L, first);
        TrackingOutput sameAttempt = new TrackingOutput();
        assertFalse(registry.register("g", 903L, sameAttempt, first));
        assertEquals(1, sameAttempt.rollbackCalls);
        registry.close();
    }

    @Test
    void publishRollbackAndFinalizeAreIdempotentAndReleaseOutputs() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        TrackingOutput rolledBack = new TrackingOutput();
        TrackingOutput finalized = new TrackingOutput();

        assertTrue(registry.register("g", 1L, rolledBack, TEST_FENCE));
        registry.commit("g", 1L, TEST_FENCE);
        registry.commit("g", 1L, TEST_FENCE);
        registry.rollback("g", 1L, TEST_FENCE);
        registry.rollback("g", 1L, TEST_FENCE);

        assertTrue(registry.register("g", 2L, finalized, TEST_FENCE));
        registry.commit("g", 2L, TEST_FENCE);
        registry.finalizeCommit("g", 2L, TEST_FENCE);
        registry.finalizeCommit("g", 2L, TEST_FENCE);

        assertEquals(1, rolledBack.commitCalls);
        assertEquals(1, rolledBack.rollbackCalls);
        assertEquals(1, finalized.commitCalls);
        assertEquals(1, finalized.finalizeCalls);
        assertEquals(0, registry.activeCount(), "a terminal tombstone must not keep a strong reference to the Sink");
        assertEquals(2, registry.entryCount());
        registry.close();
    }

    @Test
    void rollbackBeforeLateRegistrationPreventsPreparedOutputLeak() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        registry.rollback("g", 7L, TEST_FENCE);
        TrackingOutput late = new TrackingOutput();

        assertFalse(registry.register("g", 7L, late, TEST_FENCE));

        assertEquals(1, late.rollbackCalls);
        assertEquals(0, registry.activeCount());
        registry.commit("g", 7L, TEST_FENCE);
        assertEquals(0, late.commitCalls);
        registry.close();
    }

    @Test
    void wrongGroupCannotMutateAnotherJobsPreparedOutput() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        TrackingOutput output = new TrackingOutput();
        registry.register("owner", 9L, output, TEST_FENCE);

        assertThrows(IllegalArgumentException.class,
                () -> registry.commit("intruder", 9L, TEST_FENCE));
        assertThrows(IllegalArgumentException.class,
                () -> registry.rollback("intruder", 9L, TEST_FENCE));
        assertEquals(0, output.commitCalls);
        assertEquals(0, output.rollbackCalls);

        registry.rollback("owner", 9L, TEST_FENCE);
        registry.close();
    }

    @Test
    void terminalTombstonesCannotExhaustCapacityForLaterJobs() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(2, 60_000L);
        for (long stageId = 1; stageId <= 10; stageId++) {
            TrackingOutput output = new TrackingOutput();
            assertTrue(registry.register("g-" + stageId, stageId, output, TEST_FENCE));
            registry.commit("g-" + stageId, stageId, TEST_FENCE);
            registry.finalizeCommit("g-" + stageId, stageId, TEST_FENCE);
        }
        assertEquals(0, registry.activeCount());
        assertTrue(registry.entryCount() <= 2);
        registry.close();
    }

    @Test
    void ttlAndCloseRollbackEveryRetainedOutput() throws Exception {
        PreparedOutputRegistry expiring = new PreparedOutputRegistry(4, 1L);
        TrackingOutput expired = new TrackingOutput();
        expiring.register("ttl", 20L, expired, TEST_FENCE);
        Thread.sleep(5L);
        expiring.purgeExpired();
        assertEquals(1, expired.rollbackCalls);
        assertEquals(0, expiring.activeCount());
        expiring.close();

        PreparedOutputRegistry closing = new PreparedOutputRegistry(4, 60_000L);
        TrackingOutput prepared = new TrackingOutput();
        TrackingOutput published = new TrackingOutput();
        closing.register("close", 21L, prepared, TEST_FENCE);
        closing.register("close", 22L, published, TEST_FENCE);
        closing.commit("close", 22L, TEST_FENCE);

        closing.close();

        assertEquals(1, prepared.rollbackCalls);
        assertEquals(1, published.rollbackCalls);
        assertEquals(0, closing.activeCount());
    }

    @Test
    void renewalExtendsOnlyMatchingLivePreparedEntryAndExpiryStillRollsBack() throws Exception {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(4, 500L);
        TrackingOutput output = new TrackingOutput();
        registry.register("live", 41L, output, TEST_FENCE);
        Thread.sleep(300L);
        assertThrows(IllegalArgumentException.class,
                () -> registry.renew("wrong", 41L, TEST_FENCE));
        assertThrows(IllegalArgumentException.class,
                () -> registry.renew("live", 41L,
                        new RunAttemptFence("test-run", "stale", 2L)));
        registry.renew("live", 41L, TEST_FENCE);
        Thread.sleep(300L);
        registry.purgeExpired();
        assertEquals(1, registry.activeCount());
        assertEquals(0, output.rollbackCalls);
        registry.rollback("live", 41L, TEST_FENCE);
        assertThrows(IllegalStateException.class,
                () -> registry.renew("live", 41L, TEST_FENCE));
        assertEquals(1, output.rollbackCalls);
        registry.close();

        PreparedOutputRegistry abandoned = new PreparedOutputRegistry(4, 30L);
        TrackingOutput expired = new TrackingOutput();
        abandoned.register("lost", 42L, expired, TEST_FENCE);
        Thread.sleep(45L);
        assertThrows(IllegalStateException.class,
                () -> abandoned.renew("lost", 42L, TEST_FENCE));
        assertEquals(1, expired.rollbackCalls);
        abandoned.close();
    }

    @Test
    void finalizeFailureRetainsRecoveryStateAndRetryReleasesIt() {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(4, 60_000L);
        TrackingOutput output = new TrackingOutput(1);
        registry.register("finalize-retry", 30L, output, TEST_FENCE);
        registry.commit("finalize-retry", 30L, TEST_FENCE);

        assertThrows(IllegalStateException.class,
                () -> registry.finalizeCommit("finalize-retry", 30L, TEST_FENCE));
        assertEquals(1, registry.activeCount(),
                "a failed finalize keeps the Sink so the Master can retry or recover later");

        registry.finalizeCommit("finalize-retry", 30L, TEST_FENCE);
        assertEquals(2, output.finalizeCalls);
        assertEquals(0, registry.activeCount(), "a successful retry releases the strong reference to the Sink");
        registry.close();
    }

    @Test
    void manifestMembersInstallBeforeOneGroupPublishAndLateRollbackPreservesVisibleData(
            @TempDir Path domain) {
        String attempt = "cluster-attempt";
        long generation = 19L;
        ManifestFileSink left = manifestOutput(domain, "left", attempt, generation, 1L);
        ManifestFileSink right = manifestOutput(domain, "right", attempt, generation, 2L);
        PreparedOutputRegistry workerA = new PreparedOutputRegistry(8, 60_000L);
        PreparedOutputRegistry workerB = new PreparedOutputRegistry(8, 60_000L);
        workerA.register("group", 101L, left, TEST_FENCE);
        workerB.register("group", 102L, right, TEST_FENCE);

        CommitManifest.Member leftMember = ManifestCommitWire.decodeMember(
                workerA.installManifestMember(
                        "group", 101L, attempt, generation, TEST_FENCE));
        CommitManifest.Member rightMember = ManifestCommitWire.decodeMember(
                workerB.installManifestMember(
                        "group", 102L, attempt, generation, TEST_FENCE));
        CommitManifest manifest = new CommitManifest(
                left.domainId(), generation, attempt, Arrays.asList(leftMember, rightMember));

        workerA.publishManifestGroup(
                "group", 101L, attempt, generation, manifest.encode(), TEST_FENCE);
        workerA.publishManifestGroup(
                "group", 101L, attempt, generation, manifest.encode(), TEST_FENCE);
        workerB.rollback(
                "group", 102L, TEST_FENCE);

        assertEquals(generation,
                new io.rowweave.commit.ManifestFileStore(domain).latest().generation());
        workerA.finalizeCommit("group", 101L, TEST_FENCE);
        assertEquals(0, workerA.activeCount());
        workerA.close();
        workerB.close();
    }

    private static ManifestFileSink manifestOutput(
            Path domain, String member, String attempt, long generation, long id) {
        ManifestFileSink output = new ManifestFileSink(META, domain.toString(), member);
        output.bindManifestAttempt(attempt, generation);
        DataRow row = new DataRow(1);
        row.set(0, id);
        output.collect(row);
        output.prepareCommit();
        return output;
    }

    private static final class TrackingOutput implements Sink {
        private static final long serialVersionUID = 1L;
        int commitCalls;
        int rollbackCalls;
        int finalizeCalls;
        int finalizeFailuresRemaining;

        private TrackingOutput() {
            this(0);
        }

        private TrackingOutput(int finalizeFailuresRemaining) {
            this.finalizeFailuresRemaining = finalizeFailuresRemaining;
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) { }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { }
        @Override public void commitPrepared() { commitCalls++; }
        @Override public void rollbackCommit() { rollbackCalls++; }
        @Override public void finalizeCommit() {
            finalizeCalls++;
            if (finalizeFailuresRemaining-- > 0) {
                throw new IllegalStateException("injected finalize failure");
            }
        }
    }
}

