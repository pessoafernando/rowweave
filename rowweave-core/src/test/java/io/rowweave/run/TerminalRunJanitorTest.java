package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Per-round workload, fair cursor and failure isolation of the leader janitor. */
class TerminalRunJanitorTest {

    @Test
    void onePassBoundsClaimsAndPurgeStepsWithoutSkippingSameShardRemainder() {
        List<String> ids = sameShardIds(5);
        ScriptedAuthority backend = new ScriptedAuthority(refs(ids, 10L));
        RunRetentionPolicy policy = RunRetentionPolicy.testing(
                10L, 100, 5, 2, 1L);
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend, policy, () -> 100L);
        RetentionOwner owner = new RetentionOwner("leader", 7L);

        TerminalRunJanitor.PassResult first =
                janitor.runOnePass(owner, () -> true);
        TerminalRunJanitor.PassResult second =
                janitor.runOnePass(owner, () -> true);
        TerminalRunJanitor.PassResult third =
                janitor.runOnePass(owner, () -> true);

        assertEquals(5, first.scannedRefs());
        assertEquals(2, first.claimAttempts());
        assertEquals(2, first.purgeSteps());
        assertEquals(2, first.purgedRuns());
        assertFalse(first.cycleComplete());
        assertEquals(3, second.scannedRefs());
        assertEquals(2, second.claimAttempts());
        assertEquals(2, second.purgeSteps());
        assertEquals(1, third.scannedRefs());
        assertEquals(1, third.purgedRuns());
        assertEquals(ids, backend.claimedRunIds);
        assertTrue(backend.purgeBounds.stream().allMatch(bound -> bound == 2));
        assertEquals(90L, backend.cutoffs.get(0).longValue());
        TerminalRunJanitor.Snapshot snapshot = janitor.snapshot();
        assertEquals(1, snapshot.backlog());
        assertEquals(100, snapshot.lifecycleCapacityPerShard());
        assertEquals(90L, snapshot.oldestTerminalAgeMillis());
        assertEquals(5L, snapshot.claimedRuns());
        assertEquals(5L, snapshot.purgedRuns());
        assertEquals(0L, snapshot.errorRuns());
        janitor.close();
    }

    @Test
    void perRunOutcomesAndFailuresAreCountedAndDoNotBlockLaterRuns() {
        List<String> ids = sameShardIds(6);
        ScriptedAuthority backend = new ScriptedAuthority(refs(ids, 10L));
        backend.claimOutcomes.put(ids.get(0), RunRetentionClaim.notDue());
        backend.claimOutcomes.put(ids.get(1), RunRetentionClaim.lostRace());
        backend.claimOutcomes.put(ids.get(2), RunRetentionClaim.staleRun());
        backend.claimFailures.put(ids.get(3), new RunStateStoreUnavailableException(
                null, "claim unavailable", null));
        backend.purgeOutcomes.put(ids.get(4), RunPurgeProgress.corrupt());
        backend.purgeOutcomes.put(ids.get(5), RunPurgeProgress.purged());
        RunRetentionPolicy policy = RunRetentionPolicy.testing(
                1L, 100, 6, 6, 1L);
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend, policy, () -> 100L);

        TerminalRunJanitor.PassResult result = janitor.runOnePass(
                new RetentionOwner("leader", 9L), () -> true);

        assertEquals(6, result.claimAttempts());
        assertEquals(1, result.notDueRuns());
        assertEquals(1, result.lostRaceRuns());
        assertEquals(1, result.staleRuns());
        assertEquals(1, result.unavailableRuns());
        assertEquals(1, result.corruptRuns());
        assertEquals(1, result.purgedRuns());
        assertEquals(2, result.purgeSteps());
        assertTrue(result.diagnosticSamples().size() <= 16);
        assertTrue(backend.claimedRunIds.contains(ids.get(5)));
        assertEquals(2L, janitor.snapshot().errorRuns());
        janitor.close();
    }

    @Test
    void leadershipLossFencesABlockedBackgroundPassBeforeClaim() throws Exception {
        String runId = sameShardIds(1).get(0);
        BlockingAuthority backend = new BlockingAuthority(refs(
                java.util.Collections.singletonList(runId), 10L));
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend,
                RunRetentionPolicy.testing(1L, 100, 1, 1, 3_600_000L),
                () -> 100L);

        long generation = janitor.begin(
                new RetentionOwner("leader", 11L), () -> true);
        assertTrue(backend.scanEntered.await(5, TimeUnit.SECONDS));

        janitor.loseLeadership();
        backend.releaseScan.countDown();

        assertTrue(janitor.awaitStopped(generation, 5, TimeUnit.SECONDS));
        assertEquals(0, backend.claims);
        assertFalse(janitor.active());
        janitor.close();
    }

    @Test
    void closeCancelsLifecycleAndRejectsFurtherPasses() {
        ScriptedAuthority backend = new ScriptedAuthority(
                java.util.Collections.emptyList());
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend,
                RunRetentionPolicy.testing(1L, 100, 1, 1, 1L),
                () -> 100L);

        janitor.close();

        assertFalse(janitor.active());
        assertThrows(IllegalStateException.class, () -> janitor.begin(
                new RetentionOwner("leader", 12L), () -> true));
        assertThrows(IllegalStateException.class, () -> janitor.runOnePass(
                new RetentionOwner("leader", 12L), () -> true));
    }

    @Test
    void backgroundCatalogFailureIsCountedAndRetriedWithoutStoppingLifecycle()
            throws Exception {
        RetryingCatalogAuthority backend = new RetryingCatalogAuthority();
        CountDownLatch errorPublished = new CountDownLatch(1);
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend,
                RunRetentionPolicy.testing(1L, 100, 1, 1, 1L),
                () -> 100L,
                snapshot -> {
                    if (snapshot.errorRuns() == 1L) errorPublished.countDown();
                });

        long generation = janitor.begin(
                new RetentionOwner("leader", 13L), () -> true);

        assertTrue(errorPublished.await(5, TimeUnit.SECONDS));
        assertTrue(backend.successfulRetry.await(5, TimeUnit.SECONDS));
        assertEquals(1L, janitor.snapshot().errorRuns());
        assertTrue(janitor.active());
        janitor.loseLeadership();
        assertTrue(janitor.awaitStopped(generation, 5, TimeUnit.SECONDS));
        janitor.close();
    }

    @Test
    void advisoryObserverFailureCannotChangePurgeResult() {
        List<String> ids = sameShardIds(1);
        ScriptedAuthority backend = new ScriptedAuthority(refs(ids, 10L));
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend,
                RunRetentionPolicy.testing(1L, 100, 1, 1, 1L),
                () -> 100L,
                snapshot -> { throw new IllegalStateException("monitor down"); });

        TerminalRunJanitor.PassResult result = janitor.runOnePass(
                new RetentionOwner("leader", 14L), () -> true);

        assertEquals(1, result.purgedRuns());
        assertEquals(1L, janitor.snapshot().purgedRuns());
        janitor.close();
    }

    @Test
    void corruptLifecycleShardIsIsolatedSoTheNextShardStillRuns()
            throws Exception {
        CorruptShardAuthority backend = new CorruptShardAuthority(
                runIdForShard(1));
        CountDownLatch snapshotPublished = new CountDownLatch(1);
        TerminalRunJanitor janitor = new TerminalRunJanitor(
                backend, backend,
                RunRetentionPolicy.testing(1L, 100, 1, 1, 1L),
                () -> 100L,
                snapshot -> {
                    if (snapshot.errorRuns() == 1L
                            && snapshot.purgedRuns() == 1L) {
                        snapshotPublished.countDown();
                    }
                });

        long generation = janitor.begin(
                new RetentionOwner("leader", 15L), () -> true);

        assertTrue(snapshotPublished.await(5, TimeUnit.SECONDS));
        assertEquals(1L, janitor.snapshot().errorRuns());
        assertEquals(1L, janitor.snapshot().purgedRuns());
        assertTrue(backend.scannedShards.contains(0));
        assertTrue(backend.scannedShards.contains(1));
        janitor.loseLeadership();
        assertTrue(janitor.awaitStopped(generation, 5, TimeUnit.SECONDS));
        janitor.close();
    }

    private static List<RunLifecycleRef> refs(List<String> ids, long terminalAt) {
        List<RunLifecycleRef> refs = new ArrayList<>();
        for (String id : ids) {
            refs.add(RunLifecycleRef.terminal(
                    id, 2L, RunStatus.CANCELED, "artifact-" + id, terminalAt));
        }
        return refs;
    }

    private static List<String> sameShardIds(int count) {
        List<String> ids = new ArrayList<>();
        for (int value = 0; ids.size() < count; value++) {
            String id = "janitor-" + value;
            int shard = RunLifecycleCatalogKernel.shard(
                    RunLifecycleCatalogKernel.digestHex(id));
            if (shard == 0) ids.add(id);
        }
        ids.sort((left, right) -> RunLifecycleCatalogKernel.digestHex(left)
                .compareTo(RunLifecycleCatalogKernel.digestHex(right)));
        return ids;
    }

    private static String runIdForShard(int expectedShard) {
        for (int value = 0; ; value++) {
            String runId = "janitor-shard-" + expectedShard + '-' + value;
            if (RunLifecycleCatalogKernel.shard(
                    RunLifecycleCatalogKernel.digestHex(runId)) == expectedShard) {
                return runId;
            }
        }
    }

    private static final class ScriptedAuthority
            implements RunLifecycleCatalog, RunRetentionAuthority {
        final List<RunLifecycleRef> refs;
        final Map<String, RunRetentionClaim> claimOutcomes = new HashMap<>();
        final Map<String, RuntimeException> claimFailures = new HashMap<>();
        final Map<String, RunPurgeProgress> purgeOutcomes = new HashMap<>();
        final List<String> claimedRunIds = new ArrayList<>();
        final List<Long> cutoffs = new ArrayList<>();
        final List<Integer> purgeBounds = new ArrayList<>();

        ScriptedAuthority(List<RunLifecycleRef> refs) {
            this.refs = refs;
        }

        @Override
        public RunLifecycleRef lifecycle(String runId) {
            return refs.stream().filter(ref -> ref.runId().equals(runId))
                    .findFirst().orElse(null);
        }

        @Override
        public RunLifecyclePage scanLifecycle(String cursor, int limit) {
            RunLifecycleCatalogKernel.Cursor position =
                    RunLifecycleCatalogKernel.decodeCursor(cursor);
            List<RunLifecycleRef> shard = new ArrayList<>();
            for (RunLifecycleRef ref : refs) {
                String digest = RunLifecycleCatalogKernel.digestHex(ref.runId());
                if (RunLifecycleCatalogKernel.shard(digest) == position.shard()) {
                    shard.add(ref);
                }
            }
            return RunLifecycleCatalogKernel.page(cursor, limit, shard);
        }

        @Override
        public RunRetentionClaim claim(
                String runId, long expectedTerminalRevision, long cutoffMillis,
                RetentionOwner owner, long observedOwnershipEpoch) {
            claimedRunIds.add(runId);
            cutoffs.add(cutoffMillis);
            RuntimeException failure = claimFailures.get(runId);
            if (failure != null) throw failure;
            RunRetentionClaim scripted = claimOutcomes.get(runId);
            if (scripted != null) return scripted;
            RunLifecycleRef ref = lifecycle(runId);
            return RunRetentionClaim.acquired(RetentionToken.create(
                    runId, owner, 1L, expectedTerminalRevision,
                    ref.terminalAtMillis()));
        }

        @Override
        public boolean current(RetentionToken token) {
            return true;
        }

        @Override
        public RunPurgeProgress purge(RetentionToken token, int maxDeletes) {
            purgeBounds.add(maxDeletes);
            return purgeOutcomes.getOrDefault(
                    token.runId(), RunPurgeProgress.purged());
        }
    }

    private static final class BlockingAuthority
            implements RunLifecycleCatalog, RunRetentionAuthority {
        final List<RunLifecycleRef> refs;
        final CountDownLatch scanEntered = new CountDownLatch(1);
        final CountDownLatch releaseScan = new CountDownLatch(1);
        volatile int claims;

        BlockingAuthority(List<RunLifecycleRef> refs) {
            this.refs = refs;
        }

        @Override
        public RunLifecycleRef lifecycle(String runId) {
            return refs.get(0);
        }

        @Override
        public RunLifecyclePage scanLifecycle(String cursor, int limit) {
            scanEntered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    releaseScan.await();
                    break;
                } catch (InterruptedException cancellation) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
            return RunLifecycleCatalogKernel.page(cursor, limit, refs);
        }

        @Override
        public RunRetentionClaim claim(String runId, long expectedTerminalRevision,
                                       long cutoffMillis, RetentionOwner owner,
                                       long observedOwnershipEpoch) {
            claims++;
            return RunRetentionClaim.lostRace();
        }

        @Override
        public boolean current(RetentionToken token) {
            return false;
        }

        @Override
        public RunPurgeProgress purge(RetentionToken token, int maxDeletes) {
            throw new AssertionError("purge must not run after leadership loss");
        }
    }

    private static final class RetryingCatalogAuthority
            implements RunLifecycleCatalog, RunRetentionAuthority {
        final AtomicInteger scans = new AtomicInteger();
        final CountDownLatch successfulRetry = new CountDownLatch(1);

        @Override
        public RunLifecycleRef lifecycle(String runId) {
            return null;
        }

        @Override
        public RunLifecyclePage scanLifecycle(String cursor, int limit) {
            if (scans.getAndIncrement() == 0) {
                throw new RunStateStoreUnavailableException(
                        null, "catalog unavailable", null);
            }
            successfulRetry.countDown();
            return RunLifecycleCatalogKernel.page(
                    cursor, limit, java.util.Collections.emptyList());
        }

        @Override
        public RunRetentionClaim claim(String runId, long expectedTerminalRevision,
                                       long cutoffMillis, RetentionOwner owner,
                                       long observedOwnershipEpoch) {
            throw new AssertionError("empty catalog must not claim");
        }

        @Override
        public boolean current(RetentionToken token) {
            return false;
        }

        @Override
        public RunPurgeProgress purge(RetentionToken token, int maxDeletes) {
            throw new AssertionError("empty catalog must not purge");
        }
    }

    private static final class CorruptShardAuthority
            implements RunLifecycleCatalog, RunRetentionAuthority {
        final RunLifecycleRef shardOne;
        final List<Integer> scannedShards =
                java.util.Collections.synchronizedList(new ArrayList<>());

        CorruptShardAuthority(String runId) {
            shardOne = RunLifecycleRef.terminal(
                    runId, 2L, RunStatus.CANCELED, "artifact", 10L);
        }

        @Override
        public RunLifecycleRef lifecycle(String runId) {
            return shardOne;
        }

        @Override
        public RunLifecyclePage scanLifecycle(String cursor, int limit) {
            RunLifecycleCatalogKernel.Cursor position =
                    RunLifecycleCatalogKernel.decodeCursor(cursor);
            scannedShards.add(position.shard());
            if (position.shard() == 0) {
                throw new IllegalStateException("corrupt lifecycle entry");
            }
            List<RunLifecycleRef> refs = position.shard() == 1
                    ? java.util.Collections.singletonList(shardOne)
                    : java.util.Collections.emptyList();
            return RunLifecycleCatalogKernel.page(cursor, limit, refs);
        }

        @Override
        public RunRetentionClaim claim(String runId, long expectedTerminalRevision,
                                       long cutoffMillis, RetentionOwner owner,
                                       long observedOwnershipEpoch) {
            return RunRetentionClaim.acquired(RetentionToken.create(
                    runId, owner, 1L, expectedTerminalRevision,
                    shardOne.terminalAtMillis()));
        }

        @Override
        public boolean current(RetentionToken token) {
            return true;
        }

        @Override
        public RunPurgeProgress purge(RetentionToken token, int maxDeletes) {
            return RunPurgeProgress.purged();
        }
    }
}
