package io.rowweave.commit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bounded orphan janitor fenced by lease and retention. */
class ManifestFileJanitorTest {

    @Test
    void activeLeaseAndFreshClaimAreNeverCleaned(@TempDir Path domain) throws Exception {
        AtomicLong clock = new AtomicLong(10_000L);
        Orphan orphan = orphan(domain, "active-or-fresh", "payload");
        ManifestFileJanitor janitor = new ManifestFileJanitor(
                domain, 1_000L, 16, 16, clock::get);

        try (FileCommitDomainLease ignored = FileCommitDomainLease.tryAcquire(domain)) {
            ManifestFileJanitor.Report busy = janitor.runOnce();
            assertEquals(ManifestFileJanitor.Status.ACTIVE_PRESERVED, busy.status());
            assertTrue(Files.exists(orphan.data));
        }

        Files.setLastModifiedTime(orphan.claim, FileTime.fromMillis(9_500L));
        ManifestFileJanitor.Report fresh = janitor.runOnce();
        assertEquals(ManifestFileJanitor.Status.COMPLETED, fresh.status());
        assertEquals(0, fresh.recoveredAttempts());
        assertTrue(Files.exists(orphan.data));
    }

    @Test
    void expiredClaimWithoutAuthorityProofIsPreserved(@TempDir Path domain) throws Exception {
        AtomicLong clock = new AtomicLong(20_000L);
        Orphan orphan = orphan(domain, "still-live", "payload");
        Files.setLastModifiedTime(orphan.claim, FileTime.fromMillis(1_000L));

        ManifestFileJanitor.Report report = new ManifestFileJanitor(
                domain, 1_000L, 16, 16, clock::get,
                (domainId, attemptId, generation) ->
                        ManifestCleanupAuthority.Decision.PRESERVE_LIVE).runOnce();

        assertEquals(0, report.recoveredAttempts());
        assertTrue(Files.exists(orphan.data));
    }

    @Test
    void durableAbortDecisionAllowsBoundedCleanupButVisibleManifestWins(
            @TempDir Path domain) throws Exception {
        Orphan aborted = orphan(domain, "aborted", "payload-a");
        Files.setLastModifiedTime(aborted.claim, FileTime.fromMillis(1_000L));
        Orphan committed = orphan(domain, "committed", "payload-c");
        new ManifestFileStore(domain).publish(new CommitManifest(
                new ManifestFileStore(domain).domainId(), committed.generation,
                committed.attemptId, Collections.singletonList(committed.member)));
        Files.setLastModifiedTime(committed.claim, FileTime.fromMillis(1_000L));

        ManifestFileJanitor janitor = ManifestFileJanitor.withCleanupAuthority(
                domain, 1_000L, 32, 32,
                (domainId, attemptId, generation) ->
                        ManifestCleanupAuthority.Decision.DELETE_ABORTED);
        ManifestFileJanitor.Report report = janitor.runOnce();

        assertEquals(2, report.recoveredAttempts());
        assertFalse(Files.exists(aborted.data));
        assertTrue(Files.exists(committed.data));
    }

    @Test
    void expiredOrphanIsRemovedButCommittedGenerationIsOnlyFinalized(@TempDir Path domain)
            throws Exception {
        AtomicLong clock = new AtomicLong(20_000L);
        Orphan orphan = orphan(domain, "expired-orphan", "orphan");
        Files.setLastModifiedTime(orphan.claim, FileTime.fromMillis(1_000L));

        Orphan committedFiles = orphan(domain, "committed-attempt", "committed");
        ManifestFileStore store = new ManifestFileStore(domain);
        CommitManifest committed = new CommitManifest(store.domainId(), committedFiles.generation,
                committedFiles.attemptId, Collections.singletonList(committedFiles.member));
        store.publish(committed);
        Files.setLastModifiedTime(committedFiles.claim, FileTime.fromMillis(1_000L));

        ManifestFileJanitor.Report report = new ManifestFileJanitor(
                domain, 1_000L, 32, 32, clock::get,
                unreferencedExpired()).runOnce();

        assertEquals(ManifestFileJanitor.Status.COMPLETED, report.status());
        assertEquals(2, report.recoveredAttempts());
        assertFalse(Files.exists(orphan.data));
        assertNotNull(store.exact(committedFiles.generation));
        assertTrue(Files.exists(domain.resolve(committedFiles.member.relativeDataPath())));
        assertFalse(Files.exists(committedFiles.staged),
                "a committed attempt may only finalize staging, never delete manifest data");
    }

    @Test
    void cleanupFailureCreatesDurableQuarantineAndLaterRetryClearsIt(@TempDir Path domain)
            throws Exception {
        AtomicLong clock = new AtomicLong(30_000L);
        FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        String attemptId = "quarantine-attempt";
        long generation = authority.reserve(domain, attemptId);
        Path claim = authority.claimPath(domain, attemptId);
        Files.setLastModifiedTime(claim, FileTime.fromMillis(1_000L));
        Path conflictingDirectory = domain.resolve("data").resolve(Long.toString(generation))
                .resolve("not-a-file");
        Files.createDirectories(conflictingDirectory);
        ManifestFileJanitor janitor = new ManifestFileJanitor(
                domain, 1_000L, 16, 16, clock::get, unreferencedExpired());

        ManifestFileJanitor.Report failed = janitor.runOnce();

        assertEquals(1, failed.quarantinedAttempts());
        Path quarantine = janitor.quarantineRecord(attemptId);
        assertTrue(Files.isRegularFile(quarantine));
        assertTrue(new String(Files.readAllBytes(quarantine), StandardCharsets.UTF_8)
                .contains("not-a-file"));

        Files.delete(conflictingDirectory);
        ManifestFileJanitor.Report retried = janitor.runOnce();
        assertEquals(1, retried.recoveredAttempts());
        assertFalse(Files.exists(quarantine), "a later successful recovery must clear the quarantine tombstone");
    }

    @Test
    void eachRunHonorsClaimAndDeleteBudgets(@TempDir Path domain) throws Exception {
        AtomicLong clock = new AtomicLong(40_000L);
        for (int i = 0; i < 5; i++) {
            Orphan orphan = orphan(domain, "budget-" + i, "payload-" + i);
            Files.setLastModifiedTime(orphan.claim, FileTime.fromMillis(1_000L));
        }
        ManifestFileJanitor janitor = new ManifestFileJanitor(
                domain, 1_000L, 2, 2, clock::get, unreferencedExpired());

        ManifestFileJanitor.Report report = janitor.runOnce();

        assertTrue(report.examinedClaims() <= 2);
        assertTrue(report.deletedEntries() <= 4,
                "maxDeletesPerAttempt=2 and at most 2 claims examined per round");
        int rounds = 1;
        while (regularFileCount(domain.resolve("data")) > 0 && rounds < 300) {
            ManifestFileJanitor.Report next = janitor.runOnce();
            assertTrue(next.examinedClaims() <= 2);
            assertTrue(next.deletedEntries() <= 4);
            rounds++;
        }
        assertEquals(0L, regularFileCount(domain.resolve("data")),
                "the rotating shard cursor must eventually process claims beyond the budget");
    }

    private static Orphan orphan(Path domain, String attemptId, String payload) throws Exception {
        FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        long generation = authority.reserve(domain, attemptId);
        Path claim = authority.claimPath(domain, attemptId);
        ManifestFileStore store = new ManifestFileStore(domain);
        Path staged = store.stagingPath(attemptId, "member");
        Files.write(staged, payload.getBytes(StandardCharsets.UTF_8));
        CommitManifest.Member member = store.installMember(
                generation, "member", "schema", staged);
        Path data = domain.resolve(member.relativeDataPath());
        return new Orphan(attemptId, generation, claim, staged, data, member);
    }

    private static ManifestCleanupAuthority unreferencedExpired() {
        return (domainId, attemptId, generation) ->
                ManifestCleanupAuthority.Decision.DELETE_UNREFERENCED_EXPIRED;
    }

    private static long regularFileCount(Path root) throws Exception {
        if (!Files.exists(root)) {
            return 0L;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private static final class Orphan {
        private final String attemptId;
        private final long generation;
        private final Path claim;
        private final Path staged;
        private final Path data;
        private final CommitManifest.Member member;

        private Orphan(String attemptId, long generation, Path claim, Path staged,
                       Path data, CommitManifest.Member member) {
            this.attemptId = attemptId;
            this.generation = generation;
            this.claim = claim;
            this.staged = staged;
            this.data = data;
            this.member = member;
        }
    }
}
