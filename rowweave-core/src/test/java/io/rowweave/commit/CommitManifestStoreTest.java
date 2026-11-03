package io.rowweave.commit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Generation authority and the immutable manifest store. */
class CommitManifestStoreTest {

    @Test
    void manifestEncodingIsDeterministicBoundedAndCorruptionFailsLoud() {
        CommitManifest.Member a = member("a", "data/7/a.data", 11L, (byte) 1);
        CommitManifest.Member b = member("b", "data/7/b.data", 12L, (byte) 2);
        CommitManifest left = new CommitManifest("domain-1", 7L, "attempt-7", Arrays.asList(b, a));
        CommitManifest right = new CommitManifest("domain-1", 7L, "attempt-7", Arrays.asList(a, b));

        assertArrayEquals(left.encode(), right.encode(), "the member input order must not affect the durable manifest bytes");
        assertEquals(left, CommitManifest.decode(left.encode()));

        byte[] corrupt = left.encode();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CommitManifest.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> new CommitManifest("domain", 1L, "attempt", Arrays.asList(a, a)));
        assertThrows(IllegalArgumentException.class,
                () -> CommitManifest.decode(new byte[CommitManifest.MAX_ENCODED_BYTES + 1]));
    }

    @RepeatedTest(10)
    void generationReservationIsConcurrentPersistentAndAttemptIdempotent(@TempDir Path domain)
            throws Exception {
        CommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            @SuppressWarnings("unchecked")
            Future<Long>[] futures = new Future[32];
            for (int i = 0; i < futures.length; i++) {
                final int attempt = i;
                futures[i] = pool.submit((Callable<Long>) () ->
                        authority.reserve(domain, "attempt-" + attempt));
            }
            Set<Long> generations = new HashSet<>();
            for (Future<Long> future : futures) {
                generations.add(future.get());
            }
            assertEquals(32, generations.size());
            assertEquals(1L, Collections.min(generations));
            assertEquals(32L, Collections.max(generations));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(authority.reserve(domain, "attempt-3"),
                new FileCommitGenerationAuthority().reserve(domain, "attempt-3"),
                "the same attemptId must return the same generation across authority instances");
        assertEquals(33L, new FileCommitGenerationAuthority().reserve(domain, "attempt-32"));
    }

    @Test
    void generationReservationCanonicalizesSymlinkAliasesInOneJvm(@TempDir Path tmp) throws Exception {
        Path domain = Files.createDirectory(tmp.resolve("real-domain"));
        Path alias = tmp.resolve("domain-alias");
        Files.createSymbolicLink(alias, domain);
        CommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Long> real = pool.submit(() -> {
                start.await();
                return authority.reserve(domain, "real-attempt");
            });
            Future<Long> linked = pool.submit(() -> {
                start.await();
                return authority.reserve(alias, "alias-attempt");
            });
            start.countDown();
            Set<Long> values = new HashSet<>(Arrays.asList(real.get(), linked.get()));
            assertEquals(new HashSet<>(Arrays.asList(1L, 2L)), values);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void corruptedAuthorityMetadataFailsLoudWithoutReusingGeneration(@TempDir Path domain)
            throws Exception {
        FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        assertEquals(1L, authority.reserve(domain, "attempt-1"));
        Files.write(domain.resolve("authority").resolve("counter"), new byte[]{1, 2, 3});

        assertEquals(1L, authority.reserve(domain, "attempt-1"),
                "the outcome of an existing attempt claim must not be guessed from a corrupted counter");
        assertThrows(IllegalStateException.class,
                () -> authority.reserve(domain, "attempt-2"),
                "a corrupted counter must not allow scanning or resetting and then reusing a generation");
    }

    @Test
    void domainLeaseIsExclusiveAndCrashStyleCloseMakesItRecoverable(@TempDir Path domain) {
        try (FileCommitDomainLease first = FileCommitDomainLease.tryAcquire(domain)) {
            assertNotNull(first);
            assertNull(FileCommitDomainLease.tryAcquire(domain),
                    "while an active writer holds the domain lease, another writer or recovery must fail fast");
        }
        try (FileCommitDomainLease recovered = FileCommitDomainLease.tryAcquire(domain)) {
            assertNotNull(recovered, "after the writer finishes or crashes and releases the OS lock, recovery must be able to take the lease");
        }
    }

    @Test
    void unrelatedDomainsNeverConflictBecauseOfAnInternalLockStripe(@TempDir Path tmp) throws Exception {
        Path firstDomain = Files.createDirectory(tmp.resolve("domain-a"));
        int firstStripe = (firstDomain.toRealPath().toString().hashCode() & Integer.MAX_VALUE) % 64;
        Path secondDomain = null;
        Path realTmp = tmp.toRealPath();
        for (int i = 0; i < 10_000; i++) {
            String name = "domain-b-" + i;
            Path candidateIdentity = realTmp.resolve(name);
            int stripe = (candidateIdentity.toString().hashCode() & Integer.MAX_VALUE) % 64;
            if (stripe == firstStripe) {
                secondDomain = Files.createDirectory(tmp.resolve(name));
                break;
            }
        }
        assertNotNull(secondDomain, "test must find a deterministic stripe collision");

        try (FileCommitDomainLease first = FileCommitDomainLease.tryAcquire(firstDomain);
             FileCommitDomainLease second = FileCommitDomainLease.tryAcquire(secondDomain)) {
            assertNotNull(first);
            assertNotNull(second,
                    "unrelated domains may run concurrently even when an implementation hash collides");
        }
    }

    @Test
    void recoveryNeedsMatchingClaimAndNeverDeletesCommittedData(@TempDir Path domain) throws Exception {
        FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        long generation = authority.reserve(domain, "attempt-1");
        ManifestFileStore store = new ManifestFileStore(domain);
        Path staged = store.stagingPath("attempt-1", "member");
        Files.write(staged, "payload".getBytes(StandardCharsets.UTF_8));
        CommitManifest.Member member = store.installMember(generation, "member", "schema", staged);
        CommitManifest committed = new CommitManifest(
                store.domainId(), generation, "attempt-1", Collections.singletonList(member));
        store.publish(committed);

        ManifestFileRecovery.Result result = new ManifestFileRecovery(domain)
                .recoverConfirmedDeadAttempt("attempt-1", generation, 10);

        assertEquals(ManifestFileRecovery.Status.COMMITTED_PRESERVED, result.status());
        assertEquals(committed, store.exact(generation));
        assertNull(authority.find(domain, "missing-attempt"));
        assertThrows(IllegalArgumentException.class, () -> new ManifestFileRecovery(domain)
                .recoverConfirmedDeadAttempt("missing-attempt", generation, 10));
    }

    @Test
    void recoveryIsBoundedAndRefusesToRunWhileWriterLeaseIsActive(@TempDir Path domain)
            throws Exception {
        FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
        long generation = authority.reserve(domain, "orphan-attempt");
        ManifestFileStore store = new ManifestFileStore(domain);
        for (int i = 0; i < 5; i++) {
            Path staged = store.stagingPath("orphan-attempt", "m" + i);
            Files.write(staged, ("payload-" + i).getBytes(StandardCharsets.UTF_8));
            store.installMember(generation, "m" + i, "schema", staged);
        }
        ManifestFileRecovery recovery = new ManifestFileRecovery(domain);
        try (FileCommitDomainLease ignored = FileCommitDomainLease.tryAcquire(domain)) {
            assertEquals(ManifestFileRecovery.Status.ACTIVE_PRESERVED,
                    recovery.recoverConfirmedDeadAttempt(
                            "orphan-attempt", generation, 2).status());
        }

        ManifestFileRecovery.Result first = recovery.recoverConfirmedDeadAttempt(
                "orphan-attempt", generation, 2);
        assertEquals(ManifestFileRecovery.Status.INCOMPLETE, first.status());
        assertEquals(2, first.deletedEntries());
        ManifestFileRecovery.Result next;
        do {
            next = recovery.recoverConfirmedDeadAttempt("orphan-attempt", generation, 2);
        } while (next.status() == ManifestFileRecovery.Status.INCOMPLETE);
        assertEquals(ManifestFileRecovery.Status.ORPHAN_REMOVED, next.status());
        assertNull(store.exact(generation));
        assertEquals(0L, regularFileCount(domain.resolve("data").resolve(Long.toString(generation))));
        assertEquals(0L, regularFileCount(domain.resolve("staging")));
    }

    @Test
    void storePublishesImmutableManifestAndLatestIgnoresLateLowerGeneration(@TempDir Path domain)
            throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);
        assertNull(store.latest());

        CommitManifest generation2 = manifestWithData(store, domain, 2L, "attempt-2", "newer");
        CommitManifest generation1 = manifestWithData(store, domain, 1L, "attempt-1", "late-old");
        store.publish(generation2);
        store.publish(generation1);
        store.publish(generation2); // exact duplicate is idempotent

        assertEquals(generation2, store.latest());
        CommitManifest conflicting = new CommitManifest(
                generation2.domainId(), generation2.generation(), "different-attempt", generation2.members());
        assertThrows(IllegalStateException.class, () -> store.publish(conflicting));
    }

    @Test
    void concurrentDifferentContentCanNeverReplaceAnImmutableGeneration(@TempDir Path domain)
            throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);
        CommitManifest first = manifestWithData(store, domain, 1L, "attempt-a", "same-data");
        CommitManifest second = new CommitManifest(
                first.domainId(), 1L, "attempt-b", first.members());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> a = pool.submit(() -> store.publish(first));
            Future<?> b = pool.submit(() -> store.publish(second));
            int successes = 0;
            int conflicts = 0;
            for (Future<?> future : Arrays.asList(a, b)) {
                try {
                    future.get();
                    successes++;
                } catch (java.util.concurrent.ExecutionException expected) {
                    if (expected.getCause() instanceof IllegalStateException) {
                        conflicts++;
                    } else {
                        throw expected;
                    }
                }
            }
            assertEquals(1, successes);
            assertEquals(1, conflicts);
        } finally {
            pool.shutdownNow();
        }
        CommitManifest visible = store.latest();
        assertEquals(1L, visible.generation());
        org.junit.jupiter.api.Assertions.assertTrue(
                "attempt-a".equals(visible.attemptId()) || "attempt-b".equals(visible.attemptId()));
    }

    @Test
    void highestManifestCorruptionNeverFallsBackToOlderGeneration(@TempDir Path domain) throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);
        store.publish(manifestWithData(store, domain, 1L, "attempt-1", "old"));
        Path corrupt = domain.resolve("manifests").resolve("2.manifest");
        Files.write(corrupt, "corrupt".getBytes(StandardCharsets.UTF_8));

        assertThrows(IllegalStateException.class, store::latest,
                "a corrupt record at the highest generation must fail loudly and never fall back to an older committed result");
    }

    @Test
    void publishRevalidatesLengthAndContentHash(@TempDir Path domain) throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);
        CommitManifest valid = manifestWithData(store, domain, 1L, "attempt-1", "payload");
        CommitManifest.Member original = valid.members().get(0);
        CommitManifest wrong = new CommitManifest(valid.domainId(), 1L, "attempt-1",
                Collections.singletonList(new CommitManifest.Member(
                        original.name(), original.relativeDataPath(), original.schemaFingerprint(),
                        original.length() + 1L, original.sha256())));

        assertThrows(IllegalStateException.class, () -> store.publish(wrong));
        assertNull(store.latest());
    }

    @Test
    void latestRevalidatesReferencedDataAndRejectsTraversal(@TempDir Path domain) throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);
        CommitManifest manifest = manifestWithData(store, domain, 1L, "attempt-1", "payload");
        store.publish(manifest);
        Files.write(domain.resolve(manifest.members().get(0).relativeDataPath()),
                "tampered".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalStateException.class, store::latest);

        assertThrows(IllegalArgumentException.class, () -> new CommitManifest.Member(
                "result", "../outside", "schema", 1L, new byte[CommitManifest.SHA256_BYTES]));
    }

    @Test
    void manifestSymlinkIsRejectedInsteadOfFollowingExternalMetadata(@TempDir Path domain)
            throws Exception {
        Path manifests = domain.resolve("manifests");
        Files.createDirectories(manifests);
        Path external = domain.resolve("external.manifest");
        Files.write(external, "not-owned".getBytes(StandardCharsets.UTF_8));
        try {
            Files.createSymbolicLink(manifests.resolve("1.manifest"), external);
        } catch (UnsupportedOperationException unsupported) {
            return;
        }
        assertThrows(IllegalStateException.class, new ManifestFileStore(domain)::latest);
    }

    @Test
    void immutablePublishUsesRequiredDurabilityOrder(@TempDir Path domain) throws Exception {
        List<DurableFileOps.Step> steps = new ArrayList<>();
        ManifestFileStore store = new ManifestFileStore(domain,
                (step, target) -> steps.add(step));
        store.publish(manifestWithData(store, domain, 1L, "attempt-1", "payload"));

        assertEquals(Arrays.asList(
                DurableFileOps.Step.TEMP_FILE_FORCED,
                DurableFileOps.Step.IMMUTABLE_NAME_CREATED,
                DurableFileOps.Step.DIRECTORY_FORCED_AFTER_CREATE,
                DurableFileOps.Step.TEMP_NAME_REMOVED,
                DurableFileOps.Step.DIRECTORY_FORCED_AFTER_CLEANUP), steps);
    }

    @Test
    void injectedFailureBeforeNameKeepsOldButAfterNameIsQueryableAndIdempotent(@TempDir Path tmp)
            throws Exception {
        Path beforeDomain = tmp.resolve("before-name");
        ManifestFileStore before = new ManifestFileStore(beforeDomain, (step, target) -> {
            if (step == DurableFileOps.Step.TEMP_FILE_FORCED) {
                throw new InjectedFailure();
            }
        });
        CommitManifest beforeManifest = manifestWithData(
                before, beforeDomain, 1L, "attempt-before", "payload");
        assertThrows(InjectedFailure.class, () -> before.publish(beforeManifest));
        assertNull(new ManifestFileStore(beforeDomain).latest(),
                "a failure before the immutable name is created may leave only invisible staging");

        Path afterDomain = tmp.resolve("after-name");
        ManifestFileStore after = new ManifestFileStore(afterDomain, (step, target) -> {
            if (step == DurableFileOps.Step.IMMUTABLE_NAME_CREATED) {
                throw new InjectedFailure();
            }
        });
        CommitManifest afterManifest = manifestWithData(
                after, afterDomain, 1L, "attempt-after", "payload");
        assertThrows(InjectedFailure.class, () -> after.publish(afterManifest));
        assertEquals(afterManifest, new ManifestFileStore(afterDomain).latest(),
                "a failure after the name exists but before the ACK must recover the outcome by querying the manifest");
        new ManifestFileStore(afterDomain).publish(afterManifest);
    }

    private static CommitManifest manifestWithData(
            ManifestFileStore store, Path domain, long generation, String attempt, String value) throws Exception {
        Path relative = java.nio.file.Paths.get("data", Long.toString(generation), "result.data");
        Path data = domain.resolve(relative);
        Files.createDirectories(data.getParent());
        Files.write(data, value.getBytes(StandardCharsets.UTF_8));
        return new CommitManifest(store.domainId(), generation, attempt,
                Collections.singletonList(new CommitManifest.Member(
                        "result", relative.toString(), "schema-v1", Files.size(data),
                        ManifestFileStore.sha256(data))));
    }

    private static CommitManifest.Member member(
            String name, String relativePath, long length, byte fill) {
        byte[] hash = new byte[CommitManifest.SHA256_BYTES];
        Arrays.fill(hash, fill);
        return new CommitManifest.Member(name, relativePath, "schema-v1", length, hash);
    }

    private static final class InjectedFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static long regularFileCount(Path root) throws Exception {
        if (!Files.exists(root)) {
            return 0L;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }
}
