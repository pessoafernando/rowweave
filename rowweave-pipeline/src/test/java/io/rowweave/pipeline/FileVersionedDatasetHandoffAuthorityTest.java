package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileVersionedDatasetHandoffAuthorityTest {
    private static final String DIGEST = repeat('a', 64);
    private static final String SCHEMA = repeat('b', 64);
    private static final String CONTENT = repeat('c', 64);

    @TempDir
    Path temporary;

    @Test
    void independentInstancesRecoverClaimWinnerAndBinding() {
        AtomicLong time = new AtomicLong(1000L);
        FileVersionedDatasetHandoffAuthority first = authority(time);
        VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
        first.claim(attempt);

        FileVersionedDatasetHandoffAuthority second = authority(time);
        assertEquals(attempt, second.queryClaim(key()).get());
        VersionedDatasetVisibleWinner winner = second.publish(
                evidence(attempt, "version", 1));
        VersionedDatasetConsumerRequest request = request(winner.winnerDigest());
        VersionedDatasetConsumerBinding binding = first.bind(request);

        FileVersionedDatasetHandoffAuthority recovered = authority(time);
        assertEquals(winner, recovered.query(key()).get());
        assertEquals(binding, recovered.queryBinding(request).get());
        assertEquals(binding, recovered.bind(request));
    }

    @Test
    @org.junit.jupiter.api.parallel.ResourceLock("default-locale")
    void consumerBindingRemainsUniqueAcrossHostLocales() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.US);
            AtomicLong time = new AtomicLong(1000L);
            FileVersionedDatasetHandoffAuthority first = authority(time);
            VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
            first.claim(attempt);
            VersionedDatasetVisibleWinner winner = first.publish(
                    evidence(attempt, "version", 1));
            VersionedDatasetConsumerRequest request = request(winner.winnerDigest());
            VersionedDatasetConsumerBinding binding = first.bind(request);

            Locale.setDefault(new Locale("tr", "TR"));
            FileVersionedDatasetHandoffAuthority recovered = authority(time);
            assertEquals(binding, recovered.queryBinding(request).get());
            assertEquals(binding, recovered.bind(request));
            assertEquals(winner, recovered.query(key()).get());
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void durableWriteThenAckLossIsRecoveredByQuery() {
        AtomicLong time = new AtomicLong(2000L);
        AtomicBoolean failNext = new AtomicBoolean();
        FileVersionedDatasetHandoffAuthority authority =
                new FileVersionedDatasetHandoffAuthority(
                        temporary, time::incrementAndGet, () -> {
                    if (failNext.compareAndSet(true, false)) {
                        throw new SimulatedAckLoss();
                    }
                });
        VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
        authority.claim(attempt);
        failNext.set(true);
        assertThrows(SimulatedAckLoss.class,
                () -> authority.publish(evidence(attempt, "version", 1)));

        FileVersionedDatasetHandoffAuthority recovered = authority(time);
        VersionedDatasetVisibleWinner winner = recovered.query(key()).get();
        assertEquals("version", winner.evidence().versionId());
        assertEquals(winner,
                recovered.publish(evidence(attempt, "version", 1)));

        VersionedDatasetConsumerRequest request = request(winner.winnerDigest());
        failNext.set(true);
        assertThrows(SimulatedAckLoss.class, () -> authority.bind(request));
        VersionedDatasetConsumerBinding binding = authority(time)
                .queryBinding(request).get();
        assertEquals(winner, binding.winner());
        assertEquals(binding, authority(time).bind(request));
    }

    @Test
    void concurrentInstancesStillChooseOneEvidence() throws Exception {
        AtomicLong time = new AtomicLong(3000L);
        FileVersionedDatasetHandoffAuthority leftAuthority = authority(time);
        FileVersionedDatasetHandoffAuthority rightAuthority = authority(time);
        VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
        leftAuthority.claim(attempt);
        VersionedDatasetCommitEvidence left = evidence(attempt, "left", 1);
        VersionedDatasetCommitEvidence right = evidence(attempt, "right", 2);
        CountDownLatch start = new CountDownLatch(1);
        List<Object> results = Collections.synchronizedList(new ArrayList<Object>());

        Thread a = publisher(leftAuthority, left, start, results);
        Thread b = publisher(rightAuthority, right, start, results);
        a.start();
        b.start();
        start.countDown();
        a.join();
        b.join();

        assertEquals(2, results.size());
        assertEquals(1, results.stream()
                .filter(VersionedDatasetVisibleWinner.class::isInstance).count());
        assertEquals(1, results.stream()
                .filter(value -> value instanceof VersionedDatasetHandoffException
                        && "EVIDENCE_CONFLICT".equals(
                        ((VersionedDatasetHandoffException) value).code())).count());
        assertTrue(authority(time).query(key()).isPresent());
    }

    @Test
    void corruptRecordFailsClosedAndUnreferencedTempIsIgnored() throws IOException {
        AtomicLong time = new AtomicLong(4000L);
        FileVersionedDatasetHandoffAuthority authority = authority(time);
        authority.claim(attempt(1, 10, "attempt"));
        Path record;
        try (java.util.stream.Stream<Path> files = Files.walk(temporary)) {
            record = files.filter(path -> path.getFileName().toString().endsWith(".record"))
                    .findFirst().get();
        }
        Files.write(record, new byte[] {1, 2, 3},
                StandardOpenOption.TRUNCATE_EXISTING);
        PipelineValidationException corrupt = assertThrows(
                PipelineValidationException.class, () -> authority.query(key()));
        assertEquals("FRAME_LENGTH_MISMATCH", corrupt.code());

        Path otherRoot = temporary.resolve("other");
        FileVersionedDatasetHandoffAuthority clean =
                new FileVersionedDatasetHandoffAuthority(otherRoot);
        clean.claim(attempt(1, 10, "attempt"));
        Files.write(otherRoot.resolve("orphan.tmp"), new byte[] {9});
        assertEquals("attempt", clean.queryClaim(key()).get().attemptId());
    }

    @Test
    void aPublishTornBeforeTheRenameIsNeverAdoptedAndOneRetryWins()
            throws IOException {
        AtomicLong time = new AtomicLong(5000L);
        FileVersionedDatasetHandoffAuthority authority = authority(time);
        VersionedDatasetProducerAttempt claim = attempt(1, 10, "attempt");
        authority.claim(claim);

        Path record = durableRecord();
        VersionedDatasetVisibleWinner abandoned = new VersionedDatasetVisibleWinner(
                evidence(claim, "torn-version", 5), 6000L);
        byte[] orphanBytes = VersionedDatasetAuthorityRecordCodec.encode(
                new VersionedDatasetAuthorityRecordCodec.Slot(claim, abandoned));
        Path orphan = record.resolveSibling(record.getFileName().toString()
                + "." + java.util.UUID.randomUUID() + ".tmp");
        Files.write(orphan, orphanBytes);

        // The orphan is a complete, decodable winner record: only the rename
        // could have made it durable, so ignoring it is the behaviour at stake.
        assertEquals("torn-version", VersionedDatasetAuthorityRecordCodec.decode(
                orphanBytes).winner.evidence().versionId());

        FileVersionedDatasetHandoffAuthority recovered = authority(new AtomicLong(7000L));
        assertFalse(recovered.query(key()).isPresent());
        assertEquals(claim, recovered.queryClaim(key()).get());

        VersionedDatasetVisibleWinner winner = recovered.publish(
                evidence(claim, "committed-version", 9));
        assertEquals("committed-version", winner.evidence().versionId());
        assertEquals(winner, authority(new AtomicLong(8000L)).query(key()).get());
        assertTrue(Files.exists(orphan));
        assertEquals(1L, durableRecordCount());
    }

    @Test
    void anOrphanedTemporaryRecordIsNotDurableForAFreshAuthority()
            throws IOException {
        VersionedDatasetProducerAttempt claim = attempt(1, 11, "attempt-crashed");
        VersionedDatasetVisibleWinner abandoned = new VersionedDatasetVisibleWinner(
                evidence(claim, "torn-version", 6), 6100L);
        byte[] slot = VersionedDatasetAuthorityRecordCodec.encode(
                new VersionedDatasetAuthorityRecordCodec.Slot(claim, abandoned));

        String recordId = key().keyDigest();
        Path records = temporary.resolve("orphan-only");
        Path directory = records.resolve("records").resolve("handoff")
                .resolve(recordId.substring(0, 2));
        Files.createDirectories(directory);
        Files.write(directory.resolve(recordId + ".record."
                + java.util.UUID.randomUUID() + ".tmp"), slot);

        FileVersionedDatasetHandoffAuthority fresh =
                new FileVersionedDatasetHandoffAuthority(
                        temporary.resolve("orphan-only"));
        assertFalse(fresh.query(key()).isPresent());
        assertFalse(fresh.queryClaim(key()).isPresent());

        VersionedDatasetProducerAttempt published = fresh.claim(claim);
        assertEquals(claim, published);
        VersionedDatasetVisibleWinner winner = fresh.publish(
                evidence(published, "second-chance", 7));
        assertEquals("second-chance", winner.evidence().versionId());
        assertEquals(winner, new FileVersionedDatasetHandoffAuthority(
                temporary.resolve("orphan-only")).query(key()).get());
    }

    private FileVersionedDatasetHandoffAuthority authority(AtomicLong time) {
        return new FileVersionedDatasetHandoffAuthority(
                temporary, time::incrementAndGet, () -> { });
    }

    private static Thread publisher(
            VersionedDatasetHandoffAuthority authority,
            VersionedDatasetCommitEvidence evidence, CountDownLatch start,
            List<Object> results) {
        return new Thread(() -> {
            try {
                start.await();
                results.add(authority.publish(evidence));
            } catch (Throwable failure) {
                results.add(failure);
            }
        });
    }

    private static VersionedDatasetConsumerRequest request(String winnerDigest) {
        return new VersionedDatasetConsumerRequest(key(), winnerDigest,
                "consumer-step", "consumer-source", "consumer-artifact",
                "consumer-run", "consumer-attempt");
    }

    private static VersionedDatasetCommitEvidence evidence(
            VersionedDatasetProducerAttempt attempt, String version, int marker) {
        return new VersionedDatasetCommitEvidence(attempt, "provider", 1,
                version, "reader", 1, new byte[] {(byte) marker}, CONTENT,
                marker, marker * 10L, 100L + marker);
    }

    private static VersionedDatasetProducerAttempt attempt(
            long attemptNumber, long fence, String attemptId) {
        return new VersionedDatasetProducerAttempt(
                key(), "producer-run", attemptId, attemptNumber, fence);
    }

    private Path durableRecord() throws IOException {
        try (java.util.stream.Stream<Path> files = Files.walk(temporary)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".record"))
                    .findFirst().get();
        }
    }

    private long durableRecordCount() throws IOException {
        try (java.util.stream.Stream<Path> files = Files.walk(temporary)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".record"))
                    .count();
        }
    }

    private static VersionedDatasetHandoffKey key() {
        return new VersionedDatasetHandoffKey(
                "pipeline", "revision", DIGEST, "edge", "dataset", SCHEMA,
                "producer-step", "producer-sink", "producer-artifact", "logical-run");
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }

    private static final class SimulatedAckLoss extends RuntimeException { }
}
