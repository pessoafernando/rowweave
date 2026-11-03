package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryVersionedDatasetHandoffAuthorityTest {
    private static final String DIGEST = repeat('a', 64);
    private static final String SCHEMA = repeat('b', 64);
    private static final String CONTENT = repeat('c', 64);

    @Test
    void claimRequiresBothAttemptAndFenceToAdvanceMonotonically() {
        InMemoryVersionedDatasetHandoffAuthority authority = authority();
        VersionedDatasetProducerAttempt first = attempt(1, 10, "first");
        assertEquals(first, authority.claim(first));
        assertEquals(first, authority.claim(first));

        assertStateFailure("ATTEMPT_FENCE_CONFLICT",
                () -> authority.claim(attempt(1, 10, "same-fence-other-id")));
        assertStateFailure("ATTEMPT_FENCE_CONFLICT",
                () -> authority.claim(attempt(2, 10, "number-only")));
        assertStateFailure("ATTEMPT_FENCE_CONFLICT",
                () -> authority.claim(attempt(1, 11, "fence-only")));
        assertStateFailure("STALE_PRODUCER_ATTEMPT",
                () -> authority.claim(attempt(1, 9, "older")));

        VersionedDatasetProducerAttempt second = attempt(2, 11, "second");
        assertEquals(second, authority.claim(second));
        assertEquals(second, authority.queryClaim(key()).get());
        assertStateFailure("STALE_PRODUCER_ATTEMPT",
                () -> authority.claim(first));
    }

    @Test
    void publishIsFirstDurableWinnerAndLostNotificationUsesQuery() {
        InMemoryVersionedDatasetHandoffAuthority authority = authority();
        VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
        VersionedDatasetCommitEvidence evidence = evidence(attempt, "v1", 1);

        assertFalse(authority.query(key()).isPresent());
        assertStateFailure("ATTEMPT_NOT_CLAIMED", () -> authority.publish(evidence));
        authority.claim(attempt);
        authority.publish(evidence); // response/notification is deliberately ignored

        VersionedDatasetVisibleWinner recovered = authority.query(key()).get();
        assertEquals(evidence, recovered.evidence());
        assertEquals(recovered, authority.publish(evidence));
        assertStateFailure("EVIDENCE_CONFLICT",
                () -> authority.publish(evidence(attempt, "v2", 2)));
        assertStateFailure("ALREADY_VISIBLE",
                () -> authority.claim(attempt(2, 11, "retry-after-visible")));
    }

    @Test
    void retryFencesLateOldAttemptBeforeVisibility() {
        InMemoryVersionedDatasetHandoffAuthority authority = authority();
        VersionedDatasetProducerAttempt old = attempt(1, 10, "old");
        VersionedDatasetProducerAttempt current = attempt(2, 11, "current");
        authority.claim(old);
        authority.claim(current);

        assertStateFailure("PUBLISH_ATTEMPT_MISMATCH",
                () -> authority.publish(evidence(old, "late-old", 1)));
        VersionedDatasetVisibleWinner winner = authority.publish(
                evidence(current, "current", 2));
        assertEquals("current", winner.evidence().versionId());
    }

    @Test
    void consumerCannotReadEarlyOrRebindAfterRecovery() {
        InMemoryVersionedDatasetHandoffAuthority authority = authority();
        VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
        VersionedDatasetCommitEvidence evidence = evidence(attempt, "v1", 1);
        VersionedDatasetVisibleWinner proposed = new VersionedDatasetVisibleWinner(
                evidence, 1000L);
        VersionedDatasetConsumerRequest early = request(proposed.winnerDigest());

        assertStateFailure("NOT_VISIBLE", () -> authority.bind(early));
        assertFalse(authority.queryBinding(early).isPresent());

        authority.claim(attempt);
        VersionedDatasetVisibleWinner winner = authority.publish(evidence);
        VersionedDatasetConsumerRequest request = request(winner.winnerDigest());
        VersionedDatasetConsumerBinding binding = authority.bind(request);
        assertEquals(binding, authority.bind(request));
        assertEquals(binding, authority.queryBinding(request).get());
        assertEquals(winner, binding.winner());

        VersionedDatasetConsumerRequest differentWinner = request(repeat('d', 64));
        assertStateFailure("CONSUMER_BINDING_CONFLICT",
                () -> authority.queryBinding(differentWinner));
        assertStateFailure("WINNER_MISMATCH",
                () -> authority.bind(new VersionedDatasetConsumerRequest(
                        key(), repeat('e', 64), "other-step", "other-source",
                        "other-artifact", "other-run", "other-attempt")));
    }

    @Test
    void concurrentDifferentEvidenceProducesExactlyOneWinner() throws Exception {
        InMemoryVersionedDatasetHandoffAuthority authority = authority();
        VersionedDatasetProducerAttempt attempt = attempt(1, 10, "attempt");
        authority.claim(attempt);
        VersionedDatasetCommitEvidence left = evidence(attempt, "left", 1);
        VersionedDatasetCommitEvidence right = evidence(attempt, "right", 2);
        CountDownLatch start = new CountDownLatch(1);
        List<Object> outcomes = Collections.synchronizedList(new ArrayList<Object>());

        Thread first = publisher(authority, left, start, outcomes);
        Thread second = publisher(authority, right, start, outcomes);
        first.start();
        second.start();
        start.countDown();
        first.join();
        second.join();

        int success = 0;
        int conflict = 0;
        for (Object outcome : outcomes) {
            if (outcome instanceof VersionedDatasetVisibleWinner) success++;
            if (outcome instanceof VersionedDatasetHandoffException
                    && "EVIDENCE_CONFLICT".equals(
                    ((VersionedDatasetHandoffException) outcome).code())) conflict++;
        }
        assertEquals(1, success);
        assertEquals(1, conflict);
        Optional<VersionedDatasetVisibleWinner> winner = authority.query(key());
        assertTrue(winner.isPresent());
        assertTrue(winner.get().evidence().equals(left)
                || winner.get().evidence().equals(right));
    }

    private static Thread publisher(
            VersionedDatasetHandoffAuthority authority,
            VersionedDatasetCommitEvidence evidence, CountDownLatch start,
            List<Object> outcomes) {
        return new Thread(() -> {
            try {
                start.await();
                outcomes.add(authority.publish(evidence));
            } catch (Throwable failure) {
                outcomes.add(failure);
            }
        });
    }

    private static InMemoryVersionedDatasetHandoffAuthority authority() {
        AtomicLong time = new AtomicLong(1000L);
        return new InMemoryVersionedDatasetHandoffAuthority(time::incrementAndGet);
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

    private static VersionedDatasetHandoffKey key() {
        return new VersionedDatasetHandoffKey(
                "pipeline", "revision", DIGEST, "edge", "dataset", SCHEMA,
                "producer-step", "producer-sink", "producer-artifact", "logical-run");
    }

    private static void assertStateFailure(String code, Runnable action) {
        VersionedDatasetHandoffException failure = assertThrows(
                VersionedDatasetHandoffException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
