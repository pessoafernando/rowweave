package io.rowweave.pipeline;

import java.nio.file.Paths;

public final class VersionedDatasetHandoffProcessFixture {
    private static final String DIGEST = repeat('a', 64);
    private static final String SCHEMA = repeat('b', 64);
    private static final String CONTENT = repeat('c', 64);

    private VersionedDatasetHandoffProcessFixture() { }

    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("root and mode required");
        FileVersionedDatasetHandoffAuthority authority =
                new FileVersionedDatasetHandoffAuthority(Paths.get(args[0]));
        VersionedDatasetProducerAttempt attempt = new VersionedDatasetProducerAttempt(
                key(), "producer-run", "attempt", 1L, 10L);
        VersionedDatasetCommitEvidence evidence = new VersionedDatasetCommitEvidence(
                attempt, "provider", 1, "version", "reader", 1,
                new byte[] {1}, CONTENT, 1L, 10L, 100L);
        if ("producer".equals(args[1])) {
            authority.claim(attempt);
            System.out.println("WINNER=" + authority.publish(evidence).winnerDigest());
            return;
        }
        if ("consumer".equals(args[1])) {
            VersionedDatasetVisibleWinner winner = authority.query(key()).get();
            VersionedDatasetConsumerRequest request =
                    new VersionedDatasetConsumerRequest(
                            key(), winner.winnerDigest(), "consumer-step",
                            "consumer-source", "consumer-artifact",
                            "consumer-run", "consumer-attempt");
            System.out.println("BINDING=" + authority.bind(request).bindingId());
            return;
        }
        throw new IllegalArgumentException("unknown mode");
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
}
