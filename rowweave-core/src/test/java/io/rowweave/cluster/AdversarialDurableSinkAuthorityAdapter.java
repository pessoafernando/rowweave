package io.rowweave.cluster;

import io.rowweave.ProviderBatchSink;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.RunAttemptFence;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Test-only durable authority proving the Master coordinator is provider-neutral. */
public final class AdversarialDurableSinkAuthorityAdapter
        implements DurableSinkAuthorityAdapterFactory {
    private final AtomicLong generations = new AtomicLong();
    private final Map<String, State> states = new ConcurrentHashMap<>();

    @Override
    public Reservation reserve(
            RunAttemptFence fence, String publicationId,
            List<ProviderBatchSink> outputs) {
        if (outputs == null || outputs.isEmpty()) {
            throw new IllegalArgumentException("adversarial outputs are required");
        }
        String[] first = config(outputs.get(0));
        for (ProviderBatchSink output : outputs) {
            if (!first[0].equals(config(output)[0])) {
                throw new IllegalArgumentException("adversarial domains differ");
            }
        }
        String key = key(fence, publicationId);
        State created = new State(fence, publicationId,
                generations.incrementAndGet(), Paths.get(first[0]));
        State existing = states.putIfAbsent(key, created);
        State state = existing == null ? created : existing;
        return new Reserved(state);
    }

    @Override
    public BatchSinkOutcome query(
            RunAttemptFence fence, String publicationId) {
        State state = states.get(key(fence, publicationId));
        return state == null ? BatchSinkOutcome.notFound() : state.outcome();
    }

    private static String key(RunAttemptFence fence, String publicationId) {
        return fence.runId() + '\0' + fence.attemptId() + '\0'
                + fence.fenceToken() + '\0' + publicationId;
    }

    private static String[] config(ProviderBatchSink output) {
        String value = new String(
                output.configurationBytes(), StandardCharsets.UTF_8);
        int split = value.indexOf('\n');
        if (split <= 0 || split == value.length() - 1) {
            throw new IllegalArgumentException("adversarial config is invalid");
        }
        return new String[] {value.substring(0, split), value.substring(split + 1)};
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class Reserved implements Reservation {
        private final State state;
        private Reserved(State state) { this.state = state; }
        @Override public long generation() { return state.generation; }
        @Override public DurableSinkAuthorityAdapter bind(
                List<PreparedSinkLocator> locators) {
            return state.bind(locators);
        }
        @Override public void close() { }
    }

    private static final class State {
        private final RunAttemptFence fence;
        private final String publicationId;
        private final long generation;
        private final Path root;
        private volatile BatchSinkOutcome.State authorityState =
                BatchSinkOutcome.State.PREPARING;
        private volatile byte[] decisionDigest;

        private State(RunAttemptFence fence, String publicationId,
                      long generation, Path root) {
            this.fence = fence;
            this.publicationId = publicationId;
            this.generation = generation;
            this.root = root;
        }

        DurableSinkAuthorityAdapter bind(List<PreparedSinkLocator> locators) {
            List<PreparedSinkLocator> frozen = Collections.unmodifiableList(
                    new ArrayList<>(locators));
            return new DurableSinkAuthorityAdapter() {
                @Override public byte[] requireCommitDecision() {
                    if (frozen.isEmpty()) {
                        throw new IllegalStateException("locator closure is empty");
                    }
                    List<String> members = new ArrayList<>(frozen.size());
                    for (PreparedSinkLocator locator : frozen) {
                        if (!locator.attemptIdentity().fence().equals(fence)) {
                            throw new IllegalStateException("locator fence mismatch");
                        }
                        members.add(new String(
                                locator.payload(), StandardCharsets.UTF_8));
                    }
                    byte[] candidate = sha256(
                            members.toString().getBytes(StandardCharsets.UTF_8));
                    synchronized (State.this) {
                        if (authorityState == BatchSinkOutcome.State.ABORT_DECIDED
                                || authorityState == BatchSinkOutcome.State.ABORTED) {
                            throw new IllegalStateException("abort already decided");
                        }
                        if (decisionDigest != null && !MessageDigest.isEqual(
                                decisionDigest, candidate)) {
                            throw new IllegalStateException("commit decision conflict");
                        }
                        decisionDigest = candidate;
                        authorityState = BatchSinkOutcome.State.COMMIT_DECIDED;
                        return candidate.clone();
                    }
                }

                @Override public BatchSinkOutcome authorityOutcome() {
                    return outcome();
                }

                @Override public byte[] requireAbortDecision(String reasonCode) {
                    byte[] candidate = sha256((publicationId + '\0' + reasonCode)
                            .getBytes(StandardCharsets.UTF_8));
                    synchronized (State.this) {
                        if (authorityState == BatchSinkOutcome.State.COMMIT_DECIDED
                                || authorityState == BatchSinkOutcome.State.COMMITTED) {
                            throw new IllegalStateException("commit already decided");
                        }
                        decisionDigest = candidate;
                        authorityState = BatchSinkOutcome.State.ABORT_DECIDED;
                        return candidate.clone();
                    }
                }

                @Override public void completeAbort() {
                    synchronized (State.this) {
                        if (authorityState != BatchSinkOutcome.State.ABORT_DECIDED) {
                            throw new IllegalStateException("abort was not decided");
                        }
                        authorityState = BatchSinkOutcome.State.ABORTED;
                    }
                }
            };
        }

        BatchSinkOutcome outcome() {
            BatchSinkOutcome.State observed = authorityState;
            byte[] digest = decisionDigest;
            if (observed == BatchSinkOutcome.State.COMMIT_DECIDED
                    && Files.exists(root.resolve("publications")
                    .resolve(generation + ".visible"))) {
                observed = BatchSinkOutcome.State.COMMITTED;
                authorityState = observed;
            }
            return BatchSinkOutcome.of(observed, generation,
                    digest, observed == BatchSinkOutcome.State.PREPARING
                            ? "AWAITING_COMMIT_DECISION" : null);
        }
    }
}
