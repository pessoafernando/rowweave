package io.rowweave.pipeline;

import java.util.Optional;
import java.util.function.LongSupplier;

/** Unified handoff state machine over a bounded durable CAS record store. */
public final class StoreBackedVersionedDatasetHandoffAuthority
        implements VersionedDatasetHandoffAuthority {
    private static final int MAX_CAS_RETRIES = 128;

    private final VersionedDatasetAuthorityRecordStore store;
    private final LongSupplier clock;

    public StoreBackedVersionedDatasetHandoffAuthority(
            VersionedDatasetAuthorityRecordStore store) {
        this(store, System::currentTimeMillis);
    }

    StoreBackedVersionedDatasetHandoffAuthority(
            VersionedDatasetAuthorityRecordStore store, LongSupplier clock) {
        if (store == null || clock == null) {
            throw new IllegalArgumentException("store and clock are required");
        }
        this.store = store;
        this.clock = clock;
    }

    @Override
    public VersionedDatasetProducerAttempt claim(
            VersionedDatasetProducerAttempt attempt) {
        if (attempt == null) throw new IllegalArgumentException("attempt is required");
        String id = attempt.key().keyDigest();
        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            Optional<VersionedDatasetAuthorityRecord> current = store.read(
                    VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF, id);
            if (!current.isPresent()) {
                byte[] replacement = VersionedDatasetAuthorityRecordCodec.encode(
                        new VersionedDatasetAuthorityRecordCodec.Slot(attempt, null));
                if (store.compareAndSet(
                        VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF,
                        id, null, replacement)) return attempt;
                continue;
            }
            VersionedDatasetAuthorityRecordCodec.Slot slot =
                    VersionedDatasetAuthorityRecordCodec.decode(
                            current.get().bytes());
            requireKey(slot.attempt.key(), attempt.key());
            if (slot.attempt.equals(attempt)) return slot.attempt;
            if (slot.winner != null) {
                throw failure("ALREADY_VISIBLE",
                        "a visible winner permanently closes the producer claim");
            }
            long number = attempt.attemptNumber();
            long currentNumber = slot.attempt.attemptNumber();
            long fence = attempt.fenceToken();
            long currentFence = slot.attempt.fenceToken();
            if (number <= currentNumber && fence <= currentFence
                    && (number < currentNumber || fence < currentFence)) {
                throw failure("STALE_PRODUCER_ATTEMPT",
                        "producer attempt is older than the durable claim");
            }
            if (number <= currentNumber || fence <= currentFence) {
                throw failure("ATTEMPT_FENCE_CONFLICT",
                        "attempt number and fence token must advance together");
            }
            byte[] replacement = VersionedDatasetAuthorityRecordCodec.encode(
                    new VersionedDatasetAuthorityRecordCodec.Slot(attempt, null));
            if (store.compareAndSet(
                    VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF,
                    id, current.get().versionToken(), replacement)) return attempt;
        }
        throw contention();
    }

    @Override
    public Optional<VersionedDatasetProducerAttempt> queryClaim(
            VersionedDatasetHandoffKey key) {
        requireKeyArgument(key);
        Optional<VersionedDatasetAuthorityRecord> record = store.read(
                VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF,
                key.keyDigest());
        if (!record.isPresent()) return Optional.empty();
        VersionedDatasetAuthorityRecordCodec.Slot slot =
                VersionedDatasetAuthorityRecordCodec.decode(record.get().bytes());
        requireKey(slot.attempt.key(), key);
        return Optional.of(slot.attempt);
    }

    @Override
    public VersionedDatasetVisibleWinner publish(
            VersionedDatasetCommitEvidence evidence) {
        if (evidence == null) throw new IllegalArgumentException("evidence is required");
        VersionedDatasetProducerAttempt proposed = evidence.attempt();
        String id = proposed.key().keyDigest();
        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            Optional<VersionedDatasetAuthorityRecord> current = store.read(
                    VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF, id);
            if (!current.isPresent()) {
                throw failure("ATTEMPT_NOT_CLAIMED",
                        "producer attempt must be durably claimed before publish");
            }
            VersionedDatasetAuthorityRecordCodec.Slot slot =
                    VersionedDatasetAuthorityRecordCodec.decode(
                            current.get().bytes());
            requireKey(slot.attempt.key(), proposed.key());
            if (slot.winner != null) {
                if (slot.winner.evidence().equals(evidence)) return slot.winner;
                throw failure("EVIDENCE_CONFLICT",
                        "the handoff already has different visible evidence");
            }
            if (!slot.attempt.equals(proposed)) {
                throw failure("PUBLISH_ATTEMPT_MISMATCH",
                        "commit evidence does not belong to the current claim");
            }
            VersionedDatasetVisibleWinner winner =
                    new VersionedDatasetVisibleWinner(evidence, now());
            byte[] replacement = VersionedDatasetAuthorityRecordCodec.encode(
                    new VersionedDatasetAuthorityRecordCodec.Slot(
                            slot.attempt, winner));
            if (store.compareAndSet(
                    VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF,
                    id, current.get().versionToken(), replacement)) return winner;
        }
        throw contention();
    }

    @Override
    public Optional<VersionedDatasetVisibleWinner> query(
            VersionedDatasetHandoffKey key) {
        requireKeyArgument(key);
        Optional<VersionedDatasetAuthorityRecord> record = store.read(
                VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF,
                key.keyDigest());
        if (!record.isPresent()) return Optional.empty();
        VersionedDatasetAuthorityRecordCodec.Slot slot =
                VersionedDatasetAuthorityRecordCodec.decode(record.get().bytes());
        requireKey(slot.attempt.key(), key);
        return Optional.ofNullable(slot.winner);
    }

    @Override
    public VersionedDatasetConsumerBinding bind(
            VersionedDatasetConsumerRequest request) {
        if (request == null) throw new IllegalArgumentException("request is required");
        String id = bindingId(request);
        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            Optional<VersionedDatasetAuthorityRecord> existing = store.read(
                    VersionedDatasetAuthorityRecordStore.RecordKind.CONSUMER_BINDING,
                    id);
            if (existing.isPresent()) {
                VersionedDatasetConsumerBinding binding =
                        VersionedDatasetHandoffCodec.decodeConsumerBinding(
                                existing.get().bytes());
                if (binding.request().equals(request)) return binding;
                throw failure("CONSUMER_BINDING_CONFLICT",
                        "the consumer identity is already bound to another winner");
            }
            Optional<VersionedDatasetVisibleWinner> visible = query(request.key());
            if (!visible.isPresent()) {
                throw failure("NOT_VISIBLE",
                        "consumer cannot bind before a winner is visible");
            }
            if (!visible.get().winnerDigest().equals(
                    request.expectedWinnerDigest())) {
                throw failure("WINNER_MISMATCH",
                        "consumer requested a digest other than the visible winner");
            }
            VersionedDatasetConsumerBinding binding =
                    new VersionedDatasetConsumerBinding(request, visible.get(), now());
            if (store.compareAndSet(
                    VersionedDatasetAuthorityRecordStore.RecordKind.CONSUMER_BINDING,
                    id, null,
                    VersionedDatasetHandoffCodec.encodeConsumerBinding(binding))) {
                return binding;
            }
        }
        throw contention();
    }

    @Override
    public Optional<VersionedDatasetConsumerBinding> queryBinding(
            VersionedDatasetConsumerRequest request) {
        if (request == null) throw new IllegalArgumentException("request is required");
        Optional<VersionedDatasetAuthorityRecord> record = store.read(
                VersionedDatasetAuthorityRecordStore.RecordKind.CONSUMER_BINDING,
                bindingId(request));
        if (!record.isPresent()) return Optional.empty();
        VersionedDatasetConsumerBinding binding =
                VersionedDatasetHandoffCodec.decodeConsumerBinding(
                        record.get().bytes());
        if (!binding.request().equals(request)) {
            throw failure("CONSUMER_BINDING_CONFLICT",
                    "the consumer identity is bound to different evidence");
        }
        return Optional.of(binding);
    }

    private long now() {
        long value = clock.getAsLong();
        if (value <= 0L) {
            throw failure("INVALID_AUTHORITY_TIME",
                    "authority clock must return a positive epoch timestamp");
        }
        return value;
    }

    private static String bindingId(VersionedDatasetConsumerRequest request) {
        String identity = request.key().keyDigest() + '\u0000'
                + request.consumerStepId() + '\u0000'
                + request.consumerSourceOperatorId() + '\u0000'
                + request.consumerArtifactId() + '\u0000'
                + request.consumerRunId() + '\u0000'
                + request.consumerAttemptId();
        return VersionedDatasetHandoffCodec.sha256Hex(
                identity.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static void requireKeyArgument(VersionedDatasetHandoffKey key) {
        if (key == null) throw new IllegalArgumentException("handoff key is required");
    }

    private static void requireKey(
            VersionedDatasetHandoffKey durable,
            VersionedDatasetHandoffKey supplied) {
        if (!durable.equals(supplied)) {
            throw failure("KEY_DIGEST_CONFLICT",
                    "handoff key does not match the durable digest owner");
        }
    }

    private static VersionedDatasetHandoffException contention() {
        return failure("AUTHORITY_CONTENTION",
                "authority CAS retry budget was exhausted");
    }

    private static VersionedDatasetHandoffException failure(
            String code, String detail) {
        return new VersionedDatasetHandoffException(code, detail);
    }
}
