package io.rowweave.commit;

import java.util.Arrays;
import java.util.Objects;

/** Bounded, connector-neutral projection of one durable batch Run publication. */
public final class BatchSinkOutcome {

    public enum State {
        NOT_FOUND,
        PREPARING,
        COMMIT_DECIDED,
        COMMITTED,
        ABORT_DECIDED,
        ABORTED,
        CORRUPT_OR_CONFLICT
    }

    private static final int SHA256_BYTES = 32;
    private static final int MAX_DETAIL_CODE_CHARS = 64;

    private final State state;
    private final Long generation;
    private final byte[] identityDigest;
    private final String detailCode;

    private BatchSinkOutcome(
            State state, Long generation, byte[] identityDigest, String detailCode) {
        if (state == null) {
            throw new IllegalArgumentException("batch sink outcome state is required");
        }
        if (generation != null && generation <= 0L) {
            throw new IllegalArgumentException("batch sink outcome generation must be positive");
        }
        if (identityDigest != null && identityDigest.length != SHA256_BYTES) {
            throw new IllegalArgumentException("batch sink outcome identity must be SHA-256");
        }
        if (detailCode != null && (detailCode.isEmpty()
                || detailCode.length() > MAX_DETAIL_CODE_CHARS
                || !detailCode.matches("[A-Z0-9_]+"))) {
            throw new IllegalArgumentException(
                    "batch sink outcome detailCode must be bounded and redacted");
        }
        if (state == State.NOT_FOUND
                && (generation != null || identityDigest != null || detailCode != null)) {
            throw new IllegalArgumentException("NOT_FOUND cannot carry publication evidence");
        }
        this.state = state;
        this.generation = generation;
        this.identityDigest = identityDigest == null ? null : identityDigest.clone();
        this.detailCode = detailCode;
    }

    public static BatchSinkOutcome of(
            State state, Long generation, byte[] identityDigest, String detailCode) {
        return new BatchSinkOutcome(state, generation, identityDigest, detailCode);
    }

    public static BatchSinkOutcome notFound() {
        return new BatchSinkOutcome(State.NOT_FOUND, null, null, null);
    }

    public State state() { return state; }
    public Long generation() { return generation; }
    public byte[] identityDigest() {
        return identityDigest == null ? null : identityDigest.clone();
    }
    public String detailCode() { return detailCode; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof BatchSinkOutcome)) return false;
        BatchSinkOutcome that = (BatchSinkOutcome) other;
        return state == that.state
                && Objects.equals(generation, that.generation)
                && Arrays.equals(identityDigest, that.identityDigest)
                && Objects.equals(detailCode, that.detailCode);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(state, generation, detailCode)
                + Arrays.hashCode(identityDigest);
    }
}
