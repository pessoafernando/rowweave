package io.rowweave.run;

import io.rowweave.plan.StageExecutionTelemetry;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Durable, bounded proof of one accepted physical assignment's first terminal outcome. */
public final class StageTerminalReceipt {
    public static final int DIGEST_BYTES = 32;

    private final StageAssignmentToken token;
    private final byte[] commandDigest;
    private final StageTerminalOutcome outcome;
    private final long inputRows;
    private final long outputRows;
    private final String failureCategory;
    private final byte[] failureDigest;
    private final long completedAtMillis;
    private final StageExecutionTelemetry telemetry;

    private StageTerminalReceipt(StageAssignmentToken token, byte[] commandDigest,
                                 StageTerminalOutcome outcome,
                                 long inputRows, long outputRows,
                                 String failureCategory, byte[] failureDigest,
                                 long completedAtMillis,
                                 StageExecutionTelemetry telemetry) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (commandDigest == null || commandDigest.length != DIGEST_BYTES) {
            throw new IllegalArgumentException("commandDigest must be SHA-256");
        }
        if (outcome == null) throw new IllegalArgumentException("outcome is required");
        if (inputRows < 0L || outputRows < 0L) {
            throw new IllegalArgumentException("terminal row counts must be >= 0");
        }
        if (completedAtMillis < 0L || (outcome == StageTerminalOutcome.LOST
                && completedAtMillis != 0L)) {
            throw new IllegalArgumentException(
                    "completedAtMillis must be >= 0 and LOST must use zero");
        }
        if (outcome == StageTerminalOutcome.SUCCEEDED) {
            if (failureCategory != null || failureDigest != null) {
                throw new IllegalArgumentException(
                        "successful terminal receipt cannot contain failure evidence");
            }
        } else {
            RunText.required(failureCategory, "failureCategory",
                    RunText.MAX_REASON_CODE_BYTES);
            if (failureDigest == null || failureDigest.length != DIGEST_BYTES) {
                throw new IllegalArgumentException("failureDigest must be SHA-256");
            }
        }
        if (telemetry == null) {
            throw new IllegalArgumentException("terminal telemetry is required");
        }
        this.token = token;
        this.commandDigest = commandDigest.clone();
        this.outcome = outcome;
        this.inputRows = inputRows;
        this.outputRows = outputRows;
        this.failureCategory = failureCategory;
        this.failureDigest = failureDigest == null ? null : failureDigest.clone();
        this.completedAtMillis = completedAtMillis;
        this.telemetry = telemetry;
    }

    public static StageTerminalReceipt succeeded(
            StageAssignmentToken token, byte[] commandDigest,
            long inputRows, long outputRows, long completedAtMillis) {
        return new StageTerminalReceipt(token, commandDigest,
                StageTerminalOutcome.SUCCEEDED, inputRows, outputRows,
                null, null, completedAtMillis, StageExecutionTelemetry.unavailable());
    }

    public static StageTerminalReceipt succeeded(
            StageAssignmentToken token, byte[] commandDigest,
            long inputRows, long outputRows, long completedAtMillis,
            StageExecutionTelemetry telemetry) {
        return new StageTerminalReceipt(token, commandDigest,
                StageTerminalOutcome.SUCCEEDED, inputRows, outputRows,
                null, null, completedAtMillis, telemetry);
    }

    public static StageTerminalReceipt failed(
            StageAssignmentToken token, byte[] commandDigest,
            long inputRows, long outputRows, String failureCategory,
            byte[] failureDigest, long completedAtMillis) {
        return failedOutcome(token, commandDigest, StageTerminalOutcome.FAILED,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, StageExecutionTelemetry.unavailable());
    }

    public static StageTerminalReceipt failed(
            StageAssignmentToken token, byte[] commandDigest,
            long inputRows, long outputRows, String failureCategory,
            byte[] failureDigest, long completedAtMillis,
            StageExecutionTelemetry telemetry) {
        return failedOutcome(token, commandDigest, StageTerminalOutcome.FAILED,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, telemetry);
    }

    public static StageTerminalReceipt canceled(
            StageAssignmentToken token, byte[] commandDigest,
            long inputRows, long outputRows, String failureCategory,
            byte[] failureDigest, long completedAtMillis) {
        return failedOutcome(token, commandDigest, StageTerminalOutcome.CANCELED,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, StageExecutionTelemetry.unavailable());
    }

    public static StageTerminalReceipt canceled(
            StageAssignmentToken token, byte[] commandDigest,
            long inputRows, long outputRows, String failureCategory,
            byte[] failureDigest, long completedAtMillis,
            StageExecutionTelemetry telemetry) {
        return failedOutcome(token, commandDigest, StageTerminalOutcome.CANCELED,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, telemetry);
    }

    public static StageTerminalReceipt lost(
            StageAssignmentToken token, byte[] commandDigest,
            String failureCategory, byte[] failureDigest,
            long completedAtMillis) {
        return failedOutcome(token, commandDigest, StageTerminalOutcome.LOST,
                0L, 0L, failureCategory, failureDigest, completedAtMillis,
                StageExecutionTelemetry.unavailable());
    }

    public static StageTerminalReceipt lost(
            StageAssignmentToken token, byte[] commandDigest,
            String failureCategory, byte[] failureDigest,
            long completedAtMillis, StageExecutionTelemetry telemetry) {
        return failedOutcome(token, commandDigest, StageTerminalOutcome.LOST,
                0L, 0L, failureCategory, failureDigest, completedAtMillis, telemetry);
    }

    static StageTerminalReceipt decoded(
            StageAssignmentToken token, byte[] commandDigest,
            StageTerminalOutcome outcome, long inputRows, long outputRows,
            String failureCategory, byte[] failureDigest,
            long completedAtMillis) {
        return new StageTerminalReceipt(token, commandDigest, outcome,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, StageExecutionTelemetry.unavailable());
    }

    static StageTerminalReceipt decoded(
            StageAssignmentToken token, byte[] commandDigest,
            StageTerminalOutcome outcome, long inputRows, long outputRows,
            String failureCategory, byte[] failureDigest,
            long completedAtMillis, StageExecutionTelemetry telemetry) {
        return new StageTerminalReceipt(token, commandDigest, outcome,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, telemetry);
    }

    private static StageTerminalReceipt failedOutcome(
            StageAssignmentToken token, byte[] commandDigest,
            StageTerminalOutcome outcome, long inputRows, long outputRows,
            String failureCategory, byte[] failureDigest,
            long completedAtMillis, StageExecutionTelemetry telemetry) {
        return new StageTerminalReceipt(token, commandDigest, outcome,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, telemetry);
    }

    public StageAssignmentToken token() { return token; }
    public byte[] commandDigest() { return commandDigest.clone(); }
    public StageTerminalOutcome outcome() { return outcome; }
    public long inputRows() { return inputRows; }
    public long outputRows() { return outputRows; }
    public String failureCategory() { return failureCategory; }
    public byte[] failureDigest() {
        return failureDigest == null ? null : failureDigest.clone();
    }
    public long completedAtMillis() { return completedAtMillis; }
    public StageExecutionTelemetry telemetry() { return telemetry; }
    public boolean succeeded() { return outcome == StageTerminalOutcome.SUCCEEDED; }

    public StageTerminalReceipt withTelemetry(StageExecutionTelemetry replacement) {
        if (telemetry.equals(replacement)) return this;
        return new StageTerminalReceipt(token, commandDigest, outcome,
                inputRows, outputRows, failureCategory, failureDigest,
                completedAtMillis, replacement);
    }

    /** Base terminal identity/outcome equality, deliberately excluding advisory telemetry. */
    public boolean hasSameTerminalBase(StageTerminalReceipt other) {
        return other != null
                && inputRows == other.inputRows
                && outputRows == other.outputRows
                && completedAtMillis == other.completedAtMillis
                && token.equals(other.token)
                && outcome == other.outcome
                && Objects.equals(failureCategory, other.failureCategory)
                && MessageDigest.isEqual(commandDigest, other.commandDigest)
                && Arrays.equals(failureDigest, other.failureDigest);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StageTerminalReceipt)) return false;
        StageTerminalReceipt that = (StageTerminalReceipt) other;
        return hasSameTerminalBase(that) && telemetry.equals(that.telemetry);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(token, outcome, inputRows, outputRows,
                failureCategory, completedAtMillis);
        result = 31 * result + Arrays.hashCode(commandDigest);
        result = 31 * result + Arrays.hashCode(failureDigest);
        return 31 * result + telemetry.hashCode();
    }
}
