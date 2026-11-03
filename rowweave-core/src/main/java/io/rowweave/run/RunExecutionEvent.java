package io.rowweave.run;

import java.util.Objects;

/** Redacted, fixed-length projection of a Run mutation event that is safe to publish. */
public final class RunExecutionEvent {
    public static final int SCHEMA_VERSION = 1;

    public enum Type {
        RUN_CREATED,
        ATTEMPT_CREATED,
        ATTEMPT_ADVANCED,
        CANCEL_REQUESTED,
        TIMEOUT_REQUESTED,
        FENCE_REQUESTED
    }

    public enum DiagnosticCategory {
        NONE,
        ADMISSION,
        DISPATCH,
        EXECUTION,
        CANCELED,
        TIMEOUT,
        FENCED,
        COMMIT,
        RECOVERY,
        RESOURCE,
        OTHER
    }

    private final long sequence;
    private final Type type;
    private final RunStatus runFrom;
    private final RunStatus runTo;
    private final AttemptStatus attemptFrom;
    private final AttemptStatus attemptTo;
    private final long eventTimeMillis;
    private final DiagnosticCategory diagnosticCategory;
    private final String mutationDigestPrefix;

    private RunExecutionEvent(long sequence, Type type,
                              RunStatus runFrom, RunStatus runTo,
                              AttemptStatus attemptFrom, AttemptStatus attemptTo,
                              long eventTimeMillis, DiagnosticCategory diagnosticCategory,
                              String mutationDigestPrefix) {
        this.sequence = sequence;
        this.type = type;
        this.runFrom = runFrom;
        this.runTo = runTo;
        this.attemptFrom = attemptFrom;
        this.attemptTo = attemptTo;
        this.eventTimeMillis = eventTimeMillis;
        this.diagnosticCategory = diagnosticCategory;
        this.mutationDigestPrefix = mutationDigestPrefix;
        validate();
    }

    public static RunExecutionEvent of(long sequence, Type type,
                                       RunStatus runFrom, RunStatus runTo,
                                       AttemptStatus attemptFrom, AttemptStatus attemptTo,
                                       long eventTimeMillis,
                                       DiagnosticCategory diagnosticCategory,
                                       String mutationDigestPrefix) {
        return new RunExecutionEvent(sequence, type, runFrom, runTo,
                attemptFrom, attemptTo, eventTimeMillis,
                diagnosticCategory, mutationDigestPrefix);
    }

    static RunExecutionEvent project(ExecutionEvent event) {
        if (event == null) throw new IllegalArgumentException("event is required");
        return of(event.sequence(), type(event.type()), event.runFrom(), event.runTo(),
                event.attemptFrom(), event.attemptTo(), event.eventTimeMillis(),
                diagnosticCategory(event.reasonCode()), digestPrefix(event.commandDigestUnsafe()));
    }

    public int schemaVersion() { return SCHEMA_VERSION; }
    public long sequence() { return sequence; }
    public Type type() { return type; }
    public RunStatus runFrom() { return runFrom; }
    public RunStatus runTo() { return runTo; }
    public AttemptStatus attemptFrom() { return attemptFrom; }
    public AttemptStatus attemptTo() { return attemptTo; }
    public long eventTimeMillis() { return eventTimeMillis; }
    public DiagnosticCategory diagnosticCategory() { return diagnosticCategory; }
    public String mutationDigestPrefix() { return mutationDigestPrefix; }

    private void validate() {
        if (sequence <= 0L || type == null || runTo == null
                || eventTimeMillis < 0L || diagnosticCategory == null
                || !isDigestPrefix(mutationDigestPrefix)) {
            throw new IllegalArgumentException("invalid public execution event scalar invariants");
        }
        if ((type == Type.RUN_CREATED) != (runFrom == null)) {
            throw new IllegalArgumentException("invalid run transition identity");
        }
        switch (type) {
            case RUN_CREATED:
                requireAttemptTransition(null, null);
                break;
            case ATTEMPT_CREATED:
                if (attemptFrom != null || attemptTo == null) {
                    throw new IllegalArgumentException("invalid attempt creation transition");
                }
                break;
            case ATTEMPT_ADVANCED:
            case FENCE_REQUESTED:
                requirePresentAttemptTransition();
                break;
            case CANCEL_REQUESTED:
            case TIMEOUT_REQUESTED:
                if ((attemptFrom == null) != (attemptTo == null)) {
                    throw new IllegalArgumentException(
                            "invalid cancellation/timeout attempt transition");
                }
                break;
            default:
                throw new IllegalArgumentException("unknown public event type");
        }
    }

    private void requireAttemptTransition(AttemptStatus expectedFrom, AttemptStatus expectedTo) {
        if (attemptFrom != expectedFrom || attemptTo != expectedTo) {
            throw new IllegalArgumentException("invalid attempt transition");
        }
    }

    private void requirePresentAttemptTransition() {
        if (attemptFrom == null || attemptTo == null) {
            throw new IllegalArgumentException("attempt transition is required");
        }
    }

    private static boolean isDigestPrefix(String value) {
        if (value == null || value.length() != 16) return false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
        }
        return true;
    }

    private static Type type(ExecutionEvent.Type type) {
        if (type == null) throw new IllegalArgumentException("event type is required");
        switch (type) {
            case RUN_CREATED: return Type.RUN_CREATED;
            case ATTEMPT_CREATED: return Type.ATTEMPT_CREATED;
            case ATTEMPT_ADVANCED: return Type.ATTEMPT_ADVANCED;
            case CANCEL_REQUESTED: return Type.CANCEL_REQUESTED;
            case TIMEOUT_REQUESTED: return Type.TIMEOUT_REQUESTED;
            case FENCE_REQUESTED: return Type.FENCE_REQUESTED;
            default: throw new IllegalArgumentException("unknown event type");
        }
    }

    private static DiagnosticCategory diagnosticCategory(String reasonCode) {
        if (reasonCode == null || reasonCode.isEmpty()) return DiagnosticCategory.NONE;
        switch (reasonCode) {
            case "ADMISSION_REJECTED":
            case "NO_SCHEDULABLE_WORKER":
                return DiagnosticCategory.ADMISSION;
            case "PRE_DISPATCH":
            case "ASSIGNMENT_DISPATCH_FAILED":
                return DiagnosticCategory.DISPATCH;
            case "STAGE_TERMINAL_FAILURE":
            case "COMMAND_EXECUTION_FAILED":
                return DiagnosticCategory.EXECUTION;
            case "CANCELED":
            case "CANCEL_REQUESTED":
                return DiagnosticCategory.CANCELED;
            case "TIMEOUT":
            case "TIMEOUT_REQUESTED":
                return DiagnosticCategory.TIMEOUT;
            case "FENCED":
            case "FENCE_REQUESTED":
                return DiagnosticCategory.FENCED;
            case "COMMIT_PREPUBLICATION_FAILED":
            case "COMMIT_PUBLICATION_FAILED":
                return DiagnosticCategory.COMMIT;
            case "RECOVERY_FAILED":
            case "ASSIGNMENTS_NONE_FENCED":
                return DiagnosticCategory.RECOVERY;
            case "CONTROL_RESIDENT_BUDGET_EXCEEDED":
                return DiagnosticCategory.RESOURCE;
            default:
                return DiagnosticCategory.OTHER;
        }
    }

    private static String digestPrefix(byte[] digest) {
        if (digest == null || digest.length < 8) {
            throw new IllegalArgumentException("command digest is too short");
        }
        char[] hex = new char[16];
        for (int i = 0; i < 8; i++) {
            int value = digest[i] & 0xff;
            hex[2 * i] = Character.forDigit(value >>> 4, 16);
            hex[2 * i + 1] = Character.forDigit(value & 0xf, 16);
        }
        return new String(hex);
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) return true;
        if (!(value instanceof RunExecutionEvent)) return false;
        RunExecutionEvent other = (RunExecutionEvent) value;
        return sequence == other.sequence
                && eventTimeMillis == other.eventTimeMillis
                && type == other.type
                && runFrom == other.runFrom
                && runTo == other.runTo
                && attemptFrom == other.attemptFrom
                && attemptTo == other.attemptTo
                && diagnosticCategory == other.diagnosticCategory
                && mutationDigestPrefix.equals(other.mutationDigestPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, type, runFrom, runTo, attemptFrom, attemptTo,
                eventTimeMillis, diagnosticCategory, mutationDigestPrefix);
    }

    @Override
    public String toString() {
        return "RunExecutionEvent{" +
                "sequence=" + sequence +
                ", type=" + type +
                ", runFrom=" + runFrom +
                ", runTo=" + runTo +
                ", attemptFrom=" + attemptFrom +
                ", attemptTo=" + attemptTo +
                ", eventTimeMillis=" + eventTimeMillis +
                ", diagnosticCategory=" + diagnosticCategory +
                ", mutationDigestPrefix='" + mutationDigestPrefix + '\'' +
                '}';
    }
}
