package io.rowweave.run;

/** Closed CAS result for durable Sink abort authority writes. */
public final class BatchSinkAbortDecision {

    public enum Code {
        RECORDED,
        ALREADY_RECORDED,
        STALE_RUN,
        ATTEMPT_NOT_ABORTABLE,
        COMMIT_ALREADY_DECIDED,
        RECORD_CONFLICT,
        RECOVERY_OWNED,
        UNAVAILABLE
    }

    private final Code code;
    private final BatchSinkAbortRecord record;

    private BatchSinkAbortDecision(Code code, BatchSinkAbortRecord record) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean carries = code == Code.RECORDED || code == Code.ALREADY_RECORDED;
        if (carries != (record != null)) {
            throw new IllegalArgumentException("abort record does not match decision code");
        }
        this.code = code;
        this.record = record;
    }

    public static BatchSinkAbortDecision recorded(BatchSinkAbortRecord record) {
        return new BatchSinkAbortDecision(Code.RECORDED, record);
    }

    public static BatchSinkAbortDecision alreadyRecorded(BatchSinkAbortRecord record) {
        return new BatchSinkAbortDecision(Code.ALREADY_RECORDED, record);
    }

    public static BatchSinkAbortDecision rejected(Code code) {
        if (code == Code.RECORDED || code == Code.ALREADY_RECORDED) {
            throw new IllegalArgumentException("recorded code requires an abort record");
        }
        return new BatchSinkAbortDecision(code, null);
    }

    public Code code() { return code; }
    public BatchSinkAbortRecord record() { return record; }
}
