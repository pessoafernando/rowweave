package io.rowweave.mem.spill;

import java.io.IOException;

/**
 * structured spill-disk failure. Public messages deliberately exclude filesystem paths,
 * frame contents and the message of the underlying I/O exception.
 */
public final class SpillDiskException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private final SpillFailureCode code;
    private final long requestedBytes;
    private final long usedBytes;
    private final long limitBytes;

    private SpillDiskException(SpillFailureCode code, String detail,
                               long requestedBytes, long usedBytes, long limitBytes,
                               Throwable cause) {
        super(code.name() + (detail == null || detail.isEmpty() ? "" : " " + detail), cause);
        this.code = code;
        this.requestedBytes = requestedBytes;
        this.usedBytes = usedBytes;
        this.limitBytes = limitBytes;
    }

    public SpillFailureCode code() {
        return code;
    }

    public long requestedBytes() {
        return requestedBytes;
    }

    public long usedBytes() {
        return usedBytes;
    }

    public long limitBytes() {
        return limitBytes;
    }

    static SpillDiskException configInvalid(String key, Throwable cause) {
        String safeKey = key != null && key.matches("rowweave\\.worker\\.spill\\.[A-Za-z]+")
                ? key : "rowweave.worker.spill.configuration";
        return new SpillDiskException(SpillFailureCode.SPILL_CONFIG_INVALID,
                "key=" + safeKey, -1, -1, -1, cause);
    }

    static SpillDiskException workspaceUnavailable(Throwable cause) {
        return new SpillDiskException(SpillFailureCode.SPILL_WORKSPACE_UNAVAILABLE,
                null, -1, -1, -1, cause);
    }

    static SpillDiskException cleanupFailed(Throwable cause) {
        return new SpillDiskException(SpillFailureCode.SPILL_CLEANUP_FAILED,
                null, -1, -1, -1, cause);
    }

    static SpillDiskException ioWriteFailed(String identityDigestPrefix, IOException cause) {
        String identity = identityDigestPrefix != null
                && identityDigestPrefix.matches("[0-9a-f]{16}")
                ? identityDigestPrefix : "0000000000000000";
        return new SpillDiskException(SpillFailureCode.SPILL_IO_WRITE_FAILED,
                "id=" + identity, -1, -1, -1, cause);
    }

    /** Creates a bounded resource diagnostic; identity must already be a non-sensitive digest prefix. */
    public static SpillDiskException resourceLimit(
            SpillFailureCode code, long requestedBytes, long usedBytes, long limitBytes,
            String identityDigestPrefix, Throwable cause) {
        if (code != SpillFailureCode.SPILL_WORKER_QUOTA_EXCEEDED
                && code != SpillFailureCode.SPILL_JOB_QUOTA_EXCEEDED
                && code != SpillFailureCode.SPILL_MIN_FREE_SPACE) {
            throw new IllegalArgumentException("not a spill resource limit code: " + code);
        }
        String identity = identityDigestPrefix != null
                && identityDigestPrefix.matches("[0-9a-f]{16}")
                ? identityDigestPrefix : "0000000000000000";
        String detail = "id=" + identity
                + " requested=" + requestedBytes
                + " used=" + usedBytes
                + " limit=" + limitBytes;
        return new SpillDiskException(code, detail,
                requestedBytes, usedBytes, limitBytes, cause);
    }
}
