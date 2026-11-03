package io.rowweave.run;

/** Durable ownership fencing authority that must be passed before a terminal Run is deleted. */
public interface RunRetentionAuthority {
    RunRetentionClaim claim(String runId, long expectedTerminalRevision,
                            long cutoffMillis, RetentionOwner owner,
                            long observedOwnershipEpoch);

    /** Returns true only while the token is still the current durable retention owner of that Run. */
    boolean current(RetentionToken token);

    /** Performs one backend-bounded purge step; every step re-validates the exact token. */
    RunPurgeProgress purge(RetentionToken token, int maxDeletes);
}
