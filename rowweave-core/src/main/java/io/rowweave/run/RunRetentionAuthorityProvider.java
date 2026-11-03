package io.rowweave.run;

/** Optional infrastructure capability: hands the Master retention fencing under the same authority as the RunStateStore. */
public interface RunRetentionAuthorityProvider {
    RunRetentionAuthority runRetentionAuthority();
}
