package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.IncrementalRunSubmission;
import io.rowweave.plan.ExecutionReport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** ZK-discovered clients expose the same incremental route. */
class ZkRemoteIncrementalClientContractTest {

    @Test
    void zkRemoteClientExposesIncrementalSubmissionWithoutAuthorityTokens()
            throws Exception {
        assertEquals(ExecutionReport.class,
                ZkRemoteJobClient.class.getMethod(
                        "submitIncrementalRun",
                        IncrementalRunSubmission.class).getReturnType());
    }
}
