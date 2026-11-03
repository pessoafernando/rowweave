package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunSubmission;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** the real client writes budgets only on ephemeral submit envelopes. */
class RealClusterJobClientEphemeralTimeoutTest {
    private static final String EXECUTION_KEY = "rowweave.cluster.submitTimeoutMs";
    private static final String CLEANUP_KEY = "rowweave.cluster.submitCleanupGraceMs";
    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void realClientWritesConfiguredBudgetForBothEphemeralEntrypointsButNotDurableRun() {
        String previousExecution = System.getProperty(EXECUTION_KEY);
        String previousCleanup = System.getProperty(CLEANUP_KEY);
        System.setProperty(EXECUTION_KEY, "12000");
        System.setProperty(CLEANUP_KEY, "3000");
        List<RpcMessage> requests = Collections.synchronizedList(new ArrayList<>());
        try (RpcServer server = new RpcServer(request -> {
            requests.add(request);
            return RpcMessage.jobResult(true, 0L, null);
        }); RealClusterJobClient client =
                     new RealClusterJobClient("127.0.0.1", server.port())) {
            client.submitAttempt(rawGraph(), "attempt-budget");
            client.submitArtifactAttempt(artifact(), "publication-budget");
            client.submitRun(artifact(), new RunSubmission(
                    "run-budget", "attempt-run", "publication-run", 1, 0L, 10L));

            assertEquals(3, requests.size());
            assertEquals(RpcMessage.Type.SUBMIT_JOB, requests.get(0).type());
            assertBudget(requests.get(0).ephemeralSubmitTimeouts());
            assertEquals(RpcMessage.Type.SUBMIT_PLAN_ARTIFACT, requests.get(1).type());
            assertBudget(requests.get(1).ephemeralSubmitTimeouts());
            assertEquals(RpcMessage.Type.SUBMIT_RUN, requests.get(2).type());
            assertNull(requests.get(2).ephemeralSubmitTimeouts());
        } finally {
            restore(EXECUTION_KEY, previousExecution);
            restore(CLEANUP_KEY, previousCleanup);
        }
    }

    private static PlanGraph rawGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()));
    }

    private static PlanArtifact artifact() {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "timeout-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return LegacyPlanArtifactAdapter.wrap("timeout-client", graph);
    }

    private static void assertBudget(EphemeralSubmitTimeoutPolicy.Budget budget) {
        assertEquals(12_000L, budget.executionTimeoutMs());
        assertEquals(3_000L, budget.cleanupTimeoutMs());
    }

    private static void restore(String key, String previous) {
        if (previous == null) System.clearProperty(key);
        else System.setProperty(key, previous);
    }
}
