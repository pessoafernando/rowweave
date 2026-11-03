package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public final class WholeGraphProgressiveAwaitProbe {

    private WholeGraphProgressiveAwaitProbe() {
    }

    public static void main(String[] args) throws Exception {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.unary("filter", meta, 0).param("expr", "v >= 0").build(),
                PlanNode.sink(meta, 1, new DiscardSink(meta)).build()));
        ExecutionGraph executionGraph = ExecutionGraphBuilder.build(graph);
        if (!StageAssignment.assign(executionGraph, 1, Collections.singletonList("w")).isWholeGraph()) {
            System.err.println("probe graph did not select WHOLE_GRAPH");
            System.exit(3);
        }

        String host = "whole-graph-progress-worker";
        int port = 61_046;
        String incarnation = "inc-whole-graph-progress";
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        try (MasterServer master = new MasterServer(1_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    Thread reporter = new Thread(() -> reportAfterDelay(
                            master, request.workerId(), request.stageId(), incarnation,
                            request.runAttemptFence()), "whole-graph-probe-reporter");
                    reporter.setDaemon(true);
                    reporter.start();
                } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                    cancellations.incrementAndGet();
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker("w", host, port, 8, -1, incarnation));
                long started = System.nanoTime();
                RpcMessage result = master.handler().apply(RpcMessage.submitJob(
                        graph.toBytes(), "whole-graph-progress",
                        EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 1_000L)));
                long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

                boolean passed = result.success() && result.rowCount() == 7L && elapsedMs >= 150L
                        && assignments.get() == 1 && cancellations.get() == 0
                        && master.activeStageTrackerCount() == 0 && master.activeStageAssignmentCount() == 0;
                if (!passed) {
                    System.err.println("resultSuccess=" + result.success()
                            + " rows=" + result.rowCount()
                            + " error=" + result.errorMessage()
                            + " elapsedMs=" + elapsedMs
                            + " assignments=" + assignments.get()
                            + " cancellations=" + cancellations.get()
                            + " trackers=" + master.activeStageTrackerCount()
                            + " owners=" + master.activeStageAssignmentCount());
                    System.exit(2);
                }
                System.out.println("WHOLE_GRAPH_PROGRESSIVE_OK elapsedMs=" + elapsedMs);
            } finally {
                endpointRegistration.close();
            }
        }
    }

    private static void reportAfterDelay(MasterServer master, String workerId, long stageId,
                                         String incarnation,
                                         io.rowweave.run.RunAttemptFence runAttemptFence) {
        long deadline = System.nanoTime() + 200_000_000L;
        try {
            while (System.nanoTime() < deadline) {
                master.handler().apply(RpcMessage.heartbeat(workerId, 7, incarnation));
                Thread.sleep(10L);
            }
            master.handler().apply(RpcMessage.stageReport(
                    workerId, stageId, true, 7L, 7L, null, incarnation, runAttemptFence));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}

