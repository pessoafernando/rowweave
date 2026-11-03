package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MasterMuxFullBatchTest {
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.BigDecimalType));

    private static final class EmptyInput implements Source {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    @Test
    void sourceAndSinkAssignmentsCanBothEnterBeforeAnyAck() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(ShuffleTransportConfig.muxEnabled(),
                "with rowweave.shuffle.mux=false explicitly set, the mux-specific batch contract is skipped");
        String previous = System.getProperty("rowweave.cluster.assign.parallelism");
        System.setProperty("rowweave.cluster.assign.parallelism", "4");
        CountDownLatch allEntered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Set<StageCommand.Role> roles = ConcurrentHashMap.newKeySet();
        DataSchema grouped = ParallelGroupByAgg.of(META, "k").sum("v").getOutputSchema();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new EmptyInput())),
                PlanNode.unary("groupBySum", grouped, 0)
                        .listParam("keys", Collections.singletonList("k")).param("measure", "v").build(),
                PlanNode.sink(grouped, 1, new DiscardSink(grouped)).build()), 1);

        try (MasterServer master = new MasterServer(5000L);
             RpcServer fakeWorker = new RpcServer(request -> {
                 if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                     allEntered.countDown();
                     try {
                         roles.add(StageCommandCodecV2.preflight(
                                 request.stageCommandBlob(), ignored -> { }).materialize(
                                 ObjectTrustContext.localTrusted(getClass().getClassLoader()),
                                 ignored -> { }).role());
                         if (!release.await(2, TimeUnit.SECONDS)) {
                             return RpcMessage.jobResult(false, 0, "full mux batch did not overlap");
                         }
                         master.handler().apply(RpcMessage.stageReport(
                                 request.workerId(), request.stageId(), true, 0, 0, null,
                                 request.incarnationId(), request.runAttemptFence()));
                     } catch (Exception e) {
                         return RpcMessage.jobResult(false, 0, e.toString());
                     }
                 }
                 return RpcMessage.ack();
             })) {
            master.handler().apply(RpcMessage.registerWorker(
                    "mux-worker", "127.0.0.1", fakeWorker.port(), 8, 19000));
            java.util.concurrent.CompletableFuture<Void> submit = java.util.concurrent.CompletableFuture.runAsync(
                    () -> new ClusterJobClient("127.0.0.1", master.port()).submitToSink(graph));
            assertTrue(allEntered.await(1, TimeUnit.SECONDS),
                    "sources and sinks across logical stages must all enter before any ACK; remaining="
                            + allEntered.getCount() + " roles=" + roles);
            release.countDown();
            submit.get(2, TimeUnit.SECONDS);
            assertEquals(new java.util.HashSet<>(Arrays.asList(
                    StageCommand.Role.SOURCE_STAGE, StageCommand.Role.SINK)), roles);
        } finally {
            release.countDown();
            if (previous == null) System.clearProperty("rowweave.cluster.assign.parallelism");
            else System.setProperty("rowweave.cluster.assign.parallelism", previous);
        }
    }
}

