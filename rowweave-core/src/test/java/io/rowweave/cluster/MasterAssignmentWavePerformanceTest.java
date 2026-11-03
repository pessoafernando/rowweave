package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterAssignmentWavePerformanceTest {
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.BigDecimalType));

    private static final class EmptyInput implements Source {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    @Test
    void p4WaveIsAtMostSixtyPercentOfSerialAssignmentLatency() {
        String previous = System.getProperty("rowweave.cluster.assign.parallelism");
        try {
            long serial = runWithParallelism(1);
            long parallel = runWithParallelism(4);
            System.out.println("[ASSIGN-PERF] serial=" + serial + "ms parallel=" + parallel
                    + "ms ratio=" + String.format(java.util.Locale.ROOT, "%.2f", (double) parallel / serial));
            assertTrue(parallel * 100 <= serial * 60,
                    "the P4 assign wave should cost no more than 60% of the serial run: serial=" + serial + "ms parallel=" + parallel + "ms");
        } finally {
            if (previous == null) {
                System.clearProperty("rowweave.cluster.assign.parallelism");
            } else {
                System.setProperty("rowweave.cluster.assign.parallelism", previous);
            }
        }
    }

    private static long runWithParallelism(int assignParallelism) {
        System.setProperty("rowweave.cluster.assign.parallelism", Integer.toString(assignParallelism));
        DataSchema grouped = ParallelGroupByAgg.of(META, "k").sum("v").getOutputSchema();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Arrays.asList(
                        new EmptyInput(), new EmptyInput(), new EmptyInput(), new EmptyInput())),
                PlanNode.unary("groupBySum", grouped, 0)
                        .listParam("keys", Collections.singletonList("k")).param("measure", "v").build(),
                PlanNode.sink(grouped, 1, new DiscardSink(grouped)).build()), 4);

        try (MasterServer master = new MasterServer(5000L);
             RpcServer fakeWorker = new RpcServer(request -> {
                 if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                     try {
                         Thread.sleep(150L);
                     } catch (InterruptedException e) {
                         Thread.currentThread().interrupt();
                         return RpcMessage.jobResult(false, 0, "interrupted");
                     }
                     master.handler().apply(RpcMessage.stageReport(
                             request.workerId(), request.stageId(), true, 0, 0, null,
                             request.incarnationId(), request.runAttemptFence()));
                 }
                 return RpcMessage.ack();
             })) {
            master.handler().apply(RpcMessage.registerWorker(
                    "slow-worker", "127.0.0.1", fakeWorker.port(), 32));
            long started = System.nanoTime();
            new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            return (System.nanoTime() - started) / 1_000_000L;
        }
    }
}

