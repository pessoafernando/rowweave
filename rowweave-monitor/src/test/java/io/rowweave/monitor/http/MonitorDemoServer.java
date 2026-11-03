package io.rowweave.monitor.http;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StageView;
import io.rowweave.monitor.WorkerView;
import io.rowweave.plan.ExecutionTelemetrySummary;

import java.util.Arrays;

/** Serves representative monitor data for manual browser verification on port 18080. */
public final class MonitorDemoServer {

    private static final long NOW = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(100);

        monitor.onJobSubmit(JobView.builder("job-1", "orders_sum")
                .addStage(StageView.builder(0).role("SOURCE").state("FINISHED")
                        .opChain("csv>filter>map").inputRows(20_000L).outputRows(19_980L)
                        .startTime(NOW - 12_000L).endTime(NOW - 10_000L).parallelism(2)
                        .workerId("w0")
                        .addInstance(new StageView.InstanceView(0, "w0", "FINISHED", 9_990L,
                                NOW - 12_000L, NOW - 10_000L, 10_000L))
                        .addInstance(new StageView.InstanceView(1, "w1", "FINISHED", 9_990L,
                                NOW - 12_000L, NOW - 10_000L, 10_000L))
                        .build())
                .addStage(StageView.builder(1).role("SINK").state("FINISHED")
                        .opChain("groupBy>sum>output").inputRows(19_980L).outputRows(50L)
                        .startTime(NOW - 10_000L).endTime(NOW - 9_000L).parallelism(1)
                        .workerId("w1").addInput(0, "remote").build())
                .build());
        monitor.onJobComplete("job-1", "FINISHED", null);

        monitor.onJobSubmit(JobView.builder("job-2", "orders_join_dim")
                .addStage(StageView.builder(0).role("SOURCE").state("FINISHED")
                        .opChain("csv>filter").inputRows(1_000_000L).outputRows(986_400L)
                        .startTime(NOW - 40_000L).endTime(NOW - 36_000L).parallelism(2)
                        .workerId("w0").build())
                .addStage(StageView.builder(1).role("SOURCE").state("FINISHED")
                        .opChain("csv:dim").inputRows(50L).outputRows(50L)
                        .startTime(NOW - 40_000L).endTime(NOW - 39_800L).parallelism(1)
                        .workerId("w1").build())
                .addStage(StageView.builder(2).role("STAGE").state("RUNNING")
                        .opChain("join>select").inputRows(986_450L).outputRows(491_200L)
                        .startTime(NOW - 36_000L).parallelism(4).workerId("w0")
                        .addInput(0, "remote").addInput(1, "remote")
                        .addInstance(new StageView.InstanceView(0, "w0", "RUNNING", 122_800L))
                        .addInstance(new StageView.InstanceView(1, "w1", "RUNNING", 121_900L))
                        .addInstance(new StageView.InstanceView(2, "w0", "RUNNING", 123_300L))
                        .addInstance(new StageView.InstanceView(3, "w1", "RUNNING", 123_200L))
                        .build())
                .addStage(StageView.builder(3).role("SINK").state("RUNNING")
                        .opChain("groupBy>output").startTime(NOW - 30_000L)
                        .parallelism(2).workerId("w1").addInput(2, "remote").build())
                .build());

        monitor.onJobSubmit(JobView.builder("job-3", "big_shuffle")
                .addStage(StageView.builder(0).role("SOURCE").state("FINISHED")
                        .opChain("csv").inputRows(5_000_000L).outputRows(4_812_000L)
                        .startTime(NOW - 90_000L).endTime(NOW - 80_000L).parallelism(2)
                        .workerId("w0")
                        .addInstance(new StageView.InstanceView(0, "w0", "FINISHED", 2_406_000L,
                                NOW - 90_000L, NOW - 80_000L, 2_500_000L))
                        .addInstance(new StageView.InstanceView(1, "w1", "FINISHED", 2_406_000L,
                                NOW - 90_000L, NOW - 80_000L, 2_500_000L))
                        .build())
                .addStage(StageView.builder(1).role("STAGE").state("FAILED")
                        .opChain("partitionByHash>groupBy").inputRows(4_812_000L).outputRows(0L)
                        .startTime(NOW - 80_000L).endTime(NOW - 71_000L).parallelism(2)
                        .workerId("w1").error("Worker heartbeat stale after 30 seconds; shuffle partition 1 failed")
                        .addInput(0, "remote")
                        .addInstance(new StageView.InstanceView(0, "w0", "FAILED", 0L,
                                NOW - 80_000L, NOW - 71_000L, 2_406_000L))
                        .addInstance(new StageView.InstanceView(1, "w1", "FAILED", 0L,
                                NOW - 80_000L, NOW - 71_000L, 2_406_000L))
                        .build())
                .addStage(StageView.builder(2).role("SINK").state("FAILED")
                        .opChain("orderBy>top>output").startTime(NOW - 71_000L)
                        .endTime(NOW - 70_900L).parallelism(1).workerId("w0")
                        .error("Cancelled after upstream stage failure").addInput(1, "remote")
                        .build())
                .build());
        monitor.onJobComplete("job-3", "FAILED",
                "Worker lost during execution (heartbeat stale): w1",
                ExecutionTelemetrySummary.unavailable().toExecutionReport(0L));

        monitor.setWorkers(Arrays.asList(
                WorkerView.builder("w0", "127.0.0.1", 5001).slots(4).freeSlots(2)
                        .managedMemoryUsedBytes(1_073_741_824L)
                        .managedMemoryMaxBytes(2_147_483_648L)
                        .startTimeMillis(NOW - 360_000L).alive(true).build(),
                WorkerView.builder("w1", "127.0.0.1", 5002).slots(4).freeSlots(1)
                        .managedMemoryUsedBytes(1_610_612_736L)
                        .managedMemoryMaxBytes(2_147_483_648L)
                        .startTimeMillis(NOW - 300_000L).alive(true).build(),
                WorkerView.builder("w2", "10.0.0.9", 5003).slots(8).freeSlots(8)
                        .managedMemoryUsedBytes(0L).managedMemoryMaxBytes(4_294_967_296L)
                        .startTimeMillis(NOW - 600_000L).alive(false).build()));

        monitor.onJobSubmit(JobView.builder("job-4", "live_pipeline")
                .addStage(StageView.builder(0).role("SOURCE").state("RUNNING")
                        .opChain("csv>filter").inputRows(18_400L).outputRows(18_220L)
                        .startTime(NOW - 8_000L).parallelism(2).workerId("w0").build())
                .addStage(StageView.builder(1).role("STAGE").state("RUNNING")
                        .opChain("map>partitionByHash").inputRows(18_220L).outputRows(4_100L)
                        .startTime(NOW - 7_000L).parallelism(2).workerId("w1")
                        .addInput(0, "remote").build())
                .addStage(StageView.builder(2).role("SINK").state("RUNNING")
                        .opChain("groupBy>sum>output").startTime(NOW - 6_000L)
                        .parallelism(1).workerId("w0").addInput(1, "remote").build())
                .build());
        Thread mover = new Thread(() -> {
            try {
                Thread.sleep(4_000L);
                monitor.onStageUpdate("job-4", StageView.builder(0).role("SOURCE")
                        .state("FINISHED").opChain("csv>filter").inputRows(30_000L)
                        .outputRows(29_880L).startTime(NOW - 8_000L).endTime(NOW - 4_000L)
                        .parallelism(2).workerId("w0").build());
                Thread.sleep(4_000L);
                monitor.onStageUpdate("job-4", StageView.builder(1).role("STAGE")
                        .state("FINISHED").opChain("map>partitionByHash").inputRows(29_880L)
                        .outputRows(29_880L).startTime(NOW - 7_000L).endTime(NOW - 2_000L)
                        .parallelism(2).workerId("w1").addInput(0, "remote").build());
                Thread.sleep(4_000L);
                monitor.onStageUpdate("job-4", StageView.builder(2).role("SINK")
                        .state("FINISHED").opChain("groupBy>sum>output").inputRows(29_880L)
                        .outputRows(8L).startTime(NOW - 6_000L).endTime(NOW - 1_000L)
                        .parallelism(1).workerId("w0").addInput(1, "remote").build());
                monitor.onJobComplete("job-4", "FINISHED", null);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        mover.setDaemon(true);
        mover.start();

        MonitorHttpServer server = new MonitorHttpServer(monitor, 18080, null);
        System.out.println("DEMO_MONITOR_PORT=" + server.start());
        Thread.currentThread().join();
    }

    private MonitorDemoServer() { }
}
