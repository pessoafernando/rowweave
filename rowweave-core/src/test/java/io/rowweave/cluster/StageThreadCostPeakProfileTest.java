package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** maximum certified multi-breaker fragment peak is below its static cost. */
class StageThreadCostPeakProfileTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void threeConcurrentOrderByBreakersStayWithinCertifiedSixteen() throws Exception {
        List<String> command = new ArrayList<>(ForkedJvm.baseCommand());
        ForkedJvm.appendJvmArguments(command, java.util.Arrays.asList(
                "-Xms256m", "-Xmx1024m",
                "-XX:MaxDirectMemorySize=256m",
                "-XX:MaxMetaspaceSize=256m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=3221225472"));
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(StageThreadCostPeakProfileTest.class.getName());
        command.add("probe");

        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        String profile = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                child.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter input = new BufferedWriter(new OutputStreamWriter(
                     child.getOutputStream(), StandardCharsets.UTF_8))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
            while (System.nanoTime() < deadline) {
                String line = reader.readLine();
                if (line == null) break;
                output.append(line).append('\n');
                if (line.startsWith("STAGE_THREAD_PROFILE ")) {
                    profile = line;
                    break;
                }
            }
            assertTrue(profile != null, output.toString());
            assertTrue(profile.contains("model=16"), profile);
            long rssBytes = exactRssBytes(child);
            assertTrue(rssBytes < 3L * 1024L * 1024L * 1024L,
                    profile + " rssBytes=" + rssBytes);
            System.out.println(profile + " rssBytes=" + rssBytes);
            input.newLine();
            input.flush();
        } catch (Throwable failure) {
            child.destroyForcibly();
            throw failure;
        }
        assertTrue(child.waitFor(30L, TimeUnit.SECONDS), output.toString());
        assertEquals(0, child.exitValue(), output.toString());
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1 || !"probe".equals(arguments[0])) {
            throw new IllegalArgumentException("expected probe mode");
        }
        runProfile();
        System.in.read();
    }

    private static void runProfile() throws Exception {
        System.setProperty("rowweave.worker.memory", Long.toString(64L * 1024L * 1024L));
        System.setProperty("rowweave.worker.slots", "1");
        System.setProperty("rowweave.worker.operator.maxMemory",
                Long.toString(2L * 1024L * 1024L));
        PlanGraph graph = threeBreakerGraph(100_000);
        StageCommand command = StageCommand.wholeGraph(graph);
        StageThreadCostModel.Cost cost = StageThreadCostModel.estimate(
                command, dataPlane());
        assertEquals(16, cost.totalThreads());

        StageTask task = new StageTask(command, StageTask.PortListener.NONE);
        AtomicReference<StageTask.StageResult> result = new AtomicReference<>();
        Thread owner = new Thread(() -> result.set(task.run()),
                "pc3-profile-stage-owner");
        owner.start();
        int peak = 0;
        while (owner.isAlive()) {
            peak = Math.max(peak, internalStageThreads());
            Thread.sleep(1L);
        }
        owner.join();

        assertEquals(StageTask.StopReason.EOS, result.get().reason());
        assertTrue(peak >= 6, "profile did not exercise concurrent stage threads: " + peak);
        assertTrue(peak <= cost.totalThreads(),
                "observed=" + peak + " model=" + cost.totalThreads());
        System.out.println("STAGE_THREAD_PROFILE role=WHOLE_GRAPH model="
                + cost.totalThreads() + " observedPeak=" + peak);
    }

    private static long exactRssBytes(Process child) throws Exception {
        long pid = (Long) Process.class.getMethod("pid").invoke(child);
        Process ps = new ProcessBuilder("ps", "-o", "rss=", "-p",
                Long.toString(pid)).redirectErrorStream(true).start();
        String line;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                ps.getInputStream(), StandardCharsets.UTF_8))) {
            line = reader.readLine();
        }
        assertTrue(ps.waitFor(10L, TimeUnit.SECONDS),
                "exact-child RSS probe timed out");
        assertEquals(0, ps.exitValue(), "exact-child RSS probe failed");
        return Long.parseLong(line.trim()) * 1024L;
    }

    private static PlanGraph threeBreakerGraph(int rowCount) {
        List<DataRow> rows = new ArrayList<>(rowCount);
        for (int i = rowCount - 1; i >= 0; i--) {
            DataRow row = new DataRow(1);
            row.set(0, i);
            rows.add(row);
        }
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(META, rows));
        String encoded = PlanOps.encodeSortField(new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST));
        for (int i = 0; i < 3; i++) {
            nodes.add(PlanNode.unary("orderBy", META, i)
                    .listParam("sort", Collections.singletonList(encoded)).build());
        }
        nodes.add(PlanNode.sink(META, 3, new DiscardSink(META)).build());
        return new PlanGraph(nodes, 1);
    }

    private static DataPlaneConfig dataPlane() {
        Properties properties = new Properties();
        return DataPlaneConfig.fromProperties(properties,
                io.rowweave.cluster.rpc.ControlEndpointConfig
                        .fromProperties(properties), 2L * 1024L * 1024L * 1024L);
    }

    private static int internalStageThreads() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (!thread.isAlive()) continue;
            String name = thread.getName();
            if (name.startsWith("rowweave-task-")
                    || name.startsWith("rowweave-sort-")
                    || name.startsWith("rowweave-spill-")
                    || name.startsWith("rowweave-binary-drain-")
                    || name.startsWith("rowweave-fanout-")) {
                count++;
            }
        }
        return count;
    }
}
