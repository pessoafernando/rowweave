package io.rowweave;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StageView;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The default single-JVM path feeds the same live DAG views used by cluster monitoring. */
class LocalBatchMonitorTest {

    private static final DataSchema SCHEMA = new DataSchema(
            new Field("key", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

    private final InMemoryJobMonitor monitor = new InMemoryJobMonitor(8);

    @AfterEach
    void resetRuntime() {
        InProcessBatchRuntime.setProcessWideMonitor(null);
        InProcessBatchRuntime.resetProcessWideForTesting();
    }

    @Test
    void localExecutePublishesLiveStageGraphAndInstanceProgress() {
        InProcessBatchRuntime.setProcessWideMonitor(monitor);
        // Earlier tests may already have created the process-wide runtime with
        // its no-op registry. Recreate it so this runtime observes the monitor.
        InProcessBatchRuntime.resetProcessWideForTesting();

        BatchSession environment = RowWeave.createBatchSession("local-monitor");
        environment.setParallelism(2);
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            DataRow row = new DataRow(2);
            row.set(0, "key-" + (i % 4));
            row.set(1, new BigDecimal(i));
            input.add(row);
        }
        DataSet grouped = environment.fromSource(source(input))
                .groupBy(ParallelGroupByAgg.of(SCHEMA, "key").sum("amount"));
        WorkerCollectingOutput output = new WorkerCollectingOutput(grouped.getSchema());
        grouped.sinkTo(output);
        environment.execute();

        List<JobView> jobs = monitor.jobs();
        assertEquals(1, jobs.size());
        JobView job = jobs.get(0);
        assertEquals("FINISHED", job.state());
        assertTrue(job.stages().size() >= 2, "expected a multi-node execution DAG");

        StageView source = findRole(job, "SOURCE");
        StageView sink = findRole(job, "SINK");
        StageView downstream = job.stages().stream()
                .filter(stage -> !stage.inputs().isEmpty())
                .findFirst()
                .orElseThrow(() -> new AssertionError("execution DAG had no connected downstream stage"));
        assertTrue(downstream.inputs().contains(source.index()), "downstream node is connected to source");
        assertEquals("FINISHED", source.state());
        assertEquals("FINISHED", sink.state());
        assertTrue(sink.outputRows() > 0, "sink node reported produced rows");

        for (StageView stage : job.stages()) {
            assertEquals(stage.parallelism(), stage.instances().size(), "stage " + stage.index());
            assertTrue(stage.parallelism() > 0, "stage " + stage.index());
            for (StageView.InstanceView instance : stage.instances()) {
                assertEquals("FINISHED", instance.state, "stage " + stage.index()
                        + " instance " + instance.instanceIndex);
                assertTrue(instance.startTime > 0, "stage " + stage.index()
                        + " instance " + instance.instanceIndex + " start time");
            }
        }
        assertTrue(job.stages().stream().anyMatch(stage ->
                stage.inputRows() > 0 || stage.outputRows() > 0),
                "at least one node reported execution progress");
    }

    private static StageView findRole(JobView job, String role) {
        return job.stages().stream()
                .filter(stage -> role.equals(stage.role()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing " + role + " node"));
    }

    private static Source source(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return SCHEMA; }
            @Override public Iterator<DataRow> iterator() {
                return new ArrayList<>(rows).iterator();
            }
        };
    }
}
