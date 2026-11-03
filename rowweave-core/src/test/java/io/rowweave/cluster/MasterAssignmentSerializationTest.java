package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MasterAssignmentSerializationTest {
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.BigDecimalType));

    private static final class GuardedInput implements Source {
        private static final long serialVersionUID = 1L;
        static final AtomicInteger ACTIVE = new AtomicInteger();
        static final AtomicInteger PEAK = new AtomicInteger();

        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }

        private void writeObject(ObjectOutputStream out) throws IOException {
            int active = ACTIVE.incrementAndGet();
            PEAK.accumulateAndGet(active, Math::max);
            try {
                try {
                    Thread.sleep(30L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
                out.defaultWriteObject();
            } finally {
                ACTIVE.decrementAndGet();
            }
        }
    }

    @Test
    void parallelAssignmentKeepsPerJobWriteObjectSingleThreaded(@TempDir Path tmp) {
        GuardedInput.ACTIVE.set(0);
        GuardedInput.PEAK.set(0);
        DataSchema grouped = ParallelGroupByAgg.of(META, "k").sum("v").getOutputSchema();
        PlanNode source = PlanNode.sourceFromInputs(META, Arrays.asList(
                new GuardedInput(), new GuardedInput(), new GuardedInput(), new GuardedInput()));
        PlanNode agg = PlanNode.unary("groupBySum", grouped, 0)
                .listParam("keys", Collections.singletonList("k")).param("measure", "v").build();
        PlanNode sink = PlanNode.sink(grouped, 1, new DiscardSink(grouped)).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, agg, sink), 4);

        File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000L);
             WorkerServer a = WorkerServer.newUnguarded("w0", 16, "127.0.0.1", master.port(), w0);
             WorkerServer b = WorkerServer.newUnguarded("w1", 16, "127.0.0.1", master.port(), w1)) {
            new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
        }

        assertEquals(1, GuardedInput.PEAK.get(),
                "at assign concurrency 4 the custom writeObject of one job must still be entered single-threaded");
    }
}

