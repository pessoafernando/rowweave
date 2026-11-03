package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterFailFastTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();

    private static DataRow lrow(int k, String v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    @Test
    void leftSourceThrowsFailsFastNoHang(@TempDir Path tmp) throws Exception {
        Source boom = new Source() {
            @Override public DataSchema getSchema() { return LEFT; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    private int n = 0;
                    @Override public boolean hasNext() { return n++ < 1; }
                    @Override public DataRow next() { throw new RuntimeException("left boom"); }
                };
            }
        };
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            right.add(lrow(i % 5, "R" + i));
        }
        PlanNode ls = PlanNode.sourceFromInputs(LEFT, Collections.singletonList(boom));
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        File outFile = tmp.resolve("ff.tsv").toFile();
        byte[] previous = "previous-cluster-commit\n".getBytes(StandardCharsets.UTF_8);
        Files.write(outFile.toPath(), previous);
        PlanNode sink = PlanNode.sink(JOIN, 2, new FileSink(JOIN, outFile.getAbsolutePath())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(ls, rs, join, sink));

        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();
        File w1tmp = tmp.resolve("w1").toFile(); w1tmp.mkdirs();

        try (MasterServer master = new MasterServer(5000);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0tmp);
             WorkerServer w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1tmp)) {

            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());
            long start = System.currentTimeMillis();
            assertThrows(RuntimeException.class, () -> client.submitToSink(graph), "a throwing left source makes the submit fail fast");
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 30_000, "fail fast without hanging; elapsed " + elapsed + "ms");
            assertArrayEquals(previous, Files.readAllBytes(outFile.toPath()),
                    "a real cluster failure must keep the last published result");
        }
    }
}

