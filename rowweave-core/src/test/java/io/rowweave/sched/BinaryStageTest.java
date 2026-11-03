package io.rowweave.sched;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

class BinaryStageTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema OUT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType),
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    private static DataRow l(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    private static DataRow r(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    private static List<DataRow> rows(DataRow... rs) {
        return new ArrayList<>(Arrays.asList(rs));
    }

    @Test
    void smjTwoSidePushProducesJoinResult() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT,
                    new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
            CollectingSink out = new CollectingSink(OUT);
            PipelineExecutor exec = new PipelineExecutor();
            exec.runBinary(
                    Arrays.asList(PipelineExecutor.source(rows(l(1, "a"), l(2, "b"), l(3, "c")))),
                    Arrays.asList(PipelineExecutor.source(rows(r(2, "x"), r(3, "y"), r(4, "z")))),
                    smj::join,
                    Arrays.asList(PipelineExecutor.outputStage(out)));
            assertEquals(2, out.getRows().size()); // keys 2,3 match
        });
    }

    @Test
    void leftSideHasPassThroughBeforeBinary() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT,
                    new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
            CollectingSink out = new CollectingSink(OUT);
            PipelineExecutor exec = new PipelineExecutor();
            // left: source -> filter(lk>=2)
            exec.runBinary(
                    Arrays.asList(
                            PipelineExecutor.source(rows(l(1, "a"), l(2, "b"), l(3, "c"))),
                            PipelineExecutor.passThroughStream((in, sink) -> {
                                StreamEvent e;
                                while ((e = in.next()) != null) {
                                    if (e.isRecord() && (Integer) e.row().get(0) >= 2) {
                                        sink.emitRow(e.row());
                                    }
                                }
                            })),
                    Arrays.asList(PipelineExecutor.source(rows(r(1, "x"), r(2, "y"), r(3, "z")))),
                    smj::join,
                    Arrays.asList(PipelineExecutor.outputStage(out)));
            // left filtered to {2,3}; right {1,2,3} -> match 2,3
            assertEquals(2, out.getRows().size());
        });
    }

    @Test
    void downstreamAfterBinary() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT,
                    new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
            CollectingSink out = new CollectingSink(OUT);
            PipelineExecutor exec = new PipelineExecutor();
            exec.runBinary(
                    Arrays.asList(PipelineExecutor.source(rows(l(1, "a"), l(2, "b")))),
                    Arrays.asList(PipelineExecutor.source(rows(r(1, "x"), r(2, "y")))),
                    smj::join,
                    // downstream: filter(lk==1) -> output
                    Arrays.asList(
                            PipelineExecutor.passThroughStream((in, sink) -> {
                                StreamEvent e;
                                while ((e = in.next()) != null) {
                                    if (e.isRecord() && Integer.valueOf(1).equals(e.row().get(0))) {
                                        sink.emitRow(e.row());
                                    }
                                }
                            }),
                            PipelineExecutor.outputStage(out)));
            assertEquals(1, out.getRows().size());
            assertEquals(1, out.getRows().get(0).get(0));
        });
    }

    @Test
    void backpressureSmallChannelNoDeadlock() {
        assertTimeoutPreemptively(ofSeconds(15), () -> {
            SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT,
                    new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
            CollectingSink out = new CollectingSink(OUT);
            List<DataRow> left = new ArrayList<>();
            List<DataRow> right = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                left.add(l(i, "l" + i));
                right.add(r(i, "r" + i));
            }
            PipelineExecutor exec = new PipelineExecutor()
                    .withChannelFactory(() -> new LocalChannel(2)); // tiny channels
            exec.runBinary(
                    Arrays.asList(PipelineExecutor.source(left)),
                    Arrays.asList(PipelineExecutor.source(right)),
                    smj::join,
                    Arrays.asList(PipelineExecutor.outputStage(out)));
            assertEquals(50, out.getRows().size()); // all keys match 1:1
        });
    }

    @Test
    void emptyOneSideInnerEmpty() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT,
                    new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
            CollectingSink out = new CollectingSink(OUT);
            new PipelineExecutor().runBinary(
                    Arrays.asList(PipelineExecutor.source(Collections.emptyList())),
                    Arrays.asList(PipelineExecutor.source(rows(r(1, "x")))),
                    smj::join,
                    Arrays.asList(PipelineExecutor.outputStage(out)));
            assertTrue(out.getRows().isEmpty());
        });
    }

    @Test
    void rightChainFailureSurfacesToCaller() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT,
                    new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
            CollectingSink out = new CollectingSink(OUT);
            PipelineExecutor exec = new PipelineExecutor();
            RuntimeException boom = new RuntimeException("right-boom");
            // right chain: source -> a stage that throws
            RuntimeException ex = assertThrows(RuntimeException.class, () -> exec.runBinary(
                    Arrays.asList(PipelineExecutor.source(rows(l(1, "a")))),
                    Arrays.asList(
                            PipelineExecutor.source(rows(r(1, "x"))),
                            PipelineExecutor.passThroughStream((in, sink) -> {
                                throw boom;
                            })),
                    smj::join,
                    Arrays.asList(PipelineExecutor.outputStage(out))));
            // fail-fast: caller must observe the right-chain failure (not silently produce wrong join)
            Throwable c = ex;
            boolean found = false;
            while (c != null) {
                if (c == boom) {
                    found = true;
                    break;
                }
                c = c.getCause();
            }
            assertTrue(found, "right-chain failure must surface to caller");
        });
    }
}
