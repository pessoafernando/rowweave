package io.rowweave.sched;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

/**
 * channelStream latches an error placeholder once, so after an upstream failure a downstream
 * that uses a naive loop over non-null next values, without checking isError, receives just one
 * more error event and then stops instead of busy-spinning. This is a hard guard, not a
 * convention that operators must follow.
 */
class RowStreamErrorGuardTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<DataRow> bigRows(int n) {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(row(i));
        }
        return out;
    }

    private static final class DiscardSink implements Sink {
        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) { }
    }

    @Test
    void naiveDrainLoopDoesNotSpinAfterUpstreamError() {
/** A mid-stream stage uses a naive while-next-not-null loop and deliberately omits the isError check, standing in for an operator that missed it.
 * The upstream failure poisons the channel, so its channelStream must return null after the one error event, otherwise the naive loop would spin. */
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            PipelineExecutor exec = new PipelineExecutor()
                    .withChannelFactory(() -> new LocalChannel(4));
            exec.addStage("source", PipelineExecutor.source(bigRows(100)));
/** Mid-stage 1 forwards a few rows normally, then throws, which makes an upstream error that poisons the source to mid-stage 1 channel, and the fail-fast poisons every channel. */
            exec.addStage("boom", PipelineExecutor.passThroughStream((in, out) -> {
                StreamEvent e;
                int n = 0;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        out.emitRow(e.row());
                        if (++n == 3) {
                            throw new IllegalStateException("boom");
                        }
                    }
                }
            }));
/** Mid-stage 2 uses a naive loop that never checks isError or isRecord, relying only on the error latch of channelStream to avoid spinning. */
            exec.addStage("naive", (in, out) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        out.emitRow(e.row());
                    }
/** The error is deliberately not handled here, relying on channelStream returning null afterwards to end the loop. */
                }
            });
            exec.addStage("output", PipelineExecutor.outputStage(new DiscardSink()));
/** No deadlock and no spin, guarded by the timeout, and the caller receives the boom. */
            RuntimeException ex = assertThrows(RuntimeException.class, exec::run);
            boolean found = false;
            for (Throwable c = ex; c != null; c = c.getCause()) {
                if (c.getMessage() != null && c.getMessage().contains("boom")) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "root failure must surface");
        });
    }

    @Test
    void normalPipelineStillWorks() {
/** The normal path with no failure: the error latch changes nothing, since the EOS ends the stream as usual. */
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            io.rowweave.CollectingSink out = new io.rowweave.CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(bigRows(5)));
            exec.addStage("naive", (in, sink) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        sink.emitRow(e.row());
                    }
                }
            });
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();
            assertEquals(5, out.getRows().size());
        });
    }
}
