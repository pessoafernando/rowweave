package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic wait-based A/B that checks the drain wall clock converges to max(left, right). */
class ConcurrentBinaryDrainPerformanceTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @AfterEach
    void cleanup() {
        System.clearProperty("rowweave.binary.concurrentDrain");
    }

    @Test
    void balancedInputsApproachMaxInsteadOfSum() {
        long serial = run(false, 40, 4, 40, 4);
        long concurrent = run(true, 40, 4, 40, 4);
        assertTrue(concurrent * 100 < serial * 75,
                "balanced concurrent drain should be materially closer to max: serial="
                        + serial + "ms concurrent=" + concurrent + "ms");
    }

    @Test
    void skewedInputsDoNotRegressBeyondTwentyPercent() {
        long serial = run(false, 60, 3, 10, 1);
        long concurrent = run(true, 60, 3, 10, 1);
        assertTrue(concurrent * 100 <= serial * 120,
                "skewed concurrent drain regression exceeded 20%: serial="
                        + serial + "ms concurrent=" + concurrent + "ms");
    }

    @Test
    @Timeout(30)
    void smallInputsStayWithinOneMillisecondAbsoluteOverhead() throws Exception {
        Process process = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                ConcurrentBinaryDrainPerformanceProbe.class.getName()))
                .redirectErrorStream(true)
                .start();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS),
                "small-input performance probe timed out");
        String output = drain(process);
        assertEquals(0, process.exitValue(), output);
        long averageOverhead = Long.parseLong(output.trim());
        assertTrue(averageOverhead <= 1_000_000L,
                "small-input average absolute overhead exceeded 1ms: "
                        + averageOverhead + "ns");
    }

    static long measureSmallInputAverageOverheadNanos() {
        for (int i = 0; i < 5; i++) {
            runNanos(false, 1, 0, 1, 0);
            runNanos(true, 1, 0, 1, 0);
        }
        long serial = 0;
        long concurrent = 0;
        int rounds = 30;
        int batches = 5;
        // Keep the paired 30-round comparison and 1 ms limit, but
        // pool several batches so one host-scheduler spike cannot decide the gate.
        for (int batch = 0; batch < batches; batch++) {
            for (int i = 0; i < rounds; i++) {
                if ((i & 1) == 0) {
                    serial += runNanos(false, 1, 0, 1, 0);
                    concurrent += runNanos(true, 1, 0, 1, 0);
                } else {
                    concurrent += runNanos(true, 1, 0, 1, 0);
                    serial += runNanos(false, 1, 0, 1, 0);
                }
            }
        }
        return (concurrent - serial) / (rounds * batches);
    }

    private static String drain(Process process) throws Exception {
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (output.length() > 0) output.append('\n');
                output.append(line);
            }
        }
        return output.toString();
    }

    private static long run(boolean concurrent, int leftRows, int leftDelayMs,
                            int rightRows, int rightDelayMs) {
        return runNanos(concurrent, leftRows, leftDelayMs, rightRows, rightDelayMs) / 1_000_000L;
    }

    private static long runNanos(boolean concurrent, int leftRows, int leftDelayMs,
                                 int rightRows, int rightDelayMs) {
        System.setProperty("rowweave.binary.concurrentDrain", Boolean.toString(concurrent));
        SortMergeJoin join = SortMergeJoin.of(
                        LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                .withSpill(new MemoryManager(64 * 1024), new MemorySpillStore(), new HeapPool());
        CollectingRowSink sink = new CollectingRowSink();
        long start = System.nanoTime();
        join.join(new DelayedRows(leftRows, leftDelayMs, "L"),
                new DelayedRows(rightRows, rightDelayMs, "R"), sink);
        long nanos = System.nanoTime() - start;
        assertEquals(Math.min(leftRows, rightRows), sink.rows().size());
        return nanos;
    }

    private static final class DelayedRows implements RowStream {
        private final int count;
        private final int delayMs;
        private final String prefix;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private int index;

        private DelayedRows(int count, int delayMs, String prefix) {
            this.count = count;
            this.delayMs = delayMs;
            this.prefix = prefix;
        }

        @Override
        public StreamEvent next() {
            if (cancelled.get() || index >= count) {
                return null;
            }
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return StreamEvent.cancel();
            }
            DataRow row = new DataRow(2);
            row.set(0, index);
            row.set(1, prefix + index);
            index++;
            return StreamEvent.record(row);
        }

        @Override public boolean supportsCancellation() { return true; }
        @Override public boolean cancel(Throwable cause) {
            cancelled.set(true);
            return true;
        }
    }
}
