package io.rowweave.bench;

import io.rowweave.CollectingSink;
import io.rowweave.DataSchema;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PushDagExecutor;

/**
 * A light benchmark harness, test-only and without JMH. It runs one PlanGraph through
 * {@link PushDagExecutor}, where the engine manages its own ExecutionMemory taking quota from
 * the injected {@link ManagedMemory}, and measures the wall-clock time in nanoseconds plus the
 * peak reserved bytes from {@link ExecutionReport} for each execution. The sink is a
 * {@link CollectingSink}. It is used to assert the no-OOM invariant, that the peak never
 * exceeds the acquired quota, and to compare throughput scale between heap and bounded runs.
 *
 * <p>This does not break the materialization contract: the harness lives in src/test and
 * CollectingSink is a permitted test sink.
 */
public final class BenchmarkHarness {

    private BenchmarkHarness() {
    }

/** One benchmark result: wall-clock nanoseconds, peak reserved bytes, acquired quota, and output row count. */
    public static final class BenchmarkResult {
        public final long wallNanos;
        public final long peakBytes;
        public final long acquiredBytes;
        public final int outRowCount;

        BenchmarkResult(long wallNanos, long peakBytes, long acquiredBytes, int outRowCount) {
            this.wallNanos = wallNanos;
            this.peakBytes = peakBytes;
            this.acquiredBytes = acquiredBytes;
            this.outRowCount = outRowCount;
        }

/** The no-OOM invariant: the peak over the whole run never exceeds the acquired quota. */
        public boolean withinBudget() {
            return peakBytes <= acquiredBytes;
        }

        @Override
        public String toString() {
            return String.format("BenchmarkResult{wall=%.2fms, peak=%dB/acquired=%dB (%.1f%%), rows=%d}",
                    wallNanos / 1e6, peakBytes, acquiredBytes,
                    acquiredBytes == 0 ? 0.0 : 100.0 * peakBytes / acquiredBytes, outRowCount);
        }
    }

/**
 * Runs a PlanGraph whose root is a breaker chain rather than a SINK, so the root is the last
 * operator. The engine manages memory from {@code pool} with spilling injected, and the sink
 * collects into a CollectingSink. Returns the wall clock, the peak, the acquired quota and the
 * row count.
 *
 * @param graph the plan to execute, rooted at a trailing unary or binary operator rather than a SINK
 * @param outMeta the schema of the sink
 * @param pool the ManagedMemory to draw from, where a tiny pool forces spilling and keeps tests isolated
 * @param parallelism the execution parallelism
 */
    public static BenchmarkResult run(PlanGraph graph, DataSchema outMeta, ManagedMemory pool, int parallelism) {
        CollectingSink out = new CollectingSink(outMeta);
        long t0 = System.nanoTime();
        ExecutionReport report = new PushDagExecutor(parallelism, pool).executeToOutput(graph, out);
        long wall = System.nanoTime() - t0;
        return new BenchmarkResult(wall, report.peakReservedBytes(), report.acquiredBytes(), out.getRows().size());
    }
}
