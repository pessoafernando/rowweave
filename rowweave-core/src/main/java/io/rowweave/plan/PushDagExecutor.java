package io.rowweave.plan;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryOwnerRequirement;
import io.rowweave.ops.Union;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.ops.join.JoinRuntimeFactory;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Executes the logical plan held in {@link PlanGraph} IR by pushing rows, feeding each one to a
 * {@link Consumer}<{@link DataRow}> sink. Nothing is materialized in between, and there is no memo
 * array or reference counting.
 *
 * <p>A linear segment goes through {@link PlanCompiler} and {@code PipelineExecutor.run()}. When the
 * graph contains a BINARY node, {@code buildDagNode} recursively builds a {@link
 * PipelineExecutor.DagNode} tree, with binaries nested to any depth - union over union, join over
 * join, union over join - and {@code runDag} flattens it once, then starts and joins it once. Every
 * stage runs concurrently with backpressure on the channels, so two breakers can never wait on each
 * other.
 *
 * <p>Parallelism is fixed, 1 by default, and operator parallelism does not change the result, which
 * stays bit-identical; higher parallelism is configured when a cluster shuffle is wired in. The paged
 * and spill backends of the heavy operators are injected through their {@code withBufferFactory}
 * seam, with an in-heap implementation as the default.
 */
public final class PushDagExecutor {

    /** Channel capacity for a keyed shuffle: bounded in rows for backpressure, the same 256 the cluster Master uses. */
    private static final int SHUFFLE_CAPACITY = 256;
    private static final String LOCAL_SHUFFLE_BATCH_PROPERTY = "rowweave.local.shuffle.batch";
    private static final int ROUTER_BATCH_MAX_PER_PARTITION = 32;
    private static final int ROUTER_STAGED_REFERENCES_MAX = 256;

    private final int parallelism;
    // An injectable ManagedMemory for test isolation; null means the production path uses
    // GlobalMemoryPool.instance(). Memory management is engine housekeeping, so there is no
    // developer-supplied budget parameter. On the local path each execution builds a per-job
    // ExecutionMemory through forJob, acquiring lazily, and closes it in a try-finally.
    private final io.rowweave.mem.ManagedMemory injectedPool;
    // On a cluster the Master holds the one shared ExecutionMemory across every concurrent worker
    // stage of the job; if each worker called forJob itself they would contend for the whole pool, so
    // the first would take it and the rest would fail fast. Non-null means worker mode: use this
    // shared memory and do not close it, because the Master owns its lifetime. Null means local mode:
    // this executor manages its own.
    private final io.rowweave.mem.ExecutionMemory sharedMem;
    @FunctionalInterface
    interface TelemetrySnapshotter {
        StageExecutionTelemetry snapshot(io.rowweave.mem.ExecutionMemory memory);
    }
    private static final TelemetrySnapshotter DEFAULT_TELEMETRY_SNAPSHOTTER =
            PushDagExecutor::telemetryOf;
    private final TelemetrySnapshotter telemetrySnapshotter;
    private final Map<io.rowweave.mem.ExecutionMemory, StageExecutionTelemetry> frozenTelemetry =
            new IdentityHashMap<>();
    private StageExecutionTelemetryAccumulator reportedTelemetry;
    // False by default: the parallel output is equivalent as a set. The multiset is correct, the
    // order is not. Order sensitivity comes from an explicit OrderBy or Top node in the plan, whose
    // total-order operator has parallelism 1 and gathers P into 1 behind the parallel keyed stage.
    // With the flag explicitly true, a total-order OrderBy over every field is appended so the output
    // is strictly sorted; it stays off to preserve parallel throughput.
    private boolean canonicalizeOutput = false;

    public PushDagExecutor() {
        this(1);
    }

    public PushDagExecutor(int parallelism) {
        this(parallelism, (io.rowweave.mem.ManagedMemory) null);
    }

    /** An injectable ManagedMemory for tests, where a tiny pool triggers spill and gives isolation; production passes null for the global pool. Local mode. */
    public PushDagExecutor(int parallelism, io.rowweave.mem.ManagedMemory injectedPool) {
        this(parallelism, injectedPool, DEFAULT_TELEMETRY_SNAPSHOTTER);
    }

    PushDagExecutor(int parallelism, io.rowweave.mem.ManagedMemory injectedPool,
                    TelemetrySnapshotter telemetrySnapshotter) {
        this.parallelism = parallelism;
        this.injectedPool = injectedPool;
        this.sharedMem = null;
        if (telemetrySnapshotter == null) {
            throw new IllegalArgumentException("telemetry snapshotter is required");
        }
        this.telemetrySnapshotter = telemetrySnapshotter;
    }

    /** Cluster worker mode: use the shared ExecutionMemory the Master provides, and do not close it, because the Master owns its lifetime. */
    public static PushDagExecutor forWorker(int parallelism, io.rowweave.mem.ExecutionMemory sharedMem) {
        return new PushDagExecutor(parallelism, sharedMem);
    }

    /** Fluent setter for the trailing canonicalize, false by default. True appends a total-order OrderBy over every field so the output is strictly sorted. */
    public PushDagExecutor canonicalizeOutput(boolean canonicalize) {
        this.canonicalizeOutput = canonicalize;
        return this;
    }

    private PushDagExecutor(int parallelism, io.rowweave.mem.ExecutionMemory sharedMem) {
        this.parallelism = parallelism;
        this.injectedPool = null;
        this.sharedMem = sharedMem;
        this.telemetrySnapshotter = DEFAULT_TELEMETRY_SNAPSHOTTER;
    }

    /** Per-job ExecutionMemory: admits the whole graph owner set atomically before execution; an injected pool wins over the global pool. */
    private io.rowweave.mem.ExecutionMemory forJob(PlanGraph graph) {
        return forJob(graph, 0, 0);
    }

    /**
     * Per-job ExecutionMemory. {@code diskOnlyBreakers} is the number of materializing forks, because
     * a materialized edge only needs {@code newSpillStore()} and takes no pool quota. {@code
     * extraOrdinaryOwners} are real buffer instances the executor adds that are not in the portable
     * graph, for example the canonical order of a quality publication, and they must be admitted
     * atomically together with the graph owners.
     */
    private io.rowweave.mem.ExecutionMemory forJob(
            PlanGraph graph, int diskOnlyBreakers, int extraOrdinaryOwners) {
        if (diskOnlyBreakers < 0 || extraOrdinaryOwners < 0) {
            throw new IllegalArgumentException("memory owner/workspace counts must be non-negative");
        }
        MemoryOwnerRequirement[] graphRequirements = PlanOps.memoryOwnerRequirements(graph);
        MemoryOwnerRequirement[] requirements = java.util.Arrays.copyOf(
                graphRequirements, graphRequirements.length + extraOrdinaryOwners);
        java.util.Arrays.fill(requirements, graphRequirements.length, requirements.length,
                MemoryOwnerRequirement.ordinary());
        boolean workspaceNeeded = requirements.length > 0 || diskOnlyBreakers > 0;
        return injectedPool != null
                ? io.rowweave.mem.ExecutionMemory.forJob(injectedPool, workspaceNeeded, requirements)
                : io.rowweave.mem.ExecutionMemory.forJob(workspaceNeeded, requirements);
    }

    private io.rowweave.mem.ExecutionMemory forOrdinaryOwners(int ownerCount) {
        MemoryOwnerRequirement[] requirements = ordinaryOwnerRequirements(ownerCount);
        return injectedPool != null
                ? io.rowweave.mem.ExecutionMemory.forJob(injectedPool, ownerCount > 0, requirements)
                : io.rowweave.mem.ExecutionMemory.forJob(ownerCount > 0, requirements);
    }

    private static MemoryOwnerRequirement[] ordinaryOwnerRequirements(int ownerCount) {
        MemoryOwnerRequirement[] requirements = new MemoryOwnerRequirement[ownerCount];
        java.util.Arrays.fill(requirements, MemoryOwnerRequirement.ordinary());
        return requirements;
    }

    /** Executes the graph, feeding rows to the sink one at a time. No List is returned and there is no memo array. Returns the peak in an {@link ExecutionReport}. */
    public ExecutionReport execute(PlanGraph graph, Consumer<DataRow> sink) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(graph);
        return runToTerminal(graph, PipelineExecutor.sinkStage(sink));
    }

    /**
     * Executes the graph and pushes the data to a {@link Sink}, which is the trailing output stage:
     * collect row by row, then close, and abort on failure. The Sink is written at the very end of
     * execution. A SINK-rooted graph must first be narrowed with {@code subGraphRootedAt(SINK.child)}
     * before this method is called, together with the Sink belonging to that SINK. Returns the {@link
     * ExecutionReport}, whose peak is at most the acquired amount, which is the no-OOM assertion.
     */
    public ExecutionReport executeToOutput(PlanGraph graph, Sink output) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(graph);
        return runToTerminal(graph, PipelineExecutor.outputStage(output));
    }

    /**
     * Executes a <b>linear</b> stage sub-plan, binding the placeholder source to its inbound {@link
     * RowStream} by node index, which is the cluster stage boundary, and pushing the data to the
     * {@code terminal} stage. LocalWorker uses it for a middle stage, whose output goes to the outbound
     * channel sink, and for a SINK stage, whose output goes to the output stage. Linear only: one
     * producer to one consumer over 1:1 slices; keyed is N by M.
     */
    public void executeLinearBound(PlanGraph graph, Map<Integer, RowStream> sourceBindings,
                                   PipelineExecutor.Stage terminal) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(graph);
        if (!graph.isLinearSegment()) {
            throw new IllegalArgumentException(
                    "executeLinearBound requires a linear stage plan (one inbound channel per boundary); binary/keyed shapes need the routed path");
        }
        boolean ownMem = sharedMem == null;
        io.rowweave.mem.ExecutionMemory mem = ownMem ? forJob(graph) : sharedMem;
        try {
            List<PipelineExecutor.Stage> stages = new PlanCompiler(parallelism, mem).compile(graph, sourceBindings);
            PipelineExecutor exec = new PipelineExecutor();
            int i = 0;
            for (PipelineExecutor.Stage s : stages) {
                exec.addStage("n" + (i++), s);
            }
            exec.addStage("terminal", terminal);
            exec.run();
        } finally {
            if (ownMem) {
                captureTelemetry(mem);
                mem.close();  // Worker mode does not close it: the Master owns the shared ExecutionMemory.
            }
        }
    }

    /** Stage-local reported entry: captures success and failure snapshots before owned memory closes. */
    public void executeLinearBoundReported(
            PlanGraph graph, Map<Integer, RowStream> sourceBindings,
            PipelineExecutor.Stage terminal, StageExecutionTelemetryAccumulator telemetry) {
        if (telemetry == null) {
            throw new IllegalArgumentException("stage telemetry accumulator is required");
        }
        StageExecutionTelemetryAccumulator previous = reportedTelemetry;
        reportedTelemetry = telemetry;
        try {
            executeLinearBound(graph, sourceBindings, terminal);
        } finally {
            reportedTelemetry = previous;
        }
    }

    /** Same as {@link #executeLinearBound} except the tail pushes to a {@link Sink}: the SINK stage worker collects, closes, and aborts on failure. */
    public void executeLinearBoundToOutput(PlanGraph graph, Map<Integer, RowStream> sourceBindings, Sink output) {
        executeLinearBound(graph, sourceBindings, PipelineExecutor.outputStage(output));
    }

    /**
     * The SINK stage worker. Compiles the linear segment from the child of the SINK node (headIndex)
     * while <b>keeping the index space of the original graph</b> so the bindings stay aligned, and
     * pushes the tail to the {@link Sink} of that SINK. This removes the second structural transform
     * through {@code subGraphRootedAt} in runSink and the coupling between two index spaces.
     */
    public void executeSinkStage(PlanGraph graph, int headIndex,
                                 Map<Integer, RowStream> sourceBindings, Sink output) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(graph);
        boolean ownMem = sharedMem == null;
        io.rowweave.mem.ExecutionMemory mem = ownMem ? forJob(graph) : sharedMem;
        try {
            List<PipelineExecutor.Stage> stages =
                    new PlanCompiler(parallelism, mem).compileFrom(graph, headIndex, sourceBindings);
            PipelineExecutor exec = new PipelineExecutor();
            int i = 0;
            for (PipelineExecutor.Stage s : stages) {
                exec.addStage("n" + (i++), s);
            }
            exec.addStage("output", PipelineExecutor.outputStage(output));
            exec.run();
        } finally {
            if (ownMem) {
                captureTelemetry(mem);
                mem.close();
            }
        }
    }

    /**
     * Executes one <b>SINK-rooted</b> PlanGraph locally: take the Sink of the SINK, run the sub-plan
     * below it (linear or a single binary, through {@code subGraphRootedAt} so the original operators
     * and memory stage are kept) and push to that Sink. The number of rows written is the control
     * signal. Used by LocalCluster.submitToSink on the local path, the local commit, which does not go
     * through the typeId rebuild of cluster staging.
     */
    public long runSinkGraph(PlanGraph sinkGraph) {
        // The JobClient contract keeps a long rowCount; the metrics come from runSinkGraphReported.
        return runSinkGraphReported(sinkGraph).rowsOut();
    }

    /**
     * Same as {@link #runSinkGraph} but returns the {@link ExecutionReport}: rowsOut plus peak and
     * acquired, spill, and partitionRowCounts. A per-execute value object, which is the rule rather
     * than a last-result getter. runSinkGraph takes its rowsOut.
     */
    public ExecutionReport runSinkGraphReported(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        PlanNode sinkNode = sinkGraph.root();
        if (sinkNode.kind() != PlanNode.Kind.SINK) {
            throw new IllegalArgumentException("runSinkGraph requires SINK-rooted graph, got " + sinkNode.kind());
        }
        Sink output = sinkNode.output();
        if (output == null) {
            throw new IllegalStateException("SINK node has no Sink instance");
        }
        return runSinkGraphReported(sinkGraph, output);
    }

    /** For the multi-Sink coordinator: keep the original SINK plan and replace only the tail Sink lifecycle view. */
    private ExecutionReport runSinkGraphReported(PlanGraph sinkGraph, Sink output) {
        PlanNode sinkNode = sinkGraph.root();
        // P>1 with source to keyed-unary (the groupBy family or distinct) to SINK, which is parallelized locally by a keyed shuffle.
        if (parallelism > 1 && isSingleInputKeyedShape(sinkGraph)) {
            return runKeyedShuffleParallel(sinkGraph, output);
        }
        // P>1 with left x right to join or coGroup to SINK, which is parallelized locally by a binary keyed shuffle.
        if (parallelism > 1 && isBinaryKeyedShape(sinkGraph)) {
            return runBinaryKeyedShuffleParallel(sinkGraph, output);
        }
        // Take the sub-graph below the SINK (the memory stage survives, since collectSub kept it) and run it to the output through the linear or binary orchestration.
        PlanGraph childGraph = sinkGraph.subGraphRootedAt(sinkNode.child(0));
        // A non-keyed linear shape, with no keyed breaker gather, plus several Source splits for one
        // source and P>1: read the sources in parallel through R routers fanning into a single channel,
        // while the pass-through chain runs in the pipeline task. One Source or a single slice does not
        // enter this branch, since there would be no gain from parallelism.
        if (parallelism > 1 && childGraph.isLinearSegment() && linearSourceInputCount(childGraph) > 1) {
            return runLinearSourceShardedParallel(childGraph, output);
        }
        long[] count = {0};
        Sink counting = countingWrap(output, count);
        ExecutionReport memReport = runToTerminal(childGraph, PipelineExecutor.outputStage(counting));
        // The serial path leaves partitionRowCounts empty, since there are no parallel partitions; rowsOut is the number of rows written.
        return memReport.withRowsOut(count[0]);
    }

    /**
     * <b>Submits and executes the whole graph at once</b>, so a sub-graph shared by several sinks is
     * computed once. A single sink delegates to {@link #runSinkGraphReported}, which keeps the current
     * behaviour and its parallel optimizations untouched. Several sinks try a fan-out tee, where the
     * shared prefix is computed once and broadcast to each branch; without a shared prefix, meaning
     * disjoint sinks, or when a branch or the shared segment is not linear because of a binary, fall
     * back to executing each sink on its own, which stays correct because disjoint sinks share nothing.
     * <p>Scope: tee without re-join over linear branches. A diamond re-join and multiple sinks on a
     * cluster are handled by the cluster graph-building path.
     */
    public ExecutionReport runWholeGraph(PlanGraph fullGraph) {
        return runWholeGraph(fullGraph, () -> true);
    }

    /** The terminal gate for execution stays out of the PlanGraph and the wire; it decides success or cancel immediately before the Sink closes or the whole group publishes. */
    private ExecutionReport runWholeGraph(PlanGraph fullGraph, BooleanSupplier terminalGate) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(fullGraph);
        if (terminalGate == null) {
            throw new IllegalArgumentException("terminal gate is required");
        }
        PlanOps.validateExecutableTypes(fullGraph); // must precede commit-domain/output side effects
        OutputDestinationValidator.validate(fullGraph);  // Defence line for direct internal execution, and it must come before any sink.
        OutputCommitValidator.validate(fullGraph);
        List<Integer> sinks = new ArrayList<>();
        for (int i = 0; i < fullGraph.nodeCount(); i++) {
            if (fullGraph.node(i).kind() == PlanNode.Kind.SINK) {
                sinks.add(i);
            }
        }
        if (sinks.isEmpty()) {
            throw new IllegalArgumentException("whole PlanGraph has no SINK node");
        }
        if (sinks.size() <= 1
                && !(fullGraph.node(sinks.get(0)).output() instanceof io.rowweave.ManifestFileSink)) {
            Sink output = fullGraph.node(sinks.get(0)).output();
            return runSinkGraphReported(fullGraph, terminalGateOutput(output, terminalGate));
        }
        try (OutputCommitCoordinator coordinator = new OutputCommitCoordinator(fullGraph, sinks)) {
            ExecutionReport report;
            try {
                report = sinks.size() == 1
                        ? runSinkGraphReported(fullGraph, coordinator.stageOutput(sinks.get(0)))
                        : runMultiSinkGraph(fullGraph, sinks, coordinator);
            } catch (Throwable executionFailure) {
                coordinator.rollbackAfterFailure(executionFailure);
                throwUnchecked(executionFailure);
                return null;
            }
            if (!terminalGate.getAsBoolean()) {
                RuntimeException cancelled = new RuntimeException(
                        "whole graph terminal rejected by cancellation");
                coordinator.rollbackAfterFailure(cancelled);
                throw cancelled;
            }
            coordinator.publishAndFinalize();
            return report;
        }
    }

    /** Whole-graph reported entry; every owned execution context contributes exactly once. */
    public ExecutionReport runWholeGraphReported(
            PlanGraph graph, StageExecutionTelemetryAccumulator telemetry) {
        return runWholeGraphReported(graph, telemetry, () -> true);
    }

    /** For StageTask only: the telemetry collector and the terminal gate are runtime objects, so they do not pollute the serializable plan. */
    public ExecutionReport runWholeGraphReported(
            PlanGraph graph, StageExecutionTelemetryAccumulator telemetry,
            BooleanSupplier terminalGate) {
        if (telemetry == null) {
            throw new IllegalArgumentException("stage telemetry accumulator is required");
        }
        StageExecutionTelemetryAccumulator previous = reportedTelemetry;
        reportedTelemetry = telemetry;
        try {
            return runWholeGraph(graph, terminalGate);
        } finally {
            reportedTelemetry = previous;
        }
    }

    private static Sink terminalGateOutput(Sink delegate, BooleanSupplier terminalGate) {
        java.util.concurrent.atomic.AtomicBoolean abortStarted =
                new java.util.concurrent.atomic.AtomicBoolean();
        return new Sink() {
            private static final long serialVersionUID = 1L;
            @Override public DataSchema getSchema() { return delegate.getSchema(); }
            @Override public void collect(DataRow row) { delegate.collect(row); }
            @Override public void close() {
                if (!terminalGate.getAsBoolean()) {
                    if (abortStarted.compareAndSet(false, true)) delegate.abort();
                    throw new RuntimeException("output terminal rejected by cancellation");
                }
                delegate.close();
            }
            @Override public void abort() {
                if (abortStarted.compareAndSet(false, true)) delegate.abort();
            }
            @Override public boolean supportsCoordinatedCommit() { return delegate.supportsCoordinatedCommit(); }
            @Override public void prepareCommit() { delegate.prepareCommit(); }
            @Override public void commitPrepared() { delegate.commitPrepared(); }
            @Override public void rollbackCommit() { delegate.rollbackCommit(); }
            @Override public void finalizeCommit() { delegate.finalizeCommit(); }
            @Override public boolean isWorkerWritable() { return delegate.isWorkerWritable(); }
            @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
        };
    }

    private ExecutionReport runMultiSinkGraph(
            PlanGraph fullGraph, List<Integer> sinks, OutputCommitCoordinator coordinator) {
        // The linear chain of each sink, walking from sink.child along child(0) up to the SOURCE; if any chain contains a BINARY, fall back.
        List<List<Integer>> chains = new ArrayList<>();  // Each chain runs source to ... to sink.child, ascending toward the fork.
        boolean linearOk = true;
        for (int sinkIdx : sinks) {
            List<Integer> chain = linearChainToSource(fullGraph, fullGraph.node(sinkIdx).child(0));
            if (chain == null) {  // Non-linear, because a BINARY was met.
                linearOk = false;
                break;
            }
            chains.add(chain);
        }
        if (linearOk) {
            // The shared prefix on the source side is the longest common run of each chain from index 0.
            int forkPos = commonPrefixLen(chains) - 1;  // Position of the last shared node in each chain, which is the fork point.
            if (forkPos >= 0) {
                return runFanoutTee(fullGraph, sinks, chains, forkPos, coordinator);
            }
        }
        // Several sinks with a fan-out that re-joins into an order-consuming operator, meaning a
        // diamond, a self-join, or two binaries sharing a fork: materialize the whole graph so the
        // shared fork is computed once and each sink branch is built from a replay. Taken only when the
        // check finds something, otherwise disjoint or purely linear graphs fall back per sink.
        java.util.Set<Integer> matForks = MaterializationPlan.forkNodesToMaterialize(fullGraph);
        if (!matForks.isEmpty()) {
            return runMultiSinkMaterialized(fullGraph, sinks, matForks, coordinator);
        }
        // Fallback: disjoint sinks with nothing shared, or a non-linear shape, execute each sink on its own with the existing semantics and no duplicate computation.
        return runPerSink(fullGraph, sinks, coordinator);
    }

    /** Collects from head along child(0) up to the SOURCE and returns [source, ..., head] in ascending order; a node that is neither UNARY nor SOURCE, such as BINARY, yields null. */
    private static List<Integer> linearChainToSource(PlanGraph g, int head) {
        List<Integer> rev = new ArrayList<>();
        int idx = head;
        while (true) {
            PlanNode n = g.node(idx);
            rev.add(idx);
            if (n.kind() == PlanNode.Kind.SOURCE) {
                break;
            }
            if (n.kind() != PlanNode.Kind.UNARY) {
                return null;  // BINARY and the like, so non-linear and we fall back.
            }
            idx = n.child(0);
        }
        Collections.reverse(rev); // source → … → head
        return rev;
    }

    /** Length of the longest common node prefix of each chain from index 0, compared by node index. */
    private static int commonPrefixLen(List<List<Integer>> chains) {
        int min = Integer.MAX_VALUE;
        for (List<Integer> c : chains) {
            min = Math.min(min, c.size());
        }
        int len = 0;
        for (int pos = 0; pos < min; pos++) {
            int node = chains.get(0).get(pos);
            boolean allSame = true;
            for (List<Integer> c : chains) {
                if (c.get(pos) != node) {
                    allSame = false;
                    break;
                }
            }
            if (!allSame) {
                break;
            }
            len++;
        }
        return len;
    }

    /**
     * Fan-out tee: the shared segment chain[0..forkPos] is computed once, and each sink branch is
     * [the nodes after the fork ... sink.child] plus the output stage, with the fork output broadcast
     * by the tee and bound to the entry of each branch. The fork point of the shared segment emits to
     * the tee, which copies and broadcasts to every branch.
     */
    private ExecutionReport runFanoutTee(PlanGraph g, List<Integer> sinks, List<List<Integer>> chains, int forkPos,
                                         OutputCommitCoordinator coordinator) {
        boolean ownMem = sharedMem == null;
        io.rowweave.mem.ExecutionMemory mem = ownMem ? forJob(g) : sharedMem;
        try {
            PlanCompiler compiler = new PlanCompiler(parallelism, mem);
            List<Integer> shared = chains.get(0).subList(0, forkPos + 1); // source..fork
            int forkNode = shared.get(shared.size() - 1);
            // Stages of the shared segment, source through fork: compileFrom walks the chain and the fork is the head.
            List<PipelineExecutor.Stage> sharedStages =
                    compiler.compileFrom(g, forkNode, Collections.emptyMap());
            // Each branch runs from the node after the fork to sink.child, with the fork output bound through the tee as the placeholder source of the branch.
            java.util.concurrent.atomic.AtomicLong total =
                    new java.util.concurrent.atomic.AtomicLong();
            List<List<PipelineExecutor.Stage>> branches = new ArrayList<>();
            for (int s = 0; s < sinks.size(); s++) {
                List<Integer> chain = chains.get(s);
                int sinkIdx = sinks.get(s);
                Sink out = countingWrap(coordinator.stageOutput(sinkIdx), total);
                List<PipelineExecutor.Stage> branch = new ArrayList<>();
                // Build one stage per UNARY node after the fork, that is chain[forkPos+1 .. end].
                for (int pos = forkPos + 1; pos < chain.size(); pos++) {
                    branch.add(PlanOps.stageFor(g, chain.get(pos), parallelism, mem));
                }
                branch.add(PipelineExecutor.outputStage(out));
                branches.add(branch);
            }
            new PipelineExecutor().runFanout(sharedStages, branches);
            return report(mem, total.get(), null);
        } finally {
            if (ownMem) {
                captureTelemetry(mem);
                mem.close();
            }
        }
    }

    /**
     * <b>Whole-graph materialization for several sinks</b>: a diamond, a self-join, or two binaries
     * sharing a fork, with multiple outputs. <b>A shared fork is computed once</b>: every materializing
     * fork is filled once into a shared buffer in ascending node-index order over the index space of the
     * whole graph, which is topological order, and each sink branch is built with buildDagNode from its
     * own {@code child(0)} over the <b>whole graph</b>, so a buffer hit on an already materialized fork
     * replays instead of recomputing the shared upstream, then each branch runs its own runDag to that
     * sink's Sink.
     * <p>Whole-graph indices are used, and <b>not</b> {@code subGraphRootedAt}, because reindexing would
     * break the buffer keys. The fill and all sink branches <b>share the same memory</b>, so the spill
     * directories of the buffers are cleaned up by the final mem.close. forJob counts the materializing
     * forks as disk-only, outside the pool quota.
     */
    private ExecutionReport runMultiSinkMaterialized(
            PlanGraph g, List<Integer> sinks, java.util.Set<Integer> matForks,
            OutputCommitCoordinator coordinator) {
        boolean ownMem = sharedMem == null;
        int publicationOrders = 0;
        for (int sinkIdx : sinks) {
            if (requiresCanonicalPublicationOrder(g, sinkIdx)) publicationOrders++;
        }
        io.rowweave.mem.ExecutionMemory mem = ownMem
                ? forJob(g, matForks.size(), publicationOrders) : sharedMem;
        try {
            java.util.Map<Integer, io.rowweave.sched.MaterializedRowBuffer> buffers = new java.util.HashMap<>();
            java.util.List<Integer> ordered = new java.util.ArrayList<>(matForks);
            java.util.Collections.sort(ordered);  // Topological order: a child index sits below its own, so deeper forks fill first.
            for (int forkIdx : ordered) {
                io.rowweave.sched.MaterializedRowBuffer buf =
                        new io.rowweave.sched.MaterializedRowBuffer(g.node(forkIdx).outMeta(), mem.newSpillStore());
                PipelineExecutor.DagNode forkTree = buildDagNode(g, forkIdx, mem, buffers);
                new PipelineExecutor().runDag(forkTree, materializeTerminal(buf));
                buffers.put(forkIdx, buf);
            }
            long total = 0;
            for (int sinkIdx : sinks) {
                Sink output = coordinator.stageOutput(sinkIdx);
                long[] count = {0};
                Sink counting = countingWrap(output, count);
                PipelineExecutor.DagNode branch = buildDagNode(g, g.node(sinkIdx).child(0), mem, buffers);
                if (requiresCanonicalPublicationOrder(g, sinkIdx)) {
                    PlanGraph order = orderByAllFields(g.node(sinkIdx).outMeta());
                    branch = PipelineExecutor.DagNode.append(
                            branch, PlanOps.stageFor(order, 1, 1, mem));
                }
                new PipelineExecutor().runDag(branch, PipelineExecutor.outputStage(counting));
                total += count[0];
            }
            return report(mem, total, null);
        } finally {
            if (ownMem) {
                captureTelemetry(mem);
                mem.close();
            }
        }
    }

    /** Quality publications promise byte-stable members even when the API graph stays at 10/11 nodes. */
    private static boolean requiresCanonicalPublicationOrder(
            PlanGraph graph, int sinkIndex) {
        PlanNode sink = graph.node(sinkIndex);
        if (sink.kind() != PlanNode.Kind.SINK
                || !TypedQualityPublicationPlanValidator.isPublicationSinkRole(
                sink.param(PlanNode.OPERATOR_UID_PARAM))) {
            return false;
        }
        PlanNode child = graph.node(sink.child(0));
        return !("orderBy".equals(child.typeId()) || "top".equals(child.typeId()));
    }

    /** Fallback: narrow and execute each sink on its own with the existing semantics; disjoint sinks share nothing and are not computed twice. */
    private ExecutionReport runPerSink(
            PlanGraph g, List<Integer> sinks, OutputCommitCoordinator coordinator) {
        long total = 0;
        StageExecutionTelemetryAccumulator telemetry = new StageExecutionTelemetryAccumulator();
        List<PartitionDistribution> distributions = new ArrayList<>();
        for (int sinkIdx : sinks) {
            PlanGraph sinkGraph = g.subGraphRootedAt(sinkIdx);
            ExecutionReport report = runSinkGraphReported(
                    sinkGraph, coordinator.stageOutput(sinkIdx));
            total += report.rowsOut();
            telemetry.add(report.resourceTelemetry());
            distributions.addAll(report.partitionDistributions());
        }
        return ExecutionReport.fromTelemetry(telemetry.freeze(), total, distributions);
    }

    /** TypeIds of keyed breakers, the groupBy family and distinct, which can be parallelized by a keyed shuffle because equal keys land in one partition and each instance aggregates on its own. */
    private static boolean isKeyedBreakerTypeId(String typeId) {
        switch (typeId) {
            case "groupBySum": case "groupByCount": case "groupByMax": case "groupByMin":
            case "groupBy": case "distinct":
                return true;
            default:
                return false;
        }
    }

    /** TypeIds of total-order operators, OrderBy and Top with Rank to come, whose parallelism is always 1 so they gather P into 1 globally. */
    private static boolean isTotalOrderTypeId(String typeId) {
        return "orderBy".equals(typeId) || "top".equals(typeId);
    }

    /**
     * Shape recognition: a SINK above a keyed-unary, the groupBy family or distinct, above a single
     * SOURCE, which is the 3-node shape; <b>or</b> SINK over a total-order operator (orderBy or top), over
     * a keyed-unary, over SOURCE, which is the 4-node shape where the keyed stage runs in parallel and the
     * trailing operator gathers with degree 1.
     */
    private static boolean isSingleInputKeyedShape(PlanGraph sinkGraph) {
        PlanNode root = sinkGraph.root();
        if (root.kind() != PlanNode.Kind.SINK || root.childCount() != 1) {
            return false;
        }
        PlanNode below = sinkGraph.node(root.child(0));
        // 4-node shape: root.child(0) is the total-order operator (orderBy or top) and its child(0) is the keyed breaker.
        if (below.kind() == PlanNode.Kind.UNARY && below.childCount() == 1
                && isTotalOrderTypeId(below.typeId())) {
            PlanNode keyed = sinkGraph.node(below.child(0));
            return keyed.kind() == PlanNode.Kind.UNARY && keyed.childCount() == 1
                    && isKeyedBreakerTypeId(keyed.typeId())
                    && sinkGraph.node(keyed.child(0)).kind() == PlanNode.Kind.SOURCE;
        }
        // 3-node shape: root.child(0) is the keyed breaker, then through the pass-through chain to the SOURCE.
        if (below.kind() != PlanNode.Kind.UNARY || below.childCount() != 1
                || !isKeyedBreakerTypeId(below.typeId())) {
            return false;
        }
        return sourceAfterKeyedPassThrough(sinkGraph, below.child(0)) >= 0;
    }

    /**
     * Keyed pass-through whitelist: row-at-a-time stateless pass-through operators that a router can run
     * in parallel, independent of partitioning. It deliberately does <b>not</b> use {@link
     * PlanOps#isPassThrough}, which omits map, flatMap, filterFn and selectByIndex, and it excludes
     * partitionByHash, since that is a shuffle boundary.
     */
    private static boolean isKeyedPassThrough(PlanNode node) {
        String typeId = node.typeId();
        switch (typeId) {
            case "filter": case "filterFn": case "filterParams":
            case "select": case "selectByIndex": case "addFields":
            case "map": case "flatMap": case "mapPartition":
                return true;
            case "rowValidate":
                return SourcePositionPlanValidator.isPositionedValidation(node);
            case "rowClassify":
            case "qualityRoute":
            case "objectQualityRoute":
                return true;
            default:
                return false;
        }
    }

    /**
     * Walks from the child of the keyed breaker along child(0) <b>straight through</b> the pass-through
     * chain and returns the node index of the SOURCE at the end of the chain. If anything that is not
     * pass-through or not a SOURCE is met on the way, such as a breaker or a binary, or the chain head is
     * not single-input, returns -1, which means the keyed parallel shape does not match.
     */
    private static int sourceAfterKeyedPassThrough(PlanGraph graph, int idx) {
        while (true) {
            PlanNode n = graph.node(idx);
            if (n.kind() == PlanNode.Kind.SOURCE) {
                return idx;
            }
            if (n.kind() != PlanNode.Kind.UNARY || n.childCount() != 1
                    || !isKeyedPassThrough(n)) {
                return -1;
            }
            idx = n.child(0);
        }
    }

    /**
     * Keyed shuffle parallelism on the local path, inside the pipeline and <b>not through the Master</b>.
     * A single router reads the whole source lazily and partitions by key hash, feeding P keyed-operator
     * instances that each drain one inbound channel, run the keyed breaker and take their own spill
     * allowance, then M gathers into 1, then a trailing stable total-order OrderBy canonicalizes the
     * arrival order across keys, then the Sink. One thread per stage, so P+2 live threads through {@link
     * ConcurrentStages}, and no ExecutorService, whose pool is a deadlock trap under pure push. Memory is
     * forJob(P + 1): the P keyed-operator instances plus the trailing OrderBy.
     */
    private ExecutionReport runKeyedShuffleParallel(PlanGraph sinkGraph, Sink output) {
        PlanNode root = sinkGraph.root();
        PlanNode below = sinkGraph.node(root.child(0));
        // In the 4-node shape root.child(0) is the total-order operator (orderBy or top) and its child(0) is the keyed breaker; in the 3-node shape below is the keyed breaker itself.
        final PlanNode tailOrderNode = isTotalOrderTypeId(below.typeId()) ? below : null;
        PlanNode keyed = tailOrderNode != null ? sinkGraph.node(tailOrderNode.child(0)) : below;
        // The child of the keyed breaker may be the head of a pass-through chain rather than the SOURCE itself, so walk through to the SOURCE at the end of the chain.
        final int keyedChildIdx = keyed.child(0);
        final int sourceIdx = sourceAfterKeyedPassThrough(sinkGraph, keyedChildIdx);
        PlanNode source = sinkGraph.node(sourceIdx);
        // The outMeta of the keyed node's direct child (the tail of the pass-through chain), which is what
        // the partitioner, keyedKeyFields and the placeholder source use. Not sourceMeta: select and addFields
        // change column positions, so using sourceMeta would partition on the wrong column index.
        DataSchema keyedChildMeta = sinkGraph.node(keyedChildIdx).outMeta();
        // Whether there is a pass-through chain, meaning the keyed child is not the SOURCE itself.
        final boolean hasPassThrough = keyedChildIdx != sourceIdx;
        DataSchema keyedMeta = keyed.outMeta();
        String[] keys = keyedKeyFields(keyedChildMeta, keyed);
        final int p = parallelism;

        // Memory: P keyed-operator instances plus the trailing breaker, which is either an explicit total-order operator (orderBy or top) or the canonicalize OrderBy.
        int tailBreakers = (tailOrderNode != null || canonicalizeOutput) ? 1 : 0;
        io.rowweave.mem.ExecutionMemory mem = forOrdinaryOwners(p + tailBreakers);
        try {
            // Round-robin the N caller Sources into R=min(P, N) routers, giving P-way parallel source reads.
            List<SourceReadShard> routerInputs = sourceReadShards(
                    sinkGraph, sourceIdx, p);
            int r = routerInputs.size();

            // P keyed inbound channels: each receives R producers, one send per router, so producerCount=R and the close latch needs R done signals.
            List<io.rowweave.sched.MultiProducerChannel> keyedIn = new ArrayList<>(p);
            for (int i = 0; i < p; i++) {
                keyedIn.add(new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, r));
            }
            io.rowweave.sched.MultiProducerChannel sinkIn =
                    new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, p);
            List<io.rowweave.sched.Channel> allChannels =
                    new ArrayList<>(keyedIn);
            allChannels.add(sinkIn);

            // Sub-plan of the keyed operator: placeholder source at index 0 carrying keyedChildMeta, then the keyed-unary at index 1.
            PlanGraph keyedPlan = new PlanGraph(Arrays.asList(
                    PlanNode.placeholderSource(keyedChildMeta), rebuildKeyedOnChild0(keyed)));
            // Sink tail chain: (1) an explicit total-order operator (orderBy/top) becomes a placeholder source
            // feeding an OrderBy/Top rebuilt with **that node's own sort parameters**, whose index space is
            // keyedMeta — never re-encoded over all fields; (2) otherwise, if canonicalize is requested, a
            // full-field total-order OrderBy; (3) otherwise a direct pass-through (an unordered, set-equivalent result).
            PlanGraph sinkFragment = tailOrderNode != null
                    ? totalOrderTailFragment(keyedMeta, tailOrderNode)
                    : (canonicalizeOutput ? orderByAllFields(keyedMeta) : passthroughFragment(keyedMeta));

            // The partitioner is built from the meta of the node at the end of the pass-through chain, keyedChildMeta, so the key column positions are correct.
            io.rowweave.ops.Partitioner partitioner =
                    io.rowweave.ops.Partitioner.ofFields(keyedChildMeta, p, keys);

            List<io.rowweave.sched.Task> tasks = new ArrayList<>();
            List<String> names = new ArrayList<>();
            long[] sinkRows = {0};
            // Each router owns a private long[P] of counts, so nothing is shared and there is no contention; the arrays are merged after the join.
            List<long[]> routerCounts = new ArrayList<>(r);

            // Sub-graph of the pass-through chain, source through the chain rooted at the direct child of the keyed node. Each router runs it before partitioning, so the pass-through stage is parallel too.
            final PlanGraph chainGraph = hasPassThrough ? sinkGraph.subGraphRootedAt(keyedChildIdx) : null;
            // R routers: each reads its own batch of Sources, runs the pass-through chain first when there is one, then partitions by key and sends, then reports producerDone or fail.
            // On failure the handler fans out finishMulti(keyedIn) to poison all P keyed operators, which releases the routers and keyed operators parked on a send.
            for (int ri = 0; ri < r; ri++) {
                final io.rowweave.sched.stream.RowStream.CloseableRowStream rs =
                        sourceRowStreamOf(source, routerInputs.get(ri));
                final long[] localCounts = new long[p];
                routerCounts.add(localCounts);
                // The router body closes rs whether it read to the end or failed, since releasing executor resources is the job of try-finally.
                Runnable routerBody = hasPassThrough
                        ? () -> { try {
                                forWorker(1, mem).executeLinearBound(chainGraph,
                                        Collections.singletonMap(0, rs), routeStage(partitioner, keyedIn, localCounts, r));
                            } finally { rs.close(); } }
                        : () -> { try {
                                routeKeyed(rs, partitioner, keyedIn, localCounts, r);
                            } finally { rs.close(); } };
                tasks.add(new io.rowweave.sched.Task("keyed-router-" + ri, routerBody,
                        cause -> finishMulti(keyedIn, cause)));
                names.add("keyed-router-" + ri);
            }

            // P keyed-operator instances: drain keyedIn[i], run the keyed breaker, send to sinkIn.
            for (int i = 0; i < p; i++) {
                final io.rowweave.sched.MultiProducerChannel in = keyedIn.get(i);
                tasks.add(new io.rowweave.sched.Task("keyed-op-" + i,
                        () -> forWorker(1, mem).executeLinearBound(
                                keyedPlan, Collections.singletonMap(0, channelStream(in)),
                                channelSinkStage(sinkIn)),
                        cause -> {
                            if (cause != null) {
                                in.fail(cause);  // Releases the sends parked by a router on this channel, without waiting for a watchdog.
                                sinkIn.fail(cause);
                            } else {
                                sinkIn.producerDone();
                            }
                        }));
                names.add("keyed-op-" + i);
            }

            // Sink: drain sinkIn, run the trailing total-order OrderBy, write the Sink.
            Sink counting = countingWrap(output, sinkRows);
            tasks.add(new io.rowweave.sched.Task("keyed-sink",
                    () -> forWorker(1, mem).executeLinearBoundToOutput(
                            sinkFragment, Collections.singletonMap(0, channelStream(sinkIn)), counting),
                    cause -> { }));
            names.add("keyed-sink");

            io.rowweave.sched.ConcurrentStages.runFailFast(tasks, names, allChannels);
            mem.assertWithinBudget(); // ①
            // Collect the report before mem.close, then merge partitionRowCounts after the join, which gives the happens-before edge.
            int keyedStage = tailOrderNode != null ? tailOrderNode.child(0) : root.child(0);
            return report(mem, sinkRows[0], partitionDistribution(
                    routerCounts, keyedStage, PartitionRole.KEYED_OP, p));
        } finally {
            captureTelemetry(mem);
            mem.close();
        }
    }

    /** Router body: reads the source RowStream lazily and sends to the P inbound channels through the partitioner, so equal keys land in the same partition. */
    /**
     * An optional {@code localCounts}, the private long[P] of this router, which increments the count for
     * a partition on every send when it is present. <b>A per-router local array with nothing shared, so
     * there is no contention</b>; the caller merges and sums them after all routers join, which gives the
     * happens-before edge.
     */
    private static void routeKeyed(io.rowweave.sched.stream.RowStream in,
                                   io.rowweave.ops.Partitioner partitioner,
                                   List<io.rowweave.sched.MultiProducerChannel> keyedIn,
                                   long[] localCounts,
                                   int routerConcurrency) {
        int partitions = keyedIn.size();
        int batchSize = localShuffleBatchSize(partitions, routerConcurrency);
        // With P very large, batchSize=1 degrades to the row-at-a-time path entirely, so no staging array is allocated per partition.
        io.rowweave.sched.stream.StreamEvent[][] batches = batchSize > 1
                ? new io.rowweave.sched.stream.StreamEvent[partitions][]
                : null;
        int[] batchCounts = batchSize > 1 ? new int[partitions] : null;
        io.rowweave.sched.stream.StreamEvent e;
        while ((e = in.next()) != null) {
            if (e.isRecord()) {
                int part = partitioner.partitionOf(e.row());
                if (localCounts != null) {
                    localCounts[part]++;  // Read-only metric: counted after partitionOf and never fed back into routing.
                }
                if (batchSize == 1) {
                    keyedIn.get(part).send(e);  // A/B off or a very large P: the row-at-a-time path.
                } else {
                    io.rowweave.sched.stream.StreamEvent[] batch = batches[part];
                    if (batch == null) {
                        batch = new io.rowweave.sched.stream.StreamEvent[batchSize];
                        batches[part] = batch;
                    }
                    int count = batchCounts[part];
                    batch[count++] = e;
                    if (count == batchSize) {
                        keyedIn.get(part).sendRecords(batch, count);
                        Arrays.fill(batch, 0, count, null);  // Router staging keeps no extra reference to a DataRow already enqueued.
                        count = 0;
                    }
                    batchCounts[part] = count;
                }
            } else if (e.isError()) {
                throw new RuntimeException("source error before keyed shuffle", e.cause());
            }
        }
        if (batchSize > 1) {
            for (int part = 0; part < partitions; part++) {
                int count = batchCounts[part];
                if (count > 0) {
                    keyedIn.get(part).sendRecords(batches[part], count);
                    Arrays.fill(batches[part], 0, count, null);
                }
            }
        }
    }

    private static boolean localShuffleBatchEnabled() {
        return Boolean.parseBoolean(System.getProperty(LOCAL_SHUFFLE_BATCH_PROPERTY, "true"));
    }

    /** Measured: holding the lock across a batch from several producers forms a convoy, so batching is only enabled for a single router, that is one producer per channel. */
    static int localShuffleBatchSize(int partitions, int routerConcurrency) {
        if (partitions <= 0 || routerConcurrency <= 0) {
            throw new IllegalArgumentException("partitions and routerConcurrency must be > 0");
        }
        if (!localShuffleBatchEnabled() || routerConcurrency != 1) {
            return 1;
        }
        return Math.min(ROUTER_BATCH_MAX_PER_PARTITION,
                Math.max(1, ROUTER_STAGED_REFERENCES_MAX / partitions));
    }

    /** Merges the R local count arrays with bounds checking; a bad count only degrades distribution coverage and never changes the data result. */
    static PartitionDistribution partitionDistribution(List<long[]> perRouter,
                                                        int logicalStage,
                                                        PartitionRole role,
                                                        int p) {
        long[] total = new long[p];
        boolean invalid = false;
        for (long[] rc : perRouter) {
            if (rc == null || rc.length != p) {
                invalid = true;
                continue;
            }
            for (int i = 0; i < p; i++) {
                long value = rc[i];
                if (value < 0L || Long.MAX_VALUE - total[i] < value) {
                    invalid = true;
                } else {
                    total[i] += value;
                }
            }
        }
        boolean[] present = new boolean[p];
        if (!invalid) Arrays.fill(present, true);
        return new PartitionDistribution(logicalStage, role, p,
                invalid ? TelemetryCoverage.UNAVAILABLE : TelemetryCoverage.COMPLETE,
                present, invalid ? new long[p] : total);
    }

    /** Closing the inbound channels: producerDone on success, which posts EOS when the count reaches zero, and fail on error, which poisons them all and releases the parked senders. */
    private static void finishMulti(List<io.rowweave.sched.MultiProducerChannel> chs, Throwable cause) {
        for (io.rowweave.sched.MultiProducerChannel ch : chs) {
            if (cause != null) {
                ch.fail(cause);
            } else {
                ch.producerDone();
            }
        }
    }

    /**
     * Round-robin the N caller Sources (the parallel unit being the Source[] array) into R=min(p, N) batches,
     * one router per batch. With N=0 (no Source) a single empty router is used; with N>=1, R=min(p,N) and no
     * router is left empty, since an extra router would be pointless and would change producerCount.
     */
    private static List<List<io.rowweave.Source>> shardInputs(List<io.rowweave.Source> inputs, int p) {
        return InputSharding.shard(inputs, p);  // The local path and the cluster share one deterministic sharding, so the bytes agree.
    }

    /**
     * Given either an ordinary Input or a positioned subset of the bindings, returns a lazily read
     * RowStream that can be closed. Each source router uses it to read its own deterministic bucket. The
     * caller must close the stream; the ordinary path keeps using the cursor and creates no position.
     */
    private static io.rowweave.sched.stream.RowStream.CloseableRowStream sourceRowStreamOf(
            PlanNode source, SourceReadShard shard) {
        if (shard.bindings != null) {
            return io.rowweave.sched.stream.RowStream.fromSourceBindings(
                    source.param(PlanNode.OPERATOR_UID_PARAM), shard.bindings);
        }
        return io.rowweave.sched.stream.RowStream.fromInputs(shard.inputs);
    }

    /** Preserve the ordinary Source sharding path; opt-in positioned sources shard frozen bindings. */
    private static List<SourceReadShard> sourceReadShards(
            PlanGraph graph, int sourceIndex, int parallelism) {
        PlanNode source = graph.node(sourceIndex);
        List<SourceReadShard> shards = new ArrayList<>();
        if (SourcePositionPlanValidator.sourceFeedsPositionConsumer(
                graph, sourceIndex)) {
            for (List<io.rowweave.BatchSourceSplitBinding> bucket
                    : InputSharding.shardSourceBindings(
                    source.sourceSplitBindings(), parallelism)) {
                shards.add(SourceReadShard.positioned(bucket));
            }
        } else {
            for (List<io.rowweave.Source> bucket : shardInputs(
                    source.inputs(), parallelism)) {
                shards.add(SourceReadShard.ordinary(bucket));
            }
        }
        return shards;
    }

    private static final class SourceReadShard {
        private final List<io.rowweave.Source> inputs;
        private final List<io.rowweave.BatchSourceSplitBinding> bindings;

        private SourceReadShard(
                List<io.rowweave.Source> inputs,
                List<io.rowweave.BatchSourceSplitBinding> bindings) {
            this.inputs = inputs;
            this.bindings = bindings;
        }

        private static SourceReadShard ordinary(List<io.rowweave.Source> inputs) {
            return new SourceReadShard(inputs, null);
        }

        private static SourceReadShard positioned(
                List<io.rowweave.BatchSourceSplitBinding> bindings) {
            return new SourceReadShard(null, bindings);
        }
    }

    /** channel to lazily read RowStream, where the error is latched once so a retry cannot spin; the same semantics as WorkerProcess and LocalWorker. */
    private static io.rowweave.sched.stream.RowStream channelStream(io.rowweave.sched.Channel channel) {
        return io.rowweave.sched.stream.RowStream.fromChannel(channel);
    }

    /** Node index of the SOURCE in a linear childGraph; isLinearSegment guarantees it is unique. */
    private static int linearSourceIndex(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SOURCE) {
                return i;
            }
        }
        return -1;
    }

    /** Number of Source splits held by the source node of a linear childGraph, or 0 when there is no source or no inputs. */
    private static int linearSourceInputCount(PlanGraph graph) {
        int si = linearSourceIndex(graph);
        if (si < 0) {
            return 0;
        }
        List<io.rowweave.Source> inputs = graph.node(si).inputs();
        return inputs == null ? 0 : inputs.size();
    }

    /**
     * Parallel source-split reads for a non-keyed linear shape: InputSharding splits the inputs into R
     * batches, R routers each read one batch and fan into a single channel with no partitioning, and one
     * pipeline task runs source through the pass-through chain to the output. The concurrency skeleton of
     * the keyed path is reused, minus the partitioning. Pass-through is stateless, so the unordered fan-in
     * is equivalent as a set, and each slice of a non-replayable Source is read exactly once by one router
     * through round-robin assignment.
     */
    private ExecutionReport runLinearSourceShardedParallel(PlanGraph childGraph, Sink output) {
        int sourceIdx = linearSourceIndex(childGraph);
        PlanNode source = childGraph.node(sourceIdx);
        final int p = parallelism;
        io.rowweave.mem.ExecutionMemory mem = forJob(childGraph);
        try {
            List<SourceReadShard> routerInputs = sourceReadShards(
                    childGraph, sourceIdx, p);
            int r = routerInputs.size();
            // Fan-in on a single channel: R routers send, so producerCount=R and the channel closes only when the count reaches zero.
            io.rowweave.sched.MultiProducerChannel fanIn =
                    new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, r);
            List<io.rowweave.sched.Channel> allChannels = new ArrayList<>();
            allChannels.add(fanIn);
            long[] count = {0};
            Sink counting = countingWrap(output, count);

            List<io.rowweave.sched.Task> tasks = new ArrayList<>();
            List<String> names = new ArrayList<>();
            // R routers: each reads one batch of Sources and sends row by row to fanIn without partitioning; done reports producerDone, fail poisons fanIn.
            for (int ri = 0; ri < r; ri++) {
                final io.rowweave.sched.stream.RowStream.CloseableRowStream rs =
                        sourceRowStreamOf(source, routerInputs.get(ri));
                tasks.add(new io.rowweave.sched.Task("src-router-" + ri,
                        () -> {
                            try {
                                io.rowweave.sched.stream.StreamEvent e;
                                while ((e = rs.next()) != null) {
                                    if (e.isRecord()) {
                                        fanIn.send(e);
                                    } else if (e.isError()) {
                                        throw new RuntimeException("source split read error", e.cause());
                                    }
                                }
                            } finally { rs.close(); }  // Releasing executor resources is the job of try-finally.
                        },
                        cause -> { if (cause != null) fanIn.fail(cause); else fanIn.producerDone(); }));
                names.add("src-router-" + ri);
            }
            // Pipeline task: source, a placeholder bound to fanIn, through the pass-through chain to the output. Reuses executeLinearBound.
            tasks.add(new io.rowweave.sched.Task("linear-pipe",
                    () -> forWorker(1, mem).executeLinearBoundToOutput(
                            childGraph, Collections.singletonMap(sourceIdx, channelStream(fanIn)), counting),
                    cause -> { if (cause != null) fanIn.fail(cause); }));
            names.add("linear-pipe");

            io.rowweave.sched.ConcurrentStages.runFailFast(tasks, names, allChannels);
            mem.assertWithinBudget();
            return report(mem, count[0], null);
        } finally {
            captureTelemetry(mem);
            mem.close();
        }
    }

    /**
     * Terminal stage of a pass-through chain: drain the RowStream the chain outputs and send to the P
     * keyed channels through the partitioner. This is the PipelineExecutor.Stage form of {@link
     * #routeKeyed}, used to close out partitioning after a router has run the pass-through chain in front.
     */
    private static PipelineExecutor.Stage routeStage(io.rowweave.ops.Partitioner partitioner,
                                                     List<io.rowweave.sched.MultiProducerChannel> keyedIn,
                                                     long[] localCounts,
                                                     int routerConcurrency) {
        return (in, ignoredSink) -> routeKeyed(in, partitioner, keyedIn, localCounts, routerConcurrency);
    }

    /** Outbound edge of a keyed operator: drain the RowStream and send to the gather channel, where record sends and an error throws fail-fast. */
    private static PipelineExecutor.Stage channelSinkStage(io.rowweave.sched.Channel outbound) {
        return (in, ignoredSink) -> {
            io.rowweave.sched.stream.StreamEvent e;
            while ((e = in.next()) != null) {
                if (e.isRecord()) {
                    outbound.send(e);
                } else if (e.isError()) {
                    throw new RuntimeException("keyed-op error before shuffle send", e.cause());
                }
            }
        };
    }

    /** Rebuild the keyed node over child0, the placeholder source at index 0, passing params and listParams through unchanged. */
    private static PlanNode rebuildKeyedOnChild0(PlanNode keyed) {
        PlanNode.Builder b = PlanNode.unary(keyed.typeId(), keyed.outMeta(), 0);
        for (Map.Entry<String, String> e : keyed.params().entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, List<String>> e : keyed.listParams().entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
        return b.build();
    }

    /** Key fields for keyed partitioning: the keys parameter for groupBy, and keys or every column for distinct. */
    private static String[] keyedKeyFields(DataSchema childMeta, PlanNode keyed) {
        List<String> keys = keyed.listParam("keys");
        if (!keys.isEmpty()) {
            return keys.toArray(new String[0]);
        }
        String[] all = new String[childMeta.getFieldCount()];
        for (int i = 0; i < all.length; i++) {
            all[i] = childMeta.getField(i).getName();
        }
        return all;
    }

    /** placeholder-source(meta) to a stable total-order OrderBy over every field, which canonicalizes arrival order across parallel keys and makes the output bit-identical. */
    private static PlanGraph orderByAllFields(DataSchema meta) {
        PlanNode src = PlanNode.placeholderSource(meta);
        List<String> sort = new ArrayList<>();
        for (int i = 0; i < meta.getFieldCount(); i++) {
            sort.add(PlanOps.encodeSortField(new io.rowweave.ops.SortField(
                    i, io.rowweave.ops.SortField.Direction.ASC, io.rowweave.ops.SortField.NullOrder.NULLS_LAST)));
        }
        PlanNode ob = PlanNode.unary("orderBy", meta, 0).listParam("sort", sort).build();
        return new PlanGraph(Arrays.asList(src, ob));
    }

    /** The -d sink tail chain when canonicalize is false: a single placeholder source passing straight through, with no trailing operator, so the result is equivalent as a set but unordered. */
    private static PlanGraph passthroughFragment(DataSchema meta) {
        return new PlanGraph(Collections.singletonList(PlanNode.placeholderSource(meta)));
    }

    /**
     * Tail fragment for a total-order operator (orderBy/top): a placeholder source carrying keyedMeta feeding an
     * OrderBy/Top (child0 = source) rebuilt from **that node's original params and listParams**. The sort
     * parameters are passed through unchanged, because their index space is keyedMeta, and re-encoding them over
     * all fields would resolve to different columns. The tail operator runs at parallelism 1 (a single sink-in
     * consumer, forWorker(1, ...)), which makes its output globally ordered by construction.
     */
    private static PlanGraph totalOrderTailFragment(DataSchema keyedMeta, PlanNode tailOrderNode) {
        PlanNode src = PlanNode.placeholderSource(keyedMeta);
        PlanNode.Builder b = PlanNode.unary(tailOrderNode.typeId(), keyedMeta, 0);
        for (Map.Entry<String, String> e : tailOrderNode.params().entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, List<String>> e : tailOrderNode.listParams().entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
        return new PlanGraph(Arrays.asList(src, b.build()));
    }

    /** The -b shape recognition: SINK over a BINARY, join or coGroup, whose two children are both SOURCE, that is left x right into join or coGroup into SINK. */
    private static boolean isBinaryKeyedShape(PlanGraph sinkGraph) {
        PlanNode root = sinkGraph.root();
        if (root.kind() != PlanNode.Kind.SINK || root.childCount() != 1) {
            return false;
        }
        PlanNode bin = sinkGraph.node(root.child(0));
        if (bin.kind() != PlanNode.Kind.BINARY || bin.childCount() != 2
                || !("join".equals(bin.typeId()) || "coGroup".equals(bin.typeId()))) {
            return false;
        }
        // Each side walks its pass-through chain down to the SOURCE, so filter feeding join or coGroup is parallelized too.
        return sourceAfterKeyedPassThrough(sinkGraph, bin.child(0)) >= 0
                && sourceAfterKeyedPassThrough(sinkGraph, bin.child(1)) >= 0;
    }

    /**
     * Single-machine binary keyed shuffle running in parallel (join and coGroup, inside the pipeline, without
     * going through the master). Left and right each get R routers (the N Sources round-robined across them),
     * feeding P two-inbound consumers, each with its own spilling operator, then M fan into 1, then a
     * total-order OrderBy tail, then the Sink.
     *
     * <p><b>Partitioner key semantics</b>: join uses numericNormalized, which is scale-insensitive and treats
     * null as never equal to null; coGroup uses ofFields, which is scale-sensitive and groups null with null.
     * **The two must never be swapped**, since the same key landing in different partitions per side would group
     * rows wrongly.
     * <p>No cycle: each consumer's two inbounds are fed by R producers in one direction only, and both SMJ and
     * CoGroup drain both sides fully before merging, keeping the two phases decoupled.
     */
    private ExecutionReport runBinaryKeyedShuffleParallel(PlanGraph sinkGraph, Sink output) {
        PlanNode root = sinkGraph.root();
        PlanNode bin = sinkGraph.node(root.child(0));
        // Each side walks its pass-through chain down to the SOURCE. leftChildIdx and rightChildIdx are the direct children of the binary, either a chain head or the SOURCE.
        final int leftChildIdx = bin.child(0);
        final int rightChildIdx = bin.child(1);
        final int leftSrcIdx = sourceAfterKeyedPassThrough(sinkGraph, leftChildIdx);
        final int rightSrcIdx = sourceAfterKeyedPassThrough(sinkGraph, rightChildIdx);
        PlanNode leftSrc = sinkGraph.node(leftSrcIdx);
        PlanNode rightSrc = sinkGraph.node(rightSrcIdx);
        // The partitioner uses the outMeta of the binary node's direct child, the tail of the pass-through chain, not the source meta, because select changes column positions.
        DataSchema leftMeta = sinkGraph.node(leftChildIdx).outMeta();
        DataSchema rightMeta = sinkGraph.node(rightChildIdx).outMeta();
        final boolean leftPassThrough = leftChildIdx != leftSrcIdx;
        final boolean rightPassThrough = rightChildIdx != rightSrcIdx;
        DataSchema outMeta = bin.outMeta();
        String[] leftKeys = PlanOps.toArray(bin.listParam("leftKeys"));
        String[] rightKeys = PlanOps.toArray(bin.listParam("rightKeys"));
        boolean isCoGroup = "coGroup".equals(bin.typeId());
        final int p = parallelism;

        // The key semantics of the partitioner must match the operator. join ignores scale and treats
        // null as never equal to null, so numericNormalized; coGroup is scale sensitive and treats null as
        // equal to null, so ofFields, which is byte-identical to the grouping key CoGroup itself uses.
        io.rowweave.ops.Partitioner leftPart = isCoGroup
                ? io.rowweave.ops.Partitioner.ofFields(leftMeta, p, leftKeys)
                : io.rowweave.ops.Partitioner.numericNormalizedOfFields(leftMeta, p, leftKeys);
        io.rowweave.ops.Partitioner rightPart = isCoGroup
                ? io.rowweave.ops.Partitioner.ofFields(rightMeta, p, rightKeys)
                : io.rowweave.ops.Partitioner.numericNormalizedOfFields(rightMeta, p, rightKeys);

        JoinType joinType = isCoGroup ? null : JoinType.valueOf(bin.param("joinType"));
        JoinPhysicalStrategy joinStrategy = isCoGroup ? null
                : JoinPhysicalStrategy.fromPlanHint(bin.param("joinHint"));
        io.rowweave.CoGroupFunction coGroupFn =
                isCoGroup ? (io.rowweave.CoGroupFunction) bin.userFunction() : null;

        // Memory: P consumer instances plus the trailing breaker, which is one OrderBy when canonicalizing and none otherwise, so P + tailBreakers.
        int tailBreakers = canonicalizeOutput ? 1 : 0;
        MemoryOwnerRequirement[] requirements = new MemoryOwnerRequirement[p + tailBreakers];
        for (int i = 0; i < p; i++) {
            requirements[i] = MemoryOwnerRequirement.partitioned(2);
        }
        for (int i = p; i < requirements.length; i++) {
            requirements[i] = MemoryOwnerRequirement.ordinary();
        }
        io.rowweave.mem.ExecutionMemory mem = injectedPool != null
                ? io.rowweave.mem.ExecutionMemory.forJob(injectedPool, true, requirements)
                : io.rowweave.mem.ExecutionMemory.forJob(true, requirements);
        try {
            // The N Sources on each side are distributed round-robin over R routers.
            List<SourceReadShard> leftRouterInputs = sourceReadShards(
                    sinkGraph, leftSrcIdx, p);
            List<SourceReadShard> rightRouterInputs = sourceReadShards(
                    sinkGraph, rightSrcIdx, p);
            int rl = leftRouterInputs.size();
            int rr = rightRouterInputs.size();

            // P inbound channels per side: left takes rl producers, right takes rr producers, and one sink gathers them with P producers.
            List<io.rowweave.sched.MultiProducerChannel> leftIn = new ArrayList<>(p);
            List<io.rowweave.sched.MultiProducerChannel> rightIn = new ArrayList<>(p);
            for (int i = 0; i < p; i++) {
                leftIn.add(new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, rl));
                rightIn.add(new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, rr));
            }
            io.rowweave.sched.MultiProducerChannel sinkIn =
                    new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, p);
            List<io.rowweave.sched.Channel> allChannels = new ArrayList<>(leftIn);
            allChannels.addAll(rightIn);
            allChannels.add(sinkIn);

            PlanGraph sinkFragment = canonicalizeOutput
                    ? orderByAllFields(outMeta)
                    : passthroughFragment(outMeta);

            List<io.rowweave.sched.Task> tasks = new ArrayList<>();
            List<String> names = new ArrayList<>();
            long[] sinkRows = {0};
            // Each router on each side owns a private long[P]; partitionRowCounts is the merged per-partition input rows of left and right, which is the total inbound edge of the consumer.
            List<long[]> routerCounts = new ArrayList<>(rl + rr);

            // Sub-graph of the pass-through chain for each side, read with the source meta, through the pass-through chain, then partitioned.
            final PlanGraph leftChain = leftPassThrough ? sinkGraph.subGraphRootedAt(leftChildIdx) : null;
            final PlanGraph rightChain = rightPassThrough ? sinkGraph.subGraphRootedAt(rightChildIdx) : null;
            // The R left routers, reading with the source meta and running the pass-through chain before partitioning when there is one. On failure, finishMulti(leftIn) fans out to poison every channel.
            for (int ri = 0; ri < rl; ri++) {
                final io.rowweave.sched.stream.RowStream.CloseableRowStream rs =
                        sourceRowStreamOf(leftSrc, leftRouterInputs.get(ri));
                final long[] lc = new long[p];
                routerCounts.add(lc);
                Runnable body = leftPassThrough
                        ? () -> { try {
                                forWorker(1, mem).executeLinearBound(leftChain,
                                        Collections.singletonMap(0, rs), routeStage(leftPart, leftIn, lc, rl));
                            } finally { rs.close(); } }
                        : () -> { try { routeKeyed(rs, leftPart, leftIn, lc, rl); } finally { rs.close(); } };
                tasks.add(new io.rowweave.sched.Task("left-router-" + ri, body, cause -> finishMulti(leftIn, cause)));
                names.add("left-router-" + ri);
            }
            // The R right routers.
            for (int ri = 0; ri < rr; ri++) {
                final io.rowweave.sched.stream.RowStream.CloseableRowStream rs =
                        sourceRowStreamOf(rightSrc, rightRouterInputs.get(ri));
                final long[] rc = new long[p];
                routerCounts.add(rc);
                Runnable body = rightPassThrough
                        ? () -> { try {
                                forWorker(1, mem).executeLinearBound(rightChain,
                                        Collections.singletonMap(0, rs), routeStage(rightPart, rightIn, rc, rr));
                            } finally { rs.close(); } }
                        : () -> { try { routeKeyed(rs, rightPart, rightIn, rc, rr); } finally { rs.close(); } };
                tasks.add(new io.rowweave.sched.Task("right-router-" + ri, body, cause -> finishMulti(rightIn, cause)));
                names.add("right-router-" + ri);
            }

            // P consumers with two inbound edges: each runs its own operator, SortMergeJoin or CoGroup taking its own spill allowance, draining left and right and sending to sinkIn.
            for (int i = 0; i < p; i++) {
                final io.rowweave.sched.MultiProducerChannel l = leftIn.get(i);
                final io.rowweave.sched.MultiProducerChannel r = rightIn.get(i);
                final PipelineExecutor.BinaryStage consumer;
                if (isCoGroup) {
                    consumer = PipelineExecutor.managedBinaryBreaker(mem, binMM -> {
                        io.rowweave.ops.join.CoGroup cg = io.rowweave.ops.join.CoGroup.of(
                                leftMeta, rightMeta, leftKeys, rightKeys, coGroupFn)
                                .withSpill(binMM, mem.newSpillStore(), mem.segmentPool())
                                .withEqualKeyBlockThreshold(EQ_BLOCK_THRESHOLD);
                        return cg::coGroup;
                    });
                } else {
                    consumer = JoinRuntimeFactory.managed(mem, leftMeta, rightMeta, leftKeys, rightKeys,
                            joinType, joinStrategy,
                            Math.min(Integer.getInteger("rowweave.join.probeRows", 4096),
                                    SHUFFLE_CAPACITY / 2),
                            EQ_BLOCK_THRESHOLD);
                }
                tasks.add(new io.rowweave.sched.Task("binary-consumer-" + i,
                        () -> consumer.run(channelStream(l), channelStream(r), channelSinkRowSink(sinkIn)),
                        cause -> {
                            if (cause != null) {
                                l.fail(cause);
                                r.fail(cause);
                                sinkIn.fail(cause);
                            } else {
                                sinkIn.producerDone();
                            }
                        }));
                names.add("binary-consumer-" + i);
            }

            Sink counting = countingWrap(output, sinkRows);
            tasks.add(new io.rowweave.sched.Task("binary-sink",
                    () -> forWorker(1, mem).executeLinearBoundToOutput(
                            sinkFragment, Collections.singletonMap(0, channelStream(sinkIn)), counting),
                    cause -> { }));
            names.add("binary-sink");

            io.rowweave.sched.ConcurrentStages.runFailFast(tasks, names, allChannels);
            mem.assertWithinBudget();
            // Collect the report before mem.close, then merge the left and right partitionRowCounts after the join, giving the total inbound edge per partition.
            return report(mem, sinkRows[0], partitionDistribution(
                    routerCounts, root.child(0), PartitionRole.JOIN_CONSUMER, p));
        } finally {
            captureTelemetry(mem);
            mem.close();
        }
    }

    /** Adapts a RowSink onto the outbound channel: record sends, an error throws fail-fast, EOS and barriers are swallowed. Lets a binary join feed the channel directly. */
    private static io.rowweave.sched.stream.RowSink channelSinkRowSink(io.rowweave.sched.Channel outbound) {
        return event -> {
            if (event.isRecord()) {
                outbound.send(event);
            } else if (event.isError()) {
                throw new RuntimeException("join error before shuffle send", event.cause());
            }
        };
    }

    private static Sink countingWrap(Sink delegate, long[] count) {
        return new Sink() {
            @Override
            public DataSchema getSchema() {
                return delegate.getSchema();
            }

            @Override
            public void collect(DataRow row) {
                delegate.collect(row);
                count[0]++;
            }

            @Override
            public void close() {
                delegate.close();
            }

            @Override
            public void abort() {
                delegate.abort();
            }

            @Override
            public boolean supportsCoordinatedCommit() {
                return delegate.supportsCoordinatedCommit();
            }

            @Override
            public void prepareCommit() {
                delegate.prepareCommit();
            }

            @Override
            public void commitPrepared() {
                delegate.commitPrepared();
            }

            @Override
            public void rollbackCommit() {
                delegate.rollbackCommit();
            }

            @Override
            public void finalizeCommit() {
                delegate.finalizeCommit();
            }

            @Override
            public boolean isWorkerWritable() {
                return delegate.isWorkerWritable();
            }

            @Override
            public String exclusiveDestinationId() {
                return delegate.exclusiveDestinationId();
            }
        };
    }

    /** Fan-out branches write concurrently; their shared report counter must not be a racy long[]. */
    private static Sink countingWrap(
            Sink delegate, java.util.concurrent.atomic.AtomicLong count) {
        return new Sink() {
            @Override public DataSchema getSchema() { return delegate.getSchema(); }
            @Override public void collect(DataRow row) {
                delegate.collect(row);
                count.incrementAndGet();
            }
            @Override public void close() { delegate.close(); }
            @Override public void abort() { delegate.abort(); }
            @Override public boolean supportsCoordinatedCommit() {
                return delegate.supportsCoordinatedCommit();
            }
            @Override public void prepareCommit() { delegate.prepareCommit(); }
            @Override public void commitPrepared() { delegate.commitPrepared(); }
            @Override public void rollbackCommit() { delegate.rollbackCommit(); }
            @Override public void finalizeCommit() { delegate.finalizeCommit(); }
            @Override public boolean isWorkerWritable() { return delegate.isWorkerWritable(); }
            @Override public String exclusiveDestinationId() {
                return delegate.exclusiveDestinationId();
            }
        };
    }

    private ExecutionReport report(io.rowweave.mem.ExecutionMemory memory,
                                   long rowsOut,
                                   PartitionDistribution distribution) {
        StageExecutionTelemetry telemetry = freezeTelemetry(memory);
        List<PartitionDistribution> distributions = Collections.emptyList();
        if (distribution != null) {
            distributions = Collections.singletonList(distribution);
        }
        return ExecutionReport.fromTelemetry(telemetry, rowsOut, distributions);
    }

    private void captureTelemetry(io.rowweave.mem.ExecutionMemory memory) {
        StageExecutionTelemetryAccumulator target = reportedTelemetry;
        if (target == null) {
            forgetTelemetry(memory);
            return;
        }
        try {
            target.add(freezeTelemetry(memory));
        } catch (Throwable ignoredTelemetryFailure) {
            target.add(StageExecutionTelemetry.unavailable());
        } finally {
            forgetTelemetry(memory);
        }
    }

    private synchronized StageExecutionTelemetry freezeTelemetry(
            io.rowweave.mem.ExecutionMemory memory) {
        StageExecutionTelemetry telemetry = frozenTelemetry.get(memory);
        if (telemetry == null) {
            try {
                telemetry = telemetrySnapshotter.snapshot(memory);
            } catch (Throwable ignoredTelemetryFailure) {
                telemetry = StageExecutionTelemetry.unavailable();
            }
            if (telemetry == null) {
                telemetry = StageExecutionTelemetry.unavailable();
            }
            frozenTelemetry.put(memory, telemetry);
        }
        return telemetry;
    }

    private synchronized void forgetTelemetry(io.rowweave.mem.ExecutionMemory memory) {
        frozenTelemetry.remove(memory);
    }

    private static StageExecutionTelemetry telemetryOf(
            io.rowweave.mem.ExecutionMemory memory) {
        io.rowweave.mem.ExecutionMemorySnapshot snapshot = memory.telemetrySnapshot();
        return snapshot.complete()
                ? StageExecutionTelemetry.complete(snapshot.peakReservedBytes(),
                snapshot.acquiredBytes(), snapshot.pressureSpillCount(),
                snapshot.spillRunCount(), snapshot.spillLogicalBytesWritten(),
                snapshot.spillPeakReservedBytes())
                : StageExecutionTelemetry.unavailable();
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    /**
     * Shared by the linear and binary orchestrations: the terminal stage is attached last, either the
     * sink Consumer or the output Sink. This method owns the per-job ExecutionMemory lifecycle, running
     * forJob with lazy acquisition, then execution, then close in a finally, so the failure path also
     * releases back to the pool and deletes the temp directories with nothing leaked. Returns the {@link
     * ExecutionReport} peak for this execution.
     */
    private ExecutionReport runToTerminal(PlanGraph graph, PipelineExecutor.Stage terminal) {
        // A SINK-rooted graph carries a Sink instance and belongs to the staging path; execute and executeToOutput run a sub-plan without a SINK.
        if (graph.root().kind() == PlanNode.Kind.SINK) {
            throw new UnsupportedOperationException(
                    "SINK-rooted graph (Sink dispatch): subGraphRootedAt(SINK.child) first, "
                            + "then executeToOutput with the SINK's Sink");
        }
        // Decide the materializing forks first, the point where a fan-out that re-joins into an
        // order-consuming operator would otherwise deadlock. If there are any, they count toward the
        // breaker context, since materializing is disk-only and newSpillStore needs hasBreaker, and the
        // materialized assembly runs: the fill pre-pass, then a main pipeline that replays throughout.
        java.util.Set<Integer> matForks = MaterializationPlan.forkNodesToMaterialize(graph);
        io.rowweave.mem.ExecutionMemory mem = forJob(graph, matForks.size(), 0);
        try {
            if (graph.isLinearSegment()) {
                executeLinear(graph, terminal, mem);
            } else if (!matForks.isEmpty()) {
                executeWithMaterializedForks(graph, terminal, mem, matForks);
            } else {
                executeWithBinary(graph, terminal, mem);
            }
            // Collect spillCount before mem.close(); rowsOut and the partition counts are filled by the caller, since the row count is unknown here.
            return report(mem, 0, null);
        } finally {
            captureTelemetry(mem);
            mem.close();
        }
    }

    private void executeLinear(PlanGraph graph, PipelineExecutor.Stage terminal,
                               io.rowweave.mem.ExecutionMemory mem) {
        List<PipelineExecutor.Stage> stages = new PlanCompiler(parallelism, mem).compile(graph);
        PipelineExecutor exec = new PipelineExecutor();
        int i = 0;
        for (PipelineExecutor.Stage s : stages) {
            exec.addStage("n" + (i++), s);
        }
        exec.addStage("terminal", terminal);
        exec.run();
    }

    /**
     * Handles a graph with a single BINARY at one level: walk from the root along child(0) back to the
     * BINARY node, collecting the downstream UNARY chain, narrow and compile the left and right sub-graphs
     * separately with {@code subGraphRootedAt}, rebuild the binary operator, then wire it through
     * runBinary with the terminal stage at the end of the downstream chain.
     *
     * <p><b>Known limit</b>: because the two sides are rebuilt independently, a <b>shared SOURCE</b>, that
     * is a diamond DAG, is iterated once per side over the same Source. In the current IR a SOURCE node only
     * carries a replayable CollectionSource, so re-reading yields the same rows and the result stays
     * correct, but a non-replayable Source on a cluster, one with a single-pass cursor or a streaming
     * file, would read empty on the second iteration and compute a wrong result. Shared sources are
     * split per sub-graph when a cluster shuffle is wired in.
     */
    private void executeWithBinary(PlanGraph graph, PipelineExecutor.Stage terminal,
                                   io.rowweave.mem.ExecutionMemory mem) {
        // Recursively build the DagNode tree so binaries can nest to any depth, then runDag flattens it once and starts and joins it once.
        PipelineExecutor.DagNode root = buildDagNode(graph, graph.rootIndex(), mem, java.util.Collections.emptyMap());
        new PipelineExecutor().runDag(root, terminal);
    }

    /**
     * Materialized execution for a diamond, a self-join or an N-to-1 fan-out that re-joins into an
     * order-consuming operator. <b>The key move is a fill pre-pass in topological order followed by a main
     * pipeline that replays everywhere</b>: before the main pipeline is built, each materializing fork runs
     * its own fill pipeline in ascending node-index order, which PlanGraph guarantees to be topological
     * because a child index sits below its own, and fills a {@link MaterializedRowBuffer}. The main
     * pipeline then has <b>no live fork producer</b>, since every fork edge is replaced by a replay source,
     * and both inbound edges of a binary are finite replays, so the deadlock cycle disappears structurally,
     * without a latch or a watchdog.
     * <p>A shared fork is <b>computed once</b>: it is filled once and each downstream takes its own
     * {@code replay} cursor. The fill and the main pipeline <b>share one memory instance</b>, so the spill
     * directories of the buffers are cleaned up when that memory is closed.
     */
    private void executeWithMaterializedForks(PlanGraph graph, PipelineExecutor.Stage terminal,
                                              io.rowweave.mem.ExecutionMemory mem,
                                              java.util.Set<Integer> matForks) {
        java.util.Map<Integer, io.rowweave.sched.MaterializedRowBuffer> buffers = new java.util.HashMap<>();
        // Fill in topological order, ascending node index, so a deeper fork is filled first and a shallower fork's fill already finds its buffer ready.
        java.util.List<Integer> ordered = new java.util.ArrayList<>(matForks);
        java.util.Collections.sort(ordered);
        for (int forkIdx : ordered) {
            io.rowweave.sched.MaterializedRowBuffer buf =
                    new io.rowweave.sched.MaterializedRowBuffer(graph.node(forkIdx).outMeta(), mem.newSpillStore());
            // Fill pipeline: build the subtree rooted at forkIdx, replaying any fork already materialized inside it so there is no cycle, and end with materializeTerminal pouring into buf.
            PipelineExecutor.DagNode forkTree = buildDagNode(graph, forkIdx, mem, buffers);
            new PipelineExecutor().runDag(forkTree, materializeTerminal(buf));
            buffers.put(forkIdx, buf);
        }
        // Main pipeline: every fork edge is replaced by a replay source, so both binary inbound edges are finite.
        PipelineExecutor.DagNode root = buildDagNode(graph, graph.rootIndex(), mem, buffers);
        new PipelineExecutor().runDag(root, terminal);
    }

    /** Terminal stage of a fill: materialize the upstream stream into the buffer all at once, draining it to the end with spill, and exit as soon as the write is done. */
    private static PipelineExecutor.Stage materializeTerminal(io.rowweave.sched.MaterializedRowBuffer buf) {
        return (in, ignoredOut) -> buf.fill(in);
    }

    /** Replay source stage: emit rows one at a time from an independent cursor over a materialized buffer, standing in for the live fork producer subtree. */
    private static PipelineExecutor.Stage replaySource(io.rowweave.sched.MaterializedRowBuffer buf) {
        return (ignoredIn, out) -> {
            io.rowweave.sched.stream.RowStream rs = buf.replay();  // An independent cursor, since the fill has finished.
            io.rowweave.sched.stream.StreamEvent e;
            while ((e = rs.next()) != null) {
                if (e.isRecord()) {
                    out.emitRow(e.row());
                }
            }
        };
    }

    /**
     * Recursively builds a {@link PipelineExecutor.DagNode} from {@code headIndex}: follow child(0) looking
     * for a BINARY; when found, recurse into both subtrees, then that binary, then the UNARY chain above
     * it; when everything down to the SOURCE is UNARY, the node is linear. Binaries nested to any depth
     * are supported, such as union over union, join over join, or union over join.
     *
     * <p>{@code buffers} maps an already materialized fork node to its {@link
     * io.rowweave.sched.MaterializedRowBuffer}. During the walk, <b>as soon as a materialized fork node is
     * met, of any kind, UNARY, SOURCE or BINARY, the descent stops and a replay source is emitted</b>, so
     * every fork edge becomes a finite replay, the main pipeline has no live fork producer, the deadlock
     * cycle is gone and a shared fork is computed once. While a fork is being filled, {@code buffers}
     * holds only the <b>deeper</b> forks already filled and never the fork itself, so its own subtree is
     * still built by descending normally. An empty map means nothing is materialized, which is the plain path.
     */
    private PipelineExecutor.DagNode buildDagNode(PlanGraph graph, int headIndex,
                                                  io.rowweave.mem.ExecutionMemory mem,
                                                  java.util.Map<Integer, io.rowweave.sched.MaterializedRowBuffer> buffers) {
        // Walk from headIndex along child(0) looking for a BINARY, a SOURCE or a materialized fork; the UNARY nodes passed on the way are the downstream, closest to the root.
        List<Integer> downstream = new ArrayList<>();
        int idx = headIndex;
        while (true) {
            io.rowweave.sched.MaterializedRowBuffer buf = buffers.get(idx);
            if (buf != null) {
                // A materialized fork: emit the replay source plus the downstream, that is the UNARY chain above the fork, in dataflow order from the fork toward the head.
                List<PipelineExecutor.Stage> stages = new ArrayList<>();
                stages.add(replaySource(buf));
                java.util.Collections.reverse(downstream);  // Collected head to fork, reversed to fork to head, which is dataflow order.
                for (int dIdx : downstream) {
                    stages.add(PlanOps.stageFor(graph, dIdx, parallelism, mem));
                }
                return PipelineExecutor.DagNode.linear(stages);
            }
            if (graph.node(idx).kind() != PlanNode.Kind.UNARY) {
                break;  // SOURCE or BINARY.
            }
            downstream.add(idx);
            idx = graph.node(idx).child(0);
        }
        PlanNode head = graph.node(idx);
        if (head.kind() == PlanNode.Kind.SOURCE) {
            // A purely linear subtree, headIndex through source all UNARY with no materialized fork in between. compileFrom keeps the index space of the original graph for the source-to-head chain.
            List<PipelineExecutor.Stage> stages =
                    new PlanCompiler(parallelism, mem).compileFrom(graph, headIndex, java.util.Collections.emptyMap());
            return PipelineExecutor.DagNode.linear(stages);
        }
        // The head is a BINARY: build both subtrees recursively.
        java.util.Collections.reverse(downstream);  // After the binary: closest to the root first, all UNARY.
        PipelineExecutor.DagNode left = buildDagNode(graph, head.child(0), mem, buffers);
        PipelineExecutor.DagNode right = buildDagNode(graph, head.child(1), mem, buffers);
        PipelineExecutor.BinaryStage binary = binaryStageFor(graph, idx, mem);
        List<PipelineExecutor.Stage> downstreamStages = new ArrayList<>();
        for (int dIdx : downstream) {
            downstreamStages.add(PlanOps.stageFor(graph, dIdx, parallelism, mem));
        }
        return PipelineExecutor.DagNode.binary(left, right, binary, downstreamStages);
    }

    /** Equal-key block threshold behind the skew fallback of SortMergeJoin and CoGroup: a block larger than this spills to disk. The 1<<20 default is too generous, so it matches the Master. */
    private static final int EQ_BLOCK_THRESHOLD = 1024;

    /**
     * Rebuilds a BINARY operator: join becomes SortMergeJoin, union becomes Union adapted to two inputs,
     * and coGroup stays CoGroup. join and coGroup <b>always get the engine spill injected</b>, the shared
     * MemoryManager plus a per-operator DiskSpillStore, the segment pool and the equal-key block
     * threshold; union is a concatenating pass-through and takes no memory at all.
     */
    private PipelineExecutor.BinaryStage binaryStageFor(PlanGraph graph, int nodeIndex,
                                                        io.rowweave.mem.ExecutionMemory mem) {
        PlanNode node = graph.node(nodeIndex);
        DataSchema leftMeta = graph.node(node.child(0)).outMeta();
        DataSchema rightMeta = graph.node(node.child(1)).outMeta();
        switch (node.typeId()) {
            case "join": {
                JoinType type = JoinType.valueOf(node.param("joinType"));
                String[] leftKeys = PlanOps.toArray(node.listParam("leftKeys"));
                String[] rightKeys = PlanOps.toArray(node.listParam("rightKeys"));
                JoinPhysicalStrategy strategy = JoinPhysicalStrategy.fromPlanHint(node.param("joinHint"));
                return JoinRuntimeFactory.managed(mem, leftMeta, rightMeta, leftKeys, rightKeys,
                        type, strategy, Integer.getInteger("rowweave.join.probeRows", 4096),
                        EQ_BLOCK_THRESHOLD);
            }
            case "union": {
                Union u = Union.of(node.outMeta());
                return (l, r, out) -> u.applyStream(Arrays.asList(l, r), out);
            }
            case "coGroup": {
                // BINARY: leftKeys and rightKeys come from listParam and the CoGroupFunction from userFunction, which is serializable.
                String[] leftKeys = PlanOps.toArray(node.listParam("leftKeys"));
                String[] rightKeys = PlanOps.toArray(node.listParam("rightKeys"));
                io.rowweave.CoGroupFunction fn = (io.rowweave.CoGroupFunction) node.userFunction();
                io.rowweave.ops.join.CoGroup cg =
                        io.rowweave.ops.join.CoGroup.of(leftMeta, rightMeta, leftKeys, rightKeys, fn);
                return PipelineExecutor.managedBinaryBreaker(mem, binMM -> {
                    cg.withSpill(binMM, mem.newSpillStore(), mem.segmentPool())
                            .withEqualKeyBlockThreshold(EQ_BLOCK_THRESHOLD);
                    return cg::coGroup;
                });
            }
            default:
                throw new IllegalArgumentException(
                        "unsupported binary typeId in IR execution: " + node.typeId());
        }
    }
}
