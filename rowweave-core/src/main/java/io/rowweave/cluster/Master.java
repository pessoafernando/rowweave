package io.rowweave.cluster;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.mem.MemoryOwnerRequirement;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.ops.join.JoinRuntimeFactory;
import io.rowweave.sched.Channel;
import io.rowweave.sched.LocalChannel;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Master for the local path: the cluster roles as threads in one JVM. It cuts a SINK-rooted
 * {@link PlanGraph} into an {@link ExecutionGraph}, rebuilds a {@link StagePlan} for every stage,
 * assigns {@link LocalWorker} threads and wires the shuffle channels between stages, then starts the
 * workers, joins them and fails fast: when any worker fails, every channel is poisoned so that
 * nothing stays parked, reusing the poisoning pattern of PipelineExecutor.
 *
 * <p>A linear slice is one to one: exactly one channel between two stages, one producer and one
 * consumer. A keyed exchange is N by M.
 */
public final class Master {

    private static final int SHUFFLE_CAPACITY = 256;
    /**
     * Equal-key block threshold that guards sort-merge join against skew: a block above it spills its
     * right side to disk. It is set under a bounded budget, because a default of a million rows would
     * be far too large to hold.
     */
    private static final int SMJ_EQ_BLOCK_THRESHOLD = 1024;

    private final int parallelism;
    // A ManagedMemory can be injected to isolate a test; null means the production global pool. Each
    // run* method creates the one ExecutionMemory shared by every worker stage of this job together
    // with the joins, so a single budget gate holds the invariant, and closes it in a finally block.
    private final io.rowweave.mem.ManagedMemory injectedPool;

    public Master(int parallelism) {
        this(parallelism, (io.rowweave.mem.ManagedMemory) null);
    }

    /**
     * Accepts an injected ManagedMemory so a tiny pool provokes spilling and isolates a test; null in
     * production means the global pool.
     */
    public Master(int parallelism, io.rowweave.mem.ManagedMemory injectedPool) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
        }
        this.parallelism = parallelism;
        this.injectedPool = injectedPool;
    }

    /**
     * The ExecutionMemory shared by this job, which admits the whole set of owners atomically before
     * any worker is started.
     */
    private io.rowweave.mem.ExecutionMemory forJob(MemoryOwnerRequirement... requirements) {
        boolean workspaceNeeded = requirements.length > 0;
        return injectedPool != null
                ? io.rowweave.mem.ExecutionMemory.forJob(
                        injectedPool, workspaceNeeded, requirements)
                : io.rowweave.mem.ExecutionMemory.forJob(workspaceNeeded, requirements);
    }

    private static MemoryOwnerRequirement[] repeatedRequirement(
            MemoryOwnerRequirement requirement, int count) {
        MemoryOwnerRequirement[] requirements = new MemoryOwnerRequirement[count];
        java.util.Arrays.fill(requirements, requirement);
        return requirements;
    }

    /**
     * Executes a SINK-rooted PlanGraph and returns the number of rows its SINK stage wrote to the Sink,
     * which is the control signal the caller waits for. The Sink instance travels with the plan as a
     * property of the SINK node, and is written locally by the worker that owns that stage.
     */
    public long run(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        PlanNode root = sinkGraph.root();
        if (root.kind() != PlanNode.Kind.SINK) {
            throw new IllegalArgumentException("Master.run requires a SINK-rooted PlanGraph, got " + root.kind());
        }
        Sink output = root.output();
        if (output == null) {
            throw new IllegalStateException("SINK node has no Sink instance (must hold serialized Sink)");
        }

        ExecutionGraph eg = ExecutionGraphBuilder.build(sinkGraph);

        // Rebuild one StagePlan per stage.
        Map<Integer, StagePlan> plans = new HashMap<>();
        for (Stage s : eg.stages()) {
            plans.put(s.index(), StageBoundaryRebuilder.rebuild(sinkGraph, eg, s));
        }

        // Outbound channel per producer stage, one to one; a consumer takes its inbound channels by
        // the stage index it lists as input.
        Map<Integer, Channel> outChannelByStage = new HashMap<>();
        for (Stage s : eg.stages()) {
            if (!s.isSink()) {
                outChannelByStage.put(s.index(), new LocalChannel(SHUFFLE_CAPACITY));
            }
        }

        List<Channel> allChannels = new ArrayList<>(outChannelByStage.values());
        List<Task> tasks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        long[] sinkRows = {0};
        io.rowweave.mem.ExecutionMemory mem = forJob(
                PlanOps.memoryOwnerRequirements(sinkGraph));
        try {
            LocalWorker worker = new LocalWorker(parallelism, mem);

            for (Stage s : eg.stages()) {
                StagePlan plan = plans.get(s.index());
                Map<Integer, Channel> inbound = inboundChannels(s, outChannelByStage);
                if (s.isSink()) {
                    tasks.add(new Task("sink-stage" + s.index(),
                            () -> sinkRows[0] = worker.runSink(plan, inbound, output),
                            noOutboundFinish()));
                } else {
                    Channel out = outChannelByStage.get(s.index());
                    tasks.add(new Task("stage" + s.index(),
                            () -> worker.runIntermediate(plan, inbound, out),
                            outboundFinish(out)));
                }
                names.add("stage" + s.index());
            }

            startJoinFailFast(tasks, names, allChannels);
            mem.assertWithinBudget();  // the point where a successful run has proven it stayed within budget
            return sinkRows[0];
        } finally {
            mem.close();
        }
    }

    /**
     * Runs a keyed shuffle in parallel over the shape {@code source -> keyed unary (the groupBy family
     * or distinct) -> SINK}. One source fans out through an {@link io.rowweave.ops.Partitioner} to M
     * per-consumer inbound {@link io.rowweave.sched.MultiProducerChannel}s, so rows with the same key
     * always reach the same partition; M keyed breaker instances drain them and their outputs converge
     * into the one sink channel, where the sink worker writes the Sink. Returns the rows written.
     *
     * <p>Deadlock safety: each consumer holds a single inbound channel and drains only that one, so no
     * cycle can form in the wait-for graph.
     */
    public long runKeyed(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        PlanNode root = sinkGraph.root();
        if (root.kind() != PlanNode.Kind.SINK) {
            throw new IllegalArgumentException("runKeyed requires SINK-rooted PlanGraph, got " + root.kind());
        }
        Sink output = root.output();
        if (output == null) {
            throw new IllegalStateException("SINK node has no Sink instance");
        }
        int keyedIdx = root.child(0);
        PlanNode keyed = sinkGraph.node(keyedIdx);
        if (keyed.childCount() != 1) {
            throw new IllegalArgumentException("runKeyed expects source -> keyed-unary -> SINK; keyed not unary");
        }
        int sourceIdx = keyed.child(0);
        PlanNode source = sinkGraph.node(sourceIdx);
        if (source.kind() != PlanNode.Kind.SOURCE) {
            throw new IllegalArgumentException("runKeyed expects a single SOURCE feeding the keyed op");
        }
        String[] keys = keyedKeyFields(source.outMeta(), keyed);

        final int m = parallelism;
        io.rowweave.mem.ExecutionMemory mem = forJob(repeatedRequirement(
                MemoryOwnerRequirement.ordinary(), m));
        try {
            LocalWorker worker = new LocalWorker(parallelism, mem);

            // M keyed inbound channels, each with the router as its only producer, plus one sink
            // channel that converges M producers.
            List<io.rowweave.sched.MultiProducerChannel> keyedIn = new ArrayList<>(m);
            for (int i = 0; i < m; i++) {
                keyedIn.add(new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, 1));
            }
            io.rowweave.sched.MultiProducerChannel sinkIn =
                    new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, m);

            List<Channel> allChannels = new ArrayList<>(keyedIn);
            allChannels.add(sinkIn);

            // The keyed instance's sub-plan: placeholder source at index 0, keyed unary at index 1,
            // output schema is the keyed node's own.
            PlanGraph keyedPlan = new PlanGraph(java.util.Arrays.asList(
                    PlanNode.placeholderSource(source.outMeta()),
                    rebuildKeyedOnChild0(keyed)));
            // The sink sub-plan: a placeholder source at index 0 carrying the keyed output schema,
            // written directly with no tail operator.
            PlanGraph sinkConsumerPlan = new PlanGraph(java.util.Collections.singletonList(
                    PlanNode.placeholderSource(keyed.outMeta())));

            io.rowweave.ops.Partitioner partitioner =
                    io.rowweave.ops.Partitioner.ofFields(source.outMeta(), m, keys);

            // The router's StagePlan: the real source and no operators, so it routes what it reads.
            StagePlan routerPlan = new StagePlan(0,
                    new PlanGraph(java.util.Collections.singletonList(
                            source.withSourceSplitBindings(source.sourceSplitBindings()))),
                    java.util.Collections.emptyList(), false);

            List<Task> tasks = new ArrayList<>();
            List<String> names = new ArrayList<>();
            long[] sinkRows = {0};

            // The router task, one producer: once its input is drained it calls producerDone on each
            // keyed inbound channel, and the last report closes it.
            tasks.add(new Task("keyed-router",
                    () -> worker.runKeyedRouter(routerPlan, new HashMap<>(), partitioner, keyedIn),
                    cause -> finishMulti(keyedIn, cause)));
            names.add("keyed-router");

            // M keyed consumer tasks: drain keyedIn[i], run the keyed breaker, send to sinkIn, and each
            // calls producerDone on sinkIn when it is finished.
            for (int i = 0; i < m; i++) {
                final io.rowweave.sched.MultiProducerChannel in = keyedIn.get(i);
                tasks.add(new Task("keyed-consumer-" + i,
                        () -> worker.runKeyedConsumer(keyedPlan, 0, in, sinkIn),
                        cause -> {
                            // On success the router has drained its inbound channel to EOS already, so
                            // all that remains is sinkIn.producerDone. On failure, fail that inbound
                            // channel as well, which releases the router from a send parked on it
                            // immediately rather than waiting for the watchdog, then fail sinkIn to
                            // release the sink.
                            if (cause != null) {
                                in.fail(cause);
                                sinkIn.fail(cause);
                            } else {
                                sinkIn.producerDone();
                            }
                        }));
                names.add("keyed-consumer-" + i);
            }

            // The sink task: drain sinkIn and write the Sink.
            tasks.add(new Task("keyed-sink",
                    () -> sinkRows[0] = worker.runKeyedSink(sinkConsumerPlan, 0, sinkIn, output),
                    noOutboundFinish()));
            names.add("keyed-sink");

            startJoinFailFast(tasks, names, allChannels);
            mem.assertWithinBudget(); // ①
            return sinkRows[0];
        } finally {
            mem.close();
        }
    }

    /**
     * The keyed shuffle for a binary join, over the shape {@code left source x right source ->
     * join(BINARY) -> SINK}. Two independent routers each fan out to M channels through a partitioner
     * that normalises the join key numerically, so equal keys on the two sides land in the same
     * partition. The M join consumers have two inbound edges and drain the left one fully before the
     * right, which decouples the phases and leaves no cycle. Their M outputs converge into one sink
     * channel.
     */
    public long runBinaryKeyed(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        PlanNode root = sinkGraph.root();
        if (root.kind() != PlanNode.Kind.SINK) {
            throw new IllegalArgumentException("runBinaryKeyed requires SINK-rooted PlanGraph");
        }
        Sink output = root.output();
        if (output == null) {
            throw new IllegalStateException("SINK node has no Sink instance");
        }
        PlanNode join = sinkGraph.node(root.child(0));
        if (join.kind() != PlanNode.Kind.BINARY || !"join".equals(join.typeId())) {
            throw new IllegalArgumentException("runBinaryKeyed expects left × right -> join -> SINK; got " + join.typeId());
        }
        PlanNode leftSrc = sinkGraph.node(join.child(0));
        PlanNode rightSrc = sinkGraph.node(join.child(1));
        if (leftSrc.kind() != PlanNode.Kind.SOURCE || rightSrc.kind() != PlanNode.Kind.SOURCE) {
            throw new IllegalArgumentException("runBinaryKeyed expects a single SOURCE on each join side (N=1 per side)");
        }
        DataSchema leftMeta = leftSrc.outMeta();
        DataSchema rightMeta = rightSrc.outMeta();
        String[] leftKeys = join.listParam("leftKeys").toArray(new String[0]);
        String[] rightKeys = join.listParam("rightKeys").toArray(new String[0]);
        io.rowweave.ops.join.JoinType joinType =
                io.rowweave.ops.join.JoinType.valueOf(join.param("joinType"));
        JoinPhysicalStrategy joinStrategy = JoinPhysicalStrategy.fromPlanHint(join.param("joinHint"));

        final int m = parallelism;
        io.rowweave.mem.ExecutionMemory mem = forJob(repeatedRequirement(
                MemoryOwnerRequirement.partitioned(2), m));
        try {
        LocalWorker worker = new LocalWorker(parallelism, mem);

        // M left and M right inbound channels, each with one producer, its router, plus one sink
        // channel with M producers.
        List<io.rowweave.sched.MultiProducerChannel> leftIn = new ArrayList<>(m);
        List<io.rowweave.sched.MultiProducerChannel> rightIn = new ArrayList<>(m);
        for (int i = 0; i < m; i++) {
            leftIn.add(new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, 1));
            rightIn.add(new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, 1));
        }
        io.rowweave.sched.MultiProducerChannel sinkIn =
                new io.rowweave.sched.MultiProducerChannel(SHUFFLE_CAPACITY, m);

        List<Channel> allChannels = new ArrayList<>(leftIn);
        allChannels.addAll(rightIn);
        allChannels.add(sinkIn);

        // Numerically normalised partitioners: equal keys on either side land in the same partition,
        // and the match does not depend on scale.
        io.rowweave.ops.Partitioner leftPart =
                io.rowweave.ops.Partitioner.numericNormalizedOfFields(leftMeta, m, leftKeys);
        io.rowweave.ops.Partitioner rightPart =
                io.rowweave.ops.Partitioner.numericNormalizedOfFields(rightMeta, m, rightKeys);

        StagePlan leftRouterPlan = new StagePlan(0,
                new PlanGraph(java.util.Collections.singletonList(
                        leftSrc.withSourceSplitBindings(leftSrc.sourceSplitBindings()))),
                java.util.Collections.emptyList(), false);
        StagePlan rightRouterPlan = new StagePlan(1,
                new PlanGraph(java.util.Collections.singletonList(
                        rightSrc.withSourceSplitBindings(rightSrc.sourceSplitBindings()))),
                java.util.Collections.emptyList(), false);

        PlanGraph sinkConsumerPlan = new PlanGraph(java.util.Collections.singletonList(
                PlanNode.placeholderSource(join.outMeta())));

        List<Task> tasks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        long[] sinkRows = {0};

        tasks.add(new Task("left-router",
                () -> worker.runKeyedRouter(leftRouterPlan, new HashMap<>(), leftPart, leftIn),
                cause -> finishMulti(leftIn, cause)));
        names.add("left-router");
        tasks.add(new Task("right-router",
                () -> worker.runKeyedRouter(rightRouterPlan, new HashMap<>(), rightPart, rightIn),
                cause -> finishMulti(rightIn, cause)));
        names.add("right-router");

        // M join consumers, each with two inbound edges. One sort-merge instance per consumer, with a
        // bounded budget that spills and an equal-key block threshold, so skew cannot exhaust memory.
        for (int i = 0; i < m; i++) {
            final io.rowweave.sched.MultiProducerChannel l = leftIn.get(i);
            final io.rowweave.sched.MultiProducerChannel r = rightIn.get(i);
            // The same strategy and factory the local path uses at any parallelism; a partition that
            // turns out empty still holds no lease, because the managed wrapper never reserves one.
            final PipelineExecutor.BinaryStage consumer = JoinRuntimeFactory.managed(
                    mem, leftMeta, rightMeta, leftKeys, rightKeys, joinType, joinStrategy,
                    Math.min(Integer.getInteger("rowweave.join.probeRows", 4096), SHUFFLE_CAPACITY / 2),
                    SMJ_EQ_BLOCK_THRESHOLD);
            tasks.add(new Task("join-consumer-" + i,
                    () -> worker.runJoinConsumer(consumer, l, r, sinkIn),
                    cause -> {
                        // When a consumer fails, fail left, right and sinkIn together, which releases
                        // both routers from a parked send without the watchdog.
                        if (cause != null) {
                            l.fail(cause);
                            r.fail(cause);
                            sinkIn.fail(cause);
                        } else {
                            sinkIn.producerDone();
                        }
                    }));
            names.add("join-consumer-" + i);
        }

        tasks.add(new Task("join-sink",
                () -> sinkRows[0] = worker.runKeyedSink(sinkConsumerPlan, 0, sinkIn, output),
                noOutboundFinish()));
        names.add("join-sink");

        startJoinFailFast(tasks, names, allChannels);
        mem.assertWithinBudget(); // ①
        return sinkRows[0];
        } finally {
            mem.close();
        }
    }

    /**
     * Wraps up: on success every channel gets producerDone and the last report sends EOS; on failure
     * every channel is poisoned so that nothing stays parked.
     */
    private static void finishMulti(List<io.rowweave.sched.MultiProducerChannel> chs, Throwable cause) {
        for (io.rowweave.sched.MultiProducerChannel ch : chs) {
            if (cause != null) {
                ch.fail(cause);
            } else {
                ch.producerDone();
            }
        }
    }

    /** Rebuilds a keyed node so that its child 0 is the placeholder source at index 0. */
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

    /** The partition keys of a keyed node: the declared keys, or every column for distinct. */
    private static String[] keyedKeyFields(DataSchema childMeta, PlanNode keyed) {
        List<String> keys = keyed.listParam("keys");
        if (!keys.isEmpty()) {
            return keys.toArray(new String[0]);
        }
        // distinct partitions on the whole row.
        String[] all = new String[childMeta.getFieldCount()];
        for (int i = 0; i < all.length; i++) {
            all[i] = childMeta.getField(i).getName();
        }
        return all;
    }

    private static Map<Integer, Channel> inboundChannels(Stage s, Map<Integer, Channel> outChannelByStage) {
        Map<Integer, Channel> inbound = new HashMap<>();
        for (int up : s.inputStages()) {
            inbound.put(up, outChannelByStage.get(up));
        }
        return inbound;
    }

    /**
     * Wraps up an intermediate stage: on success the outbound channel is closed, which sends EOS; on
     * failure it is failed, which drains it, latches the error and wakes a send that is parked.
     */
    private static Consumer<Throwable> outboundFinish(Channel out) {
        return cause -> {
            if (cause != null) {
                out.fail(cause);
            } else {
                out.close();
            }
        };
    }

    /** A SINK stage has no outbound channel; its failure is collected and raised fail-fast. */
    private static Consumer<Throwable> noOutboundFinish() {
        return cause -> { };
    }

    /** Delegates to {@link io.rowweave.sched.ConcurrentStages#runFailFast}, the shared primitive. */
    private void startJoinFailFast(List<Task> tasks, List<String> names, List<Channel> allChannels) {
        io.rowweave.sched.ConcurrentStages.runFailFast(tasks, names, allChannels);
    }
}
