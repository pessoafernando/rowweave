package io.rowweave.sched;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.internal.RowSnapshot;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.MemoryOwnerRequirement;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The push executor: runs a chain of stages for real, one {@link Task} thread per stage, wired
 * through a bounded {@link LocalChannel}.
 *
 * <p>Every stage has the same signature, {@code (RowStream in, RowSink out)}. Whether an operator is
 * a pass-through that streams row by row or a breaker that consumes its upstream before emitting is a
 * detail inside its body and never a concern of the executor. {@link #passThroughStream} and
 * {@link #streamingBreaker} are kept as named factories only to label the operator category in the DAG
 * and to keep the metrics readable.
 *
 * <p><b>The data outlet {@link Sink} is the last stage of the pipeline</b>, built by
 * {@link #outputStage}, and not an external Consumer. The pipeline is self-contained: source, then the
 * middle stages, then the output stage, and {@link #run()} takes no arguments, because the caller only
 * has to run it and wait for every stage to finish. The Sink is written directly by the end of
 * execution, so no row travels back through the caller thread, which removes the misaligned
 * backpressure a caller thread draining the last channel would cause. The end of the data is expressed
 * by the pipeline itself.
 *
 * <p>Wiring: {@code source --ch0--> stage1 --ch1--> ... --> outputStage}. Stage i runs on its own Task
 * thread, adjacent stages are joined by a bounded channel, and credit backpressure carries a slow Sink
 * back to the source one stage at a time.
 *
 * <p><b>The only outlet</b>: {@code run()} returns void. There is no {@code run()} that returns a List,
 * and no operator returns a materialized result.
 */
public final class PipelineExecutor {

    private static final int CHANNEL_CAPACITY = 256;

    /** One execution stage: pulls the upstream {@link RowStream} and pushes the downstream {@link RowSink}. */
    @FunctionalInterface
    public interface Stage {
        void run(RowStream in, RowSink out);

        /** Release active blocking resources when the owning pipeline is cancelled. */
        default void cancel(Throwable cause) {
        }
    }

    /** A pass-through operator, such as filter, select, map or flatMap: streams row by row, reading and emitting at once with no buffering, protected by channel backpressure. */
    @FunctionalInterface
    public interface StreamStage {
        void run(RowStream in, RowSink out);
    }

    /**
     * The producing body of a pipeline breaker operator, such as sort, agg, distinct or join: it pulls
     * with {@code in.next()} until null, which means <b>the upstream is fully consumed</b>, and only then
     * emits. <b>The contract</b> is that the upstream must be drained to null before the first emit,
     * otherwise the breaker locks up its own upstream thread. Output is pushed row by row through a
     * {@link RowSink}; the full result is never materialized as a List, so memory holds only the working
     * set buffer that belongs to the operator itself.
     */
    @FunctionalInterface
    public interface BreakerStreamBody {
        void run(RowStream upstream, RowSink out);
    }

    /**
     * A binary operator such as SortMergeJoin or CoGroup: two inbound RowStreams, one outbound RowSink,
     * which matches their own join methods directly. A binary operator is a breaker: it <b>drains both
     * sides before emitting</b>, the same contract as BreakerStreamBody, now on two sides.
     */
    @FunctionalInterface
    public interface BinaryStage {
        void run(RowStream left, RowStream right, RowSink out);
    }

    /** A pass-through stage; the signature is {@link Stage}, so this factory only labels the semantics. */
    public static Stage passThroughStream(StreamStage stage) {
        return stage::run;
    }

    /** A streaming breaker stage; the signature is {@link Stage}, so this factory only labels the semantics. */
    public static Stage streamingBreaker(BreakerStreamBody body) {
        return body::run;
    }

    /**
     * A breaker stage wrapped with a release callback: once the body has finished producing, or failed,
     * its finally runs {@code onDone}, which is {@code ExecutionMemory.releaseOperatorMemory(opMM)}, and
     * the quota of this breaker goes back to the pool at once for concurrent jobs to reuse. The body
     * returning means everything has been produced, since the contract is to consume the upstream fully
     * before emitting and the N internal threads of a parallel breaker are joined before the body
     * returns.
     */
    public static Stage streamingBreaker(BreakerStreamBody body, Runnable onDone) {
        return (in, out) -> {
            Throwable primary = null;
            try {
                body.run(in, out);
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                ResourceCloser.close(primary, onDone::run);
            }
        };
    }

    /**
     * A dependency-aware managed breaker. It blocks for the first upstream event and only then requests
     * its managed memory and builds the body, so a downstream breaker that has produced nothing yet does
     * not sit on quota from the global pool. {@code runOnEmpty} is only for operators whose empty input
     * has user-visible semantics, namely whole-table ReduceGroup and combineReduce.
     *
     * <p>The first event is handed back unchanged through a single-slot replay stream. Whether the
     * factory or the body succeeds or fails, the lease is returned exactly once, and the first failure
     * follows the {@link ResourceCloser} rules.
     */
    public static Stage managedStreamingBreaker(
            ExecutionMemory memory, boolean runOnEmpty,
            Function<MemoryManager, BreakerStreamBody> factory) {
        if (memory == null) throw new IllegalArgumentException("memory must not be null");
        if (factory == null) throw new IllegalArgumentException("factory must not be null");
        return (upstream, out) -> {
            StreamEvent first = upstream.next();
            if (first == null && !runOnEmpty) {
                return;
            }
            MemoryManager operatorMemory = memory.newOperatorMemoryFor(
                    MemoryOwnerRequirement.ordinary());
            Throwable primary = null;
            try {
                BreakerStreamBody body = factory.apply(operatorMemory);
                if (body == null) {
                    throw new IllegalStateException("managed breaker factory returned null body");
                }
                body.run(first == null ? EMPTY_STREAM : prepend(first, upstream), out);
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                ResourceCloser.close(primary,
                        () -> memory.releaseOperatorMemory(operatorMemory));
            }
        };
    }

    /**
     * The two-input form of dependency-activated startup. It waits for the first left event and, if the
     * left side has already ended, probes the right side. With both sides ended, no lease is requested;
     * the binary body is built only when either side produced an event. At most one event per side is
     * replayed and no row set is read ahead, and the first-failure rules for factory, body and release
     * are exactly those of the unary managed breaker.
     */
    public static BinaryStage managedBinaryBreaker(
            ExecutionMemory memory, Function<MemoryManager, BinaryStage> factory) {
        return managedBinaryBreaker(memory, MemoryOwnerRequirement.partitioned(2), factory);
    }

    /** Binary breaker with an explicit node-local managed-memory owner contract. */
    public static BinaryStage managedBinaryBreaker(
            ExecutionMemory memory, MemoryOwnerRequirement requirement,
            Function<MemoryManager, BinaryStage> factory) {
        if (memory == null) throw new IllegalArgumentException("memory must not be null");
        if (requirement == null) throw new IllegalArgumentException("requirement must not be null");
        if (factory == null) throw new IllegalArgumentException("factory must not be null");
        return (left, right, out) -> {
            StreamEvent leftFirst = left.next();
            StreamEvent rightFirst = null;
            if (leftFirst == null) {
                rightFirst = right.next();
                if (rightFirst == null) {
                    return;
                }
            }
            MemoryManager operatorMemory = memory.newOperatorMemoryFor(requirement);
            Throwable primary = null;
            try {
                BinaryStage body = factory.apply(operatorMemory);
                if (body == null) {
                    throw new IllegalStateException("managed binary factory returned null body");
                }
                RowStream activeLeft = leftFirst == null ? EMPTY_STREAM : prepend(leftFirst, left);
                RowStream activeRight = rightFirst == null ? right : prepend(rightFirst, right);
                body.run(activeLeft, activeRight, out);
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                ResourceCloser.close(primary,
                        () -> memory.releaseOperatorMemory(operatorMemory));
            }
        };
    }

    /** A single-event replay, which preserves control events, ordering and the cancellation capability. */
    private static RowStream prepend(StreamEvent first, RowStream upstream) {
        return new RowStream() {
            private StreamEvent pending = first;

            @Override
            public StreamEvent next() {
                if (pending != null) {
                    StreamEvent event = pending;
                    pending = null;
                    return event;
                }
                return upstream.next();
            }

            @Override
            public boolean supportsCancellation() {
                return upstream.supportsCancellation();
            }

            @Override
            public boolean cancel(Throwable cause) {
                pending = null;
                return upstream.cancel(cause);
            }
        };
    }

    /** A producing stage: ignores the inbound RowStream and emits the given {@code Iterable<DataRow>} downstream row by row through emitRow. */
    public static Stage source(Iterable<DataRow> rows) {
        return (in, out) -> {
            for (DataRow r : rows) {
                out.emitRow(r);
            }
        };
    }

    /**
     * A producing stage built on SourceExecutor: builds the executor from the {@code inputs} description,
     * reads it and emits rows through emitRow, and closes it whether the read finished or failed, because
     * releasing resources is the job of try-finally. The local linear source read goes through this rather
     * than a bare concatenation of Iterables.
     */
    public static Stage sourceFromInputs(java.util.List<io.rowweave.Source> inputs) {
        return cancelAwareSource(
                () -> io.rowweave.sched.stream.RowStream.fromInputs(inputs));
    }

    /** exact source stage; positioned events are consumed by the directly adjacent validator. */
    public static Stage sourceFromBindings(
            String sourceOperatorUid,
            java.util.List<io.rowweave.BatchSourceSplitBinding> bindings) {
        return cancelAwareSource(
                () -> io.rowweave.sched.stream.RowStream.fromSourceBindings(
                        sourceOperatorUid, bindings));
    }

    private static Stage cancelAwareSource(
            Supplier<io.rowweave.sched.stream.RowStream.CloseableRowStream> streamFactory) {
        return new Stage() {
            private final AtomicReference<io.rowweave.sched.stream.RowStream.CloseableRowStream>
                    active = new AtomicReference<>();
            private final AtomicBoolean cancelled = new AtomicBoolean();

            @Override
            public void run(RowStream in, RowSink out) {
                io.rowweave.sched.stream.RowStream.CloseableRowStream stream = streamFactory.get();
                active.set(stream);
                try (io.rowweave.sched.stream.RowStream.CloseableRowStream rs = stream) {
                    if (cancelled.get()) {
                        rs.close();
                        return;
                    }
                    StreamEvent event;
                    while ((event = rs.next()) != null) {
                        if (event.isRecord()) {
                            out.emit(event);
                        } else if (event.isError()) {
                            out.emit(event);
                            return;
                        }
                    }
                } finally {
                    active.compareAndSet(stream, null);
                }
            }

            @Override
            public void cancel(Throwable cause) {
                cancelled.set(true);
                io.rowweave.sched.stream.RowStream.CloseableRowStream stream = active.get();
                if (stream != null) {
                    stream.close();
                }
            }
        };
    }

    /**
     * The trailing Sink stage: pulls the upstream RowStream, calls {@code output.collect(row)} for every
     * row, and calls {@code output.close()} once the upstream is exhausted, which flushes and releases
     * resources. A terminal stage has no downstream, so its {@code out} sink is unused. Control events:
     * a RECORD is collected, an error throws to fail fast, and EOS, barrier and cancel are ignored,
     * because closing the channel is the end-of-data signal. Every RECORD is checked before collect that
     * its actual row width matches the Sink DataSchema; a bad row takes the same abort path, so a damaged
     * schema can never be published by a job that reports success.
     */
    public static Stage outputStage(Sink output) {
        DataSchema outputMeta = output.getSchema();
        if (outputMeta == null) {
            throw new IllegalArgumentException(
                    "output schema must not be null: output=" + output.getClass().getName());
        }
        int expectedFieldCount = outputMeta.getFieldCount();
        String outputType = output.getClass().getName();
        return (in, ignoredSink) -> {
            try {
                long recordNumber = 0;
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        DataRow row = e.row();
                        recordNumber++;
                        if (row == null || row.size() != expectedFieldCount) {
                            throw new IllegalStateException(
                                    "output row width mismatch: output=" + outputType
                                            + " record=" + recordNumber
                                            + " field count " + expectedFieldCount
                                            + " expected=" + expectedFieldCount
                                            + " actual=" + (row == null ? "null" : row.size()));
                        }
                        output.collect(row);
                    } else if (e.isError()) {
                        throw new RuntimeException("pipeline output stage error", e.cause());
                    }
                    // EOS/Barrier/Cancel: channel close is the end signal
                }
                output.close();  // Normal path: commit, which flushes and releases.
            } catch (Throwable t) {
                // On failure: undo this attempt, where FileSink clears its staging, then release. An abort exception is added as suppressed, never swallowed silently.
                try {
                    output.abort();
                } catch (Throwable a) {
                    t.addSuppressed(a);
                }
                throw t;  // Still thrown, so the caller fails fast.
            }
        };
    }

    /**
     * The trailing sink stage: it pulls the upstream RowStream and calls {@code sink.accept(row)} per row, and
     * **does not close or abort**, which is how it differs from {@link #outputStage} — a bare {@link Consumer} has
     * no resource semantics, and wrapping it in a fake Sink to fire a fake close would be a trap. Control events:
     * RECORD is accepted, an Error is thrown (fail fast, never swallowed silently on a correctness-critical path),
     * and EOS, Barrier and Cancel are ignored, since closing the channel ends the stage. Used by PushDagExecutor to
     * feed an IR sub-plan row by row to an external Consumer: tests collect through CollectingRowSink, and the
     * cluster path connects SINK to a Sink.
     */
    public static Stage sinkStage(Consumer<DataRow> sink) {
        return (in, ignoredSink) -> {
            StreamEvent e;
            while ((e = in.next()) != null) {
                if (e.isRecord()) {
                    sink.accept(e.row());
                } else if (e.isError()) {
                    throw new RuntimeException("pipeline sink stage error", e.cause());
                }
                // EOS/Barrier/Cancel: channel close is the end signal
            }
        };
    }

    /** An empty RowStream: next() always returns null, which is the placeholder upstream of a producing stage. */
    private static final RowStream EMPTY_STREAM = () -> null;

    /** The placeholder downstream sink of a terminal stage: a terminal stage must not emit, so an emit is a bug and fails loudly. */
    private static final RowSink TERMINAL_SINK = event -> {
        throw new IllegalStateException("terminal stage must not emit downstream");
    };

    /** A terminal stage has no downstream channel, so onFinish is empty: failures are collected on Task.failure and the caller fails fast. */
    private static final Consumer<Throwable> NO_FINISH = cause -> { };

    private final List<Stage> stages = new ArrayList<>();
    private final List<String> stageNames = new ArrayList<>();
    private Supplier<Channel> channelFactory = () -> new LocalChannel(CHANNEL_CAPACITY);

    /**
     * Deadlock watchdog timeout in milliseconds. When <b>every task thread is parked, every channel's
     * throughput is flat, and no task has completed</b> for longer than this value, the pipeline is
     * declared deadlocked: the channels are poisoned, the threads interrupted, and the run fails fast
     * instead of hanging forever. The default is five minutes, far above any normal CPU or I/O pause of a
     * single stage, so only a genuine deadlock is caught. Adjust it through the system property
     * {@code rowweave.pipeline.deadlockTimeoutMs} or {@link #withDeadlockTimeoutMillis}. A value of zero
     * or below switches the watchdog off, which waits indefinitely.
     */
    private long deadlockTimeoutMillis =
            Long.getLong("rowweave.pipeline.deadlockTimeoutMs", 300_000L);

    public PipelineExecutor addStage(Stage stage) {
        return addStage("stage-" + stages.size(), stage);
    }

    public PipelineExecutor addStage(String name, Stage stage) {
        stages.add(stage);
        stageNames.add(name);
        return this;
    }

    /** Injects the channel factory. LocalChannel passing references is the default; a test can supply a small capacity to exercise backpressure. */
    public PipelineExecutor withChannelFactory(Supplier<Channel> channelFactory) {
        this.channelFactory = channelFactory;
        return this;
    }

    /** Sets the deadlock watchdog timeout in milliseconds; zero or below disables it. A short timeout lets a test expose a deadlock quickly. */
    public PipelineExecutor withDeadlockTimeoutMillis(long millis) {
        this.deadlockTimeoutMillis = millis;
        return this;
    }

    /**
     * Runs the whole self-contained pipeline, which is <b>the only outlet</b> and takes no arguments.
     * One thread per stage, adjacent stages joined by a bounded channel, and the data written by the last
     * stage, normally {@link #outputStage}. The caller thread joins every stage and then fails fast with
     * the exception of the first stage that failed, in stage order.
     */
    public void run() {
        if (stages.isEmpty()) {
            return;  // No stage, so no data.
        }
        List<Task> tasks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<Channel> allChannels = new ArrayList<>();
        // The linear chain: stages[0..n-2] are middle stages, each with an outbound channel, and the last stage is the terminal one with no downstream.
        RowStream upstream = wireMiddleChain(stages.subList(0, stages.size() - 1),
                stageNames.subList(0, stages.size() - 1), EMPTY_STREAM, tasks, names, allChannels);
        int last = stages.size() - 1;
        final RowStream terminalInput = upstream;
        final Stage terminalStage = stages.get(last);
        tasks.add(stageTask(stageNames.get(last), terminalStage,
                terminalInput, TERMINAL_SINK, NO_FINISH));
        names.add(stageNames.get(last));

        startJoinFailFast(tasks, names, allChannels);
    }

    /**
     * A node of a DAG subtree, which is what lets binaries nest recursively.
     * <ul>
     * <li>{@link #linear(List)}: a linear chain of stages, whose head is the source or subtree entry and
     * whose tail is the output of that subtree.</li>
     * <li>{@link #binary(DagNode, DagNode, BinaryStage, List)}: two recursively built subtrees, the
     * binary consuming both, and a linear downstream chain after it.</li>
     * </ul>
     * {@link #runDag} flattens the whole tree into one set of tasks and channels, then starts and joins it
     * once, so binaries nested to any depth cannot leave two breakers waiting on each other: every stage
     * runs concurrently on its own thread, with channel backpressure and one shared fail-fast.
     */
    public static final class DagNode {
        final List<Stage> linear;  // Non-null means a linear subtree.
        final DagNode left;  // Non-null means a binary subtree.
        final DagNode right;
        final BinaryStage binary;
        final List<Stage> downstream;  // The linear UNARY chain after the binary, not including the terminal stage.

        private DagNode(List<Stage> linear, DagNode left, DagNode right, BinaryStage binary, List<Stage> downstream) {
            this.linear = linear;
            this.left = left;
            this.right = right;
            this.binary = binary;
            this.downstream = downstream;
        }

        public static DagNode linear(List<Stage> stages) {
            return new DagNode(stages, null, null, null, null);
        }

        public static DagNode binary(DagNode left, DagNode right, BinaryStage binary, List<Stage> downstream) {
            return new DagNode(null, left, right, binary, downstream);
        }

        /** Append one unary stage without materializing or rebuilding the upstream DAG. */
        public static DagNode append(DagNode upstream, Stage stage) {
            if (upstream == null || stage == null) {
                throw new IllegalArgumentException("upstream DAG and appended stage are required");
            }
            if (upstream.linear != null) {
                List<Stage> stages = new ArrayList<>(upstream.linear);
                stages.add(stage);
                return linear(stages);
            }
            List<Stage> stages = new ArrayList<>(upstream.downstream);
            stages.add(stage);
            return binary(upstream.left, upstream.right, upstream.binary, stages);
        }
    }

    /**
     * Runs a DAG with binaries nested to any depth. The whole {@link DagNode} tree is recursively
     * flattened into one set of tasks and channels, the terminal is attached at the end, and the run is
     * started and joined <b>once</b>, so all stages are concurrent, channels carry backpressure and a
     * single fail-fast governs the outcome.
     */
    public void runDag(DagNode root, Stage terminal) {
        List<Task> tasks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<Channel> allChannels = new ArrayList<>();
        RowStream rootOut = wireSubtree(root, "n", tasks, names, allChannels);
        final RowStream terminalInput = rootOut;
        tasks.add(stageTask("terminal", terminal, terminalInput, TERMINAL_SINK, NO_FINISH));
        names.add("terminal");
        startJoinFailFast(tasks, names, allChannels);
    }

    /**
     * <b>Fan-out tee execution</b>, where a shared sub-graph is computed once and broadcast to N branches.
     * {@code sharedChain} is the linear chain from the source to the fork point, computed once, and
     * {@code branches} are N downstream branches, each a linear stage chain whose <b>last stage must be a
     * terminal stage</b>, that is an output stage. Each row produced by the shared chain is
     * <b>broadcast</b> to the entry channel of every branch as a defensive copy, so a branch mutating its
     * row in place cannot leak into another, and the last branch takes no copy. All tasks and channels are
     * flattened, then started and joined once, so each branch has its own threads with bounded-channel
     * backpressure and a slow branch only heads-of-line blocks, without deadlock. This covers a tee
     * without re-join; a diamond re-join goes through whole-graph assembly.
     */
    public void runFanout(List<Stage> sharedChain, List<List<Stage>> branches) {
        if (branches.size() < 2) {
            throw new IllegalArgumentException("runFanout requires >=2 branches; single sink use run()");
        }
        List<Task> tasks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<Channel> allChannels = new ArrayList<>();

        // The shared chain: source through fork are all middle stages, and the RowStream returned is the fork point output.
        RowStream sharedOut = wireMiddleChain(sharedChain, namesFor("shared", sharedChain.size()),
                EMPTY_STREAM, tasks, names, allChannels);

        // One entry channel per branch; the broadcast task copies each row of sharedOut and sends it to every branch entry.
        List<Channel> branchInlets = new ArrayList<>();
        for (int b = 0; b < branches.size(); b++) {
            Channel inlet = channelFactory.get();
            allChannels.add(inlet);
            branchInlets.add(inlet);
        }
        final RowStream sharedIn = sharedOut;
        final List<Channel> inlets = branchInlets;
        tasks.add(new Task("tee-broadcast", () -> broadcast(sharedIn, inlets), broadcastFinish(inlets)));
        names.add("tee-broadcast");

        // Each branch: its entry channel, then the branch's linear chain ending in the terminal output stage, which has no downstream channel.
        for (int b = 0; b < branches.size(); b++) {
            List<Stage> branch = branches.get(b);
            if (branch.isEmpty()) {
                throw new IllegalArgumentException("branch " + b + " empty; must end with terminal stage");
            }
            RowStream branchUp = wireMiddleChain(branch.subList(0, branch.size() - 1),
                    namesFor("br" + b, branch.size() - 1), channelStream(branchInlets.get(b)),
                    tasks, names, allChannels);
            final RowStream terminalInput = branchUp;
            final Stage terminalStage = branch.get(branch.size() - 1);
            final String tn = "br" + b + "-out";
            tasks.add(stageTask(tn, terminalStage,
                    terminalInput, TERMINAL_SINK, NO_FINISH));
            names.add(tn);
        }

        startJoinFailFast(tasks, names, allChannels);
    }

    /** The tee broadcast: each row of the shared chain is copied and sent to every branch entry channel, with the last branch taking the original and saving one copy. A RECORD broadcasts, an error is passed to all branches. */
    private static void broadcast(RowStream shared, List<Channel> inlets) {
        StreamEvent e;
        while ((e = shared.next()) != null) {
            if (e.isRecord()) {
                DataRow row = e.row();
                for (int i = 0; i < inlets.size(); i++) {
                    // Defensive copy: the same row goes to N branches, and a branch mapping it in place would corrupt the others. The last branch needs no copy.
                    DataRow r = (i == inlets.size() - 1) ? row : RowSnapshot.copyOf(row);
                    inlets.get(i).send(StreamEvent.record(r));
                }
            } else if (e.isError()) {
                for (Channel inlet : inlets) {
                    inlet.fail(new RuntimeException("shared upstream error", e.cause()));
                }
                return;
            }
        }
    }

    /** Closing the broadcast task: on success close every branch entry channel, on failure fail every one, which passes the fail-fast through. */
    private static Consumer<Throwable> broadcastFinish(List<Channel> inlets) {
        return cause -> {
            for (Channel inlet : inlets) {
                if (cause != null) {
                    inlet.fail(cause);
                } else {
                    inlet.close();
                }
            }
        };
    }

    /** Recursively wires a subtree into tasks and channels and returns the RowStream it outputs. */
    private RowStream wireSubtree(DagNode node, String prefix, List<Task> tasks, List<String> names,
                                  List<Channel> allChannels) {
        if (node.linear != null) {
            // A linear subtree: its first stage starts from EMPTY_STREAM, the source, the whole chain is wired as middle stages, and the tail RowStream is returned.
            return wireMiddleChain(node.linear, namesFor(prefix + "-lin", node.linear.size()),
                    EMPTY_STREAM, tasks, names, allChannels);
        }
        // A binary subtree: recurse left and right into two RowStreams, the binary task pushes midCh, and the downstream linear chain is strung from midCh to the tail.
        RowStream leftRS = wireSubtree(node.left, prefix + "L", tasks, names, allChannels);
        RowStream rightRS = wireSubtree(node.right, prefix + "R", tasks, names, allChannels);
        final Channel midCh = channelFactory.get();
        allChannels.add(midCh);
        final RowSink midSink = channelSink(midCh);
        final RowStream l = leftRS;
        final RowStream r = rightRS;
        final BinaryStage bin = node.binary;
        tasks.add(new Task(prefix + "-bin", () -> bin.run(l, r, midSink), channelFinish(midCh)));
        names.add(prefix + "-bin");
        List<Stage> ds = node.downstream == null ? java.util.Collections.emptyList() : node.downstream;
        return wireMiddleChain(ds, namesFor(prefix + "-ds", ds.size()),
                channelStream(midCh), tasks, names, allChannels);
    }

    /**
     * Binary execution, two in and one out: the left and right sub-chains each push to their own outbound
     * channel on their own threads, the binary stage consumes both channels, each wrapped as a RowStream,
     * and pushes midCh, from which the downstream chain including the trailing output stage runs to the
     * output. This handles <b>one level</b>: a sub-chain must not contain a binary, or two breakers would
     * wait on each other, which nested DagNode flattening covers instead.
     */
    public void runBinary(List<Stage> leftStages, List<Stage> rightStages,
                          BinaryStage binary, List<Stage> downstreamStages) {
        if (downstreamStages.isEmpty()) {
            throw new IllegalArgumentException("binary downstream must end with an output stage");
        }
        List<Task> tasks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<Channel> allChannels = new ArrayList<>();

        // The left and right sub-chains are all middle stages, each with an outbound channel, and the RowStream of the last channel is returned.
        RowStream leftRS = wireMiddleChain(leftStages, namesFor("left", leftStages.size()), EMPTY_STREAM,
                tasks, names, allChannels);
        RowStream rightRS = wireMiddleChain(rightStages, namesFor("right", rightStages.size()), EMPTY_STREAM,
                tasks, names, allChannels);

        // The binary task: consumes both RowStreams and pushes midCh.
        final Channel midCh = channelFactory.get();
        allChannels.add(midCh);
        final RowSink midSink = channelSink(midCh);
        final RowStream l = leftRS;
        final RowStream r = rightRS;
        tasks.add(new Task("binary", () -> binary.run(l, r, midSink), channelFinish(midCh)));
        names.add("binary");

        // The downstream chain: downstreamStages[0..n-2] as middle stages plus the terminal last, strung from midCh to the output.
        RowStream dsUpstream = wireMiddleChain(downstreamStages.subList(0, downstreamStages.size() - 1),
                namesFor("ds", downstreamStages.size() - 1), channelStream(midCh), tasks, names, allChannels);
        int last = downstreamStages.size() - 1;
        final RowStream terminalInput = dsUpstream;
        final Stage terminalStage = downstreamStages.get(last);
        tasks.add(stageTask("ds-out", terminalStage,
                terminalInput, TERMINAL_SINK, NO_FINISH));
        names.add("ds-out");

        startJoinFailFast(tasks, names, allChannels);
    }

    /**
     * Strings a run of <b>middle</b> stages onto {@code input} as a linear chain, each stage with an
     * outbound channel and its channel finish, registering them in tasks and names, and returns the
     * RowStream of the last channel for the next segment to consume. Empty stages return the input
     * unchanged.
     */
    private RowStream wireMiddleChain(List<Stage> chain, List<String> chainNames,
                                      RowStream input, List<Task> tasks, List<String> names,
                                      List<Channel> allChannels) {
        RowStream upstream = input;
        for (int i = 0; i < chain.size(); i++) {
            final Stage stage = chain.get(i);
            final RowStream stageInput = upstream;
            final Channel out = channelFactory.get();
            allChannels.add(out);
            final RowSink sink = channelSink(out);
            tasks.add(stageTask(chainNames.get(i), stage,
                    stageInput, sink, channelFinish(out)));
            names.add(chainNames.get(i));
            upstream = channelStream(out);
        }
        return upstream;
    }

    private static Task stageTask(String name, Stage stage, RowStream input, RowSink output,
                                  Consumer<Throwable> onFinish) {
        return new Task(name, () -> stage.run(input, output), onFinish, stage::cancel);
    }

    private static List<String> namesFor(String prefix, int n) {
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(prefix + "-" + i);
        }
        return out;
    }

    /**
     * Starts every task, joins them, and fails fast. <b>Any task failure poisons every channel</b>, which
     * wakes the upstream and opposite threads parked on send with a full queue so they leave through a
     * {@link ChannelPoisonedException}; otherwise the upstream of the failed stage would park forever and
     * the caller would wait in join for good. The join polls with a timeout: each round checks for a
     * failure, poisons every channel when it finds one, then keeps joining until all threads are out.
     */
    private void startJoinFailFast(List<Task> tasks, List<String> names, List<Channel> allChannels) {
        for (Task t : tasks) {
            t.start();
        }
        boolean poisoned = false;
        Throwable canonicalPrimary = null;
        int canonicalIndex = -1;
        DeadlockWatchdog watchdog = new DeadlockWatchdog(deadlockTimeoutMillis, allChannels);
        try {
            for (Task t : tasks) {
                while (!t.isCompleted()) {
                    t.awaitCompletion(50);
                    if (!poisoned && anyFailed(tasks)) {
                        // Poison every channel, releasing all parked sends, both upstream and opposite, so they exit by throwing ChannelPoisonedException.
                        canonicalPrimary = firstFailure(tasks);
                        canonicalIndex = indexOfFailure(tasks, canonicalPrimary);
                        ChannelPoisonedException poison =
                                new ChannelPoisonedException("pipeline failing; poisoning channels");
                        poisonAll(allChannels, poison, canonicalPrimary);
                        cancelAll(tasks, poison, canonicalPrimary);
                        interruptAll(tasks);
                        poisoned = true;
                    }
                    // Deadlock watchdog: skip it while already poisoning or after a failure, otherwise detect everything parked with flat throughput.
                    if (!poisoned && watchdog.checkDeadlocked(tasks)) {
                        // Declared deadlocked: poison the channels, which wakes threads parked on poll or send, and interrupt the threads,
                        // which wakes the blocking points that are not channels, such as a latch, lock or sleep. Once every thread is out,
                        // fail fast carrying the deadlock diagnosis.
                        DeadlockException dead = new DeadlockException(watchdog.describe(tasks, names));
                        ChannelPoisonedException poison =
                                new ChannelPoisonedException("pipeline deadlocked; tearing down");
                        poisonAll(allChannels, poison, dead);
                        cancelAll(tasks, poison, dead);
                        interruptAll(tasks);
                        joinRemaining(tasks);
                        throw dead;
                    }
                }
            }
        } catch (InterruptedException ie) {
            // When the caller or StageTask owner is cancelled, the join owner must not simply return early and leave pipeline children behind.
            // Poison every channel first, then interrupt and join every task, so the owner terminal can stand for the child threads having converged.
            RuntimeException failure = new RuntimeException(
                    "pipeline interrupted while awaiting completion", ie);
            ChannelPoisonedException poison =
                    new ChannelPoisonedException("pipeline owner interrupted; tearing down");
            poisonAll(allChannels, poison, failure);
            cancelAll(tasks, poison, failure);
            interruptAll(tasks);
            boolean interruptedAgain = false;
            for (Task task : tasks) {
                for (;;) {
                    try {
                        task.awaitCompletion();
                        break;
                    } catch (InterruptedException repeated) {
                        interruptedAgain = true;
                    }
                }
            }
            Thread.currentThread().interrupt();
            for (Task task : tasks) {
                Throwable childFailure = task.failure();
                appendSuppressedDistinct(failure, childFailure);
            }
            if (interruptedAgain) {
                failure.addSuppressed(new InterruptedException("interrupted again during pipeline teardown"));
            }
            throw failure;
        }
        if (canonicalPrimary != null) {
            appendOtherTaskFailures(canonicalPrimary, tasks);
            throw new RuntimeException("pipeline stage " + canonicalIndex + " ("
                    + names.get(canonicalIndex) + ") failed", canonicalPrimary);
        }
        // Fail fast by reporting the first root failure whose cause chain does not contain ChannelPoisoned. A downstream output or sink
        // wraps an upstream ERROR in a RuntimeException, so checking only the outermost type would report collateral wrapping as the root
        // cause and hide the real Sink exception. When everything is collateral, the first is still reported rather than succeeding silently.
        Throwable connected = null;
        for (int i = 0; i < tasks.size(); i++) {
            Throwable f = tasks.get(i).failure();
            if (f == null) {
                continue;
            }
            if (containsChannelPoison(f)) {
                if (connected == null) {
                    connected = f;
                }
                continue;
            }
            throw new RuntimeException("pipeline stage " + i + " (" + names.get(i) + ") failed", f);
        }
        if (connected != null) {
            throw new RuntimeException("pipeline failed (channel poisoned)", connected);
        }
    }

    private static void cancelAll(List<Task> tasks, Throwable cause, Throwable primary) {
        for (Task task : tasks) {
            try {
                task.cancel(cause);
            } catch (Throwable cleanupFailure) {
                appendSuppressedDistinct(primary, cleanupFailure);
            }
        }
    }

    private static void interruptAll(List<Task> tasks) {
        for (Task task : tasks) {
            task.interrupt();
        }
    }

    private static boolean containsChannelPoison(Throwable failure) {
        for (Throwable cursor = failure; cursor != null; cursor = cursor.getCause()) {
            if (cursor instanceof ChannelPoisonedException) {
                return true;
            }
        }
        return false;
    }

    private static boolean anyFailed(List<Task> tasks) {
        for (Task t : tasks) {
            if (t.isCompleted() && t.failure() != null) {
                return true;
            }
        }
        return false;
    }

    private static Throwable firstFailure(List<Task> tasks) {
        for (Task task : tasks) {
            Throwable failure = task.failure();
            if (failure != null) return failure;
        }
        return null;
    }

    private static int indexOfFailure(List<Task> tasks, Throwable failure) {
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i).failure() == failure) return i;
        }
        return -1;
    }

    private static void appendOtherTaskFailures(Throwable primary, List<Task> tasks) {
        for (Task task : tasks) {
            Throwable child = task.failure();
            if (child != null && child != primary
                    && !containsThrowable(primary, child)
                    && !containsThrowable(child, primary)) {
                appendSuppressedDistinct(primary, child);
            }
        }
    }

    private static boolean containsThrowable(Throwable root, Throwable target) {
        if (root == null || target == null) return false;
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());
        java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
        pending.push(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.pop();
            if (current == target) return true;
            if (!seen.add(current)) continue;
            if (current.getCause() != null) pending.push(current.getCause());
            for (Throwable suppressed : current.getSuppressed()) pending.push(suppressed);
        }
        return false;
    }

    private static void appendSuppressedDistinct(Throwable primary, Throwable candidate) {
        if (primary != null && candidate != null && primary != candidate
                && !containsThrowable(primary, candidate)
                && !containsThrowable(candidate, primary)) {
            primary.addSuppressed(candidate);
        }
    }

    /** Poisons every channel, waking threads parked on poll or send. */
    private static void poisonAll(List<Channel> channels, ChannelPoisonedException cause,
                                  Throwable primary) {
        for (Channel ch : channels) {
            try {
                ch.fail(cause);
            } catch (Throwable cleanupFailure) {
                appendSuppressedDistinct(primary, cleanupFailure);
            }
        }
    }

    /** Blocks until the remaining tasks have all exited, closing out after the deadlock is broken: the interrupts and poisoning are already sent, so the threads should leave quickly. */
    private static void joinRemaining(List<Task> tasks) throws InterruptedException {
        for (Task t : tasks) {
            t.awaitCompletion();
        }
    }

    /**
     * Deadlock watchdog. The rule is that <b>every not-yet-finished task thread is parked, that is not
     * RUNNABLE</b>, and <b>throughput across all channels is flat</b>, sustained <b>continuously</b> for
     * more than {@code timeoutMillis}. A single snapshot is not enough, which is what keeps a momentary
     * all parked from a GC or scheduler jitter from being read as a deadlock.
     * <p>Why this does not kill a running job: a thread busy in a large sort or aggregation is RUNNABLE,
     * and spill disk I/O is RUNNABLE in native code, so neither is parked. As long as one thread is
     * running, there is no deadlock, and any throughput growth resets the timer as well. A real deadlock
     * means nothing can move and no local event can change the state.
     */
    private static final class DeadlockWatchdog {
        private final long timeoutMillis;
        private final List<Channel> channels;
        private long lastThroughput = -1;
        private long stalledSinceNanos = -1;  // Start of the all-parked, flat-throughput state; -1 means not currently in it.

        DeadlockWatchdog(long timeoutMillis, List<Channel> channels) {
            this.timeoutMillis = timeoutMillis;
            this.channels = channels;
        }

        /** True means the deadlock is confirmed, having stalled past the timeout continuously. A timeoutMillis of zero or below disables the watchdog and always returns false. */
        boolean checkDeadlocked(List<Task> tasks) {
            if (timeoutMillis <= 0) {
                return false;
            }
            long tp = totalThroughput();
            boolean progressed = tp != lastThroughput;
            lastThroughput = tp;
            if (progressed || !allParked(tasks)) {
                stalledSinceNanos = -1;  // Progress, either throughput growth or a thread running, resets the stall timer.
                return false;
            }
            // Everything parked with flat throughput: start or continue the timer, and only a sustained overrun declares deadlock.
            long now = System.nanoTime();
            if (stalledSinceNanos < 0) {
                stalledSinceNanos = now;
                return false;
            }
            return (now - stalledSinceNanos) >= timeoutMillis * 1_000_000L;
        }

        private long totalThroughput() {
            long sum = 0;
            for (Channel ch : channels) {
                sum += ch.throughput();
            }
            return sum;
        }

        private static boolean allParked(List<Task> tasks) {
            boolean anyAlive = false;
            for (Task t : tasks) {
                if (t.isCompleted()) {
                    continue;
                }
                anyAlive = true;
                if (!t.isParked()) {
                    return false;  // A live thread is still running, so this is not a deadlock.
                }
            }
            return anyAlive;  // Every live thread is parked, and at least one thread is live.
        }

        String describe(List<Task> tasks, List<String> names) {
            StringBuilder sb = new StringBuilder("pipeline deadlock detected (no progress for ")
                    .append(timeoutMillis).append("ms): all live stages parked, zero channel throughput. Stages: ");
            for (int i = 0; i < tasks.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(names.get(i)).append('=')
                        .append(tasks.get(i).isCompleted() ? "done" : "parked");
            }
            return sb.toString();
        }
    }

    /**
     * Stage completion: on success, close (emitting EOS); **on failure, emit the error event first and then
     * close**, so the downstream channelStream terminates on the error rather than seeing a bare EOS, which a
     * breaker or binary operator would otherwise read as a normal end and emit results computed from partial data.
     */
    private static Consumer<Throwable> channelFinish(Channel channel) {
        return cause -> {
            if (cause != null) {
                channel.fail(cause);  // Poisoning: drain the dirty records, post the error placeholder, and wake the upstream sends parked on the queue.
            } else {
                channel.close();
            }
        };
    }

    /** Adapts a channel into a lazily read RowStream: poll yields events and EOS becomes null. A single consumer stops polling once it has seen null. */
    private static RowStream channelStream(Channel channel) {
        return RowStream.fromChannel(channel);
    }

    /**
     * Adapts a channel into a RowSink: a RECORD sends, an ERROR or CANCEL throws to fail fast, and
     * <b>EOS and BARRIER are swallowed</b>, because the only EOS is the channel.close() from {@link Task}
     * onFinish, and control events an operator emits itself never reach the wire.
     */
    private static RowSink channelSink(Channel channel) {
        return event -> {
            if (event.isRecord()) {
                channel.send(event);
            } else if (event.isError()) {
                throw new RuntimeException("stage emitted error", event.cause());
            } else if (event.isCancel()) {
                throw new RuntimeException("stage cancelled");
            }
            // END_OF_STREAM / BARRIER: swallowed (channel.close() is the sole EOS signal)
        };
    }
}
