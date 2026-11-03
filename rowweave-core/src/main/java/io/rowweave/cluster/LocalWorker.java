package io.rowweave.cluster;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.Partitioner;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.sched.Channel;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.List;
import java.util.Map;

/**
 * Worker for the local path: real cluster roles running as threads in one JVM. It runs one
 * {@link StagePlan} sub-plan. Each placeholder source is bound, per {@link StageBoundary}, to the
 * {@link RowStream} of an inbound shuffle channel, with no list materialised, and execution pushes
 * through {@link PushDagExecutor#executeLinearBound} to the stage's only outlet: an intermediate
 * stage sends to its outbound channel, a SINK stage writes through its Sink on this worker.
 *
 * <p>The linear case is one to one: each boundary has exactly one inbound channel, one producer and
 * one consumer. A keyed exchange is N by M.
 */
public final class LocalWorker {

    private final int parallelism;
    // The single ExecutionMemory the master owns, shared across every worker stage of this job;
    // a worker derives a view with forWorker and never closes it.
    private final io.rowweave.mem.ExecutionMemory sharedMem;

    /**
     * Runs with the ExecutionMemory supplied by the master, so all worker stages of the job draw on
     * one budget rather than each acquiring the whole pool.
     */
    public LocalWorker(int parallelism, io.rowweave.mem.ExecutionMemory sharedMem) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
        }
        this.parallelism = parallelism;
        this.sharedMem = sharedMem;
    }

    /**
     * Runs an intermediate stage: the placeholder source is bound to {@code inbound}, the channel the
     * upstream stage sends into, and the output is pushed to {@code outbound}. Executes synchronously
     * on the calling thread, which the master has wrapped in a worker thread.
     */
    public void runIntermediate(StagePlan plan, Map<Integer, Channel> inboundByUpstreamStage, Channel outbound) {
        Map<Integer, RowStream> bindings = bindSources(plan, inboundByUpstreamStage);
        PushDagExecutor.forWorker(parallelism, sharedMem).executeLinearBound(plan.plan(), bindings, channelSinkStage(outbound));
    }

    /**
     * Runs a SINK stage: the placeholder source is bound to the inbound channel and the output is
     * written by the SINK's Sink on this worker, because a Sink is written by the worker at the end of
     * execution. Returns the number of rows written, which is the control signal the master needs.
     */
    public long runSink(StagePlan plan, Map<Integer, Channel> inboundByUpstreamStage, Sink output) {
        if (!plan.isSink()) {
            throw new IllegalArgumentException("runSink requires a SINK-headed StagePlan");
        }
        Map<Integer, RowStream> bindings = bindSources(plan, inboundByUpstreamStage);
        // Compile from the SINK's child, which keeps the index space of plan.plan() so the bindings
        // line up, and avoids re-rooting the graph.
        io.rowweave.plan.PlanGraph g = plan.plan();
        int sinkChildHead = g.root().child(0);
        long[] count = {0};
        Sink counting = countingWrap(output, count);
        PushDagExecutor.forWorker(parallelism, sharedMem).executeSinkStage(g, sinkChildHead, bindings, counting);
        return count[0];
    }

    /** Binds each boundary's placeholder source to the RowStream of its upstream inbound channel. */
    private static Map<Integer, RowStream> bindSources(StagePlan plan, Map<Integer, Channel> inboundByUpstreamStage) {
        Map<Integer, RowStream> bindings = new java.util.HashMap<>();
        for (StageBoundary b : plan.boundaries()) {
            Channel ch = inboundByUpstreamStage.get(b.upstreamStage());
            if (ch == null) {
                throw new IllegalStateException("no inbound channel for upstream stage " + b.upstreamStage());
            }
            bindings.put(b.placeholderSourceIndex(), channelStream(ch));
        }
        return bindings;
    }

    /**
     * Terminal stage for an outbound channel: sends row by row, ignores EOS and barrier because
     * channel.close is the only EOS signal, and raises on error so the worker fails fast.
     */
    private static io.rowweave.sched.PipelineExecutor.Stage channelSinkStage(Channel outbound) {
        return (in, ignoredSink) -> {
            StreamEvent e;
            while ((e = in.next()) != null) {
                if (e.isRecord()) {
                    outbound.send(e);
                } else if (e.isError()) {
                    throw new RuntimeException("stage error before shuffle send", e.cause());
                }
            }
        };
    }

    /**
     * One instance of a keyed consumer. Its single placeholder source is bound directly to
     * {@code inboundChannel}, the keyed inbound channel rather than a boundary-stage mapping, and it
     * runs the keyed breaker plan and sends the result to {@code outbound}, converging M into the one
     * sink channel. The plan must be linear with a single placeholder source, identified by
     * {@code placeholderSourceIndex}.
     */
    public void runKeyedConsumer(io.rowweave.plan.PlanGraph plan, int placeholderSourceIndex,
                                 Channel inboundChannel, Channel outbound) {
        Map<Integer, RowStream> bindings = new java.util.HashMap<>();
        bindings.put(placeholderSourceIndex, channelStream(inboundChannel));
        PushDagExecutor.forWorker(parallelism, sharedMem).executeLinearBound(plan, bindings, channelSinkStage(outbound));
    }

    /**
     * A binary join consumer. The two inbound channels, left and right, are each wrapped as a
     * RowStream and fed to {@code join}, whose strategy the caller's factory chooses from HASH,
     * SORT_MERGE or AUTO. The result is sent to {@code outbound}, converging M into the one sink
     * channel.
     */
    public void runJoinConsumer(io.rowweave.sched.PipelineExecutor.BinaryStage join,
                                Channel leftIn, Channel rightIn, Channel outbound) {
        RowStream l = channelStream(leftIn);
        RowStream r = channelStream(rightIn);
        join.run(l, r, channelSinkRowSink(outbound));
    }

    /**
     * Adapts an outbound channel to a RowSink, so a binary join can be fed directly: a record is sent,
     * an error is raised to fail fast, and EOS or barrier is ignored.
     */
    private static io.rowweave.sched.stream.RowSink channelSinkRowSink(Channel outbound) {
        return event -> {
            if (event.isRecord()) {
                outbound.send(event);
            } else if (event.isError()) {
                throw new RuntimeException("join error before shuffle send", event.cause());
            }
            // EOS, barrier and cancel carry no signal here: channel.close is the only EOS.
        };
    }

    /**
     * The sink consumer. Its single placeholder source is bound to {@code inboundChannel}, the channel
     * that converges M into one, and it runs the tail chain and writes the Sink, returning the row
     * count.
     */
    public long runKeyedSink(io.rowweave.plan.PlanGraph plan, int placeholderSourceIndex,
                             Channel inboundChannel, Sink output) {
        long[] count = {0};
        Sink counting = countingWrap(output, count);
        Map<Integer, RowStream> bindings = new java.util.HashMap<>();
        bindings.put(placeholderSourceIndex, channelStream(inboundChannel));
        PushDagExecutor.forWorker(parallelism, sharedMem).executeLinearBoundToOutput(plan, bindings, counting);
        return count[0];
    }

    /**
     * A keyed routing producer: runs one source StagePlan, whose placeholder source is bound either to
     * an inbound channel or to its own Source, and sends each row to the one inbound channel of the M
     * consumers that owns its partition, so rows with the same key always reach the same partition.
     * Each target channel is sent to on its own, with no lock held across channels, because a nested
     * emitter would deadlock the ring. Once the input is drained, producerDone is called on every
     * channel.
     */
    public void runKeyedRouter(StagePlan plan, Map<Integer, Channel> inboundByUpstreamStage,
                               Partitioner partitioner, List<? extends Channel> consumerChannels) {
        Map<Integer, RowStream> bindings = bindSources(plan, inboundByUpstreamStage);
        io.rowweave.sched.PipelineExecutor.Stage routeStage = (in, ignoredSink) -> {
            StreamEvent e;
            while ((e = in.next()) != null) {
                if (e.isRecord()) {
                    DataRow r = e.row();
                    int part = partitioner.partitionOf(r);
                    consumerChannels.get(part).send(e);  // send to the target channel on its own, with no lock held across channels
                } else if (e.isError()) {
                    throw new RuntimeException("stage error before keyed shuffle", e.cause());
                }
            }
        };
        PushDagExecutor.forWorker(parallelism, sharedMem).executeLinearBound(plan.plan(), bindings, routeStage);
    }

    /**
     * A channel as a lazy RowStream, with the error latched once so a failed channel cannot spin;
     * the same semantics as PipelineExecutor.channelStream.
     */
    private static RowStream channelStream(Channel channel) {
        return RowStream.fromChannel(channel);
    }

    private static Sink countingWrap(Sink delegate, long[] count) {
        return new Sink() {
            @Override
            public DataSchema getSchema() {
                return delegate.getSchema();
            }

            @Override
            public void collect(io.rowweave.DataRow row) {
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
        };
    }
}
