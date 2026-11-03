package io.rowweave.cluster;

import io.rowweave.plan.PlanGraph;

/**
 * Local deployment: a single JVM holding {@link Master} plus workers as threads, implementing
 * {@linkJobClient}. The architecture is the three roles of a real distributed cluster, with a
 * worker being athread rather than a process.
 */
public final class LocalCluster implements JobClient {

    private final Master master;
    private final int parallelism;

    public LocalCluster() {
        this(1);
    }

    /** Housekeeping of the memory manager, since the master runs its own execution memory; no developer budget is a parameter here. */
    public LocalCluster(int parallelism) {
        this.parallelism = parallelism;
        this.master = new Master(parallelism);
    }

    /** Injected small managed memory for tests, to force spilling and keep the case isolated. */
    public LocalCluster(int parallelism, io.rowweave.mem.ManagedMemory injectedPool) {
        this.parallelism = parallelism;
        this.master = new Master(parallelism, injectedPool);
    }

    @Override
    public long submitToSink(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        return master.run(sinkGraph);
    }

    /**
     * Keyed shuffle submission, shaped source to keyed unary, that is the group by family or
     * distinct, toSINK. One source fans out by key to M consumers, which converge from M to one
     * sink channel. Returns thenumber of rows written.
     */
    public long submitKeyed(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        return master.runKeyed(sinkGraph);
    }

    /**
     * Binary join over a keyed shuffle, shaped left source and right source to join to SINK.
     * Two routerspartition by numerically normalized join key, M consumers take two inbound
     * edges each, and they convergefrom M to one sink channel. Returns the number of rows
     * written.
     */
    public long submitBinaryKeyed(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        return master.runBinaryKeyed(sinkGraph);
    }
}
