package io.rowweave.local;

import io.rowweave.cluster.JobClient;
import io.rowweave.BatchRuntimes;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.deployment.PortableRuntimeBindingRegistry;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.OutputDestinationValidator;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.PushDagExecutor;

/**
 * Legacy embedded JobClient: runs a SINK-rooted graph directly through {@link PushDagExecutor#runSinkGraph}.
 *
 * <p>This is no longer the default path of {@code BatchSession.execute()} and takes no new execution capability. It is
 * kept only for explicit embedded use, diagnostics and emergency fallback; default local execution uses the shared Master/Worker runtime that opens no listening port.
 *
 * @deprecated Use the default unified local runtime from {@code RowWeave.createBatchSession(...)}; construct this class explicitly only when the embedded executor is specifically required.
 */
@Deprecated
public final class LocalJobClient implements JobClient {

    private final boolean canonicalizeOutput; // Canonicalizes the tail of a parallel shuffle.
    private final PortableRuntimeBindingRegistry portableBindings;

    public LocalJobClient() {
        this(false);
    }

    /**
     * Parallelism is a job property read from the submitted graph, its single source, and is no longer held by the client. The
     * parallelism argument here is ignored, and the signature is kept only so that existing calls still compile (the value is already compiled into the graph by {@code BatchSession.toPlanGraph()}).
     * @deprecated Use the no-argument constructor; parallelism enters the graph through {@link BatchSession#setParallelism}.
     */
    @Deprecated
    public LocalJobClient(int parallelism) {
        this(false);
    }

    /** @deprecated The parallelism argument is ignored (the value comes from the graph); canonicalizeOutput still applies. */
    @Deprecated
    public LocalJobClient(int parallelism, boolean canonicalizeOutput) {
        this(canonicalizeOutput);
    }

    /** Parallelism comes from the graph; this constructor configures only canonicalizeOutput. */
    public LocalJobClient(boolean canonicalizeOutput) {
        this(canonicalizeOutput, null);
    }

    /** Public-v1 local runtime over one exact signed Worker deployment set. */
    public LocalJobClient(PortableRuntimeBindingRegistry portableBindings) {
        this(false, java.util.Objects.requireNonNull(
                portableBindings, "portableBindings"));
    }

    public LocalJobClient(
            boolean canonicalizeOutput,
            PortableRuntimeBindingRegistry portableBindings) {
        this.canonicalizeOutput = canonicalizeOutput;
        this.portableBindings = portableBindings;
    }

    @Override
    public long submitToSink(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        requireProviderSupervisorAvailable(sinkGraph);
        if (containsMapPartition(sinkGraph)) {
            return BatchRuntimes.processWideInProcess()
                    .submitToSink(sinkGraph);
        }
        // Parallelism comes from the graph, its single source, so the PushDagExecutor signature is unchanged.
        return new PushDagExecutor(sinkGraph.parallelism()).canonicalizeOutput(canonicalizeOutput).runSinkGraph(sinkGraph);
    }

    /**
     * Submits the whole graph in one call, so a subgraph shared by several sinks is computed once and broadcast through a fan-out tee.
     * A single sink is delegated inside runWholeGraph to runSinkGraphReported. Returns the ExecutionReport for the whole job, with rowsOut summed across every output.
     */
    @Override
    public io.rowweave.plan.ExecutionReport submit(PlanGraph fullGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(fullGraph);
        requireProviderSupervisorAvailable(fullGraph);
        PlanOps.validateExecutableTypes(fullGraph);
        OutputDestinationValidator.validate(fullGraph); // Defense line for a direct client call.
        OutputCommitValidator.validate(fullGraph);
        if (containsMapPartition(fullGraph)) {
            return BatchRuntimes.processWideInProcess().submit(fullGraph);
        }
        return new PushDagExecutor(fullGraph.parallelism()).canonicalizeOutput(canonicalizeOutput).runWholeGraph(fullGraph);
    }

    @Override
    public io.rowweave.plan.ExecutionReport submit(PlanArtifact artifact) {
        if (artifact != null && artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            if (portableBindings == null) {
                throw new IllegalStateException("PORTABLE_RUNTIME_REQUIRED");
            }
            PlanArtifact bound = PlanArtifactDeploymentBinding.bind(
                    artifact, portableBindings.deploymentSetId());
            ControlResidentLease lease = ControlResidentBudget.processWide()
                    .openLease();
            ControlResidentLease.OwnerToken owner = lease.ownerToken();
            try {
                PlanGraph graph = PortablePlanArtifact.compileValidated(
                        bound, portableBindings, portableBindings,
                        bytes -> lease.reserve(owner, bytes));
                return submit(graph);
            } finally {
                lease.release(owner);
            }
        }
        return JobClient.super.submit(artifact);
    }

    /** Single-machine exit that reports metrics: runSinkGraphReported returns the full ExecutionReport (peak, spill, skew, rowsOut). */
    @Override
    public io.rowweave.plan.ExecutionReport submitToSinkReported(PlanGraph sinkGraph) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        requireProviderSupervisorAvailable(sinkGraph);
        if (containsMapPartition(sinkGraph)) {
            return BatchRuntimes.processWideInProcess()
                    .submitToSinkReported(sinkGraph);
        }
        return new PushDagExecutor(sinkGraph.parallelism()).canonicalizeOutput(canonicalizeOutput).runSinkGraphReported(sinkGraph);
    }

    private static boolean containsMapPartition(PlanGraph graph) {
        for (int index = 0; index < graph.nodeCount(); index++) {
            if ("mapPartition".equals(graph.node(index).typeId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * a poisoned process-wide provider lane is permanent for the
     * containing JVM. Creating another local client/session must not route a
     * durable request through the legacy in-process output path.
     */
    private static void requireProviderSupervisorAvailable(PlanGraph graph) {
        boolean durableProvider = false;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() != io.rowweave.plan.PlanNode.Kind.SINK) {
                continue;
            }
            io.rowweave.Sink output = graph.node(i).output();
            if (output != null && (output.getClass()
                    == io.rowweave.ManifestFileSink.class
                    || output.getClass()
                    == io.rowweave.ProviderBatchSink.class)) {
                durableProvider = true;
                break;
            }
        }
        if (!durableProvider) return;
        int requestedCapacity = Integer.getInteger(
                "rowweave.provider.action.parallelism",
                Math.min(Math.max(1, graph.parallelism()),
                        io.rowweave.internal.ProviderActionSupervisor
                                .MAX_CAPACITY));
        if (io.rowweave.internal.ProviderActionSupervisor
                .processWide(requestedCapacity).isPoisoned()) {
            throw new IllegalStateException(
                    "PROVIDER_ACTION_SUPERVISOR_POISONED");
        }
    }
}
