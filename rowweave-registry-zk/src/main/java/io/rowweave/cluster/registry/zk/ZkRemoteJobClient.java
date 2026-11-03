package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.cluster.RegistryMasterLocator;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.QualityGateDecision;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunSubmission;
import io.rowweave.incremental.IncrementalRunSubmission;

/**
 * A remote {@link JobClient} that finds the master through ZooKeeper, so submitting a job needs
 * only a connect string and never an address written into the caller. It builds a
 * {@link ZkServiceRegistry} and a {@link RegistryMasterLocator}, which resolves whoever is leader at
 * the moment of each submit, and delegates the call to {@link ClusterJobClient}.
 *
 * <p>{@link ClusterJobClient} on its own connects to a host and port that an operator already
 * knows. Discovering the leader instead is what a deployment with several masters needs, because a
 * change of leader then stays invisible to whoever is submitting. The connection is opened in the
 * constructor, so a connect string that does not work fails there rather than on the first submit.
 *
 * <p>Injected through an execution environment:
 * <pre>{@code
 * try (ZkRemoteJobClient client = new ZkRemoteJobClient("zk1:2181,zk2:2181,zk3:2181")) {
 * ExecutionEnvironment s = new ExecutionEnvironment("job", "job", client);
 * buildDag(s);
 * s.execute();
 * } // close releases the ZooKeeper connection
 * }</pre>
 *
 * <p>Curator is a dependency of this module alone, so the engine carries none. Implementing
 * {@link AutoCloseable} is how {@link #close()} hands back the connection it opened.
 */
public final class ZkRemoteJobClient implements JobClient, AutoCloseable {

    private final ZkServiceRegistry registry;
    private final ClusterJobClient delegate;
    private final io.rowweave.deployment.DeploymentRuntime deploymentRuntime;

    /** Connects to a ZooKeeper quorum, for example {@code "zk1:2181,zk2:2181"}, and resolves the leader through the registry. */
    public ZkRemoteJobClient(String zkConnectString) {
        io.rowweave.deployment.DeploymentRuntime createdRuntime =
                io.rowweave.deployment.DeploymentRuntime.fromSystemProperties(
                        io.rowweave.cluster.rpc.ControlSecurityConfig.fromSystemProperties());
        ZkServiceRegistry createdRegistry = null;
        try {
            createdRegistry = new ZkServiceRegistry(zkConnectString);
            this.registry = createdRegistry;
            this.deploymentRuntime = createdRuntime;
            this.delegate = new ClusterJobClient(new RegistryMasterLocator(
                    registry, createdRuntime.controlSecurity().securityMetadata()), createdRuntime);
        } catch (RuntimeException | Error failure) {
            if (createdRegistry != null) createdRegistry.close();
            createdRuntime.close();
            throw failure;
        }
    }

    @Override
    public long submitToSink(PlanGraph sinkGraph) {
        return delegate.submitToSink(sinkGraph);
    }

    @Override
    public ExecutionReport submitToSinkReported(PlanGraph sinkGraph) {
        return delegate.submitToSinkReported(sinkGraph);
    }

    @Override
    public ExecutionReport submit(PlanGraph fullGraph) {
        return delegate.submit(fullGraph);
    }

    /** The artifact envelope reaches the master unchanged: inheriting the default unwrap would downgrade it to a plain submit. */
    @Override
    public ExecutionReport submit(PlanArtifact artifact) {
        return delegate.submit(artifact);
    }

    /** Durable Run submission keeps ZK leader discovery instead of exposing a fixed Master endpoint. */
    public ExecutionReport submitRun(PlanArtifact artifact, RunSubmission submission) {
        return delegate.submitRun(artifact, submission);
    }

    /** Submit a bounded incremental intent through freshly resolved leader discovery. */
    public ExecutionReport submitIncrementalRun(
            IncrementalRunSubmission submission) {
        return delegate.submitIncrementalRun(submission);
    }

    /** Query the current leader's durable Run authority through the same ZK-discovered route. */
    public RunSnapshot queryRun(String runId) {
        return delegate.queryRun(runId);
    }

    /** Query the bounded redacted incremental frontier through the current leader. */
    public io.rowweave.incremental.IncrementalFrontierView
    queryIncrementalFrontier(String feedId) {
        return delegate.queryIncrementalFrontier(feedId);
    }

    /** Query one bounded durable Run event page through the currently discovered leader. */
    public RunExecutionEventPage queryRunEvents(
            String runId, long afterSequence, int limit) {
        return delegate.queryRunEvents(runId, afterSequence, limit);
    }

    /** Submit one caller-identified durable Run cancellation to the current leader. */
    public RunCancellationResult cancelRun(String runId, String requestId) {
        return delegate.cancelRun(runId, requestId);
    }

    /** Query the connector-neutral durable Sink outcome through the current leader. */
    public BatchSinkOutcome queryBatchSinkOutcome(String runId) {
        return delegate.queryBatchSinkOutcome(runId);
    }

    /** Query the current attempt from the freshly ZK-resolved Master leader. */
    public QualityGateDecision queryQualityGateDecision(String runId) {
        return delegate.queryQualityGateDecision(runId);
    }

    /** Releases the ZooKeeper connection this client opened. */
    @Override
    public void close() {
        Throwable failure = null;
        try {
            delegate.close();
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            registry.close();
        } catch (Throwable closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        try {
            deploymentRuntime.close();
        } catch (Throwable closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) throw new RuntimeException(failure);
    }
}
