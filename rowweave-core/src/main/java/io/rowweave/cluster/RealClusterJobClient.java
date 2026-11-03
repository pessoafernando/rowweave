package io.rowweave.cluster;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.OutputDestinationValidator;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.commit.ManifestCommitOutcome;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.RunSubmissionPolicyValidator;

import java.nio.file.Paths;
import java.util.UUID;

/**
 * The {@link JobClient} of a real cluster: it submits a plan graph to the master over RPC,
 * blocks for the job result and returns the row count. No data comes back, because the sink
 * worker writes the sink locally and the client receives a control signal only.<p>This differs
 * from the in-process client, which runs a pipeline executor directly: here the client connects
 * to a real master process and speaks the control plane RPC.
 */
public final class RealClusterJobClient implements JobClient, AutoCloseable {

    /** Submit remains synchronous; aligns the ephemeral server budget and client wait. */
    private static final int SUBMIT_CONNECT_TIMEOUT_MS = 5_000;

    // Addressed through a MasterLocator: the older host and port constructor is wrapped as a FixedMasterLocator, so
    // it behaves as before, while a RegistryMasterLocator resolves the current leader at every submit, which points
    // the client at the new leader after a failover.
    private final MasterLocator locator;
    private final ObjectTrustContext objectTrustContext;
    private final io.rowweave.cluster.rpc.ControlSecurityProfile controlSecurityProfile;
    private final io.rowweave.cluster.rpc.ControlSecurityConfig controlSecurity;
    private final io.rowweave.cluster.rpc.ProtocolHelloV1 protocolHello;
    private final String deploymentSetId;
    private final io.rowweave.deployment.DeploymentRuntime deploymentRuntime;
    private final boolean ownsDeploymentRuntime;
    private final EphemeralSubmitTimeoutPolicy submitTimeoutPolicy;
    private final int ephemeralSubmitReadTimeoutMs;
    private final int controlReadTimeoutMs;

    public RealClusterJobClient(String masterHost, int masterPort) {
        this(new FixedMasterLocator(new Endpoint(masterHost, masterPort)));
    }

    public RealClusterJobClient(MasterLocator locator) {
        this(locator, io.rowweave.deployment.DeploymentRuntime.fromSystemProperties(
                io.rowweave.cluster.rpc.ControlSecurityConfig.fromSystemProperties()), true);
    }

    public RealClusterJobClient(
            MasterLocator locator,
            io.rowweave.deployment.DeploymentRuntime deploymentRuntime) {
        this(locator, deploymentRuntime, false);
    }

    private RealClusterJobClient(
            MasterLocator locator,
            io.rowweave.deployment.DeploymentRuntime deploymentRuntime,
            boolean ownsDeploymentRuntime) {
        this.locator = locator;
        this.deploymentRuntime = java.util.Objects.requireNonNull(
                deploymentRuntime, "deploymentRuntime");
        this.ownsDeploymentRuntime = ownsDeploymentRuntime;
        this.submitTimeoutPolicy =
                EphemeralSubmitTimeoutPolicy.clientFromSystemProperties();
        this.ephemeralSubmitReadTimeoutMs =
                submitTimeoutPolicy.clientReadTimeoutMillis();
        this.controlReadTimeoutMs =
                (int) submitTimeoutPolicy.budget().executionTimeoutMs();
        deploymentRuntime.requireAdmissionOpen();
        io.rowweave.cluster.rpc.ControlSecurityConfig security =
                deploymentRuntime.controlSecurity();
        security.requireImplementedTransport();
        this.controlSecurity = security;
        this.protocolHello = deploymentRuntime.protocolHello();
        this.controlSecurityProfile = security.profile();
        this.deploymentSetId = deploymentRuntime.deploymentSetId();
        this.objectTrustContext = deploymentRuntime.objectTrustContext();
    }

    @Override
    public long submitToSink(PlanGraph sinkGraph) {
        return submitAttempt(sinkGraph, UUID.randomUUID().toString()).rowsOut();
    }

    /** Preserve the full cluster report on the legacy single-sink reported route. */
    @Override
    public io.rowweave.plan.ExecutionReport submitToSinkReported(PlanGraph sinkGraph) {
        return submitAttempt(sinkGraph, UUID.randomUUID().toString());
    }

    /**
     * Submission with an explicit stable attempt id; a retry by the caller must reuse the same
     * id.
     */
    public io.rowweave.plan.ExecutionReport submitAttempt(PlanGraph sinkGraph, String attemptId) {
        io.rowweave.artifact.IncrementalRuntimeGate.rejectGenericExecution(sinkGraph);
        if (controlSecurityProfile
                == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS) {
            throw new SecurityException(
                    "MTLS requires PlanArtifact submission; raw SUBMIT_JOB is forbidden");
        }
        OutputDestinationValidator.validate(sinkGraph);  // rejects a duplicated exclusive destination before the master lookup or any RPC
        ClusterOutputValidator.validate(sinkGraph);
        OutputCommitValidator.validate(sinkGraph);
        ClusterPlanCapabilityValidator.validate(sinkGraph);
        requireAttemptId(attemptId);
        ControlResidentLease lease = ControlResidentBudget.processWide().openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            deploymentRuntime.requireAdmissionOpen();
            byte[] planBytes = sinkGraph.toBytes(
                    objectTrustContext, bytes -> lease.reserve(owner, bytes));
            Endpoint leader = locator.locateLeader();
            try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                         connectMaster(leader, ephemeralSubmitReadTimeoutMs)) {
                SubmitOutcome outcome = toMaster.callScoped(RpcMessage.submitJob(
                        planBytes, attemptId, submitTimeoutPolicy.budget()),
                        RealClusterJobClient::consumeSubmitOutcome);
                return outcome.requireReport();
            }
        } finally {
            lease.release(owner);
        }
    }

    /**
     * A real cluster must deliver the artifact envelope to the master, and may not degrade here
     * to the older submit.
     */
    @Override
    public io.rowweave.plan.ExecutionReport submit(PlanArtifact artifact) {
        return submitArtifactAttempt(artifact, UUID.randomUUID().toString());
    }

    /**
     * Explicit publication identity; a retry by the caller must reuse the same id.
     */
    public io.rowweave.plan.ExecutionReport submitArtifactAttempt(
            PlanArtifact artifact, String publicationId) {
        ControlResidentLease lease = ControlResidentBudget.processWide().openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            artifact = bindAndValidateDeployment(artifact);
            if (artifact.payloadFormat()
                    == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
                PortablePlanArtifact.preflightValidated(
                        artifact, deploymentRuntime.portableBindingVerifier(),
                        bytes -> lease.reserve(owner, bytes));
            } else {
                PlanArtifactValidator.validateForCurrentRuntime(artifact);
                PlanGraph graph = LegacyPlanArtifactAdapter.unwrapValidated(
                        artifact, objectTrustContext,
                        bytes -> lease.reserve(owner, bytes));
                OutputDestinationValidator.validate(graph);
                ClusterOutputValidator.validate(graph);
                OutputCommitValidator.validate(graph);
                ClusterPlanCapabilityValidator.validate(graph);
            }
            requireAttemptId(publicationId);
            byte[] artifactBytes = PlanArtifactCodec.encode(
                    artifact, bytes -> lease.reserve(owner, bytes));
            Endpoint leader = locator.locateLeader();
            try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                         connectMaster(leader, ephemeralSubmitReadTimeoutMs)) {
                SubmitOutcome outcome = toMaster.callScoped(RpcMessage.submitPlanArtifact(
                                artifactBytes, publicationId, submitTimeoutPolicy.budget()),
                        RealClusterJobClient::consumeSubmitOutcome);
                return outcome.requireReport();
            }
        } finally {
            lease.release(owner);
        }
    }

    /**
     * Durable submission with explicit stable run, attempt and publication identities.
     */
    public io.rowweave.plan.ExecutionReport submitRun(
            PlanArtifact artifact, RunSubmission submission) {
        if (submission == null) {
            throw new IllegalArgumentException("run submission is required");
        }
        ControlResidentLease lease = ControlResidentBudget.processWide().openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            return submitRunWithLease(artifact, submission, lease, owner);
        } finally {
            lease.release(owner);
        }
    }

    /** submit one authority-free bounded incremental trigger intent. */
    public io.rowweave.plan.ExecutionReport submitIncrementalRun(
            io.rowweave.incremental.IncrementalRunSubmission submission) {
        if (submission == null) {
            throw new IllegalArgumentException(
                    "incremental submission is required");
        }
        ControlResidentLease lease = ControlResidentBudget.processWide().openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            submission = bindAndValidateIncrementalSubmission(
                    submission, lease, owner);
            byte[] envelope = io.rowweave.incremental
                    .IncrementalRunSubmissionCodec.encode(
                            submission, bytes -> lease.reserve(owner, bytes));
            Endpoint leader = locator.locateLeader();
            try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                         connectMaster(leader, controlReadTimeoutMs)) {
                RpcMessage result = toMaster.call(
                        RpcMessage.submitIncrementalRun(envelope));
                if (result.type() == RpcMessage.Type.JOB_RESULT
                        && result.success()) {
                    return executionReport(result);
                }
                throw new RuntimeException("incremental cluster run failed: "
                        + (result.errorMessage() == null
                        ? "unknown" : result.errorMessage()));
            }
        } finally {
            lease.release(owner);
        }
    }

    /** idempotently bootstrap one feed before its first trigger. */
    public io.rowweave.incremental.IncrementalFrontierView
    initializeIncrementalFeed(
            String commandId, long feedIncarnation,
            io.rowweave.incremental.IncrementalRunSubmission submission) {
        if (submission == null) {
            throw new IllegalArgumentException(
                    "incremental submission is required");
        }
        ControlResidentLease lease = ControlResidentBudget.processWide().openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            submission = bindAndValidateIncrementalSubmission(
                    submission, lease, owner);
            io.rowweave.incremental.IncrementalInitializeRequest initialize =
                    new io.rowweave.incremental.IncrementalInitializeRequest(
                            submission.feedId(), feedIncarnation,
                            submission.definition().fingerprint(),
                            io.rowweave.incremental
                                    .IncrementalSourceProviderContractCodec.digest(
                                            submission.sourceContract()),
                            submission.definition().planDescriptor()
                                    .frontierContractDigest(),
                            submission.sourceContract().frontierKind());
            lease.reserve(owner, io.rowweave.incremental
                    .IncrementalInitializeRequestCodec.MAX_FRAME_BYTES);
            byte[] encoded = io.rowweave.incremental
                    .IncrementalInitializeRequestCodec.encode(initialize);
            RpcMessage request = RpcMessage.initializeIncrementalFeed(
                    submission.feedId(), commandId, encoded);
            Endpoint leader = locator.locateLeader();
            try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                         connectMaster(leader, controlReadTimeoutMs)) {
                final String feedId = submission.feedId();
                return toMaster.callScoped(request, response -> {
                    if (response.type()
                            != RpcMessage.Type.INCREMENTAL_FRONTIER_VIEW) {
                        throw new RuntimeException(
                                "feed initialization failed: unexpected response "
                                        + response.type());
                    }
                    if (!feedId.equals(response.runId())) {
                        throw new RuntimeException(
                                "feed initialization identity mismatch");
                    }
                    if (!response.success()) {
                        throw new RuntimeException(
                                "feed initialization failed: "
                                        + (response.errorMessage() == null
                                        ? "unknown" : response.errorMessage()));
                    }
                    io.rowweave.incremental.IncrementalFrontierView view =
                            io.rowweave.incremental.IncrementalFrontierViewCodec
                                    .decode(response.incrementalFrontierViewBytes());
                    if (!feedId.equals(view.feedId())
                            || feedIncarnation != view.feedIncarnation()) {
                        throw new RuntimeException(
                                "feed initialization payload identity mismatch");
                    }
                    return view;
                });
            }
        } finally {
            lease.release(owner);
        }
    }

    private io.rowweave.incremental.IncrementalRunSubmission
    bindAndValidateIncrementalSubmission(
            io.rowweave.incremental.IncrementalRunSubmission submission,
            ControlResidentLease lease,
            ControlResidentLease.OwnerToken owner) {
        PlanArtifact bound = bindAndValidateDeployment(
                submission.templateArtifact());
        submission = submission.withTemplateArtifact(bound);
        PlanArtifactValidator.validateForCurrentRuntime(bound);
        PlanGraph template = LegacyPlanArtifactAdapter
                .unwrapIncrementalValidated(
                        bound, objectTrustContext,
                        bytes -> lease.reserve(owner, bytes));
        io.rowweave.incremental.IncrementalDefinition derived =
                io.rowweave.incremental.IncrementalPlanTemplateFingerprint
                        .derive(submission.definition().definitionId(),
                                template, objectTrustContext,
                                bytes -> lease.reserve(owner, bytes));
        if (!derived.equals(submission.definition())) {
            throw new IllegalArgumentException(
                    "incremental definition differs from template");
        }
        io.rowweave.incremental.IncrementalSourceProviderContract admitted =
                deploymentRuntime.incrementalSourceProviderRegistry()
                        .requireContract(
                                submission.sourceContract().providerKey());
        if (!admitted.equals(submission.sourceContract())) {
            throw new IllegalArgumentException(
                    "incremental source contract differs from signed deployment");
        }
        OutputDestinationValidator.validate(template);
        ClusterOutputValidator.validate(template);
        OutputCommitValidator.validateDurable(template);
        ClusterPlanCapabilityValidator.validate(template);
        return submission;
    }

    /**
     * driver entry: selects the durable compiler while the complete
     * PlanGraph is still available and before ordinary artifact wrapping.
     */
    public io.rowweave.plan.ExecutionReport submitRun(
            PlanGraph graph, String definitionId, RunSubmission submission) {
        if (submission == null) {
            throw new IllegalArgumentException("run submission is required");
        }
        ControlResidentLease lease = ControlResidentBudget.processWide().openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            RunSubmissionPolicyValidator.requireSupported(submission);
            deploymentRuntime.requireAdmissionOpen();
            io.rowweave.plan.DurableSinkProviderPlanPreflight.validateClientGraph(
                    graph, deploymentRuntime.durableSinkProviderRegistry());
            PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                    definitionId, graph, objectTrustContext,
                    bytes -> lease.reserve(owner, bytes));
            return submitRunWithLease(artifact, submission, lease, owner);
        } finally {
            lease.release(owner);
        }
    }

    private io.rowweave.plan.ExecutionReport submitRunWithLease(
            PlanArtifact artifact, RunSubmission submission,
            ControlResidentLease lease, ControlResidentLease.OwnerToken owner) {
        RunSubmissionPolicyValidator.requireSupported(submission);
        artifact = bindAndValidateDeployment(artifact);
        if (artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            PortablePlanArtifact.preflightDurableValidated(
                    artifact, deploymentRuntime.portableBindingVerifier(),
                    bytes -> lease.reserve(owner, bytes));
        } else {
            PlanArtifactValidator.validateForCurrentRuntime(artifact);
            io.rowweave.plan.PlanGraphEnvelopeScanner.Scan providerScan =
                    io.rowweave.plan.PlanGraphEnvelopeScanner.scanDurable(
                            artifact.payload(), bytes -> lease.reserve(owner, bytes));
            io.rowweave.plan.DurableSinkProviderPlanPreflight.validate(
                    providerScan, deploymentRuntime.durableSinkProviderRegistry());
            PlanGraph graph = LegacyPlanArtifactAdapter.unwrapValidated(
                    artifact, objectTrustContext, bytes -> lease.reserve(owner, bytes));
            OutputDestinationValidator.validate(graph);
            ClusterOutputValidator.validate(graph);
            OutputCommitValidator.validateDurable(graph);
            ClusterPlanCapabilityValidator.validate(graph);
        }
        byte[] artifactBytes = PlanArtifactCodec.encode(
                artifact, bytes -> lease.reserve(owner, bytes));
        byte[] envelope = RunSubmissionCodec.encode(
                submission, artifactBytes, bytes -> lease.reserve(owner, bytes));
        try {
            Endpoint leader = locator.locateLeader();
            RpcMessage result;
            try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                         connectMaster(leader, controlReadTimeoutMs)) {
                result = toMaster.call(RpcMessage.submitRun(envelope));
            }
            if (result.type() == RpcMessage.Type.JOB_RESULT && result.success()) {
                return executionReport(result);
            }
            if (result.type() == RpcMessage.Type.JOB_RESULT
                    && result.durableRunDisposition()
                    == io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE) {
                throw new DurableRunSubmitException(
                        submission.runId(), DurableRunSubmitException.Category.REJECTED,
                        null, durableFailureMessage(result), null);
            }
            return reconcileSubmitOutcome(
                    submission.runId(), durableFailureMessage(result), null);
        } catch (DurableRunSubmitException classified) {
            throw classified;
        } catch (RuntimeException transportFailure) {
            return reconcileSubmitOutcome(
                    submission.runId(), "durable submit transport outcome is unknown",
                    transportFailure);
        }
    }

    private io.rowweave.plan.ExecutionReport reconcileSubmitOutcome(
            String runId, String detail, Throwable submitFailure) {
        final RunQueryEnvelope query;
        try {
            query = queryRunEnvelope(runId);
        } catch (RuntimeException queryFailure) {
            if (submitFailure != null) queryFailure.addSuppressed(submitFailure);
            throw new DurableRunSubmitException(
                    runId, DurableRunSubmitException.Category.UNKNOWN, null,
                    detail + "; authoritative query unavailable", queryFailure);
        }
        RunSnapshot snapshot = query == null ? null : query.snapshot;
        if (snapshot != null && snapshot.status() == io.rowweave.run.RunStatus.SUCCEEDED
                && snapshot.resultSummary() != null) {
            return query.telemetry.toExecutionReport(
                    snapshot.resultSummary().rowsOut());
        }
        if (snapshot != null && (snapshot.status() == io.rowweave.run.RunStatus.FAILED
                || snapshot.status() == io.rowweave.run.RunStatus.CANCELED)) {
            throw new DurableRunSubmitException(
                    runId, DurableRunSubmitException.Category.TERMINAL_FAILED,
                    snapshot, detail, submitFailure);
        }
        throw new DurableRunSubmitException(
                runId, DurableRunSubmitException.Category.UNKNOWN,
                snapshot, detail, submitFailure);
    }

    static io.rowweave.plan.ExecutionReport executionReport(RpcMessage result) {
        long rowsOut = result == null ? 0L : result.rowCount();
        try {
            if (result == null || result.type() != RpcMessage.Type.JOB_RESULT) {
                throw new IllegalArgumentException("JOB_RESULT is required");
            }
            return result.executionTelemetrySummary().toExecutionReport(rowsOut);
        } catch (RuntimeException | LinkageError unavailableTelemetry) {
            return io.rowweave.plan.ExecutionTelemetrySummary.unavailable()
                    .toExecutionReport(rowsOut);
        }
    }

    private static SubmitOutcome consumeSubmitOutcome(
            RpcMessage result) {
        if (result != null && result.type() == RpcMessage.Type.JOB_RESULT
                && result.success()) {
            return new SubmitOutcome(executionReport(result), null);
        }
        return new SubmitOutcome(null,
                result != null && result.errorMessage() != null
                        ? result.errorMessage() : "unknown");
    }

    private static final class SubmitOutcome {
        final io.rowweave.plan.ExecutionReport report;
        final String error;

        SubmitOutcome(io.rowweave.plan.ExecutionReport report, String error) {
            this.report = report;
            this.error = error;
        }

        io.rowweave.plan.ExecutionReport requireReport() {
            if (report != null) return report;
            throw new RuntimeException("cluster job failed: " + error);
        }
    }

    private static String durableFailureMessage(RpcMessage result) {
        if (result == null) return "cluster run returned no result";
        return "cluster run failed: "
                + (result.errorMessage() == null ? "unknown" : result.errorMessage());
    }

    private PlanArtifact bindAndValidateDeployment(PlanArtifact artifact) {
        deploymentRuntime.requireAdmissionOpen();
        boolean portable = artifact != null && artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1;
        if (portable || controlSecurityProfile
                == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS) {
            artifact = PlanArtifactDeploymentBinding.bind(artifact, deploymentSetId);
        }
        PlanArtifactDeploymentBinding.requireMatches(artifact, deploymentSetId,
                portable || controlSecurityProfile
                        == io.rowweave.cluster.rpc.ControlSecurityProfile.MTLS);
        return artifact;
    }

    @Override
    public void close() {
        if (ownsDeploymentRuntime) deploymentRuntime.close();
    }

    /**
     * Authoritative query; null means provably absent, while a connection, store or decode
     * failure is thrown.
     */
    public RunSnapshot queryRun(String runId) {
        RunQueryEnvelope query = queryRunEnvelope(runId);
        return query == null ? null : query.snapshot;
    }

    /** Query one bounded redacted incremental frontier view from the current leader. */
    public io.rowweave.incremental.IncrementalFrontierView
    queryIncrementalFrontier(String feedId) {
        RpcMessage request = RpcMessage.queryIncrementalFrontier(feedId);
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(request, response -> {
                if (response.type()
                        != RpcMessage.Type.INCREMENTAL_FRONTIER_VIEW) {
                    throw new RuntimeException(
                            "frontier query failed: unexpected response "
                                    + response.type());
                }
                if (!feedId.equals(response.runId())) {
                    throw new RuntimeException("frontier query identity mismatch");
                }
                if (!response.success()) {
                    if (response.errorMessage() == null) return null;
                    throw new RuntimeException(
                            "frontier query failed: " + response.errorMessage());
                }
                io.rowweave.incremental.IncrementalFrontierView view =
                        io.rowweave.incremental.IncrementalFrontierViewCodec.decode(
                                response.incrementalFrontierViewBytes());
                if (!feedId.equals(view.feedId())) {
                    throw new RuntimeException(
                            "frontier view payload identity mismatch");
                }
                return view;
            });
        }
    }

    private RunQueryEnvelope queryRunEnvelope(String runId) {
        RpcMessage request = RpcMessage.queryRun(runId);  // identity validation happens while building this
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(request, response -> {
            if (response.type() != RpcMessage.Type.RUN_SNAPSHOT) {
                throw new RuntimeException("run query failed: unexpected response " + response.type());
            }
            if (!response.success()) {
                if (response.errorMessage() == null) return null;
                throw new RuntimeException("run query failed: " + response.errorMessage());
            }
            RunSnapshot snapshot = RunStateCodec.decodeSnapshot(response.planGraphBytes());
            if (!runId.equals(snapshot.runId())) {
                throw new RuntimeException("run query identity mismatch");
            }
            io.rowweave.plan.ExecutionTelemetrySummary telemetry;
            try {
                telemetry = response.executionTelemetrySummary();
            } catch (RuntimeException | LinkageError unavailableTelemetry) {
                telemetry = io.rowweave.plan.ExecutionTelemetrySummary.unavailable();
            }
            return new RunQueryEnvelope(snapshot, telemetry);
            });
        }
    }

    private static final class RunQueryEnvelope {
        final RunSnapshot snapshot;
        final io.rowweave.plan.ExecutionTelemetrySummary telemetry;

        RunQueryEnvelope(
                RunSnapshot snapshot,
                io.rowweave.plan.ExecutionTelemetrySummary telemetry) {
            this.snapshot = snapshot;
            this.telemetry = telemetry;
        }
    }

    /**
     * One page of durable run mutation events, after an exclusive cursor.
     */
    public RunExecutionEventPage queryRunEvents(
            String runId, long afterSequence, int limit) {
        RpcMessage request = RpcMessage.queryRunEvents(
                runId, afterSequence, limit);  // arguments are validated before the leader lookup
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(request, response -> {
                if (response.type() != RpcMessage.Type.RUN_EVENTS_PAGE) {
                    throw new RuntimeException(
                            "run event query failed: unexpected response " + response.type());
                }
                if (!runId.equals(response.runId())) {
                    throw new RuntimeException("run event query identity mismatch");
                }
                if (!response.success()) {
                    throw new RuntimeException("run event query failed: "
                            + (response.errorMessage() == null
                            ? "unknown" : response.errorMessage()));
                }
                final RunExecutionEventPage page;
                try {
                    page = RunExecutionEventPageCodec.decode(
                            response.runEventPageBytes(), response::reserveResidentBytes);
                } catch (RuntimeException malformed) {
                    throw new RuntimeException(
                            "run event query returned an invalid page", malformed);
                }
                if (page.afterSequence() != afterSequence
                        || page.events().size() > limit) {
                    throw new RuntimeException("run event query page does not match request");
                }
                return page;
            });
        }
    }

    /**
     * Cancellation of one durable run by stable request id, exposed publicly once.
     */
    public RunCancellationResult cancelRun(String runId, String requestId) {
        RpcMessage request = RpcMessage.cancelRun(
                runId, requestId);  // identity validation happens before the leader lookup
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(request, response -> {
                if (response.type() != RpcMessage.Type.RUN_CANCELLATION_RESULT) {
                    throw new RuntimeException(
                            "run cancellation failed: unexpected response " + response.type());
                }
                if (!runId.equals(response.runId())) {
                    throw new RuntimeException("run cancellation identity mismatch");
                }
                if (!response.success()) {
                    throw new RuntimeException("run cancellation failed: "
                            + (response.errorMessage() == null
                            ? "unknown" : response.errorMessage()));
                }
                try {
                    RunCancellationResult.Disposition disposition =
                            RunCancellationResult.Disposition.valueOf(
                                    response.runCancellationDisposition());
                    RunStatus status = response.runCancellationStatus() == null ? null
                            : RunStatus.valueOf(response.runCancellationStatus());
                    return new RunCancellationResult(runId, disposition, status,
                            response.runCancellationRevision());
                } catch (RuntimeException malformed) {
                    throw new RuntimeException(
                            "run cancellation returned an invalid authority result", malformed);
                }
            });
        }
    }

    /** Connector-neutral durable Sink outcome query. Communication failures never become NOT_FOUND. */
    public BatchSinkOutcome queryBatchSinkOutcome(String runId) {
        RpcMessage request = RpcMessage.queryBatchSinkOutcome(runId);
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(request, response -> {
                if (response.type() != RpcMessage.Type.BATCH_SINK_OUTCOME) {
                    throw new RuntimeException(
                            "batch sink outcome query failed: unexpected response "
                                    + response.type());
                }
                if (!runId.equals(response.runId())) {
                    throw new RuntimeException("batch sink outcome query identity mismatch");
                }
                final BatchSinkOutcome.State state;
                try {
                    state = BatchSinkOutcome.State.valueOf(response.commitOutcome());
                } catch (RuntimeException malformed) {
                    throw new RuntimeException(
                            "batch sink outcome query returned invalid state", malformed);
                }
                long encodedGeneration = response.commitGeneration();
                if (encodedGeneration < 0L) {
                    throw new RuntimeException(
                            "batch sink outcome query returned invalid generation");
                }
                Long generation = encodedGeneration > 0L
                        ? encodedGeneration : null;
                return BatchSinkOutcome.of(state, generation,
                        response.commitPayload(), response.errorMessage());
            });
        }
    }

    /** Query the complete decision for the durable Run's current attempt. */
    public QualityGateDecision queryQualityGateDecision(String runId) {
        RpcMessage request = RpcMessage.queryQualityGateDecision(runId);
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(request, response -> {
                if (response.type() != RpcMessage.Type.QUALITY_GATE_DECISION) {
                    throw new RuntimeException(
                            "quality decision query failed: unexpected response "
                                    + response.type());
                }
                if (!runId.equals(response.runId())) {
                    throw new RuntimeException(
                            "quality decision query identity mismatch");
                }
                if (!response.success()) {
                    if (response.errorMessage() == null) return null;
                    throw new RuntimeException(
                            "quality decision query failed: "
                                    + response.errorMessage());
                }
                try {
                    QualityGateDecision decision = QualityGateDecisionCodec.decode(
                            response.qualityGateDecisionBytes());
                    if (!runId.equals(decision.fence().runId())) {
                        throw new IllegalArgumentException(
                                "decision Run identity mismatch");
                    }
                    return decision;
                } catch (RuntimeException malformed) {
                    throw new RuntimeException(
                            "quality decision query returned invalid decision",
                            malformed);
                }
            });
        }
    }

    /**
     * Read-only query: a communication failure is never reported as absent.
     */
    public ManifestCommitOutcome queryManifestOutcome(String domainPath, String attemptId) {
        requireAttemptId(attemptId);
        ManifestFileStore store = new ManifestFileStore(Paths.get(domainPath));
        Endpoint leader = locator.locateLeader();
        try (io.rowweave.cluster.rpc.RpcTransport toMaster =
                     connectMaster(leader, controlReadTimeoutMs)) {
            return toMaster.callScoped(RpcMessage.queryManifestOutcome(
                    store.domainPath().toString(), store.domainId(), attemptId), result -> {
            if (result.type() != RpcMessage.Type.MANIFEST_OUTCOME) {
                throw new RuntimeException("manifest outcome query failed: "
                        + (result.errorMessage() == null ? "unexpected response" : result.errorMessage()));
            }
            ManifestCommitOutcome.State state;
            try {
                state = ManifestCommitOutcome.State.valueOf(result.commitOutcome());
            } catch (RuntimeException malformed) {
                throw new RuntimeException("manifest outcome query returned invalid state", malformed);
            }
            Long generation = result.commitGeneration() > 0L ? result.commitGeneration() : null;
            return ManifestCommitOutcome.of(
                    state, generation, result.commitPayload(), result.errorMessage());
            });
        }
    }

    private static void requireAttemptId(String attemptId) {
        if (attemptId == null || attemptId.isEmpty() || attemptId.indexOf('\0') >= 0
                || attemptId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException("invalid manifest attemptId");
        }
    }

    private io.rowweave.cluster.rpc.RpcTransport connectMaster(
            Endpoint leader, int readTimeoutMillis) {
        return io.rowweave.cluster.rpc.RpcTransports.connect(
                leader.host(), leader.port(), SUBMIT_CONNECT_TIMEOUT_MS,
                readTimeoutMillis, controlSecurity, protocolHello);
    }

    /**
     * A real cluster submits the whole graph in a single call: one execution of an environment
     * is one complete plan graph, and the client neither inspects its shape nor splits it per
     * sink. Stage orchestration, materializing a fan-out and converging on failure all happen
     * on the master alone, which builds the execution graph over the complete plan, including a
     * re-join that needs blocking subpartitions. This matches the local mode, where the whole
     * graph is the only submission boundary.
     */
    @Override
    public io.rowweave.plan.ExecutionReport submit(PlanGraph fullGraph) {
        return submitAttempt(fullGraph, UUID.randomUUID().toString());
    }
}
