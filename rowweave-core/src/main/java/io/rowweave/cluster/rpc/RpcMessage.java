package io.rowweave.cluster.rpc;

import io.rowweave.cluster.Endpoint;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationAuthorityKernel;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.plan.ExecutionTelemetrySummaryCodec;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryCodec;
import io.rowweave.artifact.PlanArtifact;

import java.util.Arrays;

/**
 * Message of the control plane RPC, encoded with an explicit length prefix and per-field codecs
 * (see {@link RpcCodec}) rather than a bare ObjectInputStream. The control plane is not the hot
 * path and the message set is small, so hand-written codecs keep the deserialization attack
 * surface out of it.<p>Message types, carried as a type tag:<ul><li>REGISTER_WORKER: worker to
 * master, {workerId, host, port, slots}</li><li>HEARTBEAT: worker to master,
 * {workerId}</li><li>SUBMIT_JOB: client to master, {planGraphBytes}, the plan graph bytes and
 * not Java serialization</li><li>ASSIGN_STAGE: master to worker, {stageId, stageCommandBlob} in
 * an explicit stage command envelope</li><li>STAGE_REPORT: worker to master, {stageId, success,
 * rowCount, errorMessage}</li><li>JOB_RESULT: master to client, {success, rowCount,
 * errorMessage}</li><li>ACK: a generic confirmation, the lightweight response to register and
 * heartbeat</li></ul>
 */
public final class RpcMessage {

    public enum Type {
        REGISTER_WORKER, HEARTBEAT, SUBMIT_JOB, ASSIGN_STAGE, STAGE_REPORT, JOB_RESULT, ACK,
        /**
         * Report the master asynchronously once the stage has bound its listening port,
         * replacing orchestration over stdout.
         */
        PORT_REPORT,
        /**
         * Master to worker, cancel the child process of a stage so that a surviving or wedged
         * one is reclaimed after the first failure short-circuits the job.
         */
        CANCEL_STAGE,
        /**
         * Progress report for a running stage, carrying stageId and the current row count while
         * the state is still RUNNING. Appended at the end of the enum so existing ordinals stay
         * valid.
         */
        PROGRESS_REPORT,
        /**
         * Worker to master, a cooperative departure carrying the full RPC and data endpoint
         * generation. One-way, with no ACK expected.
         */
        DEREGISTER_WORKER,
        /**
         * Master to worker, coordinate the sinks already prepared for output.
         */
        COMMIT_PREPARED_OUTPUT, ROLLBACK_PREPARED_OUTPUT, FINALIZE_PREPARED_OUTPUT,
        /**
         * Install a manifest member, publish the whole group from one point, and expose the
         * outcome to queries.
         */
        INSTALL_MANIFEST_MEMBER, MANIFEST_MEMBER_INSTALLED, PUBLISH_MANIFEST_GROUP,
        QUERY_MANIFEST_OUTCOME, MANIFEST_OUTCOME,
        /**
         * Client to master, submit versioned plan artifact bytes. The ordinal is appended so
         * older messages keep theirs.
         */
        SUBMIT_PLAN_ARTIFACT,
        /**
         * Stable run submission envelope, authoritative query and snapshot response.
         */
        SUBMIT_RUN, QUERY_RUN, RUN_SNAPSHOT,
        /**
         * Worker to the current master, committing a durable assignment receipt before any user
         * code runs.
         */
        ASSIGNMENT_ACCEPTED,
        /**
         * Transfer an immutable manifest member from a worker's rollback ownership to the
         * journal and the collector.
         */
        SEAL_MANIFEST_MEMBER,
        /** Master to Worker reconciliation of a durable assignment; the response returns only a frozen terminal state or a safe ABSENT. */
        RECONCILE_DURABLE_STAGE, DURABLE_STAGE_RECONCILED,
        /**
         * Query the connector-neutral sink outcome by durable run identity.
         */
        QUERY_BATCH_SINK_OUTCOME, BATCH_SINK_OUTCOME,
        /**
         * Query a bounded, redacted page of run mutation events by exclusive sequence cursor.
         */
        QUERY_RUN_EVENTS, RUN_EVENTS_PAGE,
        /**
         * Client to master, a stable request cancellation and its closed-set authority result.
         */
        CANCEL_RUN, RUN_CANCELLATION_RESULT,
        /**
         * Atomic echo of the data protocol and identity after an mTLS worker registers
         * successfully.
         */
        WORKER_REGISTERED,
        /**
         * Query the full bounded quality decision for the current attempt of a durable run.
         */
        QUERY_QUALITY_GATE_DECISION, QUALITY_GATE_DECISION,
        /** Master→Worker generic prepared-group action and bounded result. */
        EXECUTE_PROVIDER_GROUP, PROVIDER_GROUP_RESULT,
        /** bounded redacted incremental frontier query. */
        QUERY_INCREMENTAL_FRONTIER, INCREMENTAL_FRONTIER_VIEW,
        /** canonical bounded incremental intent; authority tokens are Master-owned. */
        SUBMIT_INCREMENTAL_RUN,
        /** bounded source capture/materialize on the admitted Worker runtime. */
        EXECUTE_INCREMENTAL_SOURCE_ACTION, INCREMENTAL_SOURCE_ACTION_RESULT,
        /** Worker installs/publishes the engine-owned CDC Manifest closure. */
        INSTALL_INCREMENTAL_CONTROL_MEMBER, PUBLISH_INCREMENTAL_MANIFEST,
        /** client bootstraps one feed authority before its first trigger. */
        INITIALIZE_INCREMENTAL_FEED,
        /** Master renews an early prepared sink while sibling stages run. */
        RENEW_PREPARED_OUTPUT
    }

    private final Type type;
    private final String workerId;      // REGISTER/HEARTBEAT/ASSIGN/REPORT
    private final String host;          // REGISTER / ASSIGN leader claim
    private final int port;             // REGISTER / ASSIGN leader claim
    private final int slots;            // REGISTER / ASSIGN term high bits
    private final byte[] planGraphBytes;  // shared bounded bytes field for SUBMIT_JOB plan graph and SUBMIT_PLAN_ARTIFACT artifact
    private final long stageId;         // ASSIGN/REPORT
    private final byte[] stageCommandBlob;  // ASSIGN stage command envelope; only the loopback ephemeral path may accept the legacy Java blob
    private final boolean success;      // REPORT/JOB_RESULT
    private final long rowCount;  // REPORT and JOB_RESULT output rows, retained under its historical name
    private final String errorMessage;  // REPORT and JOB_RESULT cause string, never a stack object
    // Rows already processed, that is read in, by a progress or terminal stage report. rowCount is the output
    // count and inputRows the input count. While a breaker such as group by, sort or join is still filling its
    // buffer, inputRows keeps growing while the output stays zero. Zero on messages that are not reports.
    private final long inputRows;
    // HEARTBEAT carries the worker's current free slots (= slots minus running stage instances), which the
    // master uses only for reconciliation, since the ledger stays authoritative.
    // **Its own field**, not reusing slots: slots is already taken by portIndex(), and reuse would clash with
    // PORT_REPORT. -1 = unknown (an idle heartbeat, or an old frame without the field); 0 = fully loaded, a
    // legitimate value that must stay distinguishable from unknown.
    private final long freeSlots;
    // REGISTER_WORKER carries the single data port of the worker: greater than zero when multiplexing is on,
    // and -1 when it is off or the frame predates it. The master stores it in its worker record, and an
    // assignment builds a mux shuffle endpoint from host, data port and target id. Appended at the end of the
    // frame by RpcCodec, which reads -1 for an older frame.
    private final int dataPort;
    /**
     * Generation token of the worker process, appended at the end of the control frame; null
     * means a legacy frame or a message that is not from a worker.
     */
    private final String incarnationId;
    /**
     * Sink coordination group, used only by the three prepared-output control messages.
     */
    private final String commitGroupId;
    /**
     * Fields appended by the manifest cluster protocol; an older frame reads null, zero and
     * null respectively.
     */
    private final String attemptId;
    private final long commitGeneration;
    private final byte[] commitPayload;
    private final String commitDomainPath;
    private final String commitDomainId;
    private final String commitOutcome;
    /**
     * Execution attempt authority for ASSIGN, PORT, PROGRESS and STAGE; null means a legacy or
     * unrelated message.
     */
    private final RunAttemptFence runAttemptFence;
    /**
     * Stable logical run identity for QUERY_RUN; the worker, manifest and attempt fields are
     * not reused for it.
     */
    private final String runId;
    /**
     * Used only by an ephemeral SUBMIT; null is the request shape that predates durable
     * submission.
     */
    private final EphemeralSubmitTimeoutPolicy.Budget ephemeralSubmitTimeouts;
    /**
     * null is the legacy schema, a non-null value the full secure suffix; the object itself is
     * immutable.
     */
    private final RpcSecurityMetadata securityMetadata;
    /**
     * Atomic group of data plane negotiation fields, on the REGISTER request and the
     * WORKER_REGISTERED response.
     */
    private final DataPlaneRegistration dataPlaneRegistration;
    // In-process pseudo RPC hands the stage command over by object reference, with no serialization. The type is
    // Object so that this package does not depend on the cluster stage command class. It never takes part in
    // socket encoding, where RpcCodec writes only the blob field: a non-null object is used directly by the
    // worker, and otherwise the worker falls back to deserializing the blob.
    private transient Object stageCommand;
    // same-JVM local-attached Artifact binding. Never encoded by RpcCodec.
    private transient PlanArtifact inProcessPlanArtifact;
    // Default local preserves BatchSession's existing output-order performance hint.
    // Null means cluster policy; never encoded by RpcCodec.
    private transient Boolean inProcessCanonicalizeOutput;
    /**
     * Created by socket decoding; it is not part of the wire and is not produced by an
     * application message constructor.
     */
    private transient ControlResidentLease residentLease;
    private transient ControlResidentLease.OwnerToken residentOwnerToken;
    /**
     * Held separately by the RpcServer handler and encoder: a response payload must be reserved
     * before it is allocated.
     */
    private transient ControlResidentLease responseResidentLease;
    private transient ControlResidentLease.OwnerToken responseResidentOwnerToken;
    private transient long responseResidentReservedBytes;

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, 0L, -1L);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, -1L);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, freeSlots, -1);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots, int dataPort) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, freeSlots, dataPort, null);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots, int dataPort, String incarnationId) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, freeSlots, dataPort, incarnationId, null);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots, int dataPort, String incarnationId, String commitGroupId) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, freeSlots, dataPort, incarnationId, commitGroupId,
                null, 0L, null, null, null, null);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots, int dataPort, String incarnationId, String commitGroupId,
               String attemptId, long commitGeneration, byte[] commitPayload,
               String commitDomainPath, String commitDomainId, String commitOutcome) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, freeSlots, dataPort, incarnationId, commitGroupId,
                attemptId, commitGeneration, commitPayload, commitDomainPath, commitDomainId, commitOutcome, null);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots, int dataPort, String incarnationId, String commitGroupId,
               String attemptId, long commitGeneration, byte[] commitPayload,
               String commitDomainPath, String commitDomainId, String commitOutcome,
               RunAttemptFence runAttemptFence) {
        this(type, workerId, host, port, slots, planGraphBytes, stageId, stageCommandBlob,
                success, rowCount, errorMessage, inputRows, freeSlots, dataPort, incarnationId,
                commitGroupId, attemptId, commitGeneration, commitPayload, commitDomainPath,
                commitDomainId, commitOutcome, runAttemptFence, null);
    }

    RpcMessage(Type type, String workerId, String host, int port, int slots, byte[] planGraphBytes,
               long stageId, byte[] stageCommandBlob, boolean success, long rowCount, String errorMessage,
               long inputRows, long freeSlots, int dataPort, String incarnationId, String commitGroupId,
               String attemptId, long commitGeneration, byte[] commitPayload,
               String commitDomainPath, String commitDomainId, String commitOutcome,
               RunAttemptFence runAttemptFence, String runId) {
        this.type = type;
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.slots = slots;
        this.planGraphBytes = planGraphBytes;
        this.stageId = stageId;
        this.stageCommandBlob = stageCommandBlob;
        this.success = success;
        this.rowCount = rowCount;
        this.errorMessage = errorMessage;
        this.inputRows = inputRows;
        this.freeSlots = freeSlots;
        this.dataPort = dataPort;
        this.incarnationId = incarnationId;
        this.commitGroupId = commitGroupId;
        this.attemptId = attemptId;
        this.commitGeneration = commitGeneration;
        this.commitPayload = commitPayload;
        this.commitDomainPath = commitDomainPath;
        this.commitDomainId = commitDomainId;
        this.commitOutcome = commitOutcome;
        this.runAttemptFence = runAttemptFence;
        this.runId = runId;
        this.ephemeralSubmitTimeouts = null;
        this.securityMetadata = null;
        this.dataPlaneRegistration = null;
    }

    private RpcMessage(RpcMessage source, RpcSecurityMetadata securityMetadata) {
        this(source, source.ephemeralSubmitTimeouts,
                java.util.Objects.requireNonNull(securityMetadata, "securityMetadata"));
    }

    private RpcMessage(RpcMessage source,
                       EphemeralSubmitTimeoutPolicy.Budget ephemeralSubmitTimeouts) {
        this(source, java.util.Objects.requireNonNull(
                ephemeralSubmitTimeouts, "ephemeralSubmitTimeouts"), source.securityMetadata);
    }

    private RpcMessage(RpcMessage source,
                       EphemeralSubmitTimeoutPolicy.Budget ephemeralSubmitTimeouts,
                       RpcSecurityMetadata securityMetadata) {
        this(source, ephemeralSubmitTimeouts, securityMetadata,
                source.dataPlaneRegistration);
    }

    private RpcMessage(RpcMessage source,
                       DataPlaneRegistration dataPlaneRegistration) {
        this(source, source.ephemeralSubmitTimeouts, source.securityMetadata,
                java.util.Objects.requireNonNull(
                        dataPlaneRegistration, "data plane registration"));
    }

    private RpcMessage(RpcMessage source,
                       EphemeralSubmitTimeoutPolicy.Budget ephemeralSubmitTimeouts,
                       RpcSecurityMetadata securityMetadata,
                       DataPlaneRegistration dataPlaneRegistration) {
        this.type = source.type;
        this.workerId = source.workerId;
        this.host = source.host;
        this.port = source.port;
        this.slots = source.slots;
        this.planGraphBytes = source.planGraphBytes;
        this.stageId = source.stageId;
        this.stageCommandBlob = source.stageCommandBlob;
        this.success = source.success;
        this.rowCount = source.rowCount;
        this.errorMessage = source.errorMessage;
        this.inputRows = source.inputRows;
        this.freeSlots = source.freeSlots;
        this.dataPort = source.dataPort;
        this.incarnationId = source.incarnationId;
        this.commitGroupId = source.commitGroupId;
        this.attemptId = source.attemptId;
        this.commitGeneration = source.commitGeneration;
        this.commitPayload = source.commitPayload;
        this.commitDomainPath = source.commitDomainPath;
        this.commitDomainId = source.commitDomainId;
        this.commitOutcome = source.commitOutcome;
        this.runAttemptFence = source.runAttemptFence;
        this.runId = source.runId;
        this.ephemeralSubmitTimeouts = ephemeralSubmitTimeouts;
        this.securityMetadata = securityMetadata;
        this.dataPlaneRegistration = dataPlaneRegistration;
        this.stageCommand = source.stageCommand;
        this.inProcessPlanArtifact = source.inProcessPlanArtifact;
        this.inProcessCanonicalizeOutput = source.inProcessCanonicalizeOutput;
        this.residentLease = source.residentLease;
        this.residentOwnerToken = source.residentOwnerToken;
        this.responseResidentLease = source.responseResidentLease;
        this.responseResidentOwnerToken = source.responseResidentOwnerToken;
        this.responseResidentReservedBytes = source.responseResidentReservedBytes;
    }

    public static RpcMessage registerWorker(String workerId, String host, int port, int slots) {
        return new RpcMessage(Type.REGISTER_WORKER, workerId, host, port, slots, null, 0, null, false, 0, null);
    }

    /**
     * Register with the single data port of the worker; -1 means multiplexing is off.
     */
    public static RpcMessage registerWorker(String workerId, String host, int port, int slots, int dataPort) {
        return registerWorker(workerId, host, port, slots, dataPort, null);
    }

    /**
     * Register carrying the process-level incarnation of the worker.
     */
    public static RpcMessage registerWorker(String workerId, String host, int port, int slots, int dataPort,
                                            String incarnationId) {
        return new RpcMessage(Type.REGISTER_WORKER, workerId, host, port, slots, null, 0, null,
                false, 0, null, 0L, -1L, dataPort, incarnationId);
    }

    /**
     * A new worker registration must atomically carry its data plane protocol and certificate
     * identity.
     */
    public static RpcMessage registerWorker(String workerId, String host, int port, int slots, int dataPort,
                                            String incarnationId,
                                            DataPlaneRegistration dataPlaneRegistration) {
        return registerWorker(workerId, host, port, slots, dataPort, incarnationId)
                .withDataPlaneRegistration(dataPlaneRegistration);
    }

    /**
     * The dedicated success response of an mTLS master, which must not degrade into an empty
     * ACK.
     */
    public static RpcMessage workerRegistered(String incarnationId,
                                              DataPlaneRegistration dataPlaneRegistration) {
        RpcMessage response = new RpcMessage(Type.WORKER_REGISTERED, null, null, 0, 0,
                null, 0, null, true, 0, null, 0L, -1L, -1, incarnationId);
        return response.withDataPlaneRegistration(dataPlaneRegistration);
    }

    /**
     * Revoke the current worker endpoint before a cooperative close; the master acts only on a
     * full generation match.
     */
    public static RpcMessage deregisterWorker(String workerId, String host, int port, int slots, int dataPort) {
        return deregisterWorker(workerId, host, port, slots, dataPort, null);
    }

    public static RpcMessage deregisterWorker(String workerId, String host, int port, int slots, int dataPort,
                                              String incarnationId) {
        return new RpcMessage(Type.DEREGISTER_WORKER, workerId, host, port, slots, null, 0, null,
                false, 0, null, 0L, -1L, dataPort, incarnationId);
    }

    /**
     * Heartbeat with no payload, where an unknown free slot count is -1.
     */
    public static RpcMessage heartbeat(String workerId) {
        return new RpcMessage(Type.HEARTBEAT, workerId, null, 0, 0, null, 0, null, false, 0, null, 0L, -1L);
    }

    /** Heartbeat carrying free slots, for the master's reconciliation; the ledger remains authoritative. */
    public static RpcMessage heartbeat(String workerId, long freeSlots) {
        return heartbeat(workerId, freeSlots, null);
    }

    public static RpcMessage heartbeat(String workerId, long freeSlots, String incarnationId) {
        return new RpcMessage(Type.HEARTBEAT, workerId, null, 0, 0, null, 0, null,
                false, 0, null, 0L, freeSlots, -1, incarnationId);
    }

    public static RpcMessage submitJob(byte[] planGraphBytes) {
        return new RpcMessage(Type.SUBMIT_JOB, null, null, 0, 0, planGraphBytes, 0, null, false, 0, null);
    }

    public static RpcMessage submitJob(byte[] planGraphBytes, String attemptId) {
        return new RpcMessage(Type.SUBMIT_JOB, null, null, 0, 0, planGraphBytes, 0, null,
                false, 0, null, 0L, -1L, -1, null, null,
                attemptId, 0L, null, null, null, null);
    }

    public static RpcMessage submitJob(
            byte[] planGraphBytes, String attemptId,
            EphemeralSubmitTimeoutPolicy.Budget timeouts) {
        return submitJob(planGraphBytes, attemptId).withEphemeralSubmitTimeouts(timeouts);
    }

    public static RpcMessage submitPlanArtifact(byte[] artifactBytes, String publicationId) {
        return new RpcMessage(Type.SUBMIT_PLAN_ARTIFACT, null, null, 0, 0, artifactBytes, 0, null,
                false, 0, null, 0L, -1L, -1, null, null,
                publicationId, 0L, null, null, null, null);
    }

    public static RpcMessage submitPlanArtifact(
            byte[] artifactBytes, String publicationId,
            EphemeralSubmitTimeoutPolicy.Budget timeouts) {
        return submitPlanArtifact(artifactBytes, publicationId)
                .withEphemeralSubmitTimeouts(timeouts);
    }

    public static RpcMessage submitPlanArtifactInProcess(
            PlanArtifact artifact, String publicationId) {
        return submitPlanArtifactInProcess(artifact, publicationId, null);
    }

    public static RpcMessage submitPlanArtifactInProcess(
            PlanArtifact artifact, String publicationId,
            EphemeralSubmitTimeoutPolicy.Budget timeouts) {
        if (artifact == null) {
            throw new IllegalArgumentException("in-process PlanArtifact is required");
        }
        RpcMessage message = submitPlanArtifact(new byte[0], publicationId);
        message.inProcessPlanArtifact = artifact;
        return timeouts == null ? message
                : message.withEphemeralSubmitTimeouts(timeouts);
    }

    public static RpcMessage submitPlanArtifactInProcess(
            PlanArtifact artifact, String publicationId,
            EphemeralSubmitTimeoutPolicy.Budget timeouts,
            boolean canonicalizeOutput) {
        RpcMessage message = submitPlanArtifactInProcess(
                artifact, publicationId, timeouts);
        message.inProcessCanonicalizeOutput = canonicalizeOutput;
        return message;
    }

    public static RpcMessage submitRun(byte[] runSubmissionEnvelope) {
        return new RpcMessage(Type.SUBMIT_RUN, null, null, 0, 0,
                runSubmissionEnvelope, 0, null, false, 0, null);
    }

    public static RpcMessage submitIncrementalRun(byte[] submissionEnvelope) {
        if (submissionEnvelope == null || submissionEnvelope.length == 0
                || submissionEnvelope.length
                > io.rowweave.incremental.IncrementalRunSubmissionCodec
                .MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental submission bytes are out of bounds");
        }
        return new RpcMessage(Type.SUBMIT_INCREMENTAL_RUN,
                null, null, 0, 0, submissionEnvelope.clone(), 0, null,
                false, 0, null);
    }

    public static RpcMessage executeIncrementalSourceAction(
            String workerId, byte[] command, Endpoint leader,
            long termToken, String incarnationId) {
        if (workerId == null || workerId.isEmpty()
                || incarnationId == null || incarnationId.isEmpty()
                || leader == null || command == null || command.length == 0
                || command.length
                > io.rowweave.cluster.IncrementalSourceActionCommandCodec
                .MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action RPC fields are invalid");
        }
        return new RpcMessage(Type.EXECUTE_INCREMENTAL_SOURCE_ACTION,
                workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), command.clone(), 0L, null,
                false, 0L, null, 0L, -1L, (int) termToken,
                incarnationId);
    }

    public static RpcMessage incrementalSourceActionResult(byte[] result) {
        if (result == null || result.length == 0
                || result.length
                > io.rowweave.cluster.IncrementalSourceActionResultCodec
                .MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action result bytes are out of bounds");
        }
        return new RpcMessage(Type.INCREMENTAL_SOURCE_ACTION_RESULT,
                null, null, 0, 0, result.clone(), 0L, null,
                true, 0L, null);
    }

    public static RpcMessage queryRun(String runId) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        return new RpcMessage(Type.QUERY_RUN, null, null, 0, 0,
                null, 0, null, false, 0, null, 0L, -1L, -1,
                null, null, null, 0L, null, null, null, null, null, validated);
    }

    public static RpcMessage queryIncrementalFrontier(String feedId) {
        String validated = new RunAttemptFence(feedId, "query", 1L).runId();
        return new RpcMessage(Type.QUERY_INCREMENTAL_FRONTIER,
                null, null, 0, 0, null, 0, null, false, 0, null,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage initializeIncrementalFeed(
            String feedId, String commandId, byte[] requestBytes) {
        RunAttemptFence identity = new RunAttemptFence(
                feedId, commandId, 1L);
        if (requestBytes == null || requestBytes.length == 0
                || requestBytes.length
                > io.rowweave.incremental.IncrementalInitializeRequestCodec
                .MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental initialize bytes are out of bounds");
        }
        return new RpcMessage(Type.INITIALIZE_INCREMENTAL_FEED,
                null, null, 0, 0, requestBytes.clone(), 0, null,
                false, 0, null, 0L, -1L, -1, null, null,
                identity.attemptId(), 0L, null, null, null, null,
                null, identity.runId());
    }

    public static RpcMessage incrementalFrontierView(
            String feedId, byte[] viewBytes) {
        String validated = new RunAttemptFence(feedId, "query", 1L).runId();
        if (viewBytes == null || viewBytes.length == 0
                || viewBytes.length
                > io.rowweave.incremental.IncrementalFrontierViewCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("frontier view bytes are out of bounds");
        }
        return new RpcMessage(Type.INCREMENTAL_FRONTIER_VIEW,
                null, null, 0, 0, viewBytes, 0, null, true, 0, null,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage incrementalFrontierViewNotFound(String feedId) {
        String validated = new RunAttemptFence(feedId, "query", 1L).runId();
        return new RpcMessage(Type.INCREMENTAL_FRONTIER_VIEW,
                null, null, 0, 0, null, 0, null, false, 0, null,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage incrementalFrontierViewFailure(
            String feedId, String errorMessage) {
        String validated = new RunAttemptFence(feedId, "query", 1L).runId();
        if (errorMessage == null || errorMessage.isEmpty()) {
            throw new IllegalArgumentException("frontier view failure requires an error");
        }
        return new RpcMessage(Type.INCREMENTAL_FRONTIER_VIEW,
                null, null, 0, 0, null, 0, null, false, 0, errorMessage,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage runSnapshot(byte[] snapshotBytes) {
        if (snapshotBytes == null) {
            throw new IllegalArgumentException("snapshot bytes are required");
        }
        return new RpcMessage(Type.RUN_SNAPSHOT, null, null, 0, 0,
                snapshotBytes, 0, null, true, 0, null);
    }

    public static RpcMessage runSnapshot(byte[] snapshotBytes, byte[] telemetryBytes) {
        if (snapshotBytes == null) {
            throw new IllegalArgumentException("snapshot bytes are required");
        }
        return new RpcMessage(Type.RUN_SNAPSHOT, null, null, 0, 0,
                snapshotBytes, 0, null, true, 0, null, 0L, -1L, -1,
                null, null, null, 0L, telemetryPayload(Type.RUN_SNAPSHOT, telemetryBytes),
                null, null, null);
    }

    /** Internal summary path avoids cloning an already encoded telemetry payload. */
    public static RpcMessage runSnapshotWithTelemetrySummary(
            byte[] snapshotBytes,
            io.rowweave.plan.ExecutionTelemetrySummary summary) {
        if (snapshotBytes == null) {
            throw new IllegalArgumentException("snapshot bytes are required");
        }
        if (summary == null) {
            throw new IllegalArgumentException("execution telemetry summary is required");
        }
        byte[] encoded = io.rowweave.plan.ExecutionTelemetrySummaryCodec.encode(summary);
        return new RpcMessage(Type.RUN_SNAPSHOT, null, null, 0, 0,
                snapshotBytes, 0, null, true, 0, null, 0L, -1L, -1,
                null, null, null, 0L, encoded, null, null, null);
    }

    public static RpcMessage runSnapshotNotFound() {
        return new RpcMessage(Type.RUN_SNAPSHOT, null, null, 0, 0,
                null, 0, null, false, 0, null);
    }

    public static RpcMessage runSnapshotFailure(String errorMessage) {
        if (errorMessage == null || errorMessage.isEmpty()) {
            throw new IllegalArgumentException("run snapshot failure requires an error");
        }
        return new RpcMessage(Type.RUN_SNAPSHOT, null, null, 0, 0,
                null, 0, null, false, 0, errorMessage);
    }

    public static RpcMessage queryBatchSinkOutcome(String runId) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        return new RpcMessage(Type.QUERY_BATCH_SINK_OUTCOME, null, null, 0, 0,
                null, 0, null, false, 0, null, 0L, -1L, -1,
                null, null, null, 0L, null, null, null, null, null, validated);
    }

    public static RpcMessage queryQualityGateDecision(String runId) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        return new RpcMessage(Type.QUERY_QUALITY_GATE_DECISION,
                null, null, 0, 0, null, 0, null, false, 0, null,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage qualityGateDecision(
            String runId, byte[] decisionBytes) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        if (decisionBytes == null || decisionBytes.length == 0
                || decisionBytes.length
                > io.rowweave.QualityGateDecisionCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "quality decision response bytes are out of bounds");
        }
        return new RpcMessage(Type.QUALITY_GATE_DECISION,
                null, null, 0, 0, decisionBytes, 0, null, true, 0, null,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage qualityGateDecisionNotFound(String runId) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        return new RpcMessage(Type.QUALITY_GATE_DECISION,
                null, null, 0, 0, null, 0, null, false, 0, null,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage qualityGateDecisionFailure(
            String runId, String errorMessage) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        if (errorMessage == null || errorMessage.isEmpty()) {
            throw new IllegalArgumentException(
                    "quality decision query failure requires an error");
        }
        return new RpcMessage(Type.QUALITY_GATE_DECISION,
                null, null, 0, 0, null, 0, null, false, 0, errorMessage,
                0L, -1L, -1, null, null, null, 0L,
                null, null, null, null, null, validated);
    }

    public static RpcMessage batchSinkOutcome(
            String runId, String outcome, long generation,
            byte[] identityDigest, String detailCode) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        return new RpcMessage(Type.BATCH_SINK_OUTCOME, null, null, 0, 0,
                null, 0, null, "COMMITTED".equals(outcome), 0, detailCode,
                0L, -1L, -1, null, null, null, generation,
                identityDigest, null, null, outcome, null, validated);
    }

    /**
     * For QUERY_RUN_EVENTS, stageId and slots carry the exclusive cursor and the page limit of
     * 1 to 256.
     */
    public static RpcMessage queryRunEvents(String runId, long afterSequence, int limit) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        if (afterSequence < 0L) {
            throw new IllegalArgumentException("event afterSequence must be non-negative");
        }
        if (limit < 1 || limit > 256) {
            throw new IllegalArgumentException("event limit must be in [1,256]");
        }
        return new RpcMessage(Type.QUERY_RUN_EVENTS, null, null, 0, limit,
                null, afterSequence, null, false, 0, null,
                0L, -1L, -1, null, null, null, 0L, null,
                null, null, null, null, validated);
    }

    /**
     * planGraphBytes is reused as the bytes of an independent 64 KiB page codec.
     */
    public static RpcMessage runEventsPage(String runId, byte[] pageBytes) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        if (pageBytes == null || pageBytes.length == 0
                || pageBytes.length > RunExecutionEventPageCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("run event page bytes are out of bounds");
        }
        return new RpcMessage(Type.RUN_EVENTS_PAGE, null, null, 0, 0,
                pageBytes, 0L, null, true, 0L, null,
                0L, -1L, -1, null, null, null, 0L, null,
                null, null, null, null, validated);
    }

    public static RpcMessage runEventsPageFailure(String runId, String errorMessage) {
        String validated = new RunAttemptFence(runId, "query", 1L).runId();
        if (errorMessage == null || errorMessage.isEmpty()) {
            throw new IllegalArgumentException("run event page failure requires an error");
        }
        return new RpcMessage(Type.RUN_EVENTS_PAGE, null, null, 0, 0,
                null, 0L, null, false, 0L, errorMessage,
                0L, -1L, -1, null, null, null, 0L, null,
                null, null, null, null, validated);
    }

    /**
     * The attemptId field carries the caller-supplied stable cancellation request id.
     */
    public static RpcMessage cancelRun(String runId, String requestId) {
        String validatedRunId = RunCancellationAuthorityKernel.validateRunId(runId);
        String validatedRequestId = RunCancellationAuthorityKernel.validateRequestId(requestId);
        return new RpcMessage(Type.CANCEL_RUN, null, null, 0, 0,
                null, 0L, null, false, 0L, null,
                0L, -1L, -1, null, null, validatedRequestId, 0L, null,
                null, null, null, null, validatedRunId);
    }

    /**
     * commitOutcome, domainId and rowCount carry disposition, status and revision.
     */
    public static RpcMessage runCancellationResult(RunCancellationResult result) {
        if (result == null) {
            throw new IllegalArgumentException("run cancellation result is required");
        }
        return new RpcMessage(Type.RUN_CANCELLATION_RESULT, null, null, 0, 0,
                null, 0L, null, true, result.revision(), null,
                0L, -1L, -1, null, null, null, 0L, null,
                null, result.status() == null ? null : result.status().name(),
                result.disposition().name(), null, result.runId());
    }

    public static RpcMessage runCancellationFailure(String runId, String errorMessage) {
        String validatedRunId = RunCancellationAuthorityKernel.validateRunId(runId);
        requireCancellationError(errorMessage);
        return new RpcMessage(Type.RUN_CANCELLATION_RESULT, null, null, 0, 0,
                null, 0L, null, false, 0L, errorMessage,
                0L, -1L, -1, null, null, null, 0L, null,
                null, null, null, null, validatedRunId);
    }

    private static void requireCancellationError(String value) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException(
                    "run cancellation failure requires a bounded error");
        }
    }

    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null, stageId, stageCommandBlob, false, 0, null);
    }

    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         String incarnationId) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null, stageId, stageCommandBlob,
                false, 0, null, 0L, -1L, -1, incarnationId);
    }

    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null, stageId, stageCommandBlob,
                false, 0, null, 0L, -1L, -1, incarnationId, null,
                null, 0L, null, null, null, null, fence);
    }

    /**
     * Durable ASSIGN: the commitPayload field carries the bounded encoded assignment token.
     */
    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         String incarnationId, RunAttemptFence fence,
                                         byte[] stageAssignmentTokenBytes) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null,
                stageId, stageCommandBlob, false, 0, null, 0L, -1L, -1,
                incarnationId, null, null, 0L, stageAssignmentTokenBytes,
                null, null, null, fence);
    }

    /**
     * HA ASSIGN carries the incumbent leader endpoint and a 64-bit term, reusing slots and
     * dataPort so the frame is unchanged.
     */
    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         Endpoint leader, long termToken) {
        return assignStage(workerId, stageId, stageCommandBlob, leader, termToken, null);
    }

    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         Endpoint leader, long termToken, String incarnationId) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, stageCommandBlob, false, 0, null,
                0L, -1L, (int) termToken, incarnationId);
    }

    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         Endpoint leader, long termToken, String incarnationId,
                                         RunAttemptFence fence) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, stageCommandBlob, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, null,
                null, 0L, null, null, null, null, fence);
    }

    public static RpcMessage assignStage(String workerId, long stageId, byte[] stageCommandBlob,
                                         Endpoint leader, long termToken, String incarnationId,
                                         RunAttemptFence fence,
                                         byte[] stageAssignmentTokenBytes) {
        return new RpcMessage(Type.ASSIGN_STAGE, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, stageCommandBlob, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, null,
                null, 0L, stageAssignmentTokenBytes, null, null, null, fence);
    }

    /**
     * In-process pseudo RPC, passing the stage command object directly with no serialization;
     * the socket path uses the blob form.
     */
    public static RpcMessage assignStageInProcess(String workerId, long stageId, Object stageCommand) {
        RpcMessage m = new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null, stageId, null, false, 0, null);
        m.stageCommand = stageCommand;
        return m;
    }

    public static RpcMessage assignStageInProcess(String workerId, long stageId, Object stageCommand,
                                                  String incarnationId) {
        RpcMessage m = new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1, incarnationId);
        m.stageCommand = stageCommand;
        return m;
    }

    public static RpcMessage assignStageInProcess(String workerId, long stageId, Object stageCommand,
                                                  String incarnationId, RunAttemptFence fence) {
        RpcMessage m = new RpcMessage(Type.ASSIGN_STAGE, workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1, incarnationId, null,
                null, 0L, null, null, null, null, fence);
        m.stageCommand = stageCommand;
        return m;
    }

    /**
     * In-process ASSIGN carrying the leader claim version.
     */
    public static RpcMessage assignStageInProcess(String workerId, long stageId, Object stageCommand,
                                                  Endpoint leader, long termToken) {
        return assignStageInProcess(workerId, stageId, stageCommand, leader, termToken, null);
    }

    public static RpcMessage assignStageInProcess(String workerId, long stageId, Object stageCommand,
                                                  Endpoint leader, long termToken, String incarnationId) {
        RpcMessage m = new RpcMessage(Type.ASSIGN_STAGE, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId);
        m.stageCommand = stageCommand;
        return m;
    }

    public static RpcMessage assignStageInProcess(String workerId, long stageId, Object stageCommand,
                                                  Endpoint leader, long termToken, String incarnationId,
                                                  RunAttemptFence fence) {
        RpcMessage m = new RpcMessage(Type.ASSIGN_STAGE, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, null,
                null, 0L, null, null, null, null, fence);
        m.stageCommand = stageCommand;
        return m;
    }

    /**
     * Stage command object passed in process; null means the socket path, which deserializes
     * the blob field.
     */
    public Object stageCommand() { return stageCommand; }

    /**
     * Worker receipt; the token uses the existing bounded payload union field.
     */
    public static RpcMessage assignmentAccepted(
            String workerId, long stageId, byte[] stageAssignmentTokenBytes,
            byte[] stageCommandDigest, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.ASSIGNMENT_ACCEPTED, workerId, null, 0, 0,
                stageCommandDigest, stageId, null, false, 0, null,
                0L, -1L, -1, incarnationId, null, null, 0L,
                stageAssignmentTokenBytes, null, null, null, fence);
    }

    /**
     * Master to worker, cancel the child process of one stage forcibly. An unknown or finished
     * stage id is ignored by the worker.
     */
    public static RpcMessage cancelStage(String workerId, long stageId) {
        return new RpcMessage(Type.CANCEL_STAGE, workerId, null, 0, 0, null, stageId, null, false, 0, null);
    }

    public static RpcMessage cancelStage(String workerId, long stageId, String incarnationId) {
        return new RpcMessage(Type.CANCEL_STAGE, workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1, incarnationId);
    }

    public static RpcMessage cancelStage(
            String workerId, long stageId, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.CANCEL_STAGE, workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1, incarnationId, null,
                null, 0L, null, null, null, null, fence);
    }

    public static RpcMessage commitPreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId) {
        return outputControl(Type.COMMIT_PREPARED_OUTPUT, workerId, stageId, groupId, incarnationId);
    }

    public static RpcMessage renewPreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId,
            RunAttemptFence fence) {
        return outputControl(Type.RENEW_PREPARED_OUTPUT,
                workerId, stageId, groupId, incarnationId, fence);
    }

    public static RpcMessage renewPreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId, RunAttemptFence fence) {
        return outputControl(Type.RENEW_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId, fence);
    }

    public static RpcMessage commitPreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId,
            RunAttemptFence fence) {
        return outputControl(
                Type.COMMIT_PREPARED_OUTPUT, workerId, stageId, groupId, incarnationId, fence);
    }

    public static RpcMessage commitPreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId) {
        return outputControl(Type.COMMIT_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId);
    }

    public static RpcMessage commitPreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId, RunAttemptFence fence) {
        return outputControl(Type.COMMIT_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId, fence);
    }

    public static RpcMessage rollbackPreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId) {
        return outputControl(Type.ROLLBACK_PREPARED_OUTPUT, workerId, stageId, groupId, incarnationId);
    }

    public static RpcMessage rollbackPreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId,
            RunAttemptFence fence) {
        return outputControl(
                Type.ROLLBACK_PREPARED_OUTPUT, workerId, stageId, groupId, incarnationId, fence);
    }

    public static RpcMessage rollbackPreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId) {
        return outputControl(Type.ROLLBACK_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId);
    }

    public static RpcMessage rollbackPreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId, RunAttemptFence fence) {
        return outputControl(Type.ROLLBACK_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId, fence);
    }

    public static RpcMessage finalizePreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId) {
        return outputControl(Type.FINALIZE_PREPARED_OUTPUT, workerId, stageId, groupId, incarnationId);
    }

    public static RpcMessage finalizePreparedOutput(
            String workerId, long stageId, String groupId, String incarnationId,
            RunAttemptFence fence) {
        return outputControl(
                Type.FINALIZE_PREPARED_OUTPUT, workerId, stageId, groupId, incarnationId, fence);
    }

    public static RpcMessage finalizePreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId) {
        return outputControl(Type.FINALIZE_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId);
    }

    public static RpcMessage finalizePreparedOutput(
            String workerId, long stageId, String groupId, Endpoint leader, long termToken,
            String incarnationId, RunAttemptFence fence) {
        return outputControl(Type.FINALIZE_PREPARED_OUTPUT, workerId, stageId, groupId,
                leader, termToken, incarnationId, fence);
    }

    private static RpcMessage outputControl(
            Type type, String workerId, long stageId, String groupId, String incarnationId) {
        return outputControl(type, workerId, stageId, groupId, incarnationId, null);
    }

    private static RpcMessage outputControl(
            Type type, String workerId, long stageId, String groupId, String incarnationId,
            RunAttemptFence fence) {
        return new RpcMessage(type, workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1, incarnationId, groupId,
                null, 0L, null, null, null, null, fence);
    }

    private static RpcMessage outputControl(
            Type type, String workerId, long stageId, String groupId,
            Endpoint leader, long termToken, String incarnationId) {
        return outputControl(type, workerId, stageId, groupId,
                leader, termToken, incarnationId, null);
    }

    private static RpcMessage outputControl(
            Type type, String workerId, long stageId, String groupId,
            Endpoint leader, long termToken, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(type, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null,
                false, 0, null, 0L, -1L, (int) termToken, incarnationId, groupId,
                null, 0L, null, null, null, null, fence);
    }

    public static RpcMessage installManifestMember(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            String incarnationId) {
        return new RpcMessage(Type.INSTALL_MANIFEST_MEMBER, workerId, null, 0, 0,
                null, stageId, null, false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation, null, null, null, null);
    }

    public static RpcMessage installIncrementalControlMember(
            String workerId, long stageId, String groupId,
            String attemptId, long generation, byte[] command,
            String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.INSTALL_INCREMENTAL_CONTROL_MEMBER,
                workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation,
                command, null, null, null, fence);
    }

    public static RpcMessage publishIncrementalManifest(
            String workerId, long stageId, String groupId,
            String attemptId, long generation, byte[] command,
            String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.PUBLISH_INCREMENTAL_MANIFEST,
                workerId, null, 0, 0, null, stageId, null,
                false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation,
                command, null, null, null, fence);
    }

    public static RpcMessage publishIncrementalManifest(
            String workerId, long stageId, String groupId,
            String attemptId, long generation, byte[] command,
            Endpoint leader, long termToken, String incarnationId,
            RunAttemptFence fence) {
        return new RpcMessage(Type.PUBLISH_INCREMENTAL_MANIFEST,
                workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null,
                false, 0, null, 0L, -1L, (int) termToken,
                incarnationId, groupId, attemptId, generation,
                command, null, null, null, fence);
    }

    public static RpcMessage installIncrementalControlMember(
            String workerId, long stageId, String groupId,
            String attemptId, long generation, byte[] command,
            Endpoint leader, long termToken, String incarnationId,
            RunAttemptFence fence) {
        return new RpcMessage(Type.INSTALL_INCREMENTAL_CONTROL_MEMBER,
                workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null,
                false, 0, null, 0L, -1L, (int) termToken,
                incarnationId, groupId, attemptId, generation,
                command, null, null, null, fence);
    }

    public static RpcMessage installManifestMember(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.INSTALL_MANIFEST_MEMBER, workerId, null, 0, 0,
                null, stageId, null, false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation, null, null, null, null, fence);
    }

    public static RpcMessage installManifestMember(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            Endpoint leader, long termToken, String incarnationId) {
        return new RpcMessage(Type.INSTALL_MANIFEST_MEMBER, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, groupId,
                attemptId, generation, null, null, null, null);
    }

    public static RpcMessage installManifestMember(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            Endpoint leader, long termToken, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.INSTALL_MANIFEST_MEMBER, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, groupId,
                attemptId, generation, null, null, null, null, fence);
    }

    public static RpcMessage sealManifestMember(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.SEAL_MANIFEST_MEMBER, workerId, null, 0, 0,
                null, stageId, null, false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation, null,
                null, null, null, fence);
    }

    public static RpcMessage sealManifestMember(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            Endpoint leader, long termToken, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.SEAL_MANIFEST_MEMBER, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, groupId,
                attemptId, generation, null, null, null, null, fence);
    }

    public static RpcMessage reconcileDurableStage(
            String workerId, byte[] encodedToken, byte[] commandDigest,
            Endpoint leader, long termToken, String incarnationId, RunAttemptFence fence) {
        if (encodedToken == null) throw new IllegalArgumentException("assignment token is required");
        if (commandDigest == null || commandDigest.length != 32) {
            throw new IllegalArgumentException("stage command digest must be exactly 32 bytes");
        }
        if (leader == null) throw new IllegalArgumentException("leader is required");
        StageAssignmentToken token = StageAssignmentManifestCodec.decodeToken(encodedToken);
        if (!token.workerId().equals(workerId)
                || !token.workerIncarnationId().equals(incarnationId)
                || !token.fence().equals(fence)) {
            throw new IllegalArgumentException("reconciliation identity does not match assignment token");
        }
        return new RpcMessage(Type.RECONCILE_DURABLE_STAGE, workerId,
                leader.host(), leader.port(), (int) (termToken >>> 32),
                Arrays.copyOf(commandDigest, commandDigest.length), token.runtimeStageId(), null,
                false, 0L, null, 0L, -1L, (int) termToken, incarnationId, null,
                null, 0L, Arrays.copyOf(encodedToken, encodedToken.length),
                null, null, null, fence);
    }

    public static RpcMessage durableStageReconciledTerminal(StageTerminalReceipt receipt) {
        return durableStageReconciledTerminal(receipt, null);
    }

    /** reconciliation returns the same independent locator evidence as terminal report. */
    public static RpcMessage durableStageReconciledTerminal(
            StageTerminalReceipt receipt,
            io.rowweave.run.PreparedSinkReceipt preparedSink) {
        if (receipt == null) throw new IllegalArgumentException("terminal receipt is required");
        if (preparedSink != null
                && (!receipt.succeeded()
                || !preparedSink.token().equals(receipt.token())
                || !java.security.MessageDigest.isEqual(
                preparedSink.commandDigest(), receipt.commandDigest()))) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt does not match terminal receipt");
        }
        return new RpcMessage(Type.DURABLE_STAGE_RECONCILED, null, null, 0, 0,
                StageAssignmentManifestCodec.encodeTerminalReceipt(receipt),
                receipt.token().runtimeStageId(), preparedSink == null ? null
                : io.rowweave.run.PreparedSinkReceiptCodec.encode(preparedSink),
                true, receipt.outputRows(), null,
                receipt.inputRows(), -1L, -1, receipt.token().workerIncarnationId(), null,
                null, 0L, encodeStageTelemetry(receipt.telemetry()),
                null, null, "TERMINAL", receipt.token().fence());
    }

    public static RpcMessage durableStageReconciledAbsent() {
        return new RpcMessage(Type.DURABLE_STAGE_RECONCILED, null, null, 0, 0,
                null, 0L, null, true, 0L, null, 0L, -1L, -1, null, null,
                null, 0L, null, null, null, "ABSENT");
    }

    public static RpcMessage manifestMemberInstalled(byte[] encodedMember) {
        return new RpcMessage(Type.MANIFEST_MEMBER_INSTALLED, null, null, 0, 0, null, 0, null,
                true, 0, null, 0L, -1L, -1, null, null,
                null, 0L, encodedMember, null, null, null);
    }

    public static RpcMessage publishManifestGroup(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            byte[] encodedManifest, String incarnationId) {
        return new RpcMessage(Type.PUBLISH_MANIFEST_GROUP, workerId, null, 0, 0,
                null, stageId, null, false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation, encodedManifest, null, null, null);
    }

    public static RpcMessage publishManifestGroup(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            byte[] encodedManifest, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.PUBLISH_MANIFEST_GROUP, workerId, null, 0, 0,
                null, stageId, null, false, 0, null, 0L, -1L, -1,
                incarnationId, groupId, attemptId, generation, encodedManifest,
                null, null, null, fence);
    }

    public static RpcMessage publishManifestGroup(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            byte[] encodedManifest, Endpoint leader, long termToken, String incarnationId) {
        return new RpcMessage(Type.PUBLISH_MANIFEST_GROUP, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, groupId,
                attemptId, generation, encodedManifest, null, null, null);
    }

    public static RpcMessage publishManifestGroup(
            String workerId, long stageId, String groupId, String attemptId, long generation,
            byte[] encodedManifest, Endpoint leader, long termToken, String incarnationId,
            RunAttemptFence fence) {
        return new RpcMessage(Type.PUBLISH_MANIFEST_GROUP, workerId, leader.host(), leader.port(),
                (int) (termToken >>> 32), null, stageId, null, false, 0, null,
                0L, -1L, (int) termToken, incarnationId, groupId,
                attemptId, generation, encodedManifest, null, null, null, fence);
    }

    public static RpcMessage queryManifestOutcome(
            String domainPath, String domainId, String attemptId) {
        return new RpcMessage(Type.QUERY_MANIFEST_OUTCOME, null, null, 0, 0, null, 0, null,
                false, 0, null, 0L, -1L, -1, null, null,
                attemptId, 0L, null, domainPath, domainId, null);
    }

    public static RpcMessage manifestOutcome(
            String outcome, String attemptId, long generation, byte[] manifestHash, String error) {
        return new RpcMessage(Type.MANIFEST_OUTCOME, null, null, 0, 0, null, 0, null,
                "COMMITTED".equals(outcome), 0, error, 0L, -1L, -1, null, null,
                attemptId, generation, manifestHash, null, null, outcome);
    }

    public static RpcMessage executeProviderGroup(
            String workerId, long stageId, String groupId,
            String publicationId, long publicationGeneration,
            byte[] command, String incarnationId, RunAttemptFence fence) {
        if (command == null || command.length == 0) {
            throw new IllegalArgumentException("provider group command is required");
        }
        return new RpcMessage(Type.EXECUTE_PROVIDER_GROUP, workerId,
                null, 0, 0, null, stageId, null, false, 0L, null,
                0L, -1L, -1, incarnationId, groupId, publicationId,
                publicationGeneration, command, null, null, null, fence);
    }

    public static RpcMessage executeProviderGroup(
            String workerId, long stageId, String groupId,
            String publicationId, long publicationGeneration,
            byte[] command, Endpoint leader, long termToken,
            String incarnationId, RunAttemptFence fence) {
        if (command == null || command.length == 0) {
            throw new IllegalArgumentException("provider group command is required");
        }
        return new RpcMessage(Type.EXECUTE_PROVIDER_GROUP, workerId,
                leader.host(), leader.port(), (int) (termToken >>> 32),
                null, stageId, null, false, 0L, null, 0L, -1L,
                (int) termToken, incarnationId, groupId, publicationId,
                publicationGeneration, command, null, null, null, fence);
    }

    public static RpcMessage providerGroupResult(
            io.rowweave.commit.BatchSinkOutcome outcome) {
        if (outcome == null) {
            throw new IllegalArgumentException("provider group outcome is required");
        }
        return new RpcMessage(Type.PROVIDER_GROUP_RESULT, null, null, 0, 0,
                null, 0L, null, true, 0L, outcome.detailCode(), 0L, -1L,
                -1, null, null, null,
                outcome.generation() == null ? 0L : outcome.generation(),
                outcome.identityDigest(), null, null, outcome.state().name());
    }

    public io.rowweave.commit.BatchSinkOutcome providerGroupOutcome() {
        if (type != Type.PROVIDER_GROUP_RESULT) {
            throw new IllegalStateException(type + " is not a provider group result");
        }
        io.rowweave.commit.BatchSinkOutcome.State state =
                io.rowweave.commit.BatchSinkOutcome.State.valueOf(commitOutcome);
        if (state == io.rowweave.commit.BatchSinkOutcome.State.NOT_FOUND) {
            return io.rowweave.commit.BatchSinkOutcome.notFound();
        }
        return io.rowweave.commit.BatchSinkOutcome.of(
                state, commitGeneration == 0L ? null : commitGeneration,
                commitPayload, errorMessage);
    }

    /**
     * Compatibility overload that reports zero input rows; new code uses the overload that
     * carries the count.
     */
    public static RpcMessage stageReport(String workerId, long stageId, boolean success, long rowCount, String err) {
        return stageReport(workerId, stageId, success, rowCount, 0L, err);
    }

    /**
     * Terminal report carrying the rows processed as input and the rows produced as rowCount.
     */
    public static RpcMessage stageReport(String workerId, long stageId, boolean success,
                                         long inputRows, long outputRows, String err) {
        return stageReport(workerId, stageId, success, inputRows, outputRows, err, null);
    }

    public static RpcMessage stageReport(String workerId, long stageId, boolean success,
                                         long inputRows, long outputRows, String err, String incarnationId,
                                         RunAttemptFence fence) {
        return new RpcMessage(Type.STAGE_REPORT, workerId, null, 0, 0, null, stageId, null,
                success, outputRows, err, inputRows, -1L, -1, incarnationId, null,
                null, 0L, null, null, null, null, fence);
    }

    public static RpcMessage stageReport(String workerId, long stageId, boolean success,
                                         long inputRows, long outputRows, String err,
                                         String incarnationId, RunAttemptFence fence,
                                         byte[] telemetryBytes) {
        return new RpcMessage(Type.STAGE_REPORT, workerId, null, 0, 0, null, stageId, null,
                success, outputRows, err, inputRows, -1L, -1, incarnationId, null,
                null, 0L, telemetryPayload(Type.STAGE_REPORT, telemetryBytes),
                null, null, null, fence);
    }

    /** Worker internal value path: encode once directly into the immutable message backing. */
    public static RpcMessage stageReportWithTelemetry(
            String workerId, long stageId, boolean success,
            long inputRows, long outputRows, String err,
            String incarnationId, RunAttemptFence fence,
            StageExecutionTelemetry telemetry) {
        byte[] encoded = encodeStageTelemetry(telemetry);
        return new RpcMessage(Type.STAGE_REPORT, workerId, null, 0, 0, null,
                stageId, null, success, outputRows, err, inputRows, -1L, -1,
                incarnationId, null, null, 0L, encoded,
                null, null, null, fence);
    }

    /**
     * The full canonical terminal receipt can be retried with identical bytes.
     */
    public static RpcMessage durableStageReport(String workerId,
                                                StageTerminalReceipt receipt) {
        return durableStageReport(workerId, receipt, null);
    }

    public static RpcMessage durableStageReport(
            String workerId, StageTerminalReceipt receipt,
            io.rowweave.run.PreparedSinkReceipt preparedSink) {
        if (receipt == null) throw new IllegalArgumentException("terminal receipt is required");
        if (preparedSink != null
                && (!receipt.succeeded()
                || !preparedSink.token().equals(receipt.token())
                || !java.security.MessageDigest.isEqual(
                preparedSink.commandDigest(), receipt.commandDigest()))) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt does not match terminal receipt");
        }
        return new RpcMessage(Type.STAGE_REPORT, workerId, null, 0, 0,
                StageAssignmentManifestCodec.encodeTerminalReceipt(receipt),
                receipt.token().runtimeStageId(), preparedSink == null ? null
                : io.rowweave.run.PreparedSinkReceiptCodec.encode(preparedSink),
                receipt.succeeded(),
                receipt.outputRows(), receipt.failureCategory(), receipt.inputRows(),
                -1L, -1, receipt.token().workerIncarnationId(), null,
                null, 0L, encodeStageTelemetry(receipt.telemetry()),
                null, null, null, receipt.token().fence());
    }

    private static byte[] encodeStageTelemetry(StageExecutionTelemetry telemetry) {
        StageExecutionTelemetry value = telemetry == null
                ? StageExecutionTelemetry.unavailable() : telemetry;
        try {
            return StageExecutionTelemetryCodec.encode(value);
        } catch (RuntimeException | LinkageError unavailable) {
            return StageExecutionTelemetryCodec.encode(
                    StageExecutionTelemetry.unavailable());
        }
    }

    public static RpcMessage stageReport(String workerId, long stageId, boolean success,
                                         long inputRows, long outputRows, String err, String incarnationId) {
        return new RpcMessage(Type.STAGE_REPORT, workerId, null, 0, 0, null, stageId, null,
                success, outputRows, err, inputRows, -1L, -1, incarnationId);
    }

    /**
     * Compatibility overload that reports zero input rows; new code uses the overload that
     * carries the count.
     */
    public static RpcMessage progressReport(String workerId, long stageId, long rowCount, boolean running) {
        return progressReport(workerId, stageId, rowCount, 0L, running);
    }

    /**
     * Progress report while a stage runs, carrying rows processed as input and rows emitted as
     * output. The success flag is reused to say whether the stage is RUNNING and working or
     * READY and waiting for data. While a breaker fills its buffer the input count grows and
     * the output stays zero.
     */
    public static RpcMessage progressReport(String workerId, long stageId, long inputRows, long outputRows, boolean running) {
        return progressReport(workerId, stageId, inputRows, outputRows, running, null);
    }

    public static RpcMessage progressReport(String workerId, long stageId, long inputRows, long outputRows,
                                            boolean running, String incarnationId) {
        return new RpcMessage(Type.PROGRESS_REPORT, workerId, null, 0, 0, null, stageId, null,
                running, outputRows, null, inputRows, -1L, -1, incarnationId);
    }

    public static RpcMessage progressReport(String workerId, long stageId, long inputRows, long outputRows,
                                            boolean running, String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.PROGRESS_REPORT, workerId, null, 0, 0, null, stageId, null,
                running, outputRows, null, inputRows, -1L, -1, incarnationId, null,
                null, 0L, null, null, null, null, fence);
    }

    public static RpcMessage jobResult(boolean success, long rowCount, String err) {
        return new RpcMessage(Type.JOB_RESULT, null, null, 0, 0, null, 0, null, success, rowCount, err);
    }

    public static RpcMessage jobResult(boolean success, long rowCount, String err,
                                       byte[] telemetryBytes) {
        return new RpcMessage(Type.JOB_RESULT, null, null, 0, 0, null, 0, null,
                success, rowCount, err, 0L, -1L, -1, null, null,
                null, 0L, telemetryPayload(Type.JOB_RESULT, telemetryBytes),
                null, null, null);
    }

    /**
     * Internal-owner-friendly summary path: the codec creates the only private byte array,
     * so Master does not clone a separately resident-accounted 48 KiB payload.
     */
    public static RpcMessage jobResultWithTelemetrySummary(
            boolean success, long rowCount, String err,
            io.rowweave.plan.ExecutionTelemetrySummary summary) {
        if (summary == null) {
            throw new IllegalArgumentException("execution telemetry summary is required");
        }
        byte[] encoded = io.rowweave.plan.ExecutionTelemetrySummaryCodec.encode(summary);
        return new RpcMessage(Type.JOB_RESULT, null, null, 0, 0, null, 0, null,
                success, rowCount, err, 0L, -1L, -1, null, null,
                null, 0L, encoded, null, null, null);
    }

    public static RpcMessage durableJobResult(
            boolean success, long rowCount, String err,
            io.rowweave.run.DurableRunDisposition disposition) {
        if (disposition == null) {
            throw new IllegalArgumentException("durable run disposition is required");
        }
        return new RpcMessage(Type.JOB_RESULT, null, null, 0, 0, null, 0, null,
                success, rowCount, err, 0L, -1L, -1, null, null,
                null, 0L, null, null, null, disposition.name());
    }

    public static RpcMessage durableJobResultWithTelemetrySummary(
            boolean success, long rowCount, String err,
            io.rowweave.run.DurableRunDisposition disposition,
            io.rowweave.plan.ExecutionTelemetrySummary summary) {
        if (disposition == null) {
            throw new IllegalArgumentException("durable run disposition is required");
        }
        if (summary == null) {
            throw new IllegalArgumentException("execution telemetry summary is required");
        }
        byte[] encoded = io.rowweave.plan.ExecutionTelemetrySummaryCodec.encode(summary);
        return new RpcMessage(Type.JOB_RESULT, null, null, 0, 0, null, 0, null,
                success, rowCount, err, 0L, -1L, -1, null, null,
                null, 0L, encoded, null, null, disposition.name());
    }

    public static RpcMessage ack() {
        return new RpcMessage(Type.ACK, null, null, 0, 0, null, 0, null, true, 0, null);
    }

    /**
     * Report the port actually bound by a stage instance, naming the reporting worker, the
     * global stage instance id and the port.
     */
    public static RpcMessage portReport(String workerId, long stageId, int port) {
        return new RpcMessage(Type.PORT_REPORT, workerId, null, port, 0, null, stageId, null, true, 0, null);
    }

    /**
     * A join consumer reports each port separately once both of its inbound edges have bound,
     * with portIndex 0 for the left and 1 for the right, carried in the slots field. A stage
     * with a single inbound edge uses {@link #portReport(String, long, int)}, where portIndex
     * defaults to 0.
     */
    public static RpcMessage portReport(String workerId, long stageId, int port, int portIndex) {
        return portReport(workerId, stageId, port, portIndex, null);
    }

    public static RpcMessage portReport(String workerId, long stageId, int port, int portIndex,
                                        String incarnationId) {
        return new RpcMessage(Type.PORT_REPORT, workerId, null, port, portIndex, null, stageId, null,
                true, 0, null, 0L, -1L, -1, incarnationId);
    }

    public static RpcMessage portReport(String workerId, long stageId, int port, int portIndex,
                                        String incarnationId, RunAttemptFence fence) {
        return new RpcMessage(Type.PORT_REPORT, workerId, null, port, portIndex, null, stageId, null,
                true, 0, null, 0L, -1L, -1, incarnationId, null,
                null, 0L, null, null, null, null, fence);
    }

    /**
     * Port index of a port report: 0 for the left or only edge and 1 for the right, carried in
     * the slots field.
     */
    public int portIndex() { return slots; }

    public Type type() { return type; }
    public String workerId() { return workerId; }
    public String host() { return host; }
    public int port() { return port; }
    public int slots() { return slots; }
    public byte[] planGraphBytes() { return planGraphBytes; }

    public PlanArtifact inProcessPlanArtifact() { return inProcessPlanArtifact; }
    public Boolean inProcessCanonicalizeOutput() {
        return inProcessCanonicalizeOutput;
    }
    public long stageId() { return stageId; }
    public byte[] stageCommandBlob() { return stageCommandBlob; }
    public boolean success() { return success; }
    public long rowCount() { return rowCount; }
    public String errorMessage() { return errorMessage; }
    /**
     * Rows processed, that is read in; rowCount is the number of rows emitted.
     */
    public long inputRows() { return inputRows; }
    /** Free slots reported by a HEARTBEAT (-1 = unknown); the master only reconciles against them, and the ledger is authoritative. */
    public long freeSlots() { return freeSlots; }

    /**
     * The single data port reported at registration; -1 means multiplexing is off or the frame
     * predates it.
     */
    public int dataPort() { return dataPort; }

    public DataPlaneRegistration dataPlaneRegistration() { return dataPlaneRegistration; }

    /**
     * Process-level incarnation of the worker; null means a legacy message or one not from a
     * worker.
     */
    public String incarnationId() { return incarnationId; }

    public String commitGroupId() { return commitGroupId; }

    public String attemptId() { return attemptId; }
    public long commitGeneration() { return commitGeneration; }
    public byte[] commitPayload() {
        return supportsExecutionTelemetry(type) && commitPayload != null
                ? commitPayload.clone() : commitPayload;
    }
    /** Typed immutable public boundary for telemetry-bearing messages. */
    public byte[] executionTelemetryBytes() {
        if (!supportsExecutionTelemetry(type)) {
            throw new IllegalStateException(type + " cannot carry execution telemetry");
        }
        return commitPayload == null ? null : commitPayload.clone();
    }
    /** Internal consumers decode the bounded backing in-place while its RPC lease is live. */
    public io.rowweave.plan.StageExecutionTelemetry stageExecutionTelemetry() {
        if (type != Type.STAGE_REPORT && type != Type.DURABLE_STAGE_RECONCILED) {
            throw new IllegalStateException(type + " does not carry stage telemetry");
        }
        return commitPayload == null
                ? io.rowweave.plan.StageExecutionTelemetry.unavailable()
                : io.rowweave.plan.StageExecutionTelemetryCodec.decode(commitPayload);
    }
    /** Internal consumers decode the bounded backing in-place while its RPC lease is live. */
    public io.rowweave.plan.ExecutionTelemetrySummary executionTelemetrySummary() {
        if (type != Type.JOB_RESULT && type != Type.RUN_SNAPSHOT) {
            throw new IllegalStateException(type + " does not carry job telemetry");
        }
        if (commitPayload == null) {
            return io.rowweave.plan.ExecutionTelemetrySummary.unavailable();
        }
        try {
            return io.rowweave.plan.ExecutionTelemetrySummaryCodec.decode(
                    commitPayload, this::reserveResidentBytes);
        } catch (RuntimeException | LinkageError invalidTelemetry) {
            // Telemetry is advisory. A damaged extension cannot change the base
            // JOB_RESULT or RUN_SNAPSHOT decimal outcome already decoded above.
            return io.rowweave.plan.ExecutionTelemetrySummary.unavailable();
        }
    }
    /** Codec-only borrowed view; callers must never publish or mutate it. */
    byte[] borrowedCommitPayload() { return commitPayload; }
    /**
     * Only a durable assignment or reconciliation envelope may borrow the token backing.
     */
    public byte[] stageAssignmentTokenBytes() {
        if (type != Type.ASSIGN_STAGE && type != Type.ASSIGNMENT_ACCEPTED
                && type != Type.RECONCILE_DURABLE_STAGE) {
            throw new IllegalStateException(type + " does not carry a stage assignment token");
        }
        return commitPayload;
    }
    /**
     * Used only by ASSIGNMENT_ACCEPTED.
     */
    public byte[] stageCommandDigest() { return planGraphBytes; }
    /**
     * Used only by a durable stage report; a legacy report returns null.
     */
    public byte[] stageTerminalReceiptBytes() {
        return type == Type.STAGE_REPORT ? planGraphBytes : null;
    }
    /** independent outer field; never part of terminal receipt codec. */
    public byte[] preparedSinkReceiptBytes() {
        return (type == Type.STAGE_REPORT
                || type == Type.DURABLE_STAGE_RECONCILED)
                && stageCommandBlob != null
                ? stageCommandBlob.clone() : null;
    }
    public String reconciliationOutcome() { return commitOutcome; }
    public byte[] reconciledTerminalReceiptBytes() {
        return type == Type.DURABLE_STAGE_RECONCILED ? planGraphBytes : null;
    }
    public String commitDomainPath() { return commitDomainPath; }
    public String commitDomainId() { return commitDomainId; }
    public String commitOutcome() { return commitOutcome; }
    public io.rowweave.run.DurableRunDisposition durableRunDisposition() {
        if (type != Type.JOB_RESULT || commitOutcome == null) return null;
        return io.rowweave.run.DurableRunDisposition.valueOf(commitOutcome);
    }

    static boolean supportsExecutionTelemetry(Type type) {
        return type == Type.STAGE_REPORT || type == Type.DURABLE_STAGE_RECONCILED
                || type == Type.JOB_RESULT || type == Type.RUN_SNAPSHOT;
    }

    private static byte[] telemetryPayload(Type type, byte[] telemetryBytes) {
        if (!supportsExecutionTelemetry(type)) {
            throw new IllegalArgumentException(type + " cannot carry execution telemetry");
        }
        if (telemetryBytes == null) return null;
        int max = type == Type.STAGE_REPORT || type == Type.DURABLE_STAGE_RECONCILED
                ? StageExecutionTelemetryCodec.MAX_FRAME_BYTES
                : ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES;
        if (telemetryBytes.length > max) {
            throw new IllegalArgumentException("execution telemetry payload exceeds " + max + " bytes");
        }
        return telemetryBytes.clone();
    }

    public RunAttemptFence runAttemptFence() { return runAttemptFence; }

    public String runId() { return runId; }

    /**
     * null means the master applies the finite legacy request defaults.
     */
    public EphemeralSubmitTimeoutPolicy.Budget ephemeralSubmitTimeouts() {
        return ephemeralSubmitTimeouts;
    }

    /**
     * Used only by QUERY_RUN_EVENTS; stageId is reused so the older RPC frame layout is
     * unchanged.
     */
    public long eventAfterSequence() { return stageId; }

    /**
     * Used only by QUERY_RUN_EVENTS; slots is reused so the older RPC frame layout is
     * unchanged.
     */
    public int eventLimit() { return slots; }

    /**
     * Used only by a successful run events page; reuses the bounded planGraphBytes union.
     */
    public byte[] runEventPageBytes() {
        return type == Type.RUN_EVENTS_PAGE ? planGraphBytes : null;
    }

    /**
     * Used only by a successful quality gate decision.
     */
    public byte[] qualityGateDecisionBytes() {
        return type == Type.QUALITY_GATE_DECISION ? planGraphBytes() : null;
    }

    public byte[] incrementalFrontierViewBytes() {
        return type == Type.INCREMENTAL_FRONTIER_VIEW ? planGraphBytes() : null;
    }

    public byte[] incrementalSubmissionBytes() {
        return type == Type.SUBMIT_INCREMENTAL_RUN ? planGraphBytes() : null;
    }

    public byte[] incrementalInitializeBytes() {
        return type == Type.INITIALIZE_INCREMENTAL_FEED
                ? planGraphBytes() : null;
    }

    public byte[] incrementalSourceActionBytes() {
        return type == Type.EXECUTE_INCREMENTAL_SOURCE_ACTION
                ? planGraphBytes() : null;
    }

    public byte[] incrementalSourceActionResultBytes() {
        return type == Type.INCREMENTAL_SOURCE_ACTION_RESULT
                ? planGraphBytes() : null;
    }

    /**
     * Used only by CANCEL_RUN.
     */
    public String cancelRequestId() {
        return type == Type.CANCEL_RUN ? attemptId : null;
    }

    /**
     * Used only by a successful run cancellation result.
     */
    public String runCancellationDisposition() {
        return type == Type.RUN_CANCELLATION_RESULT ? commitOutcome : null;
    }

    /**
     * Used only by a successful run cancellation result.
     */
    public String runCancellationStatus() {
        return type == Type.RUN_CANCELLATION_RESULT ? commitDomainId : null;
    }

    /**
     * Used only by a successful run cancellation result.
     */
    public long runCancellationRevision() {
        return type == Type.RUN_CANCELLATION_RESULT ? rowCount : 0L;
    }

    public RpcSecurityMetadata securityMetadata() { return securityMetadata; }

    /**
     * For the internal asynchronous execution owner; an ordinary in-process message returns
     * null.
     */
    public ControlResidentLease residentLease() { return residentLease; }

    /**
     * Paired with the resident lease; after a handoff an older token may only close as stale,
     * idempotently.
     */
    public ControlResidentLease.OwnerToken residentOwnerToken() { return residentOwnerToken; }

    /**
     * The artifact, plan and object-stream materializers add to the current request lease
     * before each allocation.
     */
    public void reserveResidentBytes(long bytes) {
        if (residentLease != null) residentLease.reserve(residentOwnerToken, bytes);
    }

    /**
     * Called by the response producer before materializing bytes or a string; an in-process
     * transport treats it as a no-op.
     */
    public void reserveResponseResidentBytes(long bytes) {
        if (responseResidentLease != null) {
            responseResidentLease.reserve(responseResidentOwnerToken, bytes);
            responseResidentReservedBytes = Math.addExact(
                    responseResidentReservedBytes, bytes);
        }
    }

    long responseResidentReservedBytes() { return responseResidentReservedBytes; }

    /**
     * Linearized handoff from handler to asynchronous owner; the caller must do it before
     * publishing the new owner reference.
     */
    public ControlResidentLease.OwnerToken transferResidentOwner(ControlResidentOwner target) {
        if (residentLease == null) return null;
        ControlResidentLease.OwnerToken transferred = residentLease.transfer(residentOwnerToken, target);
        residentOwnerToken = transferred;
        return transferred;
    }

    void attachResidentLease(ControlResidentLease lease,
                             ControlResidentLease.OwnerToken ownerToken) {
        if (residentLease != null || lease == null || ownerToken == null) {
            throw new IllegalStateException("invalid RPC resident lease attachment");
        }
        this.residentLease = lease;
        this.residentOwnerToken = ownerToken;
    }

    void attachResponseResidentLease(ControlResidentLease lease,
                                     ControlResidentLease.OwnerToken ownerToken) {
        if (responseResidentLease != null || lease == null || ownerToken == null) {
            throw new IllegalStateException("invalid RPC response resident lease attachment");
        }
        this.responseResidentLease = lease;
        this.responseResidentOwnerToken = ownerToken;
    }

    boolean hasRetainedBinaryPayload() {
        return planGraphBytes != null || stageCommandBlob != null || commitPayload != null;
    }

    /**
     * Conservative resident bound on the client write side: retained arrays plus two strict
     * UTF-8 scratch passes.
     */
    long encodeResidentBytes() {
        long bytes = arrayLength(planGraphBytes) + arrayLength(stageCommandBlob)
                + arrayLength(commitPayload);
        bytes = addExact(bytes, stringScratch(workerId));
        bytes = addExact(bytes, stringScratch(host));
        bytes = addExact(bytes, stringScratch(errorMessage));
        bytes = addExact(bytes, stringScratch(incarnationId));
        bytes = addExact(bytes, stringScratch(commitGroupId));
        bytes = addExact(bytes, stringScratch(attemptId));
        bytes = addExact(bytes, stringScratch(commitDomainPath));
        bytes = addExact(bytes, stringScratch(commitDomainId));
        bytes = addExact(bytes, stringScratch(commitOutcome));
        bytes = addExact(bytes, stringScratch(runId));
        if (runAttemptFence != null) {
            bytes = addExact(bytes, stringScratch(runAttemptFence.runId()));
            bytes = addExact(bytes, stringScratch(runAttemptFence.attemptId()));
        }
        if (securityMetadata != null) {
            bytes = addExact(bytes, 128L); // two exact 64-byte ASCII set ids
        }
        if (dataPlaneRegistration != null) {
            bytes = addExact(bytes, dataPlaneRegistration.fingerprintLength());
        }
        return bytes;
    }

    private static long arrayLength(byte[] bytes) {
        return bytes == null ? 0L : bytes.length;
    }

    private static long stringScratch(String value) {
        return value == null ? 0L : 6L * value.length();
    }

    private static long addExact(long left, long right) {
        if (right > Long.MAX_VALUE - left) {
            throw new IllegalArgumentException("RPC encode resident estimate overflow");
        }
        return left + right;
    }

    /**
     * Returns an immutable copy of the message carrying the full secure suffix identity.
     */
    public RpcMessage withSecurityMetadata(RpcSecurityMetadata metadata) {
        return new RpcMessage(this, metadata);
    }

    RpcMessage withDataPlaneRegistration(DataPlaneRegistration registration) {
        return new RpcMessage(this, java.util.Objects.requireNonNull(
                registration, "data plane registration"));
    }

    RpcMessage withEphemeralSubmitTimeouts(EphemeralSubmitTimeoutPolicy.Budget timeouts) {
        return new RpcMessage(this, timeouts);
    }

    /**
     * The 64-bit leader term of an ASSIGN claim, rebuilt from the two existing ints.
     */
    public long leaderTermToken() {
        return ((long) slots << 32) | (dataPort & 0xffffffffL);
    }
}
