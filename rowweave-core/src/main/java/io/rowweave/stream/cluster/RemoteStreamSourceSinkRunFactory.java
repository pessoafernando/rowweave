package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceCoordinatorSplit;
import io.rowweave.stream.contract.StreamSourceCoordinatorState;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamCdcEnumeratorState;
import io.rowweave.stream.contract.StreamCdcEnumeratorStateCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;
import io.rowweave.stream.contract.StreamSourceCoordinatorStateCodec;
import io.rowweave.stream.contract.StreamSavepoint;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.runtime.FileCheckpointAuthority;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.FileOfflineRescaleAuthority;
import io.rowweave.stream.runtime.FileStreamCheckpointCatalog;
import io.rowweave.stream.runtime.BoundedEventTimeTemporalJoin;
import io.rowweave.stream.runtime.OfflineKeyedStateRescaler;
import io.rowweave.stream.runtime.StreamSavepointRestoreValidator;
import io.rowweave.stream.runtime.StreamCheckpointCoordinator;
import io.rowweave.stream.runtime.StreamCheckpointAuthority;
import io.rowweave.stream.runtime.StreamCheckpointCatalog;
import io.rowweave.stream.runtime.StreamSourcePositionCheckpoint;
import io.rowweave.stream.runtime.StreamSourcePositionMigrator;
import io.rowweave.stream.runtime.StreamSourceCoordinator;
import io.rowweave.stream.runtime.StaticStreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import io.rowweave.stream.api.StreamSplitSpec;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

/** Cluster run factory that places and runs every portable node/subtask. */
public final class RemoteStreamSourceSinkRunFactory
        implements StreamProductRunFactory {
    private static final int DATA_VERSION = 1;

    private final FileStreamClusterRunAuthority authority;
    private final long coordinatorTerm;
    private final String coordinatorIncarnation;
    private final StreamWorkerDirectory workerDirectory;
    private final StreamCheckpointAuthority checkpointAuthority;
    private final StreamCheckpointCatalog checkpointMemberStore;
    private final StreamCheckpointCatalog savepointStore;
    private final Path checkpointRoot;
    private final Object checkpointRetentionLock = new Object();
    private int checkpointCutsInFlight;

    public RemoteStreamSourceSinkRunFactory(
            Path authorityRoot, long coordinatorTerm,
            String coordinatorIncarnation,
            Collection<RemoteStreamWorkerEndpoint> endpoints) {
        this(authorityRoot, coordinatorTerm, coordinatorIncarnation,
                endpoints, null);
    }

    public RemoteStreamSourceSinkRunFactory(
            Path authorityRoot, long coordinatorTerm,
            String coordinatorIncarnation,
            Collection<RemoteStreamWorkerEndpoint> endpoints,
            Path checkpointRoot) {
        this(authorityRoot, coordinatorTerm, coordinatorIncarnation,
                fixedDirectory(endpoints), checkpointRoot);
    }

    public RemoteStreamSourceSinkRunFactory(
            Path authorityRoot, long coordinatorTerm,
            String coordinatorIncarnation,
            StreamWorkerDirectory workerDirectory,
            Path checkpointRoot) {
        this(new FileStreamClusterRunAuthority(authorityRoot),
                null, coordinatorTerm, coordinatorIncarnation,
                workerDirectory, checkpointRoot);
    }

    public RemoteStreamSourceSinkRunFactory(
            StreamClusterRunStateStore runStateStore,
            long coordinatorTerm, String coordinatorIncarnation,
            StreamWorkerDirectory workerDirectory,
            Path checkpointRoot) {
        this(new FileStreamClusterRunAuthority(runStateStore),
                null, coordinatorTerm, coordinatorIncarnation,
                workerDirectory, checkpointRoot);
    }

    public RemoteStreamSourceSinkRunFactory(
            StreamClusterRunStateStore runStateStore,
            StreamCheckpointAuthority checkpointAuthority,
            long coordinatorTerm, String coordinatorIncarnation,
            StreamWorkerDirectory workerDirectory,
            Path checkpointRoot) {
        this(runStateStore, checkpointAuthority, null, coordinatorTerm,
                coordinatorIncarnation, workerDirectory, checkpointRoot);
    }

    public RemoteStreamSourceSinkRunFactory(
            StreamClusterRunStateStore runStateStore,
            StreamCheckpointAuthority checkpointAuthority,
            StreamCheckpointCatalog checkpointCatalog,
            long coordinatorTerm, String coordinatorIncarnation,
            StreamWorkerDirectory workerDirectory,
            Path checkpointRoot) {
        this(new FileStreamClusterRunAuthority(runStateStore),
                checkpointAuthority, checkpointCatalog, coordinatorTerm,
                coordinatorIncarnation, workerDirectory, checkpointRoot);
    }

    private RemoteStreamSourceSinkRunFactory(
            FileStreamClusterRunAuthority authority,
            StreamCheckpointAuthority sharedCheckpointAuthority,
            long coordinatorTerm, String coordinatorIncarnation,
            StreamWorkerDirectory workerDirectory,
            Path checkpointRoot) {
        this(authority, sharedCheckpointAuthority, null, coordinatorTerm,
                coordinatorIncarnation, workerDirectory, checkpointRoot);
    }

    private RemoteStreamSourceSinkRunFactory(
            FileStreamClusterRunAuthority authority,
            StreamCheckpointAuthority sharedCheckpointAuthority,
            StreamCheckpointCatalog sharedCheckpointCatalog,
            long coordinatorTerm, String coordinatorIncarnation,
            StreamWorkerDirectory workerDirectory,
            Path checkpointRoot) {
        if (coordinatorTerm <= 0L || coordinatorIncarnation == null
                || coordinatorIncarnation.isEmpty()
                || workerDirectory == null || authority == null) {
            throw new IllegalArgumentException(
                    "Stream coordinator identity and Worker directory are required");
        }
        this.authority = authority;
        this.coordinatorTerm = coordinatorTerm;
        this.coordinatorIncarnation = StreamControlMessage.text(
                coordinatorIncarnation, "coordinator incarnation");
        this.workerDirectory = workerDirectory;
        this.checkpointAuthority = sharedCheckpointAuthority != null
                ? sharedCheckpointAuthority
                : checkpointRoot == null ? null
                : new FileCheckpointAuthority(
                checkpointRoot.resolve("authority"));
        StreamCheckpointCatalog catalog = sharedCheckpointCatalog != null
                ? sharedCheckpointCatalog
                : checkpointRoot == null ? null
                : new FileStreamCheckpointCatalog(checkpointRoot);
        this.checkpointMemberStore = catalog;
        this.savepointStore = catalog;
        this.checkpointRoot = checkpointRoot == null ? null
                : checkpointRoot.toAbsolutePath().normalize();
    }

    @Override
    public StreamProductRun create(StreamProductJobSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("Stream product job is required");
        }
        String runId = snapshot.jobId().value();
        boolean existing = authority.exists(runId);
        StreamProductExecutionGraph admitted;
        StreamClusterRunState state;
        if (existing) {
            state = authorityState(runId, null);
            admitted = admittedForState(snapshot, state);
        } else {
            admitted = StreamProductExecutionGraph.from(snapshot.artifact());
            state = authorityState(runId, place(admitted).value);
        }
        requireDurableCheckpointAuthority(admitted);
        CheckpointIdentity restoreCheckpoint = restoreCheckpoint(
                snapshot, admitted);
        if (existing) {
            reconcileBeforeRestart(snapshot, admitted, state);
            state = authority.current(runId);
            admitted = admittedForState(snapshot, state);
        }
        if (existing && state.latestCompletedCheckpoint() != null) {
            restoreCheckpoint = state.latestCompletedCheckpoint();
        }
        return run(admitted, state, restoreCheckpoint, 1L);
    }

    @Override
    public StreamProductRun restart(
            StreamProductJobSnapshot snapshot, Throwable failure) {
        if (snapshot == null
                || snapshot.status()
                != io.rowweave.stream.api.StreamJobStatus.RESTARTING) {
            throw new IllegalArgumentException(
                    "restarting Stream product job is required");
        }
        String runId = snapshot.jobId().value();
        StreamClusterRunState current = authority.current(runId);
        StreamProductExecutionGraph admitted =
                admittedForState(snapshot, current);
        requireDurableCheckpointAuthority(admitted);
        StreamWorkerLossException loss = failure
                instanceof StreamWorkerLossException
                ? workerLoss(failure, current) : null;
        reconcileBeforeRestart(snapshot, admitted, current);
        current = authority.current(runId);
        admitted = admittedForState(snapshot, current);
        if (current.latestCompletedCheckpoint() == null) {
            throw new IllegalStateException(
                    "Stream failure has no completed checkpoint to restore");
        }
        Placement placement = loss == null
                ? place(admitted)
                : place(admitted,
                        loss.workerId(), loss.workerIncarnation());
        long generation = Math.addExact(
                current.assignmentGeneration(), 1L);
        StreamClusterRunState restarted;
        if (loss == null) {
            restarted = authority.restartAfterFailure(
                    runId, current.coordinatorTerm(),
                    current.coordinatorIncarnation(),
                    "attempt-" + generation,
                    Math.addExact(current.fenceToken(), 1L), generation,
                    Math.addExact(current.channelEpoch(), 1L),
                    placement.value);
        } else {
            restarted = authority.restartAfterWorkerLoss(
                    runId, current.coordinatorTerm(),
                    current.coordinatorIncarnation(), loss.workerId(),
                    loss.workerIncarnation(), "attempt-" + generation,
                    Math.addExact(current.fenceToken(), 1L), generation,
                    Math.addExact(current.channelEpoch(), 1L),
                    placement.value);
        }
        return run(admitted, restarted,
                restarted.latestCompletedCheckpoint(), restartSequence(
                        snapshot.checkpoints().size(),
                        snapshot.progress().restarts()));
    }

    @Override
    public StreamProductDecisionConvergence convergeDurableDecision(
            StreamProductJobSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("Stream product job is required");
        }
        if (checkpointAuthority == null) return null;
        StreamClusterRunState current = authority.current(
                snapshot.jobId().value());
        StreamProductExecutionGraph admitted =
                admittedForState(snapshot, current);
        return reconcileBeforeRestart(snapshot, admitted, current);
    }

    private static StreamProductExecutionGraph admittedForState(
            StreamProductJobSnapshot snapshot,
            StreamClusterRunState state) {
        StreamProductRescaleOperation effective = null;
        for (StreamProductRescaleOperation operation : snapshot.rescales()) {
            boolean cutOver = operation.phase()
                    == StreamProductRescaleOperation.Phase.CUTOVER_DURABLE
                    || operation.phase()
                    == StreamProductRescaleOperation.Phase.CUTOVER_COMPLETE;
            if (cutOver && operation.targetGeneration()
                    <= state.assignmentGeneration()) {
                effective = operation;
            }
        }
        if (effective == null) {
            return StreamProductExecutionGraph.from(snapshot.artifact());
        }
        java.util.LinkedHashMap<String, Integer> nodeParallelism =
                new java.util.LinkedHashMap<>();
        for (StreamWorkerPlacement.Assignment assignment
                : state.placement().assignments()) {
            int parallelism = Math.addExact(
                    assignment.subtaskIndex(), 1);
            Integer previous = nodeParallelism.get(
                    assignment.operatorUid());
            if (previous == null || parallelism > previous) {
                nodeParallelism.put(assignment.operatorUid(), parallelism);
            }
        }
        return StreamProductExecutionGraph.from(snapshot.artifact(),
                effective.targetParallelism(), nodeParallelism);
    }

    private StreamProductDecisionConvergence reconcileBeforeRestart(
            StreamProductJobSnapshot snapshot,
            StreamProductExecutionGraph admitted,
            StreamClusterRunState current) {
        synchronized (checkpointRetentionLock) {
            return reconcileBeforeRestartExclusive(snapshot, admitted, current);
        }
    }

    private StreamProductDecisionConvergence reconcileBeforeRestartExclusive(
            StreamProductJobSnapshot snapshot,
            StreamProductExecutionGraph admitted,
            StreamClusterRunState current) {
        if (checkpointAuthority == null) return null;
        String referenceId = pendingReference(snapshot);
        StreamSavepoint existing = referenceId == null
                ? null : savepointStore.get(referenceId);
        if (existing != null) {
            CheckpointIdentity identity = existing.checkpointManifest()
                    .checkpointIdentity();
            if (!current.runId().equals(identity.runId())) {
                throw new IllegalStateException(
                        "Stream operation reference belongs to another run");
            }
            CheckpointDecision completed = checkpointAuthority.decision(identity);
            if (completed.status() != CheckpointDecision.Status.COMPLETED) {
                throw new CheckpointCommitPendingException(
                        "Stream operation reference is not fully committed",
                        new IllegalStateException("checkpoint remains pending"));
            }
            recordCompletedCheckpointIfMissing(current, completed);
            retainCompletedCheckpointsIfIdleExclusive(
                    checkpointPayloadStore(admitted),
                    current.runId(), admitted.job.checkpointConfig()
                            .retainedCompleted());
            return new StreamProductDecisionConvergence(referenceId,
                    completedCdcPhase(admitted, completed,
                            snapshot.progress().cdcPhase()));
        }
        CheckpointDecision pending = unpublishedDecision(current);
        if (pending == null) return null;
        if (referenceId == null) {
            throw new CheckpointCommitPendingException(
                    "durable Stream decision has no public operation reference",
                    new IllegalStateException(
                            "checkpoint operation identity is unavailable"));
        }
        CheckpointIdentity checkpoint = pending.checkpointIdentity();
        try {
            CheckpointDecision completed = pending;
            if (pending.status()
                    == CheckpointDecision.Status.COMMIT_DECIDED) {
                byte[] artifact = PlanArtifactCodec.encode(snapshot.artifact());
                int timeoutMillis = (int) Math.min(Integer.MAX_VALUE,
                        admitted.job.checkpointConfig().timeoutMillis());
                long sequence = Math.addExact(
                        checkpoint.checkpointSequence(), 1L);
                CheckpointTransferProof checkpointProof =
                        checkpointTransferProof(
                        pending, checkpointAuthority,
                        checkpointMemberStore);
                for (StreamWorkerPlacement.Assignment assignment
                        : current.placement().assignments()) {
                    PortableStreamNode node = admitted.executionGraph
                            .node(assignment.operatorUid()).portableNode();
                    if (node.opcode() != StreamOperatorCode.SINK
                            || ((StreamSinkBinding) node.binding())
                            .visibilityProfile()
                            != StreamSinkBinding.VisibilityProfile
                            .CHECKPOINT_TRANSACTIONAL) {
                        continue;
                    }
                    RemoteStreamWorkerEndpoint recovery = recoveryEndpoint(
                            (StreamSinkBinding) node.binding());
                    recovery.recoverCheckpoint(recoveryCheckpointCommand(
                                    current, recovery, admitted.commandDigest,
                                    sequence, checkpoint), artifact,
                            assignment.operatorUid(),
                            assignment.subtaskIndex(), timeoutMillis,
                            checkpointProof.forTask(assignment));
                }
                completed = checkpointAuthority.complete(
                        CheckpointDecision.completed(pending));
            }
            savepointStore.create(referenceId, checkpoint,
                    checkpointAuthority);
            recordCompletedCheckpointIfMissing(current, completed);
            retainCompletedCheckpointsIfIdleExclusive(
                    checkpointPayloadStore(admitted),
                    current.runId(), admitted.job.checkpointConfig()
                            .retainedCompleted());
            return new StreamProductDecisionConvergence(referenceId,
                    completedCdcPhase(admitted, completed,
                            snapshot.progress().cdcPhase()));
        } catch (Throwable failure) {
            throw new CheckpointCommitPendingException(
                    "old Stream attempt decision must converge before restart",
                    failure);
        }
    }

    private CheckpointDecision unpublishedDecision(
            StreamClusterRunState current) {
        long recorded = current.latestCompletedCheckpoint() == null
                ? 0L : current.latestCompletedCheckpoint()
                .checkpointSequence();
        CheckpointDecision pending = null;
        for (CheckpointDecision decision
                : checkpointAuthority.decisionsForRun(current.runId())) {
            if (decision.checkpointIdentity().checkpointSequence() <= recorded
                    || (decision.status()
                    != CheckpointDecision.Status.COMMIT_DECIDED
                    && decision.status()
                    != CheckpointDecision.Status.COMPLETED)) {
                continue;
            }
            if (pending != null) {
                throw new CheckpointCommitPendingException(
                        "multiple old Stream decisions require reconciliation",
                        new IllegalStateException(
                                "restart cannot choose between durable cuts"));
            }
            pending = decision;
        }
        return pending;
    }

    private void recordCompletedCheckpointIfMissing(
            StreamClusterRunState current, CheckpointDecision completed) {
        CheckpointIdentity checkpoint = completed.checkpointIdentity();
        CheckpointIdentity recorded = authority.current(current.runId())
                .latestCompletedCheckpoint();
        if (recorded != null
                && recorded.checkpointSequence()
                >= checkpoint.checkpointSequence()) {
            if (recorded.checkpointSequence()
                    == checkpoint.checkpointSequence()
                    && !recorded.equals(checkpoint)) {
                throw new IllegalStateException(
                        "Stream run pointer conflicts with its operation reference");
            }
            return;
        }
        authority.recordCompletedCheckpoint(current.runId(),
                current.coordinatorTerm(),
                current.coordinatorIncarnation(), completed);
    }

    private StreamCdcPhase completedCdcPhase(
            StreamProductExecutionGraph admitted,
            CheckpointDecision completed, StreamCdcPhase fallback) {
        if (admitted.residentCdcSources().isEmpty()) return fallback;
        if (checkpointPayloadStoreUnavailable(admitted)) {
            throw new IllegalStateException(
                    "resident CDC progress requires checkpoint storage");
        }
        FileCheckpointPayloadStore payloadStore =
                new FileCheckpointPayloadStore(checkpointRoot,
                        admitted.job.resourceProfile().checkpointBytes());
        List<StreamCheckpointMember> members = checkpointMemberStore.resolve(
                completed, checkpointAuthority);
        StreamCdcPhase phase = StreamCdcPhase.LOG;
        for (StreamProductExecutionGraph.SourceRoute source
                : admitted.residentCdcSources()) {
            StreamCheckpointMember sourcePosition = null;
            for (StreamCheckpointMember member : members) {
                if (member.kind()
                        == StreamCheckpointMember.Kind.SOURCE_POSITION
                        && source.node.operatorUid().equals(
                        member.operatorUid())
                        && "STREAM_CDC_ENUMERATOR_V1".equals(
                        member.codecId())) {
                    if (sourcePosition != null) {
                        throw new IllegalStateException(
                                "resident CDC checkpoint has multiple positions");
                    }
                    sourcePosition = member;
                }
            }
            if (sourcePosition == null) {
                throw new IllegalStateException(
                        "resident CDC checkpoint has no source position");
            }
            StreamSourcePositionCheckpoint position =
                    new StreamSourcePositionCheckpoint(
                            admitted.artifact.artifactId(),
                            source.node.operatorUid(), 0, source.binding,
                            source.binding.fixedSplits().get(0),
                            checkpointRoot.resolve("cdc-progress"),
                            payloadStore, 0L,
                            source.binding.fixedSplits().get(0)
                                    .descriptorFrame());
            position.restore(sourcePosition);
            StreamCdcEnumeratorState state = StreamCdcEnumeratorStateCodec
                    .decode(position.currentPositionFrame());
            if (state.phase() != StreamCdcEnumeratorState.Phase.LOG) {
                phase = StreamCdcPhase.SNAPSHOT;
            }
        }
        return phase;
    }

    private boolean checkpointPayloadStoreUnavailable(
            StreamProductExecutionGraph admitted) {
        return checkpointRoot == null || checkpointMemberStore == null
                || checkpointAuthority == null
                || admitted.job.resourceProfile().checkpointBytes() <= 0L;
    }

    private static String pendingReference(StreamProductJobSnapshot snapshot) {
        String reference = null;
        for (StreamProductCheckpointOperation operation
                : snapshot.checkpoints()) {
            if (operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                    || operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.RUNNING) {
                if (reference != null) {
                    throw new IllegalStateException(
                            "multiple public Stream cuts require reconciliation");
                }
                reference = operation.checkpointId();
            }
        }
        StreamProductSavepointOperation savepoint = snapshot.savepointOrNull();
        if (savepoint != null
                && (savepoint.status()
                == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                || savepoint.status()
                == io.rowweave.stream.api.StreamOperationStatus.RUNNING)) {
            if (reference != null) {
                throw new IllegalStateException(
                        "checkpoint and savepoint cannot both be in flight");
            }
            reference = savepoint.savepointId();
        }
        return reference;
    }

    private RemoteStreamWorkerEndpoint recoveryEndpoint(
            StreamSinkBinding binding) {
        RuntimeException first = null;
        for (RemoteStreamWorkerEndpoint endpoint : endpoints()) {
            if (!endpoint.supportsSink(binding.providerKey(),
                    binding.providerContractDigest())) continue;
            try {
                endpoint.requireReachable();
                return endpoint;
            } catch (RuntimeException unavailable) {
                if (first == null) first = unavailable;
                else first.addSuppressed(unavailable);
            }
        }
        throw new IllegalStateException(
                "transactional Sink recovery provider is unavailable", first);
    }

    private static byte[] recoveryCheckpointCommand(
            StreamClusterRunState state,
            RemoteStreamWorkerEndpoint endpoint, String digest,
            long sequence, CheckpointIdentity checkpoint) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.CHECKPOINT_TRIGGER,
                state.coordinatorTerm(), state.coordinatorIncarnation(),
                state.runId(), checkpoint.attemptId(),
                checkpoint.fenceToken(), state.assignmentGeneration(),
                endpoint.profile().workerIncarnation(), sequence, digest,
                StreamCheckpointControlCodec.encode(checkpoint)));
    }

    @Override
    public void retire(StreamProductJobSnapshot snapshot) {
        if (snapshot == null || !snapshot.status().terminal()) {
            throw new IllegalArgumentException(
                    "terminal Stream product job is required");
        }
        StreamClusterRunState current = authority.current(
                snapshot.jobId().value());
        if (checkpointAuthority != null
                && unpublishedDecision(current) != null) {
            throw new CheckpointCommitPendingException(
                    "terminal Stream run still owns an unpublished decision",
                    new IllegalStateException("checkpoint remains pending"));
        }
        authority.retire(snapshot.jobId().value(), coordinatorTerm,
                coordinatorIncarnation);
    }

    private StreamProductRun run(StreamProductExecutionGraph admitted,
            StreamClusterRunState state,
            CheckpointIdentity restoreCheckpoint,
            long launchSequence) {
        StreamCheckpointTransferBudget.validate(
                admitted, state.placement());
        Map<String, RemoteStreamWorkerEndpoint> workers = new LinkedHashMap<>();
        for (StreamWorkerPlacement.Assignment assignment
                : state.placement().assignments()) {
            workers.put(assignment.workerId(), exactEndpoint(assignment));
        }
        for (StreamProductExecutionGraph.SinkRoute route : admitted.sinks) {
            RemoteStreamWorkerEndpoint sink = exactEndpoint(assignmentFor(
                    state.placement(), route.node.operatorUid(), 0));
            if (!sink.supportsSink(route.binding.providerKey(),
                    route.binding.providerContractDigest())) {
                throw new IllegalStateException(
                        "durable Stream placement no longer provides a bound Sink");
            }
        }
        for (StreamProductExecutionGraph.SourceRoute route
                : admitted.sources) {
            RemoteStreamWorkerEndpoint owner = exactEndpoint(assignmentFor(
                    state.placement(), route.node.operatorUid(), 0));
            if (!owner.supportsSource(route.binding.providerKey(),
                    route.binding.providerContractDigest())) {
                throw new IllegalStateException(
                        "durable Stream placement no longer provides a bound source");
            }
        }
        return new RemoteRun(this, admitted,
                PlanArtifactCodec.encode(admitted.artifact),
                state, authority, checkpointAuthority,
                checkpointMemberStore, savepointStore, restoreCheckpoint,
                checkpointRoot, launchSequence, workers);
    }

    private void requireDurableCheckpointAuthority(
            StreamProductExecutionGraph admitted) {
        if (admitted.job.plan().deliveryGuarantee()
                == io.rowweave.stream.contract.PortableStreamPlan
                .DeliveryGuarantee.EXACTLY_ONCE
                && (checkpointAuthority == null
                || checkpointMemberStore == null
                || savepointStore == null
                || checkpointRoot == null)) {
            throw new IllegalStateException(
                    "exactly-once Stream execution requires durable "
                            + "checkpoint authority");
        }
    }

    private void beginCheckpointCut() {
        synchronized (checkpointRetentionLock) {
            checkpointCutsInFlight = Math.addExact(
                    checkpointCutsInFlight, 1);
        }
    }

    private void finishCheckpointCut(FileCheckpointPayloadStore payloads,
            String runId, int retainedCompleted, Throwable primary) {
        try {
            synchronized (checkpointRetentionLock) {
                if (checkpointCutsInFlight < 1) {
                    throw new IllegalStateException(
                            "Stream checkpoint cut accounting underflow");
                }
                checkpointCutsInFlight--;
                retainCompletedCheckpointsIfIdleExclusive(
                        payloads, runId, retainedCompleted);
            }
        } catch (RuntimeException | Error cleanup) {
            if (primary != null) {
                primary.addSuppressed(cleanup);
                return;
            }
            throw new CheckpointCommitPendingException(
                    "durable Stream checkpoint retention requires reconciliation",
                    cleanup);
        }
    }

    private void retainCompletedCheckpointsIfIdleExclusive(
            FileCheckpointPayloadStore payloads,
            String runId, int retainedCompleted) {
        if (checkpointCutsInFlight != 0
                || payloads == null
                || checkpointMemberStore == null
                || checkpointAuthority == null) {
            return;
        }
        new io.rowweave.stream.runtime.FileCheckpointRetention(
                checkpointAuthority, checkpointMemberStore,
                payloads,
                authority.completedCheckpointRoots(),
                authority.runIds()).retainLatest(runId, retainedCompleted);
    }

    private FileCheckpointPayloadStore checkpointPayloadStore(
            StreamProductExecutionGraph admitted) {
        return checkpointRoot == null
                || admitted.job.resourceProfile().checkpointBytes() <= 0L
                ? null : new FileCheckpointPayloadStore(
                checkpointRoot,
                admitted.job.resourceProfile().checkpointBytes());
    }

    private StreamWorkerLossException workerLoss(
            Throwable failure, StreamClusterRunState current) {
        if (failure instanceof StreamWorkerLossException) {
            return (StreamWorkerLossException) failure;
        }
        for (int index = 0;
                index < current.placement().assignments().size(); index++) {
            StreamWorkerPlacement.Assignment assignment =
                    current.placement().assignments().get(index);
            try {
                RemoteStreamWorkerEndpoint endpoint = exactEndpoint(assignment);
                endpoint.requireReachable();
            } catch (RuntimeException unavailable) {
                return new StreamWorkerLossException(
                        assignment, unavailable);
            }
        }
        throw new IllegalStateException(
                "restarting Stream job has no unavailable assigned Worker");
    }

    private static long restartSequence(
            int checkpointCommands, int completedRestarts) {
        return Math.addExact(Math.addExact(1L, checkpointCommands),
                Math.multiplyExact(2L, completedRestarts));
    }

    private CheckpointIdentity restoreCheckpoint(
            StreamProductJobSnapshot snapshot,
            StreamProductExecutionGraph admitted) {
        StreamProductRecovery recovery = snapshot.recoveryOrNull();
        if (recovery == null) return null;
        if (checkpointAuthority == null || checkpointMemberStore == null
                || savepointStore == null) {
            throw new UnsupportedOperationException(
                    "requested Stream recovery point is unavailable");
        }
        io.rowweave.stream.contract.StreamSavepoint savepoint =
                savepointStore.get(recovery.recoveryPointId().value());
        if (savepoint == null) {
            throw new IllegalStateException(
                    "requested Stream savepoint does not exist");
        }
        if (recovery.offlineRescale()) {
            return offlineRescaleCheckpoint(
                    snapshot, admitted, recovery, savepoint);
        }
        StreamSavepointRestoreValidator.validateExact(
                savepoint, snapshot.artifact());
        CheckpointIdentity checkpoint =
                savepoint.checkpointManifest().checkpointIdentity();
        io.rowweave.stream.contract.CheckpointDecision decision =
                checkpointAuthority.decision(checkpoint);
        checkpointMemberStore.resolve(decision, checkpointAuthority);
        return checkpoint;
    }

    private CheckpointIdentity offlineRescaleCheckpoint(
            StreamProductJobSnapshot snapshot,
            StreamProductExecutionGraph admitted,
            StreamProductRecovery recovery,
            io.rowweave.stream.contract.StreamSavepoint savepoint) {
        List<PortableStreamNode> stateNodes = offlineRescaleNodes(admitted);
        List<StreamProductExecutionGraph.SourceRoute> sources =
                offlineRescaleSources(admitted);
        if (recovery.recoveryPointId().kind()
                != io.rowweave.stream.api.StreamRecoveryPointId.Kind.SAVEPOINT
                || stateNodes.isEmpty()) {
            throw new UnsupportedOperationException(
                    "offline rescale requires rescalable keyed state");
        }
        CheckpointIdentity sourceCheckpoint = savepoint.checkpointManifest()
                .checkpointIdentity();
        CheckpointDecision sourceDecision = checkpointAuthority.decision(
                sourceCheckpoint);
        List<StreamCheckpointMember> sourceMembers =
                checkpointMemberStore.resolve(
                        sourceDecision, checkpointAuthority);
        List<StreamCheckpointMember> sourceState = new ArrayList<>();
        Map<String, Map<String, StreamCheckpointMember>> sourcePositions =
                new LinkedHashMap<>();
        int sourcePositionCount = 0;
        for (StreamProductExecutionGraph.SourceRoute source : sources) {
            Map<String, StreamCheckpointMember> positions =
                    new LinkedHashMap<>();
            for (StreamSourceSplit split : source.binding.fixedSplits()) {
                positions.put(split.splitId(), null);
                sourcePositionCount = Math.addExact(sourcePositionCount, 1);
            }
            sourcePositions.put(source.node.operatorUid(), positions);
        }
        for (StreamCheckpointMember member : sourceMembers) {
            if (member.kind() == StreamCheckpointMember.Kind.STATE
                    && containsOperator(stateNodes, member.operatorUid())) {
                sourceState.add(member);
            } else if (member.kind()
                    == StreamCheckpointMember.Kind.SOURCE_POSITION
                    && sourcePositions.containsKey(member.operatorUid())) {
                Map<String, StreamCheckpointMember> positions =
                        sourcePositions.get(member.operatorUid());
                if (!positions.containsKey(member.subjectId())) continue;
                if (positions.get(member.subjectId()) != null) {
                    throw new IllegalStateException(
                            "offline rescale source position is ambiguous: "
                                    + member.operatorUid() + '/'
                                    + member.subjectId());
                }
                positions.put(member.subjectId(), member);
            }
        }
        for (Map.Entry<String, Map<String, StreamCheckpointMember>> source
                : sourcePositions.entrySet()) {
            for (Map.Entry<String, StreamCheckpointMember> position
                    : source.getValue().entrySet()) {
                if (position.getValue() == null) {
                    throw new IllegalStateException(
                            "offline rescale source position is missing: "
                                    + source.getKey() + '/'
                                    + position.getKey());
                }
            }
        }
        FileCheckpointPayloadStore payloadStore =
                new FileCheckpointPayloadStore(checkpointRoot,
                        admitted.job.resourceProfile().checkpointBytes());
        long taskLocalSourceCount = 0L;
        long taskLocalTargetCount = 0L;
        for (StreamExecutionGraph.Node graphNode
                : admitted.executionGraph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            for (StreamOperatorDefinitionRegistry.TaskLocalStateContract
                    contract : StreamOperatorDefinitionRegistry.definition(
                    node.opcode()).taskLocalStateContracts(node)) {
                List<StreamCheckpointMember> selected =
                        StreamLiveTaskLocalStateRescaler.select(
                                contract, node, sourceMembers);
                if (selected.isEmpty()) {
                    throw new IllegalStateException(
                            "offline rescale checkpoint is missing task-local state "
                                    + node.operatorUid() + '/' + contract);
                }
                taskLocalSourceCount = Math.addExact(
                        taskLocalSourceCount, selected.size());
                taskLocalTargetCount = Math.addExact(
                        taskLocalTargetCount, graphNode.parallelism());
            }
        }
        long stagingBytes;
        try {
            stagingBytes = Math.multiplyExact(
                    admitted.job.resourceProfile().checkpointBytes(),
                    Math.addExact(Math.addExact(
                            Math.addExact(sourceState.size(),
                                    admitted.plan.parallelism()),
                            sourcePositionCount),
                            Math.addExact(taskLocalSourceCount,
                                    taskLocalTargetCount)));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "offline rescale staging bound overflow", overflow);
        }
        List<OfflineKeyedStateRescaler.Result> results = new ArrayList<>();
        OfflineKeyedStateRescaler rescaler = new OfflineKeyedStateRescaler(
                checkpointRoot.resolve("offline-rescale-working"),
                stagingBytes);
        for (PortableStreamNode stateNode : stateNodes) {
            for (StreamStateDescriptor descriptor
                    : offlineRescaleDescriptors(stateNode)) {
                List<StreamCheckpointMember> selected = new ArrayList<>();
                for (StreamCheckpointMember member : sourceState) {
                    if (stateNode.operatorUid().equals(member.operatorUid())
                            && descriptor.stateName().equals(
                            member.subjectId())) {
                        selected.add(member);
                    }
                }
                results.add(rescaler.rescale(sourceCheckpoint.artifactId(),
                        admitted.artifact.artifactId(),
                        stateNode.operatorUid(), descriptor,
                        admitted.plan.maxParallelism(),
                        admitted.plan.parallelism(), selected,
                        payloadStore, payloadStore));
            }
        }
        List<StreamCheckpointMember> migratedTaskLocal = new ArrayList<>();
        for (StreamExecutionGraph.Node graphNode
                : admitted.executionGraph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            for (StreamOperatorDefinitionRegistry.TaskLocalStateContract
                    contract : StreamOperatorDefinitionRegistry.definition(
                    node.opcode()).taskLocalStateContracts(node)) {
                migratedTaskLocal.addAll(
                        StreamLiveTaskLocalStateRescaler.rescale(
                                contract, node,
                                graphNode.maximumParallelism(),
                                graphNode.parallelism(),
                                graphNode.inputEdges().isEmpty() ? null
                                        : StreamEdgePartitioning.outputSchema(
                                        admitted.executionGraph,
                                        graphNode.inputEdges().get(0)),
                                StreamLiveTaskLocalStateRescaler.select(
                                        contract, node, sourceMembers),
                                payloadStore));
            }
        }
        FileOfflineRescaleAuthority rescaleAuthority =
                new FileOfflineRescaleAuthority(
                        checkpointRoot.resolve("offline-rescale-authority"),
                        payloadStore);
        String rescaleId = snapshot.jobId().value();
        FileOfflineRescaleAuthority.CompletedRescale active =
                rescaleAuthority.active();
        long generation = active != null
                && active.rescaleId().equals(rescaleId)
                ? active.generation()
                : active == null ? 1L
                : Math.addExact(active.generation(), 1L);
        rescaleAuthority.cutover(rescaleId, generation,
                savepoint.checkpointManifest().manifestDigest(),
                admitted.artifact.artifactId(),
                admitted.plan.maxParallelism(),
                admitted.plan.parallelism(), results);
        List<StreamCheckpointMember> targetMembers = new ArrayList<>();
        for (StreamProductExecutionGraph.SourceRoute source : sources) {
            List<List<StreamSourceSplit>> splitOwners =
                    StreamSourceSplitAssigner.assign(source.binding,
                            admitted.executionGraph.node(
                                    source.node.operatorUid()).parallelism());
            for (int subtask = 0; subtask < splitOwners.size(); subtask++) {
                for (StreamSourceSplit split : splitOwners.get(subtask)) {
                    targetMembers.add(StreamSourcePositionMigrator.migrate(
                            sourceCheckpoint.artifactId(),
                            admitted.artifact.artifactId(),
                            source.node.operatorUid(), subtask,
                            source.binding, split,
                            sourcePositions.get(source.node.operatorUid())
                                    .get(split.splitId()),
                            payloadStore,
                            checkpointRoot.resolve(
                                    "offline-rescale-working")));
                }
            }
        }
        for (OfflineKeyedStateRescaler.Result result : results) {
            targetMembers.addAll(result.targetMembers());
        }
        targetMembers.addAll(migratedTaskLocal);
        CheckpointIdentity targetCheckpoint = targetCheckpoint(
                snapshot, admitted, sourceCheckpoint);
        publishSyntheticCheckpoint(targetCheckpoint, targetMembers);
        return targetCheckpoint;
    }

    private CheckpointIdentity liveRescaleCheckpoint(
            StreamProductExecutionGraph source,
            StreamWorkerPlacement sourcePlacement,
            StreamProductExecutionGraph target,
            CheckpointIdentity sourceCheckpoint,
            String targetAttemptId, long targetFenceToken,
            long targetAssignmentGeneration, long targetChannelEpoch,
            StreamWorkerPlacement targetPlacement) {
        if (checkpointAuthority == null || checkpointMemberStore == null
                || checkpointRoot == null) {
            throw new UnsupportedOperationException(
                    "live rescale requires durable checkpoint storage");
        }
        CheckpointDecision sourceDecision = checkpointAuthority.decision(
                sourceCheckpoint);
        List<StreamCheckpointMember> sourceMembers =
                checkpointMemberStore.resolve(
                        sourceDecision, checkpointAuthority);
        FileCheckpointPayloadStore payloadStore =
                new FileCheckpointPayloadStore(checkpointRoot,
                        target.job.resourceProfile().checkpointBytes());
        List<StreamCheckpointMember> targetMembers =
                new ArrayList<>(sourceMembers);
        targetMembers.removeIf(member -> member.kind()
                == StreamCheckpointMember.Kind.TRANSACTIONAL_SINK);
        CheckpointIdentity targetCheckpoint = new CheckpointIdentity(
                target.artifact.definitionId(),
                target.artifact.artifactId(), sourceCheckpoint.runId(),
                targetAttemptId, targetFenceToken, coordinatorTerm,
                coordinatorIncarnation,
                sourceCheckpoint.checkpointSequence(),
                target.commandDigest,
                sourceCheckpoint.deploymentDigest(),
                sourceCheckpoint.dataProtocolVersion(),
                sourceCheckpoint.stateFormatVersion());
        long stagingBytes;
        try {
            stagingBytes = Math.multiplyExact(
                    target.job.resourceProfile().checkpointBytes(),
                    (long) Math.addExact(sourceMembers.size(),
                            target.plan.parallelism()));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "live rescale staging bound overflow", overflow);
        }
        OfflineKeyedStateRescaler rescaler = new OfflineKeyedStateRescaler(
                checkpointRoot.resolve("live-rescale-working"),
                stagingBytes);
        for (StreamExecutionGraph.Node graphNode
                : target.executionGraph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            StreamOperatorDefinitionRegistry.Definition definition =
                    StreamOperatorDefinitionRegistry.definition(
                            node.opcode());
            List<StreamStateDescriptor> descriptors =
                    definition.keyedStateDescriptors(node);
            if (definition.keyedStateMigration()
                    == StreamOperatorDefinitionRegistry
                    .KeyedStateMigrationContract
                    .TEMPORAL_JOIN_SNAPSHOT) {
                List<StreamCheckpointMember> selected =
                        selectStateMembers(node, descriptors, sourceMembers);
                targetMembers.removeAll(selected);
                long entryCharge = Math.addExact(
                        graphNode.resourceRequest().maximumFrameBytes(),
                        256L);
                int maximumEntries = (int) Math.max(1L, Math.min(
                        Integer.MAX_VALUE,
                        graphNode.resourceRequest().maximumResidentBytes()
                                / entryCharge));
                targetMembers.addAll(
                        BoundedEventTimeTemporalJoin.rescaleSnapshots(
                                node.operatorUid(),
                                Long.parseLong(node.scalarParameters().get(
                                        "join.lowerBoundMillis")),
                                Long.parseLong(node.scalarParameters().get(
                                        "join.upperBoundMillis")),
                                graphNode.maximumParallelism(),
                                graphNode.parallelism(), maximumEntries,
                                graphNode.resourceRequest().spillDiskBytes(),
                                graphNode.resourceRequest().maximumFrameBytes(),
                                graphNode.resourceRequest().maximumFrameBytes(),
                                descriptors, selected, payloadStore,
                                checkpointRoot.resolve(
                                        "live-rescale-working"),
                                stagingBytes));
            } else {
                for (StreamStateDescriptor descriptor : descriptors) {
                    List<StreamCheckpointMember> selected = new ArrayList<>();
                    for (StreamCheckpointMember member : sourceMembers) {
                        if (member.kind()
                                == StreamCheckpointMember.Kind.STATE
                                && node.operatorUid().equals(
                                member.operatorUid())
                                && descriptor.stateName().equals(
                                member.subjectId())) {
                            selected.add(member);
                        }
                    }
                    if (selected.isEmpty()) {
                        throw new IllegalStateException(
                                "live rescale checkpoint is missing keyed state "
                                        + node.operatorUid() + '/'
                                        + descriptor.stateName());
                    }
                    targetMembers.removeAll(selected);
                    targetMembers.addAll(rescaler.rescale(
                            OfflineKeyedStateRescaler.RescaleMode.LIVE,
                            target.artifact.artifactId(),
                            target.artifact.artifactId(), node.operatorUid(),
                            descriptor, graphNode.maximumParallelism(),
                            graphNode.parallelism(), selected,
                            payloadStore, payloadStore).targetMembers());
                }
            }
            for (StreamStateDescriptor descriptor
                    : definition.replicatedStateDescriptors(node)) {
                List<StreamCheckpointMember> selected = new ArrayList<>();
                for (StreamCheckpointMember member : sourceMembers) {
                    if (member.kind() == StreamCheckpointMember.Kind.STATE
                            && node.operatorUid().equals(
                            member.operatorUid())
                            && descriptor.stateName().equals(
                            member.subjectId())) {
                        selected.add(member);
                    }
                }
                if (selected.isEmpty()) {
                    throw new IllegalStateException(
                            "live rescale checkpoint is missing replicated state "
                                    + node.operatorUid() + '/'
                                    + descriptor.stateName());
                }
                targetMembers.removeAll(selected);
                targetMembers.addAll(rescaler.replicateKeyedMapState(
                        target.artifact.artifactId(),
                        target.artifact.artifactId(), node.operatorUid(),
                        descriptor, graphNode.maximumParallelism(),
                        graphNode.parallelism(), selected,
                        payloadStore, payloadStore).targetMembers());
            }
            for (StreamOperatorDefinitionRegistry.TaskLocalStateContract
                    contract : definition.taskLocalStateContracts(node)) {
                List<StreamCheckpointMember> selected =
                        StreamLiveTaskLocalStateRescaler.select(
                                contract, node, sourceMembers);
                if (selected.isEmpty()) {
                    throw new IllegalStateException(
                            "live rescale checkpoint is missing task-local state "
                                    + node.operatorUid() + '/' + contract);
                }
                targetMembers.removeAll(selected);
                targetMembers.addAll(
                        StreamLiveTaskLocalStateRescaler.rescale(
                                contract, node,
                                graphNode.maximumParallelism(),
                                graphNode.parallelism(),
                                graphNode.inputEdges().isEmpty() ? null
                                        : StreamEdgePartitioning.outputSchema(
                                        target.executionGraph,
                                        graphNode.inputEdges().get(0)),
                                selected,
                                payloadStore));
            }
        }
        if (target.job.checkpointConfig().mode()
                == io.rowweave.stream.api.StreamCheckpointMode.UNALIGNED) {
            targetMembers.removeIf(member -> member.kind()
                    == StreamCheckpointMember.Kind.CHANNEL_STATE);
            targetMembers.addAll(StreamLiveChannelStateRescaler.rescale(
                    source, sourcePlacement, target, targetPlacement,
                    targetCheckpoint,
                    targetChannelEpoch, targetAssignmentGeneration,
                    sourceMembers, payloadStore));
        }
        publishSyntheticCheckpoint(targetCheckpoint, targetMembers);
        return targetCheckpoint;
    }

    private static List<StreamStateDescriptor> offlineRescaleDescriptors(
            PortableStreamNode stateNode) {
        if (stateNode.opcode()
                != io.rowweave.stream.contract.StreamOperatorCode
                .KEYED_PROCESS) {
            return stateNode.stateDescriptors();
        }
        io.rowweave.stream.contract.StreamProcessBinding binding =
                (io.rowweave.stream.contract.StreamProcessBinding)
                        stateNode.binding();
        List<StreamStateDescriptor> descriptors = new ArrayList<>();
        for (io.rowweave.stream.contract.StreamManagedStateDescriptor state
                : binding.managedStates()) {
            descriptors.add(state.state());
        }
        for (io.rowweave.stream.contract.StreamTimerDescriptor timer
                : binding.timerFamilies()) {
            descriptors.add(StreamKeyedProcessOperatorTask
                    .timerStateDescriptor(timer));
        }
        return Collections.unmodifiableList(descriptors);
    }

    private static List<StreamProductExecutionGraph.SourceRoute>
            offlineRescaleSources(StreamProductExecutionGraph admitted) {
        if (admitted.sources.isEmpty()) {
            throw new UnsupportedOperationException(
                    "offline rescale requires at least one fixed source");
        }
        return admitted.sources;
    }

    private static boolean containsOperator(
            List<PortableStreamNode> nodes, String operatorUid) {
        for (PortableStreamNode node : nodes) {
            if (node.operatorUid().equals(operatorUid)) return true;
        }
        return false;
    }

    private static List<StreamCheckpointMember> selectStateMembers(
            PortableStreamNode node,
            List<StreamStateDescriptor> descriptors,
            List<StreamCheckpointMember> members) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (StreamStateDescriptor descriptor : descriptors) {
            counts.put(descriptor.stateName(), 0);
        }
        List<StreamCheckpointMember> selected = new ArrayList<>();
        for (StreamCheckpointMember member : members) {
            if (member.kind() == StreamCheckpointMember.Kind.STATE
                    && node.operatorUid().equals(member.operatorUid())
                    && counts.containsKey(member.subjectId())) {
                selected.add(member);
                counts.put(member.subjectId(), Math.addExact(
                        counts.get(member.subjectId()), 1));
            }
        }
        for (Map.Entry<String, Integer> count : counts.entrySet()) {
            if (count.getValue() == 0) {
                throw new IllegalStateException(
                        "live rescale checkpoint is missing keyed state "
                                + node.operatorUid() + '/'
                                + count.getKey());
            }
        }
        return selected;
    }

    private static List<PortableStreamNode> offlineRescaleNodes(
            StreamProductExecutionGraph admitted) {
        List<PortableStreamNode> selected = new ArrayList<>();
        for (StreamExecutionGraph.Node graphNode
                : admitted.executionGraph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            switch (node.opcode()) {
                case EVENT_TIME_WINDOW:
                case GENERIC_WINDOW:
                case KEYED_AGGREGATE:
                case KEYED_PROCESS:
                    selected.add(node);
                    break;
                case KEYED_CO_PROCESS:
                case TEMPORAL_JOIN:
                case ASSIGN_EVENT_TIME:
                case ASYNC_WAIT:
                    throw new UnsupportedOperationException(
                            "offline rescale is unsupported for Stream node "
                                    + node.operatorUid() + " ("
                                    + node.opcode() + ')');
                default:
                    break;
            }
        }
        return Collections.unmodifiableList(selected);
    }

    private CheckpointIdentity targetCheckpoint(
            StreamProductJobSnapshot snapshot,
            StreamProductExecutionGraph admitted,
            CheckpointIdentity source) {
        String deployment = PlanArtifactDeploymentBinding.deploymentSetId(
                admitted.artifact);
        if (deployment == null) deployment = admitted.artifact.artifactId();
        return new CheckpointIdentity(
                admitted.artifact.definitionId(),
                admitted.artifact.artifactId(), snapshot.jobId().value(),
                "attempt-1", 1L, coordinatorTerm,
                coordinatorIncarnation, source.checkpointSequence(),
                admitted.commandDigest, deployment,
                source.dataProtocolVersion(),
                source.stateFormatVersion());
    }

    private void publishSyntheticCheckpoint(
            CheckpointIdentity checkpoint,
            List<StreamCheckpointMember> members) {
        CheckpointDecision durable = checkpointAuthority.decision(checkpoint);
        if (durable != null) {
            List<StreamCheckpointMember> existing =
                    checkpointMemberStore.resolve(
                            durable, checkpointAuthority);
            if (!sameMembers(existing, members)) {
                throw new IllegalStateException(
                        "offline rescale checkpoint identity was rebound");
            }
            return;
        }
        for (StreamCheckpointMember member : members) {
            checkpointMemberStore.publish(member);
        }
        StreamSyntheticCheckpointPlan synthetic =
                StreamSyntheticCheckpointPlan.from(checkpoint, members);
        StreamCheckpointCoordinator coordinator =
                new StreamCheckpointCoordinator(
                        synthetic.expectation(),
                        checkpointAuthority, checkpointMemberStore);
        for (CheckpointTaskReceipt receipt : synthetic.receipts()) {
            coordinator.acknowledge(receipt);
        }
        coordinator.complete(coordinator.decideCommit());
    }

    private static boolean sameMembers(
            List<StreamCheckpointMember> left,
            List<StreamCheckpointMember> right) {
        if (left.size() != right.size()) return false;
        List<StreamCheckpointMember> remaining = new ArrayList<>(right);
        for (StreamCheckpointMember member : left) {
            if (!remaining.remove(member)) return false;
        }
        return remaining.isEmpty();
    }

    static CheckpointTransferProof checkpointTransferProof(
            CheckpointDecision decision,
            io.rowweave.stream.runtime.StreamCheckpointAuthority authority,
            StreamCheckpointCatalog members) {
        if (decision == null || authority == null || members == null) {
            throw new IllegalArgumentException(
                    "checkpoint transfer authority is required");
        }
        CheckpointDecision first = decision.status()
                == CheckpointDecision.Status.COMPLETED
                ? authority.firstDecision(decision.checkpointIdentity())
                : decision;
        if (first == null || first.status()
                != CheckpointDecision.Status.COMMIT_DECIDED) {
            throw new IllegalStateException(
                    "checkpoint transfer requires a commit decision");
        }
        return new CheckpointTransferProof(first,
                decision.status() == CheckpointDecision.Status.COMPLETED
                        ? decision : null,
                authority.manifest(first.manifestDigest()),
                members.resolve(first, authority));
    }

    static final class CheckpointTransferProof {
        private final CheckpointDecision decision;
        private final CheckpointDecision completion;
        private final io.rowweave.stream.contract.CheckpointManifest manifest;
        private final List<StreamCheckpointMember> members;
        private final Map<StreamTaskIdentity,
                List<StreamCheckpointMember>> byTask = new LinkedHashMap<>();
        private final Map<String, Map<String, StreamCheckpointMember>>
                sourcePositions = new LinkedHashMap<>();

        private CheckpointTransferProof(CheckpointDecision decision,
                CheckpointDecision completion,
                io.rowweave.stream.contract.CheckpointManifest manifest,
                List<StreamCheckpointMember> members) {
            this.decision = decision;
            this.completion = completion;
            this.manifest = manifest;
            this.members = members;
            for (StreamCheckpointMember member : members) {
                byTask.computeIfAbsent(new StreamTaskIdentity(
                        member.operatorUid(), member.subtaskIndex()),
                        ignored -> new ArrayList<>()).add(member);
                if (member.kind()
                        == StreamCheckpointMember.Kind.SOURCE_POSITION
                        && !"idle-source".equals(member.subjectId())) {
                    StreamCheckpointMember prior = sourcePositions
                            .computeIfAbsent(member.operatorUid(),
                                    ignored -> new LinkedHashMap<>())
                            .put(member.subjectId(), member);
                    if (prior != null) {
                        throw new IllegalArgumentException(
                                "checkpoint has duplicate source split positions");
                    }
                }
            }
        }

        List<StreamCheckpointMember> members() { return members; }

        byte[] forTask(StreamWorkerPlacement.Assignment assignment) {
            return forTask(assignment, null, false);
        }

        byte[] completedForTask(
                StreamWorkerPlacement.Assignment assignment) {
            if (completion == null) {
                throw new IllegalStateException(
                        "completed checkpoint proof is unavailable");
            }
            return StreamCheckpointTransferBundleCodec.encodeCompleted(
                    decision, completion, manifest,
                    membersFor(assignment, null, false));
        }

        byte[] forTask(StreamWorkerPlacement.Assignment assignment,
                byte[] sourceAssignmentFrame, boolean positionRequired) {
            return StreamCheckpointTransferBundleCodec.encode(decision,
                    manifest, membersFor(assignment, sourceAssignmentFrame,
                            positionRequired));
        }

        List<StreamCheckpointMember> membersFor(
                StreamWorkerPlacement.Assignment assignment,
                byte[] sourceAssignmentFrame, boolean positionRequired) {
            List<StreamCheckpointMember> selected = new ArrayList<>();
            for (StreamCheckpointMember member : byTask.getOrDefault(
                    assignment.identity(), Collections.emptyList())) {
                if (sourceAssignmentFrame == null || member.kind()
                        != StreamCheckpointMember.Kind.SOURCE_POSITION) {
                    selected.add(member);
                }
            }
            if (sourceAssignmentFrame == null) {
                return selected;
            }
            if (sourceAssignmentFrame.length == 0) {
                if (positionRequired) {
                    throw new IllegalArgumentException(
                            "source position requires an assigned split");
                }
                return selected;
            }
            StreamSourceTaskAssignmentCodec.Value source =
                    StreamSourceTaskAssignmentCodec.decode(
                            sourceAssignmentFrame);
            if (!assignment.operatorUid().equals(source.assignment()
                    .sourceOperatorUid()) || assignment.subtaskIndex()
                    != source.assignment().subtaskIndex()) {
                throw new IllegalArgumentException(
                        "source assignment differs from target task");
            }
            StreamCheckpointMember position = positionRequired
                    ? sourcePositions.getOrDefault(assignment.operatorUid(),
                    Collections.emptyMap()).get(
                            source.assignment().splitId()) : null;
            if (position == null) {
                if (positionRequired) {
                    throw new IllegalArgumentException(
                            "committed source split position is missing");
                }
            } else {
                if (!source.split().positionCodecId().equals(
                        position.codecId())) {
                    throw new IllegalArgumentException(
                            "source split position codec differs");
                }
                selected.add(position);
            }
            return selected;
        }
    }

    private StreamClusterRunState authorityState(
            String runId, StreamWorkerPlacement placement) {
        String attemptId = "attempt-1";
        if (!authority.exists(runId)) {
            if (placement == null) {
                throw new IllegalArgumentException(
                        "initial Stream placement is required");
            }
            return authority.create(StreamClusterRunState.initial(
                    coordinatorTerm, coordinatorIncarnation, runId,
                    attemptId, 1L, 1L, 1L, placement));
        }
        StreamClusterRunState current = authority.current(runId);
        if (current.coordinatorTerm() == coordinatorTerm
                && current.coordinatorIncarnation().equals(
                coordinatorIncarnation)) {
            return current;
        }
        if (coordinatorTerm <= current.coordinatorTerm()) {
            throw new StreamClusterProtocolException(
                    StreamClusterProtocolException.Code.STALE_COORDINATOR,
                    "Stream product Master term is stale");
        }
        return authority.replaceCoordinator(runId,
                current.coordinatorTerm(), current.coordinatorIncarnation(),
                coordinatorTerm, coordinatorIncarnation);
    }

    private Placement place(StreamProductExecutionGraph admitted) {
        return place(admitted, null, null);
    }

    private Placement place(StreamProductExecutionGraph admitted,
            String excludedWorkerId, String excludedWorkerIncarnation) {
        List<RemoteStreamWorkerEndpoint> currentEndpoints = endpoints();
        requireBoundProviders(admitted, currentEndpoints);
        List<RemoteStreamWorkerEndpoint> candidates = new ArrayList<>();
        for (RemoteStreamWorkerEndpoint endpoint : currentEndpoints) {
            if (!excluded(endpoint, excludedWorkerId,
                    excludedWorkerIncarnation)) candidates.add(endpoint);
        }
        if (candidates.isEmpty()) {
            throw new StreamClusterProtocolException(
                    StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                    "no Stream Workers remain after exclusion");
        }
        List<StreamWorkerCapabilityProfile> profiles = new ArrayList<>();
        Map<String, RemoteStreamWorkerEndpoint> byWorker =
                new LinkedHashMap<>();
        for (RemoteStreamWorkerEndpoint endpoint : candidates) {
            profiles.add(endpoint.profile());
            byWorker.put(endpoint.profile().workerId(), endpoint);
        }
        StreamWorkerPlacement placement = StreamNodeTaskPlacementPlanner.place(
                admitted.executionGraph, profiles,
                admitted.controlProtocolVersion(),
                DATA_VERSION, admitted.executionCapabilities(), (request,
                profile) -> {
                    RemoteStreamWorkerEndpoint endpoint = byWorker.get(
                            profile.workerId());
                    PortableStreamNode portable = admitted.executionGraph
                            .node(request.identity().operatorUid())
                            .portableNode();
                    if (portable.opcode() == StreamOperatorCode.SOURCE) {
                        StreamSourceBinding binding =
                                (StreamSourceBinding) portable.binding();
                        return endpoint.supportsSource(binding.providerKey(),
                                binding.providerContractDigest());
                    }
                    if (portable.opcode() == StreamOperatorCode.SINK) {
                        StreamSinkBinding binding =
                                (StreamSinkBinding) portable.binding();
                        return endpoint.supportsSink(binding.providerKey(),
                                binding.providerContractDigest());
                    }
                    return true;
                });
        StreamCheckpointTransferBudget.validate(admitted, placement);
        return new Placement(placement);
    }

    private void requireBoundProviders(StreamProductExecutionGraph graph,
            List<RemoteStreamWorkerEndpoint> currentEndpoints) {
        for (StreamProductExecutionGraph.SourceRoute source : graph.sources) {
            boolean available = false;
            for (RemoteStreamWorkerEndpoint endpoint : currentEndpoints) {
                if (endpoint.supportsSource(source.binding.providerKey(),
                        source.binding.providerContractDigest())) {
                    available = true;
                    break;
                }
            }
            if (!available) {
                throw new StreamBoundProviderUnavailableException(
                        "bound Stream source provider is unavailable");
            }
        }
        for (StreamProductExecutionGraph.SinkRoute sink : graph.sinks) {
            boolean available = false;
            for (RemoteStreamWorkerEndpoint endpoint : currentEndpoints) {
                if (endpoint.supportsSink(sink.binding.providerKey(),
                        sink.binding.providerContractDigest())) {
                    available = true;
                    break;
                }
            }
            if (!available) {
                throw new StreamBoundProviderUnavailableException(
                        "bound Stream Sink provider is unavailable");
            }
        }
    }

    private static boolean excluded(RemoteStreamWorkerEndpoint endpoint,
            String workerId, String workerIncarnation) {
        return workerId != null && workerIncarnation != null
                && endpoint.profile().workerId().equals(workerId)
                && endpoint.profile().workerIncarnation().equals(
                        workerIncarnation);
    }

    private RemoteStreamWorkerEndpoint exactEndpoint(
            StreamWorkerPlacement.Assignment assignment) {
        for (RemoteStreamWorkerEndpoint endpoint : endpoints()) {
            StreamWorkerCapabilityProfile profile = endpoint.profile();
            if (profile.workerId().equals(assignment.workerId())
                    && profile.workerIncarnation().equals(
                    assignment.workerIncarnation())) {
                return endpoint;
            }
        }
        throw new StreamWorkerLossException(assignment,
                new StreamClusterProtocolException(
                        StreamClusterProtocolException.Code.STALE_ATTEMPT,
                        "assigned Stream Worker incarnation is unavailable"));
    }

    private static StreamWorkerPlacement.Assignment assignmentFor(
            StreamWorkerPlacement placement, String operatorUid,
            int subtaskIndex) {
        for (StreamWorkerPlacement.Assignment assignment
                : placement.assignments()) {
            if (assignment.operatorUid().equals(operatorUid)
                    && assignment.subtaskIndex() == subtaskIndex) {
                return assignment;
            }
        }
        throw new StreamClusterProtocolException(
                StreamClusterProtocolException.Code.STALE_ATTEMPT,
                "Stream placement is missing the requested operator task");
    }

    private static List<RemoteStreamWorkerEndpoint> endpoints(
            Collection<RemoteStreamWorkerEndpoint> values) {
        if (values == null || values.isEmpty()) {
            throw new StreamClusterProtocolException(
                    StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                    "at least one current Stream Worker is required");
        }
        Map<String, RemoteStreamWorkerEndpoint> unique = new LinkedHashMap<>();
        for (RemoteStreamWorkerEndpoint endpoint : values) {
            if (endpoint == null || unique.put(
                    endpoint.profile().workerId(), endpoint) != null) {
                throw new IllegalArgumentException(
                        "Stream Worker endpoints must have unique IDs");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(unique.values()));
    }

    private List<RemoteStreamWorkerEndpoint> endpoints() {
        return endpoints(workerDirectory.snapshot());
    }

    private static StreamWorkerDirectory fixedDirectory(
            Collection<RemoteStreamWorkerEndpoint> values) {
        List<RemoteStreamWorkerEndpoint> fixed = endpoints(values);
        return () -> fixed;
    }

    private static byte[] startCommand(StreamClusterRunState state,
            StreamWorkerPlacement.Assignment assignment,
            String digest, long sequence, int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.START,
                state.coordinatorTerm(), state.coordinatorIncarnation(),
                state.runId(), state.attemptId(), state.fenceToken(),
                state.assignmentGeneration(), assignment.workerIncarnation(),
                sequence, digest,
                StreamTaskLaunchPayload.start(
                        state.channelEpoch(), parallelismOverride,
                        nodeMinimumParallelism)));
    }

    private static byte[] restoreCommand(StreamClusterRunState state,
            StreamWorkerPlacement.Assignment assignment,
            String digest, long sequence,
            CheckpointIdentity restoreCheckpoint,
            int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        byte[] checkpoint = StreamCheckpointControlCodec.encode(
                restoreCheckpoint);
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.RESTORE,
                state.coordinatorTerm(), state.coordinatorIncarnation(),
                state.runId(), state.attemptId(), state.fenceToken(),
                state.assignmentGeneration(), assignment.workerIncarnation(),
                sequence, digest, StreamTaskLaunchPayload.restore(
                state.channelEpoch(), parallelismOverride,
                nodeMinimumParallelism, checkpoint)));
    }

    private static byte[] launchCommand(StreamClusterRunState state,
            StreamWorkerPlacement.Assignment assignment,
            String digest, long sequence,
            CheckpointIdentity restoreCheckpoint,
            int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        return restoreCheckpoint == null
                ? startCommand(state, assignment, digest, sequence,
                        parallelismOverride, nodeMinimumParallelism)
                : restoreCommand(state, assignment, digest, sequence,
                        restoreCheckpoint, parallelismOverride,
                        nodeMinimumParallelism);
    }

    private static byte[] progressCommand(StreamClusterRunState state,
            StreamWorkerPlacement.Assignment assignment,
            String digest, long sequence) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.TASK_PROGRESS,
                state.coordinatorTerm(), state.coordinatorIncarnation(),
                state.runId(), state.attemptId(), state.fenceToken(),
                state.assignmentGeneration(), assignment.workerIncarnation(),
                sequence, digest, new byte[0]));
    }

    private static byte[] cancelCommand(StreamClusterRunState state,
            StreamWorkerPlacement.Assignment assignment,
            String digest, long sequence) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.CANCEL,
                state.coordinatorTerm(), state.coordinatorIncarnation(),
                state.runId(), state.attemptId(), state.fenceToken(),
                state.assignmentGeneration(), assignment.workerIncarnation(),
                sequence, digest, new byte[0]));
    }

    private static byte[] checkpointCommand(StreamClusterRunState state,
            StreamWorkerPlacement.Assignment assignment,
            String digest, long sequence, CheckpointIdentity checkpoint) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.CHECKPOINT_TRIGGER,
                state.coordinatorTerm(), state.coordinatorIncarnation(),
                state.runId(), state.attemptId(), state.fenceToken(),
                state.assignmentGeneration(), assignment.workerIncarnation(),
                sequence, digest,
                StreamCheckpointControlCodec.encode(checkpoint)));
    }

    private static final class Placement {
        final StreamWorkerPlacement value;
        private Placement(StreamWorkerPlacement value) { this.value = value; }
    }

    private static final class RemoteRun implements StreamProductRun {
        private final RemoteStreamSourceSinkRunFactory owner;
        private final StreamProductExecutionGraph admitted;
        private final byte[] artifactFrame;
        private final StreamClusterRunState state;
        private final FileStreamClusterRunAuthority runAuthority;
        private final StreamCheckpointAuthority checkpointAuthority;
        private final StreamCheckpointCatalog checkpointMemberStore;
        private final StreamCheckpointCatalog savepointStore;
        private final CheckpointIdentity restoreCheckpoint;
        private final Path checkpointRoot;
        private final FileCheckpointPayloadStore checkpointPayloadStore;
        private final Map<String, RemoteStreamWorkerEndpoint> workers;
        private final StreamTaskProgressAggregator progressAggregator;
        private final StreamRuntimeMonitorProjection monitorProjection;
        private final java.util.Set<StreamTaskIdentity> completedTasks =
                new java.util.HashSet<>();
        private volatile StreamCompletedCheckpointEvidence
                completedCheckpointEvidence;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean canceled = new AtomicBoolean();
        private final AtomicBoolean checkpointInFlight = new AtomicBoolean();
        private final Map<String, StreamSourceCoordinator>
                sourceCoordinators = new LinkedHashMap<>();
        private final Map<StreamTaskIdentity,
                io.rowweave.stream.contract.StreamSourceAssignment>
                acknowledgedSourceCompletions = new java.util.HashMap<>();
        private final Map<StreamTaskIdentity,
                io.rowweave.stream.contract.StreamSourceAssignment>
                currentSourceAssignments = new java.util.HashMap<>();
        private final Map<StreamTaskIdentity, Boolean>
                sourcePositionRequired = new LinkedHashMap<>();
        private final Map<String, Long> nextSourceDiscoveryNanos =
                new LinkedHashMap<>();
        private final long launchSequence;
        private volatile Throwable sourceDiscoveryFailure;
        private long controlSequence;
        private long checkpointSequence;
        private long nextFinishPollNanos;
        private volatile StreamCdcPhase cdcPhase;

        private RemoteRun(RemoteStreamSourceSinkRunFactory owner,
                StreamProductExecutionGraph admitted,
                byte[] artifactFrame, StreamClusterRunState state,
                FileStreamClusterRunAuthority runAuthority,
                StreamCheckpointAuthority checkpointAuthority,
                StreamCheckpointCatalog checkpointMemberStore,
                StreamCheckpointCatalog savepointStore,
                CheckpointIdentity restoreCheckpoint,
                Path checkpointRoot,
                long launchSequence,
                Map<String, RemoteStreamWorkerEndpoint> workers) {
            this.owner = owner;
            this.admitted = admitted;
            this.artifactFrame = artifactFrame;
            this.state = state;
            this.runAuthority = runAuthority;
            this.checkpointAuthority = checkpointAuthority;
            this.checkpointMemberStore = checkpointMemberStore;
            this.savepointStore = savepointStore;
            this.restoreCheckpoint = restoreCheckpoint;
            this.checkpointRoot = checkpointRoot;
            this.workers = Collections.unmodifiableMap(
                    new LinkedHashMap<>(workers));
            this.progressAggregator = new StreamTaskProgressAggregator(
                    state, admitted.executionGraph);
            this.monitorProjection = io.rowweave.monitor.MonitorRuntime.shared()
                    == io.rowweave.monitor.JobMonitorRegistry.NOOP ? null
                    : new StreamRuntimeMonitorProjection(admitted.executionGraph,
                            state.placement());
            this.checkpointPayloadStore = checkpointRoot == null
                    || admitted.job.resourceProfile().checkpointBytes() <= 0L
                    ? null
                    : new FileCheckpointPayloadStore(checkpointRoot,
                    admitted.job.resourceProfile().checkpointBytes());
            if (launchSequence <= 0L) {
                throw new IllegalArgumentException(
                        "Stream launch command sequence must be positive");
            }
            this.launchSequence = launchSequence;
            this.controlSequence = launchSequence;
            long durableSequence = state.latestCompletedCheckpoint() == null
                    ? 0L : state.latestCompletedCheckpoint()
                    .checkpointSequence();
            if (checkpointAuthority != null) {
                for (CheckpointDecision decision
                        : checkpointAuthority.decisionsForRun(state.runId())) {
                    durableSequence = Math.max(durableSequence,
                            decision.checkpointIdentity()
                                    .checkpointSequence());
                }
            }
            this.checkpointSequence = restoreCheckpoint == null
                    ? durableSequence : Math.max(durableSequence,
                    restoreCheckpoint.checkpointSequence());
            this.cdcPhase = initialCdcPhase();
        }

        @Override
        public void start() {
            if (!started.compareAndSet(false, true)) {
                throw new IllegalStateException("Stream remote run already started");
            }
            try {
                CheckpointTransferProof checkpointProof = restoreCheckpoint
                        == null ? null : checkpointTransferProof(
                        checkpointAuthority.firstDecision(restoreCheckpoint),
                        checkpointAuthority, checkpointMemberStore);
                long discoverySequence = nextControlSequence();
                Map<StreamTaskIdentity, byte[]> sourceAssignments =
                        discoverSourceAssignments(discoverySequence);
                for (Map.Entry<StreamTaskIdentity, byte[]> entry
                        : sourceAssignments.entrySet()) {
                    if (entry.getValue() != null
                            && entry.getValue().length != 0) {
                        currentSourceAssignments.put(entry.getKey(),
                                StreamSourceTaskAssignmentCodec.decode(
                                        entry.getValue()).assignment());
                    }
                }
                if (restoreCheckpoint != null) {
                    distributeRestorePayloads(checkpointProof,
                            sourceAssignments);
                }
                long deploymentSequence = nextControlSequence();
                StreamGenericTaskDeployment.deployWithTaskProof(
                        admitted, artifactFrame,
                        state.placement(), workers, admitted.maximumEvents(),
                        assignment -> checkpointProof == null ? null
                                : checkpointProof.forTask(assignment,
                                sourceAssignments.get(assignment.identity()),
                                sourcePositionRequired.getOrDefault(
                                        assignment.identity(), false)),
                        assignment -> sourceAssignments.get(
                                assignment.identity()),
                        assignment -> launchCommand(state, assignment,
                                admitted.commandDigest, deploymentSequence,
                                restoreCheckpoint,
                                admitted.parallelismOverride,
                                admitted.nodeMinimumParallelism));
                startSourceDiscoveryLoop();
            } catch (RuntimeException failure) {
                for (StreamWorkerPlacement.Assignment assignment
                        : state.placement().assignments()) {
                    try {
                        workers.get(assignment.workerId()).cancel(
                                cancelCommand(state, assignment,
                                        admitted.commandDigest, 2L));
                    } catch (RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                }
                throw failure;
            }
        }

        private Map<StreamTaskIdentity, byte[]>
                discoverSourceAssignments(long discoverySequence) {
            Map<StreamTaskIdentity, byte[]> result = new LinkedHashMap<>();
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.sources) {
                List<StreamWorkerPlacement.Assignment> sourceTasks =
                        sourceTasks(source.node.operatorUid());
                for (StreamWorkerPlacement.Assignment task : sourceTasks) {
                    result.put(task.identity(), new byte[0]);
                    sourcePositionRequired.put(task.identity(), false);
                }
                StreamSourceDiscoveryDescriptor limits = sourceLimits(
                        source.binding);
                StreamSourceCoordinator coordinator =
                        restoredSourceCoordinator(source, limits);
                StreamWorkerPlacement.Assignment enumeratorTask =
                        sourceTasks.get(0);
                StreamSplitDiscovery discovered = source.binding
                        .dynamicDiscovery()
                        ? workers.get(enumeratorTask.workerId()).discoverSource(
                        launchCommand(state, enumeratorTask,
                                admitted.commandDigest, discoverySequence,
                                restoreCheckpoint,
                                admitted.parallelismOverride,
                                admitted.nodeMinimumParallelism), artifactFrame,
                        source.node.operatorUid(),
                        coordinator.state().cursor(),
                        limits.maximumChangesPerDiscovery(),
                        discoverySpec(limits))
                        : discoverFixed(source.binding,
                        coordinator.state().cursor(), limits);
                coordinator.applyDiscovery(discovered);
                sourceCoordinators.put(source.node.operatorUid(),
                        coordinator);
                int taskIndex = 0;
                for (StreamSourceCoordinatorSplit split
                        : coordinator.state().splits()) {
                    if (split.status()
                            != StreamSourceCoordinatorSplit.Status.PENDING) {
                        continue;
                    }
                    if (taskIndex >= sourceTasks.size()) {
                        break;
                    }
                    StreamWorkerPlacement.Assignment task =
                            sourceTasks.get(taskIndex++);
                    io.rowweave.stream.contract.StreamSourceAssignment
                            assignment = coordinator.assign(split.splitId(),
                            split.splitGeneration(), task.subtaskIndex(),
                            task.workerIncarnation(),
                            state.assignmentGeneration());
                    result.put(task.identity(),
                            StreamSourceTaskAssignmentCodec.encode(
                                    assignment, split.split()));
                    sourcePositionRequired.put(task.identity(),
                            split.hasPosition()
                                    || !source.binding.dynamicDiscovery());
                }
            }
            return Collections.unmodifiableMap(result);
        }

        private StreamSourceCoordinator restoredSourceCoordinator(
                StreamProductExecutionGraph.SourceRoute source,
                StreamSourceDiscoveryDescriptor limits) {
            if (restoreCheckpoint == null) {
                return StreamSourceCoordinator.initial(state.runId(),
                        state.attemptId(), source.node.operatorUid(), limits);
            }
            CheckpointDecision decision = checkpointAuthority.decision(
                    restoreCheckpoint);
            StreamCheckpointMember coordinatorMember = null;
            for (StreamCheckpointMember member : checkpointMemberStore
                    .resolve(decision, checkpointAuthority)) {
                if (member.kind()
                        == StreamCheckpointMember.Kind.SOURCE_COORDINATOR
                        && source.node.operatorUid().equals(
                        member.operatorUid())) {
                    if (coordinatorMember != null) {
                        throw new IllegalStateException(
                                "dynamic Source restore has duplicate coordinator state");
                    }
                    coordinatorMember = member;
                }
            }
            if (coordinatorMember == null || checkpointPayloadStore == null
                    || checkpointRoot == null) {
                if (!source.binding.dynamicDiscovery()) {
                    return StreamSourceCoordinator.initial(state.runId(),
                            state.attemptId(), source.node.operatorUid(),
                            limits);
                }
                throw new IllegalStateException(
                        "Source restore has no coordinator checkpoint");
            }
            Path restored = checkpointRoot.resolve("coordinator-restore")
                    .resolve(source.node.operatorUid() + '-'
                            + restoreCheckpoint.checkpointSequence()
                            + ".state");
            try {
                checkpointPayloadStore.restore(
                        new FileCheckpointPayloadStore.StoredPayload(
                                coordinatorMember.contentDigest(),
                                coordinatorMember.contentBytes()), restored);
                StreamSourceCoordinatorState durable =
                        StreamSourceCoordinatorStateCodec.decode(
                                Files.readAllBytes(restored));
                if (!source.node.operatorUid().equals(
                        durable.sourceOperatorUid())) {
                    throw new IllegalStateException(
                            "dynamic Source coordinator checkpoint belongs to another node");
                }
                return StreamSourceCoordinator.recoverAttempt(durable,
                        state.runId(), state.attemptId(), limits);
            } catch (java.io.IOException failure) {
                throw new IllegalStateException(
                        "dynamic Source coordinator restore failed", failure);
            } finally {
                try {
                    Files.deleteIfExists(restored);
                } catch (java.io.IOException ignored) {
                    // Content-addressed payload remains authoritative.
                }
            }
        }

        private void startSourceDiscoveryLoop() {
            if (sourceCoordinators.isEmpty()) return;
            long now = System.nanoTime();
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.sources) {
                if (source.binding.dynamicDiscovery()) {
                    nextSourceDiscoveryNanos.put(
                            source.node.operatorUid(), addNanos(now,
                            source.binding.discoveryDescriptor()
                                    .discoveryIntervalMillis()));
                }
            }
        }

        private void discoverAndAssignLateSplits() {
            long now = System.nanoTime();
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.sources) {
                List<StreamWorkerPlacement.Assignment> tasks =
                        sourceTasks(source.node.operatorUid());
                StreamWorkerPlacement.Assignment enumeratorTask = tasks.get(0);
                StreamSourceCoordinator coordinator = sourceCoordinators.get(
                        source.node.operatorUid());
                StreamSourceDiscoveryDescriptor limits = sourceLimits(
                        source.binding);
                recordCompletedSplits(tasks, coordinator);
                Long due = nextSourceDiscoveryNanos.get(
                        source.node.operatorUid());
                if (source.binding.dynamicDiscovery()
                        && (due == null || now - due >= 0L)) {
                    StreamSourceCoordinatorState priorState =
                            coordinator.state();
                    StreamSplitDiscovery discovered = workers.get(
                            enumeratorTask.workerId()).discoverSource(
                            progressCommand(state, enumeratorTask,
                                    admitted.commandDigest,
                                    nextControlSequence()), artifactFrame,
                            source.node.operatorUid(),
                            priorState.cursor(),
                            limits.maximumChangesPerDiscovery(),
                            discoverySpec(limits));
                    coordinator.applyDiscovery(discovered);
                    boolean hasRemoval = false;
                    for (io.rowweave.stream.spi.StreamSplitChange change
                            : discovered.changes()) {
                        if (change.kind()
                                == io.rowweave.stream.spi.StreamSplitChange
                                .Kind.REMOVE) {
                            hasRemoval = true;
                            break;
                        }
                    }
                    if (hasRemoval) {
                        revokeRemovedSplits(source, priorState.splits(),
                                coordinator, tasks, limits);
                    }
                    nextSourceDiscoveryNanos.put(
                            source.node.operatorUid(), addNanos(now,
                            limits.discoveryIntervalMillis()));
                }
                java.util.Set<Integer> occupied = new java.util.HashSet<>();
                for (StreamSourceCoordinatorSplit split
                        : coordinator.state().splits()) {
                    if (split.status()
                            == StreamSourceCoordinatorSplit.Status.ASSIGNED) {
                        occupied.add(split.assignment().subtaskIndex());
                    }
                }
                for (StreamSourceCoordinatorSplit split
                        : coordinator.state().splits()) {
                    if (split.status()
                            != StreamSourceCoordinatorSplit.Status.PENDING) {
                        continue;
                    }
                    StreamWorkerPlacement.Assignment target = null;
                    for (StreamWorkerPlacement.Assignment task : tasks) {
                        if (!occupied.contains(task.subtaskIndex())) {
                            target = task;
                            break;
                        }
                    }
                    if (target == null) break;
                    io.rowweave.stream.contract.StreamSourceAssignment
                            assignment = coordinator.assign(split.splitId(),
                            split.splitGeneration(), target.subtaskIndex(),
                            target.workerIncarnation(),
                            state.assignmentGeneration());
                    byte[] frame = StreamSourceTaskAssignmentCodec.encode(
                            assignment, split.split());
                    workers.get(target.workerId()).assignSourceSplit(
                            progressCommand(state, target,
                                    admitted.commandDigest,
                                    nextControlSequence()),
                            target.operatorUid(), target.subtaskIndex(), frame);
                    currentSourceAssignments.put(target.identity(), assignment);
                    occupied.add(target.subtaskIndex());
                }
            }
        }

        private static long addNanos(long now, long intervalMillis) {
            try {
                return Math.addExact(now,
                        Math.multiplyExact(intervalMillis, 1_000_000L));
            } catch (ArithmeticException overflow) {
                return Long.MAX_VALUE;
            }
        }

        private void revokeRemovedSplits(
                StreamProductExecutionGraph.SourceRoute source,
                List<StreamSourceCoordinatorSplit> priorSplits,
                StreamSourceCoordinator coordinator,
                List<StreamWorkerPlacement.Assignment> tasks,
                StreamSourceDiscoveryDescriptor limits) {
            List<StreamSourceCoordinatorSplit> current =
                    coordinator.state().splits();
            Map<String, StreamSourceCoordinatorSplit> currentById =
                    new java.util.HashMap<>();
            for (StreamSourceCoordinatorSplit split : current) {
                currentById.put(split.splitId(), split);
            }
            Map<Integer, StreamWorkerPlacement.Assignment> taskBySubtask =
                    new java.util.HashMap<>();
            for (StreamWorkerPlacement.Assignment task : tasks) {
                taskBySubtask.put(task.subtaskIndex(), task);
            }
            for (StreamSourceCoordinatorSplit previous : priorSplits) {
                if (previous.status()
                        != StreamSourceCoordinatorSplit.Status.ASSIGNED
                        && previous.status()
                        != StreamSourceCoordinatorSplit.Status.FINISHED) {
                    continue;
                }
                StreamSourceCoordinatorSplit latest =
                        currentById.get(previous.splitId());
                if (latest != null
                        && latest.splitGeneration()
                        == previous.splitGeneration()
                        && (latest.status()
                        == StreamSourceCoordinatorSplit.Status.ASSIGNED
                        || latest.status()
                        == StreamSourceCoordinatorSplit.Status.FINISHED)
                        && latest.assignment().equals(
                                previous.assignment())) {
                    continue;
                }
                StreamWorkerPlacement.Assignment owner = taskBySubtask.get(
                        previous.assignment().subtaskIndex());
                if (owner == null || !owner.workerIncarnation().equals(
                        previous.assignment().workerIncarnation())) {
                    throw new IllegalStateException(
                            "removed Source split owner is unavailable");
                }
                if (!previous.assignment().equals(
                        currentSourceAssignments.get(owner.identity()))) {
                    if (previous.status()
                            == StreamSourceCoordinatorSplit.Status.FINISHED) {
                        continue;
                    }
                    throw new IllegalStateException(
                            "assigned Source split differs from its Worker owner");
                }
                workers.get(owner.workerId()).revokeSourceSplit(
                        progressCommand(state, owner,
                                admitted.commandDigest,
                                nextControlSequence()),
                        source.node.operatorUid(), owner.subtaskIndex(),
                        StreamSourceTaskAssignmentCodec.encode(
                                previous.assignment(), previous.split()),
                        limits.discoveryTimeoutMillis());
                currentSourceAssignments.remove(owner.identity());
            }
        }

        private void recordCompletedSplits(
                List<StreamWorkerPlacement.Assignment> tasks,
                StreamSourceCoordinator coordinator) {
            Map<String, StreamSourceCoordinatorSplit> currentById = null;
            for (StreamWorkerPlacement.Assignment task : tasks) {
                StreamSourceTaskAssignmentCodec.Value completed = workers.get(
                        task.workerId()).sourceSplitProgress(
                        progressCommand(state, task,
                                admitted.commandDigest,
                                nextControlSequence()),
                        task.operatorUid(), task.subtaskIndex());
                if (completed == null) continue;
                io.rowweave.stream.contract.StreamSourceAssignment value =
                        completed.assignment();
                if (!state.runId().equals(value.jobId())
                        || !state.attemptId().equals(value.attemptId())
                        || !task.operatorUid().equals(
                        value.sourceOperatorUid())
                        || task.subtaskIndex() != value.subtaskIndex()
                        || !task.workerIncarnation().equals(
                        value.workerIncarnation())) {
                    throw new IllegalStateException(
                            "completed Source split differs from its placement");
                }
                if (value.equals(acknowledgedSourceCompletions.get(
                        task.identity()))) {
                    continue;
                }
                if (currentById == null) {
                    currentById = new java.util.HashMap<>();
                    for (StreamSourceCoordinatorSplit split
                            : coordinator.state().splits()) {
                        currentById.put(split.splitId(), split);
                    }
                }
                StreamSourceCoordinatorSplit current =
                        currentById.get(value.splitId());
                if (current != null
                        && (current.splitGeneration()
                                > value.splitGeneration()
                        || (current.splitGeneration()
                                == value.splitGeneration()
                        && current.status()
                                == StreamSourceCoordinatorSplit.Status.REMOVED))) {
                    acknowledgedSourceCompletions.put(task.identity(), value);
                    continue;
                }
                coordinator.finish(value.splitId(),
                        value.splitGeneration(),
                        value.assignmentGeneration());
                acknowledgedSourceCompletions.put(task.identity(), value);
            }
        }

        private List<StreamWorkerPlacement.Assignment> sourceTasks(
                String operatorUid) {
            List<StreamWorkerPlacement.Assignment> result = new ArrayList<>();
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                if (operatorUid.equals(assignment.operatorUid())) {
                    result.add(assignment);
                }
            }
            result.sort(Comparator.comparingInt(
                    StreamWorkerPlacement.Assignment::subtaskIndex));
            if (result.isEmpty()) {
                throw new IllegalStateException(
                        "dynamic Source has no placed subtasks");
            }
            return result;
        }

        private static StreamSourceDiscoverySpec discoverySpec(
                StreamSourceDiscoveryDescriptor value) {
            return new StreamSourceDiscoverySpec(
                    value.discoveryIntervalMillis(),
                    value.discoveryTimeoutMillis(),
                    value.maximumKnownSplits(),
                    value.maximumPendingSplits(),
                    value.maximumChangesPerDiscovery(),
                    value.maximumCursorBytes(),
                    value.maximumDescriptorBytes());
        }

        private static StreamSplitDiscovery discoverFixed(
                StreamSourceBinding binding,
                io.rowweave.stream.spi.StreamDiscoveryCursor cursor,
                StreamSourceDiscoveryDescriptor limits) {
            List<StreamSplitSpec> splits = new ArrayList<>();
            for (StreamSourceSplit split : binding.fixedSplits()) {
                splits.add(new StreamSplitSpec(split.splitId(),
                        split.positionCodecId(), split.descriptorFrame()));
            }
            try (StaticStreamSourceEnumerator enumerator =
                         new StaticStreamSourceEnumerator(splits)) {
                return enumerator.discover(cursor,
                        limits.maximumChangesPerDiscovery());
            }
        }

        private static StreamSourceDiscoveryDescriptor sourceLimits(
                StreamSourceBinding binding) {
            if (binding.dynamicDiscovery()) {
                return binding.discoveryDescriptor();
            }
            int descriptorBytes = 1;
            for (StreamSourceSplit split : binding.fixedSplits()) {
                descriptorBytes = Math.max(descriptorBytes,
                        split.descriptorFrame().length);
            }
            int splitCount = binding.fixedSplits().size();
            return new StreamSourceDiscoveryDescriptor(
                    1L, 1L, splitCount, splitCount, splitCount,
                    4, descriptorBytes);
        }

        @Override
        public synchronized void cancel() {
            if (!canceled.compareAndSet(false, true)) return;
            long sequence = nextControlSequence();
            RuntimeException first = null;
            java.util.Set<String> canceledWorkers = new java.util.HashSet<>();
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                if (!canceledWorkers.add(assignment.workerId())) continue;
                try {
                    workers.get(assignment.workerId()).cancel(
                            cancelCommand(state, assignment,
                                    admitted.commandDigest, sequence));
                } catch (RuntimeException failure) {
                    if (first == null) first = failure;
                    else first.addSuppressed(failure);
                }
            }
            if (first != null) {
                canceled.set(false);
                throw first;
            }
        }

        @Override
        public synchronized void triggerCheckpoint(
                io.rowweave.stream.api.StreamCommandId commandId,
                String checkpointId) {
            if (!checkpointInFlight.compareAndSet(false, true)) {
                throw new IllegalStateException(
                        "Stream checkpoint cut is already in flight");
            }
            try {
                checkpointCut(commandId, checkpointId);
                nextFinishPollNanos = 0L;
            } finally {
                checkpointInFlight.set(false);
            }
        }

        @Override
        public boolean ownsCheckpointDeadline() {
            return true;
        }

        @Override
        public long assignmentGeneration() {
            return state.assignmentGeneration();
        }

        @Override
        public synchronized StreamProductRun liveRescale(
                io.rowweave.stream.api.StreamCommandId commandId,
                String rescaleId, int targetParallelism,
                long expectedSourceGeneration,
                long expectedTargetGeneration) {
            if (commandId == null || rescaleId == null
                    || rescaleId.isEmpty() || targetParallelism < 1
                    || expectedSourceGeneration
                    != state.assignmentGeneration()
                    || expectedTargetGeneration != Math.addExact(
                    expectedSourceGeneration, 1L)
                    || !started.get() || canceled.get()) {
                throw new IllegalArgumentException(
                        "live rescale generation is invalid");
            }
            CheckpointIdentity sourceCut = checkpointCut(
                    commandId, rescaleId);
            StreamProductExecutionGraph target =
                    StreamProductExecutionGraph.from(
                            admitted.artifact, targetParallelism,
                            activeSourceParallelismMinimums());
            Placement placement = owner.place(target);
            long targetFence = Math.addExact(state.fenceToken(), 1L);
            long targetEpoch = Math.addExact(state.channelEpoch(), 1L);
            String targetAttemptId =
                    "attempt-" + expectedTargetGeneration;
            CheckpointIdentity targetRestore =
                    owner.liveRescaleCheckpoint(admitted, state.placement(),
                            target, sourceCut,
                            targetAttemptId, targetFence,
                            expectedTargetGeneration, targetEpoch,
                            placement.value);
            StreamClusterRunState targetState =
                    runAuthority.liveRescaleCutover(
                            state.runId(), state.coordinatorTerm(),
                            state.coordinatorIncarnation(), state.attemptId(),
                            state.fenceToken(), state.assignmentGeneration(),
                            sourceCut, targetRestore, targetAttemptId,
                            targetFence, expectedTargetGeneration,
                            targetEpoch, placement.value);
            StreamProductRun replacement = owner.run(
                    target, targetState, targetRestore,
                    Math.addExact(controlSequence, 1L));
            try {
                replacement.start();
                return replacement;
            } catch (Throwable failure) {
                closeQuietly(replacement);
                throw new StreamLiveRescaleCutoverException(failure);
            }
        }

        private Map<String, Integer> activeSourceParallelismMinimums() {
            Map<String, Integer> minimums = new LinkedHashMap<>();
            for (Map.Entry<String, StreamSourceCoordinator> entry
                    : sourceCoordinators.entrySet()) {
                int active = 0;
                for (StreamSourceCoordinatorSplit split
                        : entry.getValue().state().splits()) {
                    if (split.status()
                            == StreamSourceCoordinatorSplit.Status.ASSIGNED
                            || split.status()
                            == StreamSourceCoordinatorSplit.Status.PENDING) {
                        active = Math.addExact(active, 1);
                    }
                }
                if (active > 0) minimums.put(entry.getKey(), active);
            }
            return Collections.unmodifiableMap(minimums);
        }

        private static void closeQuietly(StreamProductRun run) {
            try {
                run.close();
            } catch (Throwable ignored) {
                // Durable cutover recovery creates a fresh target run.
            }
        }

        @Override
        public synchronized boolean recoverSavepoint(
                io.rowweave.stream.api.StreamCommandId commandId,
                String savepointId) {
            if (commandId == null || savepointId == null
                    || savepointId.isEmpty()) {
                throw new IllegalArgumentException(
                        "Stream savepoint command identity is required");
            }
            if (!started.get() || canceled.get()) {
                throw new IllegalStateException(
                        "Stream remote run is not active");
            }
            if (checkpointAuthority == null || checkpointMemberStore == null
                    || savepointStore == null) {
                return false;
            }
            StreamSavepoint existing = savepointStore.get(savepointId);
            if (existing == null) {
                CheckpointDecision recoverable = recoverableDecision();
                if (recoverable == null) return false;
                publishRecoveredCheckpoint(savepointId, recoverable,
                        checkpointDeadlineNanos());
            } else if (!state.runId().equals(existing.checkpointManifest()
                    .checkpointIdentity().runId())) {
                throw new IllegalStateException(
                        "savepoint operation reference belongs to another run");
            }
            try {
                cancel();
            } catch (Throwable pending) {
                throw new CheckpointCommitPendingException(
                        "durable savepoint requires terminal reconciliation",
                        pending);
            }
            return true;
        }

        private CheckpointIdentity checkpointCut(
                io.rowweave.stream.api.StreamCommandId commandId,
                String checkpointId) {
            if (commandId == null || checkpointId == null
                    || checkpointId.isEmpty()) {
                throw new IllegalArgumentException(
                        "Stream checkpoint command identity is required");
            }
            if (!started.get() || canceled.get()) {
                throw new IllegalStateException(
                        "Stream remote run is not active");
            }
            if (checkpointAuthority == null || checkpointMemberStore == null) {
                throw new UnsupportedOperationException(
                        "checkpoint storage is unavailable");
            }
            long deadlineNanos = checkpointDeadlineNanos();
            StreamSavepoint existing = savepointStore == null
                    ? null : savepointStore.get(checkpointId);
            if (existing != null) {
                CheckpointIdentity identity = existing.checkpointManifest()
                        .checkpointIdentity();
                if (!state.runId().equals(identity.runId())) {
                    throw new IllegalStateException(
                            "checkpoint operation reference belongs to another run");
                }
                return publishRecoveredCheckpoint(
                        checkpointId, checkpointAuthority.decision(identity),
                        deadlineNanos);
            }
            CheckpointDecision recoverable = recoverableDecision();
            if (recoverable != null) {
                return publishRecoveredCheckpoint(
                        checkpointId, recoverable, deadlineNanos);
            }
            checkpointSequence = Math.addExact(checkpointSequence, 1L);
            String deployment = PlanArtifactDeploymentBinding.deploymentSetId(
                    admitted.artifact);
            if (deployment == null) {
                deployment = admitted.artifact.artifactId();
            }
            CheckpointIdentity checkpoint = new CheckpointIdentity(
                    admitted.artifact.definitionId(),
                    admitted.artifact.artifactId(), state.runId(),
                    state.attemptId(), state.fenceToken(),
                    state.coordinatorTerm(), state.coordinatorIncarnation(),
                    checkpointSequence, admitted.commandDigest, deployment,
                    DATA_VERSION, 1);
            long sequence = nextControlSequence();
            owner.beginCheckpointCut();
            Throwable primary = null;
            try {
                List<CheckpointExpectedTask> expected = new ArrayList<>();
                List<CheckpointTaskReceipt> receipts = new ArrayList<>();
                List<StreamCheckpointMember> sourceMembers = new ArrayList<>();
                List<StreamWorkerPlacement.Assignment> checkpointAssignments =
                        new ArrayList<>(state.placement().assignments());
                checkpointAssignments.sort((left, right) -> Boolean.compare(
                        isSource(right), isSource(left)));
                for (StreamWorkerPlacement.Assignment assignment
                        : checkpointAssignments) {
                    requireCheckpointNotInterrupted();
                    List<StreamCheckpointMember> members;
                    byte[] checkpointControl = checkpointCommand(
                            state, assignment, admitted.commandDigest,
                            sequence, checkpoint);
                    try {
                        members = workers.get(assignment.workerId()).checkpoint(
                                checkpointControl,
                                assignment.operatorUid(), assignment.subtaskIndex(),
                                remainingTimeoutMillis(deadlineNanos));
                    } catch (RuntimeException failure) {
                        throw new IllegalStateException(
                                "Stream checkpoint cut failed at operator "
                                        + assignment.operatorUid() + "#"
                                        + assignment.subtaskIndex() + " on Worker "
                                        + assignment.workerId(), failure);
                    }
                    requireCheckpointNotInterrupted();
                    captureCheckpointPayloads(assignment, checkpointControl,
                            members);
                    List<StreamCheckpointMember.Kind> kinds = expectedKinds(
                            assignment);
                    if (kinds.isEmpty()) {
                        if (!members.isEmpty()) {
                            throw new IllegalStateException(
                                    "stateless Stream task returned checkpoint state");
                        }
                    } else {
                        addTaskCut(checkpoint, assignment, members, kinds,
                                expected, receipts);
                    }
                    if (isSource(assignment)) sourceMembers.addAll(members);
                }
                StreamCdcPhase completedCdcPhase =
                        !admitted.residentCdcSources().isEmpty()
                        ? decodeCdcPhase(sourceMembers) : cdcPhase;
                addSourceCoordinatorCuts(checkpoint, sourceMembers,
                        expected, receipts);
                StreamCheckpointCoordinator coordinator =
                        new StreamCheckpointCoordinator(
                                new CheckpointExpectation(checkpoint, expected),
                                checkpointAuthority, checkpointMemberStore);
                for (CheckpointTaskReceipt receipt : receipts) {
                    coordinator.acknowledge(receipt);
                }
                requireCheckpointNotInterrupted();
                synchronized (owner.checkpointRetentionLock) {
                    CheckpointDecision decision = coordinator.decideCommit();
                    try {
                        completeTransactionalSinks(
                                checkpoint, decision, deadlineNanos);
                        CheckpointDecision completed = coordinator.complete(decision);
                        completeResidentSources(
                                checkpoint, completed, deadlineNanos);
                        publishCompletedCheckpoint(
                                checkpointId, completed, completedCdcPhase);
                    } catch (Throwable pending) {
                        throw new CheckpointCommitPendingException(
                                "durable Stream checkpoint commit requires reconciliation",
                                pending);
                    }
                }
                return checkpoint;
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                owner.finishCheckpointCut(
                        checkpointPayloadStore,
                        state.runId(), admitted.job.checkpointConfig()
                                .retainedCompleted(), primary);
            }
        }

        private void addSourceCoordinatorCuts(CheckpointIdentity checkpoint,
                List<StreamCheckpointMember> sourceMembers,
                List<CheckpointExpectedTask> expected,
                List<CheckpointTaskReceipt> receipts) {
            for (Map.Entry<String, StreamSourceCoordinator> entry
                    : sourceCoordinators.entrySet()) {
                StreamSourceCoordinator coordinator = entry.getValue();
                for (StreamCheckpointMember member : sourceMembers) {
                    if (member.kind()
                            != StreamCheckpointMember.Kind.SOURCE_POSITION
                            || !entry.getKey().equals(member.operatorUid())
                            || "idle-source".equals(member.subjectId())) {
                        continue;
                    }
                    StreamSourceCoordinatorSplit split = coordinatorSplit(
                            coordinator, member.subjectId());
                    coordinator.recordPosition(split.splitId(),
                            split.splitGeneration(),
                            member.logicalPosition(),
                            StreamCheckpointMemberCodec.encode(member));
                }
                byte[] stateFrame = StreamSourceCoordinatorStateCodec.encode(
                        coordinator.state());
                FileCheckpointPayloadStore.StoredPayload stored =
                        storeCoordinatorPayload(entry.getKey(), stateFrame);
                StreamCheckpointMember member =
                        StreamCheckpointMember.sourceCoordinator(
                                entry.getKey(),
                                "rowweave.stream.source-coordinator.v2",
                                stored.digest(), stored.contentBytes());
                checkpointMemberStore.publish(member);
                CheckpointEvidence evidence = member.toEvidence();
                String taskId = "stream-source-coordinator:"
                        + entry.getKey();
                expected.add(new CheckpointExpectedTask(taskId,
                        state.assignmentGeneration(),
                        state.coordinatorIncarnation(),
                        Collections.singletonList(evidence.key())));
                receipts.add(new CheckpointTaskReceipt(checkpoint, taskId,
                        state.assignmentGeneration(),
                        state.coordinatorIncarnation(),
                        Collections.singletonList(evidence)));
            }
        }

        private static StreamSourceCoordinatorSplit coordinatorSplit(
                StreamSourceCoordinator coordinator, String splitId) {
            for (StreamSourceCoordinatorSplit split
                    : coordinator.state().splits()) {
                if (split.splitId().equals(splitId)) return split;
            }
            throw new IllegalStateException(
                    "source position belongs to an unknown dynamic split");
        }

        private FileCheckpointPayloadStore.StoredPayload
                storeCoordinatorPayload(String operatorUid, byte[] frame) {
            if (checkpointPayloadStore == null || checkpointRoot == null) {
                throw new IllegalStateException(
                        "dynamic Source checkpoint storage is unavailable");
            }
            Path directory = checkpointRoot.resolve("coordinator-staging");
            Path staged = null;
            Throwable primary = null;
            try {
                Files.createDirectories(directory);
                staged = Files.createTempFile(directory,
                        operatorUid + '-', ".state");
                Files.write(staged, frame);
                return checkpointPayloadStore.store(staged);
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } catch (java.io.IOException failure) {
                primary = failure;
                throw new IllegalStateException(
                        "source coordinator checkpoint staging failed",
                        failure);
            } finally {
                if (staged != null) {
                    try {
                        Files.deleteIfExists(staged);
                    } catch (java.io.IOException cleanup) {
                        if (primary != null) primary.addSuppressed(cleanup);
                    }
                }
            }
        }

        private void captureCheckpointPayloads(
                StreamWorkerPlacement.Assignment assignment,
                byte[] checkpointControl,
                List<StreamCheckpointMember> members) {
            if (checkpointPayloadStore == null) return;
            RemoteStreamWorkerEndpoint worker = workers.get(
                    assignment.workerId());
            for (StreamCheckpointMember member : members) {
                capturePayloadClosure(worker, checkpointControl, member);
            }
        }

        private void capturePayloadClosure(RemoteStreamWorkerEndpoint worker,
                byte[] checkpointControl, StreamCheckpointMember root) {
            StreamCheckpointMember current = root;
            java.util.Set<String> visited = new java.util.HashSet<>();
            int depth = 0;
            while (payloadBacked(current)
                    && visited.add(current.contentDigest())) {
                transferFromWorker(worker, checkpointControl, current);
                if (++depth > 8) {
                    throw new IllegalStateException(
                            "checkpoint payload dependency depth exceeds its bound");
                }
                current = io.rowweave.stream.runtime
                        .FinalEventTimeDecimalWindow.nestedCheckpointMember(
                                current, checkpointPayloadStore);
            }
        }

        private void transferFromWorker(RemoteStreamWorkerEndpoint worker,
                byte[] checkpointControl, StreamCheckpointMember member) {
            FileCheckpointPayloadStore.StoredPayload payload =
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes());
            long offset = 0L;
            while (offset < payload.contentBytes()) {
                byte[] chunk = worker.fetchCheckpointPayload(
                        checkpointControl, payload.digest(),
                        payload.contentBytes(), offset);
                if (chunk.length == 0
                        || chunk.length > payload.contentBytes() - offset) {
                    throw new IllegalStateException(
                            "remote checkpoint payload chunk is invalid");
                }
                checkpointPayloadStore.acceptChunk(payload, offset, chunk);
                offset = Math.addExact(offset, chunk.length);
            }
            checkpointPayloadStore.verifyStored(payload);
        }

        private void distributeRestorePayloads(
                CheckpointTransferProof proof,
                Map<StreamTaskIdentity, byte[]> sourceAssignments) {
            if (checkpointPayloadStore == null) return;
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                byte[] restoreControl = launchCommand(state, assignment,
                        admitted.commandDigest, launchSequence,
                        restoreCheckpoint, admitted.parallelismOverride,
                        admitted.nodeMinimumParallelism);
                RemoteStreamWorkerEndpoint worker = workers.get(
                        assignment.workerId());
                for (StreamCheckpointMember member : proof.membersFor(
                        assignment,
                        sourceAssignments.get(assignment.identity()),
                        sourcePositionRequired.getOrDefault(
                                assignment.identity(), false))) {
                    distributePayloadClosure(worker, restoreControl, member);
                }
            }
        }

        private void distributePayloadClosure(
                RemoteStreamWorkerEndpoint worker, byte[] restoreControl,
                StreamCheckpointMember root) {
            StreamCheckpointMember current = root;
            java.util.Set<String> visited = new java.util.HashSet<>();
            int depth = 0;
            while (payloadBacked(current)
                    && visited.add(current.contentDigest())) {
                transferToWorker(worker, restoreControl, current);
                if (++depth > 8) {
                    throw new IllegalStateException(
                            "checkpoint payload dependency depth exceeds its bound");
                }
                current = io.rowweave.stream.runtime
                        .FinalEventTimeDecimalWindow.nestedCheckpointMember(
                                current, checkpointPayloadStore);
            }
        }

        private void transferToWorker(RemoteStreamWorkerEndpoint worker,
                byte[] restoreControl, StreamCheckpointMember member) {
            FileCheckpointPayloadStore.StoredPayload payload =
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes());
            checkpointPayloadStore.verifyStored(payload);
            long offset = 0L;
            while (offset < payload.contentBytes()) {
                byte[] chunk = checkpointPayloadStore.readChunk(
                        payload, offset,
                        FileCheckpointPayloadStore.TRANSFER_CHUNK_BYTES);
                worker.storeCheckpointPayload(restoreControl,
                        payload.digest(), payload.contentBytes(),
                        offset, chunk);
                offset = Math.addExact(offset, chunk.length);
            }
        }

        private static boolean payloadBacked(
                StreamCheckpointMember member) {
            return member != null && member.contentBytes() > 0L
                    && (member.kind()
                    == StreamCheckpointMember.Kind.SOURCE_POSITION
                    || member.kind() == StreamCheckpointMember.Kind.STATE
                    || member.kind()
                    == StreamCheckpointMember.Kind.CHANNEL_STATE
                    || member.kind()
                    == StreamCheckpointMember.Kind.SOURCE_COORDINATOR);
        }

        private CheckpointDecision recoverableDecision() {
            CheckpointIdentity latest = runAuthority.current(state.runId())
                    .latestCompletedCheckpoint();
            long recorded = latest == null
                    ? 0L : latest.checkpointSequence();
            CheckpointDecision recoverable = null;
            for (CheckpointDecision decision
                    : checkpointAuthority.decisionsForRun(state.runId())) {
                CheckpointIdentity identity = decision.checkpointIdentity();
                if (!state.attemptId().equals(identity.attemptId())
                        || state.fenceToken() != identity.fenceToken()
                        || identity.checkpointSequence() <= recorded
                        || (decision.status()
                        != CheckpointDecision.Status.COMMIT_DECIDED
                        && decision.status()
                        != CheckpointDecision.Status.COMPLETED)) {
                    continue;
                }
                if (recoverable != null) {
                    throw new IllegalStateException(
                            "multiple unpublished Stream checkpoints require reconciliation");
                }
                recoverable = decision;
            }
            return recoverable;
        }

        private CheckpointIdentity publishRecoveredCheckpoint(
                String checkpointId, CheckpointDecision durable,
                long deadlineNanos) {
            synchronized (owner.checkpointRetentionLock) {
                return publishRecoveredCheckpointExclusive(
                        checkpointId, durable, deadlineNanos);
            }
        }

        private CheckpointIdentity publishRecoveredCheckpointExclusive(
                String checkpointId, CheckpointDecision durable,
                long deadlineNanos) {
            if (durable == null) {
                throw new IllegalStateException(
                        "checkpoint operation reference has no durable decision");
            }
            CheckpointIdentity checkpoint = durable.checkpointIdentity();
            try {
                CheckpointDecision completed = durable;
                if (durable.status()
                        == CheckpointDecision.Status.COMMIT_DECIDED) {
                    completeTransactionalSinks(
                            checkpoint, durable, deadlineNanos);
                    completed = checkpointAuthority.complete(
                            CheckpointDecision.completed(durable));
                }
                if (completed.status()
                        != CheckpointDecision.Status.COMPLETED) {
                    throw new IllegalStateException(
                            "recoverable Stream checkpoint is not committed");
                }
                completeResidentSources(
                        checkpoint, completed, deadlineNanos);
                List<StreamCheckpointMember> resolved = checkpointMemberStore
                        .resolve(completed, checkpointAuthority);
                StreamCdcPhase completedCdcPhase =
                        !admitted.residentCdcSources().isEmpty()
                        ? decodeCdcPhase(resolved) : cdcPhase;
                publishCompletedCheckpoint(
                        checkpointId, completed, completedCdcPhase);
                owner.retainCompletedCheckpointsIfIdleExclusive(
                        checkpointPayloadStore,
                        state.runId(), admitted.job.checkpointConfig()
                                .retainedCompleted());
            } catch (Throwable pending) {
                throw new CheckpointCommitPendingException(
                        "durable Stream checkpoint commit requires reconciliation",
                        pending);
            }
            return checkpoint;
        }

        private void publishCompletedCheckpoint(
                String checkpointId, CheckpointDecision completed,
                StreamCdcPhase completedCdcPhase) {
            CheckpointIdentity checkpoint = completed.checkpointIdentity();
            if (savepointStore != null) {
                savepointStore.create(checkpointId, checkpoint,
                        checkpointAuthority);
            }
            CheckpointIdentity recorded = runAuthority.current(state.runId())
                    .latestCompletedCheckpoint();
            if (recorded == null
                    || checkpoint.checkpointSequence()
                    > recorded.checkpointSequence()) {
                runAuthority.recordCompletedCheckpoint(
                        state.runId(), state.coordinatorTerm(),
                        state.coordinatorIncarnation(), completed);
            }
            cdcPhase = completedCdcPhase;
            if (checkpointId.matches("stream-checkpoint:[0-9a-f]{64}")) {
                List<StreamCheckpointMember> canonical = checkpointMemberStore
                        .resolve(completed, checkpointAuthority);
                long contentBytes = 0L;
                long managedStateBytes = 0L;
                for (StreamCheckpointMember member : canonical) {
                    contentBytes = Math.addExact(
                            contentBytes, member.contentBytes());
                    if (member.kind() == StreamCheckpointMember.Kind.STATE) {
                        managedStateBytes = Math.addExact(
                                managedStateBytes, member.contentBytes());
                    }
                }
                completedCheckpointEvidence =
                        new StreamCompletedCheckpointEvidence(checkpointId,
                                checkpoint.checkpointSequence(), contentBytes,
                                managedStateBytes);
            }
        }

        private static void requireCheckpointNotInterrupted() {
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException(
                        "Stream checkpoint cut exceeded its lifecycle deadline");
            }
        }

        private long checkpointDeadlineNanos() {
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(
                    admitted.job.checkpointConfig().timeoutMillis());
            long now = System.nanoTime();
            long deadline = now + timeoutNanos;
            return deadline < now ? Long.MAX_VALUE : deadline;
        }

        private static int remainingTimeoutMillis(long deadlineNanos) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L) {
                throw new IllegalStateException(
                        "Stream checkpoint cut exceeded its configured timeout");
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
            if (TimeUnit.MILLISECONDS.toNanos(millis) < remaining) millis++;
            return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, millis));
        }

        private boolean isSource(
                StreamWorkerPlacement.Assignment assignment) {
            return admitted.executionGraph.node(assignment.operatorUid())
                    .portableNode().opcode() == StreamOperatorCode.SOURCE;
        }

        private List<StreamCheckpointMember.Kind> expectedKinds(
                StreamWorkerPlacement.Assignment assignment) {
            PortableStreamNode node = admitted.executionGraph.node(
                    assignment.operatorUid()).portableNode();
            if (node.opcode() == StreamOperatorCode.SOURCE) {
                return Collections.singletonList(
                        StreamCheckpointMember.Kind.SOURCE_POSITION);
            }
            List<StreamCheckpointMember.Kind> expected = new ArrayList<>();
            if (node.opcode() == StreamOperatorCode.SINK) {
                StreamProductExecutionGraph.SinkRoute sink = admitted.sink(
                        node.operatorUid());
                expected.add(
                        sink.binding.visibilityProfile()
                        == io.rowweave.stream.contract.StreamSinkBinding
                        .VisibilityProfile.CHECKPOINT_TRANSACTIONAL
                        ? StreamCheckpointMember.Kind.TRANSACTIONAL_SINK
                        : StreamCheckpointMember.Kind.EAGER_SINK_CAPABILITY);
            } else {
                int count;
                switch (node.opcode()) {
                    case EVENT_TIME_WINDOW:
                    case GENERIC_WINDOW:
                    case KEYED_AGGREGATE:
                        count = 1;
                        break;
                    case TEMPORAL_JOIN:
                        count = 2;
                        break;
                    case KEYED_PROCESS:
                    case KEYED_CO_PROCESS:
                        StreamProcessBinding process =
                                (StreamProcessBinding) node.binding();
                        count = process.managedStates().size()
                                + process.timerFamilies().size()
                                + (process.broadcastState() == null ? 0 : 2)
                                + (node.opcode()
                                == StreamOperatorCode.KEYED_CO_PROCESS
                                ? 1 : 0);
                        break;
                    case ASYNC_WAIT:
                    case ASSIGN_EVENT_TIME:
                    case UNION:
                        count = 1;
                        break;
                    default:
                        count = 0;
                        break;
                }
                expected.addAll(Collections.nCopies(count,
                        StreamCheckpointMember.Kind.STATE));
            }
            if (admitted.job.checkpointConfig().mode()
                    == io.rowweave.stream.api.StreamCheckpointMode.UNALIGNED) {
                expected.addAll(Collections.nCopies(
                        incomingLaneCount(assignment),
                        StreamCheckpointMember.Kind.CHANNEL_STATE));
            }
            return Collections.unmodifiableList(expected);
        }

        private int incomingLaneCount(
                StreamWorkerPlacement.Assignment assignment) {
            int count = 0;
            StreamPhysicalExecutionPlan physical =
                    StreamPhysicalExecutionPlan.from(
                            admitted.executionGraph, state.placement());
            for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
                if (lane.target().equals(assignment.identity())) count++;
            }
            return count;
        }

        private StreamCdcPhase initialCdcPhase() {
            if (admitted.residentCdcSources().isEmpty()) {
                return StreamCdcPhase.NOT_APPLICABLE;
            }
            if (restoreCheckpoint == null) return StreamCdcPhase.SNAPSHOT;
            CheckpointDecision decision = checkpointAuthority.decision(
                    restoreCheckpoint);
            return decodeCdcPhase(checkpointMemberStore.resolve(
                    decision, checkpointAuthority));
        }

        private StreamCdcPhase decodeCdcPhase(
                List<StreamCheckpointMember> members) {
            if (checkpointPayloadStore == null || checkpointRoot == null) {
                throw new IllegalStateException(
                        "resident CDC progress requires checkpoint storage");
            }
            StreamCdcPhase phase = StreamCdcPhase.LOG;
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.residentCdcSources()) {
                StreamCheckpointMember sourcePosition = null;
                for (StreamCheckpointMember member : members) {
                    if (member.kind()
                            == StreamCheckpointMember.Kind.SOURCE_POSITION
                            && source.node.operatorUid().equals(
                            member.operatorUid())
                            && "STREAM_CDC_ENUMERATOR_V1".equals(
                            member.codecId())) {
                        if (sourcePosition != null) {
                            throw new IllegalStateException(
                                    "resident CDC checkpoint has multiple positions");
                        }
                        sourcePosition = member;
                    }
                }
                if (sourcePosition == null) {
                    throw new IllegalStateException(
                            "resident CDC checkpoint has no source position");
                }
                StreamSourcePositionCheckpoint position =
                        new StreamSourcePositionCheckpoint(
                                admitted.artifact.artifactId(),
                                source.node.operatorUid(), 0,
                                source.binding,
                                source.binding.fixedSplits().get(0),
                                checkpointRoot.resolve("cdc-progress"),
                                checkpointPayloadStore, 0L,
                                source.binding.fixedSplits().get(0)
                                        .descriptorFrame());
                position.restore(sourcePosition);
                StreamCdcEnumeratorState state = StreamCdcEnumeratorStateCodec
                        .decode(position.currentPositionFrame());
                if (state.phase()
                        != StreamCdcEnumeratorState.Phase.LOG) {
                    phase = StreamCdcPhase.SNAPSHOT;
                }
            }
            return phase;
        }

        private void completeTransactionalSinks(
                CheckpointIdentity checkpoint,
                CheckpointDecision decision, long deadlineNanos) {
            if (!hasTransactionalSink()) return;
            CheckpointTransferProof checkpointProof = checkpointTransferProof(
                    decision, checkpointAuthority, checkpointMemberStore);
            RuntimeException lastFailure = null;
            do {
                requireCheckpointNotInterrupted();
                long completionSequence = nextControlSequence();
                try {
                    for (StreamWorkerPlacement.Assignment assignment
                            : state.placement().assignments()) {
                        PortableStreamNode node = admitted.executionGraph.node(
                                assignment.operatorUid()).portableNode();
                        if (node.opcode() != StreamOperatorCode.SINK
                                || ((StreamSinkBinding) node.binding())
                                .visibilityProfile()
                                != StreamSinkBinding.VisibilityProfile
                                .CHECKPOINT_TRANSACTIONAL) {
                            continue;
                        }
                        workers.get(assignment.workerId())
                                .completeCheckpoint(checkpointCommand(
                                        state, assignment,
                                        admitted.commandDigest,
                                        completionSequence, checkpoint),
                                        assignment.operatorUid(),
                                        assignment.subtaskIndex(),
                                        remainingTimeoutMillis(deadlineNanos),
                                        checkpointProof.forTask(assignment));
                    }
                    return;
                } catch (RuntimeException failure) {
                    lastFailure = failure;
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException(
                            "transactional Sink reconciliation was interrupted",
                            lastFailure);
                }
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "transactional Sink reconciliation was interrupted",
                            lastFailure);
                }
            } while (System.nanoTime() - deadlineNanos < 0L);
            throw new IllegalStateException(
                    "transactional Sink did not reconcile before checkpoint timeout",
                    lastFailure);
        }

        private void completeResidentSources(
                CheckpointIdentity checkpoint,
                CheckpointDecision completed, long deadlineNanos) {
            boolean required = false;
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                PortableStreamNode node = admitted.executionGraph.node(
                        assignment.operatorUid()).portableNode();
                if (node.opcode() == StreamOperatorCode.SOURCE
                        && "STREAM_CDC_ENUMERATOR_V2".equals(
                        node.scalarParameters().get(
                                "source.enumeratorStateFormat"))) {
                    required = true;
                    break;
                }
            }
            if (!required) return;
            CheckpointTransferProof checkpointProof = checkpointTransferProof(
                    completed, checkpointAuthority, checkpointMemberStore);
            RuntimeException lastFailure = null;
            do {
                requireCheckpointNotInterrupted();
                long completionSequence = nextControlSequence();
                try {
                    for (StreamWorkerPlacement.Assignment assignment
                            : state.placement().assignments()) {
                        PortableStreamNode node = admitted.executionGraph.node(
                                assignment.operatorUid()).portableNode();
                        if (node.opcode() != StreamOperatorCode.SOURCE
                                || !"STREAM_CDC_ENUMERATOR_V2".equals(
                                node.scalarParameters().get(
                                        "source.enumeratorStateFormat"))) {
                            continue;
                        }
                        workers.get(assignment.workerId())
                                .completeCheckpoint(checkpointCommand(
                                        state, assignment,
                                        admitted.commandDigest,
                                        completionSequence, checkpoint),
                                        assignment.operatorUid(),
                                        assignment.subtaskIndex(),
                                        remainingTimeoutMillis(deadlineNanos),
                                        checkpointProof.completedForTask(
                                                assignment));
                    }
                    return;
                } catch (RuntimeException failure) {
                    lastFailure = failure;
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException(
                            "resident CDC source completion was interrupted",
                            lastFailure);
                }
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "resident CDC source completion was interrupted",
                            interrupted);
                }
            } while (System.nanoTime() < deadlineNanos);
            throw new IllegalStateException(
                    "resident CDC source completion did not converge",
                    lastFailure);
        }

        private boolean hasTransactionalSink() {
            for (StreamProductExecutionGraph.SinkRoute sink : admitted.sinks) {
                if (sink.binding.visibilityProfile()
                        == io.rowweave.stream.contract.StreamSinkBinding
                        .VisibilityProfile.CHECKPOINT_TRANSACTIONAL) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public synchronized void stopWithSavepoint(
                io.rowweave.stream.api.StreamCommandId commandId,
                String savepointId,
                io.rowweave.StreamSavepointRequest request) {
            if (request == null
                    || !"file-savepoint-v1".equals(request.providerKey())) {
                throw new UnsupportedOperationException(
                        "Stream savepoint provider is unavailable");
            }
            if (savepointStore == null || checkpointAuthority == null) {
                throw new UnsupportedOperationException(
                        "savepoint storage is unavailable");
            }
            CheckpointIdentity checkpoint = checkpointCut(
                    commandId, savepointId);
            savepointStore.create(savepointId, checkpoint,
                    checkpointAuthority);
            try {
                cancel();
            } catch (Throwable pending) {
                throw new CheckpointCommitPendingException(
                        "durable savepoint requires terminal reconciliation",
                        pending);
            }
        }

        private void addTaskCut(
                CheckpointIdentity checkpoint,
                StreamWorkerPlacement.Assignment assignment,
                List<StreamCheckpointMember> members,
                List<StreamCheckpointMember.Kind> expectedKinds,
                List<CheckpointExpectedTask> expected,
                List<CheckpointTaskReceipt> receipts) {
            if (members == null || expectedKinds == null
                    || members.size() != expectedKinds.size()) {
                throw new IllegalStateException(
                        "remote Stream task returned an incomplete checkpoint cut");
            }
            List<CheckpointEvidence> evidence = new ArrayList<>(members.size());
            List<CheckpointEvidenceKey> keys = new ArrayList<>(members.size());
            List<StreamCheckpointMember.Kind> remaining =
                    new ArrayList<>(expectedKinds);
            for (StreamCheckpointMember member : members) {
                if (!remaining.remove(member.kind())) {
                    throw new IllegalStateException(
                            "remote Stream task returned an unexpected checkpoint member");
                }
                checkpointMemberStore.publish(member);
                CheckpointEvidence value = member.toEvidence();
                evidence.add(value);
                keys.add(value.key());
            }
            if (!remaining.isEmpty()) {
                throw new IllegalStateException(
                        "remote Stream task omitted a checkpoint member");
            }
            String taskId = "stream-task:" + assignment.taskOrdinal();
            expected.add(new CheckpointExpectedTask(
                    taskId, state.assignmentGeneration(),
                    assignment.workerIncarnation(), keys));
            receipts.add(new CheckpointTaskReceipt(
                    checkpoint, taskId, state.assignmentGeneration(),
                    assignment.workerIncarnation(), evidence));
        }

        private synchronized long nextControlSequence() {
            controlSequence = Math.addExact(controlSequence, 1L);
            return controlSequence;
        }

        @Override
        public Throwable failureDuringCheckpoint() {
            if (!started.get() || canceled.get()) return null;
            StreamWorkerLossException unavailable = unavailableWorker();
            return unavailable != null ? unavailable : sourceDiscoveryFailure;
        }

        @Override
        public Throwable failure() {
            if (!started.get() || canceled.get()) return null;
            StreamWorkerLossException unavailable = unavailableWorker();
            if (unavailable != null) return unavailable;
            synchronized (this) {
                if (!started.get() || canceled.get()) return null;
                if (!sourceCoordinators.isEmpty()
                        && sourceDiscoveryFailure == null) {
                    try {
                        discoverAndAssignLateSplits();
                    } catch (Throwable failure) {
                        sourceDiscoveryFailure = failure;
                    }
                }
                unavailable = unavailableWorker();
                if (unavailable != null) return unavailable;
                if (sourceDiscoveryFailure != null) {
                    return sourceDiscoveryFailure;
                }
                for (StreamWorkerPlacement.Assignment assignment
                        : state.placement().assignments()) {
                    try {
                        String taskFailure = workers.get(assignment.workerId())
                                .taskFailureMessage(
                                launchCommand(state, assignment,
                                        admitted.commandDigest, launchSequence,
                                        restoreCheckpoint,
                                        admitted.parallelismOverride,
                                        admitted.nodeMinimumParallelism),
                                assignment.operatorUid(),
                                assignment.subtaskIndex());
                        if (taskFailure != null) {
                            return new IllegalStateException(
                                    "remote Stream operator task failed at "
                                            + assignment.operatorUid() + ": "
                                            + taskFailure);
                        }
                    } catch (RuntimeException workerFailure) {
                        return new StreamWorkerLossException(
                                assignment, workerFailure);
                    }
                }
                return null;
            }
        }

        @Override
        public synchronized boolean finished() {
            if (!started.get() || canceled.get() || !allSourcesBounded()) {
                return false;
            }
            if (restoreCheckpoint != null && restoredSourcesTerminal()) {
                return true;
            }
            long now = System.nanoTime();
            if (now - nextFinishPollNanos < 0L) return false;
            nextFinishPollNanos = now + TimeUnit.MILLISECONDS.toNanos(250L);
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                if (!taskCompleted(assignment)) return false;
            }
            return true;
        }

        @Override
        public synchronized boolean terminalCheckpointReady() {
            if (!started.get() || canceled.get() || !allSourcesBounded()) {
                return false;
            }
            boolean transactional = false;
            for (StreamProductExecutionGraph.SinkRoute sink
                    : admitted.sinks) {
                transactional |= sink.binding.visibilityProfile()
                        == StreamSinkBinding.VisibilityProfile
                        .CHECKPOINT_TRANSACTIONAL;
            }
            if (!transactional) return false;
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                if (admitted.executionGraph.node(
                        assignment.operatorUid()).portableNode().opcode()
                        == StreamOperatorCode.SOURCE
                        && !taskCompleted(assignment)) return false;
            }
            return true;
        }

        private boolean allSourcesBounded() {
            if (admitted.sources.isEmpty()) return false;
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.sources) {
                if (!"EXPLICIT_END_OF_INPUT".equals(
                        source.node.scalarParameters().get(
                                "source.termination"))) return false;
            }
            return true;
        }

        private boolean restoredSourcesTerminal() {
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.sources) {
                StreamSourceCoordinator coordinator = sourceCoordinators.get(
                        source.node.operatorUid());
                if (coordinator == null
                        || coordinator.state().splits().isEmpty()) {
                    return false;
                }
                for (StreamSourceCoordinatorSplit split
                        : coordinator.state().splits()) {
                    if (split.status()
                            != StreamSourceCoordinatorSplit.Status.FINISHED) {
                        return false;
                    }
                }
            }
            return true;
        }

        private boolean taskCompleted(
                StreamWorkerPlacement.Assignment assignment) {
            byte[] control = launchCommand(state, assignment,
                    admitted.commandDigest, launchSequence,
                    restoreCheckpoint, admitted.parallelismOverride,
                    admitted.nodeMinimumParallelism);
            boolean completed = workers.get(assignment.workerId()).taskCompleted(
                    control, assignment.operatorUid(),
                    assignment.subtaskIndex());
            if (completed && monitorProjection != null) {
                completedTasks.add(assignment.identity());
            }
            return completed;
        }

        private StreamWorkerLossException unavailableWorker() {
            java.util.Set<String> reachable = new java.util.HashSet<>();
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                if (!reachable.add(assignment.workerId())) continue;
                try {
                    workers.get(assignment.workerId()).requireReachable();
                } catch (RuntimeException workerFailure) {
                    return new StreamWorkerLossException(
                            assignment, workerFailure);
                }
            }
            return null;
        }

        @Override public StreamCdcPhase cdcPhase() {
            return cdcPhase;
        }

        @Override
        public StreamCompletedCheckpointEvidence
                completedCheckpointEvidence() {
            return completedCheckpointEvidence;
        }

        @Override
        public synchronized StreamRunProgressSnapshot progressSnapshot() {
            for (StreamWorkerPlacement.Assignment assignment
                    : state.placement().assignments()) {
                RemoteStreamWorkerEndpoint worker = workers.get(
                        assignment.workerId());
                if (worker == null
                        || !worker.profile()
                        .supportsProgressObservation()) {
                    continue;
                }
                try {
                    progressAggregator.offer(worker.taskProgress(
                            launchCommand(state, assignment,
                                    admitted.commandDigest, launchSequence,
                                    restoreCheckpoint,
                                    admitted.parallelismOverride,
                                    admitted.nodeMinimumParallelism),
                            assignment.operatorUid(),
                            assignment.subtaskIndex()));
                } catch (RuntimeException ignored) {
                    // Observation is advisory; computation remains authoritative.
                }
            }
            return new StreamRunProgressSnapshot(
                    state.runId(), state.attemptId(), state.fenceToken(),
                    state.coordinatorTerm(), state.assignmentGeneration(),
                    progressAggregator.snapshot(0, 0L, 0, cdcPhase),
                    monitorProjection == null ? Collections.emptyList()
                            : monitorProjection.stages(progressAggregator.samples(),
                                    completedTasks));
        }

        @Override public void close() { cancel(); }
    }
}
