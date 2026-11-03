package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcCodec;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RecoveryFencedStageAssignmentAuthority;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Master ACK is strictly after durable terminal authority. */
class MasterDurableTerminalReportGateTest {

    @Test
    void providerSuccessAcksOnlyAfterIndependentPreparedCasAndRetriesIt()
            throws Exception {
        DelayedPreparedRegistry registry = new DelayedPreparedRegistry();
        RunStateStore runs = registry.runStateStore();
        RunSnapshot run = runs.apply(RunCommand.createRun(
                "create-provider-terminal", "run-provider-terminal",
                "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = runs.apply(RunCommand.createAttempt(
                "attempt-provider-terminal", run.runId(), run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                Collections.singletonList(assignment));
        StageAssignmentSnapshot published = registry.authority
                .publish(manifest).snapshot();
        byte[] commandDigest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        java.util.Arrays.fill(commandDigest, (byte) 7);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                published.token(0), commandDigest, 3L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                registry.authority.accept(acceptance).code());
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                acceptance.token(), commandDigest, 9L, 7L, 5L);
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(meta),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, assignment, "sink-op", descriptor);
        PreparedSinkLocator locator = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                descriptor.capabilityProfile(), descriptor.commitDomainId(),
                new byte[]{1, 2, 3}, 3L,
                java.security.MessageDigest.getInstance("SHA-256")
                        .digest(new byte[]{4, 5, 6}),
                12L);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                acceptance.token(), commandDigest, locator);
        RpcMessage report = RpcMessage.durableStageReport(
                "worker", terminal, prepared);

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage first = master.handler().apply(report);
            assertEquals(RpcMessage.Type.JOB_RESULT, first.type(),
                    "prepared CAS unavailability must withhold ACK");
            assertEquals(terminal, registry.store.assignmentSnapshot(fence)
                    .terminal(0), "terminal CAS may commit before locator CAS");
            assertEquals(null, registry.store.assignmentSnapshot(fence)
                    .preparedSink(0));

            RpcMessage replay = master.handler().apply(report);
            assertEquals(RpcMessage.Type.ACK, replay.type(), replay.errorMessage());
            assertEquals(prepared, registry.store.assignmentSnapshot(fence)
                    .preparedSink(0));
            assertEquals(2, registry.preparedCalls.get(),
                    "ALREADY_RECORDED terminal replay must retry locator CAS");
        }
    }

    @Test
    void corruptPreparedEvidenceIsRejectedBeforeTerminalMutation()
            throws Exception {
        DelayedPreparedRegistry registry = new DelayedPreparedRegistry();
        RunStateStore runs = registry.runStateStore();
        RunSnapshot run = runs.apply(RunCommand.createRun(
                "create-corrupt-prepared", "run-corrupt-prepared",
                "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = runs.apply(RunCommand.createAttempt(
                "attempt-corrupt-prepared", run.runId(), run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 102L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                Collections.singletonList(assignment));
        StageAssignmentSnapshot published = registry.authority
                .publish(manifest).snapshot();
        byte[] commandDigest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        java.util.Arrays.fill(commandDigest, (byte) 8);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                published.token(0), commandDigest, 3L);
        registry.authority.accept(acceptance);
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                acceptance.token(), commandDigest, 1L, 1L, 4L);
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(meta),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                acceptance.token(), commandDigest, new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION,
                SinkAttemptIdentity.forAssignment(
                        manifest, assignment, "sink-op", descriptor),
                descriptor.capabilityProfile(), descriptor.commitDomainId(),
                new byte[]{7}, 1L,
                java.security.MessageDigest.getInstance("SHA-256")
                        .digest(new byte[]{8}),
                1L));
        RpcMessage valid = RpcMessage.durableStageReport(
                "worker", terminal, prepared);
        byte[] wire = RpcCodec.encode(valid);
        byte[] preparedBytes = valid.preparedSinkReceiptBytes();
        int offset = indexOf(wire, preparedBytes);
        assertTrue(offset >= 0);
        wire[offset + preparedBytes.length - 1] ^= 1;
        RpcMessage corrupted = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(wire)));

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(corrupted);
            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertEquals(null, registry.store.assignmentSnapshot(fence).terminal(0),
                    "all evidence must decode before the first authority mutation");
        }
    }

    @Test
    void legacyMasterContractConsumesNewWorkerBaseReceiptAndIgnoresTelemetry()
            throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-legacy-master", "attempt-1", 1L);
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 103L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(
                new StageAssignmentManifest(fence, "artifact", 1L,
                        Collections.singletonList(assignment)));
        byte[] digest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        java.util.Arrays.fill(digest, (byte) 4);
        StageTerminalReceipt current = StageTerminalReceipt.succeeded(
                snapshot.token(0), digest, 8L, 7L, 6L,
                StageExecutionTelemetry.complete(1L, 2L, 3L, 4L, 5L, 6L));

        RpcMessage wireDecoded = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(RpcCodec.encode(
                        RpcMessage.durableStageReport("worker", current)))));
        StageTerminalReceipt legacyBase =
                io.rowweave.run.StageAssignmentManifestCodec.decodeTerminalReceipt(
                        wireDecoded.stageTerminalReceiptBytes());

        assertEquals(3, (wireDecoded.stageTerminalReceiptBytes()[4] & 0xff) << 8
                | wireDecoded.stageTerminalReceiptBytes()[5] & 0xff,
                "the standalone Worker-to-Master receipt must retain the v3 wire");
        assertEquals(TelemetryCoverage.UNAVAILABLE, legacyBase.telemetry().coverage());
        assertEquals(current.token(), legacyBase.token());
        assertEquals(current.commandDigest().length, legacyBase.commandDigest().length);
        assertEquals(current.inputRows(), legacyBase.inputRows());
        assertEquals(current.outputRows(), legacyBase.outputRows());
        assertEquals(current.succeeded(), legacyBase.succeeded());
        assertEquals(current.failureCategory(), legacyBase.failureCategory());
        assertNotNull(wireDecoded.executionTelemetryBytes(),
                "a new Master may consume the advisory payload independently");
    }

    @Test
    void recoveryClaimAbsorbsTerminalForPermanentlyUnacceptedToken() {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore runs = registry.runStateStore();
        RunSnapshot run = runs.apply(RunCommand.createRun(
                "create-unaccepted-terminal", "run-unaccepted-terminal",
                "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = runs.apply(RunCommand.createAttempt(
                "attempt-unaccepted-terminal", run.runId(), run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentSnapshot published = registry.durableStageAssignmentAuthority()
                .publish(new StageAssignmentManifest(
                        fence, run.artifactId(), 1L,
                        Collections.singletonList(assignment))).snapshot();
        run = runs.apply(RunCommand.advanceAttempt(
                "running-unaccepted-terminal", run.runId(), run.revision(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
        byte[] digest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        StageTerminalReceipt receipt = StageTerminalReceipt.canceled(
                published.token(0), digest, 0L, 0L, "LEADER_CHANGED",
                new byte[StageTerminalReceipt.DIGEST_BYTES], 4L);

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage beforeClaim = master.handler().apply(
                    RpcMessage.durableStageReport("worker", receipt));
            assertEquals(RpcMessage.Type.JOB_RESULT, beforeClaim.type(),
                    "without a recovery fence, missing acceptance remains retriable");

            registry.runRecoveryOwnership().claim(run.runId(), run.revision(), 0L,
                    new RecoveryOwner("replacement-master", 9L));
            RpcMessage afterClaim = master.handler().apply(
                    RpcMessage.durableStageReport("worker", receipt));

            assertEquals(RpcMessage.Type.ACK, afterClaim.type(), afterClaim.errorMessage());
            assertEquals(0, registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(fence).terminalCount(),
                    "an unaccepted token is absorbed, never promoted into terminal evidence");
        }
    }

    @Test
    void preparedLocatorUsesCurrentRecoveryOwnerAfterTerminalCasRotatesIt()
            throws Exception {
        RotatingRecoveryRegistry registry = new RotatingRecoveryRegistry();
        RunStateStore runs = registry.runStateStore();
        RunSnapshot run = runs.apply(RunCommand.createRun(
                "create-rotating-owner", "run-rotating-owner",
                "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = runs.apply(RunCommand.createAttempt(
                "attempt-rotating-owner", run.runId(), run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RecoveryToken owner = registry.runRecoveryOwnership().claim(
                run.runId(), run.revision(), 0L,
                new RecoveryOwner("recovery-master-1", 9L)).token();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 104L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                Collections.singletonList(assignment));
        StageAssignmentSnapshot published = registry.authority
                .publishRecovered(owner, manifest).snapshot();
        byte[] commandDigest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        java.util.Arrays.fill(commandDigest, (byte) 6);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                published.token(0), commandDigest, 3L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                registry.authority.acceptRecovered(owner, acceptance).code());
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                acceptance.token(), commandDigest, 1L, 1L, 4L);
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(meta),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                acceptance.token(), commandDigest, new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION,
                SinkAttemptIdentity.forAssignment(
                        manifest, assignment, "sink-op", descriptor),
                descriptor.capabilityProfile(), descriptor.commitDomainId(),
                new byte[]{7}, 1L,
                java.security.MessageDigest.getInstance("SHA-256")
                        .digest(new byte[]{8}),
                1L));

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.durableStageReport("worker", terminal, prepared));

            assertEquals(RpcMessage.Type.ACK, response.type(), response.errorMessage());
            assertEquals(prepared, registry.authority.assignmentSnapshot(fence)
                    .preparedSink(0));
            assertEquals(2L, registry.runRecoveryOwnership()
                    .ownership(run.runId()).ownershipEpoch());
        }
    }

    @Test
    void reportWithoutLocalStageTrackerStillCommitsAuthorityBeforeAck() {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore runs = registry.runStateStore();
        RunDecision created = runs.apply(RunCommand.createRun(
                "create-terminal-gate", "run-terminal-gate", "definition", "artifact",
                1, 0L, 1L));
        RunDecision attempted = runs.apply(RunCommand.createAttempt(
                "attempt-terminal-gate", "run-terminal-gate",
                created.snapshot().revision(), "attempt-1", "publication-1", 2L));
        RunSnapshot run = attempted.snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L, Collections.singletonList(assignment));
        StageAssignmentSnapshot published = registry.durableStageAssignmentAuthority()
                .publish(manifest).snapshot();
        byte[] commandDigest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        java.util.Arrays.fill(commandDigest, (byte) 7);
        registry.durableStageAssignmentAuthority().accept(new StageAssignmentAcceptance(
                published.token(0), commandDigest, 3L));
        runs.apply(RunCommand.advanceAttempt(
                "running-terminal-gate", run.runId(), run.revision(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 4L, null, null));
        StageExecutionTelemetry telemetry = StageExecutionTelemetry.complete(
                101L, 202L, 3L, 4L, 505L, 606L);
        StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                published.token(0), commandDigest, 9L, 7L, 5L, telemetry);

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.durableStageReport("worker", receipt));

            assertEquals(RpcMessage.Type.ACK, response.type(), response.errorMessage());
            StageAssignmentSnapshot after = registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(fence);
            assertNotNull(after);
            assertEquals(receipt, after.terminal(0),
                    "ACK must not precede the shared terminal receipt");
            assertEquals(telemetry, after.terminal(0).telemetry(),
                    "Master must atomically persist the decoded advisory facts with the base terminal");

            RunSnapshot latest = runs.get(run.runId());
            RunDecision failed = runs.apply(RunCommand.advanceAttempt(
                    "failed-terminal-gate", latest.runId(), latest.revision(),
                    latest.currentAttempt().attemptId(), latest.currentAttempt().fenceToken(),
                    AttemptStatus.FAILED, false, 6L, "FAILED", "terminal"));
            assertEquals(io.rowweave.run.RunStatus.FAILED,
                    failed.snapshot().status());
            assertEquals(null, registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(fence));
            assertEquals(RpcMessage.Type.ACK, master.handler().apply(
                    RpcMessage.durableStageReport("worker", receipt)).type(),
                    "exact late report after atomic terminal cleanup must be absorbed");
        }
    }

    @Test
    void corruptTelemetryCannotRejectOrRewriteAValidBaseTerminal() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore runs = registry.runStateStore();
        RunSnapshot run = runs.apply(RunCommand.createRun(
                "create-corrupt-terminal-telemetry", "run-corrupt-terminal-telemetry",
                "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = runs.apply(RunCommand.createAttempt(
                "attempt-corrupt-terminal-telemetry", run.runId(), run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 102L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentSnapshot published = registry.durableStageAssignmentAuthority()
                .publish(new StageAssignmentManifest(
                        fence, run.artifactId(), 1L,
                        Collections.singletonList(assignment))).snapshot();
        byte[] commandDigest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        java.util.Arrays.fill(commandDigest, (byte) 9);
        registry.durableStageAssignmentAuthority().accept(new StageAssignmentAcceptance(
                published.token(0), commandDigest, 3L));
        StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                published.token(0), commandDigest, 9L, 7L, 5L,
                StageExecutionTelemetry.complete(1L, 2L, 3L, 4L, 5L, 6L));
        RpcMessage valid = RpcMessage.durableStageReport("worker", receipt);
        byte[] wire = RpcCodec.encode(valid);
        byte[] telemetry = valid.executionTelemetryBytes();
        int offset = indexOf(wire, telemetry);
        assertTrue(offset >= 0);
        wire[offset + telemetry.length - 1] ^= 1;
        RpcMessage corrupted = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(wire)));

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(corrupted);

            assertEquals(RpcMessage.Type.ACK, response.type(), response.errorMessage());
            StageTerminalReceipt stored = registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(fence).terminal(0);
            assertEquals(7L, stored.outputRows());
            assertEquals(TelemetryCoverage.UNAVAILABLE, stored.telemetry().coverage());
        }
    }

    private static int indexOf(byte[] bytes, byte[] needle) {
        outer:
        for (int i = 0; i <= bytes.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (bytes[i + j] != needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    }

    private static final class DelayedPreparedRegistry implements ServiceRegistry,
            io.rowweave.run.RunStateStoreProvider,
            DurableStageAssignmentAuthorityProvider {
        private final InProcessRegistry workers = new InProcessRegistry();
        private final io.rowweave.run.InMemoryRunStateStore store =
                new io.rowweave.run.InMemoryRunStateStore();
        private final AtomicInteger preparedCalls = new AtomicInteger();
        private final DurableStageAssignmentAuthority authority =
                new DurableStageAssignmentAuthority() {
                    @Override public StageManifestPublication publish(
                            StageAssignmentManifest manifest) {
                        return store.publish(manifest);
                    }
                    @Override public StageAssignmentSnapshot assignmentSnapshot(
                            RunAttemptFence fence) {
                        return store.assignmentSnapshot(fence);
                    }
                    @Override public StageAcceptanceDecision accept(
                            StageAssignmentAcceptance acceptance) {
                        return store.accept(acceptance);
                    }
                    @Override public StageTerminalDecision recordTerminal(
                            StageTerminalReceipt receipt) {
                        return store.recordTerminal(receipt);
                    }
                    @Override public PreparedSinkDecision recordPreparedSink(
                            PreparedSinkReceipt receipt) {
                        if (preparedCalls.incrementAndGet() == 1) {
                            return PreparedSinkDecision.rejected(
                                    PreparedSinkDecision.Code.UNAVAILABLE);
                        }
                        return store.recordPreparedSink(receipt);
                    }
                };

        @Override public RunStateStore runStateStore() { return store; }
        @Override public DurableStageAssignmentAuthority
        durableStageAssignmentAuthority() { return authority; }
        @Override public void register(RegistryWorker worker) {
            workers.register(worker);
        }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return workers.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { workers.report(worker); }
        @Override public void renew(String workerId) { workers.renew(workerId); }
        @Override public void deregister(String workerId) {
            workers.deregister(workerId);
        }
        @Override public List<RegistryWorker> liveWorkers() {
            return workers.liveWorkers();
        }
        @Override public void watch(WatchListener listener) { workers.watch(listener); }
    }

    private static final class RotatingRecoveryRegistry implements ServiceRegistry,
            io.rowweave.run.RunStateStoreProvider,
            io.rowweave.run.RunRecoveryOwnershipProvider,
            DurableStageAssignmentAuthorityProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final RecoveryFencedStageAssignmentAuthority delegateAuthority =
                (RecoveryFencedStageAssignmentAuthority)
                        delegate.durableStageAssignmentAuthority();
        private final AtomicInteger terminalCalls = new AtomicInteger();
        private final RecoveryFencedStageAssignmentAuthority authority =
                new RecoveryFencedStageAssignmentAuthority() {
                    @Override public StageManifestPublication publish(
                            StageAssignmentManifest manifest) {
                        return delegateAuthority.publish(manifest);
                    }
                    @Override public StageManifestPublication publishRecovered(
                            RecoveryToken token, StageAssignmentManifest manifest) {
                        return delegateAuthority.publishRecovered(token, manifest);
                    }
                    @Override public StageAssignmentSnapshot assignmentSnapshot(
                            RunAttemptFence fence) {
                        return delegateAuthority.assignmentSnapshot(fence);
                    }
                    @Override public StageAcceptanceDecision accept(
                            StageAssignmentAcceptance acceptance) {
                        return delegateAuthority.accept(acceptance);
                    }
                    @Override public StageAcceptanceDecision acceptRecovered(
                            RecoveryToken token,
                            StageAssignmentAcceptance acceptance) {
                        return delegateAuthority.acceptRecovered(token, acceptance);
                    }
                    @Override public StageTerminalDecision recordTerminal(
                            StageTerminalReceipt receipt) {
                        return delegateAuthority.recordTerminal(receipt);
                    }
                    @Override public StageTerminalDecision recordTerminalRecovered(
                            RecoveryToken token, StageTerminalReceipt receipt) {
                        StageTerminalDecision decision = delegateAuthority
                                .recordTerminalRecovered(token, receipt);
                        if (terminalCalls.incrementAndGet() == 1
                                && decision.code()
                                == StageTerminalDecision.Code.RECORDED) {
                            RunSnapshot current = runStateStore().get(token.runId());
                            delegate.runRecoveryOwnership().claim(
                                    token.runId(), current.revision(),
                                    token.ownershipEpoch(),
                                    new RecoveryOwner("recovery-master-2", 10L));
                        }
                        return decision;
                    }
                    @Override public PreparedSinkDecision recordPreparedSink(
                            PreparedSinkReceipt receipt) {
                        return delegateAuthority.recordPreparedSink(receipt);
                    }
                    @Override public PreparedSinkDecision recordPreparedSinkRecovered(
                            RecoveryToken token, PreparedSinkReceipt receipt) {
                        return delegateAuthority.recordPreparedSinkRecovered(token, receipt);
                    }
                };

        @Override public RunStateStore runStateStore() {
            return delegate.runStateStore();
        }
        @Override public io.rowweave.run.RunRecoveryOwnership
        runRecoveryOwnership() {
            return delegate.runRecoveryOwnership();
        }
        @Override public DurableStageAssignmentAuthority
        durableStageAssignmentAuthority() {
            return authority;
        }
        @Override public void register(RegistryWorker worker) {
            delegate.register(worker);
        }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) {
            delegate.report(worker);
        }
        @Override public void renew(String workerId) {
            delegate.renew(workerId);
        }
        @Override public void deregister(String workerId) {
            delegate.deregister(workerId);
        }
        @Override public List<RegistryWorker> liveWorkers() {
            return delegate.liveWorkers();
        }
        @Override public void watch(WatchListener listener) {
            delegate.watch(listener);
        }
    }
}
