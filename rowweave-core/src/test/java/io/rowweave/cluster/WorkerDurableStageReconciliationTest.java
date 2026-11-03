package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.PreparedSinkReceiptCodec;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalOutcome;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** recovery reconciliation is identity-only and never reopens user code. */
class WorkerDurableStageReconciliationTest {

    @Test
    void reconciliationReplaysFrozenPreparedEvidenceWithTerminal(@TempDir Path tmp)
            throws Exception {
        AtomicReference<byte[]> preparedWire = new AtomicReference<>();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "prepared-reconcile-master", 63_187, request -> {
                    if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        preparedWire.set(request.preparedSinkReceiptBytes());
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "prepared-reconcile-master", 63_187,
                     new File(tmp.toFile(), "worker-prepared"))) {
            WorkerAssignmentLedger ledger = assignments(worker);
            RunAttemptFence fence = new RunAttemptFence(
                    "run-reconcile-prepared", "attempt-1", 7L);
            StageAssignmentRecord assignment = StageAssignmentRecord.create(
                    fence, 1L, 0, 111L, 0, 0, StageAssignmentRole.SINK,
                    "worker", "127.0.0.1", worker.port(), -1,
                    worker.incarnationId());
            StageAssignmentManifest manifest = new StageAssignmentManifest(
                    fence, "artifact-reconcile-prepared", 1L,
                    Collections.singletonList(assignment));
            StageAssignmentToken token = StageAssignmentSnapshot.initial(
                    manifest).token(0);
            byte[] digest = sha256("prepared-command");
            WorkerAssignmentLedger.Registration registration =
                    ledger.registerDurableSocket(
                            111L, new byte[]{1}, fence, token, digest, null, null);
            BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                    1, "rowweave.manifest-file", 1,
                    SchemaFingerprint.of(new DataSchema(
                            new io.rowweave.Field("id", DataType.LongType))),
                    "domain:member", "domain",
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
            PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                    token, digest, new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION,
                    SinkAttemptIdentity.forAssignment(
                            manifest, assignment, "sink-op", descriptor),
                    descriptor.capabilityProfile(), descriptor.commitDomainId(),
                    new byte[]{3}, 1L, sha256("data"), 10L));
            StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                    token, digest, 1L, 1L, 11L);
            ledger.freezeTerminal(registration.slot, terminal, prepared);
            ledger.complete(registration.slot);

            RpcMessage response = worker.handler().apply(RpcMessage.reconcileDurableStage(
                    "worker", StageAssignmentManifestCodec.encodeToken(token), digest,
                    new Endpoint("prepared-reconcile-master", 63_187), 1L,
                    worker.incarnationId(), fence));

            assertEquals("TERMINAL", response.reconciliationOutcome());
            assertEquals(prepared,
                    PreparedSinkReceiptCodec.decode(preparedWire.get()));
            assertEquals(prepared,
                    PreparedSinkReceiptCodec.decode(
                            response.preparedSinkReceiptBytes()));
        }
    }

    @Test
    void exactReconciliationDrainsActiveAndReplaysPendingTerminalEvidence(@TempDir Path tmp)
            throws Exception {
        AtomicInteger reports = new AtomicInteger();
        AtomicReference<byte[]> lastReport = new AtomicReference<>();
        AtomicReference<byte[]> lastTelemetry = new AtomicReference<>();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "reconcile-master", 63_186, request -> {
                    if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        reports.incrementAndGet();
                        lastReport.set(request.stageTerminalReceiptBytes().clone());
                        lastTelemetry.set(request.executionTelemetryBytes());
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "reconcile-master", 63_186,
                     new File(tmp.toFile(), "worker"))) {
            WorkerAssignmentLedger ledger = assignments(worker);
            RunAttemptFence fence = new RunAttemptFence("run-reconcile", "attempt-1", 7L);
            byte[] digest = sha256("command");
            StageAssignmentToken activeToken = token(worker, fence, 1L, 101L);

            StageAssignmentToken replacementToken = token(
                    "replacement-incarnation", worker.port(), fence, 1L, 101L);
            RpcMessage replacement = worker.handler().apply(RpcMessage.reconcileDurableStage(
                    "worker", StageAssignmentManifestCodec.encodeToken(replacementToken), digest,
                    new Endpoint("reconcile-master", 63_186), 1L,
                    "replacement-incarnation", fence));
            assertEquals(RpcMessage.Type.JOB_RESULT, replacement.type());
            assertFalse(replacement.success());
            assertEquals(0, reports.get(),
                    "a replacement incarnation must not inspect or mutate the old worker slot");

            RpcMessage absent = worker.handler().apply(request(worker, activeToken, digest));
            assertEquals(RpcMessage.Type.DURABLE_STAGE_RECONCILED, absent.type());
            assertEquals("ABSENT", absent.reconciliationOutcome());
            assertEquals(0, reports.get());

            WorkerAssignmentLedger.Registration active = ledger.registerDurableSocket(
                    101L, new byte[] {1}, fence, activeToken, digest, null, null);
            RpcMessage conflict = worker.handler().apply(
                    request(worker, activeToken, sha256("wrong")));
            assertEquals(RpcMessage.Type.JOB_RESULT, conflict.type());
            assertFalse(conflict.success());
            assertEquals(0, reports.get(), "identity conflict must have zero cancellation/report effects");

            RpcMessage drained = worker.handler().apply(request(worker, activeToken, digest));
            StageTerminalReceipt canceled = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    drained.reconciledTerminalReceiptBytes());
            assertEquals("TERMINAL", drained.reconciliationOutcome());
            assertEquals(StageTerminalOutcome.CANCELED, canceled.outcome());
            assertArrayEquals(lastReport.get(), drained.reconciledTerminalReceiptBytes());
            assertEquals(1, reports.get());

            StageAssignmentToken pendingToken = token(worker, fence, 2L, 102L);
            WorkerAssignmentLedger.Registration pending = ledger.registerDurableSocket(
                    102L, new byte[] {2}, fence, pendingToken, digest, null, null);
            StageExecutionTelemetry telemetry = StageExecutionTelemetry.complete(
                    11L, 22L, 3L, 4L, 55L, 66L);
            StageTerminalReceipt succeeded = StageTerminalReceipt.succeeded(
                    pendingToken, digest, 5L, 4L, 99L, telemetry);
            ledger.freezeTerminal(pending.slot, succeeded);
            ledger.complete(pending.slot);

            RpcMessage replayed = worker.handler().apply(request(worker, pendingToken, digest));
            assertEquals("TERMINAL", replayed.reconciliationOutcome());
            StageTerminalReceipt replayedBase = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    replayed.reconciledTerminalReceiptBytes());
            assertEquals(succeeded.token(), replayedBase.token());
            assertEquals(succeeded.outputRows(), replayedBase.outputRows());
            assertEquals(TelemetryCoverage.UNAVAILABLE, replayedBase.telemetry().coverage(),
                    "standalone reconciliation receipt must retain its v3 wire");
            assertEquals(telemetry, replayed.stageExecutionTelemetry());
            assertArrayEquals(replayed.executionTelemetryBytes(), lastTelemetry.get(),
                    "ordinary retry and reconciliation response must carry the same telemetry");
            assertArrayEquals(StageAssignmentManifestCodec.encodeTerminalReceipt(succeeded),
                    lastReport.get(), "pending evidence must be replayed byte-for-byte before response");
            assertEquals(2, reports.get());

            StageAssignmentToken claimedToken = token(worker, fence, 3L, 103L);
            WorkerAssignmentLedger.Registration claimed = ledger.registerDurableSocket(
                    103L, new byte[] {3}, fence, claimedToken, digest, null, null);
            StageTerminalReceipt claimedReceipt = StageTerminalReceipt.succeeded(
                    claimedToken, digest, 6L, 5L, 100L);
            ledger.freezeTerminal(claimed.slot, claimedReceipt);
            ledger.complete(claimed.slot);
            assertTrue(ledger.claimReport(claimed.slot),
                    "simulate the old-leader retry thread retaining report responsibility");

            RpcMessage claimContended = worker.handler().apply(
                    request(worker, claimedToken, digest));
            assertEquals("TERMINAL", claimContended.reconciliationOutcome(),
                    "reconciliation must make one idempotent delivery even when another "
                            + "thread owns the retry claim");
            assertEquals(claimedReceipt,
                    StageAssignmentManifestCodec.decodeTerminalReceipt(
                            claimContended.reconciledTerminalReceiptBytes()));
            assertArrayEquals(StageAssignmentManifestCodec.encodeTerminalReceipt(claimedReceipt),
                    lastReport.get());
            assertEquals(3, reports.get());
        }
    }

    private static RpcMessage request(WorkerServer worker, StageAssignmentToken token,
                                      byte[] digest) {
        return RpcMessage.reconcileDurableStage(
                "worker", StageAssignmentManifestCodec.encodeToken(token), digest,
                new Endpoint("reconcile-master", 63_186), 1L,
                worker.incarnationId(), token.fence());
    }

    private static StageAssignmentToken token(WorkerServer worker, RunAttemptFence fence,
                                              long generation, long stageId) {
        return token(worker.incarnationId(), worker.port(), fence, generation, stageId);
    }

    private static StageAssignmentToken token(String incarnation, int port,
                                              RunAttemptFence fence,
                                              long generation, long stageId) {
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, generation, 0, stageId, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", port, -1, incarnation);
        return StageAssignmentSnapshot.initial(new StageAssignmentManifest(
                fence, "artifact-reconcile", generation,
                Collections.singletonList(assignment))).token(0);
    }

    private static WorkerAssignmentLedger assignments(WorkerServer worker) throws Exception {
        Field field = WorkerServer.class.getDeclaredField("assignments");
        field.setAccessible(true);
        return (WorkerAssignmentLedger) field.get(worker);
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }
}
