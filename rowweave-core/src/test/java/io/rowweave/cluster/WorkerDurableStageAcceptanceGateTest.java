package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.StageExecutionTelemetryCodec;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageTerminalOutcome;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.SourceSplitAssignment;
import io.rowweave.run.SinkAttemptIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** receipt rejection must precede every StageTask/user-code path. */
class WorkerDurableStageAcceptanceGateTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final AtomicInteger USER_INPUT_CALLS = new AtomicInteger();
    private static final AtomicInteger RECEIPT_ACCEPTED = new AtomicInteger();
    private static final AtomicInteger MATERIALIZATION_CALLBACKS = new AtomicInteger();
    private static final AtomicInteger LEADER_CHANGE_INPUT_CALLS = new AtomicInteger();
    private static volatile CountDownLatch userInputOpened = new CountDownLatch(1);
    private static volatile CountDownLatch leaderChangeInputOpened = new CountDownLatch(1);

    @Test
    void providerIdentityMismatchIsRejectedBeforeAssignmentAcceptance(
            @TempDir Path tmp) throws Exception {
        AtomicInteger acceptances = new AtomicInteger();
        File workerDir = tmp.resolve("provider-identity-mismatch").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_190, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        acceptances.incrementAndGet();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_190, workerDir)) {
            RunAttemptFence fence = new RunAttemptFence(
                    "run-provider-gate", "attempt-1", 1L);
            BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                    1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
                    "destination", "domain",
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
            ProviderBatchSink output = new ProviderBatchSink(
                    META, descriptor,
                    new DurableSinkCodecDescriptor(
                            "rowweave.manifest-config", 1, 1024 * 1024),
                    new byte[]{1});
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META,
                            Collections.<Source>singletonList(new GuardedInput()))
                            .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                    PlanNode.sink(META, 0, output).build()
                            .withParam(PlanNode.OPERATOR_UID_PARAM, "sink-op")));
            SourceSplitAssignment ownership = ownership(graph);
            StageAssignmentRecord actual = StageAssignmentRecord.create(
                    fence, 1L, 0, 11L, 0, 0,
                    StageAssignmentRole.WHOLE_GRAPH,
                    "worker", "127.0.0.1", worker.port(), -1,
                    worker.incarnationId(), ownership);
            StageAssignmentManifest actualManifest = new StageAssignmentManifest(
                    fence, "artifact", 1L, Collections.singletonList(actual));
            StageAssignmentRecord drifted = StageAssignmentRecord.create(
                    fence, 2L, 0, 11L, 0, 0,
                    StageAssignmentRole.WHOLE_GRAPH,
                    "worker", "127.0.0.1", worker.port(), -1,
                    worker.incarnationId(), ownership);
            SinkAttemptIdentity wrongIdentity = SinkAttemptIdentity.forAssignment(
                    new StageAssignmentManifest(fence, "artifact", 2L,
                            Collections.singletonList(drifted)),
                    drifted, "sink-op", descriptor);
            StageCommand command = StageCommand.wholeGraph(graph)
                    .withSourceSplitAssignment(ownership)
                    .withRunAttemptFence(fence)
                    .withDurableSinkContext(
                            wrongIdentity, "publication", 1L);
            byte[] blob = StageCommandCodecV2.encode(
                    command, ObjectTrustContext.localTrusted(
                            WorkerDurableStageAcceptanceGateTest.class
                                    .getClassLoader()),
                    ignored -> { });
            byte[] token = StageAssignmentManifestCodec.encodeToken(
                    io.rowweave.run.StageAssignmentSnapshot.initial(
                            actualManifest).token(0));

            RpcMessage response = worker.handler().apply(RpcMessage.assignStage(
                    "worker", 11L, blob, worker.incarnationId(), fence, token));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(0, acceptances.get());
        }
    }

    private static final class GuardedInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final String splitId;

        private GuardedInput() { this("guarded-split"); }
        private GuardedInput(String splitId) { this.splitId = splitId; }

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return exactDescriptor(splitId);
        }

        @Override
        public Iterator<DataRow> iterator() {
            USER_INPUT_CALLS.incrementAndGet();
            userInputOpened.countDown();
            return Collections.emptyIterator();
        }
    }

    private static final class FailingReadInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return exactDescriptor("failing-read-split");
        }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
        private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
            input.defaultReadObject();
            MATERIALIZATION_CALLBACKS.incrementAndGet();
            if (RECEIPT_ACCEPTED.get() == 0) {
                throw new IOException("readObject ran before durable acceptance");
            }
            throw new IOException("injected post-acceptance materialization failure");
        }
    }

    private static final class LeaderChangeBlockingInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return exactDescriptor("leader-change-split");
        }
        @Override public Iterator<DataRow> iterator() {
            LEADER_CHANGE_INPUT_CALLS.incrementAndGet();
            leaderChangeInputOpened.countDown();
            try {
                new CountDownLatch(1).await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return Collections.emptyIterator();
        }
    }

    @Test
    void inProcessAssignmentDoesNotOpenInputWhenReceiptIsRejected(@TempDir Path tmp)
            throws Exception {
        assertRejectedBeforeUserCode(tmp.resolve("in-process").toFile(), false, 63_177);
    }

    @Test
    void socketAssignmentDoesNotOpenInputWhenReceiptIsRejected(@TempDir Path tmp)
            throws Exception {
        assertRejectedBeforeUserCode(tmp.resolve("socket").toFile(), true, 63_178);
    }

    @Test
    void unavailableReceiptRetriesAndOnlyThenOpensInput(@TempDir Path tmp) throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger receipts = new AtomicInteger();
        File workerDir = tmp.resolve("retry").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_179, request -> {
                    if (request.type() != RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        return RpcMessage.ack();
                    }
                    return receipts.incrementAndGet() == 1
                            ? RpcMessage.jobResult(false, 0,
                            "assignment acceptance unavailable: injected")
                            : RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_179, workerDir)) {
            RpcMessage response = worker.handler().apply(assignment(worker));

            assertEquals(RpcMessage.Type.ACK, response.type());
            assertEquals(2, receipts.get());
            assertTrue(userInputOpened.await(2, TimeUnit.SECONDS));
            assertEquals(1, USER_INPUT_CALLS.get());
        }
    }

    @Test
    void terminalAckLossRetriesTheSameFrozenDurableReceipt(@TempDir Path tmp) throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger terminalAttempts = new AtomicInteger();
        AtomicReference<byte[]> firstReceipt = new AtomicReference<>();
        AtomicReference<byte[]> secondReceipt = new AtomicReference<>();
        AtomicReference<byte[]> firstTelemetry = new AtomicReference<>();
        AtomicReference<byte[]> secondTelemetry = new AtomicReference<>();
        CountDownLatch terminalAcked = new CountDownLatch(1);
        File workerDir = tmp.resolve("terminal-ack-loss").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_183, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        return RpcMessage.ack();
                    }
                    if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        int attempt = terminalAttempts.incrementAndGet();
                        byte[] encoded = request.stageTerminalReceiptBytes();
                        if (attempt == 1) {
                            firstReceipt.set(encoded == null ? null : encoded.clone());
                            firstTelemetry.set(request.executionTelemetryBytes());
                            return RpcMessage.jobResult(false, 0,
                                    "stage terminal unavailable: injected ACK loss");
                        }
                        secondReceipt.set(encoded == null ? null : encoded.clone());
                        secondTelemetry.set(request.executionTelemetryBytes());
                        terminalAcked.countDown();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_183, workerDir)) {
            RpcMessage response = worker.handler().apply(assignment(worker));

            assertEquals(RpcMessage.Type.ACK, response.type());
            assertTrue(terminalAcked.await(2, TimeUnit.SECONDS));
            assertEquals(2, terminalAttempts.get());
            assertArrayEquals(firstReceipt.get(), secondReceipt.get(),
                    "ACK loss must resend byte-identical terminal evidence");
            assertArrayEquals(firstTelemetry.get(), secondTelemetry.get(),
                    "ACK loss must resend byte-identical terminal telemetry");
            assertEquals(TelemetryCoverage.COMPLETE,
                    StageExecutionTelemetryCodec.decode(secondTelemetry.get()).coverage(),
                    "an executed pass-through durable stage must report complete zero facts");
            StageTerminalReceipt receipt = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    secondReceipt.get());
            assertEquals(StageTerminalOutcome.SUCCEEDED, receipt.outcome());
            assertEquals(11L, receipt.token().runtimeStageId());
            assertEquals(1, USER_INPUT_CALLS.get(), "terminal retry must not re-run Source");
        }
    }

    @Test
    void leaderChangeDrainsTheTaskAndReportsOneFrozenCancellation(@TempDir Path tmp)
            throws Exception {
        LEADER_CHANGE_INPUT_CALLS.set(0);
        leaderChangeInputOpened = new CountDownLatch(1);
        AtomicReference<byte[]> terminalBytes = new AtomicReference<>();
        CountDownLatch terminalSeen = new CountDownLatch(1);
        File workerDir = tmp.resolve("leader-change-terminal").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_184, request -> {
                    if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        terminalBytes.set(request.stageTerminalReceiptBytes().clone());
                        terminalSeen.countDown();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_184, workerDir)) {
            PlanGraph blocking = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META,
                            Collections.<Source>singletonList(new LeaderChangeBlockingInput()))
                            .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()));
            assertEquals(RpcMessage.Type.ACK,
                    worker.handler().apply(assignment(worker, blocking, false)).type());
            assertTrue(leaderChangeInputOpened.await(2, TimeUnit.SECONDS));

            java.lang.reflect.Method drain = WorkerServer.class.getDeclaredMethod(
                    "cancelAssignmentsFromPreviousLeader");
            drain.setAccessible(true);
            assertTrue((Boolean) drain.invoke(worker));

            assertTrue(terminalSeen.await(2, TimeUnit.SECONDS));
            StageTerminalReceipt terminal = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    terminalBytes.get());
            assertEquals(StageTerminalOutcome.CANCELED, terminal.outcome());
            assertEquals(1, LEADER_CHANGE_INPUT_CALLS.get(),
                    "leader change may reconcile but must never reopen Source");
        }
    }

    @Test
    void executorRejectionAfterReceiptStillRunsZeroUserCode(@TempDir Path tmp) throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger receipts = new AtomicInteger();
        AtomicReference<RpcMessage> terminal = new AtomicReference<>();
        CountDownLatch terminalSeen = new CountDownLatch(1);
        File workerDir = tmp.resolve("executor-reject").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_180, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        receipts.incrementAndGet();
                    } else if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        terminal.set(request);
                        terminalSeen.countDown();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_180, workerDir)) {
            java.lang.reflect.Field poolField = WorkerServer.class.getDeclaredField("stagePool");
            poolField.setAccessible(true);
            ((ExecutorService) poolField.get(worker)).shutdownNow();

            RpcMessage response = worker.handler().apply(assignment(worker));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(1, receipts.get());
            assertTrue(terminalSeen.await(2, TimeUnit.SECONDS),
                    "an accepted assignment must converge to a durable terminal receipt");
            StageTerminalReceipt receipt = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    terminal.get().stageTerminalReceiptBytes());
            assertEquals(StageTerminalOutcome.FAILED, receipt.outcome());
            assertEquals("COMMAND_EXECUTION", receipt.failureCategory());
            assertEquals(TelemetryCoverage.NOT_EXECUTED,
                    terminal.get().stageExecutionTelemetry().coverage());
            assertFalse(userInputOpened.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, USER_INPUT_CALLS.get());
        }
    }

    @Test
    void closeDuringAcceptedPreparingAssignmentSendsOneDurableNotExecutedReceipt(
            @TempDir Path tmp) throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        CountDownLatch acceptancePersisted = new CountDownLatch(1);
        CountDownLatch releaseAcceptanceAck = new CountDownLatch(1);
        CountDownLatch terminalSeen = new CountDownLatch(1);
        AtomicReference<RpcMessage> terminal = new AtomicReference<>();
        File workerDir = tmp.resolve("close-preparing-durable").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_188, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        acceptancePersisted.countDown();
                        try {
                            if (!releaseAcceptanceAck.await(5, TimeUnit.SECONDS)) {
                                return RpcMessage.jobResult(false, 0,
                                        "assignment acceptance unavailable: test timeout");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return RpcMessage.jobResult(false, 0,
                                    "assignment acceptance unavailable: interrupted");
                        }
                    } else if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        terminal.set(request);
                        terminalSeen.countDown();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_188, workerDir)) {
            FutureTask<RpcMessage> assignment = new FutureTask<>(
                    () -> worker.handler().apply(assignment(worker)));
            Thread assignmentThread = new Thread(assignment, "durable-assignment-preparing");
            assignmentThread.start();
            assertTrue(acceptancePersisted.await(2, TimeUnit.SECONDS));

            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            Thread closeThread = new Thread(() -> {
                try {
                    worker.close();
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                }
            }, "durable-close-preparing");
            closeThread.start();

            assertTrue(terminalSeen.await(2, TimeUnit.SECONDS),
                    "close must publish durable evidence while the acceptance ACK is delayed");
            releaseAcceptanceAck.countDown();
            closeThread.join(5_000L);
            assignmentThread.join(5_000L);
            assertFalse(closeThread.isAlive());
            assertFalse(assignmentThread.isAlive());
            assertEquals(null, closeFailure.get());
            StageTerminalReceipt receipt = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    terminal.get().stageTerminalReceiptBytes());
            assertEquals(StageTerminalOutcome.CANCELED, receipt.outcome());
            assertEquals(TelemetryCoverage.NOT_EXECUTED,
                    terminal.get().stageExecutionTelemetry().coverage());
            assertFalse(userInputOpened.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, USER_INPUT_CALLS.get());
        } finally {
            releaseAcceptanceAck.countDown();
        }
    }

    @Test
    void closeOfRunningDurableAssignmentKeepsReceiptAndExecutedTelemetry(
            @TempDir Path tmp) throws Exception {
        LEADER_CHANGE_INPUT_CALLS.set(0);
        leaderChangeInputOpened = new CountDownLatch(1);
        CountDownLatch terminalSeen = new CountDownLatch(1);
        AtomicReference<RpcMessage> terminal = new AtomicReference<>();
        File workerDir = tmp.resolve("close-running-durable").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_189, request -> {
                    if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        terminal.set(request);
                        terminalSeen.countDown();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_189, workerDir)) {
            PlanGraph blocking = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META,
                            Collections.<Source>singletonList(new LeaderChangeBlockingInput()))
                            .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()));
            assertEquals(RpcMessage.Type.ACK,
                    worker.handler().apply(assignment(worker, blocking, false)).type());
            assertTrue(leaderChangeInputOpened.await(2, TimeUnit.SECONDS));

            worker.close();

            assertTrue(terminalSeen.await(2, TimeUnit.SECONDS));
            StageTerminalReceipt receipt = StageAssignmentManifestCodec.decodeTerminalReceipt(
                    terminal.get().stageTerminalReceiptBytes());
            assertEquals(StageTerminalOutcome.CANCELED, receipt.outcome());
            assertEquals(TelemetryCoverage.COMPLETE,
                    terminal.get().stageExecutionTelemetry().coverage(),
                    "a running durable stage must keep the StageTask terminal snapshot");
            assertEquals(1, LEADER_CHANGE_INPUT_CALLS.get());
        }
    }

    @Test
    void durableLegacyJavaEnvelopeIsRejectedBeforeReceiptOrUserCode(@TempDir Path tmp)
            throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger receipts = new AtomicInteger();
        File workerDir = tmp.resolve("legacy-durable").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_181, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) receipts.incrementAndGet();
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_181, workerDir)) {
            RpcMessage response = worker.handler().apply(assignment(worker, true));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(0, receipts.get());
            assertFalse(userInputOpened.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, USER_INPUT_CALLS.get());
        }
    }

    @Test
    void postReceiptMaterializationFailureReportsAcceptedStageTerminal(@TempDir Path tmp)
            throws Exception {
        RECEIPT_ACCEPTED.set(0);
        MATERIALIZATION_CALLBACKS.set(0);
        AtomicInteger terminalReports = new AtomicInteger();
        AtomicReference<String> terminalError = new AtomicReference<>();
        File workerDir = tmp.resolve("materialization-failure").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_182, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        RECEIPT_ACCEPTED.incrementAndGet();
                    } else if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        terminalReports.incrementAndGet();
                        terminalError.set(request.errorMessage());
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_182, workerDir)) {
            PlanGraph failingGraph = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META,
                            Collections.<Source>singletonList(new FailingReadInput()))
                            .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()));

            RpcMessage response = worker.handler().apply(assignment(worker, failingGraph, false));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(1, RECEIPT_ACCEPTED.get());
            assertEquals(1, MATERIALIZATION_CALLBACKS.get());
            assertEquals(1, terminalReports.get());
            assertEquals("COMMAND_MATERIALIZATION", terminalError.get(),
                    "durable report exposes a bounded category, not connector exception text");
            assertEquals(0, worker.runningStageCount());
        }
    }

    @Test
    void wrongSourceBucketCountAndOrderAreRejectedBeforeAcceptanceOrInputOpen(
            @TempDir Path tmp) throws Exception {
        assertSourceRoutingRejected(tmp.resolve("wrong-count").toFile(), 63_185, true);
        assertSourceRoutingRejected(tmp.resolve("wrong-order").toFile(), 63_186, false);
    }

    @Test
    void serializedSourceMismatchAfterAcceptanceStillOpensNoInput(@TempDir Path tmp)
            throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger receipts = new AtomicInteger();
        AtomicInteger terminalReports = new AtomicInteger();
        File workerDir = tmp.resolve("post-acceptance-source-mismatch").toFile();
        workerDir.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", 63_187, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        receipts.incrementAndGet();
                    } else if (request.type() == RpcMessage.Type.STAGE_REPORT) {
                        terminalReports.incrementAndGet();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", 63_187, workerDir)) {
            PlanGraph claimed = graph("claimed-split");
            PlanGraph actual = graph("different-split");
            SourceSplitAssignment claimedOwnership = ownership(claimed);

            RpcMessage response = worker.handler().apply(assignment(
                    worker, actual, false, claimedOwnership, claimedOwnership));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(1, receipts.get());
            assertEquals(1, terminalReports.get());
            assertFalse(userInputOpened.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, USER_INPUT_CALLS.get());
        }
    }

    private static void assertRejectedBeforeUserCode(File tmp, boolean socket, int masterPort)
            throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger receiptRequests = new AtomicInteger();
        AtomicReference<byte[]> reportedDigest = new AtomicReference<>();
        tmp.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", masterPort, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        receiptRequests.incrementAndGet();
                        reportedDigest.set(request.stageCommandDigest());
                        return RpcMessage.jobResult(false, 0,
                                "assignment acceptance rejected: STALE_ASSIGNMENT_GENERATION");
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", masterPort, tmp)) {
            RpcMessage assignment = assignment(worker);
            byte[] blob = assignment.stageCommandBlob();
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(blob);

            RpcMessage response;
            if (socket) {
                try (RpcClient client = new RpcClient("127.0.0.1", worker.port())) {
                    response = client.call(assignment);
                }
            } else {
                response = worker.handler().apply(assignment);
            }

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(1, receiptRequests.get());
            assertArrayEquals(digest, reportedDigest.get());
            assertFalse(userInputOpened.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, USER_INPUT_CALLS.get());
        }
    }

    private static void assertSourceRoutingRejected(
            File tmp, int masterPort, boolean wrongCount) throws Exception {
        USER_INPUT_CALLS.set(0);
        userInputOpened = new CountDownLatch(1);
        AtomicInteger receipts = new AtomicInteger();
        tmp.mkdirs();
        try (InProcessRpcRegistry.Registration master = InProcessRpcRegistry.register(
                "receipt-master", masterPort, request -> {
                    if (request.type() == RpcMessage.Type.ASSIGNMENT_ACCEPTED) {
                        receipts.incrementAndGet();
                    }
                    return RpcMessage.ack();
                });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "receipt-master", masterPort, tmp)) {
            PlanGraph graph = graph("split-a", "split-b");
            SourceSplitAssignment tokenOwnership = ownership(graph);
            PlanGraph wrong = wrongCount
                    ? graph("split-a") : graph("split-b", "split-a");
            SourceSplitAssignment commandOwnership = ownership(wrong);

            RpcMessage response = worker.handler().apply(assignment(
                    worker, graph, false, tokenOwnership, commandOwnership));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertFalse(response.success());
            assertEquals(0, receipts.get());
            assertFalse(userInputOpened.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, USER_INPUT_CALLS.get());
        }
    }

    private static RpcMessage assignment(WorkerServer worker) throws Exception {
        return assignment(worker, false);
    }

    private static RpcMessage assignment(WorkerServer worker, boolean legacy) throws Exception {
        return assignment(worker, graph(), legacy);
    }

    private static RpcMessage assignment(
            WorkerServer worker, PlanGraph graph, boolean legacy) throws Exception {
        SourceSplitAssignment ownership = ownership(graph);
        return assignment(worker, graph, legacy, ownership, ownership);
    }

    private static RpcMessage assignment(
            WorkerServer worker, PlanGraph graph, boolean legacy,
            SourceSplitAssignment tokenOwnership,
            SourceSplitAssignment commandOwnership) throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-gate", "attempt-1", 1L);
        StageCommand command = StageCommand.wholeGraph(graph)
                .withSourceSplitAssignment(commandOwnership)
                .withRunAttemptFence(fence);
        byte[] blob = legacy ? serialize(command) : StageCommandCodecV2.encode(command,
                ObjectTrustContext.localTrusted(
                        WorkerDurableStageAcceptanceGateTest.class.getClassLoader()), ignored -> { });
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 11L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker", "127.0.0.1", worker.port(), -1, worker.incarnationId(),
                tokenOwnership);
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 1L, Collections.singletonList(record));
        byte[] token = StageAssignmentManifestCodec.encodeToken(
                io.rowweave.run.StageAssignmentSnapshot.initial(manifest).token(0));
        return RpcMessage.assignStage(
                "worker", 11L, blob, worker.incarnationId(), fence, token);
    }

    private static PlanGraph graph() {
        return graph("guarded-split");
    }

    private static PlanGraph graph(String... splitIds) {
        java.util.List<Source> inputs = new java.util.ArrayList<>();
        for (String splitId : splitIds) inputs.add(new GuardedInput(splitId));
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, inputs)
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()));
    }

    private static SourceSplitAssignment ownership(PlanGraph graph) {
        PlanNode source = graph.node(0);
        return SourceSplitAssignment.create("artifact", 0, 0,
                Collections.singletonList(SourceSplitAssignment.SourceGroup.create(
                        "source", source.sourceSplitBindings())));
    }

    private static BatchSourceSplitDescriptor exactDescriptor(String splitId) {
        return new io.rowweave.CollectionSource(
                META, splitId, Collections.emptyList()).batchSourceSplit();
    }

    private static byte[] serialize(StageCommand command) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(command);
        }
        return bytes.toByteArray();
    }
}
