package io.rowweave.cluster.rpc;

import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.cluster.Endpoint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class RpcCodecTest {

    @Test
    void streamingWriterIsWireIdenticalToExactArrayEncoder() throws Exception {
        RpcMessage message = RpcMessage.submitPlanArtifact(new byte[] {1, 2, 3}, "publish");
        ByteArrayOutputStream streamed = new ByteArrayOutputStream();

        RpcCodec.encodeTo(message, streamed);

        assertArrayEquals(RpcCodec.encode(message), streamed.toByteArray());
    }

    @Test
    void durableAssignmentAndAcceptanceRoundTripTokenAndCommandDigest() throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-durable", "attempt-1", 9L);
        byte[] token = {1, 2, 3, 4};
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) 7);

        RpcMessage assignment = roundTrip(RpcMessage.assignStage(
                "worker", 11L, new byte[] {8, 9}, "incarnation", fence, token));
        assertEquals(RpcMessage.Type.ASSIGN_STAGE, assignment.type());
        assertArrayEquals(token, assignment.stageAssignmentTokenBytes());

        RpcMessage acceptance = roundTrip(RpcMessage.assignmentAccepted(
                "worker", 11L, token, digest, "incarnation", fence));
        assertEquals(RpcMessage.Type.ASSIGNMENT_ACCEPTED, acceptance.type());
        assertArrayEquals(token, acceptance.stageAssignmentTokenBytes());
        assertArrayEquals(digest, acceptance.stageCommandDigest());
    }

    @Test
    void durableStageReportRoundTripsTheExactCanonicalTerminalReceipt() throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-terminal-report", "attempt-1", 9L);
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 11L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact-terminal-report", 1L, Collections.singletonList(assignment));
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] commandDigest = MessageDigest.getInstance("SHA-256")
                .digest(new byte[] {8, 9});
        StageTerminalReceipt receipt = StageTerminalReceipt.failed(
                snapshot.token(0), commandDigest, 7L, 5L,
                "USER_CODE", MessageDigest.getInstance("SHA-256")
                        .digest("bounded failure".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                1234L);

        RpcMessage report = roundTrip(RpcMessage.durableStageReport("worker", receipt));

        assertEquals(RpcMessage.Type.STAGE_REPORT, report.type());
        assertEquals(11L, report.stageId());
        assertEquals(fence, report.runAttemptFence());
        assertEquals("incarnation", report.incarnationId());
        assertEquals(7L, report.inputRows());
        assertEquals(5L, report.rowCount());
        assertFalse(report.success());
        assertEquals(receipt, StageAssignmentManifestCodec.decodeTerminalReceipt(
                report.stageTerminalReceiptBytes()));
        assertNull(RpcMessage.stageReport(
                "worker", 11L, true, 0L, 0L, null).stageTerminalReceiptBytes(),
                "legacy report must remain distinguishable from durable terminal evidence");
    }

    @Test
    void durableStageReconciliationRoundTripsExactIdentityWithoutAStageCommand()
            throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-reconcile", "attempt-1", 11L);
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 3L, 0, 91L, 0, 0, StageAssignmentRole.SINK,
                "worker-r", "127.0.0.1", 6101, -1, "inc-r");
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(
                new StageAssignmentManifest(fence, "artifact-r", 3L,
                        Collections.singletonList(assignment)));
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(new byte[] {3, 1, 4});
        byte[] token = StageAssignmentManifestCodec.encodeToken(snapshot.token(0));
        Endpoint leader = new Endpoint("10.0.0.8", 19008);
        long term = 0x12345678abcdef01L;

        RpcMessage request = roundTrip(RpcMessage.reconcileDurableStage(
                "worker-r", token, digest, leader, term, "inc-r", fence));
        assertEquals(RpcMessage.Type.RECONCILE_DURABLE_STAGE, request.type());
        assertEquals(91L, request.stageId());
        assertArrayEquals(token, request.stageAssignmentTokenBytes());
        assertArrayEquals(digest, request.stageCommandDigest());
        assertEquals(term, request.leaderTermToken());
        assertEquals(fence, request.runAttemptFence());
        assertNull(request.stageCommandBlob(), "reconciliation must never carry executable code");

        StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                snapshot.token(0), digest, 5L, 4L, 99L);
        RpcMessage terminal = roundTrip(RpcMessage.durableStageReconciledTerminal(receipt));
        assertEquals(RpcMessage.Type.DURABLE_STAGE_RECONCILED, terminal.type());
        assertEquals("TERMINAL", terminal.reconciliationOutcome());
        assertEquals(receipt, StageAssignmentManifestCodec.decodeTerminalReceipt(
                terminal.reconciledTerminalReceiptBytes()));

        RpcMessage absent = roundTrip(RpcMessage.durableStageReconciledAbsent());
        assertEquals("ABSENT", absent.reconciliationOutcome());
        assertNull(absent.reconciledTerminalReceiptBytes());
    }

    @Test
    void assignmentAndWorkerReportsRoundTripRunAttemptFence() throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-Ω", "attempt-1", 9L);
        RpcMessage[] messages = {
                RpcMessage.assignStage("worker", 11L, new byte[] { 1, 2 }, "incarnation", fence),
                RpcMessage.cancelStage("worker", 11L, "incarnation", fence),
                RpcMessage.portReport("worker", 11L, 1234, 1, "incarnation", fence),
                RpcMessage.progressReport("worker", 11L, 7L, 5L, true, "incarnation", fence),
                RpcMessage.stageReport("worker", 11L, true, 7L, 5L, null, "incarnation", fence)
        };
        for (RpcMessage message : messages) {
            RpcMessage decoded = roundTrip(message);
            assertEquals(fence, decoded.runAttemptFence(), message.type().name());
        }
    }

    @Test
    void partialRunAttemptFenceFailsClosed() {
        byte[] framed = RpcCodec.encode(RpcMessage.assignStage(
                "worker", 11L, new byte[] { 1 }, "incarnation",
                new RunAttemptFence("run", "attempt", 9L)));
        int removed = Integer.BYTES * 2;
        byte[] truncated = Arrays.copyOf(framed, framed.length - removed);
        ByteBuffer.wrap(truncated).putInt(ByteBuffer.wrap(framed).getInt() - removed);

        assertThrows(IOException.class, () -> RpcCodec.decode(
                new DataInputStream(new ByteArrayInputStream(truncated))));
    }

    @Test
    void workerIncarnationRoundTripsAndLegacyFrameDefaultsToNull() throws Exception {
        String incarnation = "incarnation-2";
        RpcMessage[] currentMessages = {
                RpcMessage.registerWorker("w-inc", "10.0.0.9", 19009, 8, 29009, incarnation),
                RpcMessage.heartbeat("w-inc", 8L, incarnation),
                RpcMessage.deregisterWorker("w-inc", "10.0.0.9", 19009, 8, 29009, incarnation),
                RpcMessage.assignStage("w-inc", 11L, new byte[]{1}, incarnation),
                RpcMessage.cancelStage("w-inc", 11L, incarnation),
                RpcMessage.stageReport("w-inc", 11L, true, 3L, 2L, null, incarnation),
                RpcMessage.progressReport("w-inc", 11L, 2L, 1L, true, incarnation),
                RpcMessage.portReport("w-inc", 11L, 29010, 1, incarnation)
        };
        for (RpcMessage message : currentMessages) {
            assertEquals(incarnation, roundTrip(message).incarnationId(), message.type().name());
        }

        byte[] framed = RpcCodec.encode(RpcMessage.registerWorker(
                "legacy", "10.0.0.8", 19008, 4, 29008));
        int appendedBytes = 52;
        byte[] legacyFrame = Arrays.copyOf(framed, framed.length - appendedBytes);
        ByteBuffer.wrap(legacyFrame).putInt(ByteBuffer.wrap(framed).getInt() - appendedBytes);
        RpcMessage legacy = RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(legacyFrame)));
        assertNull(legacy.incarnationId(), "a legacy frame without a token falls back to the legacy-null generation explicitly");
        assertNull(legacy.runAttemptFence());
    }

    @Test
    void workerDepartureRoundTripsCompleteEndpointIdentity() throws Exception {
        RpcMessage decoded = roundTrip(
                RpcMessage.deregisterWorker("w-depart", "10.0.0.8", 19001, 7, 29001));

        assertEquals("DEREGISTER_WORKER", decoded.type().name());
        assertEquals("w-depart", decoded.workerId());
        assertEquals("10.0.0.8", decoded.host());
        assertEquals(19001, decoded.port());
        assertEquals(7, decoded.slots());
        assertEquals(29001, decoded.dataPort());
    }

    @Test
    void preparedOutputControlsRoundTripGroupStageAndIncarnation() throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-control", "attempt-control", 3L);
        RpcMessage[] controls = {
                RpcMessage.commitPreparedOutput("w", 101L, "group-7", "inc-3", fence),
                RpcMessage.rollbackPreparedOutput("w", 101L, "group-7", "inc-3", fence),
                RpcMessage.finalizePreparedOutput("w", 101L, "group-7", "inc-3", fence),
                RpcMessage.renewPreparedOutput("w", 101L, "group-7", "inc-3", fence)
        };
        for (RpcMessage control : controls) {
            RpcMessage decoded = roundTrip(control);
            assertEquals(control.type(), decoded.type());
            assertEquals("w", decoded.workerId());
            assertEquals(101L, decoded.stageId());
            assertEquals("group-7", decoded.commitGroupId());
            assertEquals("inc-3", decoded.incarnationId());
            assertEquals(fence, decoded.runAttemptFence());
        }
    }

    @Test
    void preparedOutputControlRoundTripsLeaderClaim() throws Exception {
        long term = 0x76543210fedcba98L;
        RunAttemptFence fence = new RunAttemptFence("run-leader", "attempt-leader", 4L);
        RpcMessage decoded = roundTrip(RpcMessage.commitPreparedOutput(
                "w", 102L, "group-8", new Endpoint("10.0.0.8", 19008), term, "inc-4", fence));

        assertEquals(RpcMessage.Type.COMMIT_PREPARED_OUTPUT, decoded.type());
        assertEquals("10.0.0.8", decoded.host());
        assertEquals(19008, decoded.port());
        assertEquals(term, decoded.leaderTermToken());
        assertEquals("group-8", decoded.commitGroupId());
        assertEquals("inc-4", decoded.incarnationId());
        assertEquals(fence, decoded.runAttemptFence());
    }

    @Test
    void manifestClusterProtocolRoundTripsAttemptGenerationPayloadAndFences() throws Exception {
        byte[] plan = {9, 8, 7};
        RpcMessage submit = roundTrip(RpcMessage.submitJob(plan, "attempt-stable-7"));
        assertEquals(RpcMessage.Type.SUBMIT_JOB, submit.type());
        assertArrayEquals(plan, submit.planGraphBytes());
        assertEquals("attempt-stable-7", submit.attemptId());

        long term = 0x12345678fedcba98L;
        Endpoint leader = new Endpoint("10.0.0.9", 19009);
        RunAttemptFence fence = new RunAttemptFence("run-manifest", "attempt-run", 8L);
        RpcMessage install = roundTrip(RpcMessage.installManifestMember(
                "w-7", 71L, "group-7", "attempt-stable-7", 42L,
                leader, term, "inc-7", fence));
        assertEquals(RpcMessage.Type.INSTALL_MANIFEST_MEMBER, install.type());
        assertEquals("group-7", install.commitGroupId());
        assertEquals("attempt-stable-7", install.attemptId());
        assertEquals(42L, install.commitGeneration());
        assertEquals(term, install.leaderTermToken());
        assertEquals("inc-7", install.incarnationId());
        assertEquals(fence, install.runAttemptFence());

        RpcMessage seal = roundTrip(RpcMessage.sealManifestMember(
                "w-7", 71L, "group-7", "attempt-stable-7", 42L,
                leader, term, "inc-7", fence));
        assertEquals(RpcMessage.Type.SEAL_MANIFEST_MEMBER, seal.type());
        assertEquals("group-7", seal.commitGroupId());
        assertEquals("attempt-stable-7", seal.attemptId());
        assertEquals(42L, seal.commitGeneration());
        assertEquals(term, seal.leaderTermToken());
        assertEquals(fence, seal.runAttemptFence());

        byte[] manifest = {1, 3, 3, 7};
        RpcMessage publish = roundTrip(RpcMessage.publishManifestGroup(
                "w-7", 71L, "group-7", "attempt-stable-7", 42L, manifest,
                leader, term, "inc-7", fence));
        assertEquals(RpcMessage.Type.PUBLISH_MANIFEST_GROUP, publish.type());
        assertArrayEquals(manifest, publish.commitPayload());
        assertEquals("attempt-stable-7", publish.attemptId());
        assertEquals(42L, publish.commitGeneration());
        assertEquals(term, publish.leaderTermToken());
        assertEquals(fence, publish.runAttemptFence());

        byte[] member = {4, 2};
        RpcMessage installed = roundTrip(RpcMessage.manifestMemberInstalled(member));
        assertEquals(RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, installed.type());
        assertArrayEquals(member, installed.commitPayload());
    }

    @Test
    void manifestOutcomeQueryRoundTripsCanonicalDomainAndExplicitState() throws Exception {
        RpcMessage query = roundTrip(RpcMessage.queryManifestOutcome(
                "/shared/ledger", "manifest-file:/shared/ledger", "attempt-q"));
        assertEquals(RpcMessage.Type.QUERY_MANIFEST_OUTCOME, query.type());
        assertEquals("/shared/ledger", query.commitDomainPath());
        assertEquals("manifest-file:/shared/ledger", query.commitDomainId());
        assertEquals("attempt-q", query.attemptId());

        byte[] hash = new byte[32];
        hash[0] = 17;
        RpcMessage outcome = roundTrip(RpcMessage.manifestOutcome(
                "COMMITTED", "attempt-q", 81L, hash, null));
        assertEquals(RpcMessage.Type.MANIFEST_OUTCOME, outcome.type());
        assertEquals("COMMITTED", outcome.commitOutcome());
        assertEquals("attempt-q", outcome.attemptId());
        assertEquals(81L, outcome.commitGeneration());
        assertArrayEquals(hash, outcome.commitPayload());
    }

    @Test
    void batchSinkOutcomeQueryUsesRunIdentityAndBoundedProjection() throws Exception {
        RpcMessage query = roundTrip(RpcMessage.queryBatchSinkOutcome("run-outcome-1"));
        assertEquals(RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME, query.type());
        assertEquals("run-outcome-1", query.runId());

        RpcMessage qualityQuery = roundTrip(
                RpcMessage.queryQualityGateDecision("run-quality-1"));
        assertEquals(RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
                qualityQuery.type());
        assertEquals("run-quality-1", qualityQuery.runId());

        byte[] decision = new byte[] {7, 8, 9};
        RpcMessage qualityResponse = roundTrip(
                RpcMessage.qualityGateDecision("run-quality-1", decision));
        assertEquals(RpcMessage.Type.QUALITY_GATE_DECISION,
                qualityResponse.type());
        assertEquals("run-quality-1", qualityResponse.runId());
        assertArrayEquals(decision, qualityResponse.qualityGateDecisionBytes());

        byte[] identity = new byte[32];
        identity[0] = 23;
        RpcMessage outcome = roundTrip(RpcMessage.batchSinkOutcome(
                "run-outcome-1", "COMMIT_DECIDED", 91L, identity,
                "MANIFEST_INTENT_DURABLE"));
        assertEquals(RpcMessage.Type.BATCH_SINK_OUTCOME, outcome.type());
        assertEquals("run-outcome-1", outcome.runId());
        assertEquals("COMMIT_DECIDED", outcome.commitOutcome());
        assertEquals(91L, outcome.commitGeneration());
        assertArrayEquals(identity, outcome.commitPayload());
        assertEquals("MANIFEST_INTENT_DURABLE", outcome.errorMessage());
    }

    private static RpcMessage roundTrip(RpcMessage m) throws IOException {
        byte[] framed = RpcCodec.encode(m);
        return RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(framed)));
    }

    @Test
    void registerRoundTrip() throws IOException {
        RpcMessage out = roundTrip(RpcMessage.registerWorker("w1", "127.0.0.1", 5555, 8));
        assertEquals(RpcMessage.Type.REGISTER_WORKER, out.type());
        assertEquals("w1", out.workerId());
        assertEquals("127.0.0.1", out.host());
        assertEquals(5555, out.port());
        assertEquals(8, out.slots());
    }

    @Test
    void heartbeatRoundTrip() throws IOException {
        RpcMessage out = roundTrip(RpcMessage.heartbeat("w7"));
        assertEquals(RpcMessage.Type.HEARTBEAT, out.type());
        assertEquals("w7", out.workerId());
        assertEquals(-1L, out.freeSlots(), "a heartbeat with no payload reports freeSlots=-1 (unknown)");
    }

    @Test
    void heartbeatWithFreeSlotsRoundTrip() throws IOException {
        RpcMessage out = roundTrip(RpcMessage.heartbeat("w9", 24L));
        assertEquals(RpcMessage.Type.HEARTBEAT, out.type());
        assertEquals("w9", out.workerId());
        assertEquals(24L, out.freeSlots(), "the trailing freeSlots field round-trips unchanged");
    }

    @Test
    void freeSlotsZeroDistinctFromUnknown() throws IOException {
        RpcMessage out = roundTrip(RpcMessage.heartbeat("wFull", 0L));
        assertEquals(0L, out.freeSlots(), "a saturated worker reports freeSlots=0, which is not unknown");
    }

    @Test
    void submitJobBytesRoundTrip() throws IOException {
        byte[] plan = {1, 2, 3, 4, 5};
        RpcMessage out = roundTrip(RpcMessage.submitJob(plan));
        assertEquals(RpcMessage.Type.SUBMIT_JOB, out.type());
        assertArrayEquals(plan, out.planGraphBytes());
    }

    @Test
    void submitPlanArtifactAppendsMessageTypeAndReusesBoundedBytesField() throws IOException {
        byte[] artifact = {9, 7, 5, 3};
        RpcMessage out = roundTrip(RpcMessage.submitPlanArtifact(artifact, "publication-17"));

        assertEquals(RpcMessage.Type.SUBMIT_PLAN_ARTIFACT, out.type());
        assertArrayEquals(artifact, out.planGraphBytes());
        assertEquals("publication-17", out.attemptId());
    }

    @Test
    void durableRunSubmitAndQueryMessagesRoundTripWithoutFieldReuse() throws IOException {
        byte[] envelope = {9, 8, 7};
        RpcMessage submit = roundTrip(RpcMessage.submitRun(envelope));
        assertEquals(RpcMessage.Type.SUBMIT_RUN, submit.type());
        assertArrayEquals(envelope, submit.planGraphBytes());
        assertNull(submit.runId());

        RpcMessage query = roundTrip(RpcMessage.queryRun("run-Ω-001"));
        assertEquals(RpcMessage.Type.QUERY_RUN, query.type());
        assertEquals("run-Ω-001", query.runId());
        assertNull(query.planGraphBytes());

        byte[] snapshot = {1, 2, 3, 4};
        RpcMessage found = roundTrip(RpcMessage.runSnapshot(snapshot));
        assertEquals(RpcMessage.Type.RUN_SNAPSHOT, found.type());
        assertTrue(found.success());
        assertArrayEquals(snapshot, found.planGraphBytes());

        RpcMessage missing = roundTrip(RpcMessage.runSnapshotNotFound());
        assertEquals(RpcMessage.Type.RUN_SNAPSHOT, missing.type());
        assertFalse(missing.success());
        assertNull(missing.planGraphBytes());
        assertNull(missing.errorMessage(), "null error is the structured NOT_FOUND discriminator");

        RpcMessage unavailable = roundTrip(RpcMessage.runSnapshotFailure("authority unavailable"));
        assertFalse(unavailable.success());
        assertEquals("authority unavailable", unavailable.errorMessage());
    }

    @Test
    void queryRunIdWireRejectsOversizedAndMalformedUtf8() throws Exception {
        byte[] valid = RpcCodec.encode(RpcMessage.queryRun("run"));
        int tailStart = valid.length - Integer.BYTES - 3;

        byte[] oversized = Arrays.copyOf(valid, valid.length - 3 + 257);
        ByteBuffer.wrap(oversized).putInt(ByteBuffer.wrap(valid).getInt() - 3 + 257);
        ByteBuffer.wrap(oversized, tailStart, Integer.BYTES).putInt(257);
        Arrays.fill(oversized, tailStart + Integer.BYTES, oversized.length, (byte) 'r');
        assertThrows(IOException.class, () -> RpcCodec.decode(
                new DataInputStream(new ByteArrayInputStream(oversized))));

        byte[] malformed = Arrays.copyOf(valid, valid.length - 2);
        ByteBuffer.wrap(malformed).putInt(ByteBuffer.wrap(valid).getInt() - 2);
        ByteBuffer.wrap(malformed, tailStart, Integer.BYTES).putInt(1);
        malformed[tailStart + Integer.BYTES] = (byte) 0x80;
        assertThrows(IOException.class, () -> RpcCodec.decode(
                new DataInputStream(new ByteArrayInputStream(malformed))));
    }

    @Test
    void assignmentLeaderClaimRoundTripWithoutNewFrameFields() throws IOException {
        long term = 0x89abcdef01234567L;
        RpcMessage out = roundTrip(RpcMessage.assignStage(
                "w1", 77L, new byte[]{4, 5}, new Endpoint("10.0.0.7", 19000), term));
        assertEquals(RpcMessage.Type.ASSIGN_STAGE, out.type());
        assertEquals("10.0.0.7", out.host());
        assertEquals(19000, out.port());
        assertEquals(term, out.leaderTermToken());
    }

    @Test
    void stageReportAndJobResultRoundTrip() throws IOException {
        RpcMessage rep = roundTrip(RpcMessage.stageReport("w2", 42L, false, 0, "boom"));
        assertEquals(RpcMessage.Type.STAGE_REPORT, rep.type());
        assertEquals(42L, rep.stageId());
        assertFalse(rep.success());
        assertEquals("boom", rep.errorMessage());

        RpcMessage res = roundTrip(RpcMessage.jobResult(true, 1234L, null));
        assertEquals(RpcMessage.Type.JOB_RESULT, res.type());
        assertTrue(res.success());
        assertEquals(1234L, res.rowCount());
        assertNull(res.errorMessage());
    }

    @Test
    void durableJobResultDispositionRoundTripsWithoutChangingLegacyResult() throws IOException {
        RpcMessage rejected = roundTrip(RpcMessage.durableJobResult(
                false, 0L, "rejected",
                io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE));
        assertEquals(io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                rejected.durableRunDisposition());

        RpcMessage legacy = roundTrip(RpcMessage.jobResult(false, 0L, "legacy"));
        assertNull(legacy.durableRunDisposition());
    }

    @Test
    void progressReportInputOutputRoundTrip() throws IOException {
        RpcMessage prog = roundTrip(RpcMessage.progressReport("w3", 7L, 18000L, 0L, true));
        assertEquals(RpcMessage.Type.PROGRESS_REPORT, prog.type());
        assertEquals(7L, prog.stageId());
        assertEquals(18000L, prog.inputRows(), "inputRows = rows processed so far");
        assertEquals(0L, prog.rowCount(), "rowCount = outputRows (zero while buffering)");
        assertTrue(prog.success(), "success = the running flag");
    }

    @Test
    void stageReportInputOutputRoundTrip() throws IOException {
        RpcMessage rep = roundTrip(RpcMessage.stageReport("w4", 9L, true, 30000L, 42L, null));
        assertEquals(RpcMessage.Type.STAGE_REPORT, rep.type());
        assertEquals(9L, rep.stageId());
        assertTrue(rep.success());
        assertEquals(30000L, rep.inputRows(), "inputRows = rows read (which equals rows processed)");
        assertEquals(42L, rep.rowCount(), "rowCount = outputRows (groups after aggregation)");
    }

    @Test
    void nullFieldsRoundTrip() throws IOException {
        RpcMessage out = roundTrip(RpcMessage.ack());
        assertEquals(RpcMessage.Type.ACK, out.type());
        assertNull(out.workerId());
        assertNull(out.planGraphBytes());
        assertNull(out.stageCommandBlob());
    }

    @Test
    void eofReturnsNull() throws IOException {
        assertNull(RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(new byte[0]))));
    }

    @Test
    void corruptLengthRejected() {
        byte[] bad = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}; // -1 as int
        assertThrows(IOException.class,
                () -> RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(bad))));
    }

    @Test
    void corruptHttpLengthReportsRawPrefixEvidence() {
        byte[] http = {'H', 'T', 'T', 'P'};
        IOException failure = assertThrows(IOException.class,
                () -> RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(http))));
        assertTrue(failure.getMessage().contains("1213486160"), "signed length: " + failure.getMessage());
        assertTrue(failure.getMessage().contains("0x48545450"), "hex prefix: " + failure.getMessage());
        assertTrue(failure.getMessage().contains("ASCII=HTTP"), "printable ASCII: " + failure.getMessage());
        assertTrue(failure.getMessage().contains("83886080"), "max length: " + failure.getMessage());
    }
}

