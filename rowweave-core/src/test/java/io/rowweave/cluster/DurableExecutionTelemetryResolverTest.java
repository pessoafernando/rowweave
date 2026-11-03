package io.rowweave.cluster;

import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.PartitionRole;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DurableExecutionTelemetryResolverTest {

    @Test
    void resolvesSuccessfulDurableEvidenceWithZeroRowsStillPresent() {
        Fixture fixture = successfulFixture("durable-resolve", new long[]{7L, 0L});

        ExecutionTelemetrySummary summary = DurableExecutionTelemetryResolver.resolve(
                DurableExecutionEvidenceView.present(
                        fixture.terminalRun, fixture.assignments));

        assertEquals(TelemetryCoverage.COMPLETE, summary.resourceCoverage());
        assertEquals(12L, summary.managedPeakReservedBytes());
        assertEquals(15L, summary.managedAcquiredBytes());
        assertEquals(18L, summary.pressureSpillCount());
        assertEquals(21L, summary.spillRunCount());
        assertEquals(24L, summary.spillLogicalBytesWritten());
        assertEquals(27L, summary.spillPeakReservedBytes());
        assertEquals(TelemetryCoverage.COMPLETE, summary.partitionCoverage());
        assertEquals(1, summary.partitionDistributions().size());
        assertEquals(PartitionRole.KEYED_OP,
                summary.partitionDistributions().get(0).role());
        assertArrayEquals(new boolean[]{true, true},
                summary.partitionDistributions().get(0).presenceBitmap());
        assertArrayEquals(new long[]{7L, 0L},
                summary.partitionDistributions().get(0).rowCounts());
    }

    @Test
    void snapshotOnlyAndAnyLinkageMismatchFailTelemetryClosed() {
        Fixture fixture = successfulFixture("durable-linkage", new long[]{1L, 2L});
        assertEquals(ExecutionTelemetrySummary.unavailable(),
                DurableExecutionTelemetryResolver.resolve(
                        DurableExecutionEvidenceView.snapshotOnly(fixture.terminalRun)));

        Fixture other = successfulFixture("durable-other", new long[]{1L, 2L});
        assertThrows(IllegalArgumentException.class,
                () -> DurableExecutionEvidenceView.present(
                        fixture.terminalRun, other.assignments));
    }

    @Test
    void v1TokensUseCanonicalManifestIdentityForCompleteZeroAwareDistribution()
            throws Exception {
        String runId = "durable-v1-token";
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, 0L, 1L)).snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(),
                "attempt", "publication", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = Arrays.asList(
                record(fence, 0, 3, 0, StageAssignmentRole.KEYED_OP),
                record(fence, 1, 3, 1, StageAssignmentRole.KEYED_OP));
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L, records);
        StageAssignmentSnapshot assignments = StageAssignmentSnapshot.initial(manifest);
        long[] rows = {7L, 0L};
        for (int ordinal = 0; ordinal < records.size(); ordinal++) {
            io.rowweave.run.StageAssignmentToken token = v1Token(
                    manifest, records.get(ordinal), assignments.acceptanceGeneration());
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    token, digest(ordinal + 1), 10L + ordinal);
            assignments = assignments.withAcceptance(acceptance);
            assignments = assignments.withTerminal(StageTerminalReceipt.succeeded(
                    token, acceptance.commandDigest(), rows[ordinal], rows[ordinal],
                    20L + ordinal));
        }
        run = store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "running-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "committing-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.COMMITTING, false, 5L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "succeeded-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.SUCCEEDED, false, 6L, null, null,
                RunResultSummary.discard(7L))).snapshot();

        ExecutionTelemetrySummary summary = DurableExecutionTelemetryResolver.resolve(
                DurableExecutionEvidenceView.present(run, assignments));

        assertEquals(TelemetryCoverage.UNAVAILABLE, summary.resourceCoverage());
        assertEquals(TelemetryCoverage.COMPLETE, summary.partitionCoverage());
        assertArrayEquals(new boolean[] {true, true},
                summary.partitionDistributions().get(0).presenceBitmap());
        assertArrayEquals(rows, summary.partitionDistributions().get(0).rowCounts());
    }

    private static Fixture successfulFixture(String runId, long[] keyedRows) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L))
                .snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(),
                "attempt", "publication", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        records.add(record(fence, 0, 3, 0, StageAssignmentRole.KEYED_OP));
        records.add(record(fence, 1, 3, 1, StageAssignmentRole.KEYED_OP));
        records.add(record(fence, 2, 4, 0, StageAssignmentRole.SINK));
        StageAssignmentSnapshot assignments = StageAssignmentSnapshot.initial(
                new StageAssignmentManifest(fence, run.artifactId(), 1L, records));
        for (int i = 0; i < records.size(); i++) {
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    assignments.token(i), digest(i + 1), 10L + i);
            assignments = assignments.withAcceptance(acceptance);
            long inputRows = i < keyedRows.length ? keyedRows[i] : 5L;
            assignments = assignments.withTerminal(StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), inputRows,
                    i == 2 ? 5L : inputRows, 20L + i,
                    complete(1L + 3L * i)));
        }
        run = store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "running-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "committing-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.COMMITTING, false, 5L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "succeeded-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.SUCCEEDED, false, 6L, null, null,
                RunResultSummary.discard(5L))).snapshot();
        return new Fixture(run, assignments);
    }

    private static StageAssignmentRecord record(
            RunAttemptFence fence, int ordinal, int logicalStage, int instance,
            StageAssignmentRole role) {
        return StageAssignmentRecord.create(
                fence, 1L, ordinal, 100L + ordinal, logicalStage, instance, role,
                "worker-" + ordinal, "localhost", 7001 + ordinal, -1,
                "inc-" + ordinal);
    }

    private static StageExecutionTelemetry complete(long base) {
        return StageExecutionTelemetry.complete(
                base, base + 1L, base + 2L, base + 3L, base + 4L, base + 5L);
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static io.rowweave.run.StageAssignmentToken v1Token(
            StageAssignmentManifest manifest, StageAssignmentRecord record,
            long acceptanceGeneration) throws Exception {
        ByteArrayOutputStream fieldsBytes = new ByteArrayOutputStream();
        try (DataOutputStream fields = new DataOutputStream(fieldsBytes)) {
            writeText(fields, manifest.fence().runId());
            writeText(fields, manifest.fence().attemptId());
            fields.writeLong(manifest.fence().fenceToken());
            fields.writeLong(manifest.generation());
            fields.writeLong(acceptanceGeneration);
            fields.writeInt(record.physicalOrdinal());
            writeText(fields, record.stageExecutionId());
            fields.writeLong(record.runtimeStageId());
            writeText(fields, record.workerId());
            writeText(fields, record.workerIncarnationId());
        }
        byte[] identity = fieldsBytes.toByteArray();
        ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        bodyBytes.write(identity);
        bodyBytes.write(MessageDigest.getInstance("SHA-256").digest(identity));
        return io.rowweave.run.StageAssignmentManifestCodec.decodeToken(
                frame(1, 3, bodyBytes.toByteArray()));
    }

    private static void writeText(DataOutputStream out, String value)
            throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static byte[] frame(int version, int kind, byte[] body)
            throws Exception {
        ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
        try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
            frame.writeInt(0x5257414d);
            frame.writeShort(version);
            frame.writeShort(kind);
            frame.writeInt(body.length);
            frame.write(body);
            frame.flush();
            frame.write(MessageDigest.getInstance("SHA-256")
                    .digest(frameBytes.toByteArray()));
        }
        return frameBytes.toByteArray();
    }

    private static final class Fixture {
        final RunSnapshot terminalRun;
        final StageAssignmentSnapshot assignments;

        Fixture(RunSnapshot terminalRun, StageAssignmentSnapshot assignments) {
            this.terminalRun = terminalRun;
            this.assignments = assignments;
        }
    }
}
