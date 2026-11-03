package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionResult;
import io.rowweave.QualityGatePolicy;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.ops.RowValidation;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableCommitBarrierFailureTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final DataSchema QUALITY_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void committingWriteFailureDoesNotPublishAndLeavesRunning(@TempDir Path tmp) {
        assertBarrierFailure(tmp, FaultMode.COMMITTING_BEFORE_APPLY, AttemptStatus.RUNNING);
    }

    @Test
    void committingAckLossReadsAuthorityAndContinuesToSuccess(@TempDir Path tmp) throws Exception {
        assertAckLossConverges(tmp, FaultMode.COMMITTING_AFTER_APPLY,
                "run-committing-ack-loss");
    }

    @Test
    void succeededAckLossReadsTerminalSummaryAndReturnsSuccess(@TempDir Path tmp) throws Exception {
        assertAckLossConverges(tmp, FaultMode.SUCCEEDED_AFTER_APPLY,
                "run-succeeded-ack-loss");
    }

    @Test
    void publishedManifestIsSuccessEvenWhenWorkerDepartsBeforeFinalize(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "run-finalize-worker-departure";
        Path output = tmp.resolve("visible-before-finalize");
        File spill = tmp.resolve("finalize-departure-spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "finalize-departure-worker", 4,
                     "127.0.0.1", master.port(), spill)) {
            master.setManifestPublishHooks(null, worker::close);
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            assertEquals(1L, client.submitRun(artifact(output), submission(runId)).rowsOut());
            assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                    registry.runStateStore().get(runId).status());
            try (io.rowweave.FileSource.Reader reader =
                         new ManifestFileSource(META, output.toString(), "ledger").openReader()) {
                assertEquals(42, reader.next().get(0));
                assertFalse(reader.hasNext());
            }
        }
    }

    @Test
    void fiveMemberPublicationSurvivesOneWorkerDepartureBeforeFinalize(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "run-five-member-finalize-worker-departure";
        Path output = tmp.resolve("five-visible-before-finalize");
        File spill0 = tmp.resolve("five-finalize-spill-0").toFile();
        File spill1 = tmp.resolve("five-finalize-spill-1").toFile();
        assertTrue(spill0.mkdirs());
        assertTrue(spill1.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "five-finalize-worker-0", 8,
                     "127.0.0.1", master.port(), spill0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "five-finalize-worker-1", 8,
                     "127.0.0.1", master.port(), spill1)) {
            master.setManifestPublishHooks(null, worker0::close);
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            assertEquals(5L, client.submitRun(
                    multiArtifact(output, 5), submission(runId)).rowsOut());
            assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                    registry.runStateStore().get(runId).status());
            assertEquals(5, new ManifestFileStore(output).latest().members().size());
            for (int member = 0; member < 5; member++) {
                try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                        META, output.toString(), "member-" + member).openReader()) {
                    assertEquals(42, reader.next().get(0));
                    assertFalse(reader.hasNext());
                }
            }
            assertEquals(BatchSinkOutcome.State.COMMITTED,
                    client.queryBatchSinkOutcome(runId).state());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }
    }

    @Test
    void memberJournalFailureBeforeIntentConvergesTerminalFailed(@TempDir Path tmp) {
        FaultRegistry registry = new FaultRegistry(FaultMode.MEMBER_BEFORE_APPLY);
        String runId = "run-member-journal-failure";
        Path output = tmp.resolve("member-journal-failure");
        File spill = tmp.resolve("member-journal-spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "member-journal-worker", 4,
                     "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            DurableRunSubmitException failure = assertThrows(
                    DurableRunSubmitException.class,
                    () -> client.submitRun(artifact(output), submission(runId)));

            assertEquals(DurableRunSubmitException.Category.TERMINAL_FAILED,
                    failure.category());
            assertEquals(io.rowweave.run.RunStatus.FAILED,
                    registry.store.get(runId).status());
            BatchSinkAbortRecord abort = registry.store.batchSinkAbort(
                    fenceOf(registry.store.get(runId)));
            assertEquals(BatchSinkAbortRecord.State.ABORTED, abort.state());
            BatchSinkOutcome outcome = client.queryBatchSinkOutcome(runId);
            assertEquals(BatchSinkOutcome.State.ABORTED, outcome.state());
            assertEquals("MANIFEST_ABORT_COMPLETED", outcome.detailCode());
            assertNull(new ManifestFileStore(output).latest());
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    @Test
    void intentAckLossContinuesToPublishAndSucceed(@TempDir Path tmp) throws Exception {
        assertAckLossConverges(tmp, FaultMode.INTENT_AFTER_APPLY,
                "run-intent-ack-loss");
    }

    @Test
    void typedQualityIntentAckLossPublishesTheExactThreeMemberGeneration(
            @TempDir Path tmp) throws Exception {
        FaultRegistry registry = new FaultRegistry(FaultMode.INTENT_AFTER_APPLY);
        String runId = "run-quality-intent-ack-loss";
        Path output = tmp.resolve(runId);
        File spill0 = tmp.resolve(runId + "-spill-0").toFile();
        File spill1 = tmp.resolve(runId + "-spill-1").toFile();
        assertTrue(spill0.mkdirs());
        assertTrue(spill1.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     runId + "-worker-0", 16, "127.0.0.1", master.port(), spill0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     runId + "-worker-1", 16, "127.0.0.1", master.port(), spill1)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            assertEquals(7L, client.submitRun(
                    qualityArtifact(output), submission(runId)).rowsOut());
            assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                    registry.store.get(runId).status());
            assertEquals(7L, registry.store.get(runId).resultSummary().rowsOut());
            QualityGateDecision decision = registry.store.qualityGateDecision(
                    fenceOf(registry.store.get(runId)));
            assertNotNull(decision);
            assertEquals(QualityGateDecision.Outcome.PASS, decision.outcome());
            assertEquals(decision, client.queryQualityGateDecision(runId));
            assertNull(client.queryQualityGateDecision(runId + "-missing"));
            CommitManifest manifest = new ManifestFileStore(output).latest();
            assertEquals(3, manifest.members().size());
            assertEquals(1, readMember(QUALITY_META, output, "valid").size());
            assertEquals(2, readMember(rejectedMeta(), output, "rejected").size());
            assertEquals(Arrays.asList(
                    "ACCEPTED_TOTAL|||-1||1",
                    "REJECTED_REASON|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL|1",
                    "REJECTED_REASON|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED|1",
                    "REJECTED_TOTAL|||-1||2"), qualitySummaryLines(readMember(
                    TypedQualityManifest.summarySchema(), output, "summary")));
            assertEquals(BatchSinkOutcome.State.COMMITTED,
                    client.queryBatchSinkOutcome(runId).state());
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }
    }

    @Test
    void strictQualityFailureAbortsBeforeManifestPublication(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "run-quality-gate-rejected";
        Path output = tmp.resolve(runId);
        File spill = tmp.resolve(runId + "-spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     runId + "-worker", 16,
                     "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            DurableRunSubmitException failure = assertThrows(
                    DurableRunSubmitException.class,
                    () -> client.submitRun(qualityArtifact(
                            output, QualityGatePolicy.builder()
                                    .maxRejectedRows(0L).build()),
                            submission(runId)));

            assertEquals(DurableRunSubmitException.Category.TERMINAL_FAILED,
                    failure.category());
            RunSnapshot terminal = registry.runStateStore().get(runId);
            assertEquals(io.rowweave.run.RunStatus.FAILED, terminal.status());
            assertEquals("QUALITY_GATE_REJECTED",
                    terminal.currentAttempt().reasonCode());
            QualityGateDecision decision = ((DurableStageAssignmentAuthority)
                    registry.runStateStore()).qualityGateDecision(fenceOf(terminal));
            assertNotNull(decision);
            assertEquals(QualityGateDecision.Outcome.FAIL, decision.outcome());
            assertEquals(decision, client.queryQualityGateDecision(runId));
            BatchSinkAbortRecord abort = ((DurableStageAssignmentAuthority)
                    registry.runStateStore()).batchSinkAbort(fenceOf(terminal));
            assertEquals(BatchSinkAbortRecord.State.ABORTED, abort.state());
            assertEquals("QUALITY_GATE_REJECTED", abort.reasonCode());
            assertNull(new ManifestFileStore(output).latest());
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    @Test
    void objectQualityIntentAckLossPublishesTheExactThreeMemberGeneration(
            @TempDir Path tmp) throws Exception {
        FaultRegistry registry = new FaultRegistry(FaultMode.INTENT_AFTER_APPLY);
        String runId = "run-object-quality-intent-ack-loss";
        Path output = tmp.resolve(runId);
        byte[] content = "1\t1.00\nbad\t2.00\n2\t2.00\n"
                .getBytes(StandardCharsets.UTF_8);
        ObjectStorageQualityDurabilityTest.DurableQualityProvider.reset();
        ObjectStorageQualityDurabilityTest.DurableQualityProvider.objects.put(
                "ack-loss.tsv", content);
        File spill0 = tmp.resolve(runId + "-spill-0").toFile();
        File spill1 = tmp.resolve(runId + "-spill-1").toFile();
        assertTrue(spill0.mkdirs());
        assertTrue(spill1.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     runId + "-worker-0", 16, "127.0.0.1", master.port(), spill0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     runId + "-worker-1", 16, "127.0.0.1", master.port(), spill1)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            assertEquals(6L, client.submitRun(
                    objectQualityArtifact(output, content), submission(runId)).rowsOut());
            assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                    registry.store.get(runId).status());
            CommitManifest manifest = new ManifestFileStore(output).latest();
            assertEquals(3, manifest.members().size());
            assertEquals(2, readMember(objectQualityMeta(), output, "valid").size());
            List<DataRow> rejected = readMember(
                    rejectedMeta(), output, "rejected");
            assertEquals(1, rejected.size());
            assertEquals("QUALITY_RAW_FORMAT_INVALID", rejected.get(0).getString(3));
            assertEquals(Arrays.asList(
                    "ACCEPTED_TOTAL|||-1||2",
                    "REJECTED_REASON|QUALITY_RAW_FORMAT_INVALID|OBJECT_ROW_DECODE_V1|-1|ROWWEAVE_ROW_TEXT_V1|1",
                    "REJECTED_TOTAL|||-1||1"), qualitySummaryLines(readMember(
                    TypedQualityManifest.summarySchema(), output, "summary")));
            assertEquals(BatchSinkOutcome.State.COMMITTED,
                    client.queryBatchSinkOutcome(runId).state());
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        } finally {
            ObjectStorageQualityDurabilityTest.DurableQualityProvider.reset();
        }
    }

    @Test
    void intentBackedPublishHookFailureStillPublishesAndSucceeds(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "run-intent-backed-hook-failure";
        Path output = tmp.resolve("intent-backed-hook-failure");
        File spill = tmp.resolve("intent-backed-hook-spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "intent-backed-hook-worker", 4,
                     "127.0.0.1", master.port(), spill)) {
            master.setManifestPublishHooks(() -> {
                throw new IllegalStateException("injected pre-publish hook failure");
            }, null);
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            assertEquals(1L, client.submitRun(
                    artifact(output), submission(runId)).rowsOut());
            assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                    registry.runStateStore().get(runId).status());
            RunSnapshot succeeded = registry.runStateStore().get(runId);
            assertNull(((DurableStageAssignmentAuthority) registry.runStateStore())
                    .batchSinkAbort(fenceOf(succeeded)));
            assertEquals(BatchSinkOutcome.State.COMMITTED,
                    client.queryBatchSinkOutcome(runId).state());
            try (io.rowweave.FileSource.Reader reader =
                         new ManifestFileSource(META, output.toString(), "ledger").openReader()) {
                assertEquals(42, reader.next().get(0));
                assertFalse(reader.hasNext());
            }
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    @Test
    void succeededWriteFailureKeepsPublishedOutputAndCommitting(@TempDir Path tmp)
            throws Exception {
        FaultRegistry registry = new FaultRegistry(FaultMode.SUCCEEDED_BEFORE_APPLY);
        String runId = "run-succeeded-write-failure";
        Path output = tmp.resolve("visible-manifest");
        File spill = tmp.resolve("spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "succeeded-failure-worker", 4, "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());

            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> client.submitRun(artifact(output), submission(runId)));

            assertTrue(failure.getMessage().contains("injected succeeded write failure"),
                    failure.getMessage());
            assertEquals(AttemptStatus.COMMITTING,
                    registry.store.get(runId).currentAttempt().status());
            try (io.rowweave.FileSource.Reader reader =
                         new ManifestFileSource(META, output.toString(), "ledger").openReader()) {
                assertEquals(42, reader.next().get(0));
                assertFalse(reader.hasNext());
            }
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    private static void assertBarrierFailure(
            Path tmp, FaultMode mode, AttemptStatus expectedStatus) {
        FaultRegistry registry = new FaultRegistry(mode);
        String runId = "run-" + mode.name().toLowerCase(java.util.Locale.ROOT);
        Path output = tmp.resolve("must-not-publish");
        File spill = tmp.resolve("spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "barrier-failure-worker", 4, "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());

            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> client.submitRun(artifact(output), submission(runId)));

            assertTrue(failure.getMessage().contains("injected committing"),
                    failure.getMessage());
            assertNull(new ManifestFileStore(output).latest());
            assertEquals(expectedStatus, registry.store.get(runId).currentAttempt().status());
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    private static void assertAckLossConverges(
            Path tmp, FaultMode mode, String runId) throws Exception {
        FaultRegistry registry = new FaultRegistry(mode);
        Path output = tmp.resolve(runId);
        File spill = tmp.resolve(runId + "-spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     runId + "-worker", 4, "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());

            assertEquals(1L, client.submitRun(artifact(output), submission(runId)).rowsOut());
            assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                    registry.store.get(runId).status());
            assertEquals(1L, registry.store.get(runId).resultSummary().rowsOut());
            try (io.rowweave.FileSource.Reader reader =
                         new ManifestFileSource(META, output.toString(), "ledger").openReader()) {
                assertEquals(42, reader.next().get(0));
                assertFalse(reader.hasNext());
            }
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    private static RunSubmission submission(String runId) {
        return new RunSubmission(runId, "attempt-1", "publication-1", 1, 0L, 10L);
    }

    private static PlanArtifact artifact(Path output) {
        DataRow row = new DataRow(1);
        row.set(0, 42);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, java.util.Collections.singletonList(row),
                                "barrier-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0,
                                new ManifestFileSink(META, output.toString(), "ledger"))
                        .build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 1);
        return LegacyPlanArtifactAdapter.wrap("durable-barrier-failure", graph);
    }

    private static PlanArtifact qualityArtifact(Path output) {
        return qualityArtifact(output, QualityGatePolicy.allowAll());
    }

    private static PlanArtifact qualityArtifact(
            Path output, QualityGatePolicy policy) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-ack-loss", "quality ack loss");
        session.setDefinitionId("durable-quality-ack-loss");
        session.setParallelism(2);
        DataRowClassification classified = session.fromSource(new CollectionSource(
                        QUALITY_META, "quality-ack-split", Arrays.asList(
                        qualityRow("A", 1), qualityRow(null, 2), qualityRow("B", 13))))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(QUALITY_META)
                        .notNull("adaptor.required", 0)
                        .integerRange("period.range", 1, 1, 12)
                        .build());
        classified.writeToManifest(TypedQualityManifest.of(
                output.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy));
        return session.toPlanArtifact();
    }

    private static PlanArtifact objectQualityArtifact(Path output, byte[] content) {
        DataSchema meta = objectQualityMeta();
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                meta,
                new ObjectStorageQualityDurabilityTest.DurableQualityProvider(),
                "endpoint-test", "credential-test", "domain", "ack-loss.tsv",
                "version-1", content.length, sha256Hex(content),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        ExecutionEnvironment session = new ExecutionEnvironment(
                "object-quality-ack-loss", "object quality ack loss");
        session.setDefinitionId("durable-object-quality-ack-loss");
        session.setParallelism(2);
        session.classifyObjectStorageRows("ledger.object.source", input)
                .writeToManifest(TypedQualityManifest.of(
                        output.toString(), "valid", "rejected", "summary"));
        return session.toPlanArtifact();
    }

    private static DataSchema objectQualityMeta() {
        return new DataSchema(
                new Field("id", DataType.LongType),
                new Field("amount", DataType.BigDecimalType));
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte item : digest) {
                result.append(Character.forDigit((item >>> 4) & 0xf, 16));
                result.append(Character.forDigit(item & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static DataSchema rejectedMeta() {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-meta", "quality meta");
        return session.fromSource(new CollectionSource(
                        QUALITY_META, "quality-meta-split", java.util.Collections.emptyList()))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(QUALITY_META)
                        .notNull("adaptor.required", 0).build())
                .rejectedRows().getSchema();
    }

    private static DataRow qualityRow(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static List<DataRow> readMember(DataSchema meta, Path output, String member) {
        List<DataRow> rows = new ArrayList<>();
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                meta, output.toString(), member).openReader()) {
            while (reader.hasNext()) rows.add(reader.next());
        }
        return rows;
    }

    private static List<String> qualitySummaryLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getString(0) + "|" + row.getString(1) + "|"
                    + row.getString(2) + "|" + row.getInteger(3) + "|"
                    + row.getString(4) + "|" + row.getLong(5));
        }
        return result;
    }

    private static PlanArtifact multiArtifact(Path output, int sinkCount) {
        DataRow row = new DataRow(1);
        row.set(0, 42);
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(DurableTestSources.source(
                        META, java.util.Collections.singletonList(row), "barrier-source")
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"));
        for (int member = 0; member < sinkCount; member++) {
            nodes.add(PlanNode.sink(META, 0, new ManifestFileSink(
                            META, output.toString(), "member-" + member))
                    .build().withParam(
                            PlanNode.OPERATOR_UID_PARAM, "output-" + member));
        }
        return LegacyPlanArtifactAdapter.wrap(
                "durable-multi-barrier-failure", new PlanGraph(nodes, 2));
    }

    private enum FaultMode {
        COMMITTING_BEFORE_APPLY,
        COMMITTING_AFTER_APPLY,
        MEMBER_BEFORE_APPLY,
        INTENT_AFTER_APPLY,
        SUCCEEDED_BEFORE_APPLY,
        SUCCEEDED_AFTER_APPLY
    }

    private static RunAttemptFence fenceOf(RunSnapshot snapshot) {
        return new RunAttemptFence(snapshot.runId(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());
    }

    private static final class FaultStore implements RunStateStore,
            DurableStageAssignmentAuthority,
            io.rowweave.artifact.ArtifactReferenceAuthority {
        private final InMemoryRunStateStore delegate = new InMemoryRunStateStore();
        private final FaultMode mode;

        private FaultStore(FaultMode mode) {
            this.mode = mode;
        }

        @Override
        public RunDecision apply(RunCommand command) {
            RunSnapshot before = delegate.get(command.runId());
            AttemptStatus current = before == null || before.currentAttempt() == null
                    ? null : before.currentAttempt().status();
            if (current == AttemptStatus.RUNNING
                    && mode == FaultMode.COMMITTING_BEFORE_APPLY) {
                throw new IllegalStateException("injected committing write failure");
            }
            if (current == AttemptStatus.RUNNING
                    && mode == FaultMode.COMMITTING_AFTER_APPLY) {
                delegate.apply(command);
                throw new IllegalStateException("injected committing acknowledgement loss");
            }
            if (current == AttemptStatus.COMMITTING
                    && mode == FaultMode.SUCCEEDED_BEFORE_APPLY) {
                throw new IllegalStateException("injected succeeded write failure");
            }
            if (current == AttemptStatus.COMMITTING
                    && mode == FaultMode.SUCCEEDED_AFTER_APPLY) {
                delegate.apply(command);
                throw new IllegalStateException("injected succeeded acknowledgement loss");
            }
            return delegate.apply(command);
        }

        @Override public RunSnapshot get(String runId) { return delegate.get(runId); }
        @Override public List<ExecutionEvent> events(String runId, long afterSequence, int limit) {
            return delegate.events(runId, afterSequence, limit);
        }
        @Override public StageManifestPublication publish(StageAssignmentManifest manifest) {
            return delegate.publish(manifest);
        }
        @Override public StageAssignmentSnapshot assignmentSnapshot(RunAttemptFence fence) {
            return delegate.assignmentSnapshot(fence);
        }
        @Override public StageAcceptanceDecision accept(StageAssignmentAcceptance acceptance) {
            return delegate.accept(acceptance);
        }
        @Override public StageTerminalDecision recordTerminal(StageTerminalReceipt receipt) {
            return delegate.recordTerminal(receipt);
        }
        @Override public io.rowweave.run.PreparedSinkDecision recordPreparedSink(
                io.rowweave.run.PreparedSinkReceipt receipt) {
            return delegate.recordPreparedSink(receipt);
        }
        @Override public io.rowweave.run.ManifestCommitJournalSnapshot manifestCommitJournal(
                RunAttemptFence fence) {
            return delegate.manifestCommitJournal(fence);
        }
        @Override public io.rowweave.run.QualityGateCommitJournalSnapshot
        qualityGateCommitJournal(RunAttemptFence fence) {
            return delegate.qualityGateCommitJournal(fence);
        }
        @Override public QualityGateDecision qualityGateDecision(
                RunAttemptFence fence) {
            return delegate.qualityGateDecision(fence);
        }
        @Override public QualityGateDecisionResult recordQualityGateDecision(
                QualityGateDecision decision) {
            return delegate.recordQualityGateDecision(decision);
        }
        @Override public io.rowweave.run.ManifestMemberDecision recordManifestMember(
                io.rowweave.run.ManifestMemberReceipt receipt) {
            if (mode == FaultMode.MEMBER_BEFORE_APPLY) {
                throw new IllegalStateException("injected member journal write failure");
            }
            return delegate.recordManifestMember(receipt);
        }
        @Override public io.rowweave.run.ManifestIntentDecision commitManifestIntent(
                io.rowweave.run.ManifestCommitIntent intent) {
            if (mode == FaultMode.INTENT_AFTER_APPLY) {
                delegate.commitManifestIntent(intent);
                throw new IllegalStateException("injected intent acknowledgement loss");
            }
            return delegate.commitManifestIntent(intent);
        }
        @Override public BatchSinkAbortRecord batchSinkAbort(RunAttemptFence fence) {
            return delegate.batchSinkAbort(fence);
        }
        @Override public BatchSinkAbortDecision recordBatchSinkAbort(
                BatchSinkAbortRecord record) {
            return delegate.recordBatchSinkAbort(record);
        }
        @Override public io.rowweave.artifact.ArtifactReferenceView inspect(
                String artifactId) {
            return delegate.inspect(artifactId);
        }
        @Override public io.rowweave.artifact.ArtifactBackfillStatus backfillStatus() {
            return delegate.backfillStatus();
        }
        @Override public io.rowweave.artifact.ArtifactBackfillStatus advanceBackfill(
                int maxEntries) {
            return delegate.advanceBackfill(maxEntries);
        }
    }

    private static final class FaultRegistry implements ServiceRegistry, RunStateStoreProvider,
            ArtifactRepositoryProvider,
            io.rowweave.artifact.ArtifactReferenceAuthorityProvider {
        private final InProcessRegistry workers = new InProcessRegistry();
        private final FaultStore store;

        private FaultRegistry(FaultMode mode) {
            store = new FaultStore(mode);
        }

        @Override public RunStateStore runStateStore() { return store; }
        @Override public ArtifactRepository artifactRepository() {
            return workers.artifactRepository();
        }
        @Override public io.rowweave.artifact.ArtifactReferenceAuthority
        artifactReferenceAuthority() { return store; }
        @Override public void register(RegistryWorker worker) { workers.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return workers.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { workers.report(worker); }
        @Override public void renew(String workerId) { workers.renew(workerId); }
        @Override public void deregister(String workerId) { workers.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return workers.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) { return workers.worker(workerId); }
        @Override public void watch(WatchListener listener) { workers.watch(listener); }
    }
}

