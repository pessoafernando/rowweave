package io.rowweave.cluster.registry.zk;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.DiscardSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGatePolicy;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.cluster.DurableRunSubmitException;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.RowValidation;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.FileArtifactRepository;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.commit.ManifestFileStore;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ZkRemoteJobClient end to end: an embedded ZooKeeper (TestingServer), a real Master injected with a
 * ZkServiceRegistry and elected through MasterElection, and a Worker that connects to the current
 * leader through RegistryMasterLocator. The job is committed with {@link ZkRemoteJobClient} holding
 * nothing but a connection string and runs through ExecutionEnvironment.commit, which proves the
 * client resolves the leader from ZooKeeper, submits, the job completes, and the row counts are right.
 */
class ZkRemoteJobClientTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema QUALITY_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void durableQualityPassThenFailKeepsOldGenerationAcrossTwoWorkers(
            @TempDir Path tmp) throws Exception {
        // This test verifies quality and publication authority, not the
        // production 30-second output-control SLA on a shared test host.
        try (AutoCloseable timeout = temporaryProperty(
                     "rowweave.cluster.outputCommitTimeoutMs", "120000");
             TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry0 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry1 =
                     new ZkServiceRegistry(zk.getConnectString())) {
            Path domain = tmp.resolve("quality-domain");
            File spill0 = tmp.resolve("quality-spill-0").toFile();
            File spill1 = tmp.resolve("quality-spill-1").toFile();
            assertTrue(spill0.mkdirs());
            assertTrue(spill1.mkdirs());
            try (MasterServer master = new MasterServer(
                         60_000L, 0, masterRegistry,
                         io.rowweave.monitor.JobMonitorRegistry.NOOP,
                         new FileArtifactRepository(tmp.resolve("artifacts")));
                 MasterElection election = new MasterElection(
                         masterRegistry,
                         new Endpoint("127.0.0.1", master.port()))) {
                master.bindLeadership(election);
                election.start();
                awaitRecoveryInventory(master);
                try (WorkerServer worker0 = WorkerServer.newUnguarded(
                         "quality-worker-0", 16,
                         new io.rowweave.cluster.RegistryMasterLocator(
                                 workerRegistry0), spill0);
                     WorkerServer worker1 = WorkerServer.newUnguarded(
                         "quality-worker-1", 16,
                         new io.rowweave.cluster.RegistryMasterLocator(
                                 workerRegistry1), spill1);
                     ZkRemoteJobClient client = new ZkRemoteJobClient(
                         zk.getConnectString())) {
                    long workerDeadline = System.nanoTime()
                            + TimeUnit.SECONDS.toNanos(10L);
                    while (master.workers().size() < 2
                            && System.nanoTime() < workerDeadline) {
                        Thread.sleep(20L);
                    }
                    assertEquals(2, master.workers().size());

                String passRun = "zk-quality-pass";
                assertEquals(7L, client.submitRun(
                        qualityArtifact(domain, QualityGatePolicy.allowAll()),
                        submission(passRun)).rowsOut());
                QualityGateDecision pass =
                        client.queryQualityGateDecision(passRun);
                assertEquals(QualityGateDecision.Outcome.PASS,
                        pass.outcome());
                assertEquals(1L,
                        new ManifestFileStore(domain).latest().generation());
                assertEquals("publication-" + passRun,
                        new ManifestFileStore(domain).latest().attemptId());
                assertTrue(successfulDurableAssignments(worker0, passRun) > 0,
                        "first worker must execute at least one PASS-run stage");
                assertTrue(successfulDurableAssignments(worker1, passRun) > 0,
                        "second worker must execute at least one PASS-run stage");

                String failRun = "zk-quality-fail";
                DurableRunSubmitException failure = assertThrows(
                        DurableRunSubmitException.class,
                        () -> client.submitRun(qualityArtifact(
                                domain, QualityGatePolicy.builder()
                                        .maxRejectedRows(0L).build()),
                                submission(failRun)));
                assertTrue(failure.category()
                                == DurableRunSubmitException.Category.TERMINAL_FAILED
                                || failure.category()
                                == DurableRunSubmitException.Category.UNKNOWN,
                        () -> "unexpected submission outcome: " + failure);
                RunSnapshot terminal = awaitTerminalRun(client, failRun);
                assertEquals(RunStatus.FAILED,
                        terminal == null ? null : terminal.status(),
                        "an UNKNOWN submit must reconcile to the quality failure");
                QualityGateDecision fail =
                        client.queryQualityGateDecision(failRun);
                assertEquals(QualityGateDecision.Outcome.FAIL,
                        fail.outcome());
                assertEquals(1L,
                        new ManifestFileStore(domain).latest().generation());
                assertEquals("publication-" + passRun,
                        new ManifestFileStore(domain).latest().attemptId());
                assertEquals(io.rowweave.commit.BatchSinkOutcome.State.ABORTED,
                        client.queryBatchSinkOutcome(failRun).state());
                    assertTrue(successfulDurableAssignments(worker0, failRun) > 0,
                            "first worker must execute at least one FAIL-run stage");
                    assertTrue(successfulDurableAssignments(worker1, failRun) > 0,
                            "second worker must execute at least one FAIL-run stage");
                    assertEquals(0, preparedOutputActiveCount(worker0));
                    assertEquals(0, preparedOutputActiveCount(worker1));
                }
            }
        }
    }

    private static AutoCloseable temporaryProperty(String key, String value) {
        String previous = System.getProperty(key);
        System.setProperty(key, value);
        return () -> {
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
        };
    }

    private static RunSnapshot awaitTerminalRun(
            ZkRemoteJobClient client, String runId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
        RunSnapshot snapshot;
        do {
            snapshot = client.queryRun(runId);
            if (snapshot != null && (snapshot.status() == RunStatus.FAILED
                    || snapshot.status() == RunStatus.SUCCEEDED
                    || snapshot.status() == RunStatus.CANCELED)) {
                return snapshot;
            }
            Thread.sleep(50L);
        } while (System.nanoTime() < deadline);
        return snapshot;
    }

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < n; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    private static long countLines(Path p) {
        try { return Files.lines(p).count(); } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test
    void artifactSubmissionReachesLeaderAsArtifactWithoutLegacyFallback() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry leaderRegistry = new ZkServiceRegistry(zk.getConnectString())) {
            AtomicReference<RpcMessage> observed = new AtomicReference<>();
            try (RpcServer leader = new RpcServer(request -> {
                observed.set(request);
                return RpcMessage.jobResult(true, 0L, null);
            });
                 MasterElection election = new MasterElection(
                         leaderRegistry, new Endpoint("127.0.0.1", leader.port()))) {
                election.start();
                PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                        "zk-artifact-fidelity",
                        new PlanGraph(Arrays.asList(
                                PlanNode.source(META, Collections.emptyList())
                                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                                        .withParam(PlanNode.OPERATOR_UID_PARAM, "sink"))));

                try (ZkRemoteJobClient client = new ZkRemoteJobClient(zk.getConnectString())) {
                    client.submit(artifact);
                }

                assertEquals(RpcMessage.Type.SUBMIT_PLAN_ARTIFACT, observed.get().type());
            }
        }
    }

    @Test
    void submitViaZkConnectStringRunsJob(@TempDir Path tmp) throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            String connect = zk.getConnectString();

            // Master: registry bound to the same ZooKeeper is injected, election makes it leader and publishes its endpoint.
            ZkServiceRegistry masterReg = new ZkServiceRegistry(connect);
            MasterServer master = new MasterServer(5000, 0, masterReg);
            MasterElection election = new MasterElection(masterReg, new Endpoint("127.0.0.1", master.port()));
            master.bindLeadership(election);
            election.start();

            // Worker: looks up the current leader in ZooKeeper through RegistryMasterLocator, then registers and sends heartbeats.
            ZkServiceRegistry workerReg = new ZkServiceRegistry(connect);
            io.rowweave.cluster.MasterLocator wLoc = new io.rowweave.cluster.RegistryMasterLocator(workerReg);
            File wdir = new File(tmp.toFile(), "w0"); wdir.mkdirs();
            WorkerServer worker = WorkerServer.newUnguarded("w0", 32, wLoc, wdir);
            Thread.sleep(500);  // Settle: the worker registration is visible in ZooKeeper.

            try {
                // The point of the test: only a connection string is given, so the client builds its own registry and locator and delegates to RealClusterJobClient.
                try (ZkRemoteJobClient client = new ZkRemoteJobClient(connect)) {
                    ExecutionEnvironment s = new ExecutionEnvironment("zkjob", "zkjob", client);
                    s.setParallelism(2);
                    Path f = tmp.resolve("out.tsv");
                    s.fromSource(new CollectionSource(META, rows(4000)))
                            .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                            .sinkTo(new FileSink(
                                    ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), f.toString()));
                    s.execute();
                    assertEquals(8, countLines(f), "8 groups on k, job completed fully with addressing through ZK");
                }
            } finally {
                worker.close();
                election.close();
                master.close();
                masterReg.close();
                workerReg.close();
            }
        }
    }

    private static PlanArtifact qualityArtifact(
            Path domain, QualityGatePolicy policy) {
        ExecutionEnvironment session = new ExecutionEnvironment("zk-quality", "zk quality");
        session.setDefinitionId("zk-durable-quality");
        session.setParallelism(2);
        DataRowClassification classified = session.fromSource(new CollectionSource(
                        QUALITY_META, "zk-quality-split", Arrays.asList(
                        qualityRow("A", 1), qualityRow(null, 2),
                        qualityRow("B", 13))))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(QUALITY_META)
                        .notNull("adaptor.required", 0)
                        .integerRange("period.range", 1, 1, 12)
                        .build());
        classified.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy));
        return session.toPlanArtifact();
    }

    private static DataRow qualityRow(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static RunSubmission submission(String runId) {
        return new RunSubmission(runId, "attempt-" + runId,
                "publication-" + runId, 1, 0L, 30L);
    }

    private static int preparedOutputActiveCount(WorkerServer worker) {
        try {
            java.lang.reflect.Method method = WorkerServer.class
                    .getDeclaredMethod("preparedOutputActiveCount");
            method.setAccessible(true);
            return (Integer) method.invoke(worker);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    private static int successfulDurableAssignments(
            WorkerServer worker, String runId) {
        try {
            java.lang.reflect.Field assignmentsField = WorkerServer.class
                    .getDeclaredField("assignments");
            assignmentsField.setAccessible(true);
            Object ledger = assignmentsField.get(worker);
            java.lang.reflect.Field slotsField = ledger.getClass()
                    .getDeclaredField("slots");
            slotsField.setAccessible(true);
            int successful = 0;
            synchronized (ledger) {
                @SuppressWarnings("unchecked")
                java.util.Map<Long, Object> slots =
                        (java.util.Map<Long, Object>) slotsField.get(ledger);
                for (Object slot : slots.values()) {
                    java.lang.reflect.Field fenceField = slot.getClass()
                            .getDeclaredField("runAttemptFence");
                    java.lang.reflect.Field receiptField = slot.getClass()
                            .getDeclaredField("terminalReceipt");
                    fenceField.setAccessible(true);
                    receiptField.setAccessible(true);
                    io.rowweave.run.RunAttemptFence fence =
                            (io.rowweave.run.RunAttemptFence) fenceField.get(slot);
                    io.rowweave.run.StageTerminalReceipt receipt =
                            (io.rowweave.run.StageTerminalReceipt) receiptField.get(slot);
                    if (runId.equals(fence.runId()) && receipt != null
                            && receipt.succeeded()) {
                        successful++;
                    }
                }
            }
            return successful;
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    private static void awaitRecoveryInventory(MasterServer master) {
        try {
            java.lang.reflect.Method generation = MasterServer.class
                    .getDeclaredMethod("recoveryInventoryGeneration");
            java.lang.reflect.Method await = MasterServer.class
                    .getDeclaredMethod("awaitRecoveryInventory", long.class,
                            long.class, TimeUnit.class);
            generation.setAccessible(true);
            await.setAccessible(true);
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(10L);
            long current;
            do {
                current = (Long) generation.invoke(master);
                if (current != 0L) break;
                Thread.sleep(20L);
            } while (System.nanoTime() < deadline);
            assertTrue(current != 0L);
            assertTrue((Boolean) await.invoke(
                    master, current, 10L, TimeUnit.SECONDS));
        } catch (ReflectiveOperationException | InterruptedException failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new AssertionError(failure);
        }
    }
}
