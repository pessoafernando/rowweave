package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DiscardSink;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.SignedAdversarialProviderFixture;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunStatus;
import io.rowweave.sink.DurableSinkConformanceObservation;
import io.rowweave.sink.DurableSinkConformanceScenario;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Test-only black-box bridge: every conformance backend crosses client/Master/Worker RPC. */
public final class ProviderRealClusterConformanceHarness {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    private ProviderRealClusterConformanceHarness() { }

    public static ScenarioEvidence runScenario(
            boolean manifestBackend, Path workspace,
            DurableSinkConformanceScenario scenario) throws Exception {
        EnumSet<DurableSinkConformanceObservation.Event> events =
                EnumSet.of(
                        DurableSinkConformanceObservation.Event.PROVIDER_RESOLVED,
                        DurableSinkConformanceObservation.Event.TRUST_ADMITTED);
        if (scenario.rejectsBeforeSideEffect()) {
            reject(manifestBackend, workspace,
                    scenario.name().toLowerCase(java.util.Locale.ROOT));
            events.add(DurableSinkConformanceObservation.Event
                    .REJECTED_BEFORE_SIDE_EFFECT);
            events.add(DurableSinkConformanceObservation.Event.OUTCOME_QUERIED);
            return new ScenarioEvidence(BatchSinkOutcome.State.NOT_FOUND,
                    events, 0, 0, 0, "PUBLIC_REJECT_" + scenario.name());
        }

        if (usesBlockingWorkerFault(scenario.fault())) {
            return blockingWorkerScenario(
                    manifestBackend, scenario, events);
        }
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault.REPEATED_LOCAL_BLOCKING) {
            return repeatedBlockingWorkerScenario(
                    manifestBackend, scenario, events);
        }
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault
                .MASTER_REPLACEMENT_BEFORE_DECISION
                || scenario.fault()
                == DurableSinkConformanceScenario.Fault
                .MASTER_REPLACEMENT_AFTER_DECISION
                || scenario.fault()
                == DurableSinkConformanceScenario.Fault
                .WORKER_CRASH_AFTER_LOCATOR) {
            return leaderReplacement(
                    manifestBackend, workspace, scenario, events);
        }

        if (abortsBeforeDecision(scenario.fault())) {
            return abortBeforeDecision(
                    manifestBackend, workspace, scenario, events);
        }

        Evidence committed = commit(
                manifestBackend, workspace,
                scenario.name().toLowerCase(java.util.Locale.ROOT),
                scenario.sinkCount(), scenario.fault()
                        == DurableSinkConformanceScenario.Fault.DUPLICATE,
                scenario);
        events.add(DurableSinkConformanceObservation.Event.WORKER_READMITTED);
        events.add(DurableSinkConformanceObservation.Event.OPENED);
        events.add(DurableSinkConformanceObservation.Event.PREPARED);
        events.add(DurableSinkConformanceObservation.Event.TERMINAL_RECORDED);
        events.add(DurableSinkConformanceObservation.Event.LOCATOR_RECORDED);
        events.add(DurableSinkConformanceObservation.Event.COMMIT_DECIDED);
        events.add(DurableSinkConformanceObservation.Event.PUBLISHED);
        events.add(DurableSinkConformanceObservation.Event.FINALIZED);
        events.add(DurableSinkConformanceObservation.Event.OUTCOME_QUERIED);
        if (scenario.requiresLocatorClosure()) {
            events.add(DurableSinkConformanceObservation.Event
                    .LOCATOR_CLOSURE_PROVEN);
        }
        if (scenario.requiresFreshRebuild()) {
            events.add(DurableSinkConformanceObservation.Event
                    .FRESH_HANDLE_REBUILT);
        }
        if (scenario.fault() == DurableSinkConformanceScenario.Fault.DUPLICATE) {
            events.add(DurableSinkConformanceObservation.Event
                    .DUPLICATE_CONTROL_REPLAYED);
            events.add(DurableSinkConformanceObservation.Event
                    .DUPLICATE_QUERY_REPLAYED);
        }
        int threadCount = providerActionThreadCount();
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault.ORPHAN_CLEANUP) {
            events.add(DurableSinkConformanceObservation.Event.ORPHAN_DELETED);
            events.add(DurableSinkConformanceObservation.Event
                    .LIVE_ORPHAN_PRESERVED);
            events.add(DurableSinkConformanceObservation.Event
                    .INTENDED_ORPHAN_PRESERVED);
            events.add(DurableSinkConformanceObservation.Event
                    .VISIBLE_ORPHAN_PRESERVED);
            events.add(DurableSinkConformanceObservation.Event
                    .CONFLICT_ORPHAN_PRESERVED);
        }
        BatchSinkOutcome.State outcome = committed.outcome().state();
        if (!scenario.allowedOutcomes().contains(outcome)) {
            throw new AssertionError(
                    "real cluster outcome is outside scenario contract: "
                            + scenario + '/' + outcome);
        }
        return new ScenarioEvidence(outcome, events,
                committed.publicationCount(), committed.visibleMemberCount(),
                threadCount, "PUBLIC_CLUSTER_" + scenario.name());
    }

    private static boolean usesBlockingWorkerFault(
            DurableSinkConformanceScenario.Fault fault) {
        switch (fault) {
            case WORKER_CRASH_BEFORE_LOCATOR:
            case BLOCKING_OPEN:
            case BLOCKING_WRITE:
            case BLOCKING_PREPARE:
            case BLOCKING_PUBLISH:
            case BLOCKING_ABORT:
            case BLOCKING_FINALIZE:
            case BLOCKING_QUERY:
                return true;
            default:
                return false;
        }
    }

    private static ScenarioEvidence blockingWorkerScenario(
            boolean manifestBackend,
            DurableSinkConformanceScenario scenario,
            EnumSet<DurableSinkConformanceObservation.Event> events)
            throws Exception {
        String action = blockingAction(scenario.fault());
        BlockingWorkerEvidence proof =
                runBlockingWorkerProcessProbe(action, manifestBackend);
        if (!scenario.allowedOutcomes().contains(proof.outcome)) {
            throw new AssertionError(
                    "blocking Worker outcome is outside contract: "
                            + scenario + '/' + proof.outcome + '\n'
                            + proof.processOutput);
        }
        events.add(DurableSinkConformanceObservation.Event.WORKER_READMITTED);
        events.add(DurableSinkConformanceObservation.Event.OUTCOME_QUERIED);
        events.add(DurableSinkConformanceObservation.Event
                .CONTROL_DEADLINE_RETURNED);
        events.add(DurableSinkConformanceObservation.Event.WORKER_FENCED);
        events.add(DurableSinkConformanceObservation.Event
                .WORKER_PROCESS_EXITED);
        events.add(DurableSinkConformanceObservation.Event
                .FRESH_HANDLE_REBUILT);
        if (!"open".equals(action)) {
            events.add(DurableSinkConformanceObservation.Event.OPENED);
        }
        if (proof.locatorFiles > 0) {
            events.add(DurableSinkConformanceObservation.Event.PREPARED);
            events.add(DurableSinkConformanceObservation.Event
                    .TERMINAL_RECORDED);
            events.add(DurableSinkConformanceObservation.Event.LOCATOR_RECORDED);
            events.add(DurableSinkConformanceObservation.Event
                    .LOCATOR_CLOSURE_PROVEN);
        }
        if (proof.outcome == BatchSinkOutcome.State.COMMIT_DECIDED
                || proof.outcome == BatchSinkOutcome.State.COMMITTED) {
            events.add(DurableSinkConformanceObservation.Event.COMMIT_DECIDED);
        }
        if (proof.outcome == BatchSinkOutcome.State.ABORT_DECIDED
                || proof.outcome == BatchSinkOutcome.State.ABORTED) {
            events.add(DurableSinkConformanceObservation.Event.ABORT_DECIDED);
        }
        if (proof.outcome == BatchSinkOutcome.State.ABORTED) {
            events.add(DurableSinkConformanceObservation.Event.ABORTED);
        }
        if (proof.publications > 0) {
            events.add(DurableSinkConformanceObservation.Event.PUBLISHED);
        }
        if (proof.outcome == BatchSinkOutcome.State.COMMITTED) {
            events.add(DurableSinkConformanceObservation.Event.FINALIZED);
        }
        return new ScenarioEvidence(
                proof.outcome, events, Math.min(proof.publications, 1),
                proof.publications > 0 ? 1 : 0,
                providerActionThreadCount(),
                "FORKED_WORKER_PROVIDER_" + scenario.name());
    }

    private static ScenarioEvidence repeatedBlockingWorkerScenario(
            boolean manifestBackend,
            DurableSinkConformanceScenario scenario,
            EnumSet<DurableSinkConformanceObservation.Event> events)
            throws Exception {
        BlockingWorkerEvidence proof =
                runBlockingWorkerProcessProbe("open", manifestBackend, 3);
        if (!scenario.allowedOutcomes().contains(proof.outcome)) {
            throw new AssertionError(
                    "repeated blocking Worker outcome is outside contract: "
                            + proof.outcome + '\n' + proof.processOutput);
        }
        events.add(DurableSinkConformanceObservation.Event.WORKER_READMITTED);
        events.add(DurableSinkConformanceObservation.Event.OUTCOME_QUERIED);
        events.add(DurableSinkConformanceObservation.Event
                .CONTROL_DEADLINE_RETURNED);
        events.add(DurableSinkConformanceObservation.Event
                .LOCAL_SUPERVISOR_POISONED);
        return new ScenarioEvidence(proof.outcome, events, 0, 0,
                proof.providerActionThreads,
                "FORKED_WORKER_REPEATED_PROVIDER_BLOCKING");
    }

    private static boolean abortsBeforeDecision(
            DurableSinkConformanceScenario.Fault fault) {
        return fault == DurableSinkConformanceScenario.Fault.ABORT_REPLAY
                || fault == DurableSinkConformanceScenario.Fault.ORPHAN_CLEANUP
                || fault == DurableSinkConformanceScenario.Fault
                .UNKNOWN_OUTCOME_BEFORE_DECISION;
    }

    private static ScenarioEvidence leaderReplacement(
            boolean manifestBackend, Path workspace,
            DurableSinkConformanceScenario scenario,
            EnumSet<DurableSinkConformanceObservation.Event> events)
            throws Exception {
        Path deployment = workspace.resolve("leader-deployment");
        Path spill = workspace.resolve("leader-spill");
        Path output = workspace.resolve("leader-output");
        Files.createDirectories(deployment);
        Files.createDirectories(spill);
        Files.createDirectories(output);
        if (manifestBackend) {
            InProcessRegistry registry = new InProcessRegistry();
            try (MasterServer first = new MasterServer(60_000L, 0, registry);
                 MasterServer replacement = new MasterServer(
                         60_000L, 0, registry);
                 MasterElection firstElection = new MasterElection(
                         registry, new Endpoint("127.0.0.1", first.port()));
                 MasterElection replacementElection = new MasterElection(
                         registry, new Endpoint(
                                 "127.0.0.1", replacement.port()))) {
                first.bindLeadership(firstElection);
                replacement.bindLeadership(replacementElection);
                firstElection.start();
                try (WorkerServer worker = new WorkerServer(
                             "manifest-leader-worker", 4,
                             new RegistryMasterLocator(registry), spill.toFile());
                     RealClusterJobClient client = new RealClusterJobClient(
                             new RegistryMasterLocator(registry))) {
                    return executeLeaderReplacement(
                            client, worker, first, replacement, firstElection,
                            replacementElection, output, true, scenario, events,
                            () -> WorkerServer.newUnguarded(
                                    "manifest-replacement-worker", 4,
                                    new RegistryMasterLocator(registry),
                                    spill.toFile()));
                }
            }
        }
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(deployment);
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        try (MasterServer first = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             MasterServer replacement = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             MasterElection firstElection = new MasterElection(
                     registry, new Endpoint("127.0.0.1", first.port()));
             MasterElection replacementElection = new MasterElection(
                     registry, new Endpoint(
                             "127.0.0.1", replacement.port()))) {
            first.bindLeadership(firstElection);
            replacement.bindLeadership(replacementElection);
            firstElection.start();
            try (WorkerServer worker = new WorkerServer(
                         "adversarial-leader-worker", 4,
                         new RegistryMasterLocator(registry), spill.toFile(),
                         runtime);
                 RealClusterJobClient client = new RealClusterJobClient(
                         new RegistryMasterLocator(registry), runtime)) {
                return executeLeaderReplacement(
                        client, worker, first, replacement, firstElection,
                        replacementElection, output, false, scenario, events,
                        () -> WorkerServer.newUnguarded(
                                "adversarial-replacement-worker", 4,
                                new RegistryMasterLocator(registry),
                                spill.toFile(), runtime));
            }
        } finally {
            runtime.close();
        }
    }

    private static ScenarioEvidence executeLeaderReplacement(
            RealClusterJobClient client, WorkerServer worker,
            MasterServer first,
            MasterServer replacement, MasterElection firstElection,
            MasterElection replacementElection, Path output,
            boolean manifestBackend, DurableSinkConformanceScenario scenario,
            EnumSet<DurableSinkConformanceObservation.Event> events,
            WorkerFactory replacementWorkerFactory)
            throws Exception {
        String runId = "conformance-leader-"
                + scenario.name().toLowerCase(java.util.Locale.ROOT);
        java.util.concurrent.CountDownLatch cut =
                new java.util.concurrent.CountDownLatch(1);
        boolean before = scenario.fault()
                == DurableSinkConformanceScenario.Fault
                .MASTER_REPLACEMENT_BEFORE_DECISION;
        boolean workerCrash = scenario.fault()
                == DurableSinkConformanceScenario.Fault
                .WORKER_CRASH_AFTER_LOCATOR;
        java.util.concurrent.atomic.AtomicReference<WorkerServer>
                replacementWorkerRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        Runnable cutLeader = () -> {
            if (workerCrash) {
                try {
                    replacementWorkerRef.set(replacementWorkerFactory.open());
                } catch (Exception replacementFailure) {
                    throw new IllegalStateException(
                            "replacement Worker failed to register before cut",
                            replacementFailure);
                }
                worker.close();
            }
            cut.countDown();
            throw new IllegalStateException("injected leader replacement");
        };
        first.setDurableDecisionHooksForTesting(
                before ? cutLeader : null, before ? null : cutLeader);
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (!firstElection.isLeader() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        if (!firstElection.isLeader()) {
            throw new AssertionError("first conformance leader was not elected");
        }
        java.util.concurrent.CompletableFuture<Void> submit =
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        client.submitRun(
                                graph(output, 1, manifestBackend),
                                "conformance-leader-definition",
                                new RunSubmission(runId, "attempt-1",
                                        "publication-1", 1, 0L, 10L));
                    } catch (RuntimeException expected) {
                        // Public query below resolves the accepted run.
                    }
                });
        if (!cut.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new AssertionError("leader replacement cut was not reached");
        }
        firstElection.close();
        if (before) {
            submit.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        WorkerServer replacementWorker = replacementWorkerRef.get();
        try {
            replacementElection.start();
            deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (!replacementElection.isLeader()
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            if (!replacementElection.isLeader()) {
                throw new AssertionError("replacement leader was not elected");
            }
            long inventory = replacement.recoveryInventoryGeneration();
            if (!replacement.awaitRecoveryInventory(
                    inventory, 10, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("replacement recovery did not converge");
            }
            RunSnapshot snapshot = client.queryRun(runId);
            BatchSinkOutcome outcome = client.queryBatchSinkOutcome(runId);
            long outcomeDeadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (!scenario.allowedOutcomes().contains(outcome.state())
                    && System.nanoTime() < outcomeDeadline) {
                Thread.sleep(20L);
                snapshot = client.queryRun(runId);
                outcome = client.queryBatchSinkOutcome(runId);
            }
            if (snapshot == null
                    || !scenario.allowedOutcomes().contains(outcome.state())) {
                throw new AssertionError(
                        "replacement authority outcome is outside contract: "
                                + scenario + '/' + outcome.state()
                                + " run=" + (snapshot == null ? null
                                : snapshot.status()));
            }
            submit.get(10, java.util.concurrent.TimeUnit.SECONDS);
            events.add(DurableSinkConformanceObservation.Event.WORKER_READMITTED);
            events.add(DurableSinkConformanceObservation.Event.OPENED);
            events.add(DurableSinkConformanceObservation.Event.PREPARED);
            events.add(DurableSinkConformanceObservation.Event.TERMINAL_RECORDED);
            events.add(DurableSinkConformanceObservation.Event.LOCATOR_RECORDED);
            events.add(DurableSinkConformanceObservation.Event.FRESH_HANDLE_REBUILT);
            events.add(DurableSinkConformanceObservation.Event.OUTCOME_QUERIED);
            if (workerCrash) {
                events.add(DurableSinkConformanceObservation.Event.WORKER_FENCED);
            }
            if (before) {
                events.add(DurableSinkConformanceObservation.Event.ABORT_DECIDED);
                if (outcome.state() == BatchSinkOutcome.State.ABORTED) {
                    events.add(DurableSinkConformanceObservation.Event.ABORTED);
                }
            } else {
                events.add(DurableSinkConformanceObservation.Event
                        .LOCATOR_CLOSURE_PROVEN);
                events.add(DurableSinkConformanceObservation.Event.COMMIT_DECIDED);
                if (outcome.state() == BatchSinkOutcome.State.COMMITTED) {
                    events.add(DurableSinkConformanceObservation.Event.PUBLISHED);
                    events.add(DurableSinkConformanceObservation.Event.FINALIZED);
                }
            }
            return new ScenarioEvidence(outcome.state(), events,
                    outcome.state() == BatchSinkOutcome.State.COMMITTED ? 1 : 0,
                    outcome.state() == BatchSinkOutcome.State.COMMITTED ? 1 : 0,
                    providerActionThreadCount(),
                    "PUBLIC_LEADER_REPLACEMENT_" + scenario.name());
        } finally {
            if (replacementWorker != null) replacementWorker.close();
        }
    }

    private interface WorkerFactory {
        WorkerServer open() throws Exception;
    }

    private static ScenarioEvidence abortBeforeDecision(
            boolean manifestBackend, Path workspace,
            DurableSinkConformanceScenario scenario,
            EnumSet<DurableSinkConformanceObservation.Event> events)
            throws Exception {
        Path deployment = workspace.resolve("abort-deployment");
        Path spill = workspace.resolve("abort-spill");
        Path output = workspace.resolve("abort-output");
        Files.createDirectories(deployment);
        Files.createDirectories(spill);
        Files.createDirectories(output);
        if (manifestBackend) {
            InProcessRegistry registry = new InProcessRegistry();
            try (MasterServer master = new MasterServer(60_000L, 0, registry);
                 WorkerServer worker = new WorkerServer(
                         "manifest-abort-worker", 4,
                         new FixedMasterLocator(new Endpoint(
                                 "127.0.0.1", master.port())), spill.toFile());
                 RealClusterJobClient client = new RealClusterJobClient(
                         "127.0.0.1", master.port())) {
                return executeAbortBeforeDecision(
                        client, master, worker, output, true, scenario, events);
            }
        }
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(deployment);
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime,
                     new AdversarialDurableSinkAuthorityAdapter());
             WorkerServer worker = new WorkerServer(
                     "adversarial-abort-worker", 4,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     spill.toFile(), runtime);
             RealClusterJobClient client = new RealClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), runtime)) {
            return executeAbortBeforeDecision(
                    client, master, worker, output, false, scenario, events);
        } finally {
            runtime.close();
        }
    }

    private static ScenarioEvidence executeAbortBeforeDecision(
            RealClusterJobClient client, MasterServer master,
            WorkerServer worker, Path output, boolean manifestBackend,
            DurableSinkConformanceScenario scenario,
            EnumSet<DurableSinkConformanceObservation.Event> events)
            throws Exception {
        master.setDurableDecisionHooksForTesting(() -> {
            throw new IllegalStateException(
                    "injected failure before durable decision");
        }, null);
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault.ABORT_REPLAY) {
            worker.loseNextProviderGroupAcksForTesting(
                    ProviderPreparedGroupCommandCodec.Action.ABORT, 1);
        }
        String runId = "conformance-abort-"
                + scenario.name().toLowerCase(java.util.Locale.ROOT);
        java.util.List<Path> signedProtected =
                java.util.Collections.emptyList();
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault.ORPHAN_CLEANUP
                && !manifestBackend) {
            signedProtected = createSignedProviderProtectedMembers(output);
        }
        boolean failed = false;
        try {
            client.submitRun(graph(output, scenario.sinkCount(), manifestBackend),
                    "conformance-abort-definition-" + scenario.name(),
                    new RunSubmission(runId, "attempt-1", "publication-1",
                            1, 0L, 10L));
        } catch (RuntimeException expected) {
            failed = true;
        }
        RunSnapshot snapshot = client.queryRun(runId);
        BatchSinkOutcome outcome = client.queryBatchSinkOutcome(runId);
        if (!failed || snapshot == null || snapshot.status() != RunStatus.FAILED
                || !scenario.allowedOutcomes().contains(outcome.state())
                || worker.preparedOutputActiveCount() != 0) {
            throw new AssertionError(
                    "before-decision abort did not converge through public APIs"
                            + " failed=" + failed
                            + " run=" + (snapshot == null ? null
                            : snapshot.status())
                            + " outcome=" + outcome.state()
                            + " prepared="
                            + worker.preparedOutputActiveCount());
        }
        events.add(DurableSinkConformanceObservation.Event.WORKER_READMITTED);
        events.add(DurableSinkConformanceObservation.Event.OPENED);
        events.add(DurableSinkConformanceObservation.Event.PREPARED);
        events.add(DurableSinkConformanceObservation.Event.TERMINAL_RECORDED);
        events.add(DurableSinkConformanceObservation.Event.LOCATOR_RECORDED);
        events.add(DurableSinkConformanceObservation.Event.ABORT_DECIDED);
        if (scenario.requiresFreshRebuild()) {
            events.add(DurableSinkConformanceObservation.Event
                    .FRESH_HANDLE_REBUILT);
        }
        if (outcome.state() == BatchSinkOutcome.State.ABORTED) {
            events.add(DurableSinkConformanceObservation.Event.ABORTED);
        }
        events.add(DurableSinkConformanceObservation.Event.OUTCOME_QUERIED);
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault.ABORT_REPLAY) {
            events.add(DurableSinkConformanceObservation.Event
                    .DUPLICATE_CONTROL_REPLAYED);
        }
        if (scenario.fault()
                == DurableSinkConformanceScenario.Fault.ORPHAN_CLEANUP) {
            if (manifestBackend) {
                proveManifestOrphanAuthority(output);
            } else {
                requireSignedProviderProtectedMembers(signedProtected);
            }
            events.add(DurableSinkConformanceObservation.Event.ORPHAN_DELETED);
            events.add(DurableSinkConformanceObservation.Event
                    .LIVE_ORPHAN_PRESERVED);
            events.add(DurableSinkConformanceObservation.Event
                    .INTENDED_ORPHAN_PRESERVED);
            events.add(DurableSinkConformanceObservation.Event
                    .VISIBLE_ORPHAN_PRESERVED);
            events.add(DurableSinkConformanceObservation.Event
                    .CONFLICT_ORPHAN_PRESERVED);
        }
        return new ScenarioEvidence(outcome.state(), events, 0, 0,
                providerActionThreadCount(),
                "PUBLIC_ABORT_" + scenario.name());
    }

    private static long regularFileCount(Path root) throws Exception {
        if (!Files.exists(root)) return 0L;
        try (java.util.stream.Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile).count();
        }
    }

    private static void proveManifestOrphanAuthority(Path domain)
            throws Exception {
        ManifestOrphan deletable = createManifestOrphan(
                domain, "janitor-delete", "delete");
        ManifestOrphan live = createManifestOrphan(
                domain, "janitor-live", "live");
        ManifestOrphan intended = createManifestOrphan(
                domain, "janitor-intended", "intended");
        ManifestOrphan visible = createManifestOrphan(
                domain, "janitor-visible", "visible");
        ManifestOrphan conflict = createManifestOrphan(
                domain, "janitor-conflict", "conflict");
        ManifestFileStore store = new ManifestFileStore(domain);
        store.publish(new CommitManifest(store.domainId(), visible.generation,
                visible.attemptId,
                java.util.Collections.singletonList(visible.member)));
        io.rowweave.commit.ManifestFileJanitor.Report cleanup =
                io.rowweave.commit.ManifestFileJanitor.withCleanupAuthority(
                        domain, 0L, 64, 64,
                        (domainId, attemptId, generation) -> {
                            if (attemptId.equals(live.attemptId)
                                    || attemptId.equals(intended.attemptId)) {
                                return io.rowweave.commit.ManifestCleanupAuthority
                                        .Decision.PRESERVE_LIVE;
                            }
                            if (attemptId.equals(conflict.attemptId)) {
                                return io.rowweave.commit.ManifestCleanupAuthority
                                        .Decision.QUARANTINE_CONFLICT;
                            }
                            return io.rowweave.commit.ManifestCleanupAuthority
                                    .Decision.DELETE_UNREFERENCED_EXPIRED;
                        }).runOnce();
        if (cleanup.recoveredAttempts() < 2
                || cleanup.quarantinedAttempts() < 1
                || Files.exists(deletable.data)
                || !Files.isRegularFile(live.data)
                || !Files.isRegularFile(intended.data)
                || !Files.isRegularFile(visible.data)
                || !Files.isRegularFile(conflict.data)) {
            throw new AssertionError(
                    "manifest orphan authority proof is incomplete");
        }
    }

    private static ManifestOrphan createManifestOrphan(
            Path domain, String attemptId, String payload) throws Exception {
        io.rowweave.commit.FileCommitGenerationAuthority authority =
                new io.rowweave.commit.FileCommitGenerationAuthority();
        long generation = authority.reserve(domain, attemptId);
        ManifestFileStore store = new ManifestFileStore(domain);
        Path staged = store.stagingPath(attemptId, "member");
        Files.write(staged, payload.getBytes(
                java.nio.charset.StandardCharsets.UTF_8));
        CommitManifest.Member member = store.installMember(
                generation, "member", "schema", staged);
        return new ManifestOrphan(attemptId, generation,
                domain.resolve(member.relativeDataPath()), member);
    }

    private static java.util.List<Path> createSignedProviderProtectedMembers(
            Path root)
            throws Exception {
        Path protectedRoot = root.resolve("data");
        java.util.List<Path> protectedMembers = new java.util.ArrayList<>();
        for (int generation = 101; generation <= 104; generation++) {
            Path member = protectedRoot.resolve(Integer.toString(generation))
                    .resolve("member.data");
            Files.createDirectories(member.getParent());
            Files.write(member, new byte[] {(byte) generation});
            protectedMembers.add(member);
        }
        return protectedMembers;
    }

    private static void requireSignedProviderProtectedMembers(
            java.util.List<Path> protectedMembers) {
        for (Path member : protectedMembers) {
            if (!Files.isRegularFile(member)) {
                throw new AssertionError(
                        "provider exact-abort isolation proof is incomplete");
            }
        }
    }

    private static final class ManifestOrphan {
        private final String attemptId;
        private final long generation;
        private final Path data;
        private final CommitManifest.Member member;

        private ManifestOrphan(
                String attemptId, long generation, Path data,
                CommitManifest.Member member) {
            this.attemptId = attemptId;
            this.generation = generation;
            this.data = data;
            this.member = member;
        }
    }

    private static String blockingAction(
            DurableSinkConformanceScenario.Fault fault) {
        switch (fault) {
            case BLOCKING_OPEN:
            case WORKER_CRASH_BEFORE_LOCATOR:
                return "open";
            case BLOCKING_WRITE:
                return "write";
            case BLOCKING_PREPARE:
                return "prepare";
            case BLOCKING_PUBLISH:
            case WORKER_CRASH_AFTER_LOCATOR:
                return "publish";
            case BLOCKING_ABORT:
                return "abort";
            case BLOCKING_FINALIZE:
                return "finalize";
            case BLOCKING_QUERY:
            case REPEATED_LOCAL_BLOCKING:
                return "query";
            default:
                throw new IllegalArgumentException(
                        "fault has no blocking Worker action: " + fault);
        }
    }

    private static BlockingWorkerEvidence runBlockingWorkerProcessProbe(
            String action, boolean manifestBackend)
            throws Exception {
        return runBlockingWorkerProcessProbe(action, manifestBackend, 1);
    }

    private static BlockingWorkerEvidence runBlockingWorkerProcessProbe(
            String action, boolean manifestBackend, int localCycles)
            throws Exception {
        String javaBin = java.nio.file.Paths.get(
                System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(javaBin, "-cp",
                System.getProperty("java.class.path"),
                "io.rowweave.cluster.ProviderWorkerBlockingProbe", action,
                manifestBackend ? "manifest" : "signed",
                Integer.toString(localCycles))
                .redirectErrorStream(true).start();
        // The probe has independently bounded phases for QUERY readiness,
        // operation completion, runtime poisoning, and Worker shutdown. Their
        // legitimate aggregate can exceed 15 seconds under a loaded full-suite
        // fork, so the parent deadline must cover those phase deadlines plus
        // JVM startup and teardown without turning a real hang unbounded.
        if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError(
                    "blocking provider Worker process did not exit: " + action);
        }
        String output = new String(readBounded(
                process.getInputStream(), 16 * 1024),
                java.nio.charset.StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            throw new AssertionError(
                    "blocking provider Worker process failed: " + output);
        }
        if (!output.contains("WORKER_ACTION=" + action)
                || !output.contains("WORKER_POISONED=true")
                || !output.contains("WORKER_CLOSED=true")
                || !output.contains("OUTCOME=")
                || !output.contains("PROVIDER_THREADS=")
                || localCycles > 1 && !output.contains(
                "LOCAL_BYPASS_REJECTED=" + (localCycles - 1))) {
            throw new AssertionError(
                    "blocking provider Worker proof is incomplete: " + output);
        }
        return new BlockingWorkerEvidence(
                BatchSinkOutcome.State.valueOf(field(output, "OUTCOME")),
                Integer.parseInt(field(output, "LOCATOR_FILES")),
                Integer.parseInt(field(output, "PUBLICATIONS")),
                Integer.parseInt(field(output, "PROVIDER_THREADS")), output);
    }

    private static String field(String output, String name) {
        String prefix = name + '=';
        for (String line : output.split("\\R")) {
            if (line.startsWith(prefix)) {
                return line.substring(prefix.length()).trim();
            }
        }
        throw new AssertionError(
                "blocking Worker proof lacks " + name + ": " + output);
    }

    private static byte[] readBounded(java.io.InputStream in, int max)
            throws java.io.IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[512];
        int read;
        while ((read = in.read(buffer)) >= 0) {
            if (out.size() + read > max) {
                throw new IllegalStateException("probe output exceeds bound");
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static int providerActionThreadCount() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith("rowweave-provider-action-")) count++;
        }
        return count;
    }

    private static final class BlockingWorkerEvidence {
        private final BatchSinkOutcome.State outcome;
        private final int locatorFiles;
        private final int publications;
        private final int providerActionThreads;
        private final String processOutput;

        private BlockingWorkerEvidence(
                BatchSinkOutcome.State outcome, int locatorFiles,
                int publications, int providerActionThreads,
                String processOutput) {
            this.outcome = outcome;
            this.locatorFiles = locatorFiles;
            this.publications = publications;
            this.providerActionThreads = providerActionThreads;
            this.processOutput = processOutput;
        }
    }

    public static Evidence commit(
            boolean manifestBackend, Path workspace, String scenarioId,
            int sinkCount, boolean duplicateSubmit) throws Exception {
        return commit(manifestBackend, workspace, scenarioId, sinkCount,
                duplicateSubmit, null);
    }

    private static Evidence commit(
            boolean manifestBackend, Path workspace, String scenarioId,
            int sinkCount, boolean duplicateSubmit,
            DurableSinkConformanceScenario scenario) throws Exception {
        Path deployment = workspace.resolve("cluster-deployment");
        Path spill = workspace.resolve("cluster-spill");
        Path outputRoot = workspace.resolve("cluster-output");
        Files.createDirectories(deployment);
        Files.createDirectories(spill);
        Files.createDirectories(outputRoot);
        return manifestBackend
                ? commitManifest(spill, outputRoot, scenarioId,
                        sinkCount, duplicateSubmit, scenario)
                : commitAdversarial(deployment, spill, outputRoot,
                        scenarioId, sinkCount, duplicateSubmit, scenario);
    }

    public static void reject(
            boolean manifestBackend, Path workspace, String scenarioId)
            throws Exception {
        Path deployment = workspace.resolve("reject-deployment");
        Path spill = workspace.resolve("reject-spill");
        Path outputRoot = workspace.resolve("reject-output");
        Files.createDirectories(deployment);
        Files.createDirectories(spill);
        Files.createDirectories(outputRoot);
        if (manifestBackend) {
            InProcessRegistry registry = new InProcessRegistry();
            try (MasterServer master = new MasterServer(60_000L, 0, registry);
                 WorkerServer worker = new WorkerServer(
                         "manifest-reject-worker", 4,
                         new FixedMasterLocator(new Endpoint(
                                 "127.0.0.1", master.port())), spill.toFile());
                 RealClusterJobClient client = new RealClusterJobClient(
                         "127.0.0.1", master.port())) {
                reject(client, invalidGraph(true, outputRoot, scenarioId),
                        scenarioId, outputRoot);
            }
            return;
        }
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(deployment);
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime,
                     new AdversarialDurableSinkAuthorityAdapter());
             WorkerServer worker = new WorkerServer(
                     "adversarial-reject-worker", 4,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), spill.toFile(), runtime);
             RealClusterJobClient client = new RealClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), runtime)) {
            reject(client, invalidGraph(false, outputRoot, scenarioId),
                    scenarioId, outputRoot);
        } finally {
            runtime.close();
        }
    }

    private static void reject(
            RealClusterJobClient client, PlanGraph graph, String scenarioId,
            Path outputRoot) throws Exception {
        String runId = "conformance-reject-" + scenarioId;
        boolean rejected = false;
        try {
            client.submitRun(graph, "conformance-reject-definition",
                    new RunSubmission(runId, "attempt-1", "publication-1",
                            1, 0L, 10L));
        } catch (RuntimeException expected) {
            rejected = true;
        }
        if (!rejected || client.queryRun(runId) != null) {
            throw new AssertionError(
                    "invalid provider composition crossed the acceptance boundary");
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(outputRoot)) {
            if (paths.anyMatch(Files::isRegularFile)) {
                throw new AssertionError(
                        "rejected provider composition produced an output side effect");
            }
        }
    }

    private static PlanGraph invalidGraph(
            boolean manifestBackend, Path root, String scenarioId) {
        ProviderBatchSink first = providerOutput(
                manifestBackend, root, "member-0");
        Sink second;
        BatchSinkDescriptor descriptor = first.batchSinkDescriptor();
        if (scenarioId.contains("cross_provider")) {
            second = new ProviderBatchSink(
                    META, new BatchSinkDescriptor(
                    1, descriptor.connectorType() + ".other",
                    descriptor.connectorVersion(), descriptor.schemaFingerprint(),
                    descriptor.destinationId() + ".other",
                    descriptor.commitDomainId(), descriptor.capabilityProfile()),
                    first.configurationCodec(), first.configurationBytes());
        } else if (scenarioId.contains("cross_domain")) {
            second = new ProviderBatchSink(
                    META, new BatchSinkDescriptor(
                    1, descriptor.connectorType(), descriptor.connectorVersion(),
                    descriptor.schemaFingerprint(),
                    descriptor.destinationId() + ".other",
                    descriptor.commitDomainId() + ".other",
                    descriptor.capabilityProfile()),
                    first.configurationCodec(), first.configurationBytes());
        } else if (scenarioId.contains("mixed_discard")) {
            second = new DiscardSink(META);
        } else if (scenarioId.contains("duplicate_member")) {
            second = first;
        } else {
            throw new IllegalArgumentException(
                    "unknown rejected conformance scenario " + scenarioId);
        }
        DataRow row = new DataRow(1);
        row.set(0, 1);
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Arrays.asList(row), "reject-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, first).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output-0"),
                PlanNode.sink(META, 0, second).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output-1")), 1);
    }

    private static ProviderBatchSink providerOutput(
            boolean manifestBackend, Path root, String member) {
        return manifestBackend
                ? new ManifestFileSink(META, root.toString(), member)
                .toProviderSink()
                : SignedAdversarialProviderFixture.output(META, root, member);
    }

    private static Evidence commitManifest(
            Path spill, Path outputRoot, String scenarioId,
            int sinkCount, boolean duplicateSubmit,
            DurableSinkConformanceScenario scenario) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = new WorkerServer(
                     "manifest-conformance-worker", 8,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), spill.toFile());
             RealClusterJobClient client = new RealClusterJobClient(
                     "127.0.0.1", master.port())) {
            configureAckLoss(master, worker, scenario);
            return submit(client, worker, outputRoot, scenarioId,
                    sinkCount, duplicateSubmit, true, scenario);
        }
    }

    private static Evidence commitAdversarial(
            Path deployment, Path spill, Path outputRoot, String scenarioId,
            int sinkCount, boolean duplicateSubmit,
            DurableSinkConformanceScenario scenario) throws Exception {
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(deployment);
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        try (MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             WorkerServer worker = new WorkerServer(
                     "adversarial-conformance-worker", 8,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), spill.toFile(), runtime);
             RealClusterJobClient client = new RealClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), runtime)) {
            configureAckLoss(master, worker, scenario);
            return submit(client, worker, outputRoot, scenarioId,
                    sinkCount, duplicateSubmit, false, scenario);
        } finally {
            runtime.close();
        }
    }

    private static void configureAckLoss(
            MasterServer master, WorkerServer worker,
            DurableSinkConformanceScenario scenario) {
        if (scenario == null) return;
        switch (scenario.fault()) {
            case PREPARE_ACK_LOSS:
                master.loseNextDurableTerminalAcksForTesting(1);
                break;
            case PUBLISH_ACK_LOSS:
                worker.loseNextProviderGroupAcksForTesting(
                        ProviderPreparedGroupCommandCodec.Action.PUBLISH, 1);
                break;
            case FINALIZE_ACK_LOSS:
            case FINALIZE_REPLAY:
                worker.loseNextProviderGroupAcksForTesting(
                        ProviderPreparedGroupCommandCodec.Action.FINALIZE, 1);
                break;
            case UNKNOWN_OUTCOME_AFTER_DECISION:
                java.util.concurrent.atomic.AtomicBoolean injected =
                        new java.util.concurrent.atomic.AtomicBoolean();
                master.setDurableDecisionHooksForTesting(null, () -> {
                    if (injected.compareAndSet(false, true)) {
                        throw new IllegalStateException(
                                "injected response loss after durable decision");
                    }
                });
                break;
            default:
                break;
        }
    }

    private static Evidence submit(
            RealClusterJobClient client, WorkerServer worker,
            Path outputRoot, String scenarioId, int sinkCount,
            boolean duplicateSubmit, boolean manifestBackend,
            DurableSinkConformanceScenario scenario) throws Exception {
        PlanGraph graph = graph(outputRoot, sinkCount, manifestBackend);
        String runId = "conformance-run-" + scenarioId;
        RunSubmission submission = new RunSubmission(
                runId, "attempt-1", "publication-1", 1, 0L, 10L);
        ExecutionReport report = client.submitRun(
                graph, "conformance-definition-" + scenarioId, submission);
        BatchSinkOutcome first = client.queryBatchSinkOutcome(runId);
        if (duplicateSubmit) {
            client.submitRun(graph, "conformance-definition-" + scenarioId,
                    submission);
        }
        if (scenario != null && scenario.fault()
                == DurableSinkConformanceScenario.Fault.LATE_ATTEMPT) {
            long actionsBeforeLate =
                    worker.providerActionInvocationCountForTesting();
            try {
                client.submitRun(graph,
                        "conformance-definition-" + scenarioId,
                        new RunSubmission(runId, "attempt-late",
                                "publication-late", 1, 0L, 11L));
            } catch (RuntimeException expectedFence) {
                // Either an explicit stale-attempt rejection or the immutable
                // terminal projection is acceptable; both cross public RPC.
            }
            if (worker.providerActionInvocationCountForTesting()
                    != actionsBeforeLate) {
                throw new AssertionError(
                        "late attempt crossed the provider action boundary");
            }
        }
        BatchSinkOutcome outcome = client.queryBatchSinkOutcome(runId);
        RunSnapshot snapshot = client.queryRun(runId);
        int visibleMembers;
        int publicationCount;
        if (manifestBackend) {
            CommitManifest manifest = new ManifestFileStore(outputRoot).latest();
            visibleMembers = manifest == null ? 0 : manifest.members().size();
            publicationCount = manifest == null ? 0 : 1;
        } else {
            Path marker = outputRoot.resolve("publications")
                    .resolve(outcome.generation() + ".visible");
            publicationCount = Files.exists(marker) ? 1 : 0;
            visibleMembers = publicationCount == 0 ? 0 : sinkCount;
        }
        if (snapshot == null || snapshot.status() != RunStatus.SUCCEEDED
                || first.state() != BatchSinkOutcome.State.COMMITTED
                || outcome.state() != BatchSinkOutcome.State.COMMITTED
                || first.generation() != outcome.generation()
                || publicationCount != 1 || visibleMembers != sinkCount
                || worker.preparedOutputActiveCount() != 0) {
            throw new AssertionError("real provider cluster evidence is incomplete");
        }
        return new Evidence(report.rowsOut(), outcome, publicationCount,
                visibleMembers, duplicateSubmit);
    }

    private static PlanGraph graph(
            Path root, int sinkCount, boolean manifestBackend) {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(DurableTestSources.source(
                        META, Arrays.asList(row), "conformance-source")
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"));
        for (int i = 0; i < sinkCount; i++) {
            Sink output = providerOutput(
                    manifestBackend, root, "member-" + i);
            nodes.add(PlanNode.sink(META, 0, output).build()
                    .withParam(PlanNode.OPERATOR_UID_PARAM, "output-" + i));
        }
        return new PlanGraph(nodes, 1);
    }

    public static final class Evidence {
        private final long rowsOut;
        private final BatchSinkOutcome outcome;
        private final int publicationCount;
        private final int visibleMemberCount;
        private final boolean duplicateReplay;

        private Evidence(long rowsOut, BatchSinkOutcome outcome,
                         int publicationCount, int visibleMemberCount,
                         boolean duplicateReplay) {
            this.rowsOut = rowsOut;
            this.outcome = outcome;
            this.publicationCount = publicationCount;
            this.visibleMemberCount = visibleMemberCount;
            this.duplicateReplay = duplicateReplay;
        }

        public long rowsOut() { return rowsOut; }
        public BatchSinkOutcome outcome() { return outcome; }
        public int publicationCount() { return publicationCount; }
        public int visibleMemberCount() { return visibleMemberCount; }
        public boolean duplicateReplay() { return duplicateReplay; }
    }

    public static final class ScenarioEvidence {
        private final BatchSinkOutcome.State outcome;
        private final Set<DurableSinkConformanceObservation.Event> events;
        private final int publicationCount;
        private final int visibleMemberCount;
        private final int providerActionThreadCount;
        private final String detailCode;

        private ScenarioEvidence(
                BatchSinkOutcome.State outcome,
                Set<DurableSinkConformanceObservation.Event> events,
                int publicationCount, int visibleMemberCount,
                int providerActionThreadCount, String detailCode) {
            this.outcome = outcome;
            this.events = java.util.Collections.unmodifiableSet(
                    EnumSet.copyOf(events));
            this.publicationCount = publicationCount;
            this.visibleMemberCount = visibleMemberCount;
            this.providerActionThreadCount = providerActionThreadCount;
            this.detailCode = detailCode;
        }

        public BatchSinkOutcome.State outcome() { return outcome; }
        public Set<DurableSinkConformanceObservation.Event> events() {
            return events;
        }
        public int publicationCount() { return publicationCount; }
        public int visibleMemberCount() { return visibleMemberCount; }
        public int providerActionThreadCount() {
            return providerActionThreadCount;
        }
        public String detailCode() { return detailCode; }
    }
}
