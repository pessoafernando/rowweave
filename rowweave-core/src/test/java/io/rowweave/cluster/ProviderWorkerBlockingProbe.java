package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.CollectionSource;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.SignedAdversarialProviderFixture;
import io.rowweave.local.LocalJobClient;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSnapshot;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Forked-JVM proof that an interrupt-ignoring provider action actually crosses
 * Worker execution, poisons that Worker's runtime, and fences the Worker.
 */
public final class ProviderWorkerBlockingProbe {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    private ProviderWorkerBlockingProbe() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "provider action, backend, and local cycle count are required");
        }
        String action = args[0];
        boolean manifestBackend = "manifest".equals(args[1]);
        int localCycles = Integer.parseInt(args[2]);
        if (localCycles < 1 || localCycles > 3) {
            throw new IllegalArgumentException(
                    "local cycle count must be in [1,3]");
        }
        Path workspace = Files.createTempDirectory(
                "rowweave-provider-worker-block-" + action + '-');
        Path deployment = workspace.resolve("deployment");
        Path spill = workspace.resolve("spill");
        Path output = workspace.resolve("output");
        Files.createDirectories(deployment);
        Files.createDirectories(spill);
        Files.createDirectories(output);
        SignedAdversarialProviderFixture.Session session = manifestBackend
                ? null : SignedAdversarialProviderFixture.open(deployment);
        List<SignedAdversarialProviderFixture.Session> retrySessions =
                new ArrayList<>();
        if (!manifestBackend) {
            for (int cycle = 1; cycle < localCycles; cycle++) {
                Path retryDeployment = workspace.resolve(
                        "local-retry-deployment-" + cycle);
                Files.createDirectories(retryDeployment);
                retrySessions.add(SignedAdversarialProviderFixture.open(
                        retryDeployment));
            }
        }
        DeploymentRuntime runtime = manifestBackend
                ? null : session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        MasterServer createdMaster = manifestBackend
                ? new MasterServer(60_000L, 0, registry)
                : new MasterServer(60_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP,
                registry.artifactRepository(), runtime, authority);
        WorkerServer createdWorker = manifestBackend
                ? new WorkerServer("blocking-provider-worker", 4,
                new FixedMasterLocator(new Endpoint(
                        "127.0.0.1", createdMaster.port())), spill.toFile())
                : new WorkerServer("blocking-provider-worker", 4,
                new FixedMasterLocator(new Endpoint(
                        "127.0.0.1", createdMaster.port())), spill.toFile(),
                runtime);
        RealClusterJobClient createdClient = manifestBackend
                ? new RealClusterJobClient("127.0.0.1", createdMaster.port())
                : new RealClusterJobClient(new FixedMasterLocator(new Endpoint(
                "127.0.0.1", createdMaster.port())), runtime);
        try (MasterServer master = createdMaster;
             WorkerServer worker = createdWorker;
             RealClusterJobClient client = createdClient) {
            if (manifestBackend) worker.blockProviderActionForTesting(action);
            String runId = "blocking-provider-" + action;
            java.util.concurrent.CountDownLatch queryReady =
                    new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch queryRelease =
                    new java.util.concurrent.CountDownLatch(1);
            if (manifestBackend && "query".equals(action)) {
                master.setManifestPublishHooks(() -> {
                    queryReady.countDown();
                    try {
                        if (!queryRelease.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "query fault cut was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "query fault cut interrupted", interrupted);
                    }
                }, null);
            }
            if ("abort".equals(action)) {
                master.setDurableDecisionHooksForTesting(() -> {
                    throw new IllegalStateException(
                            "force provider abort before commit decision");
                }, null);
            }
            java.util.concurrent.atomic.AtomicReference<Throwable>
                    operationFailure = new java.util.concurrent.atomic.AtomicReference<>();
            CompletableFuture<Void> operation = CompletableFuture.runAsync(() -> {
                try {
                    client.submitRun(graph(output, "block-" + action,
                                    manifestBackend),
                            "blocking-provider-definition-" + action,
                            new RunSubmission(runId, "attempt-1",
                                    "publication-1", 1, 0L, 10L));
                    if ("query".equals(action) && !manifestBackend) {
                        client.queryBatchSinkOutcome(runId);
                    }
                } catch (RuntimeException expectedSafeFailure) {
                    // The assertions below distinguish a fenced Worker from an
                    // unrelated client-side failure.
                    operationFailure.set(expectedSafeFailure);
                }
            });
            if (manifestBackend && "query".equals(action)) {
                if (!queryReady.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError(
                            "Manifest QUERY cut was not reached");
                }
                try {
                    client.queryBatchSinkOutcome(runId);
                } catch (RuntimeException expectedSafeFailure) {
                    operationFailure.compareAndSet(null, expectedSafeFailure);
                } finally {
                    queryRelease.countDown();
                }
            }
            operation.get(8, TimeUnit.SECONDS);
            long poisonDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (!worker.providerRuntimePoisoned()
                    && System.nanoTime() < poisonDeadline) {
                Thread.sleep(10L);
            }
            if (!worker.providerRuntimePoisoned()) {
                throw new AssertionError(
                        "blocking provider action did not poison Worker runtime"
                                + " invocations="
                                + worker.providerActionInvocationCountForTesting()
                                + " failure=" + operationFailure.get());
            }
            if (!worker.awaitClosedForTesting(5, TimeUnit.SECONDS)) {
                throw new AssertionError(
                        "poisoned provider Worker did not close at its fence");
            }
            int bypassRejected = 0;
            for (int cycle = 1; cycle < localCycles; cycle++) {
                proveNewLocalRuntimeCannotBypassPoison(
                        manifestBackend, workspace, cycle,
                        manifestBackend ? null : retrySessions.get(cycle - 1));
                bypassRejected++;
            }
            BatchSinkOutcome outcome = client.queryBatchSinkOutcome(runId);
            if (outcome == null) {
                throw new AssertionError("public outcome query returned null");
            }
            RunSnapshot run = client.queryRun(runId);
            long locatorFiles;
            Path data = output.resolve("data");
            if (Files.exists(data)) {
                try (java.util.stream.Stream<Path> files = Files.walk(data)) {
                    locatorFiles = files.filter(Files::isRegularFile).count();
                }
            } else {
                locatorFiles = 0L;
            }
            long publications;
            if (manifestBackend) {
                publications = new io.rowweave.commit.ManifestFileStore(output)
                        .latest() == null ? 0L : 1L;
            } else {
                Path publicationRoot = output.resolve("publications");
                if (Files.exists(publicationRoot)) {
                    try (java.util.stream.Stream<Path> files =
                                 Files.walk(publicationRoot)) {
                        publications = files.filter(Files::isRegularFile).count();
                    }
                } else {
                    publications = 0L;
                }
            }
            System.out.println("WORKER_ACTION=" + action);
            System.out.println("WORKER_POISONED=true");
            System.out.println("WORKER_CLOSED=true");
            System.out.println("OUTCOME=" + outcome.state());
            System.out.println("RUN=" + (run == null ? "NOT_FOUND"
                    : run.status()));
            System.out.println("LOCATOR_FILES=" + locatorFiles);
            System.out.println("PUBLICATIONS=" + publications);
            System.out.println("PROVIDER_THREADS="
                    + providerActionThreadCount());
            System.out.println("LOCAL_BYPASS_REJECTED=" + bypassRejected);
        } finally {
            if (runtime != null) runtime.close();
            if (session != null) session.close();
            for (SignedAdversarialProviderFixture.Session retry
                    : retrySessions) {
                retry.close();
            }
            deleteTree(workspace);
        }
    }

    private static void proveNewLocalRuntimeCannotBypassPoison(
            boolean manifestBackend, Path workspace, int cycle,
            SignedAdversarialProviderFixture.Session signedSession)
            throws Exception {
        Path output = workspace.resolve("local-retry-output-" + cycle);
        Files.createDirectories(output);
        LocalJobClient localClient = new LocalJobClient();
        ExecutionEnvironment job = new ExecutionEnvironment(
                "provider-poison-retry-" + cycle,
                "provider-poison-retry-" + cycle, localClient);
        DataRow row = new DataRow(1);
        row.set(0, cycle);
        Sink sink = manifestBackend
                ? new io.rowweave.ManifestFileSink(
                META, output.toString(), "member")
                : SignedAdversarialProviderFixture.output(
                META, output, "member");
        job.fromSource(new CollectionSource(
                META, "local-retry-" + cycle,
                java.util.Collections.singletonList(row))).sinkTo(sink);

        DeploymentRuntime freshRuntime = manifestBackend
                ? DeploymentRuntime.fromSystemPropertiesForWorker()
                : signedSession.workerRuntime();
        long started = System.nanoTime();
        boolean poisoned = false;
        try {
            job.execute();
        } catch (RuntimeException rejected) {
            poisoned = containsMessage(
                    rejected, "PROVIDER_ACTION_SUPERVISOR_POISONED");
        } finally {
            freshRuntime.close();
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - started);
        if (!poisoned || elapsedMillis > 2_000L
                || publicationCount(output, manifestBackend) != 0L) {
            throw new AssertionError(
                    "new local client/session/runtime bypassed process poison"
                            + " cycle=" + cycle
                            + " poisoned=" + poisoned
                            + " elapsedMs=" + elapsedMillis);
        }
    }

    private static boolean containsMessage(Throwable failure, String marker) {
        for (Throwable current = failure; current != null;
             current = current.getCause()) {
            if (current.getMessage() != null
                    && current.getMessage().contains(marker)) {
                return true;
            }
        }
        return false;
    }

    private static long publicationCount(
            Path output, boolean manifestBackend) throws Exception {
        if (manifestBackend) {
            return new io.rowweave.commit.ManifestFileStore(output)
                    .latest() == null ? 0L : 1L;
        }
        Path publications = output.resolve("publications");
        if (!Files.exists(publications)) return 0L;
        try (java.util.stream.Stream<Path> files = Files.walk(publications)) {
            return files.filter(Files::isRegularFile).count();
        }
    }

    private static int providerActionThreadCount() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith("rowweave-provider-action-")) {
                count++;
            }
        }
        return count;
    }

    private static PlanGraph graph(
            Path root, String member, boolean manifestBackend) {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(DurableTestSources.source(
                        META, Arrays.asList(row), "blocking-source")
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"));
        Sink output = manifestBackend
                ? new io.rowweave.ManifestFileSink(
                META, root.toString(), member)
                : SignedAdversarialProviderFixture.output(META, root, member);
        nodes.add(PlanNode.sink(META, 0, output).build()
                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"));
        return new PlanGraph(nodes, 1);
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }
}
