package io.rowweave.soak;

import io.rowweave.RowWeave;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.connector.postgres.PostgresConnectorConfiguration;
import io.rowweave.connector.postgres.PostgresConnectorContract;
import io.rowweave.connector.s3.S3ConnectorContract;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.server.DatadirCleanupManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Explicit stream release-soak executable. Default duration is a real 24h. */
public final class StreamSoakMain {
    private static final int ZOOKEEPER_SNAPSHOT_RETAIN_COUNT = 3;
    private static final int ZOOKEEPER_PURGE_INTERVAL_HOURS = 1;

    private StreamSoakMain() { }

    public static void main(String[] arguments) throws Exception {
        StreamSoakConfig config = StreamSoakConfig.parse(arguments);
        prepareOutput(config.output);
        Path failure = config.output.resolve("failure.txt");
        try {
            configureAcceleratedZooKeeperSnapshotting(config.profile);
            StreamSoakPlan plan = config.profile == StreamSoakConfig.Profile.LEGACY
                    ? null : StreamSoakPlan.freeze(config, config.output);
            StreamSoakManifest manifest = new Runner(config, plan).run();
            Path target = config.output.resolve("manifest.properties");
            manifest.write(target);
            System.out.println("STREAM_SOAK_SOAK_MANIFEST=" + target);
            System.out.println("STREAM_SOAK_SOAK_QUALIFICATION="
                    + manifest.fields().get("qualification"));
            if ("FAILED".equals(manifest.fields().get("qualification"))) {
                throw new IllegalStateException("STREAM_SOAK_SOAK_FAILED");
            }
        } catch (Throwable failed) {
            Files.write(failure, stack(failed).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            if (failed instanceof Exception) throw (Exception) failed;
            if (failed instanceof Error) throw (Error) failed;
            throw new IllegalStateException(failed);
        }
    }

    static void configureAcceleratedZooKeeperSnapshotting(
            StreamSoakConfig.Profile profile) {
        if (profile == StreamSoakConfig.Profile.ACCELERATED_4H
                || profile == StreamSoakConfig.Profile.ACCELERATED_FEASIBILITY) {
            System.setProperty("zookeeper.snapCount", Integer.toString(
                    StreamSoakConfig.ACCELERATED_ZOOKEEPER_SNAP_COUNT));
            System.setProperty("zookeeper.preAllocSize", Integer.toString(
                    StreamSoakConfig.ACCELERATED_ZOOKEEPER_PREALLOC_KIB));
            org.apache.zookeeper.server.persistence.FileTxnLog.setPreallocSize(
                    StreamSoakConfig.ACCELERATED_ZOOKEEPER_PREALLOC_KIB * 1024L);
        }
    }

    private static void prepareOutput(Path output) throws Exception {
        if (Files.exists(output)) {
            try (java.util.stream.Stream<Path> entries = Files.list(output)) {
                if (entries.findAny().isPresent()) {
                    throw new IllegalArgumentException(
                            "STREAM SOAK output directory must be empty: " + output);
                }
            }
        }
        Files.createDirectories(output);
    }

    private static String stack(Throwable failure) {
        java.io.StringWriter text = new java.io.StringWriter();
        failure.printStackTrace(new java.io.PrintWriter(text));
        return text.toString();
    }

    static ManagedZooKeeper startZooKeeper(Path output) throws Exception {
        return startZooKeeper(output, false);
    }

    private static ManagedZooKeeper startZooKeeper(Path output,
            boolean accelerated) throws Exception {
        Path data = Files.createDirectories(output.resolve("zookeeper"));
        InstanceSpec spec = new InstanceSpec(data.toFile(),
                -1, -1, -1, false, -1, -1, -1,
                zooKeeperProperties(accelerated));
        TestingServer server = new TestingServer(spec, true);
        if (accelerated) {
            return new ManagedZooKeeper(server, null,
                    new StreamSoakAcceleratedPurge(data));
        }
        DatadirCleanupManager cleanup = new DatadirCleanupManager(
                data.toFile(), data.toFile(),
                ZOOKEEPER_SNAPSHOT_RETAIN_COUNT,
                ZOOKEEPER_PURGE_INTERVAL_HOURS);
        cleanup.start();
        return new ManagedZooKeeper(server, cleanup, null);
    }

    static Map<String, Object> zooKeeperProperties() {
        return zooKeeperProperties(false);
    }

    private static Map<String, Object> zooKeeperProperties(
            boolean accelerated) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("autopurge.snapRetainCount",
                Integer.toString(ZOOKEEPER_SNAPSHOT_RETAIN_COUNT));
        properties.put("autopurge.purgeInterval",
                Integer.toString(accelerated ? 0 : ZOOKEEPER_PURGE_INTERVAL_HOURS));
        return Collections.unmodifiableMap(properties);
    }

    static void triggerCheckpoint(StreamJobHandle job,
            StreamCommandId command, long timeout) throws Exception {
        StreamControlException uncertain = null;
        for (int retry = 0; retry < 5; retry++) {
            try {
                StreamOperationStatus status = job.triggerCheckpoint(command)
                        .awaitCompletion(timeout);
                if (status == StreamOperationStatus.SUCCEEDED) return;
                throw new IllegalStateException(
                        "STREAM SOAK checkpoint failed: " + status);
            } catch (StreamControlException failed) {
                if (failed.code()
                                != StreamControlException.Code.TRANSPORT_FAILURE
                        && failed.code()
                                != StreamControlException.Code.WAIT_TIMEOUT) {
                    throw failed;
                }
                uncertain = failed;
            }
            if (retry < 4) Thread.sleep(1_000L);
        }
        throw new IllegalStateException("STREAM SOAK checkpoint failed", uncertain);
    }

    static StreamJobHandle submit(StreamJobClient client,
            StreamSubmissionId submissionId, PlanArtifact artifact,
            long maximumRecoveryMillis) throws Exception {
        if (client == null || submissionId == null || artifact == null
                || maximumRecoveryMillis <= 0L) {
            throw new IllegalArgumentException(
                    "STREAM SOAK submission and recovery budget are required");
        }
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(maximumRecoveryMillis);
        StreamControlException uncertain = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                return client.submit(submissionId, artifact);
            } catch (StreamControlException failed) {
                if (failed.code()
                                != StreamControlException.Code.TRANSPORT_FAILURE
                        && failed.code()
                                != StreamControlException.Code.WAIT_TIMEOUT) {
                    throw failed;
                }
                uncertain = failed;
            }
            long remaining = TimeUnit.NANOSECONDS.toMillis(
                    deadline - System.nanoTime());
            if (remaining <= 0L || attempt == 4) break;
            Thread.sleep(Math.min(250L, remaining));
        }
        throw new IllegalStateException(
                "STREAM SOAK submission did not resolve within recovery budget",
                uncertain);
    }

    static void awaitS3Finished(StreamJobHandle job, long timeoutMillis) {
        final StreamJobStatus result;
        try {
            result = job.awaitTermination(timeoutMillis);
        } catch (StreamControlException timeout) {
            if (timeout.code() != StreamControlException.Code.WAIT_TIMEOUT) {
                throw timeout;
            }
            throw new IllegalStateException(
                    "S3 bounded job did not finish; " + s3Diagnostics(job),
                    timeout);
        }
        if (result != StreamJobStatus.FINISHED) {
            throw new IllegalStateException(
                    "S3 bounded job did not finish; result=" + result
                            + "; " + s3Diagnostics(job));
        }
    }

    private static String s3Diagnostics(StreamJobHandle job) {
        StringBuilder detail = new StringBuilder();
        try {
            detail.append("status=").append(job.status());
        } catch (RuntimeException unavailable) {
            detail.append("status-unavailable=")
                    .append(unavailable.getClass().getSimpleName());
        }
        try {
            StreamJobEvidence evidence = job.evidence();
            detail.append(", complete=").append(evidence.complete())
                    .append(", run=").append(evidence.runId())
                    .append(", attempt=").append(evidence.attemptOrdinal())
                    .append(", term=").append(evidence.coordinatorTerm())
                    .append(", generation=")
                    .append(evidence.assignmentGeneration())
                    .append(", reported=")
                    .append(evidence.reportedTaskCount()).append('/')
                    .append(evidence.placementTaskCount())
                    .append(", checkpoint=")
                    .append(evidence.latestCompletedCheckpointSequence());
        } catch (RuntimeException unavailable) {
            detail.append(", evidence-unavailable=")
                    .append(unavailable.getClass().getSimpleName());
        }
        return detail.toString();
    }

    @FunctionalInterface
    interface DrainSample {
        void collect() throws Exception;
    }

    static StreamSoakOracle.Comparison awaitOracle(Path expected, Path output,
            long timeoutMillis, long sampleIntervalMillis,
            DrainSample sampling) throws Exception {
        if (sampling != null && sampleIntervalMillis <= 0L) {
            throw new IllegalArgumentException(
                    "terminal drain sample interval must be positive");
        }
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long nextSample = sampling == null ? Long.MAX_VALUE
                : System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(sampleIntervalMillis);
        StreamSoakOracle.Comparison last = null;
        while (System.nanoTime() < deadline) {
            long now = System.nanoTime();
            if (sampling != null && now - nextSample >= 0L) {
                sampling.collect();
                nextSample = System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(
                        sampleIntervalMillis);
            }
            last = StreamSoakOracle.compare(expected, output);
            if (last.status == StreamSoakOracle.Status.MATCH) return last;
            if (last.status == StreamSoakOracle.Status.DIVERGED) {
                throw new IllegalStateException(
                        "STREAM SOAK visible PostgreSQL output diverged from "
                                + "oracle at expected="
                                + last.expectedRows + ", observed="
                                + last.actualRows);
            }
            long untilSample = sampling == null ? 1_000L
                    : Math.max(1L, TimeUnit.NANOSECONDS.toMillis(
                    nextSample - System.nanoTime()));
            Thread.sleep(Math.min(1_000L, untilSample));
        }
        throw new IllegalStateException(
                "STREAM SOAK visible PostgreSQL output did not reach oracle: "
                        + "expected=" + (last == null ? 0L
                        : last.expectedRows)
                        + ", observed=" + (last == null ? 0L
                        : last.actualRows));
    }

    static final class ManagedZooKeeper implements AutoCloseable {
        private final TestingServer server;
        private final DatadirCleanupManager cleanup;
        private final StreamSoakAcceleratedPurge acceleratedPurge;

        ManagedZooKeeper(TestingServer server,
                DatadirCleanupManager cleanup,
                StreamSoakAcceleratedPurge acceleratedPurge) {
            this.server = server;
            this.cleanup = cleanup;
            this.acceleratedPurge = acceleratedPurge;
        }

        String getConnectString() { return server.getConnectString(); }

        File tempDirectory() { return server.getTempDirectory(); }

        DatadirCleanupManager.PurgeTaskStatus cleanupStatus() {
            if (cleanup == null) throw new IllegalStateException(
                    "hourly cleanup is not active in accelerated profile");
            return cleanup.getPurgeTaskStatus();
        }

        int snapRetainCount() { return ZOOKEEPER_SNAPSHOT_RETAIN_COUNT; }

        int purgeIntervalHours() {
            return cleanup == null ? 0 : cleanup.getPurgeInterval();
        }

        void startAcceleratedPurges() {
            if (acceleratedPurge != null) {
                acceleratedPurge.start(
                        StreamSoakConfig.ACCELERATED_PURGE_FIRST_OFFSET_MILLIS,
                        StreamSoakConfig.ACCELERATED_PURGE_INTERVAL_MILLIS);
            }
        }

        void requireHealthy() {
            if (acceleratedPurge != null)
                acceleratedPurge.requireHealthy();
        }

        long acceleratedPurgeCount() {
            return acceleratedPurge == null ? 0L
                    : acceleratedPurge.completed();
        }

        @Override public void close() throws IOException {
            try {
                if (cleanup != null) cleanup.shutdown();
                if (acceleratedPurge != null) acceleratedPurge.close();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("accelerated purge shutdown interrupted",
                        interrupted);
            } finally {
                server.close();
            }
        }
    }

    private static final class Runner {
        private static final class DrainCursor {
            private StreamJobEvidence prior;
            private String previousLsn;

            private DrainCursor(StreamJobEvidence prior, String previousLsn) {
                this.prior = prior;
                this.previousLsn = previousLsn;
            }
        }

        private final StreamSoakConfig config;
        private final StreamSoakPlan plan;
        private final Statistics statistics = new Statistics();
        private int checkpointCommand;
        private int s3Job;

        Runner(StreamSoakConfig config, StreamSoakPlan plan) {
            this.config = config;
            this.plan = plan;
        }

        StreamSoakManifest run() throws Exception {
            Path samples = config.output.resolve("samples.log");
            try (ManagedZooKeeper zk = startZooKeeper(config.output,
                    config.profile == StreamSoakConfig.Profile.ACCELERATED_4H
                    || config.profile
                    == StreamSoakConfig.Profile.ACCELERATED_FEASIBILITY);
                 StreamSoakMinio minio = StreamSoakMinio.start(config.minioBinary,
                         config.output.resolve("minio"));
                 StreamSoakPostgres postgres = StreamSoakPostgres.start(config.postgresBin,
                         config.output.resolve("postgres"));
                 StreamSoakDeployments deployments = StreamSoakDeployments.create(
                         config.output.resolve("deployments"),
                         config.s3Deployment,
                         S3ConnectorContract.DEPLOYMENT_ID,
                         config.postgresDeployment,
                         PostgresConnectorContract.DEPLOYMENT_ID);
                 StreamSoakCluster cluster = StreamSoakCluster.start(
                         config.output.resolve("cluster"),
                         zk.getConnectString(), deployments,
                         minio.credentialFrame(), postgres.credentialFrame());
                 StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
                statistics.masterProcesses = 2L;
                statistics.workerProcesses = 3L;
                StreamJobHandle job = submitPostgres(cluster, deployments,
                        postgres.configuration());
                require(job.awaitRunning(60_000L) == StreamJobStatus.RUNNING,
                        "PostgreSQL job did not enter RUNNING");
                StreamJobEvidence initial = awaitEvidence(job, null,
                        0, 0L, 60_000L);
                require(initial.placementTaskCount() >= 3,
                        "resident graph did not place three task roles");
                statistics.runId = initial.runId();
                Path expectedOracle = config.output.resolve(
                        "postgres-expected.log");
                Files.write(expectedOracle,
                        Collections.singletonList("A|1|10.00"),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
                triggerCheckpoint(job, 60_000L);
                awaitOracle(expectedOracle,
                        config.output.resolve("postgres-visible"), 60_000L);
                String previousLsn = awaitConfirmedFlush(postgres, 30_000L);
                // Qualification time starts only after the complete external
                // environment and the resident job have crossed a durable
                // checkpoint. Bootstrap time is not continuous soak time.
                long wallStarted = System.currentTimeMillis();
                long monotonicStarted = System.nanoTime();
                zk.startAcceleratedPurges();
                try (MutationProducer producer = new MutationProducer(
                        postgres, expectedOracle, config.mutationBurst,
                        config.mutationPauseMillis)) {
                    producer.start();
                    long deadline = monotonicStarted
                            + TimeUnit.MILLISECONDS.toNanos(
                            config.durationMillis);
                    long nextSample = System.nanoTime();
                    long nextS3 = System.nanoTime();
                    long[] faults = faultSchedule(monotonicStarted,
                            config.durationMillis);
                    int fault = 0;
                    StreamJobEvidence prior = initial;
                    while (System.nanoTime() < deadline) {
                        producer.requireHealthy();
                        zk.requireHealthy();
                        long now = System.nanoTime();
                        if (fault < faults.length && now >= faults[fault]) {
                            int expectedAttemptAdvance =
                                    expectedAttemptAdvance(fault);
                            StreamJobEvidence after = injectFault(
                                    cluster, job, prior, fault);
                            previousLsn = sample(log, after, prior,
                                    expectedAttemptAdvance, cluster, minio,
                                    postgres, previousLsn, monotonicStarted,
                                    wallStarted);
                            prior = after;
                            fault++;
                            nextSample = advance(nextSample,
                                    config.sampleIntervalMillis,
                                    System.nanoTime());
                        }
                        if (now >= nextS3) {
                            runS3(cluster, deployments, minio);
                            nextS3 = advance(nextS3,
                                    config.s3IntervalMillis,
                                    System.nanoTime());
                        }
                        if (now >= nextSample) {
                            StreamJobEvidence current = awaitEvidence(job,
                                    statistics.runId, prior.attemptOrdinal(),
                                    prior.coordinatorTerm(), 30_000L);
                            previousLsn = sample(log, current, prior, 0,
                                    cluster, minio, postgres, previousLsn,
                                    monotonicStarted, wallStarted);
                            prior = current;
                            nextSample = advance(nextSample,
                                    config.sampleIntervalMillis,
                                    System.nanoTime());
                        }
                        Thread.sleep(25L);
                    }
                    while (fault < faults.length) {
                        int expectedAttemptAdvance =
                                expectedAttemptAdvance(fault);
                        StreamJobEvidence after = injectFault(
                                cluster, job, prior, fault++);
                        previousLsn = sample(log, after, prior,
                                expectedAttemptAdvance, cluster, minio,
                                postgres, previousLsn, monotonicStarted,
                                wallStarted);
                        prior = after;
                    }
                    producer.close();
                    producer.requireHealthy();
                    triggerCheckpoint(job, 60_000L);
                    DrainCursor drain = new DrainCursor(prior, previousLsn);
                    StreamSoakOracle.Comparison comparison =
                            StreamSoakMain.awaitOracle(expectedOracle,
                            config.output.resolve("postgres-visible"),
                            Math.max(120_000L, Math.multiplyExact(
                                    config.maximumRecoveryMillis, 4L)),
                            config.sampleIntervalMillis, () -> {
                                StreamJobEvidence current = awaitEvidence(job,
                                        statistics.runId,
                                        drain.prior.attemptOrdinal(),
                                        drain.prior.coordinatorTerm(),
                                        30_000L);
                                drain.previousLsn = sample(log, current,
                                        drain.prior, 0, cluster, minio,
                                        postgres, drain.previousLsn,
                                        monotonicStarted, wallStarted);
                                drain.prior = current;
                            });
                    prior = drain.prior;
                    previousLsn = drain.previousLsn;
                    String finalLsn = awaitConfirmedFlush(postgres, 30_000L);
                    statistics.postgresAckMonotonic &= postgres.lsnAtLeast(
                            previousLsn, finalLsn);
                    StreamJobEvidence terminalSample = awaitEvidence(job,
                            statistics.runId, prior.attemptOrdinal(),
                            prior.coordinatorTerm(), 30_000L);
                    sample(log, terminalSample, prior, 0, cluster, minio,
                            postgres, finalLsn, monotonicStarted, wallStarted);
                    require(job.cancel(StreamCommandId.of(
                            "stream-soak-cancel"))
                            .awaitCompletion(30_000L)
                            == StreamOperationStatus.SUCCEEDED,
                            "PostgreSQL job cancellation failed");
                    require(job.awaitTermination(30_000L)
                                    == StreamJobStatus.CANCELED,
                            "PostgreSQL job did not cancel");
                    statistics.postgresExpectedRows =
                            comparison.expectedRows;
                    statistics.postgresActualRows = comparison.actualRows;
                    statistics.postgresExpectedDigest =
                            comparison.expectedDigest;
                    statistics.postgresActualDigest = comparison.actualDigest;
                    statistics.postgresOracleMatch = comparison.status
                            == StreamSoakOracle.Status.MATCH;
                }
                statistics.durationMillis = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - monotonicStarted);
                statistics.secretResolutionComplete =
                        secretResolutionComplete(cluster.roleProcesses());
                zk.requireHealthy();
                statistics.purgeCompleted = zk.acceleratedPurgeCount();
            }
            Map<String, String> summary = statistics.summary(config);
            StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                    samples, summary, plan);
            manifest.write(config.output.resolve("manifest.properties"));
            return manifest;
        }

        private StreamJobHandle submitPostgres(StreamSoakCluster cluster,
                StreamSoakDeployments deployments,
                PostgresConnectorConfiguration source) {
            StreamSession session = RowWeave.createStreamSession(
                    "stream-soak-postgres-soak");
            configure(session, deployments, StreamChangelogMode.UPSERT, 1);
            session.fromSource(source.toSourceSpec()).uid("postgres-source")
                    .calc(postgresIdentity(), source.streamSchema())
                    .uid("postgres-calc")
                    .sinkTo("postgres-sink", sink(
                            config.output.resolve("postgres-visible"),
                            "postgres", config.sinkDelayMillis));
            return cluster.client().submit(
                    StreamSubmissionId.of("stream-soak-postgres-soak"),
                    session.toPlanArtifact());
        }

        private void runS3(StreamSoakCluster cluster,
                StreamSoakDeployments deployments, StreamSoakMinio minio)
                throws Exception {
            StreamSoakMinio.S3Workload workload = minio.putBatch(config.s3Rows);
            int ordinal = ++s3Job;
            Path output = config.output.resolve(String.format(
                    "s3-visible-%06d", ordinal));
            StreamSession session = RowWeave.createStreamSession(
                    "stream-soak-s3-soak-" + ordinal);
            configure(session, deployments, StreamChangelogMode.APPEND, 1);
            session.fromSource(workload.manifest.toSourceSpec(
                            workload.configuration)).uid("s3-source")
                    .calc(s3Identity(), StreamSoakMinio.SCHEMA).uid("s3-calc")
                    .sinkTo("s3-sink", sink(output, "s3", 0L));
            StreamJobHandle handle = submit(cluster.client(),
                    StreamSubmissionId.of("stream-soak-s3-soak-" + ordinal),
                    session.toPlanArtifact(), config.maximumRecoveryMillis);
            awaitS3Finished(handle, 90_000L);
            StreamJobEvidence evidence = handle.evidence();
            require(evidence.complete(), "S3 evidence is incomplete");
            List<String> expected = new ArrayList<>();
            for (Long id : workload.ids) expected.add("S|" + id);
            List<String> actual = StreamSoakOracle.visibleRows(output);
            statistics.s3Expected.addAll(expected);
            statistics.s3Actual.addAll(actual);
            statistics.s3Jobs++;
            if (!expected.equals(actual)) statistics.unexplainedFailures++;
        }

        private void configure(StreamSession session,
                StreamSoakDeployments deployments, StreamChangelogMode mode,
                int parallelism) {
            session.setDeploymentSetId(deployments.deploymentSetId);
            session.setParallelism(parallelism);
            session.setChangelogMode(mode);
            session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    config.checkpointIntervalMillis,
                    Math.max(30_000L, config.maximumCheckpointMillis), 4));
            session.setRestartPolicy(StreamRestartPolicy.fixedDelay(
                    100, 1_000L));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 8_192L, 0, 0,
                    32_768L, 8, 32, 8_192L));
        }

        private StreamSinkSpec sink(Path output, String mode, long delay) {
            Map<String, String> options = new LinkedHashMap<>();
            options.put("output.path", output.toString());
            options.put("mode", mode);
            options.put("delay.millis", Long.toString(delay));
            return StreamSinkSpec.checkpointTransactional(
                    StreamSoakDeployments.SINK_PROVIDER,
                    StreamConnectorProviderContractCodec.digestHex(
                            StreamSoakDeployments.SINK_CONTRACT),
                    StreamConnectorConfiguration.of(options,
                            Collections.singletonMap(
                                    "marker", "vault/soak-sink")).encode());
        }

        private StreamJobEvidence injectFault(StreamSoakCluster cluster,
                StreamJobHandle job, StreamJobEvidence prior, int fault)
                throws Exception {
            long started = System.nanoTime();
            if (fault == 0 || fault == 3) {
                cluster.replaceLeader();
                statistics.leaderReplacements++;
                StreamJobEvidence after = awaitEvidence(job,
                        statistics.runId, prior.attemptOrdinal(),
                        prior.coordinatorTerm() + 1L,
                        config.maximumRecoveryMillis);
                statistics.maximumRecoveryMillis = Math.max(
                        statistics.maximumRecoveryMillis,
                        TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - started));
                return after;
            }
            String role = fault == 1 ? "worker-a" : "worker-b";
            cluster.replaceWorker(role);
            statistics.workerReplacements++;
            StreamJobEvidence after = awaitEvidence(job,
                    statistics.runId, prior.attemptOrdinal() + 1,
                    prior.coordinatorTerm(),
                    config.maximumRecoveryMillis);
            statistics.maximumRecoveryMillis = Math.max(
                    statistics.maximumRecoveryMillis,
                    TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - started));
            return after;
        }

        private String sample(StreamSoakEvidenceLog log,
                StreamJobEvidence current, StreamJobEvidence prior,
                int expectedAttemptAdvance, StreamSoakCluster cluster,
                StreamSoakMinio minio,
                StreamSoakPostgres postgres, String previousLsn,
                long monotonicStarted, long wallStarted) throws Exception {
            statistics.evidenceComplete &= current.complete();
            statistics.sameRunPreserved &= statistics.runId.equals(
                    current.runId());
            statistics.attemptsMonotonic &= current.attemptOrdinal()
                    >= prior.attemptOrdinal();
            int attemptAdvance = current.attemptOrdinal()
                    - prior.attemptOrdinal();
            if (attemptAdvance > expectedAttemptAdvance) {
                statistics.unexplainedFailures += attemptAdvance
                        - expectedAttemptAdvance;
            }
            statistics.termsMonotonic &= current.coordinatorTerm()
                    >= prior.coordinatorTerm();
            statistics.checkpointsMonotonic &=
                    current.latestCompletedCheckpointSequence()
                    >= prior.latestCompletedCheckpointSequence();
            StreamJobProgress progress = current.lifetimeProgress();
            statistics.maximumBacklogEvents = Math.max(
                    statistics.maximumBacklogEvents,
                    progress.backlogEvents());
            statistics.maximumBacklogBytes = Math.max(
                    statistics.maximumBacklogBytes, progress.backlogBytes());
            statistics.maximumBackpressuredMillis = Math.max(
                    statistics.maximumBackpressuredMillis,
                    progress.backpressuredMillis());
            statistics.maximumStateBytes = Math.max(
                    statistics.maximumStateBytes,
                    progress.managedStateBytes());
            statistics.maximumCheckpointMillis = Math.max(
                    statistics.maximumCheckpointMillis,
                    current.latestCheckpointDurationMillis());
            statistics.completedCheckpoints = Math.max(
                    statistics.completedCheckpoints,
                    current.latestCompletedCheckpointSequence());
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("kind", "progress-resource");
            fields.put("observed_epoch_millis",
                    Long.toString(System.currentTimeMillis()));
            fields.put("wall_started_epoch_millis",
                    Long.toString(wallStarted));
            fields.put("elapsed_millis", Long.toString(
                    TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - monotonicStarted)));
            fields.put("job_id", current.jobId().value());
            fields.put("run_id", current.runId());
            fields.put("attempt", Integer.toString(current.attemptOrdinal()));
            fields.put("coordinator_term",
                    Long.toString(current.coordinatorTerm()));
            fields.put("assignment_generation",
                    Long.toString(current.assignmentGeneration()));
            fields.put("checkpoint_sequence", Long.toString(
                    current.latestCompletedCheckpointSequence()));
            fields.put("checkpoint_duration_millis", Long.toString(
                    current.latestCheckpointDurationMillis()));
            fields.put("checkpoint_bytes",
                    Long.toString(current.latestCheckpointBytes()));
            fields.put("records_read", Long.toString(progress.recordsRead()));
            fields.put("records_written",
                    Long.toString(progress.recordsWritten()));
            fields.put("bytes_read", Long.toString(progress.bytesRead()));
            fields.put("bytes_written", Long.toString(progress.bytesWritten()));
            fields.put("backlog_events",
                    Long.toString(progress.backlogEvents()));
            fields.put("backlog_bytes", Long.toString(progress.backlogBytes()));
            fields.put("watermark", Long.toString(progress.operatorWatermark()));
            fields.put("state_bytes",
                    Long.toString(progress.managedStateBytes()));
            fields.put("mailbox_high_water", Integer.toString(
                    progress.mailboxHighWaterEvents()));
            fields.put("channel_high_water_bytes", Long.toString(
                    progress.channelHighWaterBytes()));
            fields.put("backpressured_millis", Long.toString(
                    progress.backpressuredMillis()));
            fields.put("placement_tasks",
                    Integer.toString(current.placementTaskCount()));
            fields.put("reported_tasks",
                    Integer.toString(current.reportedTaskCount()));
            sampleResources(fields, cluster, minio, postgres);
            String lsn = postgres.confirmedFlush();
            statistics.postgresAckMonotonic &= lsn != null
                    && postgres.lsnAtLeast(previousLsn, lsn);
            fields.put("postgres_ack_present",
                    Boolean.toString(lsn != null));
            log.append(fields);
            return lsn;
        }

        private static int expectedAttemptAdvance(int fault) {
            return fault == 1 || fault == 2 ? 1 : 0;
        }

        private void sampleResources(Map<String, String> fields,
                StreamSoakCluster cluster, StreamSoakMinio minio,
                StreamSoakPostgres postgres) throws Exception {
            for (StreamSoakCluster.RoleProcess role :
                    cluster.roleProcesses()) {
                recordUsage(fields, role.role,
                        StreamSoakResourceSampler.process(role.process));
            }
            recordUsage(fields, "minio",
                    StreamSoakResourceSampler.process(minio.process()));
            recordUsage(fields, "postgres",
                    StreamSoakResourceSampler.process(postgres.process()));
            long disk = StreamSoakResourceSampler.diskBytes(config.output);
            statistics.maximumDiskBytes = Math.max(
                    statistics.maximumDiskBytes, disk);
            fields.put("disk_bytes", Long.toString(disk));
            statistics.resourceSamples++;
        }

        private void recordUsage(Map<String, String> fields, String role,
                StreamSoakResourceSampler.ProcessUsage usage) {
            String name = role.replace('-', '.');
            fields.put(name + ".rss_bytes", Long.toString(usage.rssBytes));
            fields.put(name + ".fd", Long.toString(usage.fileDescriptors));
            statistics.maximumProcessRssBytes = Math.max(
                    statistics.maximumProcessRssBytes, usage.rssBytes);
            statistics.maximumFileDescriptors = Math.max(
                    statistics.maximumFileDescriptors,
                    usage.fileDescriptors);
        }

        private void triggerCheckpoint(StreamJobHandle job, long timeout)
                throws Exception {
            StreamSoakMain.triggerCheckpoint(job,
                    StreamCommandId.of("stream-soak-cut-"
                            + (++checkpointCommand)), timeout);
        }

        private StreamJobEvidence awaitEvidence(StreamJobHandle job,
                String runId, int minimumAttempt, long minimumTerm,
                long timeoutMillis) throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            Throwable last = null;
            StreamJobEvidence lastEvidence = null;
            while (System.nanoTime() < deadline) {
                try {
                    StreamJobStatus status = job.status();
                    if (status.terminal()) {
                        throw new IllegalStateException(
                                "resident job became terminal: " + status);
                    }
                    StreamJobEvidence evidence = job.evidence();
                    lastEvidence = evidence;
                    if (evidence.complete()
                            && (runId == null || runId.equals(evidence.runId()))
                            && evidence.attemptOrdinal() >= minimumAttempt
                            && evidence.coordinatorTerm() >= minimumTerm) {
                        return evidence;
                    }
                } catch (Throwable unavailable) {
                    last = unavailable;
                }
                Thread.sleep(100L);
            }
            throw new IllegalStateException(
                    "STREAM SOAK complete operational evidence timed out"
                            + evidenceSummary(lastEvidence), last);
        }

        private static String evidenceSummary(StreamJobEvidence evidence) {
            if (evidence == null) return "; no evidence observed";
            return "; last run=" + evidence.runId()
                    + ", attempt=" + evidence.attemptOrdinal()
                    + ", term=" + evidence.coordinatorTerm()
                    + ", generation=" + evidence.assignmentGeneration()
                    + ", complete=" + evidence.complete();
        }

        private StreamSoakOracle.Comparison awaitOracle(Path expected,
                Path output, long timeoutMillis) throws Exception {
            return StreamSoakMain.awaitOracle(expected, output,
                    timeoutMillis, 0L, null);
        }

        private static String awaitConfirmedFlush(StreamSoakPostgres postgres,
                long timeoutMillis) throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (System.nanoTime() < deadline) {
                String value = postgres.confirmedFlush();
                if (value != null) return value;
                Thread.sleep(100L);
            }
            throw new IllegalStateException(
                    "PostgreSQL confirmed flush LSN is unavailable");
        }

        private static long[] faultSchedule(long started,
                long durationMillis) {
            return new long[] {
                    started + TimeUnit.MILLISECONDS.toNanos(
                            durationMillis * 15L / 100L),
                    started + TimeUnit.MILLISECONDS.toNanos(
                            durationMillis * 35L / 100L),
                    started + TimeUnit.MILLISECONDS.toNanos(
                            durationMillis * 60L / 100L),
                    started + TimeUnit.MILLISECONDS.toNanos(
                            durationMillis * 80L / 100L)};
        }

        private static long advance(long prior, long intervalMillis,
                long now) {
            long interval = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
            long next = prior;
            do { next = Math.addExact(next, interval); }
            while (next <= now);
            return next;
        }

        private static boolean secretResolutionComplete(
                List<StreamSoakCluster.RoleProcess> roles) throws Exception {
            StringBuilder observed = new StringBuilder();
            for (StreamSoakCluster.RoleProcess role : roles) {
                if (!role.role.startsWith("worker-")) continue;
                Path marker = role.log.getParent().resolve(
                        role.role + "-secret.log");
                if (Files.isRegularFile(marker)) {
                    observed.append(new String(Files.readAllBytes(marker),
                            StandardCharsets.US_ASCII));
                }
            }
            String value = observed.toString();
            return value.contains("vault/s3")
                    && value.contains("vault/postgres")
                    && value.contains("vault/soak-sink");
        }

        private static StreamCalcSpec s3Identity() {
            return StreamCalcSpec.of(null, Collections.singletonList(
                    StreamCalcSpec.projection("id", StreamExpression.input(
                            "id", PortableDataType.LONG, false))));
        }

        private static StreamCalcSpec postgresIdentity() {
            return StreamCalcSpec.of(null, Arrays.asList(
                    StreamCalcSpec.projection("id", StreamExpression.input(
                            "id", PortableDataType.LONG, false)),
                    StreamCalcSpec.projection("amount",
                            StreamExpression.input("amount",
                                    PortableDataType.BIG_DECIMAL, true))));
        }

    }

    interface MutationSession extends AutoCloseable {
        List<String> mutate() throws Exception;
        @Override void close() throws Exception;
    }

    interface MutationSessionFactory {
        MutationSession open() throws Exception;
    }

    static final class MutationProducer implements AutoCloseable {
        private final MutationSessionFactory sessions;
        private final BufferedWriter expected;
        private final int burst;
        private final long pauseMillis;
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicReference<Throwable> failure =
                new AtomicReference<>();
        private Thread thread;

        MutationProducer(StreamSoakPostgres postgres, Path expected,
                int burst, long pauseMillis) throws Exception {
            this(postgres::openMutationSession, expected, burst,
                    pauseMillis);
        }

        MutationProducer(MutationSessionFactory sessions, Path expected,
                int burst, long pauseMillis) throws Exception {
            this.sessions = sessions;
            this.expected = Files.newBufferedWriter(expected,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE);
            this.burst = burst;
            this.pauseMillis = pauseMillis;
        }

        void start() {
            if (!running.compareAndSet(false, true)) {
                throw new IllegalStateException("producer already started");
            }
            thread = new Thread(this::produce, "stream-soak-postgres-mutator");
            thread.start();
        }

        private void produce() {
            try (MutationSession session = sessions.open()) {
                while (running.get()) {
                    for (int index = 0; index < burst && running.get();
                            index++) {
                        for (String row : session.mutate()) {
                            expected.write(row);
                            expected.newLine();
                        }
                    }
                    expected.flush();
                    if (running.get()) Thread.sleep(pauseMillis);
                }
            } catch (InterruptedException stopped) {
                if (running.get()) failure.compareAndSet(null, stopped);
                Thread.currentThread().interrupt();
            } catch (Throwable failed) {
                failure.compareAndSet(null, failed);
            } finally {
                running.set(false);
            }
        }

        void requireHealthy() {
            Throwable failed = failure.get();
            if (failed != null) {
                throw new IllegalStateException(
                        "PostgreSQL mutation producer failed", failed);
            }
        }

        @Override public void close() throws Exception {
            running.set(false);
            try {
                if (thread != null) {
                    thread.interrupt();
                    thread.join(30_000L);
                    if (thread.isAlive()) {
                        throw new IllegalStateException(
                                "PostgreSQL mutation producer did not stop");
                    }
                }
                requireHealthy();
            } finally {
                expected.close();
            }
        }
    }

    private static final class Statistics {
        String runId;
        long durationMillis;
        long masterProcesses;
        long workerProcesses;
        long leaderReplacements;
        long workerReplacements;
        long resourceSamples;
        long completedCheckpoints;
        long purgeCompleted;
        long maximumRecoveryMillis;
        long maximumProcessRssBytes;
        long maximumFileDescriptors;
        long maximumDiskBytes;
        long maximumCheckpointMillis;
        long maximumBacklogEvents;
        long maximumBacklogBytes;
        long maximumStateBytes;
        long maximumBackpressuredMillis;
        long unexplainedFailures;
        long s3Jobs;
        long postgresExpectedRows;
        long postgresActualRows;
        String postgresExpectedDigest = "";
        String postgresActualDigest = "";
        final List<String> s3Expected = new ArrayList<>();
        final List<String> s3Actual = new ArrayList<>();
        boolean evidenceComplete = true;
        boolean sameRunPreserved = true;
        boolean checkpointsMonotonic = true;
        boolean attemptsMonotonic = true;
        boolean termsMonotonic = true;
        boolean postgresAckMonotonic = true;
        boolean postgresOracleMatch;
        boolean secretResolutionComplete;

        Map<String, String> summary(StreamSoakConfig config) {
            long thresholdViolations = 0L;
            if (maximumProcessRssBytes > config.maximumProcessRssBytes)
                thresholdViolations++;
            if (maximumFileDescriptors > config.maximumFileDescriptors)
                thresholdViolations++;
            if (maximumDiskBytes > config.maximumDiskBytes)
                thresholdViolations++;
            if (maximumCheckpointMillis > config.maximumCheckpointMillis)
                thresholdViolations++;
            if (maximumRecoveryMillis > config.maximumRecoveryMillis)
                thresholdViolations++;
            if (maximumBacklogEvents > config.maximumBacklogEvents)
                thresholdViolations++;
            if (maximumStateBytes > config.maximumStateBytes)
                thresholdViolations++;
            boolean s3Match = s3Jobs > 0L && s3Expected.equals(s3Actual);
            Map<String, String> result = new LinkedHashMap<>();
            put(result, "duration_millis", durationMillis);
            result.put("evidence_complete",
                    Boolean.toString(evidenceComplete));
            result.put("resource_samples_complete",
                    Boolean.toString(resourceSamples > 0L));
            result.put("same_run_preserved",
                    Boolean.toString(sameRunPreserved));
            result.put("real_minio_used", "true");
            result.put("real_postgres_used", "true");
            result.put("signed_connectors_used", "true");
            result.put("secret_resolution_complete",
                    Boolean.toString(secretResolutionComplete));
            result.put("backlog_observed",
                    Boolean.toString(maximumBacklogEvents > 0L));
            result.put("backpressure_observed",
                    Boolean.toString(maximumBackpressuredMillis > 0L));
            result.put("checkpoints_monotonic",
                    Boolean.toString(checkpointsMonotonic));
            result.put("attempts_monotonic",
                    Boolean.toString(attemptsMonotonic));
            result.put("coordinator_terms_monotonic",
                    Boolean.toString(termsMonotonic));
            result.put("s3_oracle_match", Boolean.toString(s3Match));
            result.put("postgres_oracle_match",
                    Boolean.toString(postgresOracleMatch));
            result.put("postgres_ack_monotonic",
                    Boolean.toString(postgresAckMonotonic));
            put(result, "leader_replacements", leaderReplacements);
            put(result, "worker_replacements", workerReplacements);
            put(result, "master_processes", masterProcesses);
            put(result, "worker_processes", workerProcesses);
            put(result, "completed_checkpoints", completedCheckpoints);
            put(result, "purge_completed", purgeCompleted);
            put(result, "unexplained_failures", unexplainedFailures);
            put(result, "threshold_violations", thresholdViolations);
            put(result, "resource_samples", resourceSamples);
            put(result, "s3_jobs", s3Jobs);
            put(result, "s3_expected_rows", s3Expected.size());
            put(result, "s3_actual_rows", s3Actual.size());
            result.put("s3_expected_sha256",
                    StreamSoakOracle.digest(s3Expected));
            result.put("s3_actual_sha256",
                    StreamSoakOracle.digest(s3Actual));
            put(result, "postgres_expected_rows", postgresExpectedRows);
            put(result, "postgres_actual_rows", postgresActualRows);
            result.put("postgres_expected_sha256", postgresExpectedDigest);
            result.put("postgres_actual_sha256", postgresActualDigest);
            put(result, "max_process_rss_bytes", maximumProcessRssBytes);
            put(result, "threshold_process_rss_bytes",
                    config.maximumProcessRssBytes);
            put(result, "max_fd", maximumFileDescriptors);
            put(result, "threshold_fd", config.maximumFileDescriptors);
            put(result, "max_disk_bytes", maximumDiskBytes);
            put(result, "threshold_disk_bytes", config.maximumDiskBytes);
            put(result, "max_checkpoint_millis", maximumCheckpointMillis);
            put(result, "threshold_checkpoint_millis",
                    config.maximumCheckpointMillis);
            put(result, "max_recovery_millis", maximumRecoveryMillis);
            put(result, "threshold_recovery_millis",
                    config.maximumRecoveryMillis);
            put(result, "max_backlog_events", maximumBacklogEvents);
            put(result, "max_backlog_bytes", maximumBacklogBytes);
            put(result, "threshold_backlog_events",
                    config.maximumBacklogEvents);
            put(result, "max_state_bytes", maximumStateBytes);
            put(result, "threshold_state_bytes", config.maximumStateBytes);
            put(result, "max_backpressured_millis",
                    maximumBackpressuredMillis);
            put(result, "threshold_sample_gap_millis", Math.max(
                    120_000L, Math.max(
                            Math.multiplyExact(
                                    config.sampleIntervalMillis, 4L),
                            Math.multiplyExact(
                                    config.maximumRecoveryMillis, 2L))));
            return result;
        }

        private static void put(Map<String, String> values, String key,
                long value) {
            values.put(key, Long.toString(value));
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
