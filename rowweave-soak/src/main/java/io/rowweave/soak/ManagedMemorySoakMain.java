package io.rowweave.soak;

import io.rowweave.plan.ExecutionReport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Explicit real-process Batch qualification executable for managed memory partitions. */
public final class ManagedMemorySoakMain {
    private ManagedMemorySoakMain() { }

    public static void main(String[] arguments) throws Exception {
        ManagedMemorySoakManifest manifest = run(arguments);
        System.out.println("QUALIFICATION="
                + manifest.fields().get("qualification"));
    }

    static ManagedMemorySoakManifest run(String[] arguments) throws Exception {
        ManagedMemorySoakConfig config =
                ManagedMemorySoakConfig.parse(arguments);
        requireFreshOutput(config.output);
        ManagedMemorySoakPlan plan = config.profile
                == ManagedMemorySoakConfig.Profile.DEVELOPMENT
                ? null : ManagedMemorySoakPlan.create(config);
        Path sources = Files.createDirectories(config.output.resolve("sources"));
        Path outputs = Files.createDirectories(config.output.resolve("outputs"));
        Path jobLogPath = config.output.resolve("batch-jobs.log");
        Path resourceLogPath = config.output.resolve("resource-samples.log");
        Path activityLogPath = config.output.resolve("role-activity.log");
        List<Source> prepared = new ArrayList<>();
        for (int job = 0; job < config.concurrentJobs; job++) {
            int seed = 101 + job * 9973;
            Path source = sources.resolve(String.format(
                    java.util.Locale.ROOT, "source-%02d.tsv", job));
            prepared.add(new Source(source, seed,
                    ManagedMemorySoakWorkload.writeSource(source,
                            config.rowsPerJob,
                            config.keyCardinality, seed)));
        }

        long completedWaves = 0L;
        long wallStarted = 0L;
        long startedNanos = 0L;
        long endedNanos = 0L;
        long wallEnded = 0L;
        List<ManagedMemorySoakCluster.RoleProcess> roles =
                java.util.Collections.emptyList();
        try (StreamSoakEvidenceLog jobLog =
                     new StreamSoakEvidenceLog(jobLogPath);
             StreamSoakEvidenceLog resourceLog =
                     new StreamSoakEvidenceLog(resourceLogPath);
             ManagedMemorySoakCluster cluster =
                     ManagedMemorySoakCluster.start(config)) {
            roles = new ArrayList<>(cluster.roles());
            ExecutorService jobs = Executors.newFixedThreadPool(
                    config.concurrentJobs);
            java.util.concurrent.ScheduledExecutorService sampler =
                    Executors.newSingleThreadScheduledExecutor();
            AtomicReference<Throwable> sampleFailure = new AtomicReference<>();
            AtomicLong resourceSequence = new AtomicLong();
            wallStarted = System.currentTimeMillis();
            startedNanos = System.nanoTime();
            final long runStartNanos = startedNanos;
            final long runWallStarted = wallStarted;
            Runnable sample = () -> {
                try {
                    sampleResources(config, cluster, resourceLog,
                            resourceSequence, runStartNanos,
                            runWallStarted);
                } catch (Throwable failure) {
                    sampleFailure.compareAndSet(null, failure);
                }
            };
            sample.run();
            sampler.scheduleAtFixedRate(sample,
                    config.sampleIntervalMillis,
                    config.sampleIntervalMillis, TimeUnit.MILLISECONDS);
            try {
                long nextWave = startedNanos;
                long minimumWaves = divideCeiling(
                        config.durationMillis, config.waveIntervalMillis);
                while (requiresAnotherWave(elapsedMillis(startedNanos),
                        completedWaves, config.durationMillis,
                        minimumWaves)) {
                    Throwable asynchronous = sampleFailure.get();
                    if (asynchronous != null) throw propagate(asynchronous);
                    runWave(config, cluster, prepared, outputs, jobLog,
                            jobs, completedWaves, startedNanos,
                            wallStarted);
                    completedWaves++;
                    nextWave = Math.addExact(nextWave,
                            TimeUnit.MILLISECONDS.toNanos(
                                    config.waveIntervalMillis));
                    sleepUntil(nextWave);
                }
                sampler.shutdown();
                if (!sampler.awaitTermination(10L, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "MANAGED_MEMORY_RESOURCE_SAMPLER_DID_NOT_STOP");
                }
                sample.run();
                Throwable asynchronous = sampleFailure.get();
                if (asynchronous != null) throw propagate(asynchronous);
            } finally {
                sampler.shutdownNow();
                jobs.shutdownNow();
                sampler.awaitTermination(10L, TimeUnit.SECONDS);
                jobs.awaitTermination(10L, TimeUnit.SECONDS);
            }
            endedNanos = System.nanoTime();
            wallEnded = System.currentTimeMillis();
        }
        ManagedMemorySoakRoleActivity.write(
                roles, activityLogPath, config.concurrentJobs);

        Map<String, String> summary = new LinkedHashMap<>();
        summary.put("completed_waves", Long.toString(completedWaves));
        summary.put("duration_millis", Long.toString(
                TimeUnit.NANOSECONDS.toMillis(endedNanos - startedNanos)));
        summary.put("wall_duration_millis",
                Long.toString(wallEnded - wallStarted));
        return ManagedMemorySoakManifest.evaluate(
                config, plan, jobLogPath, resourceLogPath,
                activityLogPath, summary);
    }

    private static void requireFreshOutput(Path output) throws Exception {
        if (Files.exists(output)) {
            if (!Files.isDirectory(output)) {
                throw new IllegalStateException(
                        "MANAGED_MEMORY_SOAK_OUTPUT_NOT_EMPTY: " + output);
            }
            try (java.nio.file.DirectoryStream<Path> entries =
                         Files.newDirectoryStream(output)) {
                if (entries.iterator().hasNext()) {
                    throw new IllegalStateException(
                            "MANAGED_MEMORY_SOAK_OUTPUT_NOT_EMPTY: " + output);
                }
            }
        } else {
            Files.createDirectories(output);
        }
    }

    private static void runWave(
            ManagedMemorySoakConfig config,
            ManagedMemorySoakCluster cluster,
            List<Source> sources,
            Path outputRoot,
            StreamSoakEvidenceLog evidence,
            ExecutorService executor,
            long wave,
            long startedNanos,
            long wallStarted) throws Exception {
        List<Future<JobResult>> futures = new ArrayList<>();
        CyclicBarrier submissionBarrier = new CyclicBarrier(
                config.concurrentJobs);
        for (int job = 0; job < config.concurrentJobs; job++) {
            final int jobIndex = job;
            final Source source = sources.get(job);
            final Path output = outputRoot.resolve(String.format(
                    java.util.Locale.ROOT,
                    "wave-%08d-job-%02d.tsv", wave, job));
            final String attempt = String.format(java.util.Locale.ROOT,
                    "managed-memory-%08d-%02d", wave, job);
            futures.add(executor.submit(new Callable<JobResult>() {
                @Override public JobResult call() throws Exception {
                    long submissionElapsed = elapsedMillis(startedNanos);
                    long submissionEpoch = System.currentTimeMillis();
                    submissionBarrier.await(30L, TimeUnit.SECONDS);
                    ExecutionReport report = cluster.submit(
                            source.path, output, attempt);
                    long completionElapsed = elapsedMillis(startedNanos);
                    long completionEpoch = System.currentTimeMillis();
                    ManagedMemorySoakWorkload.OracleResult oracle =
                            ManagedMemorySoakWorkload.verify(output,
                                    config.rowsPerJob,
                                    config.keyCardinality, source.seed);
                    return new JobResult(jobIndex, source, output,
                            report, oracle, submissionElapsed,
                            submissionEpoch, completionElapsed,
                            completionEpoch);
                }
            }));
        }
        List<JobResult> results = new ArrayList<>();
        for (Future<JobResult> future : futures) results.add(future.get());
        long workspaceJobs = awaitWorkspaceCleanup(
                cluster.roleRoot(), 10_000L);
        for (JobResult result : results) {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("kind", "batch-job");
            fields.put("wave", Long.toString(wave));
            fields.put("job", Integer.toString(result.job));
            fields.put("submission_elapsed_millis",
                    Long.toString(result.submissionElapsedMillis));
            fields.put("submission_epoch_millis",
                    Long.toString(result.submissionEpochMillis));
            fields.put("elapsed_millis",
                    Long.toString(result.completionElapsedMillis));
            fields.put("wall_started_epoch_millis",
                    Long.toString(wallStarted));
            fields.put("observed_epoch_millis",
                    Long.toString(result.completionEpochMillis));
            fields.put("source_sha256", result.source.sha256);
            fields.put("source_seed", Integer.toString(result.source.seed));
            fields.put("output_sha256", result.oracle.sha256);
            fields.put("oracle_sha256", result.oracle.oracleSha256);
            fields.put("rows_out", Long.toString(result.report.rowsOut()));
            fields.put("oracle_match", Boolean.toString(
                    result.oracle.rows == result.report.rowsOut()));
            fields.put("resource_coverage",
                    result.report.resourceTelemetryCoverage().name());
            fields.put("within_budget",
                    Boolean.toString(result.report.withinBudget()));
            fields.put("peak_reserved_bytes", Long.toString(
                    result.report.peakReservedBytes()));
            fields.put("acquired_bytes", Long.toString(
                    result.report.acquiredBytes()));
            fields.put("pressure_spill_count", Long.toString(
                    result.report.pressureSpillCount()));
            fields.put("spill_run_count", Long.toString(
                    result.report.spillRunCount()));
            fields.put("spill_logical_bytes", Long.toString(
                    result.report.spillLogicalBytesWritten()));
            fields.put("workspace_job_directories",
                    Long.toString(workspaceJobs));
            evidence.append(fields);
            Files.delete(result.output);
        }
    }

    private static void sampleResources(
            ManagedMemorySoakConfig config,
            ManagedMemorySoakCluster cluster,
            StreamSoakEvidenceLog evidence,
            AtomicLong sequence,
            long startedNanos,
            long wallStarted) throws Exception {
        long diskBytes = StreamSoakResourceSampler.diskBytes(config.output);
        for (ManagedMemorySoakCluster.RoleProcess role : cluster.roles()) {
            StreamSoakResourceSampler.ProcessUsage usage =
                    StreamSoakResourceSampler.process(role.process);
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("kind", "process-resource");
            fields.put("sequence", Long.toString(sequence.getAndIncrement()));
            fields.put("role", role.role);
            fields.put("pid", Long.toString(usage.pid));
            fields.put("alive", Boolean.toString(role.process.isAlive()));
            fields.put("elapsed_millis",
                    Long.toString(elapsedMillis(startedNanos)));
            fields.put("wall_started_epoch_millis",
                    Long.toString(wallStarted));
            fields.put("observed_epoch_millis",
                    Long.toString(System.currentTimeMillis()));
            fields.put("rss_bytes", Long.toString(usage.rssBytes));
            fields.put("file_descriptors",
                    Long.toString(usage.fileDescriptors));
            fields.put("disk_bytes", Long.toString(diskBytes));
            evidence.append(fields);
        }
    }

    private static long awaitWorkspaceCleanup(Path roleRoot,
                                              long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long count;
        do {
            count = workspaceJobDirectories(roleRoot);
            if (count == 0L) return 0L;
            Thread.sleep(25L);
        } while (System.nanoTime() < deadline);
        return count;
    }

    static long workspaceJobDirectories(Path root) throws Exception {
        if (!Files.exists(root)) return 0L;
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isDirectory)
                    .filter(path -> path.getFileName() != null
                            && path.getFileName().toString()
                            .startsWith("job-"))
                    .count();
        }
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - startedNanos);
    }

    static boolean requiresAnotherWave(long elapsedMillis,
                                       long completedWaves,
                                       long requiredDurationMillis,
                                       long minimumWaves) {
        return elapsedMillis < requiredDurationMillis
                || completedWaves < minimumWaves;
    }

    private static long divideCeiling(long value, long divisor) {
        return Math.addExact(value, divisor - 1L) / divisor;
    }

    private static void sleepUntil(long targetNanos)
            throws InterruptedException {
        while (true) {
            long remaining = targetNanos - System.nanoTime();
            if (remaining <= 0L) return;
            TimeUnit.NANOSECONDS.sleep(remaining);
        }
    }

    private static Exception propagate(Throwable failure) {
        if (failure instanceof Exception) return (Exception) failure;
        if (failure instanceof Error) throw (Error) failure;
        return new RuntimeException(failure);
    }

    private static final class Source {
        private final Path path;
        private final int seed;
        private final String sha256;

        private Source(Path path, int seed, String sha256) {
            this.path = path;
            this.seed = seed;
            this.sha256 = sha256;
        }
    }

    private static final class JobResult {
        private final int job;
        private final Source source;
        private final Path output;
        private final ExecutionReport report;
        private final ManagedMemorySoakWorkload.OracleResult oracle;
        private final long submissionElapsedMillis;
        private final long submissionEpochMillis;
        private final long completionElapsedMillis;
        private final long completionEpochMillis;

        private JobResult(int job, Source source, Path output,
                          ExecutionReport report,
                          ManagedMemorySoakWorkload.OracleResult oracle,
                          long submissionElapsedMillis,
                          long submissionEpochMillis,
                          long completionElapsedMillis,
                          long completionEpochMillis) {
            this.job = job;
            this.source = source;
            this.output = output;
            this.report = report;
            this.oracle = oracle;
            this.submissionElapsedMillis = submissionElapsedMillis;
            this.submissionEpochMillis = submissionEpochMillis;
            this.completionElapsedMillis = completionElapsedMillis;
            this.completionEpochMillis = completionEpochMillis;
        }
    }
}
