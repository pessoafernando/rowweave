package io.rowweave.cluster.rpc;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.FilterFunction;
import io.rowweave.Source;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.MapFunction;
import io.rowweave.Sink;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.ClusterMain;
import io.rowweave.cluster.DurableRunSubmitException;
import io.rowweave.cluster.FixedMasterLocator;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentManifestCodec;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.DeploymentRuntimeConfig;
import io.rowweave.incremental.IncrementalDefinition;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierView;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.incremental.IncrementalPlanTemplateFingerprint;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalRunSubmission;
import io.rowweave.incremental.JdbcIncrementalSourceProfile;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.RunExecutionEvent;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunSubmission;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("multiprocess")
class MtlsProcessClusterTest {
    private static final String PASSWORD = "changeit";
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema JOIN_META = SortMergeJoin.of(
            META, META, new String[]{"id"}, new String[]{"id"}, JoinType.INNER)
            .getOutputSchema();

    @Test
    void boundedCdcRunsAcrossMutualTlsMasterAndTwoWorkerProcesses(
            @TempDir Path temp) throws Exception {
        Identity masterIdentity = identity(temp, "cdc-master");
        Identity workerIdentity = identity(temp, "cdc-worker");
        Identity clientIdentity = identity(temp, "cdc-client");
        Path trustStore = temp.resolve("cdc-control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("cdc-control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));
        Path accessJar = jdbcIncrementalAccessJar(temp);
        Path markers = temp.resolve("cdc-worker-markers");
        Files.createDirectory(markers);

        Child master = null;
        Child worker = null;
        Child worker2 = null;
        try {
            Path artifacts = temp.resolve("cdc-artifacts");
            Files.createDirectory(artifacts);
            Properties masterProperties = securityProperties(
                    masterIdentity, trustStore, authz);
            masterProperties.setProperty("rowweave.master.enable", "true");
            masterProperties.setProperty("rowweave.master.port", "0");
            masterProperties.setProperty(
                    "rowweave.master.artifact.dir", artifacts.toString());
            master = launch(masterProperties, "MASTER_PORT=", 30_000L);
            int masterPort = Integer.parseInt(master.readyValue);

            Properties workerProperties = securityProperties(
                    workerIdentity, trustStore, authz);
            workerProperties.setProperty(
                    "logback.configurationFile", productionLogback());
            workerProperties.setProperty("rowweave.worker.enable", "true");
            workerProperties.setProperty(
                    "rowweave.worker.master.host", "127.0.0.1");
            workerProperties.setProperty(
                    "rowweave.worker.master.port", Integer.toString(masterPort));
            workerProperties.setProperty("rowweave.worker.slots", "1");
            workerProperties.setProperty(
                    "rowweave.test.cdc.marker.dir", markers.toString());
            worker = launch(
                    workerProperties, "WORKER_READY=", 30_000L, accessJar);
            worker2 = launch(
                    workerProperties, "WORKER_READY=", 30_000L, accessJar);

            Properties clientProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            ControlSecurityConfig base =
                    ControlSecurityConfig.fromProperties(clientProperties);
            Path output = temp.resolve("cdc-manifest");
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         clientProperties, base,
                         Runtime.getRuntime().maxMemory());
                 ClusterJobClient client = new ClusterJobClient(
                         new FixedMasterLocator(new Endpoint(
                                 "127.0.0.1", masterPort)), runtime)) {
                IncrementalRunSubmission submission =
                        jdbcIncrementalSubmission(output);

                IncrementalFrontierView initialized =
                        client.initializeIncrementalFeed(
                                "initialize-process-feed", 1L, submission);
                ExecutionReport report = client.submitIncrementalRun(submission);
                IncrementalFrontierView committed =
                        client.queryIncrementalFrontier(submission.feedId());

                assertEquals(2L, report.rowsOut());
                assertEquals("READY", initialized.state());
                assertEquals("READY", committed.state());
                assertTrue(committed.revision() > initialized.revision());
                try (FileSource.Reader reader = new ManifestFileSource(
                        cdcMeta(), output.toString(), "changes").openReader()) {
                    assertEquals(1L, reader.next().get(0));
                    assertEquals(2L, reader.next().get(0));
                    assertFalse(reader.hasNext());
                }
            }
            try (java.util.stream.Stream<Path> opened = Files.list(markers)) {
                assertEquals(2L, opened.count(),
                        "both source splits must open in Worker JVMs");
            }
            assertTrue(master.process.isAlive(), master.output());
            assertTrue(worker.process.isAlive(), worker.output());
            assertTrue(worker2.process.isAlive(), worker2.output());
        } finally {
            close(worker2);
            close(worker);
            close(master);
        }
    }

    @Test
    void builtInArtifactRunsAcrossMutualTlsClientMasterWorker(@TempDir Path temp)
            throws Exception {
        Identity masterIdentity = identity(temp, "master");
        Identity workerIdentity = identity(temp, "worker");
        Identity clientIdentity = identity(temp, "client");
        Path trustStore = temp.resolve("control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        Child master = null;
        Child worker = null;
        Child worker2 = null;
        try {
            Properties masterProperties = securityProperties(
                    masterIdentity, trustStore, authz);
            masterProperties.setProperty("rowweave.master.enable", "true");
            masterProperties.setProperty("rowweave.master.port", "0");
            master = launch(masterProperties, "MASTER_PORT=", 30_000L);
            int masterPort = Integer.parseInt(master.readyValue);

            Properties workerProperties = securityProperties(
                    workerIdentity, trustStore, authz);
            workerProperties.setProperty(
                    "logback.configurationFile", productionLogback());
            workerProperties.setProperty("rowweave.worker.enable", "true");
            workerProperties.setProperty("rowweave.worker.master.host", "127.0.0.1");
            workerProperties.setProperty("rowweave.worker.master.port", Integer.toString(masterPort));
            workerProperties.setProperty("rowweave.worker.slots", "8");
            worker = launch(workerProperties, "WORKER_READY=", 30_000L);

            Properties worker2Properties = securityProperties(
                    workerIdentity, trustStore, authz);
            worker2Properties.setProperty(
                    "logback.configurationFile", productionLogback());
            worker2Properties.setProperty("rowweave.worker.enable", "true");
            worker2Properties.setProperty("rowweave.worker.master.host", "127.0.0.1");
            worker2Properties.setProperty("rowweave.worker.master.port", Integer.toString(masterPort));
            worker2Properties.setProperty("rowweave.worker.slots", "8");
            worker2 = launch(worker2Properties, "WORKER_READY=", 30_000L);

            Path source = temp.resolve("source.tsv");
            writeRows(source, row(1, "10.00"), row(2, "20.00"), row(3, "30.00"));
            Path target = temp.resolve("result.tsv");
            Path unionLeft = temp.resolve("union-left.tsv");
            Path unionRight = temp.resolve("union-right.tsv");
            Path unionTarget = temp.resolve("union-result.tsv");
            writeRows(unionLeft, row(11, "110.00"), row(12, "120.00"));
            writeRows(unionRight, row(21, "210.00"), row(22, "220.00"));
            DataSchema grouped = ParallelGroupByAgg.of(META, "id")
                    .sum("amount").getOutputSchema();
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META, Collections.singletonList(
                            new FileSource(META, source.toString()))),
                    PlanNode.unary("groupBySum", grouped, 0)
                            .listParam("keys", Collections.singletonList("id"))
                            .param("measure", "amount").build(),
                    PlanNode.sink(grouped, 1,
                            new FileSink(grouped, target.toString())).build()), 2);

            Properties clientProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            ControlSecurityConfig base = ControlSecurityConfig.fromProperties(clientProperties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         clientProperties, base, Runtime.getRuntime().maxMemory());
                 ClusterJobClient client = new ClusterJobClient(
                         new FixedMasterLocator(new Endpoint("127.0.0.1", masterPort)), runtime)) {
                ExecutionReport report = client.submit(
                        LegacyPlanArtifactAdapter.wrap("mtls-built-in", graph));
                assertEquals(3L, report.rowsOut());
                assertEquals(TelemetryCoverage.COMPLETE,
                        report.resourceTelemetryCoverage());
                assertEquals(TelemetryCoverage.NOT_APPLICABLE,
                        report.partitionTelemetryCoverage());
                assertTrue(report.partitionDistributions().isEmpty());

                Path passThroughTarget = temp.resolve("pass-through.tsv");
                PlanGraph passThrough = new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META, Collections.singletonList(
                                new FileSource(META, source.toString()))),
                        PlanNode.sink(META, 0,
                                new FileSink(META, passThroughTarget.toString())).build()), 1);
                ExecutionReport passThroughReport = client.submit(
                        LegacyPlanArtifactAdapter.wrap("mtls-pass-through", passThrough));
                assertEquals(3L, passThroughReport.rowsOut());
                assertEquals(TelemetryCoverage.COMPLETE,
                        passThroughReport.resourceTelemetryCoverage());
                assertEquals(0L, passThroughReport.peakReservedBytes());
                assertEquals(0L, passThroughReport.acquiredBytes());
                assertEquals(0L, passThroughReport.pressureSpillCount());
                assertEquals(0L, passThroughReport.spillRunCount());
                assertEquals(0L, passThroughReport.spillLogicalBytesWritten());
                assertEquals(0L, passThroughReport.spillPeakReservedBytes());
                assertEquals(TelemetryCoverage.NOT_APPLICABLE,
                        passThroughReport.partitionTelemetryCoverage());
                assertTrue(passThroughReport.partitionDistributions().isEmpty());

                PlanGraph unionGraph = new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META, Collections.singletonList(
                                new FileSource(META, unionLeft.toString()))),
                        PlanNode.sourceFromInputs(META, Collections.singletonList(
                                new FileSource(META, unionRight.toString()))),
                        PlanNode.binary("union", META, 0, 1).build(),
                        PlanNode.sink(META, 2,
                                new FileSink(META, unionTarget.toString())).build()), 2);
                ExecutionReport unionReport = client.submit(LegacyPlanArtifactAdapter.wrap(
                        "mtls-built-in-union", unionGraph));
                assertEquals(4L, unionReport.rowsOut());
                assertEquals(TelemetryCoverage.COMPLETE,
                        unionReport.resourceTelemetryCoverage());
                assertEquals(TelemetryCoverage.NOT_APPLICABLE,
                        unionReport.partitionTelemetryCoverage());
                assertTrue(unionReport.partitionDistributions().isEmpty());

                Path splitA = temp.resolve("multi-input-a.tsv");
                Path splitB = temp.resolve("multi-input-b.tsv");
                Path fanoutA = temp.resolve("fanout-a.tsv");
                Path fanoutB = temp.resolve("fanout-b.tsv");
                writeRows(splitA, row(31, "310.00"), row(32, "320.00"));
                writeRows(splitB, row(41, "410.00"), row(42, "420.00"));
                PlanGraph multiInputFanout = new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META, Arrays.asList(
                                new FileSource(META, splitA.toString()),
                                new FileSource(META, splitB.toString()))),
                        PlanNode.sink(META, 0,
                                new FileSink(META, fanoutA.toString())).build(),
                        PlanNode.sink(META, 0,
                                new FileSink(META, fanoutB.toString())).build()), 2);
                ExecutionReport fanoutReport = client.submit(LegacyPlanArtifactAdapter.wrap(
                        "mtls-multi-input-fanout", multiInputFanout));
                assertEquals(8L, fanoutReport.rowsOut());
                assertEquals(TelemetryCoverage.COMPLETE,
                        fanoutReport.resourceTelemetryCoverage());
                assertTrue(fanoutReport.peakReservedBytes() > 0L,
                        "shared multi-output fan-out owns a managed materialization buffer");
                assertTrue(fanoutReport.acquiredBytes() > 0L);
                assertTrue(fanoutReport.withinBudget());
                assertEquals(0L, fanoutReport.pressureSpillCount());
                assertEquals(0L, fanoutReport.spillRunCount());
                assertEquals(0L, fanoutReport.spillLogicalBytesWritten());
                assertEquals(0L, fanoutReport.spillPeakReservedBytes());
                assertEquals(TelemetryCoverage.NOT_APPLICABLE,
                        fanoutReport.partitionTelemetryCoverage());
                assertTrue(fanoutReport.partitionDistributions().isEmpty());

                Path joinLeft = temp.resolve("join-left.tsv");
                Path joinRight = temp.resolve("join-right.tsv");
                writeRows(joinLeft, row(1, "10.00"), row(2, "20.00"),
                        row(3, "30.00"), row(4, "40.00"), row(5, "50.00"),
                        row(6, "60.00"));
                writeRows(joinRight, row(2, "200.00"), row(4, "400.00"),
                        row(6, "600.00"));
                String[] hints = {"HASH", "SORT_MERGE", null};
                String[] strategies = {"HASH", "SORT_MERGE", "AUTO"};
                long[] elapsedNanos = new long[strategies.length];
                for (int i = 0; i < strategies.length; i++) {
                    Path joinTarget = temp.resolve(
                            "join-" + strategies[i].toLowerCase() + ".tsv");
                    long started = System.nanoTime();
                    ExecutionReport joinReport = client.submit(
                            LegacyPlanArtifactAdapter.wrap(
                                    "mtls-join-" + strategies[i].toLowerCase(),
                                    joinGraph(joinLeft, joinRight, joinTarget,
                                            hints[i])));
                    elapsedNanos[i] = System.nanoTime() - started;
                    assertEquals(3L, joinReport.rowsOut());
                    assertEquals(TelemetryCoverage.COMPLETE,
                            joinReport.resourceTelemetryCoverage());
                    assertEquals(TelemetryCoverage.COMPLETE,
                            joinReport.partitionTelemetryCoverage());
                    assertEquals(Arrays.asList(
                                    "2|20.00|2|200.00",
                                    "4|40.00|4|400.00",
                                    "6|60.00|6|600.00"),
                            renderRows(FileSink.read(
                                    JOIN_META, joinTarget.toString())));
                    String witness = "ROWWEAVE_JOIN_RUNTIME strategy=" + strategies[i];
                    String newLogs = awaitCombinedLog(
                            worker, worker2, witness, 5_000L);
                    assertTrue(newLogs.contains(witness),
                            strategies[i] + " must execute in a Worker JVM:\n" + newLogs);
                }
                System.out.printf("[DP7-MTLS-JOIN] HASH=%d ms, SMJ=%d ms, AUTO=%d ms%n",
                        TimeUnit.NANOSECONDS.toMillis(elapsedNanos[0]),
                        TimeUnit.NANOSECONDS.toMillis(elapsedNanos[1]),
                        TimeUnit.NANOSECONDS.toMillis(elapsedNanos[2]));

                Path throughputSource = temp.resolve("reuse-throughput-source.tsv");
                writeRows(throughputSource, 50_000);
                long throughputStarted = System.nanoTime();
                ExecutionReport throughput = client.submit(
                        LegacyPlanArtifactAdapter.wrap(
                                "mtls-reused-fanout-throughput",
                                reusedFanoutGraph(throughputSource)));
                long throughputNanos = System.nanoTime() - throughputStarted;
                assertEquals(512L, throughput.rowsOut());
                System.out.printf("[DP7-MTLS-FANOUT] inputRows=50000, "
                                + "logicalCopies=100000, elapsed=%d ms, rowsPerSecond=%.1f%n",
                        TimeUnit.NANOSECONDS.toMillis(throughputNanos),
                        100_000d * TimeUnit.SECONDS.toNanos(1L) / throughputNanos);
            }

            List<DataRow> rows = FileSink.read(grouped, target.toString());
            assertEquals(3, rows.size());
            java.util.Map<Integer, java.math.BigDecimal> byId = new java.util.HashMap<>();
            for (DataRow row : rows) {
                byId.put((Integer) row.get(0), (java.math.BigDecimal) row.get(1));
            }
            assertEquals(new java.math.BigDecimal("10.0000000000"), byId.get(1));
            assertEquals(new java.math.BigDecimal("20.0000000000"), byId.get(2));
            assertEquals(new java.math.BigDecimal("30.0000000000"), byId.get(3));
            List<DataRow> unionRows = FileSink.read(META, unionTarget.toString());
            assertEquals(4, unionRows.size(),
                    "MTLS union must share one authorized target without losing either edge");
            List<String> expectedFanout = Arrays.asList(
                    "31|310.00", "32|320.00", "41|410.00", "42|420.00");
            assertEquals(expectedFanout, renderRows(FileSink.read(
                    META, temp.resolve("fanout-a.tsv").toString())));
            assertEquals(expectedFanout, renderRows(FileSink.read(
                    META, temp.resolve("fanout-b.tsv").toString())));

            String connectionLogs = awaitCombinedLog(worker, worker2,
                    "ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME", 5_000L);
            List<String> connectionMetrics = linesContaining(
                    connectionLogs, "ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME");
            assertFalse(connectionMetrics.isEmpty(), connectionLogs);
            long maxLogicalStreams = 0L;
            long totalRecords = 0L;
            long totalConnectNanos = 0L;
            long reusedWireBytes = 0L;
            long reusedActiveDataNanos = 0L;
            int reusedPhysicalConnections = 0;
            for (String metric : connectionMetrics) {
                assertTrue(metric.contains("protocol=SHUFFLE_MTLS_V1"), metric);
                long connectNanos = metricValue(metric, "connectNanos");
                long logicalStreams = metricValue(metric, "logicalStreams");
                assertTrue(connectNanos > 0L, metric);
                totalConnectNanos += connectNanos;
                maxLogicalStreams = Math.max(maxLogicalStreams, logicalStreams);
                totalRecords += metricValue(metric, "records");
                if (logicalStreams > 1L) {
                    reusedPhysicalConnections++;
                    reusedWireBytes += metricValue(metric, "wireBytes");
                    reusedActiveDataNanos += metricValue(metric, "activeDataNanos");
                }
            }
            assertTrue(maxLogicalStreams > 1L,
                    "real MTLS worker-pair must reuse a physical connection: "
                            + connectionMetrics);
            assertTrue(totalRecords > 0L, connectionMetrics.toString());
            assertTrue(reusedPhysicalConnections > 0, connectionMetrics.toString());
            assertTrue(reusedWireBytes > 0L && reusedActiveDataNanos > 0L,
                    connectionMetrics.toString());
            System.out.printf("[DP7-MTLS-SHUFFLE] physicalLifetimes=%d, "
                            + "maxLogicalStreams=%d, records=%d, coldConnectAvg=%.3f ms, "
                            + "reusedConnections=%d, reusedWireThroughput=%.3f MiB/s%n",
                    connectionMetrics.size(), maxLogicalStreams, totalRecords,
                    totalConnectNanos / 1_000_000d / connectionMetrics.size(),
                    reusedPhysicalConnections,
                    reusedWireBytes * 1_000_000_000d
                            / reusedActiveDataNanos / (1024d * 1024d));
            assertTrue(master.process.isAlive(), master.output());
            assertTrue(worker.process.isAlive(), worker.output());
            assertTrue(worker2.process.isAlive(), worker2.output());
        } finally {
            close(worker2);
            close(worker);
            close(master);
        }
    }

    @Test
    void workerLossCancelsRealMtlsShuffleAndReplacementRestoresCapacity(
            @TempDir Path temp) throws Exception {
        Identity masterIdentity = identity(temp, "loss-master");
        Identity workerIdentity = identity(temp, "loss-worker");
        Identity clientIdentity = identity(temp, "loss-client");
        Path trustStore = temp.resolve("loss-control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("loss-control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        String timeoutKey = "rowweave.cluster.submitTimeoutMs";
        String cleanupKey = "rowweave.cluster.submitCleanupGraceMs";
        String previousTimeout = System.setProperty(timeoutKey, "15000");
        String previousCleanup = System.setProperty(cleanupKey, "5000");
        Child master = null;
        Child worker = null;
        Child failedWorker = null;
        Child replacement = null;
        Child survivingWorker = null;
        try {
            Properties masterProperties = securityProperties(
                    masterIdentity, trustStore, authz);
            masterProperties.setProperty("rowweave.master.enable", "true");
            masterProperties.setProperty("rowweave.master.port", "0");
            masterProperties.setProperty("rowweave.heartbeat.staleness.ms", "1500");
            master = launch(masterProperties, "MASTER_PORT=", 30_000L);
            int masterPort = Integer.parseInt(master.readyValue);

            Properties workerProperties = securityProperties(
                    workerIdentity, trustStore, authz);
            workerProperties.setProperty(
                    "logback.configurationFile", productionLogback());
            workerProperties.setProperty("rowweave.worker.enable", "true");
            workerProperties.setProperty("rowweave.worker.master.host", "127.0.0.1");
            workerProperties.setProperty(
                    "rowweave.worker.master.port", Integer.toString(masterPort));
            workerProperties.setProperty("rowweave.worker.slots", "8");
            workerProperties.setProperty("rowweave.heartbeat.staleness.ms", "1500");
            worker = launch(workerProperties, "WORKER_READY=", 30_000L);
            failedWorker = launch(workerProperties, "WORKER_READY=", 30_000L);

            Path largeSource = temp.resolve("worker-loss-source.tsv");
            writeRows(largeSource, 1_000_000);
            PlanGraph interrupted = groupByGraph(largeSource, 2);
            Properties clientProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            ControlSecurityConfig base = ControlSecurityConfig.fromProperties(clientProperties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         clientProperties, base, Runtime.getRuntime().maxMemory());
                 ClusterJobClient client = new ClusterJobClient(
                         new FixedMasterLocator(new Endpoint(
                                 "127.0.0.1", masterPort)), runtime)) {
                java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                        new java.util.concurrent.atomic.AtomicReference<>();
                long started = System.nanoTime();
                Thread submitter = new Thread(() -> {
                    try {
                        client.submit(LegacyPlanArtifactAdapter.wrap(
                                "mtls-worker-loss", interrupted));
                    } catch (Throwable error) {
                        failure.set(error);
                    }
                }, "mtls-worker-loss-submit");
                submitter.start();

                String openWitness =
                        "ROWWEAVE_SHUFFLE_CONNECTION_OPEN protocol=SHUFFLE_MTLS_V1";
                Child failureTarget = awaitEitherChildLog(
                        worker, failedWorker, openWitness, 10_000L);
                survivingWorker = failureTarget == worker ? failedWorker : worker;
                close(failureTarget);

                submitter.join(20_000L);
                assertFalse(submitter.isAlive(),
                        "worker loss must converge within execution+cleanup budgets");
                assertTrue(failure.get() != null,
                        "an authenticated shuffle cannot succeed after a participating Worker exits");
                long failureMillis = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - started);
                assertTrue(failureMillis < 20_000L,
                        "worker loss exceeded bounded convergence: " + failureMillis + "ms");

                replacement = launch(workerProperties, "WORKER_READY=", 30_000L);
                Thread.sleep(2_000L);
                Path recoverySource = temp.resolve("replacement-source.tsv");
                writeRows(recoverySource, 4_096);
                ExecutionReport recovered = client.submit(
                        LegacyPlanArtifactAdapter.wrap(
                                "mtls-worker-replacement",
                                groupByGraph(recoverySource, 2)));
                assertEquals(256L, recovered.rowsOut());
                System.out.printf("[DP7-MTLS-RECOVERY] workerLoss=%d ms, "
                                + "replacementRows=%d%n",
                        failureMillis, recovered.rowsOut());
            }
            assertTrue(master.process.isAlive(), master.output());
            assertTrue(survivingWorker != null && survivingWorker.process.isAlive(),
                    survivingWorker == null ? "missing surviving Worker"
                            : survivingWorker.output());
            assertTrue(replacement.process.isAlive(), replacement.output());
        } finally {
            close(replacement);
            close(failedWorker);
            close(worker);
            close(master);
            restoreProperty(timeoutKey, previousTimeout);
            restoreProperty(cleanupKey, previousCleanup);
        }
    }

    @Test
    void signedInputConcreteFunctionAndLambdaRunFromDeploymentInEveryRole(
            @TempDir Path temp) throws Exception {
        Identity masterIdentity = identity(temp, "signed-master");
        Identity workerIdentity = identity(temp, "signed-worker");
        Identity clientIdentity = identity(temp, "signed-client");
        Path trustStore = temp.resolve("signed-control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("signed-control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        DeploymentMaterial deployment = deployment(temp);
        Child master = null;
        Child worker = null;
        try {
            Path artifacts = temp.resolve("signed-artifacts");
            Files.createDirectory(artifacts);
            Properties masterProperties = securityProperties(
                    masterIdentity, trustStore, authz);
            configureDeployment(masterProperties, deployment);
            masterProperties.setProperty("rowweave.master.enable", "true");
            masterProperties.setProperty("rowweave.master.port", "0");
            masterProperties.setProperty(
                    "rowweave.master.artifact.dir", artifacts.toString());
            master = launch(masterProperties, "MASTER_PORT=", 30_000L);
            int masterPort = Integer.parseInt(master.readyValue);

            Properties workerProperties = securityProperties(
                    workerIdentity, trustStore, authz);
            configureDeployment(workerProperties, deployment);
            workerProperties.setProperty("rowweave.worker.enable", "true");
            workerProperties.setProperty("rowweave.worker.master.host", "127.0.0.1");
            workerProperties.setProperty("rowweave.worker.master.port", Integer.toString(masterPort));
            workerProperties.setProperty("rowweave.worker.slots", "8");
            worker = launch(workerProperties, "WORKER_READY=", 30_000L);

            Properties clientProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            configureDeployment(clientProperties, deployment);
            ControlSecurityConfig base = ControlSecurityConfig.fromProperties(clientProperties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         clientProperties, base, Runtime.getRuntime().maxMemory());
                 ClusterJobClient client = new ClusterJobClient(
                         new FixedMasterLocator(new Endpoint("127.0.0.1", masterPort)), runtime)) {
                ClassLoader loader = runtime.applicationClassLoader("application");
                Source input = (Source) loader.loadClass("app.JobComponents$SignedInput")
                        .getDeclaredConstructor().newInstance();
                MapFunction map = (MapFunction) loader.loadClass("app.JobComponents$SignedMap")
                        .getDeclaredConstructor().newInstance();
                FilterFunction filter = (FilterFunction) loader.loadClass("app.JobComponents")
                        .getMethod("filter").invoke(null);
                assertEquals(loader, input.getClass().getClassLoader());
                assertEquals(loader, map.getClass().getClassLoader());
                assertEquals(loader, filter.getClass().getClassLoader());

                Path target = temp.resolve("signed-result.tsv");
                PlanGraph graph = new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META, Collections.singletonList(input)),
                        PlanNode.unaryFn("map", META, 0, map),
                        PlanNode.unaryFn("filterFn", META, 1, filter),
                        PlanNode.sink(META, 2,
                                new FileSink(META, target.toString())).build()), 2);
                ExecutionReport report = client.submit(LegacyPlanArtifactAdapter.wrap(
                        "mtls-signed-application", graph, runtime.objectTrustContext()));
                assertEquals(2L, report.rowsOut());

                List<DataRow> rows = new ArrayList<>(
                        FileSink.read(META, target.toString()));
                rows.sort((left, right) -> Integer.compare(
                        (Integer) left.get(0), (Integer) right.get(0)));
                assertEquals(2, rows.size());
                assertEquals(2, rows.get(0).get(0));
                assertEquals(new java.math.BigDecimal("120.00"), rows.get(0).get(1));
                assertEquals(3, rows.get(1).get(0));
                assertEquals(new java.math.BigDecimal("130.00"), rows.get(1).get(1));

                byte[] objectBytes = "4\t40.00\n5\t50.00\n"
                        .getBytes(StandardCharsets.UTF_8);
                Path object = temp.resolve("signed-object-source.tsv").toAbsolutePath();
                Files.write(object, objectBytes);
                ObjectStorageSourceProvider provider = (ObjectStorageSourceProvider)
                        loader.loadClass("app.JobComponents$SignedObjectProvider")
                                .getDeclaredConstructor().newInstance();
                Source objectInput = new ObjectStorageSnapshotSource(
                        META, provider, "endpoint-fixture", "credential-fixture",
                        "fixture", object.toString(), "version-1", objectBytes.length,
                        sha256Hex(objectBytes), ObjectStorageSnapshotSource.MIN_ROW_BYTES);
                Path objectTarget = temp.resolve("signed-object-result.tsv");
                PlanGraph objectGraph = new PlanGraph(Arrays.asList(
                        operator(PlanNode.sourceFromInputs(
                                META, Collections.singletonList(objectInput)),
                                "object-source"),
                        operator(PlanNode.sink(META, 0,
                                new FileSink(META, objectTarget.toString())).build(),
                                "object-output")), 2);
                ExecutionReport objectReport = client.submit(
                        LegacyPlanArtifactAdapter.wrap(
                                "mtls-signed-object-source", objectGraph,
                                runtime.objectTrustContext()));
                assertEquals(2L, objectReport.rowsOut());
                List<DataRow> objectRows = FileSink.read(META, objectTarget.toString());
                assertEquals(2, objectRows.size());
                assertEquals(4, objectRows.get(0).get(0));
                assertEquals(new java.math.BigDecimal("40.00"),
                        objectRows.get(0).get(1));
                assertEquals(5, objectRows.get(1).get(0));
                assertEquals(new java.math.BigDecimal("50.00"),
                        objectRows.get(1).get(1));

                JdbcSnapshotSourceProvider jdbcProvider =
                        (JdbcSnapshotSourceProvider) loader
                                .loadClass("app.JobComponents$SignedJdbcProvider")
                                .getDeclaredConstructor().newInstance();
                String jdbcQueryDigest = String.join("",
                        Collections.nCopies(64, "a"));
                String jdbcPartitionDigest = String.join("",
                        Collections.nCopies(64, "b"));
                String jdbcSnapshotId = String.join("",
                        Collections.nCopies(64, "c"));
                Source jdbcInput = new JdbcSnapshotSource(
                        META, jdbcProvider, "endpoint-fixture", "credential-fixture",
                        "ledger", "ledger-query", "partition-0", "snapshot",
                        jdbcQueryDigest, jdbcPartitionDigest, jdbcSnapshotId,
                        64, 30, 4096);
                Path jdbcTarget = temp.resolve("signed-jdbc-result.tsv");
                PlanGraph jdbcGraph = new PlanGraph(Arrays.asList(
                        operator(PlanNode.sourceFromInputs(
                                META, Collections.singletonList(jdbcInput)),
                                "jdbc-source"),
                        operator(PlanNode.sink(META, 0,
                                new FileSink(META, jdbcTarget.toString())).build(),
                                "jdbc-output")), 2);
                ExecutionReport jdbcReport = client.submit(
                        LegacyPlanArtifactAdapter.wrap(
                                "mtls-signed-jdbc-source", jdbcGraph,
                                runtime.objectTrustContext()));
                assertEquals(2L, jdbcReport.rowsOut());
                List<DataRow> jdbcRows = FileSink.read(META, jdbcTarget.toString());
                assertEquals(2, jdbcRows.size());
                assertEquals(6, jdbcRows.get(0).get(0));
                assertEquals(new java.math.BigDecimal("60.00"),
                        jdbcRows.get(0).get(1));
                assertEquals(7, jdbcRows.get(1).get(0));
                assertEquals(new java.math.BigDecimal("70.00"),
                        jdbcRows.get(1).get(1));

                PlanGraph durableObjectGraph = new PlanGraph(Arrays.asList(
                        operator(PlanNode.sourceFromInputs(
                                META, Collections.singletonList(objectInput)),
                                "durable-object-source"),
                        operator(PlanNode.sink(META, 0,
                                new DiscardSink(META)).build(),
                                "durable-object-output")), 2);
                ExecutionReport durableObjectReport = client.submitRun(
                        LegacyPlanArtifactAdapter.wrap(
                                "mtls-durable-object-source", durableObjectGraph,
                                runtime.objectTrustContext()),
                        submission("run-mtls-object-source"));
                assertEquals(2L, durableObjectReport.rowsOut());
                assertEquals(RunStatus.SUCCEEDED,
                        client.queryRun("run-mtls-object-source").status());

                PlanGraph durableJdbcGraph = new PlanGraph(Arrays.asList(
                        operator(PlanNode.sourceFromInputs(
                                META, Collections.singletonList(jdbcInput)),
                                "durable-jdbc-source"),
                        operator(PlanNode.sink(META, 0,
                                new DiscardSink(META)).build(),
                                "durable-jdbc-output")), 2);
                ExecutionReport durableJdbcReport = client.submitRun(
                        LegacyPlanArtifactAdapter.wrap(
                                "mtls-durable-jdbc-source", durableJdbcGraph,
                                runtime.objectTrustContext()),
                        submission("run-mtls-jdbc-source"));
                assertEquals(2L, durableJdbcReport.rowsOut());
                assertEquals(RunStatus.SUCCEEDED,
                        client.queryRun("run-mtls-jdbc-source").status());
            }
            assertTrue(master.process.isAlive(), master.output());
            assertTrue(worker.process.isAlive(), worker.output());
        } finally {
            close(worker);
            close(master);
            deployment.restoreWritable();
        }
    }

    @Test
    void durableDiscardAndMultiManifestSucceedWhileCustomOutputStopsBeforeLookup(
            @TempDir Path temp) throws Exception {
        Identity masterIdentity = identity(temp, "durable-master");
        Identity workerIdentity = identity(temp, "durable-worker");
        Identity clientIdentity = identity(temp, "durable-client");
        Path trustStore = temp.resolve("durable-control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("durable-control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        DeploymentMaterial deployment = deployment(temp);
        Child master = null;
        Child worker = null;
        try {
            Path artifacts = temp.resolve("artifacts");
            Files.createDirectory(artifacts);
            Properties masterProperties = securityProperties(
                    masterIdentity, trustStore, authz);
            configureDeployment(masterProperties, deployment);
            masterProperties.setProperty("rowweave.master.enable", "true");
            masterProperties.setProperty("rowweave.master.port", "0");
            masterProperties.setProperty("rowweave.master.artifact.dir", artifacts.toString());
            master = launch(masterProperties, "MASTER_PORT=", 30_000L);
            int masterPort = Integer.parseInt(master.readyValue);

            Properties workerProperties = securityProperties(
                    workerIdentity, trustStore, authz);
            configureDeployment(workerProperties, deployment);
            workerProperties.setProperty("rowweave.worker.enable", "true");
            workerProperties.setProperty("rowweave.worker.master.host", "127.0.0.1");
            workerProperties.setProperty("rowweave.worker.master.port", Integer.toString(masterPort));
            workerProperties.setProperty("rowweave.worker.slots", "8");
            worker = launch(workerProperties, "WORKER_READY=", 30_000L);

            Properties clientProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            configureDeployment(clientProperties, deployment);
            ControlSecurityConfig base = ControlSecurityConfig.fromProperties(clientProperties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         clientProperties, base, Runtime.getRuntime().maxMemory())) {
                AtomicInteger lookups = new AtomicInteger();
                try (ClusterJobClient client = new ClusterJobClient(() -> {
                    lookups.incrementAndGet();
                    return new Endpoint("127.0.0.1", masterPort);
                }, runtime)) {
                    ClassLoader loader = runtime.applicationClassLoader("application");
                    Source input = (Source) loader.loadClass("app.JobComponents$SignedInput")
                            .getDeclaredConstructor().newInstance();

                    PlanGraph discard = new PlanGraph(Arrays.asList(
                            operator(PlanNode.sourceFromInputs(
                                    META, Collections.singletonList(input)), "discard-source"),
                            operator(PlanNode.sink(META, 0,
                                    new DiscardSink(META)).build(), "discard-output")), 1);
                    ExecutionReport discarded = client.submitRun(
                            LegacyPlanArtifactAdapter.wrap(
                                    "durable-discard", discard, runtime.objectTrustContext()),
                            submission("run-mtls-discard"));
                    assertEquals(3L, discarded.rowsOut());
                    assertEquals(RunStatus.SUCCEEDED,
                            client.queryRun("run-mtls-discard").status());
                    RunExecutionEventPage events = client.queryRunEvents(
                            "run-mtls-discard", 0L, 256);
                    assertEquals(RunExecutionEventPage.State.FOUND, events.state());
                    assertEquals(6, events.events().size());
                    assertEquals(RunStatus.SUCCEEDED,
                            events.events().get(events.events().size() - 1).runTo());

                    DurableRunSubmitException deadlineFailure = assertThrows(
                            DurableRunSubmitException.class,
                            () -> client.submitRun(
                                    LegacyPlanArtifactAdapter.wrap(
                                            "durable-deadline", discard,
                                            runtime.objectTrustContext()),
                                    new RunSubmission(
                                            "run-mtls-deadline", "attempt-deadline",
                                            "publication-deadline", 1, 10L, 10L)));
                    assertEquals(DurableRunSubmitException.Category.TERMINAL_FAILED,
                            deadlineFailure.category());
                    assertEquals(RunStatus.FAILED,
                            client.queryRun("run-mtls-deadline").status());
                    RunExecutionEventPage deadlineEvents = client.queryRunEvents(
                            "run-mtls-deadline", 0L, 256);
                    assertEquals(1L, deadlineEvents.events().stream()
                            .filter(event -> event.type()
                                    == RunExecutionEvent.Type.TIMEOUT_REQUESTED)
                            .count());

                    Path domain = temp.resolve("manifest-domain");
                    PlanGraph manifest = new PlanGraph(Arrays.asList(
                            operator(PlanNode.sourceFromInputs(
                                    META, Collections.singletonList(input)), "manifest-source"),
                            operator(PlanNode.sink(META, 0,
                                    new ManifestFileSink(META, domain.toString(), "ledger-a"))
                                    .build(), "manifest-output-a"),
                            operator(PlanNode.sink(META, 0,
                                    new ManifestFileSink(META, domain.toString(), "ledger-b"))
                                    .build(), "manifest-output-b")), 1);
                    ExecutionReport published = client.submitRun(
                            LegacyPlanArtifactAdapter.wrap(
                                    "durable-manifest", manifest, runtime.objectTrustContext()),
                            submission("run-mtls-manifest"));
                    assertEquals(6L, published.rowsOut());
                    for (String member : new String[] {"ledger-a", "ledger-b"}) {
                        try (FileSource.Reader reader = new ManifestFileSource(
                                META, domain.toString(), member).openReader()) {
                            assertEquals(1, reader.next().get(0));
                            assertEquals(2, reader.next().get(0));
                            assertEquals(3, reader.next().get(0));
                            assertFalse(reader.hasNext());
                        }
                    }

                    Sink custom = (Sink) loader.loadClass(
                            "app.JobComponents$SignedOutput")
                            .getDeclaredConstructor().newInstance();
                    PlanGraph rejected = new PlanGraph(Arrays.asList(
                            operator(PlanNode.sourceFromInputs(
                                    META, Collections.singletonList(input)), "custom-source"),
                            operator(PlanNode.sink(META, 0, custom).build(), "custom-output")), 1);
                    int before = lookups.get();
                    IllegalArgumentException failure = assertThrows(
                            IllegalArgumentException.class,
                            () -> client.submitRun(LegacyPlanArtifactAdapter.wrap(
                                            "durable-custom", rejected,
                                            runtime.objectTrustContext()),
                                    submission("run-mtls-custom")));
                    assertTrue(failure.getMessage().startsWith(
                            "durable run requires crash-reconcilable output:"),
                            failure.getMessage());
                    assertEquals(before, lookups.get(),
                            "custom durable Sink must stop before leader lookup/network");
                }
            }
        } finally {
            close(worker);
            close(master);
            deployment.restoreWritable();
        }
    }

    @Test
    void untrustedCertificateAndDeploymentSetStopBeforeUserCode(
            @TempDir Path temp) throws Exception {
        Identity masterIdentity = identity(temp, "negative-master");
        Identity workerIdentity = identity(temp, "negative-worker");
        Identity clientIdentity = identity(temp, "negative-client");
        Identity untrustedIdentity = identity(temp, "negative-untrusted-client");
        Path trustStore = temp.resolve("negative-control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("negative-control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n"
                + untrustedIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        DeploymentMaterial setA = deployment(temp.resolve("set-a"), "set-a");
        DeploymentMaterial setB = deployment(temp.resolve("set-b"), "set-b");
        Path marker = temp.resolve("user-code-ran.marker");
        Child master = null;
        Child worker = null;
        try {
            Properties masterProperties = securityProperties(
                    masterIdentity, trustStore, authz);
            configureDeployment(masterProperties, setA);
            masterProperties.setProperty("rowweave.master.enable", "true");
            masterProperties.setProperty("rowweave.master.port", "0");
            master = launch(masterProperties, "MASTER_PORT=", 30_000L);
            int masterPort = Integer.parseInt(master.readyValue);

            Properties workerProperties = securityProperties(
                    workerIdentity, trustStore, authz);
            configureDeployment(workerProperties, setA);
            workerProperties.setProperty("rowweave.worker.enable", "true");
            workerProperties.setProperty("rowweave.worker.master.host", "127.0.0.1");
            workerProperties.setProperty("rowweave.worker.master.port", Integer.toString(masterPort));
            workerProperties.setProperty("rowweave.worker.slots", "8");
            workerProperties.setProperty("rowweave.test.userCodeMarker", marker.toString());
            worker = launch(workerProperties, "WORKER_READY=", 30_000L);

            Properties untrustedProperties = securityProperties(
                    untrustedIdentity, trustStore, authz);
            configureDeployment(untrustedProperties, setA);
            ControlSecurityConfig untrustedBase =
                    ControlSecurityConfig.fromProperties(untrustedProperties);
            String clusterSetId;
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         untrustedProperties, untrustedBase, Runtime.getRuntime().maxMemory());
                 ClusterJobClient client = new ClusterJobClient(
                         new FixedMasterLocator(new Endpoint("127.0.0.1", masterPort)), runtime)) {
                clusterSetId = runtime.deploymentSetId();
                Path target = temp.resolve("untrusted-certificate.tsv");
                assertThrows(RuntimeException.class,
                        () -> client.submit(signedArtifact(
                                runtime, target, "untrusted-certificate")));
                assertFalse(Files.exists(target));
            }
            assertFalse(Files.exists(marker),
                    "untrusted certificate must reject before application Source.iterator");

            Properties mismatchProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            configureDeployment(mismatchProperties, setB);
            ControlSecurityConfig mismatchBase =
                    ControlSecurityConfig.fromProperties(mismatchProperties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         mismatchProperties, mismatchBase, Runtime.getRuntime().maxMemory());
                 ClusterJobClient client = new ClusterJobClient(
                         new FixedMasterLocator(new Endpoint("127.0.0.1", masterPort)), runtime)) {
                assertFalse(clusterSetId.equals(runtime.deploymentSetId()),
                        "negative fixture must carry a genuinely different deployment set");
                Path target = temp.resolve("wrong-set.tsv");
                assertThrows(RuntimeException.class,
                        () -> client.submit(signedArtifact(runtime, target, "wrong-set")));
                assertFalse(Files.exists(target));
            }
            assertFalse(Files.exists(marker),
                    "deployment set mismatch must reject before application Source.iterator");
        } finally {
            close(worker);
            close(master);
            setA.restoreWritable();
            setB.restoreWritable();
        }
    }

    @Test
    void tamperedPreinstalledJarFailsProcessStartupBeforeReady(@TempDir Path temp)
            throws Exception {
        Identity masterIdentity = identity(temp, "tampered-master");
        Path trustStore = temp.resolve("tampered-control-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        Path authz = temp.resolve("tampered-control-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n")
                .getBytes(StandardCharsets.US_ASCII));
        DeploymentMaterial deployment = deployment(temp.resolve("tampered-set"), "original");
        try {
            byte[] tampered = Files.readAllBytes(deployment.jar);
            tampered[tampered.length / 2] ^= 1;
            Files.write(deployment.jar, tampered);
            Properties properties = securityProperties(masterIdentity, trustStore, authz);
            configureDeployment(properties, deployment);
            properties.setProperty("rowweave.master.enable", "true");
            properties.setProperty("rowweave.master.port", "0");

            String output = launchExpectFailure(properties, 15_000L);
            assertTrue(output.contains("DEPLOYMENT_DIGEST_MISMATCH"), output);
            assertFalse(output.contains("MASTER_PORT="), output);
        } finally {
            deployment.restoreWritable();
        }
    }

    private static Properties securityProperties(
            Identity identity, Path trustStore, Path authz) {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.PROFILE_PROPERTY, "MTLS");
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty(ControlTlsContext.KEY_STORE_PROPERTY, identity.keyStore.toString());
        properties.setProperty(ControlTlsContext.KEY_STORE_PASSWORD_PROPERTY, PASSWORD);
        properties.setProperty(ControlTlsContext.TRUST_STORE_PROPERTY, trustStore.toString());
        properties.setProperty(ControlTlsContext.TRUST_STORE_PASSWORD_PROPERTY, PASSWORD);
        properties.setProperty(ControlTlsContext.AUTHZ_FILE_PROPERTY, authz.toString());
        return properties;
    }

    private static void configureDeployment(
            Properties properties, DeploymentMaterial deployment) {
        properties.setProperty(DeploymentRuntime.DEPLOYMENT_COUNT_PROPERTY, "1");
        properties.setProperty(DeploymentRuntime.MANIFEST_PROPERTY_PREFIX + "0",
                deployment.manifest.toString());
        properties.setProperty(DeploymentRuntime.SIGNER_COUNT_PROPERTY, "1");
        properties.setProperty(DeploymentRuntime.SIGNER_ID_PROPERTY_PREFIX + "0", "release");
        properties.setProperty(DeploymentRuntime.SIGNER_PATH_PROPERTY_PREFIX + "0",
                deployment.signerKey.toString());
        properties.setProperty(DeploymentRuntime.TRUST_ROOT_PROPERTY,
                deployment.trustRoot.toString());
        properties.setProperty(DeploymentRuntimeConfig.MEMORY_BYTES_PROPERTY,
                Long.toString(8L * 1024L * 1024L));
    }

    private static String productionLogback() throws Exception {
        return new File(MtlsProcessClusterTest.class.getProtectionDomain().getCodeSource()
                .getLocation().toURI()).toPath().resolve("logback-process.xml").toString();
    }

    private static io.rowweave.artifact.PlanArtifact signedArtifact(
            DeploymentRuntime runtime, Path target, String definition) throws Exception {
        ClassLoader loader = runtime.applicationClassLoader("application");
        Source input = (Source) loader.loadClass("app.JobComponents$SignedInput")
                .getDeclaredConstructor().newInstance();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(input)),
                PlanNode.sink(META, 0, new FileSink(META, target.toString())).build()), 1);
        return LegacyPlanArtifactAdapter.wrap(
                definition, graph, runtime.objectTrustContext());
    }

    private static Child launch(Properties properties, String readyPrefix, long timeoutMillis)
            throws Exception {
        return launch(properties, readyPrefix, timeoutMillis, null);
    }

    private static Child launch(
            Properties properties, String readyPrefix, long timeoutMillis,
            Path additionalClasspath) throws Exception {
        List<String> command = command(properties, additionalClasspath);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        Child child = new Child(process);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8));
        Thread drain = new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (child) {
                        child.log.append(line).append('\n');
                        if (child.readyValue == null && line.startsWith(readyPrefix)) {
                            child.readyValue = line.substring(readyPrefix.length()).trim();
                            child.notifyAll();
                        }
                    }
                }
            } catch (Exception ignored) {
                // Diagnostic output already retained; process state below determines failure.
            } finally {
                synchronized (child) {
                    child.notifyAll();
                }
            }
        }, "mtls-process-output");
        drain.setDaemon(true);
        drain.start();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (child) {
            while (child.readyValue == null && process.isAlive()
                    && System.currentTimeMillis() < deadline) {
                child.wait(Math.max(1L, deadline - System.currentTimeMillis()));
            }
        }
        if (child.readyValue == null) {
            close(child);
            fail("process did not become ready for " + readyPrefix + "\n" + child.output());
        }
        return child;
    }

    private static String launchExpectFailure(Properties properties, long timeoutMillis)
            throws Exception {
        List<String> command = command(properties);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        Thread drain = new Thread(() -> {
            try {
                byte[] buffer = new byte[4096];
                for (int read; (read = process.getInputStream().read(buffer)) >= 0; ) {
                    output.write(buffer, 0, read);
                }
            } catch (Exception ignored) {
                // Process exit and retained output are the assertions.
            }
        }, "mtls-failed-process-output");
        drain.setDaemon(true);
        drain.start();
        if (!process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            fail("invalid deployment process did not exit");
        }
        drain.join(5_000L);
        assertTrue(process.exitValue() != 0, "invalid deployment process must fail startup");
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private static List<String> command(Properties properties) {
        return command(properties, null);
    }

    private static List<String> command(
            Properties properties, Path additionalClasspath) {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());
        command.add("-Xms128m");
        command.add("-Xmx4096m");
        command.add("-XX:MaxDirectMemorySize=512m");
        command.add("-XX:MaxMetaspaceSize=256m");
        command.add("-XX:ReservedCodeCacheSize=128m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=4");
        command.add("-Drowweave.process.memoryLimitBytes=12884901888");
        command.add("-cp");
        String classpath = System.getProperty("java.class.path");
        if (additionalClasspath != null) {
            classpath += File.pathSeparator
                    + additionalClasspath.toAbsolutePath();
        }
        command.add(classpath);
        List<String> names = new ArrayList<>(properties.stringPropertyNames());
        Collections.sort(names);
        for (String name : names) {
            command.add("-D" + name + '=' + properties.getProperty(name));
        }
        command.add("io.rowweave.cluster.ClusterMain");
        return command;
    }

    private static void close(Child child) {
        if (child == null) return;
        child.process.destroy();
        try {
            if (!child.process.waitFor(5, TimeUnit.SECONDS)) {
                child.process.destroyForcibly();
                child.process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            child.process.destroyForcibly();
        }
    }

    private static void writeRows(Path target, DataRow... rows) {
        FileSink output = new FileSink(META, target.toString());
        for (DataRow row : rows) output.collect(row);
        output.close();
    }

    private static void writeRows(Path target, int count) {
        FileSink output = new FileSink(META, target.toString());
        try {
            for (int i = 0; i < count; i++) {
                output.collect(row(i % 256, Integer.toString(i % 10_000)));
            }
        } finally {
            output.close();
        }
    }

    private static PlanGraph groupByGraph(Path source, int parallelism) {
        DataSchema grouped = ParallelGroupByAgg.of(META, "id")
                .sum("amount").getOutputSchema();
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                        new FileSource(META, source.toString()))),
                PlanNode.unary("groupBySum", grouped, 0)
                        .listParam("keys", Collections.singletonList("id"))
                        .param("measure", "amount").build(),
                PlanNode.sink(grouped, 1, new DiscardSink(grouped)).build()),
                parallelism);
    }

    private static PlanGraph reusedFanoutGraph(Path source) {
        DataSchema grouped = ParallelGroupByAgg.of(META, "id")
                .sum("amount").getOutputSchema();
        PlanNode first = PlanNode.unary("groupBySum", grouped, 0)
                .listParam("keys", Collections.singletonList("id"))
                .param("measure", "amount").build();
        PlanNode second = PlanNode.unary("groupBySum", grouped, 0)
                .listParam("keys", Collections.singletonList("id"))
                .param("measure", "amount").build();
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                        new FileSource(META, source.toString()))),
                first,
                second,
                PlanNode.sink(grouped, 1, new DiscardSink(grouped)).build(),
                PlanNode.sink(grouped, 2, new DiscardSink(grouped)).build()), 2);
    }

    private static PlanGraph joinGraph(
            Path left, Path right, Path target, String hint) {
        PlanNode.Builder join = PlanNode.binary("join", JOIN_META, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Collections.singletonList("id"))
                .listParam("rightKeys", Collections.singletonList("id"));
        if (hint != null) {
            join.param("joinHint", hint);
        }
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                        new FileSource(META, left.toString()))),
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                        new FileSource(META, right.toString()))),
                join.build(),
                PlanNode.sink(JOIN_META, 2,
                        new FileSink(JOIN_META, target.toString())).build()), 2);
    }

    private static String awaitCombinedLog(
            Child first, Child second, String token, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        String output;
        do {
            output = first.output() + second.output();
            if (output.contains(token)) {
                return output;
            }
            Thread.sleep(10L);
        } while (System.currentTimeMillis() < deadline);
        return output;
    }

    private static Child awaitEitherChildLog(
            Child first, Child second, String token, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        do {
            if (first.output().contains(token)) {
                return first;
            }
            if (second.output().contains(token)) {
                return second;
            }
            Thread.sleep(10L);
        } while ((first.process.isAlive() || second.process.isAlive())
                && System.currentTimeMillis() < deadline);
        fail("neither Worker entered real MTLS shuffle:\n"
                + first.output() + second.output());
        return null;
    }

    private static void restoreProperty(String name, String previous) {
        if (previous == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previous);
        }
    }

    private static List<String> linesContaining(String text, String token) {
        List<String> matches = new ArrayList<>();
        for (String line : text.split("\\R")) {
            if (line.contains(token)) {
                matches.add(line);
            }
        }
        return matches;
    }

    private static long metricValue(String metric, String name) {
        String prefix = name + '=';
        int start = metric.indexOf(prefix);
        if (start < 0) {
            fail("missing " + name + " in metric: " + metric);
        }
        start += prefix.length();
        int end = start;
        while (end < metric.length() && Character.isDigit(metric.charAt(end))) {
            end++;
        }
        if (end == start) {
            fail("invalid " + name + " in metric: " + metric);
        }
        return Long.parseLong(metric.substring(start, end));
    }

    private static List<String> renderRows(List<DataRow> rows) {
        List<String> rendered = new ArrayList<>();
        for (DataRow row : rows) {
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    value.append('|');
                }
                Object cell = row.get(i);
                value.append(cell instanceof java.math.BigDecimal
                        ? ((java.math.BigDecimal) cell).toPlainString()
                        : String.valueOf(cell));
            }
            rendered.add(value.toString());
        }
        Collections.sort(rendered);
        return rendered;
    }

    private static DataRow row(int id, String amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, new java.math.BigDecimal(amount));
        return row;
    }

    private static PlanNode operator(PlanNode node, String uid) {
        return node.withParam(PlanNode.OPERATOR_UID_PARAM, uid);
    }

    private static RunSubmission submission(String runId) {
        return new RunSubmission(
                runId, "attempt-1", "publication-1", 1, 0L, 10L);
    }

    private static DeploymentMaterial deployment(Path temp) throws Exception {
        return deployment(temp, "primary");
    }

    private static DataSchema cdcMeta() {
        return new DataSchema(
                new Field("id", DataType.LongType),
                new Field("op", DataType.StringType),
                new Field("position", DataType.LongType));
    }

    private static IncrementalRunSubmission jdbcIncrementalSubmission(
            Path output) throws Exception {
        DataSchema meta = cdcMeta();
        JdbcIncrementalSourceProfile profile = JdbcIncrementalSourceProfile
                .sequence(new JdbcIncrementalSourceProfile.Configuration(
                        meta, "process-endpoint", "process-credential",
                        "process-dataset", "process-query", repeat('a', 64),
                        JdbcIncrementalSourceProfile.SourceKind
                                .IMMUTABLE_CHANGE_TABLE,
                        true, true, true, true, 64, 30, 4096));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", digest("frontier-contract".getBytes(StandardCharsets.UTF_8)),
                Collections.singletonList(0), 1,
                Collections.singletonList(2),
                hexBytes(io.rowweave.commit.SchemaFingerprint.of(meta)),
                false, jdbcPartitionSetDigest(),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(meta)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source")
                .withIncrementalPlanDescriptor(descriptor);
        PlanNode sink = PlanNode.sink(meta, 0,
                        new ManifestFileSink(meta, output.toString(), "changes")
                                .toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "sink").build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, sink), 2);
        io.rowweave.plan.ObjectTrustContext trust =
                io.rowweave.plan.ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "process-jdbc-cdc", graph, trust, ignored -> { });
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                definition.definitionId(), graph, trust, ignored -> { });
        return IncrementalRunSubmission.normal(
                "process-feed", "process-trigger", IncrementalRunMode.SNAPSHOT_V1,
                "process-attempt", "process-publication", 2, 0L, 10L,
                definition, artifact, profile.contract(), profile.sourceConfig());
    }

    private static byte[] jdbcPartitionSetDigest() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        writeWireText(out, "p0");
        writeWireText(out, repeat('b', 64));
        writeWireText(out, "p1");
        writeWireText(out, repeat('c', 64));
        out.flush();
        return digest(bytes.toByteArray());
    }

    private static void writeWireText(DataOutputStream out, String value)
            throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] digest(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] hexBytes(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static Path jdbcIncrementalAccessJar(Path temp) throws Exception {
        String source = "package fixture;"
                + "public final class ProcessJdbcAccess implements io.rowweave.incremental.JdbcIncrementalSourceAccess{"
                + "public boolean supports(io.rowweave.incremental.JdbcIncrementalSourceProfile p){return \"process-endpoint\".equals(p.endpointRef());}"
                + "private static String repeat(char c){char[] v=new char[64];java.util.Arrays.fill(v,c);return new String(v);}"
                + "public Capture capture(io.rowweave.incremental.JdbcIncrementalSourceProfile p,io.rowweave.incremental.IncrementalCaptureRequest r){return new Capture(io.rowweave.incremental.IncrementalFrontier.int64Sequence(2L),new io.rowweave.incremental.JdbcIncrementalSourceProfile.Snapshot(\"snapshot-ref\",repeat('d')));}"
                + "public java.util.List<io.rowweave.incremental.JdbcIncrementalSourceProfile.Partition> materialize(io.rowweave.incremental.JdbcIncrementalSourceProfile p,io.rowweave.incremental.CapturedTargetReceipt r,io.rowweave.incremental.JdbcIncrementalSourceProfile.Snapshot s){return java.util.Arrays.asList(new io.rowweave.incremental.JdbcIncrementalSourceProfile.Partition(\"p0\",repeat('b')),new io.rowweave.incremental.JdbcIncrementalSourceProfile.Partition(\"p1\",repeat('c')));}"
                + "public io.rowweave.SourceExecutor<io.rowweave.IncrementalSourceSplit> open(final io.rowweave.incremental.JdbcIncrementalSourceProfile p,final io.rowweave.IncrementalSourceSplit i,final io.rowweave.JdbcSnapshotReadRequest r){return new io.rowweave.SourceExecutor<io.rowweave.IncrementalSourceSplit>(i,i.getSchema()){private boolean emitted;public void open(){try{String runtime=java.lang.management.ManagementFactory.getRuntimeMXBean().getName().replace('@','_');java.nio.file.Files.write(java.nio.file.Paths.get(System.getProperty(\"rowweave.test.cdc.marker.dir\"),r.partitionRef()+\"-\"+runtime),new byte[]{1});}catch(java.io.IOException e){throw new RuntimeException(e);}}public boolean hasNext(){return !emitted;}public io.rowweave.DataRow next(){emitted=true;long id=\"p0\".equals(r.partitionRef())?1L:2L;io.rowweave.DataRow row=new io.rowweave.DataRow(3);row.set(0,Long.valueOf(id));row.set(1,\"UPSERT\");row.set(2,Long.valueOf(0L));return row;}public void close(){}};}"
                + "}";
        Map<String, byte[]> resources = new LinkedHashMap<>();
        resources.put(
                "META-INF/services/io.rowweave.incremental.JdbcIncrementalSourceAccess",
                "fixture.ProcessJdbcAccess\n".getBytes(StandardCharsets.UTF_8));
        byte[] jar = compiledJar(
                temp.resolve("cdc-access-compile"),
                Collections.singletonMap("fixture.ProcessJdbcAccess", source),
                "cdc-access", resources);
        Path path = temp.resolve("cdc-access.jar").toAbsolutePath();
        Files.write(path, jar);
        return path;
    }

    private static DeploymentMaterial deployment(Path temp, String variant) throws Exception {
        Files.createDirectories(temp);
        String source = "package app;"
                + "public final class JobComponents {"
                + " private static io.rowweave.DataRow row(int id,String amount){"
                + "  io.rowweave.DataRow row=new io.rowweave.DataRow(2);"
                + "  row.set(0,id);row.set(1,new java.math.BigDecimal(amount));return row;}"
                + " public static final class SignedInput implements io.rowweave.BatchSource {"
                + "  private static final long serialVersionUID=1L;"
                + "  public io.rowweave.DataSchema getSchema(){return new io.rowweave.DataSchema("
                + "   new io.rowweave.Field(\"id\",io.rowweave.DataType.IntegerType),"
                + "   new io.rowweave.Field(\"amount\",io.rowweave.DataType.BigDecimalType));}"
                + "  public java.util.Iterator<io.rowweave.DataRow> iterator(){"
                + "   String marker=System.getProperty(\"rowweave.test.userCodeMarker\");"
                + "   if(marker!=null){try{java.nio.file.Files.write(java.nio.file.Paths.get(marker),new byte[]{1});}"
                + "    catch(java.io.IOException failure){throw new RuntimeException(failure);}}"
                + "   return java.util.Arrays.asList("
                + "   row(1,\"10.00\"),row(2,\"20.00\"),row(3,\"30.00\")).iterator();}"
                + "  public io.rowweave.BatchSourceSplitDescriptor batchSourceSplit(){"
                + "   return new io.rowweave.BatchSourceSplitDescriptor(1,\"signed-input\",1,"
                + "    \"signed-input-split\",io.rowweave.commit.SchemaFingerprint.of(getSchema()),"
                + "    \"signed-input-snapshot-v1\","
                + "    io.rowweave.BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);"
                + "  }"
                + " }"
                + " public static final class SignedMap implements io.rowweave.MapFunction {"
                + "  private static final long serialVersionUID=1L;"
                + "  public io.rowweave.DataRow map(io.rowweave.DataRow in){"
                + "   io.rowweave.DataRow out=new io.rowweave.DataRow(2);out.set(0,in.get(0));"
                + "   out.set(1,((java.math.BigDecimal)in.get(1)).add(new java.math.BigDecimal(\"100.00\")));"
                + "   return out;}"
                + " }"
                + " public static io.rowweave.FilterFunction filter(){"
                + "  return row -> ((Integer)row.get(0)) >= 2;}"
                + " public static final class SignedObjectProvider implements io.rowweave.ObjectStorageSourceProvider {"
                + "  private static final long serialVersionUID=1L;"
                + "  public SignedObjectProvider(){}"
                + "  public String providerType(){return \"signed-file-fixture\";}"
                + "  public int providerVersion(){return 1;}"
                + "  public io.rowweave.ObjectStorageReadHandle open(io.rowweave.ObjectStorageReadRequest request){"
                + "   try{java.nio.file.Path path=java.nio.file.Paths.get(request.objectKey());"
                + "    return new SignedObjectHandle(request.versionId(),java.nio.file.Files.size(path),"
                + "     java.nio.file.Files.newInputStream(path));}"
                + "   catch(java.io.IOException failure){throw new RuntimeException(failure);}}"
                + " }"
                + " public static final class SignedObjectHandle implements io.rowweave.ObjectStorageReadHandle {"
                + "  private final String version;private final long length;private final java.io.InputStream stream;"
                + "  SignedObjectHandle(String version,long length,java.io.InputStream stream){"
                + "   this.version=version;this.length=length;this.stream=stream;}"
                + "  public String providerType(){return \"signed-file-fixture\";}"
                + "  public int providerVersion(){return 1;}"
                + "  public String versionId(){return version;}"
                + "  public long contentLength(){return length;}"
                + "  public java.io.InputStream stream(){return stream;}"
                + "  public void close(){try{stream.close();}catch(java.io.IOException failure){"
                + "   throw new RuntimeException(failure);}}"
                + " }"
                + " public static final class SignedJdbcProvider implements io.rowweave.JdbcSnapshotSourceProvider {"
                + "  private static final long serialVersionUID=1L;"
                + "  public SignedJdbcProvider(){}"
                + "  public String providerType(){return \"signed-jdbc-fixture\";}"
                + "  public int providerVersion(){return 1;}"
                + "  public io.rowweave.JdbcSnapshotReadHandle open(io.rowweave.JdbcSnapshotReadRequest request){"
                + "   try{javax.sql.rowset.CachedRowSet rows=javax.sql.rowset.RowSetProvider.newFactory().createCachedRowSet();"
                + "    javax.sql.rowset.RowSetMetaDataImpl meta=new javax.sql.rowset.RowSetMetaDataImpl();"
                + "    meta.setColumnCount(2);meta.setColumnName(1,\"id\");"
                + "    meta.setColumnType(1,java.sql.Types.INTEGER);meta.setColumnName(2,\"amount\");"
                + "    meta.setColumnType(2,java.sql.Types.DECIMAL);rows.setMetaData(meta);"
                + "    for(int id=6;id<=7;id++){rows.moveToInsertRow();rows.updateInt(1,id);"
                + "     rows.updateBigDecimal(2,new java.math.BigDecimal(id==6?\"60.00\":\"70.00\"));rows.insertRow();}"
                + "    rows.moveToCurrentRow();rows.beforeFirst();return new SignedJdbcHandle(request,rows);"
                + "   }catch(java.sql.SQLException failure){throw new RuntimeException(\"fixture open failed\");}}"
                + " }"
                + " public static final class SignedJdbcHandle implements io.rowweave.JdbcSnapshotReadHandle {"
                + "  private final io.rowweave.JdbcSnapshotReadRequest request;"
                + "  private final javax.sql.rowset.CachedRowSet rows;"
                + "  SignedJdbcHandle(io.rowweave.JdbcSnapshotReadRequest request,javax.sql.rowset.CachedRowSet rows){"
                + "   this.request=request;this.rows=rows;}"
                + "  public String providerType(){return \"signed-jdbc-fixture\";}"
                + "  public int providerVersion(){return 1;}"
                + "  public String queryDigest(){return request.queryDigest();}"
                + "  public String partitionDigest(){return request.partitionDigest();}"
                + "  public String snapshotId(){return request.snapshotId();}"
                + "  public String schemaFingerprint(){return io.rowweave.commit.SchemaFingerprint.of("
                + "   new io.rowweave.DataSchema(new io.rowweave.Field(\"id\",io.rowweave.DataType.IntegerType),"
                + "   new io.rowweave.Field(\"amount\",io.rowweave.DataType.BigDecimalType)));}"
                + "  public java.sql.ResultSet resultSet(){return rows;}"
                + "  public void close(){try{rows.close();}catch(java.sql.SQLException failure){"
                + "   throw new RuntimeException(\"fixture close failed\");}}"
                + " }"
                + " public static final class SignedOutput implements io.rowweave.Sink {"
                + "  private static final long serialVersionUID=1L;"
                + "  public io.rowweave.DataSchema getSchema(){return new SignedInput().getSchema();}"
                + "  public void collect(io.rowweave.DataRow row){}"
                + "  public boolean isWorkerWritable(){return true;}"
                + "  public boolean supportsCoordinatedCommit(){return true;}"
                + "  public void prepareCommit(){} public void commitPrepared(){}"
                + "  public void rollbackCommit(){} public void finalizeCommit(){}"
                + " }"
                + "}";
        byte[] jar = compiledJar(temp.resolve("signed-compile"),
                Collections.singletonMap("app.JobComponents", source), variant);
        Path jarPath = temp.resolve("application.jar").toAbsolutePath();
        Files.write(jarPath, jar);
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                "application", jar.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                "application", jarPath, jar.length, digest, "release", signature.sign());
        Path root = temp.resolve("deployment-trust").toAbsolutePath();
        Files.createDirectory(root);
        Path keyPath = root.resolve("release.der");
        Path manifestPath = root.resolve("application.rwdep");
        Files.write(keyPath, signer.getPublic().getEncoded());
        Files.write(manifestPath, DeploymentManifestCodec.encode(manifest));
        readOnly(keyPath, false);
        readOnly(manifestPath, false);
        readOnly(root, true);
        return new DeploymentMaterial(root, keyPath, manifestPath, jarPath);
    }

    private static byte[] compiledJar(
            Path directory, Map<String, String> sources, String variant)
            throws Exception {
        return compiledJar(directory, sources, variant,
                Collections.emptyMap());
    }

    private static byte[] compiledJar(
            Path directory, Map<String, String> sources, String variant,
            Map<String, byte[]> resources) throws Exception {
        Path sourceRoot = directory.resolve("src");
        Path classes = directory.resolve("classes");
        Files.createDirectories(sourceRoot);
        Files.createDirectories(classes);
        List<String> arguments = new ArrayList<>();
        Collections.addAll(arguments, "-source", "8", "-target", "8", "-classpath",
                System.getProperty("java.class.path"), "-d", classes.toString());
        for (Map.Entry<String, String> entry : sources.entrySet()) {
            Path file = sourceRoot.resolve(entry.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.write(file, entry.getValue().getBytes(StandardCharsets.UTF_8));
            arguments.add(file.toString());
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertTrue(compiler != null, "tests require a JDK compiler");
        assertEquals(0, compiler.run(null, null, null, arguments.toArray(new String[0])));
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    entries.put(classes.relativize(file).toString().replace('\\', '/'),
                            Files.readAllBytes(file));
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (JarOutputStream output = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry item = new JarEntry(entry.getKey());
                item.setTime(0L);
                output.putNextEntry(item);
                output.write(entry.getValue());
                output.closeEntry();
            }
            JarEntry variantEntry = new JarEntry("deployment-variant.txt");
            variantEntry.setTime(0L);
            output.putNextEntry(variantEntry);
            output.write(variant.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
            for (Map.Entry<String, byte[]> entry : resources.entrySet()) {
                JarEntry resource = new JarEntry(entry.getKey());
                resource.setTime(0L);
                output.putNextEntry(resource);
                output.write(entry.getValue());
                output.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static String sha256Hex(byte[] value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
        char[] output = new char[digest.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < digest.length; i++) {
            output[i * 2] = digits[(digest[i] >>> 4) & 0xf];
            output[i * 2 + 1] = digits[digest[i] & 0xf];
        }
        return new String(output);
    }

    private static void readOnly(Path target, boolean directory) throws Exception {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ));
    }

    private static void writable(Path target, boolean directory) throws Exception {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }

    private static Identity identity(Path temp, String name) throws Exception {
        Path keyStore = temp.resolve(name + ".p12");
        Path certificate = temp.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        return new Identity(keyStore, certificate, ControlTlsContext.fingerprint(leaf));
    }

    private static void importCertificate(Path trustStore, String alias, Path certificate)
            throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.keytoolCommand(arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) bytes.write(buffer, 0, read);
        return bytes.toByteArray();
    }

    private static final class Identity {
        private final Path keyStore;
        private final Path certificate;
        private final String fingerprint;

        private Identity(Path keyStore, Path certificate, String fingerprint) {
            this.keyStore = keyStore;
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }

    private static final class DeploymentMaterial {
        private final Path trustRoot;
        private final Path signerKey;
        private final Path manifest;
        private final Path jar;

        private DeploymentMaterial(Path trustRoot, Path signerKey, Path manifest, Path jar) {
            this.trustRoot = trustRoot;
            this.signerKey = signerKey;
            this.manifest = manifest;
            this.jar = jar;
        }

        private void restoreWritable() throws Exception {
            writable(trustRoot, true);
            writable(signerKey, false);
            writable(manifest, false);
        }
    }

    private static final class Child {
        private final Process process;
        private final StringBuilder log = new StringBuilder();
        private String readyValue;

        private Child(Process process) {
            this.process = process;
        }

        private String output() {
            synchronized (this) {
                return log.toString();
            }
        }
    }
}

