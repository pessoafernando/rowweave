package io.rowweave.cluster;

import app.mapfixture.MapPartitionFunctionProvider;
import app.mapfixture.MapPartitionSinkProvider;
import app.mapfixture.MapPartitionSourceProvider;
import app.mapfixture.MapPartitionTestInput;
import app.mapfixture.PrefixingMapPartition;
import app.mapfixture.RecordingMapPartitionOutput;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.MapPartitionFunction;
import io.rowweave.MapPartitionEmissionLimit;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.ControlMessageId;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ProtocolHelloConfig;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolHelloV1Codec;
import io.rowweave.cluster.rpc.ProtocolRevisionRange;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentManifestCodec;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.DeploymentRuntimeConfig;
import io.rowweave.deployment.PortableFunctionProviderRegistry;
import io.rowweave.deployment.PortableRuntimeBindingRegistry;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StageView;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableConnectorProviderContractCodec;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableFunctionProviderContract;
import io.rowweave.portable.PortableFunctionProviderContractCodec;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** signed provider publication, negotiated placement, and real M+2W execution. */
class PortableMapPartitionClusterProductTest {
    private static final DataSchema LEGACY_META = new DataSchema(
            new Field("value", DataType.LongType));

    @Test
    void preV1WorkersCannotClaimPostBaselinePortableCapabilities() {
        InProcessRegistry registry = new InProcessRegistry();
        registry.register(new RegistryWorker(
                "legacy-worker", "127.0.0.1", 19_901, 4, -1,
                "legacy-incarnation", null, null, null));
        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            assertEquals(1, schedulableWorkers(master,
                    Collections.singletonList(
                            PortableBatchPlan.BATCH_CAPABILITY)).size(),
                    "the frozen pre-v1 Batch baseline remains compatible");
            assertTrue(schedulableWorkers(master, Arrays.asList(
                    PortableBatchPlan.BATCH_CAPABILITY,
                    PortableBatchPlan.MAP_PARTITION_CAPABILITY)).isEmpty(),
                    "a Worker without negotiated capability evidence must not receive opcode 67");
        }
    }

    @Test
    void signedProviderRunsTwoPhysicalPartitionsOnTwoCapableWorkers(
            @TempDir Path temp) throws Exception {
        System.clearProperty(PrefixingMapPartition.INVOCATIONS_PROPERTY);
        System.clearProperty(RecordingMapPartitionOutput.ROWS_PROPERTY);
        PublishedDeployment deployment = publish(temp);
        try {
            Properties capableProperties = deployment.properties(hello(
                    Arrays.asList(PortableBatchPlan.BATCH_CAPABILITY,
                            PortableBatchPlan.MAP_PARTITION_CAPABILITY)));
            Properties oldProperties = deployment.properties(hello(
                    Arrays.asList(PortableBatchPlan.BATCH_CAPABILITY,
                            PortableBatchPlan.MAP_PARTITION_CAPABILITY),
                    Collections.singletonList(7)));
            ControlSecurityConfig security =
                    ControlSecurityConfig.fromProperties(capableProperties);
            InProcessRegistry registry = new InProcessRegistry();
            InMemoryJobMonitor monitor = new InMemoryJobMonitor(8);
            try (DeploymentRuntime capableRuntime = DeploymentRuntime.load(
                         capableProperties, security,
                         Runtime.getRuntime().maxMemory(), null);
                 DeploymentRuntime oldRuntime = DeploymentRuntime.load(
                         oldProperties, security,
                         Runtime.getRuntime().maxMemory(), null);
                 MasterServer master = new MasterServer(
                         5_000L, 0, registry, monitor,
                         registry.artifactRepository(), capableRuntime)) {
                master.startFixedLeadershipServices();
                FixedMasterLocator locator = new FixedMasterLocator(
                        new Endpoint("127.0.0.1", master.port()));
                Files.createDirectories(temp.resolve("old-worker"));
                Files.createDirectories(temp.resolve("worker-0"));
                Files.createDirectories(temp.resolve("worker-1"));
                try (WorkerServer oldWorker = WorkerServer.newUnguarded(
                             "old-worker", 8, locator,
                             temp.resolve("old-worker").toFile(), oldRuntime);
                     WorkerServer worker0 = WorkerServer.newUnguarded(
                             "map-worker-0", 8, locator,
                             temp.resolve("worker-0").toFile(), capableRuntime);
                     WorkerServer worker1 = WorkerServer.newUnguarded(
                             "map-worker-1", 8, locator,
                             temp.resolve("worker-1").toFile(), capableRuntime);
                     RealClusterJobClient client = new RealClusterJobClient(
                             locator, capableRuntime)) {
                    RegistryWorker registeredOld = registry.worker("old-worker");
                    assertNotNull(registeredOld);
                    assertTrue(registeredOld.protocolHello()
                            .runtimeCapabilities().contains(
                                    PortableBatchPlan.MAP_PARTITION_CAPABILITY));
                    assertFalse(registeredOld.protocolHello()
                            .stageCommandVersions().contains(
                                    StageCommandCodecV2
                                            .FIXED_ROUTING_VERSION));

                    PlanArtifact artifact = PortablePlanArtifact.wrap(
                            "portable-map-partition-product",
                            plan(capableRuntime.deploymentSetId(),
                                    deployment.jarDigest));
                    ExecutionReport report = client.submit(artifact);
                    assertEquals(2L, report.rowsOut(),
                            "invocations=" + System.getProperty(
                                    PrefixingMapPartition.INVOCATIONS_PROPERTY)
                                    + ", rows=" + System.getProperty(
                                    RecordingMapPartitionOutput.ROWS_PROPERTY));
                    assertEquals("2", System.getProperty(
                            PrefixingMapPartition.INVOCATIONS_PROPERTY));
                    assertEquals(Arrays.asList(3L, 7L),
                            recordedRows());

                    assertEquals(1, monitor.jobs().size());
                    JobView job = monitor.jobs().get(0);
                    StageView mapStage = job.stages().stream()
                            .filter(stage -> stage.opChain().contains(
                                    "mapPartition"))
                            .findFirst().orElse(null);
                    assertNotNull(mapStage);
                    assertEquals(2, mapStage.parallelism());
                    Set<String> placements = new LinkedHashSet<>();
                    for (StageView.InstanceView instance
                            : mapStage.instances()) {
                        placements.add(instance.workerId);
                    }
                    assertEquals(new LinkedHashSet<>(Arrays.asList(
                            "map-worker-0", "map-worker-1")), placements);
                    assertFalse(placements.contains("old-worker"));
                }
            }
        } finally {
            deployment.restoreWritable();
            System.clearProperty(PrefixingMapPartition.INVOCATIONS_PROPERTY);
            System.clearProperty(RecordingMapPartitionOutput.ROWS_PROPERTY);
        }
    }

    @Test
    void rawAndLegacyGraphsExcludeWorkersWithoutFixedRoutingV9(
            @TempDir Path temp) throws Exception {
        PublishedDeployment deployment = publish(temp);
        try {
            Properties capableProperties = deployment.properties(hello(
                    Arrays.asList(PortableBatchPlan.BATCH_CAPABILITY,
                            PortableBatchPlan.MAP_PARTITION_CAPABILITY)));
            Properties staleProperties = deployment.properties(hello(
                    Arrays.asList(PortableBatchPlan.BATCH_CAPABILITY,
                            PortableBatchPlan.MAP_PARTITION_CAPABILITY),
                    Arrays.asList(7, 8)));
            ControlSecurityConfig security =
                    ControlSecurityConfig.fromProperties(capableProperties);
            InProcessRegistry registry = new InProcessRegistry();
            InMemoryJobMonitor monitor = new InMemoryJobMonitor(8);
            try (DeploymentRuntime capableRuntime = DeploymentRuntime.load(
                         capableProperties, security,
                         Runtime.getRuntime().maxMemory(), null);
                 DeploymentRuntime staleRuntime = DeploymentRuntime.load(
                         staleProperties, security,
                         Runtime.getRuntime().maxMemory(), null);
                 MasterServer master = new MasterServer(
                         5_000L, 0, registry, monitor,
                         registry.artifactRepository(), capableRuntime)) {
                master.startFixedLeadershipServices();
                FixedMasterLocator locator = new FixedMasterLocator(
                        new Endpoint("127.0.0.1", master.port()));
                Files.createDirectories(temp.resolve("legacy-stale"));
                Files.createDirectories(temp.resolve("legacy-capable-0"));
                Files.createDirectories(temp.resolve("legacy-capable-1"));
                try (WorkerServer stale = WorkerServer.newUnguarded(
                             "legacy-stale", 8, locator,
                             temp.resolve("legacy-stale").toFile(),
                             staleRuntime);
                     WorkerServer capable0 = WorkerServer.newUnguarded(
                             "legacy-capable-0", 8, locator,
                             temp.resolve("legacy-capable-0").toFile(),
                             capableRuntime);
                     WorkerServer capable1 = WorkerServer.newUnguarded(
                             "legacy-capable-1", 8, locator,
                             temp.resolve("legacy-capable-1").toFile(),
                             capableRuntime);
                     RealClusterJobClient client = new RealClusterJobClient(
                             locator, capableRuntime)) {
                    Path rawOutput = temp.resolve("raw-map-partition.tsv");
                    client.submit(legacyGraph(rawOutput));
                    assertEquals(Arrays.asList(3L, 7L),
                            legacyRows(rawOutput));

                    Path artifactOutput = temp.resolve(
                            "legacy-artifact-map-partition.tsv");
                    client.submit(LegacyPlanArtifactAdapter.wrap(
                            "legacy-map-partition-v9",
                            legacyGraph(artifactOutput)));
                    assertEquals(Arrays.asList(3L, 7L),
                            legacyRows(artifactOutput));

                    assertEquals(2, monitor.jobs().size());
                    for (JobView job : monitor.jobs()) {
                        StageView mapStage = job.stages().stream()
                                .filter(stage -> stage.opChain().contains(
                                        "mapPartition"))
                                .findFirst().orElse(null);
                        assertNotNull(mapStage);
                        Set<String> placements = new LinkedHashSet<>();
                        for (StageView.InstanceView instance
                                : mapStage.instances()) {
                            placements.add(instance.workerId);
                        }
                        assertEquals(new LinkedHashSet<>(Arrays.asList(
                                "legacy-capable-0",
                                "legacy-capable-1")), placements);
                        assertFalse(placements.contains("legacy-stale"));
                    }
                }
            }
        } finally {
            deployment.restoreWritable();
        }
    }

    @Test
    @Tag("multiprocess")
    @Timeout(90)
    void signedProviderPreservesPartitionsAcrossIndependentMasterAndWorkers(
            @TempDir Path temp) throws Exception {
        PublishedDeployment deployment = publish(temp);
        Path observations = temp.resolve("process-observations");
        Files.createDirectories(observations);
        Properties capable = deployment.properties(hello(Arrays.asList(
                PortableBatchPlan.BATCH_CAPABILITY,
                PortableBatchPlan.MAP_PARTITION_CAPABILITY)));
        Properties old = deployment.properties(hello(Arrays.asList(
                PortableBatchPlan.BATCH_CAPABILITY,
                PortableBatchPlan.MAP_PARTITION_CAPABILITY),
                Collections.singletonList(7)));
        RunningRole master = null;
        RunningRole oldWorker = null;
        RunningRole worker0 = null;
        RunningRole worker1 = null;
        try {
            master = launchRole(capable, Arrays.asList(
                    "-Drowweave.master.enable=true",
                    "-Drowweave.master.port=0"), "MASTER_PORT=");
            int masterPort = Integer.parseInt(master.readyValue);
            oldWorker = launchRole(old, workerArguments(
                    masterPort, "old"), "WORKER_READY=");
            worker0 = launchRole(capable, workerArguments(
                    masterPort, "capable-0"), "WORKER_READY=");
            worker1 = launchRole(capable, workerArguments(
                    masterPort, "capable-1"), "WORKER_READY=");

            ControlSecurityConfig security =
                    ControlSecurityConfig.fromProperties(capable);
            try (DeploymentRuntime clientRuntime = DeploymentRuntime.load(
                         capable, security, Runtime.getRuntime().maxMemory());
                 RealClusterJobClient client = new RealClusterJobClient(
                         new FixedMasterLocator(new Endpoint(
                                 "127.0.0.1", masterPort)), clientRuntime)) {
                ExecutionReport report = client.submit(
                        PortablePlanArtifact.wrap(
                                "portable-map-partition-real-process",
                                plan(clientRuntime.deploymentSetId(),
                                        deployment.jarDigest, observations)));
                assertEquals(2L, report.rowsOut());
            }

            assertEquals(Arrays.asList(3L, 7L),
                    observedProcessRows(observations));
            assertEquals(Arrays.asList("capable-0", "capable-1"),
                    observedInvocationWorkers(observations));
        } catch (Exception | AssertionError failure) {
            failure.addSuppressed(new AssertionError(processLogs(
                    master, oldWorker, worker0, worker1)));
            throw failure;
        } finally {
            closeRole(worker1);
            closeRole(worker0);
            closeRole(oldWorker);
            closeRole(master);
            deployment.restoreWritable();
        }
    }

    private static List<String> workerArguments(
            int masterPort, String label) {
        return Arrays.asList(
                "-Drowweave.worker.enable=true",
                "-Drowweave.worker.master.port=" + masterPort,
                "-Drowweave.worker.slots=8",
                "-Drowweave.worker.host=127.0.0.1",
                "-Drowweave.test.map-partition.worker.label=" + label);
    }

    private static RunningRole launchRole(
            Properties properties, List<String> roleArguments,
            String readyPrefix) throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.baseCommand();
        List<String> arguments = new ArrayList<>();
        List<String> names = new ArrayList<>(
                properties.stringPropertyNames());
        Collections.sort(names);
        for (String name : names) {
            arguments.add("-D" + name + "=" + properties.getProperty(name));
        }
        arguments.addAll(roleArguments);
        io.rowweave.test.ForkedJvm.appendJvmArguments(command, arguments);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ClusterMain.class.getName());
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).start();
        RunningRole role = new RunningRole(process);
        Thread reader = new Thread(() -> {
            try (BufferedReader lines = new BufferedReader(
                    new InputStreamReader(process.getInputStream(),
                            StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    synchronized (role) {
                        role.log.add(line);
                        if (role.readyValue == null
                                && line.startsWith(readyPrefix)) {
                            role.readyValue = line.substring(
                                    readyPrefix.length()).trim();
                            role.notifyAll();
                        }
                    }
                }
            } catch (Exception failure) {
                synchronized (role) {
                    role.log.add("reader failure: " + failure);
                    role.notifyAll();
                }
            }
        }, "map-partition-role-reader");
        reader.setDaemon(true);
        reader.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        synchronized (role) {
            while (role.readyValue == null && process.isAlive()
                    && System.nanoTime() < deadline) {
                TimeUnit.NANOSECONDS.timedWait(role,
                        Math.max(1L, deadline - System.nanoTime()));
            }
        }
        if (role.readyValue == null) {
            closeRole(role);
            throw new AssertionError("cluster role did not become ready: "
                    + role.log);
        }
        return role;
    }

    private static void closeRole(RunningRole role) {
        if (role == null) return;
        role.process.destroy();
        try {
            if (!role.process.waitFor(5, TimeUnit.SECONDS)) {
                role.process.destroyForcibly();
                role.process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            role.process.destroyForcibly();
        }
    }

    private static String processLogs(RunningRole... roles) {
        StringBuilder logs = new StringBuilder();
        for (RunningRole role : roles) {
            if (role == null) continue;
            synchronized (role) {
                logs.append(role.log).append('\n');
            }
        }
        return logs.toString();
    }

    private static List<Long> observedProcessRows(Path root)
            throws Exception {
        List<Long> rows = new ArrayList<>();
        try (java.util.stream.Stream<Path> files = Files.list(root)) {
            for (Path file : (Iterable<Path>) files
                    .filter(path -> path.getFileName().toString()
                            .startsWith("rows-"))::iterator) {
                for (String line : Files.readAllLines(
                        file, StandardCharsets.UTF_8)) {
                    rows.add(Long.parseLong(line));
                }
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> observedInvocationWorkers(Path root)
            throws Exception {
        List<String> workers = new ArrayList<>();
        try (java.util.stream.Stream<Path> files = Files.list(root)) {
            for (Path file : (Iterable<Path>) files
                    .filter(path -> path.getFileName().toString()
                            .startsWith("invocation-"))::iterator) {
                workers.add(new String(Files.readAllBytes(file),
                        StandardCharsets.UTF_8));
            }
        }
        Collections.sort(workers);
        return workers;
    }

    private static final class RunningRole {
        private final Process process;
        private final List<String> log = new ArrayList<>();
        private String readyValue;

        private RunningRole(Process process) {
            this.process = process;
        }
    }

    private static PortableBatchPlan plan(
            String deploymentSetId, String jarDigest) throws Exception {
        return plan(deploymentSetId, jarDigest, null);
    }

    private static PortableBatchPlan plan(
            String deploymentSetId, String jarDigest,
            Path observationRoot) throws Exception {
        PortableRowSchema schema = new PortableRowSchema(
                Collections.singletonList(new PortableField(
                        "id", "id", "id", PortableDataType.LONG, false)));
        String runtimeSchema = SchemaFingerprint.of(
                PortablePlanCompiler.toDataSchema(schema));
        PortableConnectorProviderContract sourceContract = sourceContract();
        PortableConnectorProviderContract sinkContract = sinkContract();
        List<byte[]> splits = Arrays.asList(
                split("split-a", runtimeSchema),
                split("split-b", runtimeSchema));
        PortablePlanNode source = new PortablePlanNode(
                "source", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSourceBinding(sourceContract.providerKey(),
                        PortableConnectorProviderContractCodec.digest(
                                sourceContract), new byte[]{1}, splits));
        byte[] functionConfig = observationRoot == null
                ? new byte[]{7}
                : ("observe:" + observationRoot.toAbsolutePath())
                .getBytes(StandardCharsets.UTF_8);
        FunctionDeploymentReference function =
                new FunctionDeploymentReference(
                        deploymentSetId, "map-partition-fixture", jarDigest,
                        PrefixingMapPartition.class.getName(),
                        FunctionAbi.MAP_PARTITION_V1,
                        "rowweave.opaque-bytes.v1", hex(sha256(functionConfig)),
                        functionConfig,
                        new FunctionResourceProfile(2048L, 0L, 1));
        Map<String, String> limits = new LinkedHashMap<>();
        limits.put("maxOutputRowsPerPartition", "10");
        limits.put("maxOutputBytesPerPartition", "4096");
        PortablePlanNode map = new PortablePlanNode(
                "partition-map", PortableOperatorCode.FUNCTION_MAP_PARTITION,
                Collections.singletonList(source.operatorId()),
                Collections.singletonList(schema.fingerprint()), schema,
                limits, Collections.emptyMap(), function);
        byte[] sinkDescriptor = BatchSinkDescriptorCodec.encode(
                new BatchSinkDescriptor(
                        1, "map-partition-sink", 1, runtimeSchema,
                        "recording", "test",
                        BatchSinkDescriptor.CapabilityProfile
                                .SIDE_EFFECT_FREE_V1));
        byte[] sinkConfig = observationRoot == null
                ? new byte[]{4} : functionConfig;
        PortablePlanNode sink = new PortablePlanNode(
                "sink", PortableOperatorCode.SINK,
                Collections.singletonList(map.operatorId()),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSinkBinding(sinkContract.providerKey(),
                        PortableConnectorProviderContractCodec.digest(
                                sinkContract), sinkDescriptor, sinkConfig));
        return new PortableBatchPlan(
                1, 2, Collections.singletonList(sink.operatorId()),
                Arrays.asList(source, map, sink), Arrays.asList(
                        PortableBatchPlan.BATCH_CAPABILITY,
                        PortableBatchPlan.MAP_PARTITION_CAPABILITY));
    }

    private static byte[] split(String id, String schema) {
        return BatchSourceSplitDescriptorCodec.encode(
                new BatchSourceSplitDescriptor(
                        1, "map-partition-source", 1, id, schema,
                        "snapshot-1",
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .EXACT_ORDERED_FROM_START));
    }

    private static PortableConnectorProviderContract sourceContract() {
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SOURCE,
                "map-partition-source-v1", "rowweave.opaque-bytes.v1", 16,
                BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES, 4);
    }

    private static PortableConnectorProviderContract sinkContract() {
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SINK,
                "map-partition-sink-v1", "rowweave.opaque-bytes.v1", 4096,
                BatchSinkDescriptorCodec.MAX_FRAME_BYTES, 1);
    }

    private static PortableFunctionProviderContract functionContract() {
        return new PortableFunctionProviderContract(
                1, PrefixingMapPartition.class.getName(),
                FunctionAbi.MAP_PARTITION_V1, "rowweave.opaque-bytes.v1", 4096,
                new FunctionResourceProfile(4096L, 0L, 1));
    }

    private static List<Long> recordedRows() {
        String value = System.getProperty(
                RecordingMapPartitionOutput.ROWS_PROPERTY, "");
        List<Long> rows = new ArrayList<>();
        if (!value.isEmpty()) {
            for (String item : value.split(",")) {
                rows.add(Long.parseLong(item));
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static PlanGraph legacyGraph(Path output) {
        ExecutionEnvironment session = new ExecutionEnvironment(
                "legacy-map-partition-v9",
                "legacy-map-partition-v9",
                new io.rowweave.local.LocalJobClient(false));
        session.setParallelism(2);
        DataSet input = session.fromSource(
                new CollectionSource(LEGACY_META, "legacy-a",
                        Arrays.asList(legacyRow(1L), legacyRow(2L))),
                new CollectionSource(LEGACY_META, "legacy-b",
                        Arrays.asList(legacyRow(3L), legacyRow(4L))));
        input.mapPartition(new LegacySumFunction(),
                        new MapPartitionEmissionLimit(1L, 1024L))
                .sinkTo(new FileSink(
                        LEGACY_META, output.toString()));
        return session.toPlanGraph();
    }

    private static List<Long> legacyRows(Path output) {
        List<Long> values = new ArrayList<>();
        for (DataRow row : FileSink.read(
                LEGACY_META, output.toString())) {
            values.add(row.getLong(0));
        }
        Collections.sort(values);
        return values;
    }

    private static DataRow legacyRow(long value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    public static final class LegacySumFunction
            implements MapPartitionFunction {
        private static final long serialVersionUID = 1L;
        @Override public void mapPartition(
                PartitionDataRowCursor rows, Collector collector) {
            long sum = 0L;
            DataRow current;
            while ((current = rows.next()) != null) {
                sum = Math.addExact(sum, current.getLong(0));
            }
            collector.collect(legacyRow(sum));
        }
        @Override public DataSchema getResultSchema() {
            return LEGACY_META;
        }
    }

    private static PublishedDeployment publish(Path temp) throws Exception {
        byte[] jar = providerJar();
        Path jarPath = temp.resolve("map-partition-fixture.jar")
                .toAbsolutePath();
        Files.write(jarPath, jar);
        KeyPair signer = keyPair();
        DeploymentManifest deployment = manifest(
                "map-partition-fixture", jarPath, jar, signer);
        Path root = temp.resolve("trust").toAbsolutePath();
        Files.createDirectories(root);
        Path key = root.resolve("release.der");
        Path manifest = root.resolve("map-partition.rwdep");
        Files.write(key, signer.getPublic().getEncoded());
        Files.write(manifest, DeploymentManifestCodec.encode(deployment));
        readOnly(key, false);
        readOnly(manifest, false);
        readOnly(root, true);
        return new PublishedDeployment(root, key, manifest,
                hex(sha256(jar)));
    }

    private static byte[] providerJar() throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        addClass(entries, MapPartitionSourceProvider.class);
        addClass(entries, MapPartitionTestInput.class);
        addClass(entries, MapPartitionFunctionProvider.class);
        addClass(entries, PrefixingMapPartition.class);
        addClass(entries, MapPartitionSinkProvider.class);
        addClass(entries, RecordingMapPartitionOutput.class);
        entries.put(PortableRuntimeBindingRegistry.SOURCE_SERVICE_RESOURCE,
                (MapPartitionSourceProvider.class.getName()
                        + "=META-INF/rowweave/map-source.contract\n")
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(PortableRuntimeBindingRegistry.SINK_SERVICE_RESOURCE,
                (MapPartitionSinkProvider.class.getName()
                        + "=META-INF/rowweave/map-sink.contract\n")
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(PortableFunctionProviderRegistry.SERVICE_RESOURCE,
                (MapPartitionFunctionProvider.class.getName()
                        + "=META-INF/rowweave/map-function.contract\n")
                        .getBytes(StandardCharsets.UTF_8));
        entries.put("META-INF/rowweave/map-source.contract",
                PortableConnectorProviderContractCodec.encode(
                        sourceContract()));
        entries.put("META-INF/rowweave/map-sink.contract",
                PortableConnectorProviderContractCodec.encode(
                        sinkContract()));
        entries.put("META-INF/rowweave/map-function.contract",
                PortableFunctionProviderContractCodec.encode(
                        functionContract()));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream output = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry item = new JarEntry(entry.getKey());
                item.setTime(0L);
                output.putNextEntry(item);
                output.write(entry.getValue());
                output.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static void addClass(Map<String, byte[]> entries, Class<?> type)
            throws Exception {
        String name = type.getName().replace('.', '/') + ".class";
        try (InputStream input = type.getClassLoader()
                .getResourceAsStream(name)) {
            if (input == null) {
                throw new AssertionError("missing test class " + name);
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) bytes.write(buffer, 0, read);
            }
            entries.put(name, bytes.toByteArray());
        }
    }

    private static DeploymentManifest manifest(
            String id, Path source, byte[] jar, KeyPair signer)
            throws Exception {
        byte[] digest = sha256(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                id, jar.length, digest));
        return new DeploymentManifest(id, source, jar.length, digest,
                "release", signature.sign());
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        return generator.generateKeyPair();
    }

    private static ProtocolHelloV1 hello(List<String> capabilities) {
        List<Integer> stageCommands = capabilities.contains(
                PortableBatchPlan.MAP_PARTITION_CAPABILITY)
                ? Arrays.asList(7, 8,
                        StageCommandCodecV2.FIXED_ROUTING_VERSION)
                : Collections.singletonList(7);
        return hello(capabilities, stageCommands);
    }

    private static ProtocolHelloV1 hello(
            List<String> capabilities,
            List<Integer> stageCommands) {
        List<Integer> messages = new ArrayList<>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            messages.add(ControlMessageId.wireId(type));
        }
        return new ProtocolHelloV1(repeat('a', 64), 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(0, 1), messages,
                Collections.singleton(1), stageCommands,
                Collections.singleton(1), Collections.singleton(1),
                capabilities, Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(),
                80 * 1024 * 1024, 128L * 1024L * 1024L);
    }

    @SuppressWarnings("unchecked")
    private static List<RegistryWorker> schedulableWorkers(
            MasterServer master, java.util.Collection<String> capabilities) {
        try {
            Method method = MasterServer.class.getDeclaredMethod(
                    "schedulableWorkers", java.util.Collection.class);
            method.setAccessible(true);
            return (List<RegistryWorker>) method.invoke(master, capabilities);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    private static byte[] sha256(byte[] bytes) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void readOnly(Path target, boolean directory)
            throws Exception {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ));
    }

    private static void writable(Path target, boolean directory)
            throws Exception {
        if (!Files.exists(target)) return;
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE));
    }

    private static final class PublishedDeployment {
        private final Path root;
        private final Path key;
        private final Path manifest;
        private final String jarDigest;

        private PublishedDeployment(
                Path root, Path key, Path manifest, String jarDigest) {
            this.root = root;
            this.key = key;
            this.manifest = manifest;
            this.jarDigest = jarDigest;
        }

        private Properties properties(ProtocolHelloV1 hello) {
            Properties properties = new Properties();
            properties.setProperty(
                    DeploymentRuntime.DEPLOYMENT_COUNT_PROPERTY, "1");
            properties.setProperty(
                    DeploymentRuntime.MANIFEST_PROPERTY_PREFIX + "0",
                    manifest.toString());
            properties.setProperty(
                    DeploymentRuntime.SIGNER_COUNT_PROPERTY, "1");
            properties.setProperty(
                    DeploymentRuntime.SIGNER_ID_PROPERTY_PREFIX + "0",
                    "release");
            properties.setProperty(
                    DeploymentRuntime.SIGNER_PATH_PROPERTY_PREFIX + "0",
                    key.toString());
            properties.setProperty(
                    DeploymentRuntime.TRUST_ROOT_PROPERTY, root.toString());
            properties.setProperty(
                    DeploymentRuntimeConfig.MEMORY_BYTES_PROPERTY,
                    Long.toString(16L * 1024L * 1024L));
            properties.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                    hex(ProtocolHelloV1Codec.encode(hello)));
            return properties;
        }

        private void restoreWritable() throws Exception {
            writable(root, true);
            writable(key, false);
            writable(manifest, false);
        }
    }
}
