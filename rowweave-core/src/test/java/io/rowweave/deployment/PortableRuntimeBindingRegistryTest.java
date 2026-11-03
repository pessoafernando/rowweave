package io.rowweave.deployment;

import io.rowweave.BatchSink;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PlanArtifactValidationException;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.FixedMasterLocator;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.PreparedStageCommand;
import io.rowweave.cluster.RealClusterJobClient;
import io.rowweave.cluster.StageCommand;
import io.rowweave.cluster.StageCommandCodecV2;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.local.LocalJobClient;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableConnectorProviderContractCodec;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortablePlanException;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortableRuntimeBindingRegistryTest {
    private static final String PROBE =
            "rowweave.test.portable.connector.provider.loaded";
    private static final String[] CALLBACK_PROBES = {
            "rowweave.test.portable.source.contract.thread",
            "rowweave.test.portable.source.meta.thread",
            "rowweave.test.portable.source.descriptor.thread",
            "rowweave.test.portable.sink.contract.thread",
            "rowweave.test.portable.sink.meta.thread",
            "rowweave.test.portable.sink.descriptor.thread",
            "rowweave.test.portable.sink.workerWritable.thread",
            "rowweave.test.portable.sink.destination.thread",
            "rowweave.test.portable.sink.coordinated.thread"
    };

    @Test
    void signedContractsCompileWithoutStaticRoleLoadingProviders(
            @TempDir Path temp) throws Exception {
        System.clearProperty(PROBE);
        clearCallbackProbes();
        PortableConnectorProviderContract sourceContract = sourceContract();
        PortableConnectorProviderContract sinkContract = sinkContract();
        byte[] jar = providerJar(sourceContract, sinkContract);
        KeyPair signer = keyPair();
        Path path = temp.resolve("connectors.jar").toAbsolutePath();
        Files.write(path, jar);
        DeploymentMemoryBudget budget =
                DeploymentMemoryBudget.forTesting(jar.length + 500_000L);
        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest(path, jar, signer)),
                Collections.singletonMap("key", signer.getPublic()), budget)) {
            PortableBatchPlan plan = plan(sourceContract, sinkContract);
            PortableRuntimeBindingRegistry metadata =
                    PortableRuntimeBindingRegistry.scan(
                            deployments,
                            PortableRuntimeBindingRegistry.Role.STATIC_ONLY,
                            null, 0L);
            PortableSourceBinding source = sourceBinding(plan);
            assertPortableCode("SOURCE_DESCRIPTOR_INVALID", () ->
                    metadata.verifySource(new PortableSourceBinding(
                            source.providerKey(), source.providerContractDigest(),
                            source.configurationFrame(),
                            Collections.singletonList(new byte[]{2}))));
            PortableSinkBinding sink = sinkBinding(plan);
            assertPortableCode("SINK_DESCRIPTOR_INVALID", () ->
                    metadata.verifySink(new PortableSinkBinding(
                            sink.providerKey(), sink.providerContractDigest(),
                            new byte[]{3}, sink.configurationFrame())));
            metadata.verifySource(sourceBinding(plan));
            metadata.verifySink(sinkBinding(plan));
            assertEquals(null, System.getProperty(PROBE));
            assertThrows(IllegalStateException.class,
                    () -> metadata.resolveSource(sourceBinding(plan)));

            PlanArtifact unboundArtifact = PortablePlanArtifact.wrap(
                    "connector-plan", plan);
            PlanArtifact artifact = PlanArtifactDeploymentBinding.bind(
                    unboundArtifact, deployments.deploymentSetId());
            assertEquals(plan, PortablePlanArtifact.preflightValidated(
                    artifact, metadata, ignored -> { }));
            assertEquals(null, System.getProperty(PROBE));

            PortableRuntimeBindingRegistry worker =
                    PortableRuntimeBindingRegistry.scan(
                            deployments,
                            PortableRuntimeBindingRegistry.Role.WORKER,
                            ProviderActionSupervisor.processWide(2), 5000L);
            PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                    StageCommandCodecV2.encode(
                            StageCommand.wholePortableArtifact(
                                    PlanArtifactCodec.encode(artifact))
                                    .withRunAttemptFence(new RunAttemptFence(
                                            "portable-run", "attempt-1", 1L)),
                            ObjectTrustContext.localTrusted(), ignored -> { }),
                    ignored -> { });
            prepared.preflightPortableArtifact(metadata, true, ignored -> { });
            assertEquals(null, System.getProperty(PROBE));
            StageCommand materialized = prepared.materializePortableArtifact(
                    worker, worker, ignored -> { });
            for (String callbackProbe : CALLBACK_PROBES) {
                String callbackThread = System.getProperty(callbackProbe);
                assertTrue(callbackThread != null
                                && callbackThread.startsWith(
                                "rowweave-provider-action-"),
                        callbackProbe + " escaped provider supervision: "
                                + callbackThread);
            }
            assertEquals(StageCommand.Role.WHOLE_GRAPH, materialized.role());
            assertEquals(2, materialized.fragment().nodeCount());
            assertEquals("yes", System.getProperty(PROBE));
            PlanArtifactValidationException unbound = assertThrows(
                    PlanArtifactValidationException.class,
                    () -> PortablePlanArtifact.compileValidated(
                            unboundArtifact, worker, worker, ignored -> { }));
            assertEquals("DEPLOYMENT_FEATURE_REQUIRED", unbound.code());
            PlanArtifact wrongArtifact = PlanArtifactDeploymentBinding.bind(
                    unboundArtifact,
                    "0000000000000000000000000000000000000000000000000000000000000000");
            PlanArtifactValidationException mismatch = assertThrows(
                    PlanArtifactValidationException.class,
                    () -> PortablePlanArtifact.compileValidated(
                            wrongArtifact, worker, worker, ignored -> { }));
            assertEquals("DEPLOYMENT_SET_MISMATCH", mismatch.code());
            PlanGraph graph = PortablePlanArtifact.compileValidated(
                    artifact, worker, worker, ignored -> { });
            assertEquals(2, graph.nodeCount());
            assertTrue(graph.node(0).inputs().get(0)
                    instanceof BatchSource);
            assertTrue(graph.node(1).output() instanceof BatchSink);
            assertEquals("yes", System.getProperty(PROBE));

            ExecutionReport report = new LocalJobClient(worker)
                    .submit(unboundArtifact);
            assertEquals(0L, report.rowsOut());

            try (DeploymentRuntime clusterRuntime =
                         DeploymentRuntime.forTesting(
                                 deployments, null, true)) {
                AtomicReference<String> workerIncarnation = new AtomicReference<>();
                AtomicReference<RpcMessage> terminalReport =
                        new AtomicReference<>();
                CountDownLatch terminal = new CountDownLatch(1);
                Files.createDirectories(temp.resolve("worker"));
                try (RpcServer master = new RpcServer(request -> {
                         if (request.type()
                                 == RpcMessage.Type.REGISTER_WORKER) {
                             workerIncarnation.set(request.incarnationId());
                         } else if (request.type()
                                 == RpcMessage.Type.STAGE_REPORT) {
                             terminalReport.set(request);
                             terminal.countDown();
                         }
                         return RpcMessage.ack();
                     });
                     WorkerServer workerServer = new WorkerServer(
                             "portable-worker", 4,
                             new FixedMasterLocator(new Endpoint(
                                     "127.0.0.1", master.port())),
                             temp.resolve("worker").toFile(),
                             clusterRuntime)) {
                    RunAttemptFence fence = new RunAttemptFence(
                            "portable-stage-run", "attempt-1", 1L);
                    byte[] command = StageCommandCodecV2.encode(
                            StageCommand.wholePortableArtifact(
                                    PlanArtifactCodec.encode(artifact))
                                    .withRunAttemptFence(fence),
                            ObjectTrustContext.localTrusted(), ignored -> { });
                    RpcMessage accepted = workerServer.handler().apply(
                            RpcMessage.assignStage(
                                    "portable-worker", 91L, command,
                                    workerIncarnation.get(), fence));
                    assertEquals(RpcMessage.Type.ACK, accepted.type());
                    assertTrue(terminal.await(5, TimeUnit.SECONDS));
                    assertTrue(terminalReport.get().success(),
                            terminalReport.get().errorMessage());
                }

                AtomicReference<PlanArtifact> submitted = new AtomicReference<>();
                AtomicReference<PlanArtifact> durableSubmitted =
                        new AtomicReference<>();
                try (RpcServer server = new RpcServer(request -> {
                         if (request.type()
                                 == RpcMessage.Type.SUBMIT_PLAN_ARTIFACT) {
                             submitted.set(PlanArtifactCodec.decode(
                                     request.planGraphBytes()));
                         } else if (request.type()
                                 == RpcMessage.Type.SUBMIT_RUN) {
                             RunSubmissionCodec.Decoded decoded =
                                     RunSubmissionCodec.decode(
                                             request.planGraphBytes());
                             durableSubmitted.set(PlanArtifactCodec.decode(
                                     decoded.artifactBytes()));
                         }
                         return RpcMessage.jobResult(true, 0L, null);
                     });
                     RealClusterJobClient client = new RealClusterJobClient(
                             new FixedMasterLocator(new Endpoint(
                                     "127.0.0.1", server.port())),
                             clusterRuntime)) {
                    client.submitArtifactAttempt(
                            unboundArtifact, "portable-publication");
                    client.submitRun(unboundArtifact, new RunSubmission(
                            "portable-run", "portable-attempt",
                            "portable-durable-publication", 1, 0L, 1L));
                }
                assertEquals(deployments.deploymentSetId(),
                        PlanArtifactDeploymentBinding.deploymentSetId(
                                submitted.get()));
                assertEquals(
                        PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1,
                        submitted.get().payloadFormat());
                assertEquals(deployments.deploymentSetId(),
                        PlanArtifactDeploymentBinding.deploymentSetId(
                                durableSubmitted.get()));
                assertEquals(PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1,
                        durableSubmitted.get().payloadFormat());

                Files.createDirectories(temp.resolve("cluster-worker"));
                InProcessRegistry clusterRegistry = new InProcessRegistry();
                MasterServer clusterMaster = new MasterServer(
                        5_000L, 0, clusterRegistry,
                        io.rowweave.monitor.JobMonitorRegistry.NOOP,
                        clusterRegistry.artifactRepository(), clusterRuntime);
                try {
                    clusterMaster.startFixedLeadershipServices();
                    try (WorkerServer clusterWorker = new WorkerServer(
                                 "portable-cluster-worker", 8,
                                 new FixedMasterLocator(new Endpoint(
                                         "127.0.0.1", clusterMaster.port())),
                                 temp.resolve("cluster-worker").toFile(),
                                 clusterRuntime);
                         RealClusterJobClient clusterClient =
                                 new RealClusterJobClient(
                                         new FixedMasterLocator(new Endpoint(
                                                 "127.0.0.1",
                                                 clusterMaster.port())),
                                         clusterRuntime)) {
                        ExecutionReport clusterReport =
                                clusterClient.submit(unboundArtifact);
                        assertEquals(0L, clusterReport.rowsOut());
                        ExecutionReport durableReport = clusterClient.submitRun(
                                unboundArtifact, new RunSubmission(
                                        "portable-real-run",
                                        "portable-real-attempt",
                                        "portable-real-publication",
                                        1, 0L, 1L));
                        assertEquals(0L, durableReport.rowsOut());
                        assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                                clusterClient.queryRun(
                                        "portable-real-run").status());
                        assertEquals(
                                io.rowweave.commit.BatchSinkOutcome.State.COMMITTED,
                                clusterClient.queryBatchSinkOutcome(
                                        "portable-real-run").state());
                    }
                } finally {
                    clusterMaster.close();
                }

                InProcessRegistry recoveryRegistry = new InProcessRegistry();
                String storedArtifactId = recoveryRegistry.artifactRepository()
                        .put(PlanArtifactCodec.encode(artifact));
                assertEquals(artifact.artifactId(), storedArtifactId);
                io.rowweave.run.RunDecision created =
                        recoveryRegistry.runStateStore().apply(
                                io.rowweave.run.RunCommand.createRun(
                                        "create-portable-recovery",
                                        "portable-recovery-run",
                                        artifact.definitionId(), storedArtifactId,
                                        1, 0L, 1L));
                assertEquals(io.rowweave.run.RunDecision.Code.ACCEPTED,
                        created.code());
                Files.createDirectories(temp.resolve("recovery-worker"));
                try (MasterServer recoveryMaster = new MasterServer(
                             5_000L, 0, recoveryRegistry,
                             io.rowweave.monitor.JobMonitorRegistry.NOOP,
                             recoveryRegistry.artifactRepository(),
                             clusterRuntime);
                     WorkerServer recoveryWorker = new WorkerServer(
                             "portable-recovery-worker", 8,
                             new FixedMasterLocator(new Endpoint(
                                     "127.0.0.1", recoveryMaster.port())),
                             temp.resolve("recovery-worker").toFile(),
                             clusterRuntime);
                     MasterElection election = new MasterElection(
                             recoveryRegistry, new Endpoint(
                                     "127.0.0.1", recoveryMaster.port()))) {
                    recoveryMaster.bindLeadership(election);
                    election.start();
                    long recoveryDeadline = System.nanoTime()
                            + TimeUnit.SECONDS.toNanos(10L);
                    while (recoveryRegistry.runStateStore()
                            .get("portable-recovery-run").status()
                            != io.rowweave.run.RunStatus.SUCCEEDED
                            && System.nanoTime() < recoveryDeadline) {
                        Thread.sleep(10L);
                    }
                    assertEquals(io.rowweave.run.RunStatus.SUCCEEDED,
                            recoveryRegistry.runStateStore()
                                    .get("portable-recovery-run").status());
                }
            }

        } finally {
            System.clearProperty(PROBE);
            clearCallbackProbes();
        }
        assertEquals(0L, budget.usedBytes());
    }

    private static PortableSourceBinding sourceBinding(PortableBatchPlan plan) {
        return (PortableSourceBinding) plan.nodes().get(0).binding();
    }

    private static PortableSinkBinding sinkBinding(PortableBatchPlan plan) {
        return (PortableSinkBinding) plan.nodes().get(1).binding();
    }

    private static PortableBatchPlan plan(
            PortableConnectorProviderContract sourceContract,
            PortableConnectorProviderContract sinkContract) {
        PortableRowSchema schema = new PortableRowSchema(
                Collections.singletonList(new PortableField(
                        "id", "id", "id", PortableDataType.LONG, false)));
        String runtimeSchema = SchemaFingerprint.of(
                PortablePlanCompiler.toDataSchema(schema));
        byte[] split = BatchSourceSplitDescriptorCodec.encode(
                new BatchSourceSplitDescriptor(
                        1, "test-source", 1, "split", runtimeSchema,
                        "snapshot", BatchSourceSplitDescriptor.ReplayGuarantee
                        .EXACT_ORDERED_FROM_START));
        byte[] descriptor = BatchSinkDescriptorCodec.encode(
                new BatchSinkDescriptor(
                        1, "test-sink", 1, runtimeSchema,
                        "destination", "domain",
                        BatchSinkDescriptor.CapabilityProfile
                                .SIDE_EFFECT_FREE_V1));
        PortablePlanNode source = new PortablePlanNode(
                "source", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSourceBinding(sourceContract.providerKey(),
                        PortableConnectorProviderContractCodec.digest(
                                sourceContract), new byte[]{1},
                        Collections.singletonList(split)));
        PortablePlanNode sink = new PortablePlanNode(
                "sink", PortableOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSinkBinding(sinkContract.providerKey(),
                        PortableConnectorProviderContractCodec.digest(
                                sinkContract), descriptor, new byte[]{4}));
        return new PortableBatchPlan(1, 1,
                Collections.singletonList("sink"),
                Arrays.asList(source, sink), Collections.singletonList(
                        PortableBatchPlan.BATCH_CAPABILITY));
    }

    private static PortableConnectorProviderContract sourceContract() {
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SOURCE, "test-source-v1",
                "rowweave.opaque-bytes.v1", 16,
                BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES, 4);
    }

    private static PortableConnectorProviderContract sinkContract() {
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SINK, "test-sink-v1",
                "rowweave.opaque-bytes.v1", 16,
                BatchSinkDescriptorCodec.MAX_FRAME_BYTES, 1);
    }

    private static byte[] providerJar(
            PortableConnectorProviderContract sourceContract,
            PortableConnectorProviderContract sinkContract) throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        addClass(entries, app.portablefixture.SourceProvider.class);
        addClass(entries, app.portablefixture.SinkProvider.class);
        addClass(entries, app.portablefixture.TestInput.class);
        addClass(entries, app.portablefixture.TestOutput.class);
        entries.put(PortableRuntimeBindingRegistry.SOURCE_SERVICE_RESOURCE,
                (app.portablefixture.SourceProvider.class.getName()
                        + "=META-INF/rowweave/source.contract\n")
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(PortableRuntimeBindingRegistry.SINK_SERVICE_RESOURCE,
                (app.portablefixture.SinkProvider.class.getName()
                        + "=META-INF/rowweave/sink.contract\n")
                        .getBytes(StandardCharsets.UTF_8));
        entries.put("META-INF/rowweave/source.contract",
                PortableConnectorProviderContractCodec.encode(sourceContract));
        entries.put("META-INF/rowweave/sink.contract",
                PortableConnectorProviderContractCodec.encode(sinkContract));
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
        try (InputStream input = type.getClassLoader().getResourceAsStream(name)) {
            if (input == null) throw new AssertionError("missing test class " + name);
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
            Path path, byte[] jar, KeyPair signer) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                "connectors", jar.length, digest));
        return new DeploymentManifest("connectors", path, jar.length, digest,
                "key", signature.sign());
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        return generator.generateKeyPair();
    }

    private static void assertPortableCode(String code, Runnable action) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static void clearCallbackProbes() {
        for (String callbackProbe : CALLBACK_PROBES) {
            System.clearProperty(callbackProbe);
        }
    }

}
