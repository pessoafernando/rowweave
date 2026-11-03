package io.rowweave.deployment;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.EnumSet;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** operations config becomes one verified process deployment identity. */
class DeploymentRuntimeTest {

    @Test
    void indexedOperationsConfigLoadsExactSetAndSourceChangeClosesAdmission(
            @TempDir Path temp) throws Exception {
        KeyPair signer = keyPair();
        byte[] jar = jar();
        Path source = temp.resolve("application.jar").toAbsolutePath();
        Files.write(source, jar);
        DeploymentManifest deployment = manifest("application", source, jar, signer);
        Path trustRoot = temp.resolve("trust").toAbsolutePath();
        Files.createDirectory(trustRoot);
        Path key = trustRoot.resolve("release.der");
        Path manifest = trustRoot.resolve("application.rwdep");
        Files.write(key, signer.getPublic().getEncoded());
        Files.write(manifest, DeploymentManifestCodec.encode(deployment));
        try {
            readOnly(key, false);
            readOnly(manifest, false);
            readOnly(trustRoot, true);
            Properties properties = configured(trustRoot, key, manifest);
            String expectedSetId;
            try (VerifiedDeploymentSet expected = VerifiedDeploymentSet.load(
                    java.util.Collections.singletonList(deployment),
                    java.util.Collections.singletonMap("release", signer.getPublic()),
                    DeploymentMemoryBudget.forTesting(8L * 1024L * 1024L))) {
                expectedSetId = expected.deploymentSetId();
            }
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                    properties, ControlSecurityConfig.fromProperties(new Properties()),
                    1024L * 1024L * 1024L)) {
                assertEquals(expectedSetId, runtime.deploymentSetId());
                assertNotNull(runtime.portableBindingVerifier());
                assertEquals(new java.util.HashSet<>(
                                IncrementalSourceBuiltinContracts.all().stream()
                                        .map(io.rowweave.incremental
                                                .IncrementalSourceProviderContract
                                                ::providerKey)
                                        .collect(java.util.stream.Collectors.toList())),
                        runtime.incrementalSourceProviderRegistry().providerKeys());
                runtime.requireAdmissionOpen();

                Files.write(source, jar("changed"));
                assertFalse(runtime.probeSources());
                DeploymentValidationException changed = assertThrows(
                        DeploymentValidationException.class, runtime::requireAdmissionOpen);
                assertEquals("DEPLOYMENT_SOURCE_CHANGED", changed.code());
            }
        } finally {
            writable(trustRoot, true);
            writable(key, false);
            writable(manifest, false);
        }
    }

    @Test
    void incompleteIndexedConfigFailsBeforeAnyDeploymentLoad(@TempDir Path temp) {
        Properties properties = new Properties();
        properties.setProperty(DeploymentRuntime.DEPLOYMENT_COUNT_PROPERTY, "1");
        properties.setProperty(DeploymentRuntime.MANIFEST_PROPERTY_PREFIX + "0",
                temp.resolve("missing.rwdep").toAbsolutePath().toString());
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DeploymentRuntime.load(properties,
                        ControlSecurityConfig.fromProperties(new Properties()),
                        1024L * 1024L * 1024L));
        assertEquals(true, failure.getMessage().contains("trustRoot"));
    }

    @Test
    void oneVerifiedRuntimeIdentityIsSharedByClientMasterAndWorker(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair();
        byte[] jar = jar();
        Path source = temp.resolve("shared.jar").toAbsolutePath();
        Files.write(source, jar);
        Path trustRoot = temp.resolve("trust").toAbsolutePath();
        Files.createDirectory(trustRoot);
        Path key = trustRoot.resolve("release.der");
        Path manifest = trustRoot.resolve("shared.rwdep");
        Path workerDirectory = temp.resolve("worker");
        Files.createDirectory(workerDirectory);
        Files.write(key, signer.getPublic().getEncoded());
        Files.write(manifest, DeploymentManifestCodec.encode(
                manifest("shared", source, jar, signer)));
        try {
            readOnly(key, false);
            readOnly(manifest, false);
            readOnly(trustRoot, true);
            Properties properties = configured(trustRoot, key, manifest);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                    properties, ControlSecurityConfig.fromProperties(new Properties()),
                    1024L * 1024L * 1024L, null);
                 io.rowweave.cluster.MasterServer master = new io.rowweave.cluster.MasterServer(
                         5_000L, 0, new io.rowweave.cluster.registry.InProcessRegistry(),
                         io.rowweave.monitor.JobMonitorRegistry.NOOP, null, runtime);
                 io.rowweave.cluster.rpc.InProcessRpcRegistry.Registration masterRegistration =
                         io.rowweave.cluster.rpc.InProcessRpcRegistry.register(
                                 "127.0.0.1", master.port(), master.handler());
                 io.rowweave.cluster.WorkerServer worker = new io.rowweave.cluster.WorkerServer(
                         "deployment-worker", 8,
                         new io.rowweave.cluster.FixedMasterLocator(
                                 new io.rowweave.cluster.Endpoint("127.0.0.1", master.port())),
                         workerDirectory.toFile(), runtime);
                 io.rowweave.cluster.rpc.InProcessRpcRegistry.Registration workerRegistration =
                         io.rowweave.cluster.rpc.InProcessRpcRegistry.register(
                                 "127.0.0.1", worker.port(), worker.handler());
                 io.rowweave.cluster.RealClusterJobClient client =
                         new io.rowweave.cluster.RealClusterJobClient(
                                 new io.rowweave.cluster.FixedMasterLocator(
                                         new io.rowweave.cluster.Endpoint(
                                                 "127.0.0.1", master.port())), runtime)) {
                io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                        new io.rowweave.Field("id", io.rowweave.DataType.IntegerType));
                io.rowweave.plan.PlanGraph graph = new io.rowweave.plan.PlanGraph(
                        java.util.Arrays.asList(
                                io.rowweave.plan.PlanNode.source(
                                        meta, java.util.Collections.emptyList())
                                        .withParam(io.rowweave.plan.PlanNode.OPERATOR_UID_PARAM,
                                                "source"),
                                io.rowweave.plan.PlanNode.sink(
                                        meta, 0, new io.rowweave.DiscardSink(meta)).build()
                                        .withParam(io.rowweave.plan.PlanNode.OPERATOR_UID_PARAM,
                                                "output")));
                PlanArtifact artifact = PlanArtifactDeploymentBinding.bind(
                        io.rowweave.artifact.LegacyPlanArtifactAdapter.wrap(
                                "shared-definition", graph), runtime.deploymentSetId());
                assertEquals(0L, client.submit(artifact).rowsOut());
            }
        } finally {
            writable(trustRoot, true);
            writable(key, false);
            writable(manifest, false);
        }
    }

    private static Properties configured(Path root, Path key, Path manifest) {
        Properties properties = new Properties();
        properties.setProperty(DeploymentRuntime.DEPLOYMENT_COUNT_PROPERTY, "1");
        properties.setProperty(DeploymentRuntime.MANIFEST_PROPERTY_PREFIX + "0", manifest.toString());
        properties.setProperty(DeploymentRuntime.SIGNER_COUNT_PROPERTY, "1");
        properties.setProperty(DeploymentRuntime.SIGNER_ID_PROPERTY_PREFIX + "0", "release");
        properties.setProperty(DeploymentRuntime.SIGNER_PATH_PROPERTY_PREFIX + "0", key.toString());
        properties.setProperty(DeploymentRuntime.TRUST_ROOT_PROPERTY, root.toString());
        properties.setProperty(DeploymentRuntimeConfig.MEMORY_BYTES_PROPERTY,
                Long.toString(8L * 1024L * 1024L));
        return properties;
    }

    private static DeploymentManifest manifest(
            String id, Path source, byte[] jar, KeyPair signer) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(id, jar.length, digest));
        return new DeploymentManifest(id, source, jar.length, digest,
                "release", signature.sign());
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        return generator.generateKeyPair();
    }

    private static byte[] jar() throws Exception {
        return jar("content");
    }

    private static byte[] jar(String content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream output = new JarOutputStream(bytes)) {
            JarEntry entry = new JarEntry("application.txt");
            entry.setTime(0L);
            output.putNextEntry(entry);
            output.write(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return bytes.toByteArray();
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
}
