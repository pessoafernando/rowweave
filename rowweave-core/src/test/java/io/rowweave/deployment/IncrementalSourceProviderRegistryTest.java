package io.rowweave.deployment;

import io.rowweave.IncrementalSourceProvider;
import io.rowweave.incremental.IncrementalCaptureResponseCodec;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierCodec;
import io.rowweave.incremental.IncrementalMaterializationResponseCodec;
import io.rowweave.incremental.IncrementalSourceActionBudget;
import io.rowweave.incremental.IncrementalSourceActionExecutor;
import io.rowweave.incremental.IncrementalSourceCodecDescriptor;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.IncrementalSourceProviderKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** source providers are signed metadata until Worker admission. */
public class IncrementalSourceProviderRegistryTest {
    private static final String PROVIDER_CLASS = "example.IncrementalProvider";
    private static final String CONTRACT_RESOURCE =
            "META-INF/rowweave/incremental-source/contracts/test.bin";
    private static final String INITIALIZED_PROPERTY =
            "rowweave.test.incremental-provider-registry.initialized";

    @AfterEach
    void clearProbe() {
        System.clearProperty(INITIALIZED_PROPERTY);
    }

    @Test
    void staticRoleReadsExactSignedContractWithoutLoadingProvider(@TempDir Path temp)
            throws Exception {
        IncrementalSourceProviderContract contract = contractForProvider();
        try (VerifiedDeploymentSet deployments = deployment(
                temp, providerJar(temp, contract))) {
            IncrementalSourceProviderRegistry registry =
                    IncrementalSourceProviderRegistry.scanForTesting(
                            deployments, admitted(contract),
                            IncrementalSourceProviderRegistry.Role.STATIC_ONLY);

            assertEquals(contract, registry.requireContract(contract.providerKey()));
            assertNull(System.getProperty(INITIALIZED_PROPERTY));
            assertThrows(IllegalStateException.class,
                    () -> registry.loadProvider(contract.providerKey(),
                            io.rowweave.internal.ProviderActionSupervisor
                                    .processWide(2),
                            System.nanoTime() + 1_000_000_000L));
            assertNull(System.getProperty(INITIALIZED_PROPERTY));
        }
    }

    @Test
    void unadmittedAndDigestDriftFailBeforeProviderInitialization(@TempDir Path temp)
            throws Exception {
        IncrementalSourceProviderContract contract = contractForProvider();
        try (VerifiedDeploymentSet deployments = deployment(
                temp, providerJar(temp, contract))) {
            DeploymentValidationException unadmitted = assertThrows(
                    DeploymentValidationException.class,
                    () -> IncrementalSourceProviderRegistry.scanForTesting(
                            deployments,
                            IncrementalSourceProviderAdmissionPolicy.forTesting(
                                    Collections.emptyList()),
                            IncrementalSourceProviderRegistry.Role.WORKER));
            assertEquals("SOURCE_PROVIDER_NOT_ADMITTED", unadmitted.code());

            DeploymentValidationException drift = assertThrows(
                    DeploymentValidationException.class,
                    () -> IncrementalSourceProviderRegistry.scanForTesting(
                            deployments,
                            IncrementalSourceProviderAdmissionPolicy.forTesting(
                                    Collections.singletonList(
                                            IncrementalSourceProviderAdmissionPolicy.entry(
                                                    "signed-test", PROVIDER_CLASS,
                                                    contract.providerKey(),
                                                    new byte[32]))),
                            IncrementalSourceProviderRegistry.Role.WORKER));
            assertEquals("SOURCE_PROVIDER_CONTRACT_DRIFT", drift.code());
            assertNull(System.getProperty(INITIALIZED_PROPERTY));
        }
    }

    @Test
    void workerLoadsOnlyAdmittedOwnerThroughProcessSupervisor(@TempDir Path temp)
            throws Exception {
        IncrementalSourceProviderContract contract = contractForProvider();
        try (VerifiedDeploymentSet deployments = deployment(
                temp, providerJar(temp, contract))) {
            IncrementalSourceProviderRegistry registry =
                    IncrementalSourceProviderRegistry.scanForTesting(
                            deployments, admitted(contract),
                            IncrementalSourceProviderRegistry.Role.WORKER);

            assertNotNull(registry.loadProvider(contract.providerKey(),
                    io.rowweave.internal.ProviderActionSupervisor.processWide(2),
                    System.nanoTime() + 1_000_000_000L));
            assertEquals("yes", System.getProperty(INITIALIZED_PROPERTY));
            assertThrows(NoSuchMethodException.class,
                    () -> IncrementalSourceProviderRegistry.class.getMethod(
                            "loadProvider", IncrementalSourceProviderKey.class));
        }
    }

    @Test
    void productionPolicyAdmitsOnlyTheThreeCoreExactProfiles() throws Exception {
        assertEquals(3, IncrementalSourceProviderAdmissionPolicy.production().size());
        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.emptyList(), Collections.emptyMap(),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L))) {
            IncrementalSourceProviderRegistry staticRegistry =
                    IncrementalSourceProviderRegistry.scan(
                            deployments,
                            IncrementalSourceProviderRegistry.Role.STATIC_ONLY);
            assertEquals(3, staticRegistry.providerKeys().size());
            assertTrue(staticRegistry.providerKeys().contains(
                    io.rowweave.incremental.JdbcIncrementalSourceProfile
                            .sequenceContract().providerKey()));
            assertTrue(staticRegistry.providerKeys().contains(
                    io.rowweave.incremental.JdbcIncrementalSourceProfile
                            .timestampKeyContract().providerKey()));
            assertTrue(staticRegistry.providerKeys().contains(
                    io.rowweave.incremental.ObjectManifestIncrementalSourceProfile
                            .builtInContract().providerKey()));
            assertThrows(DeploymentValidationException.class,
                    () -> staticRegistry.requireContract(
                            new IncrementalSourceProviderKey(
                                    "unsupported.mutable-table", 1)));

            IncrementalSourceProviderRegistry workerRegistry =
                    IncrementalSourceProviderRegistry.scan(
                            deployments,
                            IncrementalSourceProviderRegistry.Role.WORKER);
            io.rowweave.internal.ProviderActionSupervisor supervisor =
                    io.rowweave.internal.ProviderActionSupervisor.processWide(2);
            assertEquals("JdbcSequenceIncrementalSourceProvider",
                    workerRegistry.loadProvider(
                            io.rowweave.incremental.JdbcIncrementalSourceProfile
                                    .sequenceContract().providerKey(),
                            supervisor, System.nanoTime() + 1_000_000_000L)
                            .getClass().getSimpleName());
            assertEquals("JdbcTimestampKeyIncrementalSourceProvider",
                    workerRegistry.loadProvider(
                            io.rowweave.incremental.JdbcIncrementalSourceProfile
                                    .timestampKeyContract().providerKey(),
                            supervisor, System.nanoTime() + 1_000_000_000L)
                            .getClass().getSimpleName());
            assertEquals("ObjectManifestIncrementalSourceProvider",
                    workerRegistry.loadProvider(
                            io.rowweave.incremental
                                    .ObjectManifestIncrementalSourceProfile
                                    .builtInContract().providerKey(),
                            supervisor, System.nanoTime() + 1_000_000_000L)
                            .getClass().getSimpleName());
        }
    }

    public static IncrementalSourceProviderContract contractForProvider() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.signed", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 16),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID, 1,
                        IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                false,
                new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L, 1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static IncrementalSourceProviderAdmissionPolicy admitted(
            IncrementalSourceProviderContract contract) {
        return IncrementalSourceProviderAdmissionPolicy.forTesting(
                Collections.singletonList(
                        IncrementalSourceProviderAdmissionPolicy.entry(
                                "signed-test", PROVIDER_CLASS,
                                contract.providerKey(),
                                IncrementalSourceProviderContractCodec.digest(contract))));
    }

    private static byte[] providerJar(Path temp,
                                      IncrementalSourceProviderContract contract)
            throws Exception {
        String source = "package example;"
                + "public final class IncrementalProvider implements "
                + "io.rowweave.IncrementalSourceProvider {"
                + "static { System.setProperty(\"" + INITIALIZED_PROPERTY
                + "\", \"yes\"); }"
                + "public IncrementalProvider() {}"
                + "public io.rowweave.incremental.IncrementalSourceProviderContract contract(){"
                + "return io.rowweave.deployment."
                + "IncrementalSourceProviderRegistryTest.contractForProvider();}"
                + "public byte[] canonicalizeConfiguration("
                + "io.rowweave.incremental.IncrementalSourceConfig c){return c.configurationBytes();}"
                + "public io.rowweave.incremental.IncrementalCaptureResponse captureTarget("
                + "io.rowweave.incremental.IncrementalSourceConfig c,"
                + "io.rowweave.incremental.IncrementalCaptureRequest r){return null;}"
                + "public io.rowweave.incremental.IncrementalMaterializationResponse materializeInterval("
                + "io.rowweave.incremental.IncrementalSourceConfig c,"
                + "io.rowweave.incremental.CapturedTargetReceipt r){return null;}"
                + "}";
        Path sourceRoot = temp.resolve("incremental-provider-src");
        Path classes = temp.resolve("incremental-provider-classes");
        Path file = sourceRoot.resolve("example/IncrementalProvider.java");
        Files.createDirectories(file.getParent());
        Files.createDirectories(classes);
        Files.write(file, source.getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), file.toString()) != 0) {
            throw new AssertionError("test incremental provider compilation failed");
        }
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            files.filter(Files::isRegularFile).forEach(compiled -> {
                try {
                    entries.put(classes.relativize(compiled).toString()
                                    .replace('\\', '/'),
                            Files.readAllBytes(compiled));
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
        entries.put(IncrementalSourceProviderRegistry.SERVICE_RESOURCE,
                (PROVIDER_CLASS + '=' + CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(CONTRACT_RESOURCE,
                IncrementalSourceProviderContractCodec.encode(contract));
        return jar(entries);
    }

    private static VerifiedDeploymentSet deployment(Path temp, byte[] jar)
            throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        Path source = temp.resolve("signed-test.jar").toAbsolutePath();
        Files.write(source, jar);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                "signed-test", jar.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                "signed-test", source, jar.length, digest, "test-signer",
                signature.sign());
        return VerifiedDeploymentSet.load(Collections.singletonList(manifest),
                Collections.singletonMap("test-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L));
    }

    private static byte[] jar(Map<String, byte[]> entries) throws Exception {
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
}
