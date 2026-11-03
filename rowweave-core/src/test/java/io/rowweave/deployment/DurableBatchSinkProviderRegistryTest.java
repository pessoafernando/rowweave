package io.rowweave.deployment;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableBatchSinkProvider;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableBatchSinkProviderContractCodec;
import io.rowweave.DurableSinkAttemptBudget;
import io.rowweave.DurableSinkCleanupContract;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
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

/** signed static discovery must precede deployment code. */
public class DurableBatchSinkProviderRegistryTest {
    private static final String PROVIDER_CLASS = "example.AdversarialProvider";
    private static final String CONTRACT_RESOURCE =
            "META-INF/rowweave/durable-sink/contracts/adversarial.bin";
    private static final String INITIALIZED_PROPERTY =
            "rowweave.test.provider-registry.initialized";

    @AfterEach
    void clearProbe() {
        System.clearProperty(INITIALIZED_PROPERTY);
    }

    @Test
    void exactSignedPolicyAdmitsStaticContractWithoutInitializingClass(
            @TempDir Path temp) throws Exception {
        DurableBatchSinkProviderContract contract = contractForProvider();
        try (VerifiedDeploymentSet deployments = deployment(
                temp, providerJar(temp, contract))) {
            DurableBatchSinkProviderAdmissionPolicy policy =
                    DurableBatchSinkProviderAdmissionPolicy.forTesting(
                            Collections.singletonList(
                                    DurableBatchSinkProviderAdmissionPolicy.entry(
                                            "signed-test", PROVIDER_CLASS,
                                            contract.providerKey(),
                                            DurableBatchSinkProviderContractCodec
                                                    .digest(contract))));
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scanForTesting(
                            deployments, policy,
                            DurableBatchSinkProviderRegistry.Role.STATIC_ONLY);

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
    void unadmittedOrDigestMismatchedCandidateFailsBeforeClassInitialization(
            @TempDir Path temp) throws Exception {
        DurableBatchSinkProviderContract contract = contractForProvider();
        try (VerifiedDeploymentSet deployments = deployment(
                temp, providerJar(temp, contract))) {
            DeploymentValidationException unadmitted = assertThrows(
                    DeploymentValidationException.class,
                    () -> DurableBatchSinkProviderRegistry.scanForTesting(
                            deployments,
                            DurableBatchSinkProviderAdmissionPolicy.forTesting(
                                    Collections.emptyList()),
                            DurableBatchSinkProviderRegistry.Role.WORKER));
            assertEquals("PROVIDER_NOT_ADMITTED", unadmitted.code());
            assertNull(System.getProperty(INITIALIZED_PROPERTY));

            byte[] wrongDigest = new byte[32];
            DeploymentValidationException drift = assertThrows(
                    DeploymentValidationException.class,
                    () -> DurableBatchSinkProviderRegistry.scanForTesting(
                            deployments,
                            DurableBatchSinkProviderAdmissionPolicy.forTesting(
                                    Collections.singletonList(
                                            DurableBatchSinkProviderAdmissionPolicy.entry(
                                                    "signed-test", PROVIDER_CLASS,
                                                    contract.providerKey(), wrongDigest))),
                            DurableBatchSinkProviderRegistry.Role.WORKER));
            assertEquals("PROVIDER_CONTRACT_DRIFT", drift.code());
            assertNull(System.getProperty(INITIALIZED_PROPERTY));
        }
    }

    @Test
    void workerLoadsOnlyTheAdmittedOwnerAndRevalidatesRuntimeContract(
            @TempDir Path temp) throws Exception {
        DurableBatchSinkProviderContract contract = contractForProvider();
        try (VerifiedDeploymentSet deployments = deployment(
                temp, providerJar(temp, contract))) {
            DurableBatchSinkProviderAdmissionPolicy policy =
                    DurableBatchSinkProviderAdmissionPolicy.forTesting(
                            Collections.singletonList(
                                    DurableBatchSinkProviderAdmissionPolicy.entry(
                                            "signed-test", PROVIDER_CLASS,
                                            contract.providerKey(),
                                            DurableBatchSinkProviderContractCodec
                                                    .digest(contract))));
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scanForTesting(
                            deployments, policy,
                            DurableBatchSinkProviderRegistry.Role.WORKER);

            assertNull(System.getProperty(INITIALIZED_PROPERTY));
            assertNotNull(registry.loadProvider(
                    contract.providerKey(),
                    io.rowweave.internal.ProviderActionSupervisor
                            .processWide(2),
                    System.nanoTime() + 1_000_000_000L));
            assertEquals(contract, registry.requireContract(contract.providerKey()));
            assertEquals("yes", System.getProperty(INITIALIZED_PROPERTY));
        }
    }

    @Test
    void oversizedServiceResourceIsRejectedBeforeExpansion(
            @TempDir Path temp) throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        byte[] oversized = new byte[
                DurableBatchSinkProviderRegistry.MAX_SERVICE_BYTES + 1];
        new java.util.Random(1L).nextBytes(oversized);
        entries.put(DurableBatchSinkProviderRegistry.SERVICE_RESOURCE, oversized);
        try (VerifiedDeploymentSet deployments = deployment(temp, jar(entries))) {
            DeploymentValidationException failure = assertThrows(
                    DeploymentValidationException.class,
                    () -> DurableBatchSinkProviderRegistry.scanForTesting(
                            deployments,
                            DurableBatchSinkProviderAdmissionPolicy.forTesting(
                                    Collections.emptyList()),
                            DurableBatchSinkProviderRegistry.Role.STATIC_ONLY));
            assertEquals("PROVIDER_SERVICE_INVALID", failure.code());
        }
    }

    @Test
    void productionSnapshotAlwaysContainsExactCoreContracts() throws Exception {
        assertEquals(2, DurableBatchSinkProviderAdmissionPolicy.production().size(),
                "production policy must freeze both built-in contract digests");
        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.emptyList(), Collections.emptyMap(),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L))) {
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scan(
                            deployments,
                            DurableBatchSinkProviderRegistry.Role.STATIC_ONLY);
            assertTrue(registry.providerKeys().contains(new DurableSinkProviderKey(
                    "rowweave.discard", 1,
                    BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1)));
            assertTrue(registry.providerKeys().contains(new DurableSinkProviderKey(
                    "rowweave.manifest-file", 1,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1)));
        }
    }

    @Test
    void providerSurfaceRequiresConfigurationDecodeReencode() throws Exception {
        assertEquals(byte[].class,
                io.rowweave.DurableBatchSinkProvider.class.getMethod(
                        "canonicalizeConfiguration",
                        io.rowweave.ProviderBatchSink.class)
                        .getReturnType());
    }

    @Test
    void providerRuntimeLoadCannotBypassProcessSupervisor() throws Exception {
        assertThrows(NoSuchMethodException.class,
                () -> DurableBatchSinkProviderRegistry.class.getMethod(
                        "loadProvider", DurableSinkProviderKey.class));
        assertEquals(DurableBatchSinkProvider.class,
                DurableBatchSinkProviderRegistry.class.getMethod(
                        "loadProvider", DurableSinkProviderKey.class,
                        io.rowweave.internal.ProviderActionSupervisor.class,
                        long.class).getReturnType());
    }

    public static DurableBatchSinkProviderContract contractForProvider() {
        return new DurableBatchSinkProviderContract(
                DurableBatchSinkProviderContract.CURRENT_SPI_VERSION,
                new DurableSinkProviderKey("test.adversarial", 1,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1),
                new DurableSinkCodecDescriptor("test.config", 1, 1024),
                new DurableSinkCodecDescriptor("test.locator", 1, 4096),
                new DurableSinkCodecDescriptor("rowweave.batch-sink-outcome", 1, 1024),
                DurableBatchSinkProviderContract.IdempotencyScope
                        .ATTEMPT_AND_PUBLICATION_V1,
                new DurableSinkCleanupContract(60_000L, 16, 8, true,
                        DurableBatchSinkProviderContract.OrphanOwnership
                                .ATTEMPT_PRIVATE_V1,
                        DurableBatchSinkProviderContract.QuarantinePolicy
                                .PRESERVE_ON_UNKNOWN_OR_CONFLICT,
                        DurableBatchSinkProviderContract.SafeDeleteProof
                                .ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1),
                new DurableSinkAttemptBudget(1024L * 1024L, 8, 4096,
                        16 * 1024, 30_000L),
                DurableBatchSinkProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static byte[] providerJar(
            Path temp, DurableBatchSinkProviderContract contract) throws Exception {
        Map<String, String> sources = Collections.singletonMap(PROVIDER_CLASS,
                "package example;"
                + "public final class AdversarialProvider implements "
                + "io.rowweave.DurableBatchSinkProvider {"
                + "static { System.setProperty(\"" + INITIALIZED_PROPERTY
                + "\", \"yes\"); }"
                + "public AdversarialProvider() {}"
                + "public io.rowweave.DurableBatchSinkProviderContract contract() {"
                + "return io.rowweave.deployment."
                + "DurableBatchSinkProviderRegistryTest.contractForProvider();}"
                + "public byte[] canonicalizeConfiguration("
                + "io.rowweave.ProviderBatchSink o){"
                + "return o.configurationBytes();}"
                + "public byte[] canonicalizeLocator("
                + "io.rowweave.run.PreparedSinkLocator l){"
                + "return l.payload();}"
                + "public io.rowweave.DurableBatchSinkWriteAttempt openWriteAttempt("
                + "io.rowweave.ProviderBatchSink o,"
                + "io.rowweave.run.SinkAttemptIdentity i,"
                + "io.rowweave.DurableSinkActionContext c){return null;}"
                + "public io.rowweave.DurableBatchSinkPreparedGroup openPreparedGroup("
                + "java.util.List o,java.util.List l,"
                + "io.rowweave.DurableSinkActionContext c){return null;}"
                + "}");
        Map<String, byte[]> entries = compile(temp, sources);
        entries.put(DurableBatchSinkProviderRegistry.SERVICE_RESOURCE,
                (PROVIDER_CLASS + '=' + CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(CONTRACT_RESOURCE,
                DurableBatchSinkProviderContractCodec.encode(contract));
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

    private static Map<String, byte[]> compile(
            Path directory, Map<String, String> sources) throws Exception {
        Path sourceRoot = directory.resolve("provider-src");
        Path classes = directory.resolve("provider-classes");
        Files.createDirectories(sourceRoot);
        Files.createDirectories(classes);
        java.util.List<String> arguments = new java.util.ArrayList<>();
        Collections.addAll(arguments, "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString());
        for (Map.Entry<String, String> source : sources.entrySet()) {
            Path file = sourceRoot.resolve(
                    source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.write(file, source.getValue().getBytes(StandardCharsets.UTF_8));
            arguments.add(file.toString());
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                arguments.toArray(new String[0])) != 0) {
            throw new AssertionError("test provider compilation failed");
        }
        Map<String, byte[]> compiled = new LinkedHashMap<>();
        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    compiled.put(classes.relativize(file).toString().replace('\\', '/'),
                            Files.readAllBytes(file));
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
        return compiled;
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
