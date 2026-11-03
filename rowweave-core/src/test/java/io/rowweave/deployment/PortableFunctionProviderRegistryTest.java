package io.rowweave.deployment;

import io.rowweave.MapFunction;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableFunctionProviderContract;
import io.rowweave.portable.PortableFunctionProviderContractCodec;
import io.rowweave.portable.PortablePlanException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortableFunctionProviderRegistryTest {

    private static final String LOAD_PROBE =
            "rowweave.test.portable.function.provider.loaded";

    @Test
    void staticScanVerifiesSignedReferenceBeforeWorkerLoadsProvider(
            @TempDir Path temp) throws Exception {
        System.clearProperty(LOAD_PROBE);
        PortableFunctionProviderContract contract = contract();
        byte[] jar = providerJar(temp.resolve("compile"), contract);
        KeyPair signer = keyPair();
        Path jarPath = temp.resolve("functions.jar").toAbsolutePath();
        Files.write(jarPath, jar);
        DeploymentManifest manifest = manifest(
                "domain-functions", jarPath, jar, signer);
        DeploymentMemoryBudget budget =
                DeploymentMemoryBudget.forTesting(jar.length + 500_000L);
        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap("key", signer.getPublic()), budget)) {
            FunctionDeploymentReference reference = reference(
                    deployments.deploymentSetId(), jar);
            PortableFunctionProviderRegistry metadata =
                    PortableFunctionProviderRegistry.scan(
                            deployments,
                            PortableFunctionProviderRegistry.Role.STATIC_ONLY);

            metadata.verify(reference);
            assertEquals(null, System.getProperty(LOAD_PROBE));
            assertThrows(IllegalStateException.class,
                    () -> metadata.resolve(reference,
                            io.rowweave.internal.ProviderActionSupervisor
                                    .processWide(2),
                            System.nanoTime() + 5_000_000_000L));

            PortableFunctionProviderRegistry worker =
                    PortableFunctionProviderRegistry.scan(
                            deployments,
                            PortableFunctionProviderRegistry.Role.WORKER);
            assertTrue(worker.resolve(reference,
                    io.rowweave.internal.ProviderActionSupervisor.processWide(2),
                    System.nanoTime() + 5_000_000_000L) instanceof MapFunction);
            assertEquals("yes", System.getProperty(LOAD_PROBE));
        } finally {
            System.clearProperty(LOAD_PROBE);
        }
        assertEquals(0L, budget.usedBytes());
    }

    @Test
    void identityOrContractDriftRejectsWithoutClassInitialization(
            @TempDir Path temp) throws Exception {
        System.clearProperty(LOAD_PROBE);
        byte[] jar = providerJar(temp.resolve("compile"), contract());
        KeyPair signer = keyPair();
        Path jarPath = temp.resolve("functions.jar").toAbsolutePath();
        Files.write(jarPath, jar);
        DeploymentMemoryBudget budget =
                DeploymentMemoryBudget.forTesting(jar.length + 500_000L);
        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest(
                        "domain-functions", jarPath, jar, signer)),
                Collections.singletonMap("key", signer.getPublic()), budget)) {
            PortableFunctionProviderRegistry registry =
                    PortableFunctionProviderRegistry.scan(
                            deployments,
                            PortableFunctionProviderRegistry.Role.WORKER);
            FunctionDeploymentReference reference = reference(
                    deployments.deploymentSetId(), jar);
            FunctionDeploymentReference drift = new FunctionDeploymentReference(
                    reference.deploymentSetId(), reference.deploymentId(),
                    repeat('f', 64), reference.className(),
                    reference.functionAbi(), reference.configurationCodecId(),
                    reference.configurationDigest(),
                    reference.configurationFrame(), reference.resourceProfile());

            PortablePlanException failure = assertThrows(
                    PortablePlanException.class, () -> registry.verify(drift));
            assertEquals("FUNCTION_DEPLOYMENT_MISMATCH", failure.code());
            assertEquals(null, System.getProperty(LOAD_PROBE));
        } finally {
            System.clearProperty(LOAD_PROBE);
        }
        assertEquals(0L, budget.usedBytes());
    }

    private static PortableFunctionProviderContract contract() {
        return new PortableFunctionProviderContract(
                1, "app.NormalizeAmount", FunctionAbi.MAP_V1,
                "rowweave.opaque-bytes.v1", 16,
                new FunctionResourceProfile(4096, 0, 1));
    }

    private static FunctionDeploymentReference reference(
            String deploymentSetId, byte[] jar) throws Exception {
        byte[] config = {1};
        return new FunctionDeploymentReference(
                deploymentSetId, "domain-functions", hex(sha256(jar)),
                "app.NormalizeAmount", FunctionAbi.MAP_V1,
                "rowweave.opaque-bytes.v1", hex(sha256(config)), config,
                new FunctionResourceProfile(2048, 0, 1));
    }

    private static byte[] providerJar(
            Path directory, PortableFunctionProviderContract contract)
            throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("app.NormalizeAmount",
                "package app; public final class NormalizeAmount implements "
                        + "io.rowweave.MapFunction { public NormalizeAmount(){} "
                        + "public io.rowweave.DataRow map(io.rowweave.DataRow row)"
                        + "{return row;} }");
        sources.put("app.NormalizeProvider",
                "package app; public final class NormalizeProvider implements "
                        + "io.rowweave.portable.PortableFunctionProvider {"
                        + "static {System.setProperty(\"" + LOAD_PROBE
                        + "\",\"yes\");} public NormalizeProvider(){} "
                        + "public io.rowweave.portable.PortableFunctionProviderContract contract()"
                        + "{return new io.rowweave.portable.PortableFunctionProviderContract(1,"
                        + "\"app.NormalizeAmount\",io.rowweave.portable.FunctionAbi.MAP_V1,"
                        + "\"rowweave.opaque-bytes.v1\",16,new "
                        + "io.rowweave.portable.FunctionResourceProfile(4096,0,1));}"
                        + "public java.io.Serializable create(byte[] config)"
                        + "{return new app.NormalizeAmount();}}");
        Map<String, byte[]> entries = compile(directory, sources);
        entries.put(PortableFunctionProviderRegistry.SERVICE_RESOURCE,
                ("app.NormalizeProvider=META-INF/rowweave/normalize.contract\n")
                        .getBytes(StandardCharsets.UTF_8));
        entries.put("META-INF/rowweave/normalize.contract",
                PortableFunctionProviderContractCodec.encode(contract));
        return jar(entries);
    }

    private static Map<String, byte[]> compile(
            Path directory, Map<String, String> sources) throws Exception {
        Path sourceRoot = directory.resolve("src");
        Path classes = directory.resolve("classes");
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
        assertTrue(compiler != null);
        assertEquals(0, compiler.run(
                null, null, null, arguments.toArray(new String[0])));
        Map<String, byte[]> compiled = new LinkedHashMap<>();
        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    compiled.put(classes.relativize(file).toString()
                                    .replace('\\', '/'),
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

    private static DeploymentManifest manifest(
            String id, Path path, byte[] jar, KeyPair signer) throws Exception {
        byte[] digest = sha256(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                id, jar.length, digest));
        return new DeploymentManifest(id, path, jar.length, digest,
                "key", signature.sign());
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        return generator.generateKeyPair();
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
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
