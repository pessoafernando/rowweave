package io.rowweave.connector.postgres;

import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
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
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresDeploymentPackagerTest {
    @Test
    void canonicalImageIsDeterministicScannerAcceptedAndLoadsProvider(
            @TempDir Path temp) throws Exception {
        Path input = temp.resolve("runtime.jar");
        writeInput(input);
        Path first = temp.resolve("postgres-deployment-1.jar");
        Path second = temp.resolve("postgres-deployment-2.jar");

        PostgresDeploymentPackager.pack(input, first);
        PostgresDeploymentPackager.pack(input, second);

        assertArrayEquals(Files.readAllBytes(first), Files.readAllBytes(second));
        Map<String, byte[]> entries = entries(first);
        assertEquals(PostgresConnectorProvider.class.getName() + '='
                        + PostgresDeploymentPackager.SOURCE_CONTRACT_RESOURCE
                        + '\n',
                new String(entries.get(PostgresDeploymentPackager
                        .SOURCE_SERVICE_RESOURCE), StandardCharsets.UTF_8));
        assertEquals(PostgresConnectorContract.contract(),
                StreamConnectorProviderContractCodec.decode(entries.get(
                        PostgresDeploymentPackager.SOURCE_CONTRACT_RESOURCE)));
        assertFalse(entries.containsKey("io/rowweave/Source.class"));

        try (VerifiedDeploymentSet deployments = signed(first)) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            StreamConnectorProviderContract contract = registry.requireContract(
                    StreamConnectorProviderContract.Kind.SOURCE,
                    PostgresConnectorContract.PROVIDER_KEY);
            assertEquals(PostgresConnectorContract.contract(), contract);
            Class<?> deployed = deployments.resolveTrustedClass(
                    PostgresConnectorProvider.class.getName());
            assertNotSame(PostgresConnectorProvider.class, deployed);
            Object provider = deployed.getConstructor().newInstance();
            assertTrue(provider instanceof StreamCdcSourceProvider);
        }
    }

    private static void writeInput(Path jar) throws Exception {
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(jar))) {
            entry(output, "io/rowweave/Source.class", new byte[] {1});
            Path classes = java.nio.file.Paths.get("target/classes");
            try (java.util.stream.Stream<Path> paths = Files.walk(classes)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".class"))
                        .filter(path -> classes.relativize(path).toString()
                                .replace(java.io.File.separatorChar, '/')
                                .startsWith(
                                        "io/rowweave/connector/postgres/"))
                        .sorted().forEach(path -> {
                            try {
                                entry(output, classes.relativize(path).toString()
                                                .replace(java.io.File
                                                        .separatorChar, '/'),
                                        Files.readAllBytes(path));
                            } catch (Exception failure) {
                                throw new RuntimeException(failure);
                            }
                        });
            }
        }
    }

    private static void entry(JarOutputStream output, String name,
            byte[] bytes) throws Exception {
        JarEntry entry = new JarEntry(name);
        entry.setTime(123456789L);
        output.putNextEntry(entry);
        output.write(bytes);
        output.closeEntry();
    }

    private static Map<String, byte[]> entries(Path jar) throws Exception {
        Map<String, byte[]> result = new TreeMap<>();
        try (JarFile input = new JarFile(jar.toFile())) {
            Enumeration<JarEntry> entries = input.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                try (InputStream stream = input.getInputStream(entry)) {
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    for (;;) {
                        int read = stream.read(buffer);
                        if (read < 0) break;
                        bytes.write(buffer, 0, read);
                    }
                    result.put(entry.getName(), bytes.toByteArray());
                }
            }
        }
        return result;
    }

    private static VerifiedDeploymentSet signed(Path jar) throws Exception {
        byte[] image = Files.readAllBytes(jar);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(image);
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                PostgresConnectorContract.DEPLOYMENT_ID,
                image.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                PostgresConnectorContract.DEPLOYMENT_ID,
                jar.toAbsolutePath().normalize(), image.length, digest,
                "postgres-test-signer", signature.sign());
        return VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "postgres-test-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(32L * 1024 * 1024));
    }
}
