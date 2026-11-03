package io.rowweave.connector.s3;

import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.spi.StreamSourceProvider;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3DeploymentPackagerTest {
    @Test
    void canonicalImageIsDeterministicScannerAcceptedAndLoadsProvider(
            @TempDir Path temp) throws Exception {
        Path input = temp.resolve("shaded-input.jar");
        writeInput(input, false);
        Path first = temp.resolve("s3-deployment-1.jar");
        Path second = temp.resolve("s3-deployment-2.jar");

        S3DeploymentPackager.pack(input, first);
        S3DeploymentPackager.pack(input, second);

        assertArrayEquals(Files.readAllBytes(first), Files.readAllBytes(second));
        Map<String, byte[]> entries = entries(first);
        assertTrue(entries.containsKey(
                S3DeploymentPackager.SOURCE_SERVICE_RESOURCE));
        assertTrue(entries.containsKey(
                S3DeploymentPackager.SOURCE_CONTRACT_RESOURCE));
        assertEquals(S3ConnectorProvider.class.getName() + '='
                        + S3DeploymentPackager.SOURCE_CONTRACT_RESOURCE + '\n',
                new String(entries.get(
                        S3DeploymentPackager.SOURCE_SERVICE_RESOURCE),
                        StandardCharsets.UTF_8));
        assertEquals(S3ConnectorContract.contract(),
                StreamConnectorProviderContractCodec.decode(entries.get(
                        S3DeploymentPackager.SOURCE_CONTRACT_RESOURCE)));
        assertFalse(entries.containsKey("io/rowweave/Source.class"));
        assertFalse(entries.containsKey("META-INF/versions/9/module-info.class"));
        for (String name : entries.keySet()) {
            assertFalse(name.endsWith("/"), name);
        }

        try (VerifiedDeploymentSet deployments = signed(first);
             Registry registry = registry(deployments)) {
            StreamConnectorProviderContract contract = registry.requireContract(
                    StreamConnectorProviderContract.Kind.SOURCE,
                    S3ConnectorContract.PROVIDER_KEY);
            assertEquals(S3ConnectorContract.contract(), contract);
            Class<?> deployed = deployments.resolveTrustedClass(
                    S3ConnectorProvider.class.getName());
            assertNotSame(S3ConnectorProvider.class, deployed);
            StreamSourceProvider provider = (StreamSourceProvider)
                    deployed.getConstructor().newInstance();
            assertEquals(S3ConnectorContract.PROVIDER_KEY,
                    provider.providerKey());
            assertEquals(S3ConnectorContract.contractDigest(),
                    provider.contractDigest());
        }
    }

    @Test
    void traversalDuplicateAndInputOutputAliasFailBeforePublication(
            @TempDir Path temp) throws Exception {
        Path traversal = temp.resolve("traversal.jar");
        writeInput(traversal, true);
        Path output = temp.resolve("deployment.jar");
        assertFailure("S3_DEPLOYMENT_ARCHIVE_INVALID",
                () -> S3DeploymentPackager.pack(traversal, output));
        assertFalse(Files.exists(output));
        assertFailure("S3_DEPLOYMENT_ARCHIVE_INVALID",
                () -> S3DeploymentPackager.pack(traversal, traversal));
    }

    private static void writeInput(Path jar, boolean traversal)
            throws Exception {
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(jar))) {
            if (traversal) {
                entry(output, "../escape", new byte[] {1});
                return;
            }
            entry(output, "META-INF/", new byte[0]);
            entry(output, "META-INF/versions/9/module-info.class",
                    new byte[] {1});
            entry(output, "io/rowweave/Source.class", new byte[] {1});
            Path classes = java.nio.file.Paths.get("target/classes");
            try (java.util.stream.Stream<Path> paths = Files.walk(classes)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".class"))
                        .filter(path -> classes.relativize(path).toString()
                                .replace(java.io.File.separatorChar, '/')
                                .startsWith("io/rowweave/connector/s3/"))
                        .sorted()
                        .forEach(path -> {
                            try {
                                entry(output, classes.relativize(path).toString()
                                                .replace(java.io.File.separatorChar, '/'),
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
                assertFalse(entry.isDirectory());
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
                S3ConnectorContract.DEPLOYMENT_ID, image.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                S3ConnectorContract.DEPLOYMENT_ID,
                jar.toAbsolutePath().normalize(), image.length, digest,
                "s3-test-signer", signature.sign());
        return VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "s3-test-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(64L * 1024 * 1024));
    }

    private static Registry registry(VerifiedDeploymentSet deployments) {
        return new Registry(StreamConnectorProviderRegistry.scan(
                deployments, StreamConnectorProviderRegistry.Role.STATIC_ONLY));
    }

    private static void assertFailure(String code, ThrowingRunnable action) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, action::run);
        assertEquals(code, failure.getMessage());
    }

    private interface ThrowingRunnable { void run() throws Exception; }

    private static final class Registry implements AutoCloseable {
        private final StreamConnectorProviderRegistry delegate;
        private Registry(StreamConnectorProviderRegistry delegate) {
            this.delegate = delegate;
        }
        private StreamConnectorProviderContract requireContract(
                StreamConnectorProviderContract.Kind kind, String key) {
            return delegate.requireContract(kind, key);
        }
        @Override public void close() { }
    }
}
