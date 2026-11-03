package io.rowweave.connector.postgres;

import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresDeploymentArtifactIT {
    @Test
    void deploymentImageIsSelfContainedJava8AndProductionTrustAccepted()
            throws Exception {
        Path image = Paths.get(System.getProperty("postgres.deployment.jar"))
                .toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(image), image.toString());
        assertTrue(Files.size(image) > 0L
                && Files.size(image) <= 32L * 1024 * 1024);

        Set<String> names = new HashSet<>();
        try (JarFile jar = new JarFile(image.toFile(), false)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                assertFalse(entry.isDirectory(), name);
                assertTrue(names.add(name), name);
                assertFalse(name.startsWith("META-INF/versions/"), name);
                assertFalse("module-info.class".equals(name), name);
                assertFalse(name.startsWith("io/rowweave/")
                        && !name.startsWith(
                        "io/rowweave/connector/postgres/"), name);
                if (name.endsWith(".class")) {
                    try (InputStream input = jar.getInputStream(entry)) {
                        byte[] header = new byte[8];
                        int offset = 0;
                        while (offset < header.length) {
                            int read = input.read(header, offset,
                                    header.length - offset);
                            assertTrue(read > 0, name);
                            offset += read;
                        }
                        int major = ((header[6] & 0xff) << 8)
                                | (header[7] & 0xff);
                        assertTrue(major <= 52,
                                name + " requires class major " + major);
                    }
                }
            }
        }
        assertTrue(names.contains(
                PostgresDeploymentPackager.SOURCE_SERVICE_RESOURCE));
        assertTrue(names.contains(
                PostgresDeploymentPackager.SOURCE_CONTRACT_RESOURCE));
        assertTrue(names.contains("org/postgresql/Driver.class"));
        assertFalse(names.contains("io/rowweave/Source.class"));
        assertFalse(names.contains(PostgresDeploymentPackager.class.getName()
                .replace('.', '/') + ".class"));

        try (VerifiedDeploymentSet deployments = signed(image)) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            StreamConnectorProviderContract contract = registry.requireContract(
                    StreamConnectorProviderContract.Kind.SOURCE,
                    PostgresConnectorContract.PROVIDER_KEY);
            assertEquals(PostgresConnectorContract.contract(), contract);
            Class<?> provider = deployments.resolveTrustedClass(
                    PostgresConnectorProvider.class.getName());
            assertNotSame(PostgresConnectorProvider.class, provider);
            assertEquals(provider.getClassLoader(),
                    deployments.resolveTrustedClass("org.postgresql.Driver")
                            .getClassLoader());
        }
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
                "postgres-artifact-signer", signature.sign());
        return VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "postgres-artifact-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(32L * 1024 * 1024));
    }
}
