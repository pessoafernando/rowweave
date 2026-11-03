package io.rowweave.connector.s3;

import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.spi.StreamSourceProvider;
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

/** Product-image gate over the actual Maven-built deployment artifact. */
class S3DeploymentArtifactIT {
    private static final long MAX_IMAGE_BYTES = 64L * 1024L * 1024L;

    @Test
    void deploymentImageIsSelfContainedJava8AndAcceptedByProductionTrust()
            throws Exception {
        Path image = Paths.get(System.getProperty("s3.deployment.jar"))
                .toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(image), image.toString());
        assertTrue(Files.size(image) > 0L && Files.size(image) <= MAX_IMAGE_BYTES,
                "bounded deployment image");

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
                        && !name.startsWith("io/rowweave/connector/s3/"),
                        name);
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
                        assertEquals(0xca, header[0] & 0xff, name);
                        assertEquals(0xfe, header[1] & 0xff, name);
                        assertEquals(0xba, header[2] & 0xff, name);
                        assertEquals(0xbe, header[3] & 0xff, name);
                        int major = ((header[6] & 0xff) << 8)
                                | (header[7] & 0xff);
                        assertTrue(major <= 52,
                                name + " requires class major " + major);
                        if (name.startsWith(
                                "io/rowweave/connector/s3/")) {
                            assertEquals(52, major, name);
                        }
                    }
                }
            }
        }
        assertTrue(names.contains(
                S3DeploymentPackager.SOURCE_SERVICE_RESOURCE));
        assertTrue(names.contains(
                S3DeploymentPackager.SOURCE_CONTRACT_RESOURCE));
        assertTrue(names.contains(
                "software/amazon/awssdk/services/s3/S3Client.class"));
        assertTrue(names.contains(
                "com/fasterxml/jackson/core/JsonParser.class"));
        assertFalse(names.contains("io/rowweave/Source.class"));
        assertFalse(names.stream().anyMatch(
                name -> name.startsWith("org/slf4j/")),
                "the Worker-owned SLF4J API must not be isolated in the deployment image");
        assertFalse(names.contains(S3DeploymentPackager.class.getName()
                .replace('.', '/') + ".class"));

        try (VerifiedDeploymentSet deployments = signed(image)) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            StreamConnectorProviderContract contract = registry.requireContract(
                    StreamConnectorProviderContract.Kind.SOURCE,
                    S3ConnectorContract.PROVIDER_KEY);
            assertEquals(S3ConnectorContract.contract(), contract);

            Class<?> deployed = deployments.resolveTrustedClass(
                    S3ConnectorProvider.class.getName());
            assertNotSame(S3ConnectorProvider.class, deployed);
            assertTrue(StreamSourceProvider.class.isAssignableFrom(deployed));
            assertEquals(deployed.getClassLoader(),
                    deployments.resolveTrustedClass(
                            "software.amazon.awssdk.services.s3.S3Client")
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
                S3ConnectorContract.DEPLOYMENT_ID, image.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                S3ConnectorContract.DEPLOYMENT_ID,
                jar.toAbsolutePath().normalize(), image.length, digest,
                "s3-artifact-test", signature.sign());
        return VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "s3-artifact-test", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(128L * 1024L * 1024L));
    }
}
