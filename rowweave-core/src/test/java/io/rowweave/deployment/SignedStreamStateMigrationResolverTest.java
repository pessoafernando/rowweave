package io.rowweave.deployment;

import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamStateMigrationBinding;
import io.rowweave.stream.runtime.StreamStateMigrationAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.Arrays;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SignedStreamStateMigrationResolverTest {
    private static final String DEPLOYMENT_ID = "state-migration";
    private static final String CLASS_NAME =
            "rowweave.conformance.UppercaseStateMigration";
    private static final String NON_DETERMINISTIC_CLASS =
            "rowweave.conformance.AlternatingStateMigration";

    @TempDir
    Path temporaryDirectory;

    @Test
    void onlyExactSignedDeploymentBindingCanResolveAdapter() throws Exception {
        byte[] jar = compiledJar(temporaryDirectory.resolve("compile"));
        byte[] jarDigest = MessageDigest.getInstance("SHA-256").digest(jar);
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        Path jarPath = temporaryDirectory.resolve("migration.jar").toAbsolutePath();
        Files.write(jarPath, jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                DEPLOYMENT_ID, jar.length, jarDigest));
        DeploymentManifest manifest = new DeploymentManifest(
                DEPLOYMENT_ID, jarPath, jar.length, jarDigest,
                "migration-signer", signature.sign());

        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "migration-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(8L * 1024L * 1024L))) {
            StreamStateMigrationBinding exact = binding(
                    deployments.deploymentSetId(), hex(jarDigest), CLASS_NAME);
            SignedStreamStateMigrationResolver staticOnly =
                    new SignedStreamStateMigrationResolver(
                            deployments,
                            SignedStreamStateMigrationResolver.Role.STATIC_ONLY);
            IllegalStateException runtimeRequired = assertThrows(
                    IllegalStateException.class,
                    () -> staticOnly.resolve(exact));
            assertEquals("STATE_MIGRATION_RUNTIME_REQUIRED",
                    runtimeRequired.getMessage());

            SignedStreamStateMigrationResolver resolver =
                    new SignedStreamStateMigrationResolver(
                            deployments,
                            SignedStreamStateMigrationResolver.Role.WORKER);
            StreamStateMigrationAdapter adapter = resolver.resolve(exact);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            adapter.migrate(new ByteArrayInputStream(bytes("readout")), output);
            assertArrayEquals(bytes("READOUT"), output.toByteArray());

            assertCode("STATE_MIGRATION_DEPLOYMENT_MISMATCH",
                    () -> resolver.resolve(binding(
                            digest('f'), hex(jarDigest), CLASS_NAME)));
            assertCode("STATE_MIGRATION_DEPLOYMENT_MISMATCH",
                    () -> resolver.resolve(binding(
                            deployments.deploymentSetId(), digest('e'),
                            CLASS_NAME)));
            assertCode("STATE_MIGRATION_CLASS_INVALID",
                    () -> resolver.resolve(binding(
                            deployments.deploymentSetId(), hex(jarDigest),
                            "rowweave.conformance.MissingMigration")));
        }
    }

    static Fixture openFixture(Path directory) throws Exception {
        Files.createDirectories(directory);
        byte[] jar = compiledJar(directory.resolve("compile"));
        byte[] jarDigest = MessageDigest.getInstance("SHA-256").digest(jar);
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        Path jarPath = directory.resolve("migration.jar").toAbsolutePath();
        Files.write(jarPath, jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                DEPLOYMENT_ID, jar.length, jarDigest));
        DeploymentManifest manifest = new DeploymentManifest(
                DEPLOYMENT_ID, jarPath, jar.length, jarDigest,
                "migration-signer", signature.sign());
        VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "migration-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(8L * 1024L * 1024L));
        return new Fixture(deployments, hex(jarDigest));
    }

    static final class Fixture implements AutoCloseable {
        private final VerifiedDeploymentSet deployments;
        private final String jarDigest;

        private Fixture(
                VerifiedDeploymentSet deployments, String jarDigest) {
            this.deployments = deployments;
            this.jarDigest = jarDigest;
        }

        SignedStreamStateMigrationResolver resolver() {
            return new SignedStreamStateMigrationResolver(
                    deployments,
                    SignedStreamStateMigrationResolver.Role.WORKER);
        }

        StreamStateMigrationBinding binding() {
            return SignedStreamStateMigrationResolverTest.binding(
                    deployments.deploymentSetId(), jarDigest, CLASS_NAME);
        }

        StreamStateMigrationBinding nonDeterministicBinding() {
            return SignedStreamStateMigrationResolverTest.binding(
                    deployments.deploymentSetId(), jarDigest,
                    NON_DETERMINISTIC_CLASS);
        }

        @Override
        public void close() {
            deployments.close();
        }
    }

    private static StreamStateMigrationBinding binding(
            String deploymentSetId, String jarDigest, String className) {
        return new StreamStateMigrationBinding(
                "ledger", state("decimal-v1", 1), state("decimal-v2", 2),
                deploymentSetId, DEPLOYMENT_ID, jarDigest, className,
                sha256Hex(bytes("READOUT")), 4096L);
    }

    private static StreamStateDescriptor state(
            String serializer, int schemaVersion) {
        return new StreamStateDescriptor(
                "readout", StreamStateDescriptor.Kind.KEYED_VALUE,
                serializer, schemaVersion,
                StreamStateDescriptor.TtlMode.NONE, 0L);
    }

    private static byte[] compiledJar(Path root) throws Exception {
        Path source = root.resolve("src").resolve(
                CLASS_NAME.replace('.', '/') + ".java");
        Path alternating = root.resolve("src").resolve(
                NON_DETERMINISTIC_CLASS.replace('.', '/') + ".java");
        Path classes = root.resolve("classes");
        Files.createDirectories(source.getParent());
        Files.createDirectories(classes);
        Files.write(source, adapterSource().getBytes(StandardCharsets.UTF_8));
        Files.write(alternating,
                alternatingAdapterSource().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString(),
                alternating.toString()) != 0) {
            throw new AssertionError("signed migration adapter compile failed");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes)) {
            for (String className : Arrays.asList(
                    CLASS_NAME, NON_DETERMINISTIC_CLASS)) {
                JarEntry entry = new JarEntry(
                        className.replace('.', '/') + ".class");
                entry.setTime(0L);
                jar.putNextEntry(entry);
                jar.write(Files.readAllBytes(classes.resolve(
                        className.replace('.', '/') + ".class")));
                jar.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static String adapterSource() {
        return "package rowweave.conformance;"
                + "public final class UppercaseStateMigration implements "
                + "io.rowweave.stream.runtime.StreamStateMigrationAdapter{"
                + "public UppercaseStateMigration(){}"
                + "public void migrate(java.io.InputStream in,"
                + "java.io.OutputStream out)throws Exception{"
                + "int value;while((value=in.read())!=-1){"
                + "out.write(java.lang.Character.toUpperCase((char)value));}}}";
    }

    private static String alternatingAdapterSource() {
        return "package rowweave.conformance;"
                + "public final class AlternatingStateMigration implements "
                + "io.rowweave.stream.runtime.StreamStateMigrationAdapter{"
                + "private static final java.util.concurrent.atomic.AtomicInteger "
                + "CALLS=new java.util.concurrent.atomic.AtomicInteger();"
                + "public AlternatingStateMigration(){}"
                + "public void migrate(java.io.InputStream in,"
                + "java.io.OutputStream out)throws Exception{"
                + "boolean upper=(CALLS.getAndIncrement()&1)==0;int value;"
                + "while((value=in.read())!=-1){out.write(upper?"
                + "java.lang.Character.toUpperCase((char)value):"
                + "java.lang.Character.toLowerCase((char)value));}}}";
    }

    private static void assertCode(String code, Runnable action) {
        DeploymentValidationException failure = assertThrows(
                DeploymentValidationException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String sha256Hex(byte[] value) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] result = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < value.length; index++) {
            result[index * 2] = digits[(value[index] >>> 4) & 0xf];
            result[index * 2 + 1] = digits[value[index] & 0xf];
        }
        return new String(result);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
