package io.rowweave.deployment;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical ZIP parser must fail closed before any loader exists. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeploymentArchiveScannerTest {
    private KeyPair signer;

    @BeforeAll
    void createSigner() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        signer = generator.generateKeyPair();
    }

    @Test
    void validDescriptorArchiveBuildsDigestAndClassIndex(@TempDir Path temp) throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("app/Foo.class", new byte[]{1, 2, 3, 4});
        entries.put("app/config.json", "{}".getBytes(StandardCharsets.UTF_8));
        entries.put("app/empty.txt", new byte[0]);
        byte[] jar = jar(entries);

        try (VerifiedDeploymentSet set = load(temp, "app", jar, 1_000_000L)) {
            VerifiedDeploymentImage image = set.image("app");
            assertEquals(3, image.entryCount());
            assertEquals(Collections.singleton("app.Foo"), image.classNames());
            assertTrue(image.hasEntry("app/config.json"));
            assertEquals(hex(sha256(entries.get("app/config.json"))),
                    image.entryDigest("app/config.json"));
        }
    }

    @Test
    void zip64EncryptionAndTrailingBytesAreRejected(@TempDir Path temp) throws Exception {
        byte[] valid = jar(Collections.singletonMap("a.txt", new byte[]{1, 2, 3}));

        byte[] zip64 = valid.clone();
        int eocd = findLast(zip64, 0x06054b50);
        putU16(zip64, eocd + 8, 0xffff);
        putU16(zip64, eocd + 10, 0xffff);
        assertRejected(temp, "zip64", zip64, "DEPLOYMENT_ZIP_FORMAT_INVALID");

        byte[] encrypted = valid.clone();
        int local = findFirst(encrypted, 0x04034b50);
        int central = findFirst(encrypted, 0x02014b50);
        putU16(encrypted, local + 6, getU16(encrypted, local + 6) | 1);
        putU16(encrypted, central + 8, getU16(encrypted, central + 8) | 1);
        assertRejected(temp, "encrypted", encrypted, "DEPLOYMENT_ARCHIVE_FORBIDDEN");

        assertRejected(temp, "trailing", Arrays.copyOf(valid, valid.length + 1),
                "DEPLOYMENT_ZIP_FORMAT_INVALID");
    }

    @Test
    void namesMustBeStrictCanonicalUniqueUtf8(@TempDir Path temp) throws Exception {
        assertRejected(temp, "dotdot", jar(Collections.singletonMap("app/../x.txt", new byte[]{1})),
                "DEPLOYMENT_ENTRY_NAME_INVALID");
        assertRejected(temp, "backslash", jar(Collections.singletonMap("app\\x.txt", new byte[]{1})),
                "DEPLOYMENT_ENTRY_NAME_INVALID");

        byte[] invalidUtf8 = jar(Collections.singletonMap("x.txt", new byte[]{1}));
        replaceAll(invalidUtf8, "x.txt".getBytes(StandardCharsets.US_ASCII),
                new byte[]{(byte) 0xc0, '.', 't', 'x', 't'});
        assertRejected(temp, "utf8", invalidUtf8, "DEPLOYMENT_ENTRY_NAME_INVALID");

        Map<String, byte[]> distinct = new LinkedHashMap<>();
        distinct.put("a.txt", new byte[]{1});
        distinct.put("b.txt", new byte[]{2});
        byte[] duplicate = jar(distinct);
        assertEquals(2, replaceAll(duplicate, "b.txt".getBytes(StandardCharsets.US_ASCII),
                "a.txt".getBytes(StandardCharsets.US_ASCII)));
        assertRejected(temp, "duplicate", duplicate, "DEPLOYMENT_DUPLICATE_ENTRY");
    }

    @Test
    void runtimeNamespaceMultiReleaseNativeAndNestedArchivesAreForbidden(@TempDir Path temp)
            throws Exception {
        assertRejected(temp, "namespace",
                jar(Collections.singletonMap("io/rowweave/Evil.class", new byte[]{1})),
                "DEPLOYMENT_FORBIDDEN_NAMESPACE");
        assertRejected(temp, "jdk-namespace",
                jar(Collections.singletonMap("java/lang/Evil.class", new byte[]{1})),
                "DEPLOYMENT_FORBIDDEN_NAMESPACE");
        assertRejected(temp, "multi-release",
                jar(Collections.singletonMap("META-INF/versions/17/app/Foo.class", new byte[]{1})),
                "DEPLOYMENT_ARCHIVE_FORBIDDEN");
        assertRejected(temp, "native",
                jar(Collections.singletonMap("lib/native.so", new byte[]{1})),
                "DEPLOYMENT_ARCHIVE_FORBIDDEN");
        assertRejected(temp, "nested",
                jar(Collections.singletonMap("lib/dependency.jar", new byte[]{1})),
                "DEPLOYMENT_ARCHIVE_FORBIDDEN");
    }

    @Test
    void signedConnectorExtensionNamespaceIsAllowedButCoreRemainsForbidden(
            @TempDir Path temp) throws Exception {
        byte[] connector = jar(Collections.singletonMap(
                "io/rowweave/connector/s3/Provider.class",
                new byte[] {1, 2, 3}));
        try (VerifiedDeploymentSet set = load(
                temp, "connector", connector, 1_000_000L)) {
            assertTrue(set.image("connector").classNames().contains(
                    "io.rowweave.connector.s3.Provider"));
        }

        assertRejected(temp, "connector-lookalike",
                jar(Collections.singletonMap(
                        "io/rowweave/connectors/s3/Provider.class",
                        new byte[] {1})),
                "DEPLOYMENT_FORBIDDEN_NAMESPACE");
        assertRejected(temp, "connector-parent",
                jar(Collections.singletonMap(
                        "io/rowweave/connector.class", new byte[] {1})),
                "DEPLOYMENT_FORBIDDEN_NAMESPACE");
    }

    @Test
    void expandedLimitCompressionRatioAndCrcAreEnforcedWithAtomicBudgetRelease(@TempDir Path temp)
            throws Exception {
        byte[] oversized = jar(Collections.singletonMap("large.bin", new byte[]{1}));
        int central = findFirst(oversized, 0x02014b50);
        putU32(oversized, central + 24, 16L * 1024L * 1024L + 1L);
        assertRejected(temp, "oversized", oversized, "DEPLOYMENT_ENTRY_LIMIT_EXCEEDED");

        byte[] zeros = new byte[1024 * 1024];
        assertRejected(temp, "ratio", jar(Collections.singletonMap("zeros.bin", zeros)),
                "DEPLOYMENT_COMPRESSION_RATIO_EXCEEDED");

        byte[] corrupt = jar(Collections.singletonMap("crc.bin", randomBytes(4096)));
        int local = findFirst(corrupt, 0x04034b50);
        int data = local + 30 + getU16(corrupt, local + 26) + getU16(corrupt, local + 28);
        corrupt[data + 3] ^= 1;
        assertRejected(temp, "crc", corrupt, "DEPLOYMENT_ENTRY_INTEGRITY_FAILED");
    }

    @Test
    void duplicateClassAcrossDeploymentsAndActiveEntryBudgetFailAtomically(@TempDir Path temp)
            throws Exception {
        byte[] one = jar(Collections.singletonMap("app/Foo.class", new byte[]{1}));
        byte[] two = jar(Collections.singletonMap("app/Foo.class", new byte[]{2}));
        Path onePath = temp.resolve("one.jar");
        Path twoPath = temp.resolve("two.jar");
        Files.write(onePath, one);
        Files.write(twoPath, two);
        DeploymentManifest first = manifest("one", onePath, one);
        DeploymentManifest second = manifest("two", twoPath, two);
        DeploymentMemoryBudget budget = DeploymentMemoryBudget.forTesting(1_000_000L);

        DeploymentValidationException duplicate = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(Arrays.asList(first, second),
                        Collections.singletonMap("key", signer.getPublic()), budget));
        assertEquals("DEPLOYMENT_DUPLICATE_CLASS", duplicate.code());
        assertEquals(0L, budget.usedBytes());

        byte[] content = randomBytes(8192);
        byte[] jar = jar(Collections.singletonMap("entry.bin", content));
        Path source = temp.resolve("budget.jar");
        Files.write(source, jar);
        DeploymentMemoryBudget tiny = DeploymentMemoryBudget.forTesting(jar.length + 2_000L);
        DeploymentValidationException exhausted = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(Collections.singletonList(
                                manifest("budget", source, jar)),
                        Collections.singletonMap("key", signer.getPublic()), tiny));
        assertEquals("DEPLOYMENT_MEMORY_BUDGET_EXCEEDED", exhausted.code());
        assertEquals(0L, tiny.usedBytes());
    }

    private VerifiedDeploymentSet load(Path temp, String id, byte[] jar, long budget) throws Exception {
        Path source = temp.resolve(id + ".jar");
        Files.write(source, jar);
        return VerifiedDeploymentSet.load(Collections.singletonList(manifest(id, source, jar)),
                Collections.singletonMap("key", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(budget));
    }

    private void assertRejected(Path temp, String id, byte[] jar, String code) throws Exception {
        Path source = temp.resolve(id + ".jar");
        Files.write(source, jar);
        DeploymentMemoryBudget budget = DeploymentMemoryBudget.forTesting(32L * 1024L * 1024L);
        DeploymentValidationException failure = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(Collections.singletonList(
                                manifest(id, source, jar)),
                        Collections.singletonMap("key", signer.getPublic()), budget));
        assertEquals(code, failure.code(), failure.toString());
        assertEquals(0L, budget.usedBytes());
    }

    private DeploymentManifest manifest(String id, Path source, byte[] jar) throws Exception {
        byte[] digest = sha256(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(id, jar.length, digest));
        return new DeploymentManifest(id, source.toAbsolutePath(), jar.length, digest,
                "key", signature.sign());
    }

    private static byte[] jar(Map<String, byte[]> entries) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry item = new JarEntry(entry.getKey());
                item.setTime(0L);
                jar.putNextEntry(item);
                jar.write(entry.getValue());
                jar.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new Random(7L).nextBytes(bytes);
        return bytes;
    }

    private static byte[] sha256(byte[] bytes) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    private static String hex(byte[] bytes) {
        StringBuilder text = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) text.append(String.format("%02x", value & 0xff));
        return text.toString();
    }

    private static int findFirst(byte[] bytes, int signature) {
        for (int i = 0; i <= bytes.length - 4; i++) if (getU32(bytes, i) == (signature & 0xffffffffL)) return i;
        throw new AssertionError("signature not found: " + Integer.toHexString(signature));
    }

    private static int findLast(byte[] bytes, int signature) {
        for (int i = bytes.length - 4; i >= 0; i--) if (getU32(bytes, i) == (signature & 0xffffffffL)) return i;
        throw new AssertionError("signature not found: " + Integer.toHexString(signature));
    }

    private static int replaceAll(byte[] bytes, byte[] from, byte[] to) {
        int count = 0;
        for (int i = 0; i <= bytes.length - from.length; i++) {
            boolean same = true;
            for (int j = 0; j < from.length; j++) if (bytes[i + j] != from[j]) { same = false; break; }
            if (same) {
                System.arraycopy(to, 0, bytes, i, to.length);
                count++;
                i += from.length - 1;
            }
        }
        return count;
    }

    private static int getU16(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8);
    }

    private static long getU32(byte[] bytes, int offset) {
        return (getU16(bytes, offset) & 0xffffL) | ((long) getU16(bytes, offset + 2) << 16);
    }

    private static void putU16(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
    }

    private static void putU32(byte[] bytes, int offset, long value) {
        putU16(bytes, offset, (int) value);
        putU16(bytes, offset + 2, (int) (value >>> 16));
    }
}
