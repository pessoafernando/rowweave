package io.rowweave.deployment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
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
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** signed source becomes one immutable, budget-owned image. */
class TrustedDeploymentSetTest {

    @Test
    void validSignatureCreatesPathIndependentImmutableImageAndStableSetId(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] jar = jar("app/resource.txt", "version-one".getBytes("UTF-8"));
        Path firstPath = temp.resolve("first.jar");
        Path secondPath = temp.resolve("elsewhere/second.jar");
        Files.createDirectories(secondPath.getParent());
        Files.write(firstPath, jar);
        Files.write(secondPath, jar);
        DeploymentManifest first = manifest("orders-v1", firstPath, jar, "release-key", signer);
        DeploymentManifest second = manifest("orders-v1", secondPath, jar, "release-key", signer);
        DeploymentMemoryBudget firstBudget = DeploymentMemoryBudget.forTesting(jar.length + 16_384L);
        DeploymentMemoryBudget secondBudget = DeploymentMemoryBudget.forTesting(jar.length + 16_384L);

        String firstSetId;
        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(
                Collections.singletonList(first),
                Collections.singletonMap("release-key", signer.getPublic()), firstBudget)) {
            firstSetId = set.deploymentSetId();
            assertTrue(firstSetId.matches("[0-9a-f]{64}"), firstSetId);
            assertTrue(set.image("orders-v1").compressedContentEquals(jar));
            assertTrue(firstBudget.usedBytes() >= jar.length);

            byte[] replacement = jar.clone();
            replacement[replacement.length - 1] ^= 1;
            Files.write(firstPath, replacement);
            assertTrue(set.image("orders-v1").compressedContentEquals(jar),
                    "verified image must never reread the source path");
        }
        assertEquals(0L, firstBudget.usedBytes());

        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(
                Collections.singletonList(second),
                Collections.singletonMap("release-key", signer.getPublic()), secondBudget)) {
            assertEquals(firstSetId, set.deploymentSetId(),
                    "absolute install path and detached signature bytes are not set identity");
        }
        assertEquals(0L, secondBudget.usedBytes());
    }

    @Test
    void digestSignatureWeakKeyAndSymlinkFailClosedAndReleaseAllBudget(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] jar = jar("app/resource.txt", new byte[]{1, 2, 3});
        Path source = temp.resolve("app.jar");
        Files.write(source, jar);

        assertFailureWithZeroBudget("DEPLOYMENT_DIGEST_MISMATCH",
                manifest("app", source, mutate(jar), "key", signer), signer, jar.length);

        DeploymentManifest valid = manifest("app", source, jar, "key", signer);
        byte[] badSignature = valid.detachedSignature();
        badSignature[0] ^= 1;
        assertFailureWithZeroBudget("DEPLOYMENT_SIGNATURE_INVALID",
                new DeploymentManifest(valid.deploymentId(), valid.absoluteJarPath(),
                        valid.jarLength(), valid.sha256(), valid.signerKeyId(), badSignature),
                signer, jar.length);

        KeyPair weak = keyPair(2048);
        assertFailureWithZeroBudget("DEPLOYMENT_SIGNER_KEY_TOO_WEAK",
                manifest("app", source, jar, "key", weak), weak, jar.length);

        Path link = temp.resolve("link.jar");
        Files.createSymbolicLink(link, source.getFileName());
        assertFailureWithZeroBudget("DEPLOYMENT_SOURCE_NOT_REGULAR",
                manifest("app", link.toAbsolutePath(), jar, "key", signer), signer, jar.length);
    }

    @Test
    void duplicateIdentityInvalidTextAndBudgetExhaustionAreDeterministic(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] jar = jar("x.txt", new byte[]{7});
        Path source = temp.resolve("app.jar");
        Files.write(source, jar);
        DeploymentManifest manifest = manifest("app", source, jar, "key", signer);
        Map<String, java.security.PublicKey> keys = Collections.singletonMap(
                "key", signer.getPublic());

        DeploymentMemoryBudget duplicateBudget = DeploymentMemoryBudget.forTesting(1_000_000L);
        DeploymentValidationException duplicate = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(Arrays.asList(manifest, manifest), keys,
                        duplicateBudget));
        assertEquals("DUPLICATE_DEPLOYMENT_ID", duplicate.code());
        assertEquals(0L, duplicateBudget.usedBytes());

        assertThrows(IllegalArgumentException.class,
                () -> new DeploymentManifest("../app", source.toAbsolutePath(), jar.length,
                        sha256(jar), "key", new byte[]{1}));

        DeploymentMemoryBudget tiny = DeploymentMemoryBudget.forTesting(jar.length - 1L);
        DeploymentValidationException exhausted = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(
                        Collections.singletonList(manifest), keys, tiny));
        assertEquals("DEPLOYMENT_MEMORY_BUDGET_EXCEEDED", exhausted.code());
        assertEquals(0L, tiny.usedBytes());
    }

    @Test
    void setIdentityIsCanonicalByDeploymentIdAndChangesWithSignedIdentity(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] one = jar("one.txt", new byte[]{1});
        byte[] two = jar("two.txt", new byte[]{2});
        Path onePath = temp.resolve("one.jar");
        Path twoPath = temp.resolve("two.jar");
        Files.write(onePath, one);
        Files.write(twoPath, two);
        DeploymentManifest a = manifest("a", onePath, one, "key", signer);
        DeploymentManifest b = manifest("b", twoPath, two, "key", signer);
        Map<String, java.security.PublicKey> keys = Collections.singletonMap("key", signer.getPublic());

        String ordered;
        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(Arrays.asList(a, b), keys,
                DeploymentMemoryBudget.forTesting(1_000_000L))) {
            ordered = set.deploymentSetId();
        }
        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(Arrays.asList(b, a), keys,
                DeploymentMemoryBudget.forTesting(1_000_000L))) {
            assertEquals(ordered, set.deploymentSetId());
        }
        DeploymentManifest changedSignerId = manifest("a", onePath, one, "other-key", signer);
        Map<String, java.security.PublicKey> twoNames = new LinkedHashMap<>();
        twoNames.put("key", signer.getPublic());
        twoNames.put("other-key", signer.getPublic());
        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(
                Arrays.asList(changedSignerId, b), twoNames,
                DeploymentMemoryBudget.forTesting(1_000_000L))) {
            assertNotEquals(ordered, set.deploymentSetId());
        }
    }

    @Test
    void laterImageFailureAtomicallyReleasesEarlierImageAndIndex(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] firstJar = jar("first.txt", new byte[]{1});
        byte[] secondJar = jar("second.txt", new byte[]{2});
        Path firstPath = temp.resolve("first.jar");
        Path secondPath = temp.resolve("second.jar");
        Files.write(firstPath, firstJar);
        Files.write(secondPath, secondJar);
        DeploymentManifest first = manifest("first", firstPath, firstJar, "key", signer);
        DeploymentManifest badSecond = manifest(
                "second", secondPath, mutate(secondJar), "key", signer);
        DeploymentMemoryBudget budget = DeploymentMemoryBudget.forTesting(1_000_000L);

        DeploymentValidationException failure = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(Arrays.asList(first, badSecond),
                        Collections.singletonMap("key", signer.getPublic()), budget));

        assertEquals("DEPLOYMENT_DIGEST_MISMATCH", failure.code());
        assertEquals(0L, budget.usedBytes(),
                "compressed/index charges from every earlier image must roll back");
    }

    @Test
    void sourceReplacementOrSameLengthMutationClosesOnlyNewAdmission(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] jar = jar("app/resource.txt", "stable-image".getBytes("UTF-8"));
        Path source = temp.resolve("app.jar");
        Files.write(source, jar);
        DeploymentMemoryBudget budget = DeploymentMemoryBudget.forTesting(1_000_000L);

        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest("app", source, jar, "key", signer)),
                Collections.singletonMap("key", signer.getPublic()), budget)) {
            set.requireAdmissionOpen();
            assertTrue(set.probeSources());

            byte[] changed = jar.clone();
            changed[changed.length / 2] ^= 1;
            Files.write(source, changed);

            assertFalse(set.probeSources());
            DeploymentValidationException closed = assertThrows(
                    DeploymentValidationException.class, set::requireAdmissionOpen);
            assertEquals("DEPLOYMENT_SOURCE_CHANGED", closed.code());
            assertTrue(set.image("app").compressedContentEquals(jar),
                    "an admitted attempt keeps using its immutable verified image");
            assertFalse(set.probeSources(), "closed admission is monotonic");
        }
        assertEquals(0L, budget.usedBytes());
    }

    @Test
    void pathReplacementAndSameInodeTruncationAreBothDetected(@TempDir Path temp)
            throws Exception {
        KeyPair signer = keyPair(3072);
        byte[] jar = jar("app/resource.txt", "source-probe".getBytes("UTF-8"));

        Path replaced = temp.resolve("replaced.jar");
        Files.write(replaced, jar);
        DeploymentMemoryBudget replacementBudget = DeploymentMemoryBudget.forTesting(1_000_000L);
        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest("replace", replaced, jar, "key", signer)),
                Collections.singletonMap("key", signer.getPublic()), replacementBudget)) {
            Path candidate = temp.resolve("candidate.jar");
            Files.write(candidate, jar);
            Files.move(candidate, replaced, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            assertFalse(set.probeSources(), "pathname replacement must change source identity");
            assertTrue(set.image("replace").compressedContentEquals(jar));
        }
        assertEquals(0L, replacementBudget.usedBytes());

        Path truncated = temp.resolve("truncated.jar");
        Files.write(truncated, jar);
        Object originalFileKey = Files.readAttributes(truncated,
                java.nio.file.attribute.BasicFileAttributes.class).fileKey();
        DeploymentMemoryBudget truncationBudget = DeploymentMemoryBudget.forTesting(1_000_000L);
        try (VerifiedDeploymentSet set = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest("truncate", truncated, jar, "key", signer)),
                Collections.singletonMap("key", signer.getPublic()), truncationBudget)) {
            try (java.nio.channels.SeekableByteChannel channel = Files.newByteChannel(truncated,
                    java.nio.file.StandardOpenOption.WRITE)) {
                channel.truncate(jar.length - 1L);
            }
            assertEquals(originalFileKey, Files.readAttributes(truncated,
                    java.nio.file.attribute.BasicFileAttributes.class).fileKey(),
                    "test must exercise same-inode truncation");
            assertFalse(set.probeSources());
            assertTrue(set.image("truncate").compressedContentEquals(jar));
        }
        assertEquals(0L, truncationBudget.usedBytes());
    }

    private static void assertFailureWithZeroBudget(
            String code, DeploymentManifest manifest, KeyPair signer, long jarLength) {
        DeploymentMemoryBudget budget = DeploymentMemoryBudget.forTesting(jarLength + 16_384L);
        DeploymentValidationException failure = assertThrows(DeploymentValidationException.class,
                () -> VerifiedDeploymentSet.load(Collections.singletonList(manifest),
                        Collections.singletonMap("key", signer.getPublic()), budget));
        assertEquals(code, failure.code());
        assertEquals(0L, budget.usedBytes());
    }

    private static DeploymentManifest manifest(
            String id, Path path, byte[] jar, String signerId, KeyPair signer) throws Exception {
        byte[] digest = sha256(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(id, jar.length, digest));
        return new DeploymentManifest(id, path.toAbsolutePath(), jar.length, digest,
                signerId, signature.sign());
    }

    private static byte[] jar(String name, byte[] content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes)) {
            JarEntry entry = new JarEntry(name);
            entry.setTime(0L);
            jar.putNextEntry(entry);
            jar.write(content);
            jar.closeEntry();
        }
        return bytes.toByteArray();
    }

    private static KeyPair keyPair(int bits) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(bits);
        return generator.generateKeyPair();
    }

    private static byte[] sha256(byte[] bytes) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    private static byte[] mutate(byte[] bytes) {
        byte[] copy = bytes.clone();
        copy[copy.length / 2] ^= 1;
        return copy;
    }
}
