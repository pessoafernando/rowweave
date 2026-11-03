package io.rowweave.deployment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPairGenerator;
import java.util.Collections;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** signer keys only come from a bounded read-only trust root. */
class TrustedSignerRegistryTest {

    @Test
    void loadsCanonicalStrongRsaKeyFromReadOnlyTrustRoot(@TempDir Path temp) throws Exception {
        Path root = temp.resolve("trust");
        Files.createDirectory(root);
        Path key = root.resolve("release-key.der");
        Files.write(key, rsa(3072));
        try {
            makeReadOnly(key, false);
            makeReadOnly(root, true);
            TrustedSignerRegistry registry = TrustedSignerRegistry.load(root,
                    Collections.singletonMap("release-key", key));
            assertEquals(1, registry.signerKeys().size());
            assertEquals("RSA", registry.signerKeys().get("release-key").getAlgorithm());
        } finally {
            makeWritable(root, true);
            makeWritable(key, false);
        }
    }

    @Test
    void writableRootEscapeWeakMalformedAndOversizeKeysFailClosed(@TempDir Path temp)
            throws Exception {
        Path root = temp.resolve("trust");
        Files.createDirectory(root);
        Path strong = root.resolve("strong.der");
        Files.write(strong, rsa(3072));
        assertFailure("TRUST_ROOT_WRITABLE", root, strong);

        Path outside = temp.resolve("outside.der");
        Files.write(outside, rsa(3072));
        try {
            makeReadOnly(strong, false);
            makeReadOnly(root, true);
            assertFailure("SIGNER_KEY_OUTSIDE_TRUST_ROOT", root, outside);

            Path weak = root.resolve("weak.der");
            makeWritable(root, true);
            Files.write(weak, rsa(2048));
            makeReadOnly(weak, false);
            makeReadOnly(root, true);
            assertFailure("DEPLOYMENT_SIGNER_KEY_TOO_WEAK", root, weak);

            Path malformed = root.resolve("malformed.der");
            makeWritable(root, true);
            Files.write(malformed, new byte[]{1, 2, 3});
            makeReadOnly(malformed, false);
            makeReadOnly(root, true);
            assertFailure("SIGNER_KEY_INVALID", root, malformed);

            Path oversize = root.resolve("oversize.der");
            makeWritable(root, true);
            Files.write(oversize, new byte[8193]);
            makeReadOnly(oversize, false);
            makeReadOnly(root, true);
            assertFailure("SIGNER_KEY_TOO_LARGE", root, oversize);
        } finally {
            makeWritable(root, true);
            for (Path file : new Path[]{strong, outside, root.resolve("weak.der"),
                    root.resolve("malformed.der"), root.resolve("oversize.der")}) {
                if (Files.exists(file)) makeWritable(file, false);
            }
        }
    }

    private static void assertFailure(String code, Path root, Path key) {
        DeploymentValidationException failure = assertThrows(DeploymentValidationException.class,
                () -> TrustedSignerRegistry.load(root,
                        Collections.singletonMap("release-key", key)));
        assertEquals(code, failure.code());
    }

    private static byte[] rsa(int bits) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(bits);
        return generator.generateKeyPair().getPublic().getEncoded();
    }

    private static void makeReadOnly(Path target, boolean directory) throws Exception {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ));
    }

    private static void makeWritable(Path target, boolean directory) throws Exception {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }
}
