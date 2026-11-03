package io.rowweave.deployment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.interfaces.RSAKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Immutable signer public-key registry loaded from an operator-owned read-only root. */
public final class TrustedSignerRegistry {
    private static final int MAX_KEY_BYTES = 8192;
    private static final int MAX_KEYS = 4096;
    private static final Pattern IDENTITY = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final Set<PosixFilePermission> WRITES = EnumSet.of(
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.OTHERS_WRITE);

    private final Map<String, PublicKey> signerKeys;

    private TrustedSignerRegistry(Map<String, PublicKey> signerKeys) {
        this.signerKeys = signerKeys;
    }

    public static TrustedSignerRegistry load(Path trustRoot, Map<String, Path> configuredKeys) {
        if (trustRoot == null || configuredKeys == null) {
            throw new IllegalArgumentException("trust root and signer keys are required");
        }
        if (configuredKeys.size() > MAX_KEYS) {
            throw failure("SIGNER_KEY_LIMIT_EXCEEDED", "too many signer keys");
        }
        Path root = canonicalAbsolute(trustRoot, "TRUST_ROOT_INVALID");
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(root)) {
            throw failure("TRUST_ROOT_INVALID", "trust root must be a non-symlink directory");
        }
        requireReadOnly(root, "TRUST_ROOT_WRITABLE");
        Map<String, PublicKey> accepted = new LinkedHashMap<>();
        for (Map.Entry<String, Path> entry : configuredKeys.entrySet()) {
            String id = entry.getKey();
            if (id == null || !IDENTITY.matcher(id).matches()) {
                throw failure("SIGNER_KEY_ID_INVALID", "invalid signer key id");
            }
            Path keyPath = canonicalAbsolute(entry.getValue(), "SIGNER_KEY_PATH_INVALID");
            if (!root.equals(keyPath.getParent())) {
                throw failure("SIGNER_KEY_OUTSIDE_TRUST_ROOT", id);
            }
            if (!Files.isRegularFile(keyPath, LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(keyPath)) {
                throw failure("SIGNER_KEY_PATH_INVALID", id);
            }
            requireReadOnly(keyPath, "SIGNER_KEY_WRITABLE");
            byte[] encoded = readBounded(keyPath);
            PublicKey key = decodeRsa(id, encoded);
            accepted.put(id, key);
        }
        return new TrustedSignerRegistry(Collections.unmodifiableMap(accepted));
    }

    public Map<String, PublicKey> signerKeys() { return signerKeys; }

    static void requireReadOnlyChild(Path trustRoot, Path source, String code) {
        Path root = canonicalAbsolute(trustRoot, code);
        Path child = canonicalAbsolute(source, code);
        if (!root.equals(child.getParent())
                || !Files.isRegularFile(child, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(child)) {
            throw failure(code, "trusted file must be a direct non-symlink child of trust root");
        }
        requireReadOnly(root, "TRUST_ROOT_WRITABLE");
        requireReadOnly(child, code);
    }

    private static byte[] readBounded(Path source) {
        try (InputStream input = Files.newInputStream(source)) {
            byte[] bounded = new byte[MAX_KEY_BYTES + 1];
            int count = 0;
            while (count < bounded.length) {
                int read = input.read(bounded, count, bounded.length - count);
                if (read < 0) break;
                if (read == 0) continue;
                count += read;
            }
            if (count < 1 || count > MAX_KEY_BYTES || input.read() != -1) {
                throw failure("SIGNER_KEY_TOO_LARGE", source.toString());
            }
            return java.util.Arrays.copyOf(bounded, count);
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new DeploymentValidationException("SIGNER_KEY_READ_FAILED",
                    source.toString(), failure);
        }
    }

    private static PublicKey decodeRsa(String id, byte[] encoded) {
        try {
            PublicKey key = KeyFactory.getInstance("RSA")
                    .generatePublic(new X509EncodedKeySpec(encoded));
            if (!(key instanceof RSAKey) || ((RSAKey) key).getModulus().bitLength() < 3072) {
                throw failure("DEPLOYMENT_SIGNER_KEY_TOO_WEAK", id);
            }
            if (!java.util.Arrays.equals(encoded, key.getEncoded())) {
                throw failure("SIGNER_KEY_INVALID", id);
            }
            return key;
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (GeneralSecurityException failure) {
            throw new DeploymentValidationException("SIGNER_KEY_INVALID", id, failure);
        }
    }

    private static Path canonicalAbsolute(Path value, String code) {
        if (value == null || !value.isAbsolute() || !value.normalize().equals(value)
                || value.toString().length() > 4096 || value.toString().indexOf('\0') >= 0) {
            throw failure(code, "path must be bounded, absolute and normalized");
        }
        return value;
    }

    private static void requireReadOnly(Path source, String code) {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(
                    source, LinkOption.NOFOLLOW_LINKS);
            for (PosixFilePermission write : WRITES) {
                if (permissions.contains(write)) throw failure(code, source.toString());
            }
            if (Files.isWritable(source)) throw failure(code, source.toString());
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (UnsupportedOperationException | IOException failure) {
            throw new DeploymentValidationException(code,
                    "cannot prove runtime identity read-only access: " + source, failure);
        }
    }

    private static DeploymentValidationException failure(String code, String message) {
        return new DeploymentValidationException(code, message);
    }
}
