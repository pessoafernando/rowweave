package io.rowweave.deployment;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.regex.Pattern;

/** Canonical RWXXDEP1 manifest entry supplied by operations. */
public final class DeploymentManifest {
    public static final String FORMAT = "RWXXDEP1";
    private static final byte[] SIGNING_DOMAIN = "RWXX-DEPLOYMENT-V1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final Pattern IDENTITY = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    private final String deploymentId;
    private final Path absoluteJarPath;
    private final long jarLength;
    private final byte[] sha256;
    private final String signerKeyId;
    private final byte[] detachedSignature;

    public DeploymentManifest(String deploymentId, Path absoluteJarPath, long jarLength,
                              byte[] sha256, String signerKeyId, byte[] detachedSignature) {
        this.deploymentId = identity(deploymentId, "deploymentId");
        this.signerKeyId = identity(signerKeyId, "signerKeyId");
        if (absoluteJarPath == null || !absoluteJarPath.isAbsolute()
                || !absoluteJarPath.normalize().equals(absoluteJarPath)
                || absoluteJarPath.toString().length() > 4096
                || absoluteJarPath.toString().indexOf('\0') >= 0) {
            throw new IllegalArgumentException("absoluteJarPath must be bounded, absolute and normalized");
        }
        if (jarLength < 1L || jarLength > VerifiedDeploymentSet.MAX_COMPRESSED_JAR_BYTES) {
            throw new IllegalArgumentException("jarLength out of bounds");
        }
        if (sha256 == null || sha256.length != 32) {
            throw new IllegalArgumentException("sha256 must contain exactly 32 bytes");
        }
        if (detachedSignature == null || detachedSignature.length < 1
                || detachedSignature.length > 1024) {
            throw new IllegalArgumentException("detachedSignature length out of bounds");
        }
        this.absoluteJarPath = absoluteJarPath;
        this.jarLength = jarLength;
        this.sha256 = sha256.clone();
        this.detachedSignature = detachedSignature.clone();
    }

    public String deploymentId() { return deploymentId; }
    public Path absoluteJarPath() { return absoluteJarPath; }
    public long jarLength() { return jarLength; }
    public byte[] sha256() { return sha256.clone(); }
    public String signerKeyId() { return signerKeyId; }
    public byte[] detachedSignature() { return detachedSignature.clone(); }

    public static byte[] signingPayload(String deploymentId, long jarLength, byte[] sha256) {
        String id = identity(deploymentId, "deploymentId");
        if (jarLength < 1L || jarLength > VerifiedDeploymentSet.MAX_COMPRESSED_JAR_BYTES
                || sha256 == null || sha256.length != 32) {
            throw new IllegalArgumentException("invalid deployment signing identity");
        }
        byte[] idBytes = id.getBytes(StandardCharsets.US_ASCII);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    SIGNING_DOMAIN.length + 4 + idBytes.length + 8 + 32);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.write(SIGNING_DOMAIN);
                out.writeInt(idBytes.length);
                out.write(idBytes);
                out.writeLong(jarLength);
                out.write(sha256);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    private static String identity(String value, String name) {
        if (value == null || !IDENTITY.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must match [A-Za-z0-9._-]{1,128}");
        }
        return value;
    }
}
