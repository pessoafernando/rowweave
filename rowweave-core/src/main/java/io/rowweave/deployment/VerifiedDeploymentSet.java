package io.rowweave.deployment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.RSAKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Atomically verified set of preinstalled signed deployment images. */
public final class VerifiedDeploymentSet implements AutoCloseable {
    private static final String EMPTY_SET_ID =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    static final long MAX_COMPRESSED_JAR_BYTES = 64L * 1024L * 1024L;
    private static final long MAX_SET_COMPRESSED_BYTES = 256L * 1024L * 1024L;
    private static final int MAX_DEPLOYMENTS = 4096;

    private final Map<String, VerifiedDeploymentImage> images;
    private final Map<String, VerifiedDeploymentImage> classOwners;
    private final String deploymentSetId;
    private final DeploymentMemoryBudget.Lease indexLease;
    private final DeploymentMemoryBudget.Lease classIndexLease;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean admissionOpen = new AtomicBoolean(true);

    private VerifiedDeploymentSet(Map<String, VerifiedDeploymentImage> images,
                                  Map<String, VerifiedDeploymentImage> classOwners,
                                  String deploymentSetId,
                                  DeploymentMemoryBudget.Lease indexLease,
                                  DeploymentMemoryBudget.Lease classIndexLease) {
        this.images = images;
        this.classOwners = classOwners;
        this.deploymentSetId = deploymentSetId;
        this.indexLease = indexLease;
        this.classIndexLease = classIndexLease;
    }

    public static VerifiedDeploymentSet load(
            List<DeploymentManifest> manifests, Map<String, PublicKey> signerKeys,
            DeploymentMemoryBudget budget) {
        if (manifests == null || signerKeys == null || budget == null) {
            throw new IllegalArgumentException("manifests, signer keys and budget are required");
        }
        if (manifests.size() > MAX_DEPLOYMENTS) {
            throw failure("DEPLOYMENT_LIMIT_EXCEEDED", "too many deployments");
        }
        if (manifests.isEmpty()) {
            return new VerifiedDeploymentSet(Collections.<String, VerifiedDeploymentImage>emptyMap(),
                    Collections.<String, VerifiedDeploymentImage>emptyMap(),
                    setId(Collections.<DeploymentManifest>emptyList()), null, null);
        }

        long indexBytes = indexCharge(manifests);
        DeploymentMemoryBudget.Lease indexLease = budget.reserve(indexBytes);
        DeploymentMemoryBudget.Lease classIndexLease = null;
        List<VerifiedDeploymentImage> accepted = new ArrayList<>(manifests.size());
        try {
            Set<String> ids = new HashSet<>();
            long compressedTotal = 0L;
            long expandedTotal = 0L;
            long entryTotal = 0L;
            long classTotal = 0L;
            for (DeploymentManifest manifest : manifests) {
                if (manifest == null) throw failure("INVALID_DEPLOYMENT_MANIFEST", "null manifest");
                if (!ids.add(manifest.deploymentId())) {
                    throw failure("DUPLICATE_DEPLOYMENT_ID", manifest.deploymentId());
                }
                compressedTotal = Math.addExact(compressedTotal, manifest.jarLength());
                if (compressedTotal > MAX_SET_COMPRESSED_BYTES) {
                    throw failure("DEPLOYMENT_LIMIT_EXCEEDED", "compressed set exceeds hard limit");
                }
                VerifiedDeploymentImage image = loadOne(manifest, signerKeys, budget);
                accepted.add(image);
                entryTotal = Math.addExact(entryTotal, image.entryCount());
                expandedTotal = Math.addExact(expandedTotal, image.expandedBytes());
                if (entryTotal > DeploymentArchiveScanner.MAX_ENTRIES
                        || expandedTotal > DeploymentArchiveScanner.MAX_EXPANDED_BYTES) {
                    throw failure("DEPLOYMENT_ENTRY_LIMIT_EXCEEDED", "deployment set exceeds entry/expanded limit");
                }
                classTotal = Math.addExact(classTotal, image.classNames().size());
                if (classTotal > DeploymentArchiveScanner.MAX_CLASSES) {
                    throw failure("DEPLOYMENT_ENTRY_LIMIT_EXCEEDED", "deployment set exceeds class limit");
                }
            }
            classIndexLease = budget.reserve(Math.addExact(128L, 256L * classTotal));
            List<VerifiedDeploymentImage> ordered = new ArrayList<>(accepted);
            ordered.sort(Comparator.comparing(image -> image.manifest().deploymentId()));
            Map<String, VerifiedDeploymentImage> byId = new LinkedHashMap<>();
            Map<String, VerifiedDeploymentImage> byClass = new LinkedHashMap<>();
            List<DeploymentManifest> canonical = new ArrayList<>(ordered.size());
            for (VerifiedDeploymentImage image : ordered) {
                byId.put(image.manifest().deploymentId(), image);
                canonical.add(image.manifest());
                for (String className : image.classNames()) {
                    if (byClass.put(className, image) != null) {
                        throw failure("DEPLOYMENT_DUPLICATE_CLASS", className);
                    }
                }
            }
            return new VerifiedDeploymentSet(Collections.unmodifiableMap(byId),
                    Collections.unmodifiableMap(byClass), setId(canonical), indexLease,
                    classIndexLease);
        } catch (RuntimeException | Error failure) {
            for (VerifiedDeploymentImage image : accepted) image.close();
            if (classIndexLease != null) classIndexLease.close();
            indexLease.close();
            throw failure;
        }
    }

    public static VerifiedDeploymentSet load(
            List<DeploymentManifest> manifests, TrustedSignerRegistry signerRegistry,
            DeploymentMemoryBudget budget) {
        if (signerRegistry == null) throw new IllegalArgumentException("signer registry is required");
        return load(manifests, signerRegistry.signerKeys(), budget);
    }

    public String deploymentSetId() { return deploymentSetId; }

    public static String emptyDeploymentSetId() { return EMPTY_SET_ID; }

    public void requireAdmissionOpen() {
        if (closed.get() || !admissionOpen.get()) {
            throw failure("DEPLOYMENT_SOURCE_CHANGED",
                    "deployment source verification changed; controlled restart required");
        }
    }

    /** Periodic fail-closed probe; it never mutates or replaces the verified images. */
    public boolean probeSources() {
        if (closed.get() || !admissionOpen.get()) return false;
        for (VerifiedDeploymentImage image : images.values()) {
            if (!image.sourceMatches()) {
                admissionOpen.set(false);
                return false;
            }
        }
        return admissionOpen.get();
    }

    void closeAdmission() {
        admissionOpen.set(false);
    }

    public DeploymentClassLoader classLoader(String deploymentId) {
        return image(deploymentId).classLoader(io.rowweave.Source.class.getClassLoader());
    }

    /** Used only by ObjectTrustContext: every application name has exactly one signed owner. */
    public Class<?> resolveTrustedClass(String serializedName) throws ClassNotFoundException {
        if (closed.get()) throw new ClassNotFoundException("deployment set is closed");
        String component = componentClassName(serializedName);
        VerifiedDeploymentImage owner = component == null ? null : classOwners.get(component);
        ClassLoader loader = owner == null ? io.rowweave.Source.class.getClassLoader()
                : owner.classLoader(io.rowweave.Source.class.getClassLoader());
        return Class.forName(serializedName, false, loader);
    }

    public boolean trustsClass(Class<?> type) {
        if (type == null || closed.get()) return false;
        VerifiedDeploymentImage owner = classOwners.get(type.getName());
        return owner != null && owner.ownsClass(type);
    }

    VerifiedDeploymentImage image(String deploymentId) {
        if (closed.get()) throw new IllegalStateException("deployment set is closed");
        VerifiedDeploymentImage image = images.get(deploymentId);
        if (image == null) throw new IllegalArgumentException("unknown deployment: " + deploymentId);
        return image;
    }

    /** Package-private signed-image view for bounded static capability discovery. */
    List<VerifiedDeploymentImage> imagesInCanonicalOrder() {
        if (closed.get()) throw new IllegalStateException("deployment set is closed");
        return Collections.unmodifiableList(new ArrayList<>(images.values()));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (VerifiedDeploymentImage image : images.values()) image.close();
        if (classIndexLease != null) classIndexLease.close();
        if (indexLease != null) indexLease.close();
    }

    private static String componentClassName(String serializedName) {
        if (serializedName == null || serializedName.isEmpty()) return null;
        if (serializedName.charAt(0) != '[') return serializedName;
        int arrays = 0;
        while (arrays < serializedName.length() && serializedName.charAt(arrays) == '[') arrays++;
        return arrays < serializedName.length() && serializedName.charAt(arrays) == 'L'
                && serializedName.endsWith(";")
                ? serializedName.substring(arrays + 1, serializedName.length() - 1) : null;
    }

    private static VerifiedDeploymentImage loadOne(
            DeploymentManifest manifest, Map<String, PublicKey> signerKeys,
            DeploymentMemoryBudget budget) {
        Path source = manifest.absoluteJarPath();
        final BasicFileAttributes before = attributes(source);
        if (!before.isRegularFile() || Files.isSymbolicLink(source)) {
            throw failure("DEPLOYMENT_SOURCE_NOT_REGULAR",
                    "source must be a non-symlink regular file: " + source);
        }
        final long actualLength = before.size();
        if (actualLength != manifest.jarLength()) {
            throw failure("DEPLOYMENT_LENGTH_MISMATCH", "manifest length does not match source");
        }
        DeploymentMemoryBudget.Lease compressedLease = budget.reserve(actualLength);
        try {
            byte[] image = new byte[Math.toIntExact(actualLength)];
            readExactly(source, image);
            BasicFileAttributes after = attributes(source);
            DeploymentSourceIdentity sourceIdentity = DeploymentSourceIdentity.of(before);
            if (Files.isSymbolicLink(source) || !sourceIdentity.matches(after)) {
                throw failure("DEPLOYMENT_SOURCE_CHANGED", "source changed during verified read");
            }
            if (!MessageDigest.isEqual(manifest.sha256(), sha256(image))) {
                throw failure("DEPLOYMENT_DIGEST_MISMATCH", "manifest digest does not match image");
            }
            verifySignature(manifest, signerKeys.get(manifest.signerKeyId()));
            DeploymentArchiveScanner.Index index = DeploymentArchiveScanner.scan(image, budget);
            return new VerifiedDeploymentImage(manifest, image, compressedLease, index, budget,
                    sourceIdentity);
        } catch (RuntimeException | Error failure) {
            compressedLease.close();
            throw failure;
        }
    }

    private static BasicFileAttributes attributes(Path source) {
        try {
            return Files.readAttributes(source, BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw failure("DEPLOYMENT_SOURCE_READ_FAILED", "cannot stat deployment source", failure);
        }
    }

    private static void readExactly(Path source, byte[] target) {
        try (InputStream input = Files.newInputStream(source)) {
            int offset = 0;
            while (offset < target.length) {
                int read = input.read(target, offset, target.length - offset);
                if (read < 0) throw failure("DEPLOYMENT_LENGTH_MISMATCH", "source truncated during read");
                if (read == 0) continue;
                offset += read;
            }
            if (input.read() != -1) {
                throw failure("DEPLOYMENT_LENGTH_MISMATCH", "source grew during read");
            }
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (IOException failure) {
            throw failure("DEPLOYMENT_SOURCE_READ_FAILED", "cannot read deployment source", failure);
        }
    }

    private static void verifySignature(DeploymentManifest manifest, PublicKey key) {
        if (key == null) {
            throw failure("DEPLOYMENT_SIGNER_UNKNOWN", manifest.signerKeyId());
        }
        if (!(key instanceof RSAKey) || ((RSAKey) key).getModulus().bitLength() < 3072) {
            throw failure("DEPLOYMENT_SIGNER_KEY_TOO_WEAK", manifest.signerKeyId());
        }
        try {
            Signature verifier = Signature.getInstance("SHA256withRSA");
            verifier.initVerify(key);
            verifier.update(DeploymentManifest.signingPayload(
                    manifest.deploymentId(), manifest.jarLength(), manifest.sha256()));
            if (!verifier.verify(manifest.detachedSignature())) {
                throw failure("DEPLOYMENT_SIGNATURE_INVALID", manifest.deploymentId());
            }
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (GeneralSecurityException failure) {
            throw failure("DEPLOYMENT_SIGNATURE_INVALID", manifest.deploymentId(), failure);
        }
    }

    private static long indexCharge(List<DeploymentManifest> manifests) {
        long bytes = 256L + 64L * manifests.size();
        for (DeploymentManifest manifest : manifests) {
            if (manifest != null) {
                bytes = Math.addExact(bytes, 2L * manifest.deploymentId().length());
                bytes = Math.addExact(bytes, 2L * manifest.signerKeyId().length());
                bytes = Math.addExact(bytes, 512L);
            }
        }
        return bytes;
    }

    private static String setId(List<DeploymentManifest> manifests) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (DeploymentManifest manifest : manifests) {
                updateAscii(digest, manifest.deploymentId());
                updateLong(digest, manifest.jarLength());
                digest.update(manifest.sha256());
                updateAscii(digest, manifest.signerKeyId());
            }
            return hex(digest.digest());
        } catch (GeneralSecurityException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void updateAscii(MessageDigest digest, String value) {
        updateInt(digest, value.length());
        for (int i = 0; i < value.length(); i++) digest.update((byte) value.charAt(i));
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static void updateLong(MessageDigest digest, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) digest.update((byte) (value >>> shift));
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (GeneralSecurityException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        char[] chars = new char[bytes.length * 2];
        final char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            chars[i * 2] = digits[(bytes[i] >>> 4) & 0xf];
            chars[i * 2 + 1] = digits[bytes[i] & 0xf];
        }
        return new String(chars);
    }

    private static DeploymentValidationException failure(String code, String message) {
        return new DeploymentValidationException(code, message);
    }

    private static DeploymentValidationException failure(
            String code, String message, Throwable cause) {
        return new DeploymentValidationException(code, message, cause);
    }
}
