package io.rowweave.commit;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Primitives to publish an immutable generation and read the highest legal generation for a manifest commit domain. */
public final class ManifestFileStore {

    private final Path domain;
    private final String domainId;
    private final DurableFileOps.Observer durabilityObserver;

    public ManifestFileStore(Path domain) {
        this(domain, (step, target) -> { });
    }

    ManifestFileStore(Path domain, DurableFileOps.Observer durabilityObserver) {
        if (domain == null) {
            throw new IllegalArgumentException("domain must not be null");
        }
        if (durabilityObserver == null) {
            throw new IllegalArgumentException("durabilityObserver must not be null");
        }
        this.domain = domain.toAbsolutePath().normalize();
        this.domainId = "manifest-file:" + canonicalIdentity(this.domain);
        this.durabilityObserver = durabilityObserver;
    }

    public String domainId() {
        return domainId;
    }

    public Path domainPath() {
        return domain;
    }

    /** attemptId is used only as a hashed staging locator; external text is never treated as a path. */
    public Path stagingPath(String attemptId, String memberName) {
        validateAttemptId(attemptId);
        validateMemberName(memberName);
        Path directory = stagingDirectory(attemptId);
        DurableFileOps.ensureDirectory(directory);
        return directory.resolve(memberName + ".tmp");
    }

    public Path stagingDirectory(String attemptId) {
        validateAttemptId(attemptId);
        String attemptHash = hex(CommitManifest.sha256(attemptId.getBytes(StandardCharsets.UTF_8)));
        return domain.resolve("staging").resolve(attemptHash);
    }

    /** Staging left open by close must be forced first; this step creates no visible generation. */
    public void forceStaged(Path staged) {
        requireOwnedStaging(staged);
        DurableFileOps.forceFile(staged);
    }

    /** Installs one immutable generation member; on retry an existing file is accepted only when its content matches exactly. */
    public CommitManifest.Member installMember(
            long generation, String memberName, String schemaFingerprint, Path staged) {
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        validateMemberName(memberName);
        requireOwnedStaging(staged);
        DurableFileOps.forceFile(staged);
        long length = size(staged);
        byte[] checksum = sha256(staged);
        String relative = "data/" + generation + "/" + memberName + ".data";
        Path target = domain.resolve(relative).normalize();
        try {
            DurableFileOps.installImmutable(staged, target);
        } catch (FileAlreadyExistsException existing) {
            requireOwnedRegularData(target);
            if (size(target) != length || !MessageDigest.isEqual(sha256(target), checksum)) {
                throw new IllegalStateException("immutable manifest member conflict: " + target, existing);
            }
        }
        requireOwnedRegularData(target);
        if (size(target) != length || !MessageDigest.isEqual(sha256(target), checksum)) {
            throw new IllegalStateException("installed manifest member failed verification: " + target);
        }
        return new CommitManifest.Member(memberName, relative, schemaFingerprint, length, checksum);
    }

    /** May be called only while the manifest for the exact generation has not appeared yet. */
    public void deleteUncommittedMember(long generation, String memberName, Path staged) {
        validateMemberName(memberName);
        if (exact(generation) != null) {
            throw new IllegalStateException("cannot delete data referenced by published manifest generation "
                    + generation);
        }
        Path target = domain.resolve("data").resolve(Long.toString(generation))
                .resolve(memberName + ".data").normalize();
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            requireOwnedRegularData(target);
            if (staged == null || !Files.isRegularFile(staged, LinkOption.NOFOLLOW_LINKS)
                    || size(target) != size(staged)
                    || !MessageDigest.isEqual(sha256(target), sha256(staged))) {
                throw new IllegalStateException(
                        "refusing to delete unowned/conflicting manifest data: " + target);
            }
            deleteAndForce(target);
        }
        if (staged != null) {
            Path normalized = staged.toAbsolutePath().normalize();
            if (normalized.startsWith(domain.resolve("staging"))) {
                deleteAndForce(normalized);
            }
        }
    }

    public void deleteStaging(Path staged) {
        if (staged == null) {
            return;
        }
        Path normalized = staged.toAbsolutePath().normalize();
        if (!normalized.startsWith(domain.resolve("staging"))) {
            throw new IllegalArgumentException("staging file is outside manifest domain: " + staged);
        }
        deleteAndForce(normalized);
    }

    public void publish(CommitManifest manifest) {
        if (manifest == null || !domainId.equals(manifest.domainId())) {
            throw new IllegalArgumentException("manifest commit domain mismatch");
        }
        validateMemberData(manifest);
        Path manifests = domain.resolve("manifests");
        DurableFileOps.ensureDirectory(manifests);
        Path target = manifests.resolve(Long.toString(manifest.generation()) + ".manifest");
        byte[] encoded = manifest.encode();
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            requireOwnedRegularManifest(target);
            requireIdentical(target, encoded);
            return;
        }
        try {
            String tag = manifest.generation() + "-" + hex(CommitManifest.sha256(
                    manifest.attemptId().getBytes(StandardCharsets.UTF_8)));
            DurableFileOps.atomicCreate(target, encoded, durabilityObserver, tag);
        } catch (FileAlreadyExistsException raced) {
            requireIdentical(target, encoded);
        }
    }

    public CommitManifest latest() {
        Path manifests = domain.resolve("manifests");
        if (!Files.isDirectory(manifests, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        Path highestPath = null;
        long highest = Long.MIN_VALUE;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(manifests)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (!name.endsWith(".manifest")) {
                    continue;
                }
                String token = name.substring(0, name.length() - ".manifest".length());
                final long generation;
                try {
                    generation = Long.parseLong(token);
                } catch (NumberFormatException malformed) {
                    throw new IllegalStateException("malformed manifest generation filename: " + entry,
                            malformed);
                }
                if (generation <= 0L) {
                    throw new IllegalStateException("invalid manifest generation filename: " + entry);
                }
                if (generation > highest) {
                    highest = generation;
                    highestPath = entry;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list manifest domain: " + manifests, e);
        }
        if (highestPath == null) {
            return null;
        }
        try {
            requireOwnedRegularManifest(highestPath);
            CommitManifest decoded = CommitManifest.decode(readManifest(highestPath));
            if (decoded.generation() != highest || !domainId.equals(decoded.domainId())) {
                throw new IllegalStateException("manifest identity does not match filename/domain: " + highestPath);
            }
            validateMemberData(decoded);
            return decoded;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("highest manifest is corrupt: " + highestPath, failure);
        }
    }

    /** Looks up the exact generation; returns null when missing, fails loudly when present but corrupt, and never falls back. */
    public CommitManifest exact(long generation) {
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        Path path = domain.resolve("manifests").resolve(generation + ".manifest");
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        try {
            requireOwnedRegularManifest(path);
            CommitManifest decoded = CommitManifest.decode(readManifest(path));
            if (decoded.generation() != generation || !domainId.equals(decoded.domainId())) {
                throw new IllegalStateException("manifest identity does not match filename/domain: " + path);
            }
            validateMemberData(decoded);
            return decoded;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("manifest generation is corrupt: " + path, failure);
        }
    }

    /**
     * The outcome is decided only from the exact generation the authority has resolved; history manifests are not scanned and an outcome index never substitutes for the visibility fact.
     */
    public ManifestCommitOutcome queryOutcome(String attemptId, Long generation) {
        validateAttemptId(attemptId);
        if (generation == null) {
            return ManifestCommitOutcome.notFound();
        }
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        final CommitManifest manifest;
        try {
            manifest = exact(generation);
        } catch (RuntimeException corrupt) {
            return ManifestCommitOutcome.conflict(
                    generation, "manifest is corrupt: " + corrupt.getMessage());
        }
        if (manifest == null) {
            return ManifestCommitOutcome.preparing(generation);
        }
        if (!attemptId.equals(manifest.attemptId())) {
            return ManifestCommitOutcome.conflict(
                    generation, "manifest attemptId conflicts with requested attemptId");
        }
        return ManifestCommitOutcome.committed(
                generation, CommitManifest.sha256(manifest.encode()));
    }

    private void validateMemberData(CommitManifest manifest) {
        Path generationRoot = domain.resolve("data")
                .resolve(Long.toString(manifest.generation())).normalize();
        for (CommitManifest.Member member : manifest.members()) {
            Path relative = Paths.get(member.relativeDataPath());
            Path data = domain.resolve(relative).normalize();
            if (!data.startsWith(generationRoot) || Files.isSymbolicLink(data)
                    || !Files.isRegularFile(data, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("manifest member is outside immutable generation data: " + data);
            }
            try {
                Path realGenerationRoot = generationRoot.toRealPath();
                Path realData = data.toRealPath();
                if (!realData.startsWith(realGenerationRoot)) {
                    throw new IllegalStateException(
                            "manifest member escapes generation through symlink ancestor: " + data);
                }
                long actualLength = Files.size(data);
                if (actualLength != member.length()) {
                    throw new IllegalStateException("manifest member length mismatch: " + member.name()
                            + " expected=" + member.length() + " actual=" + actualLength);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed to stat manifest member: " + data, e);
            }
            if (!MessageDigest.isEqual(sha256(data), member.sha256())) {
                throw new IllegalStateException("manifest member checksum mismatch: " + member.name());
            }
        }
    }

    private void requireOwnedStaging(Path staged) {
        if (staged == null) {
            throw new IllegalArgumentException("staging file must not be null");
        }
        Path normalized = staged.toAbsolutePath().normalize();
        Path stagingRoot = domain.resolve("staging");
        if (!normalized.startsWith(stagingRoot) || Files.isSymbolicLink(normalized)
                || !Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("staging file is not an owned regular file: " + staged);
        }
        try {
            if (!normalized.toRealPath().startsWith(stagingRoot.toRealPath())) {
                throw new IllegalStateException("staging file escapes manifest domain: " + staged);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to resolve staging file: " + staged, e);
        }
    }

    private static void validateAttemptId(String attemptId) {
        if (attemptId == null || attemptId.isEmpty()
                || attemptId.getBytes(StandardCharsets.UTF_8).length > 1024
                || attemptId.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("invalid manifest attemptId");
        }
    }

    private static void validateMemberName(String memberName) {
        new CommitManifest.Member(memberName, "data/1/probe.data", "probe", 0L,
                new byte[CommitManifest.SHA256_BYTES]);
    }

    private static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to stat manifest data: " + path, e);
        }
    }

    private static void requireOwnedRegularData(Path path) {
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("manifest data is not an owned regular file: " + path);
        }
    }

    private static void deleteAndForce(Path path) {
        try {
            if (Files.deleteIfExists(path)) {
                DurableFileOps.forceDirectory(path.getParent());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete uncommitted manifest artifact: " + path, e);
        }
    }

    private static void requireIdentical(Path target, byte[] expected) {
        byte[] actual = readManifest(target);
        if (!MessageDigest.isEqual(actual, expected)) {
            throw new IllegalStateException("manifest generation conflict: " + target);
        }
    }

    private static void requireOwnedRegularManifest(Path path) {
        if (Files.isSymbolicLink(path)
                || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("manifest is not an owned regular file: " + path);
        }
    }

    private static byte[] readManifest(Path path) {
        try {
            long size = Files.size(path);
            if (size < 0 || size > CommitManifest.MAX_ENCODED_BYTES) {
                throw new IllegalStateException("manifest exceeds encoded limit: " + path + " size=" + size);
            }
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read manifest: " + path, e);
        }
    }

    public static byte[] sha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[64 * 1024];
            try (InputStream in = Files.newInputStream(path)) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to hash manifest member: " + path, e);
        }
    }

    private static Path canonicalIdentity(Path path) {
        return ManifestDomainIdentity.canonicalPath(path);
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            result[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(result);
    }
}
