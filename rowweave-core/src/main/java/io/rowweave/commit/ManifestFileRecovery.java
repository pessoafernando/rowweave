package io.rowweave.commit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

/** Bounded orphan recovery that runs only with the domain lease held and an exactly matching claim. */
public final class ManifestFileRecovery {

    public enum Status {
        ACTIVE_PRESERVED,
        INCOMPLETE,
        ORPHAN_REMOVED,
        COMMITTED_PRESERVED
    }

    public static final class Result {
        private final Status status;
        private final int deletedEntries;

        private Result(Status status, int deletedEntries) {
            this.status = status;
            this.deletedEntries = deletedEntries;
        }

        public Status status() {
            return status;
        }

        public int deletedEntries() {
            return deletedEntries;
        }
    }

    private final ManifestFileStore store;
    private final FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();

    public ManifestFileRecovery(Path domain) {
        store = new ManifestFileStore(domain);
    }

    /**
     * The caller must already have confirmed that the writer process is dead; this method still fences on lease plus durable claim plus exact manifest.
     */
    public Result recoverConfirmedDeadAttempt(
            String attemptId, long generation, int maxEntries) {
        if (generation <= 0L || maxEntries <= 0) {
            throw new IllegalArgumentException("generation and maxEntries must be > 0");
        }
        FileCommitDomainLease lease = FileCommitDomainLease.tryAcquire(store.domainPath());
        if (lease == null) {
            return new Result(Status.ACTIVE_PRESERVED, 0);
        }
        try (FileCommitDomainLease ignored = lease) {
            return recoverWithLease(attemptId, generation, maxEntries);
        }
    }

    Result recoverWithLease(String attemptId, long generation, int maxEntries) {
        Long claimed = authority.find(store.domainPath(), attemptId);
        if (claimed == null || claimed.longValue() != generation) {
            throw new IllegalArgumentException(
                    "attempt claim does not match recovery generation: attemptId=" + attemptId
                            + " generation=" + generation + " claimed=" + claimed);
        }
        CommitManifest committed = store.exact(generation);
        Budget budget = new Budget(maxEntries);
        if (committed == null) {
            deleteDirectoryEntries(store.domainPath().resolve("data")
                    .resolve(Long.toString(generation)), budget, null);
        }
        deleteDirectoryEntries(store.stagingDirectory(attemptId), budget, null);
        deleteDirectoryEntries(store.domainPath().resolve("manifests"), budget,
                manifestTempPrefix(generation, attemptId));
        if (budget.incomplete) {
            return new Result(Status.INCOMPLETE, budget.deleted);
        }
        return new Result(committed == null
                ? Status.ORPHAN_REMOVED : Status.COMMITTED_PRESERVED, budget.deleted);
    }

    public static String manifestTempPrefix(long generation, String attemptId) {
        if (generation <= 0L || attemptId == null || attemptId.isEmpty()) {
            throw new IllegalArgumentException("invalid generation/attemptId");
        }
        return ".rowweave-manifest-" + generation + "-"
                + hex(CommitManifest.sha256(attemptId.getBytes(StandardCharsets.UTF_8))) + "-";
    }

    private static void deleteDirectoryEntries(Path directory, Budget budget, String namePrefix) {
        if (budget.incomplete || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        boolean deletedAny = false;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (namePrefix != null && !name.startsWith(namePrefix)) {
                    continue;
                }
                if (budget.remaining == 0) {
                    budget.incomplete = true;
                    return;
                }
                if (Files.isSymbolicLink(entry)
                        || !Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalStateException(
                            "refusing to remove non-regular orphan artifact: " + entry);
                }
                Files.delete(entry);
                budget.remaining--;
                budget.deleted++;
                deletedAny = true;
            }
            if (deletedAny) {
                DurableFileOps.forceDirectory(directory);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed bounded manifest recovery scan: " + directory, e);
        }
        if (namePrefix == null) {
            deleteEmptyDirectory(directory);
        }
    }

    private static void deleteEmptyDirectory(Path directory) {
        try (DirectoryStream<Path> remaining = Files.newDirectoryStream(directory)) {
            if (remaining.iterator().hasNext()) {
                return;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to inspect recovered directory: " + directory, e);
        }
        try {
            if (Files.deleteIfExists(directory) && directory.getParent() != null) {
                DurableFileOps.forceDirectory(directory.getParent());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to remove recovered directory: " + directory, e);
        }
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

    private static final class Budget {
        private int remaining;
        private int deleted;
        private boolean incomplete;

        private Budget(int remaining) {
            this.remaining = remaining;
        }
    }
}
