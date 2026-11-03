package io.rowweave.commit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongSupplier;

/** Bounded single-pass manifest orphan janitor admitted by the domain lease and claim retention. */
public final class ManifestFileJanitor {

    public enum Status {
        ACTIVE_PRESERVED,
        COMPLETED,
        PARTIAL
    }

    public static final class Report {
        private final Status status;
        private final int examinedClaims;
        private final int recoveredAttempts;
        private final int quarantinedAttempts;
        private final int deletedEntries;

        private Report(Status status, int examinedClaims, int recoveredAttempts,
                       int quarantinedAttempts, int deletedEntries) {
            this.status = status;
            this.examinedClaims = examinedClaims;
            this.recoveredAttempts = recoveredAttempts;
            this.quarantinedAttempts = quarantinedAttempts;
            this.deletedEntries = deletedEntries;
        }

        public Status status() { return status; }
        public int examinedClaims() { return examinedClaims; }
        public int recoveredAttempts() { return recoveredAttempts; }
        public int quarantinedAttempts() { return quarantinedAttempts; }
        public int deletedEntries() { return deletedEntries; }
    }

    private final ManifestFileStore store;
    private final FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
    private final ManifestFileRecovery recovery;
    private final long retentionMillis;
    private final int maxClaimsPerRun;
    private final int maxDeletesPerAttempt;
    private final LongSupplier clock;
    private final ManifestCleanupAuthority cleanupAuthority;
    private int nextShard;

    public ManifestFileJanitor(
            Path domain, long retentionMillis, int maxClaimsPerRun, int maxDeletesPerAttempt) {
        this(domain, retentionMillis, maxClaimsPerRun, maxDeletesPerAttempt,
                System::currentTimeMillis,
                (domainId, attemptId, generation) ->
                        ManifestCleanupAuthority.Decision.PRESERVE_LIVE);
    }

    public static ManifestFileJanitor withCleanupAuthority(
            Path domain, long retentionMillis, int maxClaimsPerRun, int maxDeletesPerAttempt,
            ManifestCleanupAuthority cleanupAuthority) {
        return new ManifestFileJanitor(
                domain, retentionMillis, maxClaimsPerRun, maxDeletesPerAttempt,
                System::currentTimeMillis, cleanupAuthority);
    }

        ManifestFileJanitor(Path domain, long retentionMillis, int maxClaimsPerRun,
                        int maxDeletesPerAttempt, LongSupplier clock) {
        this(domain, retentionMillis, maxClaimsPerRun, maxDeletesPerAttempt, clock,
                (domainId, attemptId, generation) ->
                        ManifestCleanupAuthority.Decision.PRESERVE_LIVE);
    }

    ManifestFileJanitor(Path domain, long retentionMillis, int maxClaimsPerRun,
                        int maxDeletesPerAttempt, LongSupplier clock,
                        ManifestCleanupAuthority cleanupAuthority) {
        if (retentionMillis < 0L || maxClaimsPerRun <= 0 || maxDeletesPerAttempt <= 0
                || clock == null || cleanupAuthority == null) {
            throw new IllegalArgumentException("invalid manifest janitor bounds");
        }
        store = new ManifestFileStore(domain);
        recovery = new ManifestFileRecovery(domain);
        this.retentionMillis = retentionMillis;
        this.maxClaimsPerRun = maxClaimsPerRun;
        this.maxDeletesPerAttempt = maxDeletesPerAttempt;
        this.clock = clock;
        this.cleanupAuthority = cleanupAuthority;
    }

    /** Concurrent calls still advance the shard cursor serially; across processes the domain lease remains the fence. */
    public synchronized Report runOnce() {
        FileCommitDomainLease lease = FileCommitDomainLease.tryAcquire(store.domainPath());
        if (lease == null) {
            return new Report(Status.ACTIVE_PRESERVED, 0, 0, 0, 0);
        }
        try (FileCommitDomainLease ignored = lease) {
            long now = clock.getAsLong();
            int[] recovered = {0};
            int[] quarantined = {0};
            int[] deleted = {0};
            boolean[] incomplete = {false};
            FileCommitGenerationAuthority.ClaimScan scan = authority.scanClaims(
                    store.domainPath(), maxClaimsPerRun, nextShard, (claimPath, claim) -> {
                        long modified = lastModified(claimPath);
                        if (now < modified || now - modified < retentionMillis) {
                            return;
                        }
                        ManifestCleanupAuthority.Decision cleanup =
                                cleanupAuthority.decide(
                                        store.domainId(), claim.attemptId(), claim.generation());
                        if (cleanup == null
                                || cleanup == ManifestCleanupAuthority.Decision.PRESERVE_LIVE) {
                            return;
                        }
                        if (cleanup
                                == ManifestCleanupAuthority.Decision.QUARANTINE_CONFLICT) {
                            quarantined[0]++;
                            writeQuarantine(claim.attemptId(), claim.generation(),
                                    new IllegalStateException(
                                            "cleanup authority reported conflicting evidence"));
                            return;
                        }
                        try {
                            ManifestFileRecovery.Result result = recovery.recoverWithLease(
                                    claim.attemptId(), claim.generation(), maxDeletesPerAttempt);
                            deleted[0] += result.deletedEntries();
                            if (result.status() == ManifestFileRecovery.Status.INCOMPLETE) {
                                incomplete[0] = true;
                            } else {
                                recovered[0]++;
                                deleteQuarantine(claim.attemptId());
                            }
                        } catch (RuntimeException cleanupFailure) {
                            quarantined[0]++;
                            writeQuarantine(claim.attemptId(), claim.generation(), cleanupFailure);
                        }
                    });
            nextShard = scan.nextShard();
            Status status = scan.truncated() || incomplete[0] ? Status.PARTIAL : Status.COMPLETED;
            return new Report(status, scan.examined(), recovered[0], quarantined[0], deleted[0]);
        }
    }

    Path quarantineRecord(String attemptId) {
        String hash = hex(CommitManifest.sha256(attemptId.getBytes(StandardCharsets.UTF_8)));
        return store.domainPath().resolve("quarantine").resolve(hash + ".record");
    }

    private void writeQuarantine(String attemptId, long generation, RuntimeException failure) {
        String message = String.valueOf(failure.getMessage());
        if (message.length() > 4_096) {
            message = message.substring(0, 4_096);
        }
        String record = "attemptId=" + attemptId + "\n"
                + "generation=" + generation + "\n"
                + "failure=" + failure.getClass().getName() + ": " + message + "\n";
        DurableFileOps.atomicReplace(
                quarantineRecord(attemptId), record.getBytes(StandardCharsets.UTF_8));
    }

    private void deleteQuarantine(String attemptId) {
        Path record = quarantineRecord(attemptId);
        try {
            if (Files.deleteIfExists(record)) {
                DurableFileOps.forceDirectory(record.getParent());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to clear manifest quarantine: " + record, e);
        }
    }

    private static long lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read manifest claim retention time: " + path, e);
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
}
