package io.rowweave.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Read-only bounded health view for the artifact janitor and dual migration barriers.  */
public final class ArtifactRetentionView {
    private final int backlog;
    private final int scanPageCapacity;
    private final String inventoryScope;
    private final int inventoryObservedShard;
    private final int inventoryEntries;
    private final long inventoryBytes;
    private final int inventoryEntryCapacity;
    private final long inventoryByteCapacity;
    private final long oldestCandidateAgeMillis;
    private final long deletedArtifacts;
    private final long retainedReferenced;
    private final long staleOwners;
    private final long errorArtifacts;
    private final long notReadyPasses;
    private final boolean cycleComplete;
    private final String referenceStatus;
    private final String inventoryStatus;
    private final List<String> diagnosticSamples;

    public ArtifactRetentionView(
            int backlog, int scanPageCapacity,
            long oldestCandidateAgeMillis, long deletedArtifacts,
            long retainedReferenced, long staleOwners,
            long errorArtifacts, long notReadyPasses,
            boolean cycleComplete, String referenceStatus,
            String inventoryStatus, List<String> diagnosticSamples) {
        this(backlog, scanPageCapacity, "UNKNOWN", -1,
                0, 0L, 1, 1L,
                oldestCandidateAgeMillis, deletedArtifacts,
                retainedReferenced, staleOwners, errorArtifacts,
                notReadyPasses, cycleComplete, referenceStatus,
                inventoryStatus, diagnosticSamples);
    }

    public ArtifactRetentionView(
            int backlog, int scanPageCapacity,
            String inventoryScope, int inventoryObservedShard,
            int inventoryEntries, long inventoryBytes,
            int inventoryEntryCapacity, long inventoryByteCapacity,
            long oldestCandidateAgeMillis, long deletedArtifacts,
            long retainedReferenced, long staleOwners,
            long errorArtifacts, long notReadyPasses,
            boolean cycleComplete, String referenceStatus,
            String inventoryStatus, List<String> diagnosticSamples) {
        if (backlog < 0 || scanPageCapacity < 1
                || inventoryEntries < 0 || inventoryBytes < 0L
                || inventoryEntryCapacity < 1 || inventoryByteCapacity < 1L
                || inventoryEntries > inventoryEntryCapacity
                || inventoryBytes > inventoryByteCapacity
                || oldestCandidateAgeMillis < 0L || deletedArtifacts < 0L
                || retainedReferenced < 0L || staleOwners < 0L
                || errorArtifacts < 0L || notReadyPasses < 0L) {
            throw new IllegalArgumentException(
                    "invalid Artifact retention metrics");
        }
        requireStatus(referenceStatus, "referenceStatus");
        requireStatus(inventoryStatus, "inventoryStatus");
        requireScope(inventoryScope, inventoryObservedShard);
        if (diagnosticSamples == null || diagnosticSamples.size() > 16) {
            throw new IllegalArgumentException(
                    "invalid Artifact retention diagnostic samples");
        }
        this.backlog = backlog;
        this.scanPageCapacity = scanPageCapacity;
        this.inventoryScope = inventoryScope;
        this.inventoryObservedShard = inventoryObservedShard;
        this.inventoryEntries = inventoryEntries;
        this.inventoryBytes = inventoryBytes;
        this.inventoryEntryCapacity = inventoryEntryCapacity;
        this.inventoryByteCapacity = inventoryByteCapacity;
        this.oldestCandidateAgeMillis = oldestCandidateAgeMillis;
        this.deletedArtifacts = deletedArtifacts;
        this.retainedReferenced = retainedReferenced;
        this.staleOwners = staleOwners;
        this.errorArtifacts = errorArtifacts;
        this.notReadyPasses = notReadyPasses;
        this.cycleComplete = cycleComplete;
        this.referenceStatus = referenceStatus;
        this.inventoryStatus = inventoryStatus;
        this.diagnosticSamples = Collections.unmodifiableList(
                new ArrayList<>(diagnosticSamples));
    }

    private static void requireScope(String scope, int shard) {
        if (!("UNKNOWN".equals(scope) || "GLOBAL".equals(scope)
                || "SHARD".equals(scope))) {
            throw new IllegalArgumentException("inventoryScope is invalid");
        }
        if (("SHARD".equals(scope) && (shard < 0 || shard > 255))
                || (!"SHARD".equals(scope) && shard != -1)) {
            throw new IllegalArgumentException(
                    "inventoryObservedShard is invalid");
        }
    }

    private static void requireStatus(String status, String field) {
        if (!("READY".equals(status) || "BACKFILLING".equals(status)
                || "CORRUPT".equals(status))) {
            throw new IllegalArgumentException(field + " is invalid");
        }
    }

    /** LIVE + DELETING counts observed in the most recent page scan, not claimed as globally exact totals.  */
    public int backlog() { return backlog; }
    public int scanPageCapacity() { return scanPageCapacity; }
    public String inventoryScope() { return inventoryScope; }
    public int inventoryObservedShard() { return inventoryObservedShard; }
    public int inventoryEntries() { return inventoryEntries; }
    public long inventoryBytes() { return inventoryBytes; }
    public int inventoryEntryCapacity() { return inventoryEntryCapacity; }
    public long inventoryByteCapacity() { return inventoryByteCapacity; }
    public long oldestCandidateAgeMillis() { return oldestCandidateAgeMillis; }
    public long deletedArtifacts() { return deletedArtifacts; }
    public long retainedReferenced() { return retainedReferenced; }
    public long staleOwners() { return staleOwners; }
    public long errorArtifacts() { return errorArtifacts; }
    public long notReadyPasses() { return notReadyPasses; }
    public boolean cycleComplete() { return cycleComplete; }
    public String referenceStatus() { return referenceStatus; }
    public String inventoryStatus() { return inventoryStatus; }
    public List<String> diagnosticSamples() { return diagnosticSamples; }
}
