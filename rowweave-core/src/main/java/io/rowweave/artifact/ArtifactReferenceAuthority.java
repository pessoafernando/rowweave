package io.rowweave.artifact;

/** An exact Artifact reference index inside the Run authority, with the backfill barrier that gates its use. */
public interface ArtifactReferenceAuthority {
    /** Only a READY index may report READY_UNREFERENCED; not-ready, corrupt and unavailable all throw. */
    ArtifactReferenceView inspect(String artifactId);

    ArtifactBackfillStatus backfillStatus();

    /** Handles at most maxEntries lifecycle references in one call. */
    ArtifactBackfillStatus advanceBackfill(int maxEntries);
}
