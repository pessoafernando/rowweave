package io.rowweave.artifact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** One page of canonical, bounded DELETING recovery work. */
public final class ArtifactRecoveryPage {
    private final List<ArtifactRecoveryCandidate> candidates;
    private final String nextCursor;
    private final boolean cycleComplete;

    public ArtifactRecoveryPage(List<ArtifactRecoveryCandidate> candidates,
                                String nextCursor, boolean cycleComplete) {
        if (candidates == null) {
            throw new IllegalArgumentException("candidates is required");
        }
        if (candidates.size() > ArtifactCandidatePage.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("recovery page exceeds hard limit");
        }
        if (nextCursor != null
                && (nextCursor.isEmpty() || nextCursor.length() > 1024)) {
            throw new IllegalArgumentException("recovery cursor is invalid");
        }
        List<ArtifactRecoveryCandidate> canonical =
                new ArrayList<>(candidates.size());
        Set<String> ids = new HashSet<>();
        for (ArtifactRecoveryCandidate candidate : candidates) {
            if (candidate == null) {
                throw new IllegalArgumentException("null recovery candidate");
            }
            if (!ids.add(candidate.candidate().artifactId())) {
                throw new IllegalArgumentException("duplicate recovery candidate");
            }
            canonical.add(candidate);
        }
        canonical.sort(Comparator.comparing(
                value -> value.candidate().artifactId()));
        this.candidates = Collections.unmodifiableList(canonical);
        this.nextCursor = nextCursor;
        this.cycleComplete = cycleComplete;
    }

    public List<ArtifactRecoveryCandidate> candidates() { return candidates; }
    public String nextCursor() { return nextCursor; }
    public boolean cycleComplete() { return cycleComplete; }
}
