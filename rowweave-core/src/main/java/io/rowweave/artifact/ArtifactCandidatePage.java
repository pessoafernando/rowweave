package io.rowweave.artifact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** One page of canonical, immutable Artifact GC candidates, at most 1000 entries. */
public final class ArtifactCandidatePage {
    public static final int MAX_PAGE_SIZE = 1000;

    private final List<ArtifactCandidate> candidates;
    private final String nextCursor;
    private final boolean cycleComplete;

    public ArtifactCandidatePage(List<ArtifactCandidate> candidates,
                                 String nextCursor, boolean cycleComplete) {
        if (candidates == null) {
            throw new IllegalArgumentException("candidates is required");
        }
        if (candidates.size() > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("candidate page exceeds hard limit");
        }
        if (nextCursor != null && (nextCursor.isEmpty() || nextCursor.length() > 1024)) {
            throw new IllegalArgumentException("candidate cursor is invalid");
        }
        List<ArtifactCandidate> canonical = new ArrayList<>(candidates.size());
        Set<String> ids = new HashSet<>();
        for (ArtifactCandidate candidate : candidates) {
            if (candidate == null) {
                throw new IllegalArgumentException("null Artifact candidate");
            }
            if (!ids.add(candidate.artifactId())) {
                throw new IllegalArgumentException("duplicate Artifact candidate");
            }
            canonical.add(candidate);
        }
        canonical.sort(Comparator.comparing(ArtifactCandidate::artifactId));
        this.candidates = Collections.unmodifiableList(canonical);
        this.nextCursor = nextCursor;
        this.cycleComplete = cycleComplete;
    }

    public List<ArtifactCandidate> candidates() { return candidates; }
    public String nextCursor() { return nextCursor; }
    public boolean cycleComplete() { return cycleComplete; }
}
