package io.rowweave.run;

/**
 * Bounded, immutable authority for a Run's successful terminal state. If the success response is lost, a client
 * recovers the exact rowsOut and publication visibility from the RunSnapshot alone.
 */
public final class RunResultSummary {
    public enum PublicationKind { DISCARD, MANIFEST }

    private static final int SHA256_BYTES = 32;

    private final long rowsOut;
    private final PublicationKind publicationKind;
    private final Long manifestGeneration;
    private final byte[] manifestSha256;
    private final byte[] sinkIdentityDigest;

    private RunResultSummary(long rowsOut, PublicationKind publicationKind,
                             Long manifestGeneration, byte[] manifestSha256,
                             byte[] sinkIdentityDigest) {
        if (rowsOut < 0L) throw new IllegalArgumentException("rowsOut must be >= 0");
        if (publicationKind == null) {
            throw new IllegalArgumentException("publicationKind is required");
        }
        if (publicationKind == PublicationKind.DISCARD) {
            if (manifestGeneration != null || manifestSha256 != null
                    || sinkIdentityDigest != null) {
                throw new IllegalArgumentException("discard summary cannot carry manifest identity");
            }
        } else if (manifestGeneration == null || manifestGeneration <= 0L
                || manifestSha256 == null || manifestSha256.length != SHA256_BYTES
                || (sinkIdentityDigest != null
                && sinkIdentityDigest.length != SHA256_BYTES)) {
            throw new IllegalArgumentException(
                    "manifest summary requires positive generation and SHA-256");
        }
        this.rowsOut = rowsOut;
        this.publicationKind = publicationKind;
        this.manifestGeneration = manifestGeneration;
        this.manifestSha256 = manifestSha256 == null ? null : manifestSha256.clone();
        this.sinkIdentityDigest = sinkIdentityDigest == null
                ? null : sinkIdentityDigest.clone();
    }

    public static RunResultSummary discard(long rowsOut) {
        return new RunResultSummary(
                rowsOut, PublicationKind.DISCARD, null, null, null);
    }

    public static RunResultSummary manifest(long rowsOut, long generation, byte[] manifestSha256) {
        return new RunResultSummary(rowsOut, PublicationKind.MANIFEST,
                generation, manifestSha256, null);
    }

    public static RunResultSummary manifest(
            long rowsOut, long generation, byte[] manifestSha256,
            byte[] sinkIdentityDigest) {
        return new RunResultSummary(rowsOut, PublicationKind.MANIFEST,
                generation, manifestSha256, sinkIdentityDigest);
    }

    /** Builds a DISCARD result only from durable successful output-stage receipts. */
    public static RunResultSummary discardFrom(StageAssignmentSnapshot evidence) {
        return discard(outputRows(evidence));
    }

    /** Builds a MANIFEST result only from durable receipts plus exact published identity. */
    public static RunResultSummary manifestFrom(StageAssignmentSnapshot evidence,
                                                long generation, byte[] manifestSha256) {
        return manifest(outputRows(evidence), generation, manifestSha256);
    }

    public static RunResultSummary manifestFrom(
            StageAssignmentSnapshot evidence, long generation,
            byte[] manifestSha256, byte[] sinkIdentityDigest) {
        return manifest(outputRows(evidence), generation,
                manifestSha256, sinkIdentityDigest);
    }

    private static long outputRows(StageAssignmentSnapshot evidence) {
        if (evidence == null) throw new IllegalArgumentException("assignment evidence is required");
        long rows = 0L;
        int outputs = 0;
        for (int i = 0; i < evidence.manifest().assignmentCount(); i++) {
            StageAssignmentRole role = evidence.manifest().assignment(i).role();
            if (role != StageAssignmentRole.SINK && role != StageAssignmentRole.WHOLE_GRAPH) {
                continue;
            }
            outputs++;
            StageTerminalReceipt terminal = evidence.terminal(i);
            if (evidence.acceptance(i) == null || terminal == null || !terminal.succeeded()) {
                throw new IllegalArgumentException(
                        "result summary requires successful accepted output evidence at " + i
                                + " (accepted=" + (evidence.acceptance(i) != null)
                                + ", terminal=" + (terminal == null ? "missing" : terminal.outcome())
                                + ")");
            }
            rows = Math.addExact(rows, terminal.outputRows());
        }
        if (outputs == 0) {
            throw new IllegalArgumentException("assignment evidence has no output role");
        }
        return rows;
    }

    public long rowsOut() { return rowsOut; }
    public PublicationKind publicationKind() { return publicationKind; }
    public Long manifestGeneration() { return manifestGeneration; }
    public byte[] manifestSha256() {
        return manifestSha256 == null ? null : manifestSha256.clone();
    }

    public byte[] sinkIdentityDigest() {
        return sinkIdentityDigest == null ? null : sinkIdentityDigest.clone();
    }

    byte[] manifestSha256Unsafe() { return manifestSha256; }
    byte[] sinkIdentityDigestUnsafe() { return sinkIdentityDigest; }
}
