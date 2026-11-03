package io.rowweave.artifact;

/** Only a READY authority can produce a zero-reference proof. */
public final class ArtifactReferenceView {
    public enum Code { READY_REFERENCED, READY_UNREFERENCED }

    private final Code code;
    private final long referenceCount;

    private ArtifactReferenceView(Code code, long referenceCount) {
        this.code = code;
        this.referenceCount = referenceCount;
    }

    public static ArtifactReferenceView referenced(long referenceCount) {
        if (referenceCount <= 0L) {
            throw new IllegalArgumentException("referenceCount must be > 0");
        }
        return new ArtifactReferenceView(Code.READY_REFERENCED, referenceCount);
    }

    public static ArtifactReferenceView unreferenced() {
        return new ArtifactReferenceView(Code.READY_UNREFERENCED, 0L);
    }

    public Code code() { return code; }
    public long referenceCount() { return referenceCount; }
}
