package io.rowweave.cluster;

/**
 * The binding of one boundary placeholder source in a stage sub-plan to its upstream stage.
 * When a stage becomes an executable plan graph on its own, a child reference crossing the
 * stage boundary is replaced by a placeholder source, and at execution the upstream stage's
 * output is fed into it through a shuffle channel, in pure push with no rows buffered in a
 * list. Immutable.
 */
public final class StageBoundary {

    private final int placeholderSourceIndex;  // index of the placeholder source within the node list of the extracted stage plan
    private final int upstreamStage;  // upstream stage whose output feeds that placeholder source

    public StageBoundary(int placeholderSourceIndex, int upstreamStage) {
        this.placeholderSourceIndex = placeholderSourceIndex;
        this.upstreamStage = upstreamStage;
    }

    public int placeholderSourceIndex() {
        return placeholderSourceIndex;
    }

    public int upstreamStage() {
        return upstreamStage;
    }

    @Override
    public String toString() {
        return "boundary(src#" + placeholderSourceIndex + "<-stage" + upstreamStage + ")";
    }
}
