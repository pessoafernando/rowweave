package io.rowweave.incremental;

import java.util.Arrays;

/** Closed core comparator for concrete frontier kinds. */
public final class IncrementalFrontierOrder {
    private IncrementalFrontierOrder() {
    }

    public static int compareConcrete(IncrementalFrontier left, IncrementalFrontier right) {
        if (left == null || right == null) throw new IllegalArgumentException("frontiers are required");
        if (left.kind() == IncrementalFrontier.Kind.BEGIN_V1
                || right.kind() == IncrementalFrontier.Kind.BEGIN_V1) {
            throw new IllegalArgumentException("BEGIN is not an ordinary concrete frontier");
        }
        if (left.kind() != right.kind()) {
            throw new IllegalArgumentException("frontier kinds differ");
        }
        switch (left.kind()) {
            case INT64_SEQUENCE_V1:
                return Long.compare(left.sequence(), right.sequence());
            case TIMESTAMP_KEY_V1:
                int epoch = Long.compare(left.epochMicros(), right.epochMicros());
                return epoch != 0 ? epoch : OrderedFrontierKey.compare(
                        left.orderedKey(), right.orderedKey());
            case MANIFEST_GENERATION_V1:
                int generation = Long.compare(left.generation(), right.generation());
                if (generation != 0) return generation;
                if (!Arrays.equals(left.manifestDigest(), right.manifestDigest())) {
                    throw new FrontierConflictException(
                            "same manifest generation has different immutable digest");
                }
                return 0;
            default:
                throw new IllegalArgumentException("unsupported frontier kind");
        }
    }

    public static boolean isSnapshotTransition(
            IncrementalFrontier from, IncrementalFrontier target,
            IncrementalFrontier.Kind frozenConcreteKind) {
        if (from == null || target == null || frozenConcreteKind == null
                || from.kind() != IncrementalFrontier.Kind.BEGIN_V1
                || frozenConcreteKind == IncrementalFrontier.Kind.BEGIN_V1
                || target.kind() != frozenConcreteKind) {
            throw new IllegalArgumentException("invalid snapshot frontier transition");
        }
        return true;
    }
}
