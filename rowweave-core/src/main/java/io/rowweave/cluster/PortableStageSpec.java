package io.rowweave.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Bounded Worker-side slice of one portable physical stage assignment. */
final class PortableStageSpec {
    static final int MAX_MEMBERS = 4096;

    private final int logicalStage;
    private final List<Integer> members;
    private final int instanceIndex;
    private final int instanceCount;
    private final boolean canonicalizeSink;

    PortableStageSpec(
            int logicalStage, List<Integer> members,
            int instanceIndex, int instanceCount,
            boolean canonicalizeSink) {
        if (logicalStage < 0 || members == null || members.isEmpty()
                || members.size() > MAX_MEMBERS
                || instanceCount < 1 || instanceIndex < 0
                || instanceIndex >= instanceCount) {
            throw new IllegalArgumentException("invalid portable stage slice");
        }
        List<Integer> copy = new ArrayList<>(members.size());
        int previous = -1;
        for (Integer member : members) {
            if (member == null || member < 0 || member <= previous) {
                throw new IllegalArgumentException(
                        "portable stage members must be strictly increasing");
            }
            copy.add(member);
            previous = member;
        }
        this.logicalStage = logicalStage;
        this.members = Collections.unmodifiableList(copy);
        this.instanceIndex = instanceIndex;
        this.instanceCount = instanceCount;
        this.canonicalizeSink = canonicalizeSink;
    }

    int logicalStage() { return logicalStage; }
    List<Integer> members() { return members; }
    int instanceIndex() { return instanceIndex; }
    int instanceCount() { return instanceCount; }
    boolean canonicalizeSink() { return canonicalizeSink; }
}
